use std::io;
use std::os::unix::io::RawFd;

use io_uring::{IoUring, opcode, squeue};

/// Thin wrapper around `io_uring::IoUring` for storage I/O.
///
/// Provides synchronous-looking APIs backed by async io_uring submissions.
/// Each call pushes an SQE, submits, and waits for the completion.
pub struct UringWriter {
    ring: IoUring,
}

impl UringWriter {
    /// Create a new `UringWriter` with a ring of `entries` SQE slots.
    pub fn new(entries: u32) -> io::Result<Self> {
        let ring = IoUring::new(entries)?;
        Ok(Self { ring })
    }

    /// Write `data` at `offset` and fsync, linked as a single atomic operation.
    ///
    /// For append-mode files, the offset is ignored by the kernel.
    /// Returns the number of bytes written.
    pub fn write_and_fsync(&mut self, fd: RawFd, data: &[u8], offset: u64) -> io::Result<u32> {
        let write_e = opcode::Write::new(io_uring::types::Fd(fd), data.as_ptr(), data.len() as u32)
            .offset(offset)
            .build()
            .flags(squeue::Flags::IO_LINK)
            .user_data(0);

        let fsync_e = opcode::Fsync::new(io_uring::types::Fd(fd))
            .build()
            .user_data(1);

        unsafe {
            let mut sq = self.ring.submission();
            sq.push(&write_e).map_err(|_| io::Error::other("sq full"))?;
            sq.push(&fsync_e).map_err(|_| io::Error::other("sq full"))?;
        }

        self.ring.submit_and_wait(2)?;

        let cq = self.ring.completion();
        let mut write_res = None;
        let mut fsync_res = None;
        for cqe in cq {
            match cqe.user_data() {
                0 => write_res = Some(cqe.result()),
                1 => fsync_res = Some(cqe.result()),
                _ => {}
            }
        }

        let written = write_res.ok_or_else(|| io::Error::other("missing write cqe"))?;
        if written < 0 {
            return Err(io::Error::from_raw_os_error(-written));
        }
        let fsync_r = fsync_res.ok_or_else(|| io::Error::other("missing fsync cqe"))?;
        if fsync_r < 0 {
            return Err(io::Error::from_raw_os_error(-fsync_r));
        }

        Ok(written as u32)
    }

    /// Vectored write at `offset`. Returns total bytes written.
    pub fn writev(&mut self, fd: RawFd, iovecs: &[libc::iovec], offset: u64) -> io::Result<u32> {
        let entry = opcode::Writev::new(
            io_uring::types::Fd(fd),
            iovecs.as_ptr(),
            iovecs.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(0);

        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| io::Error::other("sq full"))?;
        }

        self.ring.submit_and_wait(1)?;

        // Drain all CQEs, match on user_data to find ours.
        let cq = self.ring.completion();
        let mut res = None;
        for cqe in cq {
            if cqe.user_data() == 0 {
                res = Some(cqe.result());
            }
        }
        let res = res.ok_or_else(|| io::Error::other("missing writev cqe"))?;
        if res < 0 {
            return Err(io::Error::from_raw_os_error(-res));
        }
        Ok(res as u32)
    }

    /// Submit an fsync and wait for completion.
    pub fn fsync(&mut self, fd: RawFd) -> io::Result<()> {
        let entry = opcode::Fsync::new(io_uring::types::Fd(fd))
            .build()
            .user_data(0);

        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| io::Error::other("sq full"))?;
        }

        self.ring.submit_and_wait(1)?;

        // Drain all CQEs, match on user_data to find ours.
        let cq = self.ring.completion();
        let mut res = None;
        for cqe in cq {
            if cqe.user_data() == 0 {
                res = Some(cqe.result());
            }
        }
        let res = res.ok_or_else(|| io::Error::other("missing fsync cqe"))?;
        if res < 0 {
            return Err(io::Error::from_raw_os_error(-res));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::io::AsRawFd;

    #[test]
    fn test_fsync_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_fsync.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"hello fsync").unwrap();

        let mut uring = UringWriter::new(4).unwrap();
        uring.fsync(f.as_raw_fd()).unwrap();
        drop(f);

        // Data must be durable after fsync
        let mut content = String::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert_eq!(content, "hello fsync");
    }

    #[test]
    fn test_writev_multiple_iovecs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_writev.bin");
        let f = std::fs::File::create(&path).unwrap();

        let part1 = b"hello ";
        let part2 = b"world";
        let part3 = b"!!!";

        let iovecs = [
            libc::iovec {
                iov_base: part1.as_ptr() as *mut libc::c_void,
                iov_len: part1.len(),
            },
            libc::iovec {
                iov_base: part2.as_ptr() as *mut libc::c_void,
                iov_len: part2.len(),
            },
            libc::iovec {
                iov_base: part3.as_ptr() as *mut libc::c_void,
                iov_len: part3.len(),
            },
        ];

        let mut uring = UringWriter::new(4).unwrap();
        let written = uring.writev(f.as_raw_fd(), &iovecs, 0).unwrap();
        assert_eq!(written as usize, 6 + 5 + 3);

        uring.fsync(f.as_raw_fd()).unwrap();
        drop(f);

        let content = std::fs::read(&path).unwrap();
        assert_eq!(content, b"hello world!!!");
    }

    #[test]
    fn test_write_and_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_linked.bin");
        let f = std::fs::File::create(&path).unwrap();

        let mut uring = UringWriter::new(8).unwrap();
        let written = uring
            .write_and_fsync(f.as_raw_fd(), b"linked write", 0)
            .unwrap();
        assert_eq!(written as usize, 12);
        drop(f);

        let content = std::fs::read(&path).unwrap();
        assert_eq!(content, b"linked write");
    }

    #[test]
    fn test_multiple_sequential_ops() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_multi.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        let mut uring = UringWriter::new(4).unwrap();

        // Multiple fsyncs on the same ring — CQE draining must not confuse them
        f.write_all(b"first").unwrap();
        uring.fsync(f.as_raw_fd()).unwrap();

        f.write_all(b"second").unwrap();
        uring.fsync(f.as_raw_fd()).unwrap();

        f.write_all(b"third").unwrap();
        uring.fsync(f.as_raw_fd()).unwrap();
        drop(f);

        let content = std::fs::read(&path).unwrap();
        assert_eq!(content, b"firstsecondthird");
    }

    #[test]
    fn test_fsync_bad_fd() {
        let mut uring = UringWriter::new(4).unwrap();
        // fd -1 is invalid
        let result = uring.fsync(-1);
        assert!(result.is_err());
    }
}
