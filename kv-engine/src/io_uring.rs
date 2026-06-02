use std::io;
use std::os::unix::io::RawFd;

use io_uring::{IoUring, opcode, squeue};

/// Thin wrapper around `io_uring::IoUring` for storage I/O.
///
/// Provides synchronous-looking APIs backed by async io_uring submissions.
/// Each call pushes an SQE, submits, and waits for the completion.
/// Uses incrementing user_data tokens to prevent stale CQE collision.
pub struct UringWriter {
    ring: IoUring,
    next_user_data: u64,
}

impl UringWriter {
    /// Create a new `UringWriter` with a ring of `entries` SQE slots.
    pub fn new(entries: u32) -> io::Result<Self> {
        let ring = IoUring::new(entries)?;
        Ok(Self {
            ring,
            next_user_data: 0,
        })
    }

    fn alloc_user_data(&mut self) -> u64 {
        let id = self.next_user_data;
        self.next_user_data = self.next_user_data.wrapping_add(1);
        id
    }

    /// Submit and wait, retrying on EINTR to ensure the kernel has finished
    /// reading our buffers before we return.
    fn submit_and_wait_retry(&self, want: usize) -> io::Result<usize> {
        loop {
            match self.ring.submit_and_wait(want) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Wait for a specific CQE token, draining orphaned entries as needed.
    /// Loops: drain CQ → check for token → if not found, submit_and_wait(1) → repeat.
    fn wait_for_token(&mut self, token: u64) -> io::Result<i32> {
        loop {
            let cq = self.ring.completion();
            for cqe in cq {
                if cqe.user_data() == token {
                    return Ok(cqe.result());
                }
                // orphaned CQE — consume and continue
            }
            // Token not found in current CQ, wait for more completions
            self.submit_and_wait_retry(1)?;
        }
    }

    /// Wait for two specific CQE tokens (used for linked write+fsync).
    fn wait_for_tokens(&mut self, token_a: u64, token_b: u64) -> io::Result<(i32, i32)> {
        let mut res_a = None;
        let mut res_b = None;
        loop {
            let cq = self.ring.completion();
            for cqe in cq {
                if cqe.user_data() == token_a {
                    res_a = Some(cqe.result());
                } else if cqe.user_data() == token_b {
                    res_b = Some(cqe.result());
                }
                // orphaned CQE — consume and continue
            }
            if let (Some(a), Some(b)) = (res_a, res_b) {
                return Ok((a, b));
            }
            // Not both found yet, wait for more completions
            self.submit_and_wait_retry(1)?;
        }
    }

    /// Write `data` at `offset` and fsync, linked as a single atomic operation.
    ///
    /// For append-mode files, the offset is ignored by the kernel.
    /// Returns the number of bytes written.
    pub fn write_and_fsync(&mut self, fd: RawFd, data: &[u8], offset: u64) -> io::Result<u32> {
        let len = u32::try_from(data.len())
            .map_err(|_| io::Error::other("data too large for io_uring write"))?;
        let write_token = self.alloc_user_data();
        let fsync_token = self.alloc_user_data();

        let write_e = opcode::Write::new(io_uring::types::Fd(fd), data.as_ptr(), len)
            .offset(offset)
            .build()
            .flags(squeue::Flags::IO_LINK)
            .user_data(write_token);

        let fsync_e = opcode::Fsync::new(io_uring::types::Fd(fd))
            .build()
            .user_data(fsync_token);

        unsafe {
            let mut sq = self.ring.submission();
            // Check capacity for both SQEs upfront to avoid leaving an orphaned
            // IO_LINK write in the queue if only the fsync push fails.
            if sq.len() + 2 > sq.capacity() {
                return Err(io::Error::other("sq full"));
            }
            sq.push(&write_e).map_err(|_| io::Error::other("sq full"))?;
            sq.push(&fsync_e).map_err(|_| io::Error::other("sq full"))?;
        }

        self.submit_and_wait_retry(2)?;

        let (write_res, fsync_res) = self.wait_for_tokens(write_token, fsync_token)?;
        if write_res < 0 {
            return Err(io::Error::from_raw_os_error(-write_res));
        }
        if fsync_res < 0 {
            return Err(io::Error::from_raw_os_error(-fsync_res));
        }

        Ok(write_res as u32)
    }

    /// Vectored write at `offset`. Returns total bytes written.
    pub fn writev(&mut self, fd: RawFd, iovecs: &[libc::iovec], offset: u64) -> io::Result<u32> {
        let iov_len =
            u32::try_from(iovecs.len()).map_err(|_| io::Error::other("too many iovecs"))?;
        let token = self.alloc_user_data();
        let entry = opcode::Writev::new(io_uring::types::Fd(fd), iovecs.as_ptr(), iov_len)
            .offset(offset)
            .build()
            .user_data(token);

        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| io::Error::other("sq full"))?;
        }

        self.submit_and_wait_retry(1)?;

        let res = self.wait_for_token(token)?;
        if res < 0 {
            return Err(io::Error::from_raw_os_error(-res));
        }
        Ok(res as u32)
    }

    /// Submit an fsync and wait for completion.
    pub fn fsync(&mut self, fd: RawFd) -> io::Result<()> {
        let token = self.alloc_user_data();
        let entry = opcode::Fsync::new(io_uring::types::Fd(fd))
            .build()
            .user_data(token);

        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| io::Error::other("sq full"))?;
        }

        self.submit_and_wait_retry(1)?;

        let res = self.wait_for_token(token)?;
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
        let result = uring.fsync(-1);
        assert!(result.is_err());
    }
}
