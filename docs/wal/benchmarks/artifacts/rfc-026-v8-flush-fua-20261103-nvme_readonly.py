"""Small, read-only NVMe admin interface for the authorized diagnostic."""

import ctypes
import os


class Command(ctypes.Structure):
    _fields_ = [("opcode", ctypes.c_uint8), ("flags", ctypes.c_uint8),
                ("rsvd1", ctypes.c_uint16), ("nsid", ctypes.c_uint32),
                ("cdw2", ctypes.c_uint32), ("cdw3", ctypes.c_uint32),
                ("metadata", ctypes.c_uint64), ("addr", ctypes.c_uint64),
                ("metadata_len", ctypes.c_uint32), ("data_len", ctypes.c_uint32),
                *[(f"cdw{i}", ctypes.c_uint32) for i in range(10, 16)],
                ("timeout_ms", ctypes.c_uint32), ("result", ctypes.c_uint32)]


assert ctypes.sizeof(Command) == 72
IOCTL_ADMIN = (3 << 30) | (72 << 16) | (ord("N") << 8) | 0x41
libc = ctypes.CDLL(None, use_errno=True)
libc.ioctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_void_p]
libc.ioctl.restype = ctypes.c_int


def read_admin(fd, opcode, length=0, nsid=0, cdw10=0, cdw11=0):
    assert (opcode, cdw10) in [(0x06, 1), (0x02, (127 << 16) | 2), (0x0A, 0x0C), (0x0A, 0x06)]
    payload = ctypes.create_string_buffer(length) if length else None
    command = Command(opcode=opcode, nsid=nsid, data_len=length, cdw10=cdw10,
                      cdw11=cdw11, timeout_ms=5000,
                      addr=ctypes.addressof(payload) if payload is not None else 0)
    rc = libc.ioctl(fd, IOCTL_ADMIN, ctypes.byref(command))
    if rc < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    if rc:
        raise RuntimeError(f"NVMe status {rc:#x} for read-only opcode {opcode:#x}")
    return command.result, payload.raw if payload is not None else b""
