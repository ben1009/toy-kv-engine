"""Ordinary file I/O only: compare fdatasync against per-write RWF_DSYNC."""

from pathlib import Path
import json
import mmap
import os
import stat
import sys
import time


def stats(values):
    if not values:
        return None
    a = sorted(values)
    return {"count": len(a), "mean_us": sum(a) / len(a) / 1000,
            "p50_us": a[len(a) // 2] / 1000, "p99_us": a[int((len(a) - 1) * .99)] / 1000,
            "max_us": a[-1] / 1000, "over_1ms": sum(n > 1_000_000 for n in a),
            "over_5ms": sum(n > 5_000_000 for n in a)}


p = Path(__file__).resolve().parent
reference = p.parent / "rfc-026-v8-background-affinity-20261102/standalone-direct-probe.data"
assert os.geteuid() == 1000 and os.sched_getaffinity(0) == {8}
s = reference.lstat()
assert stat.S_ISREG(s.st_mode) and s.st_size == 8 * 1024 * 1024 and s.st_uid == 1000
fd = os.open(reference, os.O_RDWR | os.O_DIRECT | os.O_NOFOLLOW)
assert os.fstat(fd).st_ino == s.st_ino
buf = mmap.mmap(-1, 4096)
payload = bytes(range(256)) * 16
buf[:] = payload
view = memoryview(buf)
sequence = ["flush4", "flush1", "dsync1", "dsync1", "flush1", "flush4"]
if len(sys.argv) == 2:
    assert sys.argv[1] in ["smoke", "restored"]
    sequence = ["dsync1", "flush1", "flush4"] if sys.argv[1] == "smoke" else ["flush4", "flush4"]
count = 16 if "smoke" in sys.argv else 1024
results = []
try:
    assert os.preadv(fd, [view], 0) == 4096 and bytes(view) == payload
    for mode in sequence:
        assert os.fstat(fd).st_ino == s.st_ino
        os.fdatasync(fd)  # identical prelude, excluded from each phase
        writes, syncs, request_ns = [], [], []
        started = time.monotonic_ns()
        for i in range(count):
            begin = time.monotonic_ns()
            if mode == "dsync1":
                assert os.pwritev(fd, [view], i * 4096, os.RWF_DSYNC) == 4096
            else:
                assert os.pwrite(fd, view, i * 4096) == 4096
            writes.append(time.monotonic_ns() - begin)
            if mode != "dsync1" and (mode == "flush1" or (i + 1) % 4 == 0):
                sync_start = time.monotonic_ns()
                os.fdatasync(fd)
                syncs.append(time.monotonic_ns() - sync_start)
            request_ns.append(time.monotonic_ns() - begin)
        ended = time.monotonic_ns()
        q = {"mode": mode, "pid": os.getpid(), "start_ns": started, "end_ns": ended,
             "ops_s": count / ((ended - started) / 1e9), "operations": count,
             "write": stats(writes), "sync": stats(syncs), "durable_request": stats(request_ns),
             "write_ns": writes, "sync_ns": syncs, "inode": s.st_ino}
        if mode == "flush4":
            q["first_after_sync"] = stats(writes[::4])
        results.append(q)
        time.sleep(.05)
    assert os.preadv(fd, [view], (count - 1) * 4096) == 4096 and bytes(view) == payload
finally:
    os.close(fd)
    view.release()
    buf.close()
print(json.dumps({"uid": os.geteuid(), "cpu_mask": sorted(os.sched_getaffinity(0)),
                  "path": str(reference), "inode": s.st_ino, "results": results}))
