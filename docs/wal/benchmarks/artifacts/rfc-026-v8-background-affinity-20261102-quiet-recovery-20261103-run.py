"""One paired durability probe around a fixed powered-on quiet interval."""

from pathlib import Path
import ctypes
import datetime
import hashlib
import json
import mmap
import os
import resource
import signal
import stat
import subprocess
import time


p = Path(__file__).resolve().parent
reference = p.parent / "rfc-026-v8-background-affinity-20261102/standalone-direct-probe.data"
quiet_seconds = 30 * 60
count = 1024
block_size = 4096
report = {
    "purpose": "Test whether a fixed 30-minute powered-on interval without benchmark writes changes same-file durable-write latency. Does not identify garbage collection or qualify engine performance.",
    "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "reference": str(reference), "block_bytes": block_size,
    "operations_per_probe": count, "bytes_written_per_probe": count * block_size,
    "fdatasync_every_writes": 4, "quiet_seconds_planned": quiet_seconds,
    "allowed_cpus": sorted(os.sched_getaffinity(0)),
    "settings_changed": False, "device_admin_queries": False,
    "quiet_method": "No benchmark, build, maintenance or profiling process is launched during the pause. Read diskstats every five seconds and process names each minute; keep samples in RAM until after the endpoint probe. OS background work is observed, not disabled.",
    "comparison": "Both endpoints use the same already initialized 8 MiB inode and same offsets/payload/sync schedule. There is no allocation, truncate, cache drop, TRIM or reinitialization between endpoints. The measured writes remain durable. Small diagnostic sample, not a V4/V7/V8 score.",
    "probes": [], "quiet_samples": [], "interfering_jobs": [],
    "complete": False,
}
assert report["allowed_cpus"] == [8]


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def disk_snapshot():
    names = ["reads", "reads_merged", "sectors_read", "read_ms", "writes", "writes_merged",
             "sectors_written", "write_ms", "inflight", "io_ms", "weighted_io_ms",
             "discards", "discards_merged", "sectors_discarded", "discard_ms", "flushes", "flush_ms"]
    for line in Path("/proc/diskstats").read_text().splitlines():
        fields = line.split()
        if fields[2] == "nvme0n1":
            return dict(zip(names, map(int, fields[3:])))
    raise RuntimeError("Physical NVMe device is absent")


def delta(before, after):
    return {k: after[k] - before[k] for k in before if k != "inflight"}


def jobs():
    found = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            comm = (proc / "comm").read_text().strip()
            if comm.startswith(("write-perf", "rustc", "cargo")) or comm in ["fio", "perf", "dd", "fstrim", "sst"]:
                found.append({"pid": int(proc.name), "comm": comm})
        except OSError:
            pass
    return found


def stats(values):
    ordered = sorted(values)
    return {"count": len(ordered), "mean_us": sum(ordered) / len(ordered) / 1000,
            "p50_us": ordered[len(ordered) // 2] / 1000,
            "p99_us": ordered[int((len(ordered) - 1) * .99)] / 1000,
            "max_us": ordered[-1] / 1000,
            "over_1ms": sum(n > 1_000_000 for n in ordered),
            "over_5ms": sum(n > 5_000_000 for n in ordered)}


def interrupt(signum, _frame):
    report["interrupted_by_signal"] = signum
    raise InterruptedError(f"quiet recovery interrupted by signal {signum}")


signal.signal(signal.SIGTERM, interrupt)
signal.signal(signal.SIGINT, interrupt)
assert not (p / "result.json").exists()
s = reference.lstat()
assert stat.S_ISREG(s.st_mode) and s.st_size == 8 * 1024 * 1024 and s.st_uid == os.getuid()
report["inode"] = s.st_ino
report["device"] = s.st_dev
report["filesystem"] = subprocess.check_output(
    ["findmnt", "-n", "-o", "FSTYPE,SOURCE,TARGET", "-T", str(reference)], text=True).strip()
assert report["filesystem"].startswith("ext4 /dev/nvme0n1p3 ")
report["initial_jobs"] = jobs()
assert not report["initial_jobs"], report["initial_jobs"]

buffer = mmap.mmap(-1, block_size)
assert ctypes.addressof(ctypes.c_char.from_buffer(buffer)) % block_size == 0
payload = bytes(range(256)) * (block_size // 256)
buffer[:] = payload
view = memoryview(buffer)
fd = os.open(reference, os.O_RDWR | os.O_DIRECT | os.O_NOFOLLOW)
assert os.fstat(fd).st_ino == s.st_ino


def probe(name):
    assert os.fstat(fd).st_ino == s.st_ino and os.fstat(fd).st_size == s.st_size
    # Same durable prelude at both endpoints, outside their timed writes.
    os.fdatasync(fd)
    before = disk_snapshot()
    usage_before = resource.getrusage(resource.RUSAGE_SELF)
    writes, syncs = [], []
    started = time.perf_counter_ns()
    for index in range(count):
        offset = index * block_size
        begin = time.perf_counter_ns()
        assert os.pwrite(fd, view, offset) == block_size
        writes.append(time.perf_counter_ns() - begin)
        if (index + 1) % 4 == 0:
            begin = time.perf_counter_ns()
            os.fdatasync(fd)
            syncs.append(time.perf_counter_ns() - begin)
    elapsed = (time.perf_counter_ns() - started) / 1e9
    usage_after = resource.getrusage(resource.RUSAGE_SELF)
    after = disk_snapshot()
    assert os.preadv(fd, [view], (count - 1) * block_size) == block_size
    assert bytes(view) == payload
    result = {"name": name, "utc": now(), "elapsed_s": elapsed, "ops_s": count / elapsed,
              "write": stats(writes), "sync": stats(syncs),
              "write_ns": writes, "sync_ns": syncs,
              "diskstats_before": before, "diskstats_after": after,
              "diskstats_delta": delta(before, after),
              "cpu_s": usage_after.ru_utime + usage_after.ru_stime - usage_before.ru_utime - usage_before.ru_stime,
              "context": {"voluntary": usage_after.ru_nvcsw - usage_before.ru_nvcsw,
                          "involuntary": usage_after.ru_nivcsw - usage_before.ru_nivcsw},
              "final_block_contents_validated": True}
    report["probes"].append(result)
    print(json.dumps({k: v for k, v in result.items() if k in ["name", "utc", "elapsed_s", "ops_s", "write", "sync"]}), flush=True)


try:
    assert os.preadv(fd, [view], 0) == block_size
    assert bytes(view) == payload, "Existing disposable reference has unexpected content"
    probe("before-quiet")
    start = time.monotonic()
    report["quiet_started_utc"] = now()
    initial = disk_snapshot()
    report["quiet_diskstats_before"] = initial
    next_minute = 60
    while time.monotonic() - start < quiet_seconds:
        time.sleep(min(5, max(0, quiet_seconds - (time.monotonic() - start))))
        elapsed = time.monotonic() - start
        snapshot = disk_snapshot()
        report["quiet_samples"].append({"elapsed_s": elapsed, "utc": now(), "diskstats": snapshot})
        if elapsed >= next_minute:
            active = jobs()
            if active:
                report["interfering_jobs"].append({"elapsed_s": elapsed, "jobs": active})
            difference = delta(initial, snapshot)
            print(json.dumps({"quiet_minutes": round(elapsed / 60, 2),
                              "whole_device_read_mib": difference["sectors_read"] / 2048,
                              "whole_device_write_mib": difference["sectors_written"] / 2048,
                              "whole_device_discard_mib": difference["sectors_discarded"] / 2048,
                              "whole_device_flushes": difference["flushes"],
                              "inflight": snapshot["inflight"], "interfering_jobs": active}), flush=True)
            next_minute += 60
    report["quiet_elapsed_s"] = time.monotonic() - start
    report["quiet_diskstats_after"] = disk_snapshot()
    report["quiet_diskstats_delta"] = delta(initial, report["quiet_diskstats_after"])
    report["final_jobs"] = jobs()
    assert not report["final_jobs"], report["final_jobs"]
    probe("after-quiet")
    report["complete"] = True
finally:
    report["finished_utc"] = now()
    os.close(fd)
    view.release()
    buffer.close()
    (p / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"complete": report["complete"], "report": str(p / "result.json")}), flush=True)
