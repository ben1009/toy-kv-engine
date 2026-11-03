from pathlib import Path
import json
import mmap
import os
import resource
import time


p = Path(__file__).resolve().parent
path = p / "standalone-direct-probe.data"
size = 8 * 1024 * 1024
count = 8192


def device_snapshot():
    for line in Path("/proc/diskstats").read_text().splitlines():
        fields = line.split()
        if fields[2] == "nvme0n1":
            values = list(map(int, fields[3:]))
            names = ["reads", "reads_merged", "sectors_read", "read_ms", "writes", "writes_merged", "sectors_written", "write_ms", "inflight", "io_ms", "weighted_io_ms", "discards", "discards_merged", "sectors_discarded", "discard_ms", "flushes", "flush_ms"]
            return dict(zip(names, values))
    raise RuntimeError("NVMe diskstats absent")


def stats(values):
    if not values:
        return None
    ordered = sorted(values)
    return {"count": len(values), "mean_us": sum(values) / len(values) / 1000,
            "p50_us": ordered[len(values) // 2] / 1000,
            "p99_us": ordered[int((len(values) - 1) * .99)] / 1000,
            "max_us": ordered[-1] / 1000,
            "over_1ms": sum(x > 1_000_000 for x in values),
            "over_5ms": sum(x > 5_000_000 for x in values)}


buffer = mmap.mmap(-1, 1024 * 1024)
buffer[:] = bytes(range(256)) * (len(buffer) // 256)
view = memoryview(buffer)
fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_DIRECT, 0o600)
try:
    os.posix_fallocate(fd, 0, size)
    for offset in range(0, size, len(buffer)):
        assert os.pwrite(fd, view, offset) == len(buffer)
    os.fdatasync(fd)
    report = {"pid": os.getpid(), "allowed_cpus": sorted(os.sched_getaffinity(0)),
              "filesystem": "same local ext4 as WAL captures", "path": str(path),
              "initialized_file_bytes": size, "block_bytes": 4096, "operations_per_phase": count,
              "scope": "Synchronous Python os.pwrite/preadv O_DIRECT and fdatasync; no engine, io_uring, MVCC, memtable, WAL coordinator, or affinity-registration hook. Diagnostic only, not an adoption score.",
              "phases": []}
    for phase in ["sync-every-four-first", "write-without-periodic-sync", "direct-read", "sync-every-four-second"]:
        before = device_snapshot()
        usage_before = resource.getrusage(resource.RUSAGE_SELF)
        operations, syncs, slow = [], [], []
        started = time.perf_counter_ns()
        for index in range(count):
            offset = (index * 4096) % size
            begin = time.perf_counter_ns()
            if phase == "direct-read":
                assert os.preadv(fd, [view[:4096]], offset) == 4096
            else:
                assert os.pwrite(fd, view[:4096], offset) == 4096
            duration = time.perf_counter_ns() - begin
            operations.append(duration)
            if duration > 1_000_000:
                slow.append({"index": index, "offset": offset, "duration_ns": duration})
            if phase.startswith("sync-") and (index + 1) % 4 == 0:
                begin = time.perf_counter_ns()
                os.fdatasync(fd)
                syncs.append(time.perf_counter_ns() - begin)
        elapsed = (time.perf_counter_ns() - started) / 1e9
        usage_after = resource.getrusage(resource.RUSAGE_SELF)
        after = device_snapshot()
        delta = {key: after[key] - before[key] for key in before if key != "inflight"}
        record = {"phase": phase, "elapsed_s": elapsed, "ops_s": count / elapsed,
                  "operation": stats(operations), "sync": stats(syncs), "diskstats_delta": delta,
                  "cpu_s": usage_after.ru_utime + usage_after.ru_stime - usage_before.ru_utime - usage_before.ru_stime,
                  "context": {"voluntary": usage_after.ru_nvcsw - usage_before.ru_nvcsw,
                              "involuntary": usage_after.ru_nivcsw - usage_before.ru_nivcsw},
                  "slowest_operations": sorted(slow, key=lambda x: x["duration_ns"], reverse=True)[:20]}
        report["phases"].append(record)
        print(json.dumps({key: record[key] for key in ["phase", "elapsed_s", "ops_s", "operation", "sync", "diskstats_delta"]}), flush=True)
        if phase == "write-without-periodic-sync":
            # Deliberately outside that phase: its score makes no durability claim.
            os.fdatasync(fd)
    (p / "storage-probe.json").write_text(json.dumps(report, indent=2) + "\n")
finally:
    os.close(fd)
    view.release()
    buffer.close()
