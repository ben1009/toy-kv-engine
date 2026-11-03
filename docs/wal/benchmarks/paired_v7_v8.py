"""Run bounded V7/V8 diagnostic blocks with independent storage probes.

Uses the same instrumented write-perf binary for both formats. No device
features, scheduler settings, or runtime sources are changed. See
paired-v7-v8-method.md for the limits of endpoint storage matching.
"""

import argparse
import ctypes
import hashlib
import json
import math
import mmap
import os
from pathlib import Path
import signal
import stat
import subprocess
import time


POLICY = {
    "fast_below_us": 100,
    "slow_at_or_above_us": 500,
    "post_flush_p50_drift": [50, 0.25],
    "post_flush_p99_drift": [150, 0.50],
    "sync_p50_drift": [50, 0.35],
    "sync_p99_drift": [250, 0.50],
    "long_write_count_drift": [8, 0.50],
}
ORDERS = (("v7", "v8", "v8", "v7"), ("v8", "v7", "v7", "v8"))


def stats(values):
    values = sorted(values)
    return {
        "count": len(values),
        "p50_us": values[len(values) // 2] / 1000,
        "p99_us": values[int((len(values) - 1) * 0.99)] / 1000,
        "mean_us": sum(values) / len(values) / 1000,
        "max_us": values[-1] / 1000,
        "over_5ms": sum(v > 5_000_000 for v in values),
    }


def state_for_median(value, policy=POLICY):
    if value < policy["fast_below_us"]:
        return "fast"
    if value >= policy["slow_at_or_above_us"]:
        return "slow"
    return "intermediate"


def drift(a, b, limit):
    absolute, relative = limit
    return abs(a - b) > max(absolute, relative * min(a, b))


def probe_state(probe, policy=POLICY):
    first = probe["write_ns"][::4]
    quarter_size = len(first) // 4
    quarters = [stats(first[i * quarter_size:(i + 1) * quarter_size])["p50_us"] for i in range(4)]
    states = {state_for_median(v, policy) for v in quarters}
    if len(states) != 1 or drift(min(quarters), max(quarters), policy["post_flush_p50_drift"]):
        return "transitioning"
    return state_for_median(stats(first)["p50_us"], policy)


def compare_probes(a, b, policy=POLICY):
    reasons = []
    if a["storage_state"] != b["storage_state"] or a["storage_state"] == "transitioning":
        reasons.append("storage_state_changed_or_transitioning")
    for name, x, y, limit in [
        ("post_flush_p50", a["first_after_sync"]["p50_us"], b["first_after_sync"]["p50_us"], "post_flush_p50_drift"),
        ("post_flush_p99", a["first_after_sync"]["p99_us"], b["first_after_sync"]["p99_us"], "post_flush_p99_drift"),
        ("sync_p50", a["sync"]["p50_us"], b["sync"]["p50_us"], "sync_p50_drift"),
        ("sync_p99", a["sync"]["p99_us"], b["sync"]["p99_us"], "sync_p99_drift"),
        ("long_write_count", a["write"]["over_5ms"], b["write"]["over_5ms"], "long_write_count_drift"),
    ]:
        if drift(x, y, policy[limit]):
            reasons.append(name + "_drift")
    return reasons


def summarize_block(block, policy=POLICY):
    reasons = []
    for index, probe in enumerate(block["probes"]):
        if index:
            reasons += [f"probe_{index}:" + why for why in compare_probes(block["probes"][0], probe, policy)]
        if probe["storage_state"] == "transitioning":
            reasons.append(f"probe_{index}:within_probe_transition")
    observed_state = "transitioning" if reasons else (
        block["probes"][0]["storage_state"] if block["probes"] else "unknown")
    runs = block["runs"]
    if len(runs) != 4 or len(block["probes"]) != 5:
        reasons.append("incomplete_block")
    if len({r["binary_sha256"] for r in runs}) != 1:
        reasons.append("binary_mismatch")
    if len({r["boot_id"] for r in runs}) != 1:
        reasons.append("boot_mismatch")
    if len({tuple(r["cpu_mask"]) for r in runs}) != 1:
        reasons.append("cpu_mask_mismatch")
    workloads = {json.dumps({"params": {key: r["record"]["params"].get(key) for key in (
        "num", "value_size", "batch_size", "threads", "seed", "latency_sample_every", "wal_io_mode")},
        "engine_options": r["record"].get("engine_options")}, sort_keys=True) for r in runs}
    if len(workloads) != 1:
        reasons.append("workload_mismatch")
    if tuple(r["format"] for r in runs) not in ORDERS:
        reasons.append("order_mismatch")
    pairs = []
    if not reasons:
        for offset in (0, 2):
            pair = {r["format"]: r for r in runs[offset:offset + 2]}
            ratios = {name: pair["v8"]["metrics"][name] / pair["v7"]["metrics"][name]
                      for name in ("puts_s", "p99_ms", "cpu_s", "syncs", "voluntary")}
            pairs.append({"run_indices": [offset, offset + 1], "v8_over_v7": ratios})
    return {
        "storage_state": observed_state,
        "probe_compatible_diagnostic_block": not reasons,
        "rejection_reasons": reasons,
        "adjacent_pairs": pairs,
        "paired_geomean_v8_over_v7": {
            key: math.sqrt(pairs[0]["v8_over_v7"][key] * pairs[1]["v8_over_v7"][key])
            for key in pairs[0]["v8_over_v7"]
        } if pairs else None,
        "all_runs_pass_existing_storage_filter": len(runs) == 4 and all(
            not r["metrics"]["existing_storage_flag"] for r in runs),
        "RFC_performance_qualified": False,
        "scope": "Instrumented paired diagnostic only. Matched probes cannot prove identical device state throughout a run or identical I/O completion patterns.",
    }


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def disk_snapshot():
    for line in Path("/proc/diskstats").read_text().splitlines():
        fields = line.split()
        if fields[2] == "nvme0n1":
            return {"writes": int(fields[7]), "sectors_written": int(fields[9]),
                    "flushes": int(fields[18]), "inflight": int(fields[11])}
    raise RuntimeError("Expected physical NVMe diskstats are absent")


def storage_probe(fd, buf, cpu, count=1024):
    saved_mask = os.sched_getaffinity(0)
    os.sched_setaffinity(0, {cpu})
    try:
        os.fdatasync(fd)
        writes, syncs = [], []
        before = disk_snapshot()
        started = time.monotonic_ns()
        for index in range(count):
            begin = time.monotonic_ns()
            if os.pwrite(fd, buf, index * 4096) != 4096:
                raise RuntimeError("Short direct probe write")
            writes.append(time.monotonic_ns() - begin)
            if (index + 1) % 4 == 0:
                begin = time.monotonic_ns()
                os.fdatasync(fd)
                syncs.append(time.monotonic_ns() - begin)
        ended = time.monotonic_ns()
        after = disk_snapshot()
    finally:
        os.sched_setaffinity(0, saved_mask)
    result = {"start_ns": started, "end_ns": ended, "write_ns": writes, "sync_ns": syncs,
              "write": stats(writes), "first_after_sync": stats(writes[::4]), "sync": stats(syncs),
              "diskstats_before": before, "diskstats_after": after, "probe_cpu": cpu}
    result["storage_state"] = probe_state(result)
    return result


def phase_estimates(phases):
    result = {name: {"cpu_s": 0, "wall_s": 0, "calls": 0, "samples": 0} for name in phases["phase_names"]}
    for thread in phases["threads"]:
        for name, bucket in zip(phases["phase_names"], thread["buckets"], strict=True):
            total = result[name]
            total["calls"] += bucket["calls"]
            total["samples"] += bucket["samples"]
            if bucket["samples"]:
                scale = bucket["calls"] / bucket["samples"] / 1e9
                total["cpu_s"] += bucket["cpu_ns"] * scale
                total["wall_s"] += bucket["wall_ns"] * scale
    return result


def analyze_directory(out):
    plan = json.loads((out / "plan.json").read_text())
    raw_blocks = [json.loads((out / f"block{i}.json").read_text()) if (out / f"block{i}.json").is_file()
                  else {"runs": [], "probes": []} for i in range(2)]
    blocks = [summarize_block(block, plan["policy"]) for block in raw_blocks]
    summary = {"blocks": blocks, "all_raw_blocks_preserved": all(
                   len(block["runs"]) == 4 and len(block["probes"]) == 5 for block in raw_blocks),
               "compatible_blocks": sum(b["probe_compatible_diagnostic_block"] for b in blocks),
               "RFC_performance_qualified": False, "runtime_changes": False}
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def run_capture(binary, out, name, fmt, args, binary_hash, boot_id):
    if sha256(binary) != binary_hash or Path("/proc/sys/kernel/random/boot_id").read_text().strip() != boot_id:
        raise RuntimeError("Binary or boot changed during cohort")
    command = ["taskset", "-c", args.cpus, str(binary), "--bench", "wal_batch_concurrent",
               "--wal-batch-size", "64", "--pitr", "--pitr-format", fmt,
               "--wal-io-mode", "parallel", "--num", str(args.num), "--threads", "16",
               "--value-size", "1024", "--target-sst-size", "2147483648", "--compaction", "none",
               "--output", "json", "--latency-sample-every", "10", "--path", str(out / (name + "-db"))]
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TOYKV_", "KV_PERF_")) and k != "LD_PRELOAD"}
    phase_fd = os.memfd_create("v7-v8-phases", 0)
    env["TOYKV_WAL_ATTRIBUTION"] = f"/proc/self/fd/{phase_fd}"
    started = time.time_ns()
    try:
        child = subprocess.Popen(command, env=env, pass_fds=(phase_fd,), stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = child.communicate(timeout=180)
        except BaseException:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.communicate()
            raise
        if child.returncode:
            (out / (name + "-failure.json")).write_text(json.dumps({
                "command": command, "returncode": child.returncode,
                "stdout": stdout.decode(errors="replace"), "stderr": stderr.decode(errors="replace")}, indent=2) + "\n")
            raise RuntimeError(f"{name} failed: {stderr.decode(errors='replace')[-4000:]}")
        record = json.loads(stdout)
        os.lseek(phase_fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(phase_fd), "r") as stream:
            phases = json.load(stream)
    finally:
        os.close(phase_fd)
    r = record["result"]
    pipeline = r["wal_pipeline"]
    if record["params"]["wal_format_version"] != int(fmt[1:]) or r["ops"] != args.num:
        raise RuntimeError("Format or completed operation count mismatch")
    if fmt == "v8":
        digest = r["wal_v8_digest"]
        if digest["payload_hash_bytes"] != args.num * 1050 or digest["post_encode_hash_reread_bytes"] != 0:
            raise RuntimeError("V8 payload digest accounting mismatch")
    mean_sync = pipeline["fdatasync_ns"] / pipeline["sync_count"] / 1e6
    metrics = {"puts_s": r["ops_per_sec"], "p99_ms": record["latency"]["p99_ms"],
               "cpu_s": (r["process_cpu_user_ms"] + r["process_cpu_system_ms"]) / 1000,
               "syncs": pipeline["sync_count"], "mean_sync_ms": mean_sync,
               "reported_commit_groups": pipeline["commit_groups"],
               "reported_solo_groups": pipeline["commit_solo_groups"],
               "worker_notification_requests": pipeline["worker_eventfd_notifications"],
               "eventfd_writes": pipeline["worker_eventfd_writes"],
               "buffers_per_reported_commit_group": pipeline["commit_buffers"] / pipeline["commit_groups"]}
    for field in ("voluntary", "involuntary"):
        metrics[field] = phases["process_after"][field] - phases["process_before"][field]
    metrics["existing_storage_flag"] = metrics["p99_ms"] > 10 or mean_sync > 1
    return {"name": name, "format": fmt, "command": command, "binary_sha256": binary_hash,
            "boot_id": boot_id, "cpu_mask": args.cpu_list, "started_unix_ns": started,
            "finished_unix_ns": time.time_ns(), "metrics": metrics, "record": record, "phases": phases,
            "phase_estimates_inclusive": phase_estimates(phases), "stderr": stderr.decode(errors="replace"),
            "cycles": None, "instructions": None,
            "hardware_counter_note": "Not collected in this phase-clock diagnostic; no IPC claim."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--scratch-file", type=Path,
                        help="Existing initialized disposable 8 MiB file; first 4 MiB overwritten per probe")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--analyze-dir", type=Path, help="Recompute saved summaries without running any probe or benchmark")
    parser.add_argument("--cpus", default="0-15")
    parser.add_argument("--probe-cpu", type=int, default=8)
    parser.add_argument("--num", type=int, default=750000)
    args = parser.parse_args()
    if args.analyze_dir:
        print(json.dumps(analyze_directory(args.analyze_dir), indent=2))
        return
    if not all((args.binary, args.scratch_file, args.output_dir)):
        parser.error("--binary, --scratch-file and --output-dir are required for measurement")
    if args.cpus != "0-15" or args.probe_cpu not in range(16) or not 4096 <= args.num <= 750000:
        parser.error("This diagnostic uses P-core CPUs 0-15, a probe CPU in that mask, and 4096..750000 puts")
    args.cpu_list = list(range(16))
    if not set(args.cpu_list) <= os.sched_getaffinity(0):
        parser.error("Requested CPU mask is unavailable")
    repo = next(p for p in Path(__file__).resolve().parents if (p / "Cargo.toml").is_file())
    out = args.output_dir.resolve()
    reference = args.scratch_file.resolve()
    binary = args.binary.resolve(strict=True)
    if not out.is_relative_to(repo / "target") or out.exists() or not reference.is_relative_to(repo / "target"):
        parser.error("Use a new output directory and an existing scratch file under this repository's target/")
    s = reference.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid() or s.st_size != 8 * 1024 * 1024:
        parser.error("Scratch file must be a regular 8 MiB file owned by the current user")
    fs = subprocess.check_output(["findmnt", "-n", "-o", "FSTYPE,SOURCE,TARGET", "-T", str(reference)], text=True).strip()
    if not fs.startswith("ext4 /dev/nvme0n1p3 ") or reference.stat().st_dev != out.parent.stat().st_dev:
        parser.error("Use the existing ext4 benchmark device for both probes and databases")
    active = []
    for proc in Path("/proc").iterdir():
        if proc.name.isdigit():
            try:
                comm = (proc / "comm").read_text().strip()
                if comm.startswith(("rustc", "cargo", "write-perf")) or comm in ("fio", "perf", "fstrim", "dd"):
                    active.append(comm)
            except OSError:
                pass
    if active:
        parser.error(f"Other benchmark/build jobs are active: {active}")

    def interrupted(signum, _frame):
        raise InterruptedError(f"Signal {signum}; stopping owned benchmark child")

    signal.signal(signal.SIGTERM, interrupted)
    out.mkdir()
    binary_hash = sha256(binary)
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    plan = {"binary": str(binary), "binary_sha256": binary_hash, "boot_id": boot_id,
            "kernel": os.uname().release, "filesystem": fs, "cpu_mask": args.cpu_list,
            "scratch_file": str(reference), "scratch_inode": s.st_ino, "orders": ORDERS,
            "num": args.num, "writers": 16, "batch_entries": 64, "value_bytes": 1024,
            "probe_writes": 1024, "probe_fdatasync_every": 4, "policy": POLICY,
            "script_sha256": sha256(Path(__file__)), "runtime_changed": False, "device_settings_changed": False,
            "qualifies_RFC_performance": False, "diagnostic_instrumentation": True,
            "scope": "Same binary/boot/mask. Shared probes before and after each capture; preserve rejected blocks. CPU scopes are sampled and inclusive. No within-run state or matched completion-pattern claim."}
    (out / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    buf = mmap.mmap(-1, 4096)
    payload = bytes(range(256)) * 16
    buf[:] = payload
    if ctypes.addressof(ctypes.c_char.from_buffer(buf)) % 4096:
        raise RuntimeError("Direct buffer alignment failed")
    fd = os.open(reference, os.O_RDWR | os.O_DIRECT | os.O_NOFOLLOW)
    try:
        if os.fstat(fd).st_ino != s.st_ino or os.preadv(fd, [buf], 0) != 4096 or bytes(buf) != payload:
            raise RuntimeError("Scratch inode or expected contents mismatch")
        for index, order in enumerate(ORDERS):
            block = {"block": index, "runs": [], "probes": []}
            # Capture data and phase JSON stay in RAM until the whole block ends.
            try:
                block["probes"].append(storage_probe(fd, buf, args.probe_cpu))
                for position, fmt in enumerate(order):
                    name = f"block{index}-{position}-{fmt}"
                    print("START", name, flush=True)
                    run = run_capture(binary, out, name, fmt, args, binary_hash, boot_id)
                    block["runs"].append(run)
                    block["probes"].append(storage_probe(fd, buf, args.probe_cpu))
                    print(json.dumps({"name": name, **run["metrics"], "probe_state_after": block["probes"][-1]["storage_state"]}), flush=True)
            finally:
                (out / f"block{index}.json").write_text(json.dumps(block, indent=2) + "\n")
            block_summary = summarize_block(block)
            print(json.dumps({"block": index, **block_summary}), flush=True)
        if os.preadv(fd, [buf], 1023 * 4096) != 4096 or bytes(buf) != payload:
            raise RuntimeError("Final scratch contents mismatch")
    finally:
        os.close(fd)
        buf.close()
    analyze_directory(out)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
