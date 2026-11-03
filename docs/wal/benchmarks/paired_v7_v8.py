"""Run bounded V7/V8 diagnostic pairs with independent storage probes.

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
PAIR_PROTOCOL = "kv-engine.wal-paired-diagnostic.v2"
PAIR_SCHEDULE = (
    {"target_state": "fast", "order": ["v7", "v8"]},
    {"target_state": "fast", "order": ["v8", "v7"]},
    {"target_state": "slow", "order": ["v7", "v8"]},
    {"target_state": "slow", "order": ["v8", "v7"]},
)
ADMISSION_MAX_PROBES = 6
RATIO_FIELDS = ("puts_s", "p99_ms", "cpu_s", "syncs", "voluntary")


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


def checked_probe(probe, policy=POLICY):
    """Reject inconsistent saved summaries instead of trusting state labels."""
    writes, syncs = probe["write_ns"], probe["sync_ns"]
    if len(writes) != 1024 or len(syncs) != 256 or any(
        type(value) is not int or value <= 0 for value in [*writes, *syncs]
    ):
        raise ValueError("Invalid raw probe samples")
    expected = {"write": stats(writes), "first_after_sync": stats(writes[::4]), "sync": stats(syncs),
                "storage_state": probe_state(probe, policy)}
    if any(probe[key] != value for key, value in expected.items()):
        raise ValueError("Probe summaries differ from raw samples")
    return probe


def admission_ready(probes, target, policy=POLICY):
    if len(probes) < 2:
        return False
    a, b = (checked_probe(p, policy) for p in probes[-2:])
    return a["storage_state"] == b["storage_state"] == target and not compare_probes(a, b, policy)


def acquire_stable_probes(probe_fn, target, limit=ADMISSION_MAX_PROBES, policy=POLICY, probes=None):
    probes = [] if probes is None else probes
    for _ in range(limit):
        probes.append(probe_fn())
        if admission_ready(probes, target, policy):
            return probes, True
    return probes, False


def run_pair(spec, probe_fn, capture_fn, limit=ADMISSION_MAX_PROBES, policy=POLICY, result=None):
    result = {} if result is None else result
    result.update({"target_state": spec["target_state"], "order": spec["order"],
                   "admission_probes": [], "admitted": False, "runs": [], "probes": []})
    probes, admitted = acquire_stable_probes(probe_fn, spec["target_state"], limit, policy,
                                           probes=result["admission_probes"])
    result.update({"admission_probes": probes, "admitted": admitted})
    if not admitted:
        return result
    result["probes"].append(probes[-1])
    for position, fmt in enumerate(spec["order"]):
        result["runs"].append(capture_fn(position, fmt))
        result["probes"].append(probe_fn())
    return result


def pair_identity_reasons(runs, plan, spec):
    reasons = []
    if [r.get("format") for r in runs] != spec["order"]:
        reasons.append("order_mismatch")
    for field, expected in (("binary_sha256", plan["binary_sha256"]), ("boot_id", plan["boot_id"]),
                            ("cpu_mask", plan["cpu_mask"])):
        if any(run.get(field) != expected for run in runs):
            reasons.append(field + "_mismatch")
    expected_params = {"num": plan["num"], "threads": plan["writers"], "batch_size": plan["batch_entries"],
                       "value_size": plan["value_bytes"], "wal_io_mode": "parallel",
                       "seed": 42, "latency_sample_every": 10}
    expected_options = {"wal": True, "compaction": "none", "target_sst_size": 2147483648}
    for index, run in enumerate(runs):
        try:
            record, phases = run["record"], run["phases"]
            if any(record["params"].get(key) != value for key, value in expected_params.items()) or any(
                record["engine_options"].get(key) != value for key, value in expected_options.items()
            ) or record["params"]["wal_format_version"] != int(run["format"][1:]):
                reasons.append(f"run_{index}:workload_mismatch")
            raw = record["result"]
            expected_metrics = {"puts_s": raw["ops_per_sec"], "p99_ms": record["latency"]["p99_ms"],
                                "cpu_s": (raw["process_cpu_user_ms"] + raw["process_cpu_system_ms"]) / 1000,
                                "syncs": raw["wal_pipeline"]["sync_count"],
                                "voluntary": phases["process_after"]["voluntary"] - phases["process_before"]["voluntary"]}
            if raw["ops"] != plan["num"] or any(
                not math.isfinite(value) or value <= 0 or run["metrics"][key] != value
                for key, value in expected_metrics.items()
            ):
                reasons.append(f"run_{index}:invalid_metrics")
            expected_flag = expected_metrics["p99_ms"] > 10 or (
                raw["wal_pipeline"]["fdatasync_ns"] / raw["wal_pipeline"]["sync_count"] / 1e6 > 1)
            if run["metrics"].get("existing_storage_flag") != expected_flag:
                reasons.append(f"run_{index}:invalid_storage_flag")
            if run["format"] == "v8":
                digest = raw["wal_v8_digest"]
                if digest["payload_hash_bytes"] != plan["num"] * 1050 or digest["post_encode_hash_reread_bytes"] != 0:
                    reasons.append(f"run_{index}:digest_accounting_mismatch")
        except (KeyError, TypeError, ValueError, OverflowError):
            reasons.append(f"run_{index}:invalid_capture")
    return reasons


def summarize_pair(pair, plan, spec):
    policy = plan["policy"]
    reasons = ["capture_error"] if pair.get("error") else []
    admission = pair.get("admission_probes", [])
    admitted = False
    try:
        for probe in admission:
            checked_probe(probe, policy)
        if len(admission) > plan["admission_max_probes"]:
            reasons.append("admission_probe_limit_exceeded")
        admitted = admission_ready(admission, spec["target_state"], policy)
        if any(admission_ready(admission[:end], spec["target_state"], policy)
               for end in range(2, len(admission))):
            reasons.append("admission_continued_after_success")
    except (KeyError, TypeError, ValueError):
        reasons.append("invalid_admission_probe")
    if not admitted or pair.get("admitted") is not True:
        reasons.append("stable_state_not_admitted")
    if pair.get("target_state") != spec["target_state"] or pair.get("order") != spec["order"]:
        reasons.append("schedule_mismatch")
    probes, runs = pair.get("probes", []), pair.get("runs", [])
    if len(probes) != 3 or len(runs) != 2:
        reasons.append("incomplete_pair")
    if admitted and probes and probes[0] != admission[-1]:
        reasons.append("pair_did_not_start_at_admission_boundary")
    valid_probes = []
    for index, probe in enumerate(probes):
        try:
            valid_probes.append(checked_probe(probe, policy))
        except (KeyError, TypeError, ValueError):
            reasons.append(f"probe_{index}:invalid_raw_probe")
    for index, probe in enumerate(valid_probes):
        if probe["storage_state"] != spec["target_state"]:
            reasons.append(f"probe_{index}:wrong_or_transitioning_state")
        for other in range(index):
            reasons += [f"probes_{other}_{index}:" + why
                        for why in compare_probes(valid_probes[other], probe, policy)]
    reasons += pair_identity_reasons(runs, plan, spec)
    ratios = None
    if not reasons:
        by_format = {run["format"]: run["metrics"] for run in runs}
        ratios = {key: by_format["v8"][key] / by_format["v7"][key] for key in RATIO_FIELDS}
    return {"target_state": spec["target_state"], "order": spec["order"],
            "status": "accepted" if not reasons else ("rejected" if admitted else "not_admitted"),
            "probe_compatible_diagnostic_pair": not reasons, "rejection_reasons": reasons,
            "v8_over_v7": ratios,
            "both_runs_pass_existing_storage_filter": len(runs) == 2 and all(
                not r.get("metrics", {}).get("existing_storage_flag", True) for r in runs),
            "RFC_performance_qualified": False}


def summarize_pairs(pairs):
    states = {}
    for state in ("fast", "slow"):
        accepted = [p for p in pairs if p["target_state"] == state and p["probe_compatible_diagnostic_pair"]]
        orders = {tuple(p["order"]) for p in accepted}
        both = orders == {("v7", "v8"), ("v8", "v7")}
        states[state] = {"accepted_pairs": len(accepted), "both_orders_observed": both,
                         "geomean_v8_over_v7": {
                             key: math.exp(sum(math.log(p["v8_over_v7"][key]) for p in accepted) / len(accepted))
                             for key in RATIO_FIELDS
                         } if both else None}
    return states


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
    if plan.get("protocol") == PAIR_PROTOCOL:
        raw_pairs = [json.loads((out / f"pair{i}.json").read_text()) if (out / f"pair{i}.json").is_file()
                     else {} for i in range(len(plan["pair_schedule"]))]
        pairs = [summarize_pair(pair, plan, spec) for pair, spec in zip(raw_pairs, plan["pair_schedule"], strict=True)]
        summary = {"protocol": PAIR_PROTOCOL, "pairs": pairs, "by_storage_state": summarize_pairs(pairs),
                   "compatible_pairs": sum(p["probe_compatible_diagnostic_pair"] for p in pairs),
                   "all_scheduled_attempts_present": all(bool(pair) for pair in raw_pairs),
                   "RFC_performance_qualified": False, "runtime_changes": False}
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        return summary
    if plan.get("protocol") is not None:
        raise ValueError("Unsupported benchmark protocol; do not reinterpret records")
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
    plan = {"protocol": PAIR_PROTOCOL, "pair_schedule": PAIR_SCHEDULE,
            "admission_max_probes": ADMISSION_MAX_PROBES, "admission_consecutive_probes": 2,
            "attempts_per_scheduled_pair": 1,
            "binary": str(binary), "binary_sha256": binary_hash, "boot_id": boot_id,
            "kernel": os.uname().release, "filesystem": fs, "cpu_mask": args.cpu_list,
            "scratch_file": str(reference), "scratch_inode": s.st_ino,
            "num": args.num, "writers": 16, "batch_entries": 64, "value_bytes": 1024,
            "probe_writes": 1024, "probe_fdatasync_every": 4, "policy": POLICY,
            "script_sha256": sha256(Path(__file__)), "runtime_changed": False, "device_settings_changed": False,
            "qualifies_RFC_performance": False, "diagnostic_instrumentation": True,
            "scope": "Prespecified pair-level gate and bounded stable-state admission; no retrospective reuse of v1 data. Three measurement probes must agree pairwise. Preserve skipped/rejected attempts and separate fast/slow ratios. Diagnostic only, no within-run state or identical completion-pattern claim."}
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
        for index, spec in enumerate(PAIR_SCHEDULE):
            pair = {"pair_index": index}

            def probe():
                value = storage_probe(fd, buf, args.probe_cpu)
                print(json.dumps({"pair": index, "probe_state": value["storage_state"],
                                  "post_sync_p50_us": value["first_after_sync"]["p50_us"],
                                  "post_sync_p99_us": value["first_after_sync"]["p99_us"]}), flush=True)
                return value

            def capture(position, fmt):
                name = f"pair{index}-{spec['target_state']}-{position}-{fmt}"
                print("START", name, flush=True)
                value = run_capture(binary, out, name, fmt, args, binary_hash, boot_id)
                print(json.dumps({"name": name, **value["metrics"]}), flush=True)
                return value

            # Captures and phase JSON stay in RAM until this attempt ends.
            try:
                run_pair(spec, probe, capture, result=pair)
            except BaseException as error:
                pair["error"] = str(error)
                raise
            finally:
                (out / f"pair{index}.json").write_text(json.dumps(pair, indent=2) + "\n")
            print(json.dumps({"pair": index, **summarize_pair(pair, plan, spec)}), flush=True)
        if os.preadv(fd, [buf], 1023 * 4096) != 4096 or bytes(buf) != payload:
            raise RuntimeError("Final scratch contents mismatch")
    finally:
        os.close(fd)
        buf.close()
    analyze_directory(out)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
