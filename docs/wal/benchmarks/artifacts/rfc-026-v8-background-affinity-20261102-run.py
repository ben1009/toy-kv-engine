from pathlib import Path
import hashlib
import json
import os
import signal
import subprocess
import sys
import time


p = Path(__file__).resolve().parent
binary = p / "write-perf-background-affinity"
modes = ["writers", "kernel_only", "io", "reaper", "userspace", "whole"]


def cpu_list(value):
    result = set()
    for part in value.split(","):
        lo, _, hi = part.partition("-")
        result.update(range(int(lo), int(hi or lo) + 1))
    return result


def expected_background(task, mode, pid):
    if mode == "whole":
        return set(range(16))
    kernel = task["name"].startswith("iou-wrk-")
    if kernel:
        return set(range(32 if mode == "writers" else 16))
    if task["tid"] == pid:
        return set(range(32))
    selected = (
        mode in ["userspace", "kernel"]
        or (mode in ["io", "reaper"] and task["name"] == "wal-io-worker")
        or (mode == "reaper" and task["name"].startswith("wal-v8-compl"))
    )
    return set(range(16 if selected else 32))


def validate_roles(record, mode, writers=16, fmt="v8"):
    assert record["mode"] == ("writers" if mode == "kernel_only" else mode)
    entries = record["writers"]
    assert len(entries) == writers
    assert {x["writer_index"] for x in entries} == set(range(writers))
    writer_tids = {x["tid"] for x in entries}
    assert len(writer_tids) == writers
    assert all(set(x["allowed_cpus"]) == set(range(16)) for x in entries)
    assert record["pin_writers"] == (mode != "whole")
    for key in ["background_before", "background_after"]:
        background = [x for x in record[key] if x["tid"] not in writer_tids]
        assert background
        for task in background:
            assert cpu_list(task["allowed_cpus"]) == expected_background(task, mode, record["pid"]), (mode, key, task)
        assert any(x["name"] == "wal-io-worker" for x in background), (key, background)
        if fmt == "v8":
            assert any(x["name"].startswith("wal-v8-compl") for x in background), (key, background)
    assert any(x["name"].startswith("iou-wrk-") for x in record["background_after"]), record
    for task in record["background_original"]:
        expected = set(range(16 if mode == "whole" or (mode != "writers" and task["name"].startswith("iou-wrk-")) else 32))
        assert cpu_list(task["allowed_cpus"]) == expected, (mode, task)
    assert all(x >= 0 for x in record["context"].values())


def run_capture(name, fmt, mode, num, register_kernel=True):
    env = dict(os.environ)
    for key in ["TOYKV_WAL_ATTRIBUTION", "TOYKV_PREPARATION_MICRO", "TOYKV_WAL_CONTEXT", "KV_PERF_CONTROL_DIR", "TOYKV_AFFINITY_REPORT", "TOYKV_WRITER_PCORE", "TOYKV_AFFINITY_MODE", "TOYKV_IOWQ_PCORE"]:
        env.pop(key, None)
    env["TOYKV_AFFINITY_REPORT"] = str(p / (name + "-affinity.json"))
    env["TOYKV_AFFINITY_MODE"] = "writers" if mode == "kernel_only" else mode
    env["TOYKV_IOWQ_PCORE"] = "0" if mode == "writers" else "1"
    if not register_kernel:
        env.pop("TOYKV_IOWQ_PCORE")
    cmd = [str(binary), "--bench", "wal_batch_concurrent", "--wal-batch-size", "64",
           "--pitr", "--pitr-format", fmt, "--wal-io-mode", "parallel",
           "--num", str(num), "--threads", "16", "--value-size", "1024",
           "--target-sst-size", "2147483648", "--compaction", "none",
           "--output", "json", "--latency-sample-every", "10",
           "--path", str(p / (name + "-db"))]
    if mode == "whole":
        cmd = ["taskset", "-c", "0-15"] + cmd
    print("START", name, flush=True)
    started = time.time()
    with (p / (name + ".json")).open("w") as out, (p / (name + ".stderr")).open("w") as err:
        child = subprocess.Popen(cmd, stdout=out, stderr=err, env=env, start_new_session=True)
        try:
            code = child.wait(timeout=180)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            code = 124
    stderr = (p / (name + ".stderr")).read_text()
    registrations = [line for line in stderr.splitlines() if line.startswith("TOYKV_IOWQ_AFFINITY ")]
    status = {"name": name, "format": fmt, "mode": mode, "command": cmd, "num": num,
              "returncode": code, "started_at_unix": started, "finished_at_unix": time.time(),
              "iowq_affinity_registrations": registrations,
              "kernel_registration_requested": register_kernel}
    (p / (name + "-status.json")).write_text(json.dumps(status, indent=2) + "\n")
    if code:
        print("FAILED", name, code, stderr[-1200:], flush=True)
        raise SystemExit(code)
    assert bool(registrations) == register_kernel, (mode, registrations)
    kernel_mask = "0-31" if mode == "writers" else "0-15"
    assert all(f"registered=1 cpus={kernel_mask} " in line for line in registrations), (mode, registrations)
    data = json.loads((p / (name + ".json")).read_text())
    placement = json.loads((p / (name + "-affinity.json")).read_text())
    validate_roles(placement, mode, fmt=fmt)
    q, latency = data["result"], data["latency"]
    assert q["ops"] == num
    w = q["wal_pipeline"]
    mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
    if fmt == "v8":
        assert q["wal_v8_digest"]["payload_hash_bytes"] == num * 1050
        assert q["wal_v8_digest"]["post_encode_hash_reread_bytes"] == 0
    print(json.dumps({"name": name, "puts_s": round(q["ops_per_sec"]),
                      "p99_ms": round(latency["p99_ms"], 3),
                      "cpu_s": round((q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000, 3),
                      "syncs": w["sync_count"], "mean_sync_ms": round(mean, 3),
                      "stall_flag": latency["p99_ms"] > 10 or mean > 1,
                      "role_masks_validated": True, "context": placement["context"]}), flush=True)


def main():
    expected_hash = json.loads((p / "build.json").read_text())["binary_sha256"]
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == expected_hash
    assert sorted(os.sched_getaffinity(0)) == list(range(32))
    assert Path("/sys/devices/cpu_core/cpus").read_text().strip() == "0-15"
    assert Path("/sys/devices/cpu_atom/cpus").read_text().strip() == "16-31"
    if "--smoke" in sys.argv:
        schedule = [("v8", mode) for mode in modes] + [("v7", mode) for mode in ["writers", "kernel_only"]]
        for index, (fmt, mode) in enumerate(schedule):
            run_capture(f"smoke-{index:02}-{fmt}-{mode}", fmt, mode, 4096)
        (p / "smoke-checks.json").write_text(json.dumps({"captures": len(schedule), "all_commands_successful": True,
                                                      "all_affinity_masks_validated": True,
                                                      "kernel_registration_verified": True,
                                                      "normal_performance_scores": False}, indent=2) + "\n")
        print("SMOKE DONE", flush=True)
        return
    assert (p / "smoke-checks.json").exists()
    orders = [modes, list(reversed(modes)),
              ["reaper", "whole", "io", "kernel_only", "writers", "userspace"],
              ["userspace", "writers", "kernel_only", "io", "whole", "reaper"]]
    schedule = [{"round": r, "position": i, "mode": mode, "format": "v8"}
                for r, order in enumerate(orders) for i, mode in enumerate(order)]
    plan = {
        "base_revision": "604b870e801d2ef3e5e6f6cf5275328e2107cf49",
        "binary": str(binary), "binary_sha256": expected_hash,
        "schedule": schedule,
        "modes": {"writers": "Writers P-core; all other userspace and io-wq masks 0-31.",
                  "kernel_only": "Writers plus explicitly registered io-wq P-core; all userspace background threads remain 0-31. Harness affinity mode is writers; kernel registration is the only change.",
                  "io": "Kernel-only plus wal-io-worker P-core; other userspace background roles 0-31.",
                  "reaper": "IO plus V8 completion reaper P-core.",
                  "userspace": "Reaper plus all remaining userspace background threads (engine runtime and Tokio); main stays 0-31. V8 has no additional dedicated userspace WAL role.",
                  "whole": "Whole-process taskset 0-15, including main; same explicit io-wq P-core registration as all non-control arms."},
        "workload": {"puts": 750000, "writers": 16, "batch": 64, "value_bytes": 1024,
                     "format": "v8", "wal_io_mode": "parallel", "target_sst_size": 2147483648,
                     "compaction": "none", "latency_sample_every": 10},
        "cooldown_before_each_round_seconds": 45, "between_runs_seconds": 3,
        "selection": "Highest eligible throughput per mode with all same-run CPU, p99, hash and context counters; preserve medians and every raw run. Four fixed rounds, two order/reverse pairs, no tuning between runs.",
        "storage_flag": "p99_ms >10 OR mean fdatasync_ms >1, unchanged across modes; all flagged records retained and excluded from scores.",
        "measurement": "One normal-counter executable; no --profile, perf, phase clocks, or per-batch affinity changes. Existing 1/64 digest CPU samples. Background affinity setup and /proc snapshots before timer; writer set/get at thread entry inside existing launch interval. Context delta and final thread audit outside timed CPU/wall metrics.",
        "kernel_hook": "Startup-only bench-feature hook uses io-uring 0.6.4 register_iowq_aff in EVERY mode: 0-31 for writers, 0-15 for all other modes. Both preliminary smoke attempts showed that pinning the I/O worker also narrowed new kernel workers, even after full-mask registration. Failed preflights are preserved separately and never scored. Therefore kernel-only is tested BEFORE cumulative userspace roles, holding actual kernel masks constant for IO/reaper/shared-background comparisons. Success logged and actual masks audited. Source changes removed after building.",
        "role_limit": "Affinity means allowed CPUs, not actual residency. Whole-process changes core count, frequency/SMT/cache and pipeline timing. Foreground writers include publication and some DATA/coordinator/frontier work; names identify dedicated background roles.",
        "runtime_changed": False, "benchmark_hook_retained": False,
        "historical_scores": "Context only; all six scored modes use this same executable and cohort.",
    }
    assert not (p / "plan.json").exists()
    (p / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    for index, item in enumerate(schedule):
        if item["position"] == 0:
            print("COOLDOWN 45s", "round", item["round"], flush=True)
            time.sleep(45)
        run_capture(f"{index:02}-v8-{item['mode']}", "v8", item["mode"], 750000)
        time.sleep(3)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
