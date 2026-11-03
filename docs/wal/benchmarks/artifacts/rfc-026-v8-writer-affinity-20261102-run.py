from pathlib import Path
import hashlib
import json
import os
import signal
import subprocess
import sys
import time


p = Path(__file__).resolve().parent
binary = p / "write-perf-writer-affinity"
expected_hash = "b422f940a95fef7fa81da96d85e3787e8909e8226fe6d5e62a720fd0d8e1e81d"


def cpu_list(value):
    result = set()
    for part in value.split(","):
        lo, _, hi = part.partition("-")
        result.update(range(int(lo), int(hi or lo) + 1))
    return result


def validate_roles(record, mode, writers=16, fmt="v8"):
    expected_writer = set(range(32 if mode == "default" else 16))
    expected_background = set(range(16 if mode == "whole" else 32))
    entries = record["writers"]
    assert len(entries) == writers
    assert {x["writer_index"] for x in entries} == set(range(writers))
    writer_tids = {x["tid"] for x in entries}
    assert len(writer_tids) == writers
    assert all(set(x["allowed_cpus"]) == expected_writer for x in entries)
    assert record["pin_writers"] == (mode == "writers")
    for key in ["background_before", "background_after"]:
        background = [x for x in record[key] if x["tid"] not in writer_tids]
        assert background
        assert all(cpu_list(x["allowed_cpus"]) == expected_background for x in background), (mode, key, background)
        assert any(x["name"].startswith("wal-io-worker") for x in background), (key, background)
        if fmt == "v8":
            assert any(x["name"].startswith("wal-v8-compl") for x in background), (key, background)
    assert all(x >= 0 for x in record["context"].values())


def run_capture(name, fmt, mode, num):
    env = dict(os.environ)
    for key in ["TOYKV_WAL_ATTRIBUTION", "TOYKV_PREPARATION_MICRO", "TOYKV_WAL_CONTEXT", "KV_PERF_CONTROL_DIR", "TOYKV_AFFINITY_REPORT", "TOYKV_WRITER_PCORE"]:
        env.pop(key, None)
    env["TOYKV_AFFINITY_REPORT"] = str(p / (name + "-affinity.json"))
    env["TOYKV_WRITER_PCORE"] = "1" if mode == "writers" else "0"
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
    status = {"name": name, "format": fmt, "mode": mode, "command": cmd, "num": num,
              "returncode": code, "started_at_unix": started, "finished_at_unix": time.time()}
    (p / (name + "-status.json")).write_text(json.dumps(status, indent=2) + "\n")
    if code:
        print("FAILED", name, code, (p / (name + ".stderr")).read_text()[-1200:], flush=True)
        raise SystemExit(code)
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
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == expected_hash
    assert sorted(os.sched_getaffinity(0)) == list(range(32))
    assert Path("/sys/devices/cpu_core/cpus").read_text().strip() == "0-15"
    assert Path("/sys/devices/cpu_atom/cpus").read_text().strip() == "16-31"
    if "--smoke" in sys.argv:
        schedule = [(fmt, mode) for fmt in ["v7", "v8"] for mode in ["default", "writers", "whole"]]
        for index, (fmt, mode) in enumerate(schedule):
            run_capture(f"smoke-{index:02}-{fmt}-{mode}", fmt, mode, 4096)
        (p / "smoke-checks.json").write_text(json.dumps({"captures": 6, "all_commands_successful": True,
                                                      "all_writer_and_background_affinity_masks_validated": True,
                                                      "normal_performance_scores": False}, indent=2) + "\n")
        print("SMOKE DONE", flush=True)
        return
    assert (p / "smoke-checks.json").exists()
    orders = [
        ["default", "writers", "whole"], ["whole", "writers", "default"],
        ["writers", "whole", "default"], ["default", "whole", "writers"],
        ["whole", "default", "writers"], ["writers", "default", "whole"],
    ]
    schedule = [{"round": r, "position": i, "mode": mode, "format": "v8"}
                for r, order in enumerate(orders) for i, mode in enumerate(order)]
    plan = {
        "base_revision": "da7d4d6778dcc3d251a9796cbb80ea09dbc5a0ee",
        "binary": str(binary), "binary_sha256": expected_hash,
        "schedule": schedule,
        "modes": {"default": "All userspace threads inherit CPUs 0-31.",
                  "writers": "Benchmark writers set their own mask to 0-15 once at thread entry; main/background threads retain 0-31.",
                  "whole": "taskset -c 0-15 on the process; all userspace threads inherit 0-15."},
        "workload": {"puts": 750000, "writers": 16, "batch": 64, "value_bytes": 1024,
                     "format": "v8", "wal_io_mode": "parallel", "target_sst_size": 2147483648,
                     "compaction": "none", "latency_sample_every": 10},
        "cooldown_before_each_round_seconds": 45, "between_runs_seconds": 3,
        "selection": "Highest eligible throughput per mode with all same-run CPU, p99, hash and context counters; also preserve eligible medians, cohort membership and every raw run. Fixed schedule, no tuning between runs.",
        "storage_flag": "p99_ms >10 OR mean fdatasync_ms >1; identical for every mode, all flagged records retained and excluded from scores.",
        "measurement": "One normal-counter executable; no --profile, perf, new phase clocks, or per-batch affinity calls. Existing V8 digest CPU sampling at 1/64 tickets. Timed getrusage contexts; thread masks audited before/after the interval and once per writer at entry.",
        "audit_cost": "Thread affinity calls, one affinity query and returned placement metadata per writer occur inside the existing thread-launch timed interval in all modes; only writer-mode adds setaffinity. /proc snapshots and audit JSON serialization are outside timed CPU/wall metrics.",
        "role_limit": "V8 executes some submission, prefix folding, frontier-driver and sync work on foreground writers; writer-only affinity includes those responsibilities. Background mask is an allowed set, not proof of actual P/E execution residency.",
        "runtime_changed": False, "benchmark_hook_retained": False,
        "historical_scores": "Earlier binaries and cohorts remain context only; this cohort makes the three-way affinity comparison with one new executable.",
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
