from pathlib import Path
import hashlib
import json
import os
import subprocess
import time


p = Path(__file__).resolve().parent
previous = p.parent / "rfc-026-v8-notification-split-20261102"
bins = {
    "baseline": previous / "write-perf-baseline",
    "kept": previous / "write-perf-cursor",
}
expected_hashes = {
    "baseline": "5387771a5278302e65236fbfac65856c87b7e51bb37be5f0e9e62c05ac19279a",
    "kept": "937a3843a044e107e15db756674be151875e388ff3a0a392cbd4e97463bbd2aa",
}
hashes = {arm: hashlib.sha256(binary.read_bytes()).hexdigest() for arm, binary in bins.items()}
assert hashes == expected_hashes
cases = [("batch", 1), ("point", 1), ("batch", 8), ("point", 8), ("batch", 16), ("point", 16)]
schedule = []
for round_index in range(2):
    ordered_cases = cases if round_index == 0 else list(reversed(cases))
    for case_index, (kind, writers) in enumerate(ordered_cases):
        arms = ["baseline", "kept"] if (case_index + round_index) % 2 == 0 else ["kept", "baseline"]
        for arm in arms:
            schedule.append([kind, writers, arm])
plan = {
    "base_revision": "fdde06706ffc96c458c50fb87a431c13ec76d4d7",
    "schedule": schedule,
    "binary_sha256": hashes,
    "binary_paths": {arm: str(binary) for arm, binary in bins.items()},
    "selection": "Highest eligible throughput per workload/writer/arm; p99, CPU and counters from the same sample.",
    "stall_filter": {"p99_ms_greater_than": 10, "mean_fdatasync_ms_greater_than": 1},
    "purpose": "Fresh unchanged-code rerun after the maintainer explicitly requested keeping the combined notification and cursor changes. Keep code regardless of performance outcome; no push or merge.",
    "initial_cooldown_seconds": 45,
    "between_runs_seconds": 3,
    "operations": {"batch": 750000, "point_1": 5000, "point_8_16": 20000},
    "value_size": 1024,
    "wal_batch_size": 64,
    "latency_sample_every": 10,
    "detailed_wal_profile": False,
    "publication_cpu_probe": False,
    "context_probe": "Batch-only before/after timed interval; absent for point writes.",
}
plan_path = p / "plan.json"
if plan_path.exists():
    assert json.loads(plan_path.read_text()) == plan
    print("RESUME unchanged plan; preserve completed results", flush=True)
else:
    plan_path.write_text(json.dumps(plan, indent=2) + "\n")
    print("COOLDOWN 45 seconds", flush=True)
    time.sleep(45)

for sample_index, (kind, writers, arm) in enumerate(schedule):
    name = f"rerun-{sample_index:02}-{kind}-{arm}-{writers}"
    result_path = p / (name + ".json")
    status_path = p / (name + "-status.json")
    if status_path.exists():
        status = json.loads(status_path.read_text())
        if status["returncode"] == 0:
            assert json.loads(result_path.read_text())["schema"] == "kv-engine.write-perf.v1"
            print("PRESERVED", name, flush=True)
            continue
    env = dict(os.environ, TOYKV_WAL_CONTEXT=str(p / (name + "-context.json")))
    cmd = [
        str(bins[arm]),
        "--bench", "wal_batch_concurrent" if kind == "batch" else "wal_concurrent",
        "--pitr", "--pitr-format", "v8", "--wal-io-mode", "parallel",
        "--num", str(750000 if kind == "batch" else (5000 if writers == 1 else 20000)),
        "--threads", str(writers), "--value-size", "1024",
        "--target-sst-size", "2147483648", "--compaction", "none",
        "--output", "json", "--latency-sample-every", "10",
        "--path", str(p / (name + "-db")),
    ]
    if kind == "batch":
        cmd += ["--wal-batch-size", "64"]
    print("START", name, flush=True)
    started_at = time.time()
    with result_path.open("w") as out, (p / (name + ".stderr")).open("w") as err:
        try:
            completed = subprocess.run(cmd, stdout=out, stderr=err, env=env, timeout=180)
            returncode = completed.returncode
        except subprocess.TimeoutExpired:
            returncode = 124
    status_path.write_text(json.dumps({
        "name": name, "command": cmd, "returncode": returncode,
        "started_at_unix": started_at, "finished_at_unix": time.time(),
    }, indent=2) + "\n")
    if returncode:
        print("FAILED", name, returncode, (p / (name + ".stderr")).read_text()[-1000:], flush=True)
        time.sleep(3)
        continue
    d = json.loads(result_path.read_text())
    assert d["schema"] == "kv-engine.write-perf.v1"
    q, latency = d["result"], d["latency"]
    w = q["wal_pipeline"]
    mean_sync_ms = w["fdatasync_ns"] / w["sync_count"] / 1e6
    context_path = p / (name + "-context.json")
    context = json.loads(context_path.read_text()) if context_path.exists() else {}
    print(json.dumps({
        "name": name,
        "puts_s": round(q["ops_per_sec"]), "p99_ms": round(latency["p99_ms"], 3),
        "cpu_s": round((q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000, 3),
        "voluntary": context.get("voluntary"), "involuntary": context.get("involuntary"),
        "eventfd_writes": w["worker_eventfd_writes"],
        "syncs": w["sync_count"], "mean_sync_ms": round(mean_sync_ms, 3),
        "stall_flag": latency["p99_ms"] > 10 or mean_sync_ms > 1,
    }), flush=True)
    time.sleep(3)

print("DONE", flush=True)
