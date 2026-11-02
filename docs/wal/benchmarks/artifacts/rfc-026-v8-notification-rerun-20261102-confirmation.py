from pathlib import Path
import hashlib
import json
import os
import subprocess
import time


p = Path(__file__).resolve().parent
assert "DONE" in (p / "run.log").read_text().splitlines()
original_plan = json.loads((p / "plan.json").read_text())
bins = {arm: Path(path) for arm, path in original_plan["binary_paths"].items()}
hashes = {arm: hashlib.sha256(binary.read_bytes()).hexdigest() for arm, binary in bins.items()}
assert hashes == original_plan["binary_sha256"]
cases = [("batch", 8), ("point", 8), ("batch", 16)]
schedule = []
for round_index in range(2):
    for kind, writers in cases if round_index == 0 else list(reversed(cases)):
        arms = ["baseline", "kept"] if round_index == 0 else ["kept", "baseline"]
        for arm in arms:
            schedule.append([kind, writers, arm])
plan = {
    "schedule": schedule,
    "binary_sha256": hashes,
    "binary_paths": original_plan["binary_paths"],
    "purpose": "One bounded follow-up for missing eligible concurrent arms in the first rerun; no code, parameters or filter changes. Reverse both case and arm order. Preserve first cohort separately. Keep implementation regardless of result.",
    "initial_cooldown_seconds": 45, "between_runs_seconds": 3,
    "selection": original_plan["selection"], "stall_filter": original_plan["stall_filter"],
    "operations": original_plan["operations"],
}
plan_path = p / "confirmation-plan.json"
assert not plan_path.exists(), "Do not overwrite a completed confirmation cohort"
plan_path.write_text(json.dumps(plan, indent=2) + "\n")
print("COOLDOWN 45 seconds", flush=True)
time.sleep(45)
for sample_index, (kind, writers, arm) in enumerate(schedule):
    name = f"confirmation-{sample_index:02}-{kind}-{arm}-{writers}"
    result_path = p / (name + ".json")
    assert not result_path.exists(), name
    env = dict(os.environ, TOYKV_WAL_CONTEXT=str(p / (name + "-context.json")))
    cmd = [
        str(bins[arm]), "--bench", "wal_batch_concurrent" if kind == "batch" else "wal_concurrent",
        "--pitr", "--pitr-format", "v8", "--wal-io-mode", "parallel",
        "--num", str(750000 if kind == "batch" else 20000),
        "--threads", str(writers), "--value-size", "1024", "--target-sst-size", "2147483648",
        "--compaction", "none", "--output", "json", "--latency-sample-every", "10",
        "--path", str(p / (name + "-db")),
    ]
    if kind == "batch":
        cmd += ["--wal-batch-size", "64"]
    print("START", name, flush=True)
    started_at = time.time()
    with result_path.open("w") as out, (p / (name + ".stderr")).open("w") as err:
        try:
            returncode = subprocess.run(cmd, stdout=out, stderr=err, env=env, timeout=180).returncode
        except subprocess.TimeoutExpired:
            returncode = 124
    (p / (name + "-status.json")).write_text(json.dumps({
        "name": name, "command": cmd, "returncode": returncode,
        "started_at_unix": started_at, "finished_at_unix": time.time(),
    }, indent=2) + "\n")
    if returncode:
        print("FAILED", name, returncode, flush=True)
        time.sleep(3)
        continue
    d = json.loads(result_path.read_text())
    q, latency = d["result"], d["latency"]
    w = q["wal_pipeline"]
    mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
    context_path = p / (name + "-context.json")
    context = json.loads(context_path.read_text()) if context_path.exists() else {}
    print(json.dumps({
        "name": name, "puts_s": round(q["ops_per_sec"]), "p99_ms": round(latency["p99_ms"], 3),
        "cpu_s": round((q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000, 3),
        "voluntary": context.get("voluntary"), "eventfd_writes": w["worker_eventfd_writes"],
        "syncs": w["sync_count"], "mean_sync_ms": round(mean, 3),
        "stall_flag": latency["p99_ms"] > 10 or mean > 1,
    }), flush=True)
    time.sleep(3)
print("DONE", flush=True)
