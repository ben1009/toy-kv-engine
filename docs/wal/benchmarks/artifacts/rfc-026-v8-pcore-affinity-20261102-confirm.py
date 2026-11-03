from pathlib import Path
import hashlib
import json
import os
import signal
import subprocess
import time


p = Path(__file__).resolve().parent
binary = p.parent / "rfc-026-v8-notification-split-20261102/write-perf-cursor"
original_plan = json.loads((p / "plan.json").read_text())
assert hashlib.sha256(binary.read_bytes()).hexdigest() == original_plan["binary_sha256"]["normal"]
assert sorted(os.sched_getaffinity(0)) == list(range(32))
assert "DONE" in (p / "run.log").read_text().splitlines()
orders = [
    [("v7", "hybrid"), ("v7", "pcore"), ("v8", "pcore"), ("v8", "hybrid")],
    [("v8", "hybrid"), ("v8", "pcore"), ("v7", "pcore"), ("v7", "hybrid")],
]
schedule = [{"mode": "normal", "round": r, "position": i, "format": fmt, "affinity": affinity}
            for r, order in enumerate(orders) for i, (fmt, affinity) in enumerate(order)]
plan = {
    **original_plan,
    "reason": "Initial normal cohort had zero eligible V7 controls in both affinity cells (13/16 total records flagged). One additional fixed balanced normal-only cohort after a longer idle; no source, binary, workload, filter or policy change.",
    "schedule": schedule,
    "initial_cooldown_seconds": 120,
    "cooldown_before_each_round_seconds": 120,
    "between_runs_seconds": 10,
    "selection": "Preserve separate cohort labels. Pool highest eligible normal score per cell for same-binary best comparison; also report initial/confirmation counts and all raw runs. No further score-dependent retries.",
}
assert not (p / "confirmation-plan.json").exists()
(p / "confirmation-plan.json").write_text(json.dumps(plan, indent=2) + "\n")
for index, item in enumerate(schedule):
    fmt, affinity = item["format"], item["affinity"]
    name = f"confirm-{index:02}-normal-{fmt}-{affinity}"
    if item["position"] == 0:
        print("COOLDOWN 120s (nonblocking runner)", "round", item["round"], flush=True)
        for step in range(4):
            time.sleep(30)
            print("idle", (step + 1) * 30, "of 120s", flush=True)
    env = dict(os.environ)
    for key in ["TOYKV_WAL_ATTRIBUTION", "TOYKV_PREPARATION_MICRO", "TOYKV_WAL_CONTEXT", "KV_PERF_CONTROL_DIR"]:
        env.pop(key, None)
    env["TOYKV_WAL_CONTEXT"] = str(p / (name + "-context.json"))
    command = [str(binary), "--bench", "wal_batch_concurrent", "--wal-batch-size", "64",
               "--pitr", "--pitr-format", fmt, "--wal-io-mode", "parallel",
               "--num", "750000", "--threads", "16", "--value-size", "1024",
               "--target-sst-size", "2147483648", "--compaction", "none",
               "--output", "json", "--latency-sample-every", "10",
               "--path", str(p / (name + "-db"))]
    if affinity == "pcore":
        command = ["taskset", "-c", "0-15"] + command
    print("START", name, flush=True)
    started = time.time()
    with (p / (name + ".json")).open("w") as out, (p / (name + ".stderr")).open("w") as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, env=env, start_new_session=True)
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
    (p / (name + "-status.json")).write_text(json.dumps({
        **item, "name": name, "command": command, "returncode": code,
        "started_at_unix": started, "finished_at_unix": time.time(),
    }, indent=2) + "\n")
    if code:
        print("FAILED", name, code, flush=True)
        raise SystemExit(code)
    data = json.loads((p / (name + ".json")).read_text())
    q, latency = data["result"], data["latency"]
    w = q["wal_pipeline"]
    mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
    print(json.dumps({"name": name, "puts_s": round(q["ops_per_sec"]),
                      "p99_ms": round(latency["p99_ms"], 3),
                      "cpu_s": round((q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000, 3),
                      "syncs": w["sync_count"], "mean_sync_ms": round(mean, 3),
                      "stall_flag": latency["p99_ms"] > 10 or mean > 1}), flush=True)
    time.sleep(10)
print("DONE", flush=True)
