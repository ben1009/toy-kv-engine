from pathlib import Path
import hashlib
import json
import os
import subprocess
import time


p = Path(__file__).resolve().parent
diag = p / "write-perf-producer-attribution"
control = p.parent / "rfc-026-v8-notification-split-20261102/write-perf-cursor"
schedule = []
for round_index in range(2):
    for writers in ([1, 16] if round_index == 0 else [16, 1]):
        for fmt in (["v7", "v8"] if round_index == 0 else ["v8", "v7"]):
            schedule.append(["diagnostic", fmt, writers])
for fmt in ["v7", "v8", "v8", "v7"]:
    schedule.append(["control", fmt, 16])
plan = {
    "base_revision": "b8da402a276d7969f4c40ca99787a10a63d93f33",
    "schedule": schedule,
    "binary_sha256": {"diagnostic": hashlib.sha256(diag.read_bytes()).hexdigest(), "control": hashlib.sha256(control.read_bytes()).hexdigest()},
    "binary_paths": {"diagnostic": str(diag), "control": str(control)},
    "diagnostic": "Whole-batch CPU/wall phase scopes sampled 1/64 per thread, existing ticket-sampled BLAKE3 metrics, no allocation wrapper, no detailed --profile. perf cpu-clock:u 499Hz DWARF4096 enabled only for timed interval; output memfd until completion.",
    "control": "Existing kept normal-counter executable, no attribution hooks or perf sampling; batch context-switch probe only.",
    "workload": {"puts": 750000, "batch": 64, "value_bytes": 1024, "target_sst_size": 2147483648, "compaction": "none", "wal_io_mode": "parallel", "latency_sample_every": 10},
    "cooldown_seconds": 45, "between_runs_seconds": 3,
    "flag": "p99 >10ms or mean fdatasync >1ms; retain every diagnostic and report flagged status. Diagnostics never enter performance best-score rankings.",
    "selection": "Highest eligible throughput only within each diagnostic/control format and writer count, with all same-run CPU and phase data; all runs and medians retained. One fixed cohort, no code tuning.",
    "retained_runtime": "Notification separation, single durability driver and published cursor unchanged; all temporary instrumentation will be removed after profiling.",
}
assert plan["binary_sha256"]["control"] == "937a3843a044e107e15db756674be151875e388ff3a0a392cbd4e97463bbd2aa"
assert not (p / "plan.json").exists()
(p / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
print("COOLDOWN 45 seconds", flush=True)
time.sleep(45)
for index, (mode, fmt, writers) in enumerate(schedule):
    name = f"{index:02}-{mode}-{fmt}-{writers}"
    env = dict(os.environ)
    env.pop("TOYKV_WAL_ATTRIBUTION", None)
    env.pop("TOYKV_PREPARATION_MICRO", None)
    cmd = [str(diag if mode == "diagnostic" else control),
           "--bench", "wal_batch_concurrent", "--wal-batch-size", "64",
           "--pitr", "--pitr-format", fmt, "--wal-io-mode", "parallel",
           "--num", "750000", "--threads", str(writers), "--value-size", "1024",
           "--target-sst-size", "2147483648", "--compaction", "none",
           "--output", "json", "--latency-sample-every", "10",
           "--path", str(p / (name + "-db"))]
    fd = None
    if mode == "diagnostic":
        ctl = p / (name + "-control")
        ctl.mkdir()
        for pipe in ["control", "ack"]:
            os.mkfifo(ctl / pipe)
        env["KV_PERF_CONTROL_DIR"] = str(ctl)
        env["TOYKV_WAL_ATTRIBUTION"] = str(p / (name + "-phases.json"))
        fd = os.memfd_create("producer-perf", 0)
        cmd = ["perf", "record", "-q", "-e", "cpu-clock:u", "-F", "499",
               "--call-graph", "dwarf,4096", "--delay=-1", "--control", f"fifo:{ctl}/control,{ctl}/ack",
               "-o", f"/proc/self/fd/{fd}", "--"] + cmd
    else:
        env["TOYKV_WAL_CONTEXT"] = str(p / (name + "-context.json"))
    print("START", name, flush=True)
    started = time.time()
    try:
        with (p / (name + ".json")).open("w") as out, (p / (name + ".stderr")).open("w") as err:
            result = subprocess.run(cmd, stdout=out, stderr=err, env=env, pass_fds=[] if fd is None else [fd], timeout=180)
        code = result.returncode
    except subprocess.TimeoutExpired:
        code = 124
    finally:
        if fd is not None:
            os.lseek(fd, 0, 0)
            with (p / (name + ".perf")).open("wb") as out:
                while block := os.read(fd, 1024 * 1024):
                    out.write(block)
            os.close(fd)
    (p / (name + "-status.json")).write_text(json.dumps({"command": cmd, "returncode": code, "started_at_unix": started, "finished_at_unix": time.time()}, indent=2) + "\n")
    if code:
        print("FAILED", name, code, (p / (name + ".stderr")).read_text()[-1200:], flush=True)
        break
    d = json.loads((p / (name + ".json")).read_text())
    q, latency = d["result"], d["latency"]
    wal = q["wal_pipeline"]
    mean = wal["fdatasync_ns"] / wal["sync_count"] / 1e6
    print(json.dumps({"name": name, "puts_s": round(q["ops_per_sec"]), "p99_ms": round(latency["p99_ms"], 3), "cpu_s": round((q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000, 3), "syncs": wal["sync_count"], "mean_sync_ms": round(mean, 3), "stall_flag": latency["p99_ms"] > 10 or mean > 1}), flush=True)
    time.sleep(3)
print("DONE", flush=True)
