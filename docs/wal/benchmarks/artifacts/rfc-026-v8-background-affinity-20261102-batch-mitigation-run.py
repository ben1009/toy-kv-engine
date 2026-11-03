from pathlib import Path
import hashlib
import importlib.util
import json
import os
import signal
import subprocess
import time


p = Path(__file__).resolve().parent
base = p.parent
spec = importlib.util.spec_from_file_location("affinity_base", base / "run.py")
affinity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(affinity)
binary = base / "write-perf-background-affinity"
expected = json.loads((base / "build.json").read_text())["binary_sha256"]
assert hashlib.sha256(binary.read_bytes()).hexdigest() == expected
assert sorted(os.sched_getaffinity(0)) == list(range(32))
schedule = [64, 256, 256, 64]
plan = {"purpose": "Application-level mitigation screen: amortize synchronous storage costs with larger atomic write batches. This changes workload/request size, not production runtime or the SSD, and is not a V8 retention/parity benchmark.",
        "binary": str(binary), "binary_sha256": expected,
        "schedule": schedule, "puts_per_capture": 750000, "writers": 16,
        "value_bytes": 1024, "format": "v8", "wal_io_mode": "parallel",
        "cpu_mask": "whole process 0-15, explicitly registered io-wq 0-15 in every capture",
        "initial_idle_seconds": 15, "between_captures_seconds": 3,
        "durability": "Unchanged engine semantics: ACK after durable frontier sync; batch256 changes the atomic application request from 64 to 256 entries.",
        "latency": "p99 is per application batch, not per individual put; report alongside throughput and sync count.",
        "storage_flag": "Keep existing p99_ms >10 OR mean fdatasync_ms >1 labels. Preserve EVERY run, including flagged records, since slow-storage behavior is the mitigation target. Do not treat flagged performance as a qualified RFC gain.",
        "interpretation": "Require the mechanism (reduced sync count) and consistent directions in both adjacent reversed-order comparisons before describing an operational benefit; changing device state remains a confound. No runtime adoption or threshold relaxation.",
        "device_settings_changed": False, "runtime_changed": False}
assert not (p / "plan.json").exists()
(p / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
print("COOLDOWN 15s", flush=True)
time.sleep(15)
records = []
for index, batch in enumerate(schedule):
    name = f"{index:02}-batch{batch}"
    env = dict(os.environ)
    for key in ["TOYKV_WAL_ATTRIBUTION", "TOYKV_PREPARATION_MICRO", "TOYKV_WAL_CONTEXT", "KV_PERF_CONTROL_DIR", "TOYKV_WRITER_PCORE"]:
        env.pop(key, None)
    env["TOYKV_AFFINITY_MODE"] = "whole"
    env["TOYKV_AFFINITY_REPORT"] = str(p / (name + "-affinity.json"))
    env["TOYKV_IOWQ_PCORE"] = "1"
    command = ["taskset", "-c", "0-15", str(binary), "--bench", "wal_batch_concurrent",
               "--wal-batch-size", str(batch), "--pitr", "--pitr-format", "v8", "--wal-io-mode", "parallel",
               "--num", "750000", "--threads", "16", "--value-size", "1024",
               "--target-sst-size", "2147483648", "--compaction", "none", "--output", "json",
               "--latency-sample-every", "10", "--path", str(p / (name + "-db"))]
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
    status = {"name": name, "batch": batch, "command": command, "returncode": code,
              "started_at_unix": started, "finished_at_unix": time.time()}
    (p / (name + "-status.json")).write_text(json.dumps(status, indent=2) + "\n")
    assert code == 0, status
    raw = json.loads((p / (name + ".json")).read_text())
    audit = json.loads((p / (name + "-affinity.json")).read_text())
    affinity.validate_roles(audit, "whole")
    registrations = [line for line in (p / (name + ".stderr")).read_text().splitlines() if line.startswith("TOYKV_IOWQ_AFFINITY ")]
    assert registrations and all("registered=1 cpus=0-15 " in line for line in registrations)
    q = raw["result"]
    assert q["ops"] == 750000
    assert q["wal_v8_digest"]["payload_hash_bytes"] == 787500000
    assert q["wal_v8_digest"]["post_encode_hash_reread_bytes"] == 0
    w = q["wal_pipeline"]
    mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
    row = {"name": name, "batch": batch, "status": status, "record": raw, "audit": audit,
           "registrations": registrations, "puts_s": q["ops_per_sec"], "p99_ms": raw["latency"]["p99_ms"],
           "cpu_s": (q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000,
           "syncs": w["sync_count"], "mean_sync_ms": mean,
           "storage_flag": raw["latency"]["p99_ms"] > 10 or mean > 1}
    records.append(row)
    print(json.dumps({key: row[key] for key in ["name", "puts_s", "p99_ms", "cpu_s", "syncs", "mean_sync_ms", "storage_flag"]}), flush=True)
    time.sleep(3)
(p / "measurements.jsonl").write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in records))
print("DONE", flush=True)
