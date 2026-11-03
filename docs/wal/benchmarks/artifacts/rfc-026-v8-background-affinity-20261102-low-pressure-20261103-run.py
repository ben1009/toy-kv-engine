from pathlib import Path
import hashlib
import importlib.util
import json
import os
import time


p = Path(__file__).resolve().parent
base = p.parent
spec = importlib.util.spec_from_file_location("affinity_base", base / "run.py")
affinity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(affinity)
binary = affinity.binary
expected = json.loads((base / "build.json").read_text())["binary_sha256"]
assert hashlib.sha256(binary.read_bytes()).hexdigest() == expected
assert sorted(os.sched_getaffinity(0)) == list(range(32))
affinity.p = p
orders = [["writers", "whole"], ["whole", "writers"]]
schedule = [{"round": r, "position": i, "mode": mode, "format": "v8"}
            for r, order in enumerate(orders) for i, mode in enumerate(order)]
plan = {"purpose": "Work within the existing single physical SSD: test whether reducing benchmark write volume gives usable endpoint controls. Lower-pressure diagnostic only, not a replacement for the 750k-put RFC performance gate.",
        "binary": str(binary), "binary_sha256": expected, "schedule": schedule,
        "puts_per_capture": 150000, "writers": 16, "application_batch_entries": 64,
        "value_bytes": 1024, "format": "v8", "wal_io_mode": "parallel",
        "storage": "Fresh local repository paths on ext4 /home; no tmpfs substitution or other SSD.",
        "masks": "Same writer-only and whole-process endpoints as prior cohorts, including explicit io-wq masks and all role audits.",
        "idle_before_each_pair_seconds": 15, "between_captures_seconds": 3,
        "storage_flag": "Existing p99_ms >10 OR mean fdatasync_ms >1; unchanged.",
        "selection": "Preserve all four captures and pair results. Report storage eligibility separately from RFC qualification. Short runs have fewer latency samples and no sustained-write/parity claim; no earlier 750k scores are replaced.",
        "runtime_changed": False, "device_settings_changed": False,
        "tracing_or_admin_queries_during_capture": False}
assert not (p / "plan.json").exists()
(p / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
rows = []
for index, item in enumerate(schedule):
    if item["position"] == 0:
        print("COOLDOWN 15s", "pair", item["round"], flush=True)
        time.sleep(15)
    name = f"{index:02}-v8-{item['mode']}"
    affinity.run_capture(name, "v8", item["mode"], 150000)
    raw = json.loads((p / (name + ".json")).read_text())
    audit = json.loads((p / (name + "-affinity.json")).read_text())
    status = json.loads((p / (name + "-status.json")).read_text())
    q = raw["result"]
    w = q["wal_pipeline"]
    mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
    rows.append({**item, "name": name, "record": raw, "audit": audit, "status": status,
                 "puts_s": q["ops_per_sec"], "p99_ms": raw["latency"]["p99_ms"],
                 "cpu_s": (q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000,
                 "syncs": w["sync_count"], "mean_sync_ms": mean,
                 "storage_flag": raw["latency"]["p99_ms"] > 10 or mean > 1,
                 "RFC_performance_qualified": False})
    time.sleep(3)
(p / "measurements.jsonl").write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
summary = {"captures": len(rows), "storage_eligible": sum(not row["storage_flag"] for row in rows),
           "jointly_eligible_pairs": [all(not row["storage_flag"] for row in rows[i:i+2]) for i in [0, 2]],
           "RFC_performance_qualified": False,
           "scope": "150k-put diagnostic: workload reduced by 80%; same binary, batch64, roles, device and thresholds. Not a sustained SSD or V4/V7 parity score.",
           "runtime_changed": False, "device_settings_changed": False}
(p / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary), flush=True)
print("DONE", flush=True)
