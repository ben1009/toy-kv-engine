from pathlib import Path
import hashlib
import json
import time

import run


p = Path(__file__).resolve().parent
old = p / "preflight-inheritance/write-perf-background-affinity"
current = p / "write-perf-background-affinity"
binaries = {"unregistered": old, "registered": current}
hashes = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}
assert hashes["unregistered"] == "d323b9633846138f5dde2f5e8f765fd6c1ecef32cc5292038ba1b858c0e09657"
assert hashes["registered"] == json.loads((p / "build.json").read_text())["binary_sha256"]
schedule = ["unregistered", "registered", "registered", "unregistered"]
plan = {"purpose": "Whole-process P-core calibration of the startup affinity-registration hook, using the earlier preflight executable with the same foreground harness. Original main cohort remains separate.",
        "schedule": schedule, "binary_sha256": hashes, "puts": 750000, "writers": 16, "batch": 64,
        "format": "v8", "storage_flag": "p99_ms >10 OR mean fdatasync_ms >1, unchanged",
        "cooldown_before_each_pair_seconds": 60, "between_captures_seconds": 3,
        "selection": "All records retained; highest eligible within each calibration arm, with same-run metrics. No comparison of raw throughput from storage-flagged captures.",
        "change": "Both use taskset 0-15 and the same one-time writer/background mask audit. Registered executable invokes register_iowq_aff(0-15) before each ring starts; unregistered does not. No hook remains in source."}
assert not (p / "calibration-plan.json").exists()
(p / "calibration-plan.json").write_text(json.dumps(plan, indent=2) + "\n")
for index, arm in enumerate(schedule):
    if index % 2 == 0:
        print("COOLDOWN 60s", flush=True)
        time.sleep(60)
    run.binary = binaries[arm]
    run.run_capture(f"calibration-{index:02}-{arm}", "v8", "whole", 750000,
                    register_kernel=arm == "registered")
    time.sleep(3)
print("CALIBRATION DONE", flush=True)
