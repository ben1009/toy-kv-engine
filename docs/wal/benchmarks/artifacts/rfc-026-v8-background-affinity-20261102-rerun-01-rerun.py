from pathlib import Path
import hashlib
import json
import os
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import run


p = Path(__file__).resolve().parent
binary = run.binary
expected = json.loads((p.parent / "build.json").read_text())["binary_sha256"]
assert hashlib.sha256(binary.read_bytes()).hexdigest() == expected
assert sorted(os.sched_getaffinity(0)) == list(range(32))
assert (p.parent / "smoke-checks.json").exists()
run.p = p
orders = [run.modes, list(reversed(run.modes))]
schedule = [{"round": r, "position": i, "mode": mode, "format": "v8"}
            for r, order in enumerate(orders) for i, mode in enumerate(order)]
plan = json.loads((p.parent / "plan.json").read_text())
plan.update({"schedule": schedule, "cohort": "user-requested rerun-01",
             "cooldown_before_each_round_seconds": None,
             "cooldown_before_each_pair_seconds": 60,
             "selection": "Highest eligible throughput per mode in this fresh cohort, all same-run metrics; preserve medians and every raw capture. Twelve captures, forward/reverse six-mode orders, no runtime or mask changes. Original cohort remains separate.",
             "runtime_changed": False, "binary_sha256": expected})
assert not (p / "plan.json").exists()
(p / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
for index, item in enumerate(schedule):
    if item["position"] % 2 == 0:
        print("COOLDOWN 60s", "round", item["round"], "pair", item["position"] // 2, flush=True)
        time.sleep(60)
    run.run_capture(f"{index:02}-v8-{item['mode']}", "v8", item["mode"], 750000)
    time.sleep(3)
print("RERUN DONE", flush=True)
