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
plan = json.loads((p.parent / "plan.json").read_text())
control_orders = [["writers", "whole"], ["whole", "writers"]]
controls = [{"round": r, "position": i, "mode": mode, "format": "v8"}
            for r, order in enumerate(control_orders) for i, mode in enumerate(order)]
role_orders = [run.modes, list(reversed(run.modes))]
conditional = [{"round": r + 2, "position": i, "mode": mode, "format": "v8"}
               for r, order in enumerate(role_orders) for i, mode in enumerate(order)]
plan.update({"schedule": controls, "conditional_role_schedule": conditional,
             "cohort": "user-requested rerun-02 with endpoint controls first",
             "cooldown_before_each_round_seconds": None,
             "cooldown_before_each_pair_seconds": None,
             "control_idle_seconds": [15, 45],
             "conditional_role_idle_before_each_pair_seconds": 45,
             "expansion_gate": "Run the twelve additional role captures only if at least one of the two writer-only/whole-process control pairs is jointly eligible. Fixed existing filter: p99_ms <=10 AND mean fdatasync_ms <=1 for both endpoints. Otherwise finish after four controls and report the lack of eligible controls.",
             "selection": "Highest eligible throughput per mode, all same-run metrics; preserve all raw captures and medians. Control pairs executed forward/reverse. Role expansion is predeclared conditional on a jointly eligible control pair. No device/admin queries or tracing during this cohort.",
             "runtime_changed": False, "binary_sha256": expected})
assert not (p / "plan.json").exists()
(p / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")


def capture(index, item):
    name = f"{index:02}-v8-{item['mode']}"
    run.run_capture(name, "v8", item["mode"], 750000)
    data = json.loads((p / (name + ".json")).read_text())
    w = data["result"]["wal_pipeline"]
    eligible = data["latency"]["p99_ms"] <= 10 and w["fdatasync_ns"] / w["sync_count"] / 1e6 <= 1
    time.sleep(3)
    return eligible


eligible_pairs = []
for r, idle in enumerate([15, 45]):
    print("COOLDOWN", idle, "control pair", r, flush=True)
    time.sleep(idle)
    pair = [capture(2 * r + i, controls[2 * r + i]) for i in range(2)]
    eligible_pairs.append(all(pair))
decision = {"jointly_eligible_control_pairs": eligible_pairs,
            "role_expansion_enabled": any(eligible_pairs),
            "gate_evaluated_after_all_four_controls": True}
(p / "gate.json").write_text(json.dumps(decision, indent=2) + "\n")
if decision["role_expansion_enabled"]:
    plan["schedule"].extend(conditional)
    (p / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    for offset, item in enumerate(conditional, 4):
        if item["position"] % 2 == 0:
            print("COOLDOWN 45s", "role round", item["round"], flush=True)
            time.sleep(45)
        capture(offset, item)
else:
    print("ROLE EXPANSION SKIPPED: neither endpoint control pair was jointly eligible", flush=True)
print("RERUN DONE", flush=True)
