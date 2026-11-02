from collections import defaultdict
from pathlib import Path
import hashlib
import json
import shutil
import statistics


p = Path(__file__).resolve().parent
repo = p.parent.parent
artifacts = repo / "docs/wal/benchmarks/artifacts"
prefix = p.name
plan = json.loads((p / "plan.json").read_text())
assert "DONE" in (p / "run.log").read_text().splitlines()
rows = []
scheduled = [("rerun", index, *case) for index, case in enumerate(plan["schedule"])]
confirmation_complete = (p / "confirmation.log").exists() and "DONE" in (p / "confirmation.log").read_text().splitlines()
if confirmation_complete:
    confirmation_plan = json.loads((p / "confirmation-plan.json").read_text())
    scheduled.extend(("confirmation", index, *case) for index, case in enumerate(confirmation_plan["schedule"]))
for cohort, index, kind, writers, arm in scheduled:
    name = f"{cohort}-{index:02}-{kind}-{arm}-{writers}"
    raw_path = p / (name + ".json")
    status_path = p / (name + "-status.json")
    status = json.loads(status_path.read_text())
    assert status["returncode"] == 0, name
    record = json.loads(raw_path.read_text())
    assert record["schema"] == "kv-engine.write-perf.v1"
    result = record["result"]
    wal = result["wal_pipeline"]
    mean_sync_ms = wal["fdatasync_ns"] / wal["sync_count"] / 1e6
    context_path = p / (name + "-context.json")
    context = json.loads(context_path.read_text()) if context_path.exists() else None
    rows.append({
        "sample": name,
        "cohort": cohort,
        "kind": kind, "writers": writers, "arm": arm,
        "source_record": str(raw_path.relative_to(repo)),
        "source_record_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
        "status": status,
        "stall_flag": record["latency"]["p99_ms"] > 10 or mean_sync_ms > 1,
        "mean_sync_ms": mean_sync_ms,
        "context_switches": context,
        "context_record_sha256": hashlib.sha256(context_path.read_bytes()).hexdigest() if context else None,
        "record": record,
    })
assert len(rows) == (36 if confirmation_complete else 24)
groups = defaultdict(list)
for row in rows:
    groups[row["cohort"], row["kind"], row["writers"], row["arm"]].append(row)
summary = []
for (cohort, kind, writers, arm), measurements in sorted(groups.items()):
    eligible = [row for row in measurements if not row["stall_flag"]]
    selected = max(eligible, key=lambda row: row["record"]["result"]["ops_per_sec"]) if eligible else None
    best = None
    if selected:
        q = selected["record"]["result"]
        w = q["wal_pipeline"]
        best = {
            "sample": selected["sample"],
            "puts_s": q["ops_per_sec"], "p99_ms": selected["record"]["latency"]["p99_ms"],
            "cpu_s": (q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000,
            "context_switches": selected["context_switches"],
            "eventfd_requests": w["worker_eventfd_notifications"],
            "eventfd_writes": w["worker_eventfd_writes"],
            "sync_count": w["sync_count"], "mean_sync_ms": selected["mean_sync_ms"],
            "fdatasync_ms": w["fdatasync_ns"] / 1e6,
        }
    summary.append({
        "cohort": cohort, "kind": kind, "writers": writers, "arm": arm,
        "eligible": len(eligible), "total": len(measurements),
        "median_eligible_puts_s": statistics.median(row["record"]["result"]["ops_per_sec"] for row in eligible) if eligible else None,
        "median_eligible_cpu_s": statistics.median((row["record"]["result"]["process_cpu_user_ms"] + row["record"]["result"]["process_cpu_system_ms"]) / 1000 for row in eligible) if eligible else None,
        "best": best,
    })
comparisons = []
for cohort, kind, writers in sorted({(row["cohort"], row["kind"], row["writers"]) for row in rows}):
    arms = {s["arm"]: s for s in summary if s["cohort"] == cohort and s["kind"] == kind and s["writers"] == writers}
    baseline, kept = arms["baseline"]["best"], arms["kept"]["best"]
    differences = None
    if baseline and kept:
        differences = {key + "_change_pct": (kept[key] / baseline[key] - 1) * 100 if baseline[key] else None for key in ["puts_s", "p99_ms", "cpu_s", "sync_count", "eventfd_writes"]}
        if baseline["context_switches"] and kept["context_switches"]:
            for key in ["voluntary", "involuntary"]:
                b, k = baseline["context_switches"][key], kept["context_switches"][key]
                differences[key + "_switches_change_pct"] = (k / b - 1) * 100 if b else None
    comparisons.append({"cohort": cohort, "kind": kind, "writers": writers, "arms": arms, "differences": differences})
document = {
    "cohorts": sorted({row["cohort"] for row in rows}), "total": len(rows),
    "stall_flagged": sum(row["stall_flag"] for row in rows),
    "selection": plan["selection"], "retention": "Kept at explicit maintainer request; no performance-triggered revert.",
    "arms": summary, "comparisons": comparisons,
}
(p / "measurements.jsonl").write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
(p / "summary.json").write_text(json.dumps(document, indent=2) + "\n")
for suffix in ["measurements.jsonl", "summary.json", "plan.json", "run.py", "restoration.json", "archive.py", "confirmation-plan.json", "confirmation.py"]:
    shutil.copyfile(p / suffix, artifacts / (prefix + "-" + suffix))
print("Measurements", len(rows), "flagged", document["stall_flagged"])
for row in summary:
    best = row["best"]
    print(row["cohort"], row["kind"], row["writers"], row["arm"], str(row["eligible"]) + "/" + str(row["total"]), None if best is None else (round(best["puts_s"]), round(best["p99_ms"], 3), round(best["cpu_s"], 3), best["context_switches"], best["eventfd_writes"], best["sync_count"]))
for comparison in comparisons:
    print(comparison["cohort"], comparison["kind"], comparison["writers"], comparison["differences"])
