from collections import Counter, defaultdict
from pathlib import Path
import hashlib
import json
import re
import statistics


p = Path(__file__).resolve().parent
repo = p.parent.parent
plan = json.loads((p / "plan.json").read_text())
rows = []
scheduled = []
for plan_file, cohort, prefix in [("plan.json", "initial", ""),
                                  ("confirmation-plan.json", "confirmation", "confirm-"),
                                  ("v7-rerun-plan.json", "v7-user-rerun", "v7-rerun-")]:
    if not (p / plan_file).exists():
        continue
    for index, item in enumerate(json.loads((p / plan_file).read_text())["schedule"]):
        name = f"{prefix}{index:02}-{item['mode']}-{item['format']}-{item['affinity']}"
        scheduled.append((cohort, name, item))
for cohort, name, item in scheduled:
    status_path = p / (name + "-status.json")
    if not status_path.exists():
        continue
    status = json.loads(status_path.read_text())
    assert status["returncode"] == 0, name
    path = p / (name + ".json")
    data = json.loads(path.read_text())
    q = data["result"]
    w = q["wal_pipeline"]
    mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
    phases_path = p / (name + "-phases.json")
    phases = json.loads(phases_path.read_text()) if phases_path.exists() else None
    context_path = p / (name + "-context.json")
    context = json.loads(context_path.read_text()) if context_path.exists() else None
    estimates = {}
    if phases:
        for thread in phases["threads"]:
            for label, bucket in zip(phases["phase_names"], thread["buckets"]):
                if not bucket["calls"]:
                    continue
                estimate = estimates.setdefault(label, {"calls": 0, "samples": 0, "cpu_s": 0., "wall_s": 0.})
                estimate["calls"] += bucket["calls"]
                estimate["samples"] += bucket["samples"]
                if bucket["samples"]:
                    scale = bucket["calls"] / bucket["samples"] / 1e9
                    estimate["cpu_s"] += bucket["cpu_ns"] * scale
                    estimate["wall_s"] += bucket["wall_ns"] * scale
        context = {key: phases["process_after"][key] - phases["process_before"][key]
                   for key in ["voluntary", "involuntary"]}
    digest = q.get("wal_v8_digest")
    hash_estimates = None
    if digest:
        # write-perf partitions 750,000 operations evenly across 16 writers.
        # Each writer uses ceil(46,875/64)=733 logical batches.
        tickets = 11728
        if estimates:
            assert estimates["encode"]["calls"] == tickets
        assert digest["payload_hash_bytes"] == 787500000
        assert digest["post_encode_hash_reread_bytes"] == 0
        samples = digest["cpu_timing_sampled_tickets"]
        hash_estimates = {key.removesuffix("_ns") + "_estimated_s": digest[key] * tickets / samples / 1e9
                          for key in ["payload_hash_cpu_ns", "header_batch_hash_cpu_ns", "ordered_link_hash_cpu_ns"]}
    row = {
        **item, "cohort": cohort, "name": name, "diagnostic_only": item["mode"] == "diagnostic",
        "source_record": str(path.relative_to(repo)),
        "source_record_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "status": status, "record": data,
        "puts_s": q["ops_per_sec"], "p99_ms": data["latency"]["p99_ms"],
        "cpu_s": (q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000,
        "mean_sync_ms": mean,
        "stall_flag": data["latency"]["p99_ms"] > 10 or mean > 1,
        "context": context, "phase_estimates_inclusive": estimates,
        "phase_record": phases, "digest_cpu_estimates": hash_estimates,
    }
    rows.append(row)

groups = defaultdict(list)
for row in rows:
    groups[row["mode"], row["format"], row["affinity"]].append(row)
summary = []
for (mode, fmt, affinity), values in sorted(groups.items()):
    eligible = [r for r in values if not r["stall_flag"]]
    selected = max(eligible, key=lambda r: r["puts_s"]) if eligible else None
    summary.append({
        "mode": mode, "format": fmt, "affinity": affinity,
        "eligible": len(eligible), "total": len(values),
        "best_eligible": {k: selected[k] for k in ["name", "puts_s", "p99_ms", "cpu_s", "context", "mean_sync_ms", "digest_cpu_estimates"]} if selected else None,
        "median_eligible": {k: statistics.median(r[k] for r in eligible) for k in ["puts_s", "p99_ms", "cpu_s"]} if eligible else None,
    })
cohorts = defaultdict(list)
for row in rows:
    cohorts[row["cohort"], row["mode"], row["format"], row["affinity"]].append(row)
cohort_counts = [{"cohort": cohort, "mode": mode, "format": fmt, "affinity": affinity,
                  "total": len(values), "eligible": sum(not r["stall_flag"] for r in values)}
                 for (cohort, mode, fmt, affinity), values in sorted(cohorts.items())]
doc = {"samples": len(rows), "stall_flagged": sum(r["stall_flag"] for r in rows),
       "groups": summary, "cohort_counts": cohort_counts, "runtime_modified": False}
(p / "measurements.jsonl").write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
(p / "summary.json").write_text(json.dumps(doc, indent=2) + "\n")
for row in rows:
    print(row["name"], "flagged" if row["stall_flag"] else "eligible",
          round(row["puts_s"]), round(row["p99_ms"], 3), round(row["cpu_s"], 3), row["context"])
print("SUMMARY")
for row in summary:
    print(json.dumps(row))

cpu_results = {}
for path in sorted(p.glob("*-stacks.txt")):
    current = None
    samples = []
    for line in path.read_text().splitlines():
        match = re.match(r"^\S.*?\s+(\d+)/(\d+)\s+\[(\d+)\]\s+(\d+\.\d+): cpu-clock:u:", line)
        if match:
            if current:
                samples.append(current)
            current = {"pid": int(match[1]), "tid": int(match[2]), "cpu": int(match[3]), "stack": []}
        elif current and line.startswith("\t"):
            parts = line.strip().split(" ", 1)
            if len(parts) == 2:
                current["stack"].append(parts[1])
    if current:
        samples.append(current)
    name = path.stem.removesuffix("-stacks")
    phases = json.loads((p / (name + "-phases.json")).read_text())
    phase_index = phases["phase_names"].index("v7_logical_hash")
    ordered_threads = {t["tid"] for t in phases["threads"] if t["buckets"][phase_index]["calls"]}
    counts = defaultdict(Counter)
    tids = defaultdict(set)
    for sample in samples:
        leaf = sample["stack"][0] if sample["stack"] else "[missing]"
        category = "blake3_hash_many" if "blake3_hash_many" in leaf else (
            "blake3_other" if "blake3" in leaf else (
                "sha256" if "sha256" in leaf else ("crc32" if "crc32" in leaf else "other")))
        kind = "P" if sample["cpu"] < 16 else "E"
        for key in ["all", category]:
            counts[key][kind] += 1
            tids[key].add(sample["tid"])
        if category == "sha256" and sample["tid"] in ordered_threads:
            counts["sha256_on_ordered_packer"][kind] += 1
            tids["sha256_on_ordered_packer"].add(sample["tid"])
    if "pcore" in name:
        assert counts["all"]["E"] == 0, name
    cpu_results[name] = {key: {"P_samples": counter["P"], "E_samples": counter["E"],
                               "E_pct": 100 * counter["E"] / sum(counter.values()), "threads": len(tids[key])}
                         for key, counter in counts.items()}
(p / "cpu-sample-summary.json").write_text(json.dumps({
    "method": "Leaf-symbol counts from timed --sample-cpu cpu-clock:u recordings; kernel P=0-15, E=16-31. Time-weighted samples, not job/byte proportions. Ordered SHA subset matches thread IDs with measured V7 logical hashing.",
    "samples": cpu_results,
}, indent=2) + "\n")
