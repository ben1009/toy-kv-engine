from collections import defaultdict
from pathlib import Path
import hashlib
import json
import re
import shutil
import statistics


p = Path(__file__).resolve().parent
repo = p.parent.parent
art = repo / "docs/wal/benchmarks/artifacts"
prefix = p.name
rows = []
for plan_name, sample_prefix, log_name in [("plan.json", "", "run.log"), ("cpu-followup-plan.json", "cpu-", "cpu-followup.log")]:
    plan = json.loads((p / plan_name).read_text())
    assert "DONE" in (p / log_name).read_text().splitlines()
    for i, (mode, fmt, writers) in enumerate(plan["schedule"]):
        name = f"{sample_prefix}{i:02}-{mode}-{fmt}-{writers}"
        raw = p / (name + ".json")
        record = json.loads(raw.read_text())
        status = json.loads((p / (name + "-status.json")).read_text())
        assert status["returncode"] == 0
        q, latency = record["result"], record["latency"]
        w = q["wal_pipeline"]
        mean_sync_ms = w["fdatasync_ns"] / w["sync_count"] / 1e6
        phase_file = p / (name + "-phases.json")
        phases = json.loads(phase_file.read_text()) if phase_file.exists() else None
        context_file = p / (name + "-context.json")
        context = json.loads(context_file.read_text()) if context_file.exists() else None
        estimates = {}
        update_histogram = [0] * 8
        if phases:
            for thread in phases["threads"]:
                update_histogram = [a + b for a, b in zip(update_histogram, thread["hash_updates"])]
                for label, bucket in zip(phases["phase_names"], thread["buckets"]):
                    if not bucket["calls"]:
                        continue
                    item = estimates.setdefault(label, {"calls": 0, "samples": 0, "cpu_s": 0., "wall_s": 0.})
                    item["calls"] += bucket["calls"]
                    item["samples"] += bucket["samples"]
                    if bucket["samples"]:
                        scale = bucket["calls"] / bucket["samples"] / 1e9
                        item["cpu_s"] += bucket["cpu_ns"] * scale
                        item["wall_s"] += bucket["wall_ns"] * scale
        digest = q.get("wal_v8_digest")
        hash_estimates = None
        if digest:
            tickets = sum(record["params"]["num"] // writers // 64 + (record["params"]["num"] // writers % 64 != 0) for _ in range(writers))
            # Scope call counts provide the exact number of admitted logical
            # batches, including uneven final per-thread batches.
            if estimates:
                tickets = estimates["encode"]["calls"]
            samples = digest["cpu_timing_sampled_tickets"]
            hash_estimates = {k.removesuffix("_ns") + "_estimated_s": digest[k] * tickets / samples / 1e9 for k in ["payload_hash_cpu_ns", "header_batch_hash_cpu_ns", "ordered_link_hash_cpu_ns"]}
        rows.append({
            "sample": name, "cohort": "cpu-aware-followup" if sample_prefix else "initial",
            "mode": mode, "format": fmt, "writers": writers,
            "diagnostic_only": mode == "diagnostic",
            "source_record": str(raw.relative_to(repo)),
            "source_record_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
            "status": status, "record": record,
            "stall_flag": latency["p99_ms"] > 10 or mean_sync_ms > 1,
            "mean_sync_ms": mean_sync_ms,
            "phase_estimates_inclusive": estimates,
            "phase_record": phases,
            "phase_record_sha256": hashlib.sha256(phase_file.read_bytes()).hexdigest() if phases else None,
            "hash_update_histogram": update_histogram if phases else None,
            "digest_cpu_estimates": hash_estimates,
            "context_record": context,
        })
assert len(rows) == 14
groups = defaultdict(list)
for row in rows:
    groups[row["cohort"], row["mode"], row["format"], row["writers"]].append(row)
summary = []
for (cohort, mode, fmt, writers), group in sorted(groups.items()):
    eligible = [r for r in group if not r["stall_flag"]]
    selected = max(eligible, key=lambda r: r["record"]["result"]["ops_per_sec"]) if eligible else None
    summary.append({
        "cohort": cohort, "mode": mode, "format": fmt, "writers": writers,
        "eligible": len(eligible), "total": len(group),
        "best_eligible_sample": selected["sample"] if selected else None,
        "best_eligible_puts_s": selected["record"]["result"]["ops_per_sec"] if selected else None,
        "median_eligible_puts_s": statistics.median(r["record"]["result"]["ops_per_sec"] for r in eligible) if eligible else None,
    })
micro_files = ["micro.json", *[f.name for f in sorted(p.glob("micro-core-*.json"))]]
micro_raw = {name: json.loads((p / name).read_text()) for name in micro_files}
micro_groups = defaultdict(list)
for name, data in micro_raw.items():
    assert data["all_canonical_bytes_crcs_and_entry_digests_validated"]
    if name == "micro.json":
        continue
    cpu = int(name.split("cpu")[-1].split(".")[0])
    for sample in data["samples"]:
        micro_groups[cpu, sample["entries"], sample["stage"]].append(sample["cpu_ns"] / sample["iterations"] / 1000)
micro_summary = [{
    "cpu": cpu, "entries": entries, "stage": stage, "samples": len(values),
    "median_cpu_us_per_batch": statistics.median(values), "min_cpu_us_per_batch": min(values), "max_cpu_us_per_batch": max(values),
} for (cpu, entries, stage), values in sorted(micro_groups.items())]
document = {
    "samples": len(rows), "stall_flagged": sum(r["stall_flag"] for r in rows),
    "groups": summary,
    "scope_limit": "Whole-batch timings are sampled/inclusive; do not add child allocation/entry scopes to their encode parent. CPU-only values cannot be subtracted from live profiles. All diagnostic throughput excluded from performance qualification.",
    "runtime_unchanged": True,
}
(p / "measurements.jsonl").write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
(p / "summary.json").write_text(json.dumps(document, indent=2) + "\n")
(p / "micro-raw.json").write_text(json.dumps(micro_raw, separators=(",", ":")) + "\n")
(p / "micro-summary.json").write_text(json.dumps(micro_summary, indent=2) + "\n")
for suffix in ["measurements.jsonl", "summary.json", "micro-raw.json", "micro-summary.json", "micro-plan.json", "micro-cores-plan.json", "plan.json", "cpu-followup-plan.json", "source-hashes.json", "instrumented-hashes.json", "restoration.json", "producer-attribution.patch", "cpu-topology.json", "core-masks.json", "cpu-sample-summary.json", "run.py", "cpu-followup.py", "micro-cores.py", "analyze-cpus.py", "archive.py", "perf-export.json", "perf-stack-retry.json"]:
    shutil.copyfile(p / suffix, art / (prefix + "-" + suffix))
for f in sorted(p.glob("*-symbols.txt")):
    normalized = "\n".join(re.sub(r"\s+", " ", line.strip()) for line in f.read_text().splitlines()) + "\n"
    (art / (prefix + "-" + f.name)).write_text(normalized)
print("Archived", len(rows), "measurements,", document["stall_flagged"], "flagged; 840 isolated CPU samples.")
for row in rows:
    q = row["record"]["result"]
    phases = {k: round(v["cpu_s"], 3) for k, v in row["phase_estimates_inclusive"].items()}
    print(row["sample"], "flag", row["stall_flag"], "CPU", round((q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000, 3), "phases", phases, "digest", row["digest_cpu_estimates"])
