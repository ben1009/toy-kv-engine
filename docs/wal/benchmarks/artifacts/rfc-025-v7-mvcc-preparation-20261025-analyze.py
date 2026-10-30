#!/usr/bin/env python3
"""Reproduce paired throughput/latency comparisons and validate run provenance."""
import csv
import hashlib
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def median(values):
    return statistics.median(values)


def load(stem):
    raw_path = ROOT / (stem + ".jsonl")
    manifest_path = ROOT / (stem + "-runs.tsv")
    records = {}
    for line in raw_path.read_bytes().splitlines():
        record = json.loads(line)
        records[str(record["run_id"])] = (record, hashlib.sha256(line).hexdigest())
    pairs = {}
    hashes = set()
    for row in csv.DictReader(manifest_path.open(), delimiter="\t"):
        record, digest = records[row["run_id"]]
        assert digest == row["raw_sha256"], row["source_record"]
        assert record["params"]["wal_io_mode"] == "parallel"
        assert record["params"]["wal_format_version"] == (4 if row["arm"].endswith("v4") else 7)
        hashes.add(row["binary_sha256"])
        if row["phase"] == "scored":
            key = (int(row["writers"]), int(row["pair"]))
            pairs.setdefault(key, {})[row["arm"]] = record
    provenance = {
        "binary_sha256": sorted(hashes),
        "artifact_sha256": {
            suffix: hashlib.sha256((ROOT / (stem + suffix)).read_bytes()).hexdigest()
            for suffix in ("-run.sh", "-runs.tsv", ".jsonl")
        },
    }
    return pairs, provenance


def throughput(record):
    return record["result"]["ops_per_sec"]


def cpu(record):
    result = record["result"]
    return result["process_cpu_user_ms"] + result["process_cpu_system_ms"]


def compare(stem):
    pairs, provenance = load(stem)
    assert len(pairs) == 5, "expected five complete paired comparisons"
    arms = sorted(next(iter(pairs.values())))
    summary = {**provenance, "pairs": [], "median_puts_per_sec": {}}
    for arm in arms:
        summary["median_puts_per_sec"][arm] = median(throughput(p[arm]) for p in pairs.values())
    for (writers, pair), p in pairs.items():
        assert set(p) == {"old_v4", "new_v4", "old_v7", "new_v7"}
        summary["pairs"].append({
            "writers": writers, "pair": pair,
            "v7_throughput_change": throughput(p["new_v7"]) / throughput(p["old_v7"]),
            "normalized_v7_v4_change": (throughput(p["new_v7"]) / throughput(p["new_v4"])) / (throughput(p["old_v7"]) / throughput(p["old_v4"])),
            "v7_p50_change": p["new_v7"]["latency"]["p50_ms"] / p["old_v7"]["latency"]["p50_ms"],
            "v7_p99_change": p["new_v7"]["latency"]["p99_ms"] / p["old_v7"]["latency"]["p99_ms"],
            "v7_cpu_change": cpu(p["new_v7"]) / cpu(p["old_v7"]),
        })
    summary["median_changes"] = {
        key: median(pair[key] for pair in summary["pairs"])
        for key in ("v7_throughput_change", "normalized_v7_v4_change", "v7_p50_change", "v7_p99_change", "v7_cpu_change")
    }
    return summary


def sweep(stem):
    pairs, provenance = load(stem)
    summaries = []
    assert {key[0] for key in pairs} == {1, 2, 4, 8}
    for writers in sorted({key[0] for key in pairs}):
        cells = [p for key, p in pairs.items() if key[0] == writers]
        assert len(cells) == 3
        assert all(set(p) == {"v4", "v7"} for p in cells)
        summaries.append({
            "writers": writers,
            "v4_puts_per_sec": median(throughput(p["v4"]) for p in cells),
            "v7_puts_per_sec": median(throughput(p["v7"]) for p in cells),
            "paired_v7_v4_throughput": [throughput(p["v7"]) / throughput(p["v4"]) for p in cells],
            "v7_v4_p50": median(p["v7"]["latency"]["p50_ms"] / p["v4"]["latency"]["p50_ms"] for p in cells),
            "v7_v4_p99": median(p["v7"]["latency"]["p99_ms"] / p["v4"]["latency"]["p99_ms"] for p in cells),
            "v7_v4_cpu": median(cpu(p["v7"]) / cpu(p["v4"]) for p in cells),
        })
    return {**provenance, "writer_sweep": summaries}


if __name__ == "__main__":
    output = {}
    for experiment in ("coalesce-spin-tmpfs", "mvcc-preparation-tmpfs", "mvcc-preparation-ext4"):
        output[experiment] = compare("rfc-025-v7-" + experiment + "-8writers-20261025")
    output["mvcc-preparation-tmpfs-1writer"] = compare("rfc-025-v7-mvcc-preparation-tmpfs-1writer-20261025")
    output["writer_sweep"] = sweep("rfc-025-v7-mvcc-preparation-v4-parallel-tmpfs-1-2-4-8writers-20261025")
    print(json.dumps(output, indent=2))
