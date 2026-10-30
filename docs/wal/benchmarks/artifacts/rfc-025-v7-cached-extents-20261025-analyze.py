#!/usr/bin/env python3
"""Verify extent-cache comparisons and reproduce paired and peak results."""
import csv
import hashlib
import json
import runpy
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ANALYSIS = runpy.run_path(str(ROOT / "rfc-025-v7-mvcc-preparation-20261025-analyze.py"), run_name="analysis")
EXPECTED_BINARIES = {
    "61d59ba0e739ec67338979a4a4a259e06d1940c1e92c49ff72cf38bdd32fb75e",
    "9a9a108ae23a3928ad88e42664ecc79c85a8e2d3f8ed5f81ef6aeb3b034e469e",
}


def compare(stem, batch=False, writers=8):
    summary = ANALYSIS["compare"](stem)
    assert set(summary["binary_sha256"]) == EXPECTED_BINARIES
    pairs, _ = ANALYSIS["load"](stem)
    summary["best_runs"] = {}
    summary["latency_samples_per_run"] = 16 if batch else (500 if writers == 1 else 504)
    for arm in ("old_v4", "new_v4", "old_v7", "new_v7"):
        records = [p[arm] for p in pairs.values()]
        for record in records:
            assert record["workload"] == ("wal_batch_concurrent" if batch else "wal_concurrent")
            assert record["params"]["threads"] == writers
            assert record["params"]["value_size"] == 1024
            assert record["params"]["batch_size"] == (64 if batch else None)
            assert record["result"]["ops"] == (100_000 if batch else 50_000)
            assert record["engine_options"]["target_sst_size"] == 1 << 30
            assert record["latency"]["samples"] == summary["latency_samples_per_run"]
        best = max(records, key=lambda r: r["result"]["ops_per_sec"])
        summary["best_runs"][arm] = {
            "puts_per_sec": best["result"]["ops_per_sec"],
            "run_id": best["run_id"],
            "p50_ms": best["latency"]["p50_ms"],
            "p99_ms": best["latency"]["p99_ms"],
            "cpu_ms": ANALYSIS["cpu"](best),
            "pipeline": best["result"]["wal_pipeline"],
        }
    summary["candidate_paired_v7_v4_ratio"] = statistics.median(
        p["new_v7"]["result"]["ops_per_sec"] / p["new_v4"]["result"]["ops_per_sec"] for p in pairs.values()
    )
    if "-ext4-" in stem:
        with (ROOT / (stem + "-runs.tsv")).open() as stream:
            temperatures = [int(row["nvme_temperature_millic"]) / 1000 for row in csv.DictReader(stream, delimiter="\t")]
        summary["nvme_temperature_c_range_all_runs"] = [min(temperatures), max(temperatures)]
    return summary


def prior_peaks(stem, arm):
    pairs, provenance = ANALYSIS["load"](stem)
    return max(p[arm]["result"]["ops_per_sec"] for p in pairs.values()), provenance


if __name__ == "__main__":
    datasets = sys.argv[1:] or ["tmpfs", "ext4", "tmpfs-batch64", "tmpfs-single", "ext4-batch64"]
    output = {}
    for key in datasets:
        writers = 1 if key == "tmpfs-single" else 8
        suffix = "tmpfs-1writer" if writers == 1 else key + "-8writers"
        output[key] = compare("rfc-025-v7-cached-extents-" + suffix + "-20261025", batch=key.endswith("batch64"), writers=writers)
    output["reference_peaks"] = {
        "v7_ext4_point": prior_peaks("rfc-025-v7-packer-encoding-ext4-8writers-20261025", "new_v7"),
        "v4_ext4_point": prior_peaks("rfc-025-v7-small-extent-init-ext4-8writers-20261025", "new_v4"),
        "v7_ext4_batch64": prior_peaks("rfc-025-v7-packer-encoding-ext4-batch64-8writers-20261025", "new_v7"),
        "v4_ext4_batch64": prior_peaks("rfc-025-v7-small-extent-init-ext4-batch64-8writers-20261025", "old_v4"),
    }
    output["highest_comparable_ext4_scores"] = {}
    for key, reference in (("ext4", "point"), ("ext4-batch64", "batch64")):
        if key not in output:
            continue
        best = output[key]["best_runs"]
        baseline = max(output["reference_peaks"]["v7_ext4_" + reference][0], best["old_v7"]["puts_per_sec"])
        v4 = max(output["reference_peaks"]["v4_ext4_" + reference][0], best["old_v4"]["puts_per_sec"], best["new_v4"]["puts_per_sec"])
        candidate = best["new_v7"]["puts_per_sec"]
        output["highest_comparable_ext4_scores"][key] = {
            "retained_v7_high": baseline,
            "candidate_v7_high": candidate,
            "highest_v4": v4,
            "candidate_over_retained_v7": candidate / baseline,
            "candidate_over_highest_v4": candidate / v4,
        }
    output["analysis_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(json.dumps(output, indent=2))
