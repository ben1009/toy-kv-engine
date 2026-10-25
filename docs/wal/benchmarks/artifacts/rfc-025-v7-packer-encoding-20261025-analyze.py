#!/usr/bin/env python3
"""Verify deferred-encoding comparisons and reproduce paired and peak results."""
import hashlib
import json
import runpy
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ANALYSIS = runpy.run_path(str(ROOT / "rfc-025-v7-mvcc-preparation-20261025-analyze.py"), run_name="analysis")
EXPECTED_BINARIES = {
    "06efc8d7dc473ddfd770832c9075c6cfffdc9ab05df13460481d4f2302f2625b",
    "9a9a108ae23a3928ad88e42664ecc79c85a8e2d3f8ed5f81ef6aeb3b034e469e",
}


def compare(stem, batch=False, writers=8):
    summary = ANALYSIS["compare"](stem)
    assert set(summary["binary_sha256"]) == EXPECTED_BINARIES
    pairs, _ = ANALYSIS["load"](stem)
    summary["best_runs"] = {}
    for arm in ("old_v4", "new_v4", "old_v7", "new_v7"):
        records = [p[arm] for p in pairs.values()]
        for record in records:
            assert record["workload"] == ("wal_batch_concurrent" if batch else "wal_concurrent")
            assert record["params"]["threads"] == writers
            assert record["params"]["value_size"] == 1024
            assert record["params"]["batch_size"] == (64 if batch else None)
            assert record["result"]["ops"] == (100_000 if batch else 50_000)
            assert record["engine_options"]["target_sst_size"] == 1 << 30
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
    return summary


def prior_peaks(stem, arm):
    pairs, provenance = ANALYSIS["load"](stem)
    return max(p[arm]["result"]["ops_per_sec"] for p in pairs.values()), provenance


if __name__ == "__main__":
    output = {
        key: compare("rfc-025-v7-packer-encoding-" + key + "-8writers-20261025", batch=key.endswith("batch64"))
        for key in ("tmpfs", "ext4", "ext4-batch64", "tmpfs-batch64")
    }
    output["tmpfs-single"] = compare("rfc-025-v7-packer-encoding-tmpfs-1writer-20261025", writers=1)
    output["tmpfs-single-rerun1"] = compare(
        "rfc-025-v7-packer-encoding-tmpfs-1writer-20261025-rerun1", writers=1
    )
    single_pairs = output["tmpfs-single"]["pairs"] + output["tmpfs-single-rerun1"]["pairs"]
    output["tmpfs-single-pooled"] = {
        "pair_count": len(single_pairs),
        "median_changes": {
            key: statistics.median(pair[key] for pair in single_pairs)
            for key in output["tmpfs-single"]["median_changes"]
        },
    }
    previous = "rfc-025-v7-small-extent-init-ext4-8writers-20261025"
    previous_v4, previous_provenance = prior_peaks(previous, "new_v4")
    previous_v7, _ = prior_peaks(previous, "new_v7")
    ext4 = output["ext4"]
    baseline = max(previous_v7, ext4["best_runs"]["old_v7"]["puts_per_sec"])
    v4 = max(previous_v4, *(ext4["best_runs"][a]["puts_per_sec"] for a in ("old_v4", "new_v4")))
    candidate = ext4["best_runs"]["new_v7"]["puts_per_sec"]
    output["highest_comparable_ext4_scores"] = {
        "prior_provenance": previous_provenance,
        "baseline_v7_puts_per_sec": baseline,
        "candidate_v7_puts_per_sec": candidate,
        "best_v4_puts_per_sec": v4,
        "candidate_over_baseline_v7": candidate / baseline,
        "candidate_over_best_v4": candidate / v4,
    }
    previous_batch_v4, batch_v4_provenance = prior_peaks(
        "rfc-025-v7-small-extent-init-ext4-batch64-8writers-20261025", "old_v4"
    )
    previous_batch_v7, batch_v7_provenance = prior_peaks(
        "rfc-025-v7-coalesced-retirement-ext4-batch64-8writers-20261025-rerun1", "old_v7"
    )
    batch = output["ext4-batch64"]["best_runs"]
    baseline = max(previous_batch_v7, batch["old_v7"]["puts_per_sec"])
    v4 = max(previous_batch_v4, *(batch[a]["puts_per_sec"] for a in ("old_v4", "new_v4")))
    candidate = batch["new_v7"]["puts_per_sec"]
    output["highest_comparable_ext4_batch64_scores"] = {
        "prior_v4_provenance": batch_v4_provenance,
        "prior_v7_provenance": batch_v7_provenance,
        "baseline_v7_puts_per_sec": baseline,
        "candidate_v7_puts_per_sec": candidate,
        "best_v4_puts_per_sec": v4,
        "candidate_over_baseline_v7": candidate / baseline,
        "candidate_over_best_v4": candidate / v4,
    }
    output["analysis_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(json.dumps(output, indent=2))
