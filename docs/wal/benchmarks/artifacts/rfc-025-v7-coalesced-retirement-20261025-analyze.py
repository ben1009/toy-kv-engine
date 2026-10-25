#!/usr/bin/env python3
"""Verify paired resource-retirement benchmarks and compare highest scores."""
import hashlib
import json
import runpy
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = "rfc-025-v7-coalesced-retirement-"
ANALYSIS = runpy.run_path(
    str(ROOT / "rfc-025-v7-mvcc-preparation-20261025-analyze.py"), run_name="analysis"
)
EXPECTED_BINARIES = {
    "06efc8d7dc473ddfd770832c9075c6cfffdc9ab05df13460481d4f2302f2625b",
    "34c3447f08d7ef8b05bdc037b1134318b5bf185d0141582c6b4191308cfda576",
}


def compare(stem, batch=False):
    summary = ANALYSIS["compare"](stem)
    assert set(summary["binary_sha256"]) == EXPECTED_BINARIES
    pairs, _ = ANALYSIS["load"](stem)
    summary["best_runs"] = {}
    for arm in ("old_v4", "new_v4", "old_v7", "new_v7"):
        records = [p[arm] for p in pairs.values()]
        for record in records:
            assert record["workload"] == ("wal_batch_concurrent" if batch else "wal_concurrent")
            assert record["params"]["threads"] == 8
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
        p["new_v7"]["result"]["ops_per_sec"] / p["new_v4"]["result"]["ops_per_sec"]
        for p in pairs.values()
    )
    return summary


if __name__ == "__main__":
    tmpfs = compare(BASE + "tmpfs-8writers-20261025")
    tmpfs_rerun = compare(BASE + "tmpfs-8writers-20261025-rerun1")
    ext4 = compare(BASE + "ext4-8writers-20261025")
    batch64 = compare(BASE + "ext4-batch64-8writers-20261025", batch=True)
    batch64_rerun = compare(BASE + "ext4-batch64-8writers-20261025-rerun1", batch=True)
    previous, provenance = ANALYSIS["load"](
        "rfc-025-v7-small-extent-init-ext4-8writers-20261025"
    )
    previous_v7 = max(p["new_v7"]["result"]["ops_per_sec"] for p in previous.values())
    previous_v4 = max(p["new_v4"]["result"]["ops_per_sec"] for p in previous.values())
    best_v7_baseline = max(previous_v7, ext4["best_runs"]["old_v7"]["puts_per_sec"])
    best_v4 = max(
        previous_v4,
        ext4["best_runs"]["old_v4"]["puts_per_sec"],
        ext4["best_runs"]["new_v4"]["puts_per_sec"],
    )
    peak = ext4["best_runs"]["new_v7"]["puts_per_sec"]
    profile = "rfc-025-v7-small-extent-init-cpu-profile-20261025"
    output = {
        "outcome": "rejected",
        "tmpfs": tmpfs,
        "tmpfs_rerun": tmpfs_rerun,
        "combined_tmpfs_normalized_change": statistics.median(
            p["normalized_v7_v4_change"] for p in tmpfs["pairs"] + tmpfs_rerun["pairs"]
        ),
        "combined_tmpfs_cpu_change": statistics.median(
            p["v7_cpu_change"] for p in tmpfs["pairs"] + tmpfs_rerun["pairs"]
        ),
        "ext4": ext4,
        "batch64": batch64,
        "batch64_rerun": batch64_rerun,
        "combined_batch64_normalized_change": statistics.median(
            p["normalized_v7_v4_change"] for p in batch64["pairs"] + batch64_rerun["pairs"]
        ),
        "combined_batch64_raw_change": statistics.median(
            p["v7_throughput_change"] for p in batch64["pairs"] + batch64_rerun["pairs"]
        ),
        "highest_comparable_batch64_scores": {
            "baseline_v7_puts_per_sec": max(
                s["best_runs"]["old_v7"]["puts_per_sec"] for s in (batch64, batch64_rerun)
            ),
            "candidate_v7_puts_per_sec": max(
                s["best_runs"]["new_v7"]["puts_per_sec"] for s in (batch64, batch64_rerun)
            ),
        },
        "highest_comparable_ext4_scores": {
            "previous_dataset": provenance,
            "previous_retained_v7_puts_per_sec": best_v7_baseline,
            "candidate_v7_puts_per_sec": peak,
            "best_v4_puts_per_sec": best_v4,
            "candidate_v7_over_best_baseline_v7": peak / best_v7_baseline,
            "candidate_v7_over_best_v4": peak / best_v4,
        },
        "diagnostic_profile_artifact_sha256": {
            suffix: hashlib.sha256((ROOT / (profile + suffix)).read_bytes()).hexdigest()
            for suffix in (".jsonl", "-runs.tsv", "-v4.txt", "-v7.txt")
        },
        "analysis_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    print(json.dumps(output, indent=2))
