#!/usr/bin/env python3
"""Compare shared extent initialization, retaining stalls and reporting peaks."""
import csv
import hashlib
import json
import runpy
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = "rfc-025-v7-shared-extent-init-ext4-8writers-20261025"
ANALYSIS = runpy.run_path(
    str(ROOT / "rfc-025-v7-mvcc-preparation-20261025-analyze.py"),
    run_name="analysis",
)


def experiment(stem):
    summary = ANALYSIS["compare"](stem)
    pairs, _ = ANALYSIS["load"](stem)
    is_batch = "batch64" in stem
    for pair in pairs.values():
        for record in pair.values():
            assert record["workload"] == ("wal_batch_concurrent" if is_batch else "wal_concurrent")
            assert record["params"]["threads"] == 8
            assert record["params"]["value_size"] == 1024
            assert record["params"]["batch_size"] == (64 if is_batch else None)
            assert record["result"]["ops"] == (100_000 if is_batch else 50_000)
            assert record["engine_options"]["target_sst_size"] == 1 << 30
    summary["best_puts_per_sec"] = {
        arm: max(p[arm]["result"]["ops_per_sec"] for p in pairs.values())
        for arm in ("old_v4", "new_v4", "old_v7", "new_v7")
    }
    summary["best_run_ids"] = {
        arm: max(
            (p[arm] for p in pairs.values()), key=lambda r: r["result"]["ops_per_sec"]
        )["run_id"]
        for arm in ("old_v4", "new_v4", "old_v7", "new_v7")
    }
    summary["candidate_paired_v7_over_v4"] = statistics.median(
        p["new_v7"]["result"]["ops_per_sec"] / p["new_v4"]["result"]["ops_per_sec"]
        for p in pairs.values()
    )
    summary["peak_v7_change"] = (
        summary["best_puts_per_sec"]["new_v7"] / summary["best_puts_per_sec"]["old_v7"]
    )
    summary["peak_candidate_v7_over_v4"] = (
        summary["best_puts_per_sec"]["new_v7"] / summary["best_puts_per_sec"]["new_v4"]
    )
    return summary


def historical_best(stem, arm, expected_version):
    """Pin the earlier peak's dataset, arm, settings, and source-record hash."""
    pairs, provenance = ANALYSIS["load"](stem)
    candidates = [p[arm] for p in pairs.values() if arm in p]
    best = max(candidates, key=lambda r: r["result"]["ops_per_sec"])
    assert best["workload"] == "wal_concurrent"
    assert best["params"]["wal_format_version"] == expected_version
    assert best["params"]["threads"] == 8
    assert best["params"]["value_size"] == 1024
    assert best["result"]["ops"] == 50_000
    assert best["engine_options"]["target_sst_size"] == 1 << 30
    rows = list(csv.DictReader((ROOT / (stem + "-runs.tsv")).open(), delimiter="\t"))
    row = next(r for r in rows if r["run_id"] == str(best["run_id"]))
    assert row["filesystem"] == "ext4" and row["phase"] == "scored"
    return {
        "puts_per_sec": best["result"]["ops_per_sec"],
        "source_record": row["source_record"],
        "binary_sha256": row["binary_sha256"],
        "raw_sha256": row["raw_sha256"],
        "dataset": stem,
        "provenance": provenance,
    }


if __name__ == "__main__":
    retained_base = BASE.replace("shared-extent-init", "small-extent-init")
    point = experiment(retained_base)
    expected_binaries = {
        "b5078356212a72389b2920cd55f9dbfcbc6f69c52b10e2b09cd0db857ec1ef0b",
        "06efc8d7dc473ddfd770832c9075c6cfffdc9ab05df13460481d4f2302f2625b",
    }
    assert set(point["binary_sha256"]) == expected_binaries
    batch = experiment(retained_base.replace("ext4-8writers", "ext4-batch64-8writers"))
    assert set(batch["binary_sha256"]) == expected_binaries
    previous_v7 = historical_best(
        "rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-rerun1", "new_v7", 7
    )
    previous_v4 = historical_best(
        "rfc-025-v7-ahash-v4-parallel-ext4-1-2-4-8writers-20261025", "v4", 4
    )
    peak = point["best_puts_per_sec"]["new_v7"]
    best_v4 = max(
        previous_v4["puts_per_sec"],
        point["best_puts_per_sec"]["new_v4"],
        point["best_puts_per_sec"]["old_v4"],
    )
    output = {
        "point_writes": point,
        "batch64": batch,
        "unconditional_initialization": {
            "point_writes": experiment(BASE),
            "batch64": experiment(BASE.replace("ext4-8writers", "ext4-batch64-8writers")),
        },
        "historical_peak_comparison": {
            "previous_v4": previous_v4,
            "previous_v7": previous_v7,
            "new_v7_puts_per_sec": peak,
            "new_v7_over_previous_best_v7": peak / previous_v7["puts_per_sec"],
            "new_v7_over_historical_v4": peak / previous_v4["puts_per_sec"],
            "best_observed_v4_puts_per_sec": best_v4,
            "new_v7_over_best_v4": peak / best_v4,
        },
        "analysis_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    print(json.dumps(output, indent=2))
