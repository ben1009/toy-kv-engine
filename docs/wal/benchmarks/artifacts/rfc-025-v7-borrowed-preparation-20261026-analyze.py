#!/usr/bin/env python3
"""Verify borrowed producer preparation and reproduce paired and peak results."""
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
    "1ec5ef47699236c329c1837981cb6f2fcd49cd522311ceb7e4e0352a83e2a31e",
    "614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9",
}


def compare(stem, batch=False, writers=8):
    summary = ANALYSIS["compare"](stem)
    assert set(summary["binary_sha256"]) == EXPECTED_BINARIES
    pairs, _ = ANALYSIS["load"](stem)
    with (ROOT / (stem + "-runs.tsv")).open() as stream:
        rows = list(csv.DictReader(stream, delimiter="\t"))
    raw = [json.loads(line) for line in (ROOT / (stem + ".jsonl")).read_bytes().splitlines()]
    records = {record["run_id"]: record for record in raw}
    assert len(rows) == 24
    assert len(raw) == len(records) == 24
    assert {row["run_id"] for row in rows} == set(records)
    assert sum(row["phase"] == "scored" for row in rows) == 20
    assert {row["arm"] for row in rows if row["phase"] == "warmup"} == {"old_v4", "new_v4", "old_v7", "new_v7"}
    expected_samples = 16 if batch else (500 if writers == 1 else 504)
    for row in rows:
        expected = (
            "614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9"
            if row["arm"].startswith("old") else
            "1ec5ef47699236c329c1837981cb6f2fcd49cd522311ceb7e4e0352a83e2a31e"
        )
        assert row["binary_sha256"] == expected
        assert row["phase"] in ("scored", "warmup")
        assert int(row["pair"]) in (range(1, 6) if row["phase"] == "scored" else [0])
        record = records[row["run_id"]]
        assert record["workload"] == ("wal_batch_concurrent" if batch else "wal_concurrent")
        assert record["params"]["threads"] == writers
        assert record["params"]["value_size"] == 1024
        assert record["params"]["batch_size"] == (64 if batch else None)
        assert record["result"]["ops"] == (100_000 if batch else 50_000)
        assert record["engine_options"]["target_sst_size"] == 1 << 30
        assert record["latency"]["samples"] == expected_samples
    summary["best_runs"] = {}
    summary["latency_samples_per_run"] = expected_samples
    for arm in ("old_v4", "new_v4", "old_v7", "new_v7"):
        records = [p[arm] for p in pairs.values()]
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
    datasets = sys.argv[1:] or ["tmpfs", "tmpfs-batch64", "tmpfs-single", "ext4", "ext4-batch64", "confirm-ext4-batch64"]
    output = {}
    for key in datasets:
        writers = 1 if key == "tmpfs-single" else 8
        suffix = "tmpfs-1writer" if writers == 1 else key + "-8writers"
        output[key] = compare("rfc-025-v7-borrowed-preparation-" + suffix + "-20261026", batch=key.endswith("batch64"), writers=writers)
    if "ext4" in output and "confirm-ext4" in output:
        cells = [output[key] for key in ("ext4", "confirm-ext4")]
        pairs = [pair for cell in cells for pair in cell["pairs"]]
        output["pooled_ext4_point"] = {
            "pair_count": len(pairs),
            "median_changes": {
                metric: statistics.median(pair[metric] for pair in pairs)
                for metric in cells[0]["median_changes"]
            },
            "best_puts_per_sec": {
                arm: max(cell["best_runs"][arm]["puts_per_sec"] for cell in cells)
                for arm in cells[0]["best_runs"]
            },
        }
    if "ext4-batch64" in output and "confirm-ext4-batch64" in output:
        cells = [output[key] for key in ("ext4-batch64", "confirm-ext4-batch64")]
        pairs = [pair for cell in cells for pair in cell["pairs"]]
        output["pooled_ext4_batch64"] = {
            "pair_count": len(pairs),
            "median_changes": {
                metric: statistics.median(pair[metric] for pair in pairs)
                for metric in cells[0]["median_changes"]
            },
            "best_puts_per_sec": {
                arm: max(cell["best_runs"][arm]["puts_per_sec"] for cell in cells)
                for arm in cells[0]["best_runs"]
            },
        }
    output["reference_peaks"] = {
        "v7_ext4_point": prior_peaks("rfc-025-v7-cached-extents-ext4-8writers-20261025", "new_v7"),
        "v4_ext4_point": prior_peaks("rfc-025-v7-small-extent-init-ext4-8writers-20261025", "new_v4"),
        "v7_ext4_batch64": prior_peaks("rfc-025-v7-cached-extents-ext4-batch64-8writers-20261025", "new_v7"),
        "v4_ext4_batch64": prior_peaks("rfc-025-v7-small-extent-init-ext4-batch64-8writers-20261025", "old_v4"),
    }
    output["reference_peaks"]["v7_ext4_point_initial_baseline"] = prior_peaks("rfc-025-v7-ticket-snapshot-ext4-8writers-20261025", "old_v7")
    output["reference_peaks"]["v7_ext4_batch64_initial_baseline"] = prior_peaks("rfc-025-v7-ticket-snapshot-ext4-batch64-8writers-20261025", "old_v7")
    output["reference_peaks"]["v7_ext4_point_coalescing_control"] = prior_peaks("rfc-025-v7-validated-logical-batch-ext4-8writers-20261025", "old_v7")
    output["highest_comparable_ext4_scores"] = {}
    for key, reference in (("ext4", "point"), ("ext4-batch64", "batch64")):
        if key not in output:
            continue
        best = output[key]["best_runs"]
        scores = output["pooled_ext4_point"]["best_puts_per_sec"] if key == "ext4" and "pooled_ext4_point" in output else {
            arm: record["puts_per_sec"] for arm, record in best.items()
        }
        if key == "ext4-batch64" and "pooled_ext4_batch64" in output:
            scores = output["pooled_ext4_batch64"]["best_puts_per_sec"]
        baseline = max(output["reference_peaks"]["v7_ext4_" + reference][0], output["reference_peaks"]["v7_ext4_" + reference + "_initial_baseline"][0], scores["old_v7"])
        v4 = max(output["reference_peaks"]["v4_ext4_" + reference][0], scores["old_v4"], scores["new_v4"])
        if key == "ext4":
            baseline = max(baseline, output["reference_peaks"]["v7_ext4_point_coalescing_control"][0])
        candidate = scores["new_v7"]
        output["highest_comparable_ext4_scores"][key] = {
            "retained_v7_high": baseline,
            "candidate_v7_high": candidate,
            "highest_v4": v4,
            "candidate_over_retained_v7": candidate / baseline,
            "candidate_over_highest_v4": candidate / v4,
        }
    if "pooled_ext4_batch64" in output:
        records = {arm: [] for arm in ("old_v7", "new_v7")}
        for key in ("ext4-batch64", "confirm-ext4-batch64"):
            pairs, _ = ANALYSIS["load"]("rfc-025-v7-borrowed-preparation-" + key + "-8writers-20261026")
            for arm in records:
                records[arm].extend(pair[arm] for pair in pairs.values())
        output["pooled_batch_pipeline_medians"] = {
            arm: {
                metric: statistics.median(record["result"]["wal_pipeline"][metric] for record in values)
                for metric in ("commit_groups", "commit_solo_groups", "sync_count", "frontier_writes", "worker_eventfd_writes")
            }
            for arm, values in records.items()
        }
    if "pooled_ext4_batch64" in output and "ext4" in output:
        batch = output["pooled_ext4_batch64"]["median_changes"]
        point = output["ext4"]["median_changes"]
        output["retention_guard"] = {
            "pooled_batch_raw_reaches_baseline": batch["v7_throughput_change"] >= 1,
            "pooled_batch_normalized_reaches_baseline": batch["normalized_v7_v4_change"] >= 1,
            "pooled_batch_cpu_does_not_increase": batch["v7_cpu_change"] <= 1,
            "point_raw_loss_at_most_five_percent": point["v7_throughput_change"] >= 0.95,
        }
        output["retention_guard"]["keep"] = all(output["retention_guard"].values())
    output["analysis_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(json.dumps(output, indent=2))
