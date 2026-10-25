#!/usr/bin/env python3
"""Reproduce the user-requested batch rerun without replacing earlier evidence."""
import csv
import hashlib
import json
import runpy
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COMMON = runpy.run_path(str(ROOT / "rfc-025-v7-maintenance-snapshot-20261025-analyze.py"), run_name="analysis")
PREFIX = "rfc-025-v7-maintenance-snapshot-"
RERUN = PREFIX + "rerun-ext4-batch64-8writers-20261026"
PREVIOUS = [PREFIX + suffix + "-8writers-20261025" for suffix in ("ext4-batch64", "confirm-ext4-batch64")]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    rerun = COMMON["compare"](RERUN, batch=True)
    cells = [COMMON["compare"](stem, batch=True) for stem in PREVIOUS] + [rerun]
    pairs = [pair for cell in cells for pair in cell["pairs"]]
    ids = []
    for stem in PREVIOUS + [RERUN]:
        with (ROOT / (stem + "-runs.tsv")).open() as stream:
            ids.extend(row["run_id"] for row in csv.DictReader(stream, delimiter="\t"))
    assert len(ids) == len(set(ids)) == 72
    rerun_pairs, _ = COMMON["ANALYSIS"]["load"](RERUN)
    arms = ("old_v4", "new_v4", "old_v7", "new_v7")
    output = {
        "rerun": rerun,
        "rerun_per_arm": {
            arm: {
                "median_puts_per_sec": statistics.median(p[arm]["result"]["ops_per_sec"] for p in rerun_pairs.values()),
                "peak_puts_per_sec": rerun["best_runs"][arm]["puts_per_sec"],
                "median_p50_ms": statistics.median(p[arm]["latency"]["p50_ms"] for p in rerun_pairs.values()),
                "median_p99_ms": statistics.median(p[arm]["latency"]["p99_ms"] for p in rerun_pairs.values()),
                "median_cpu_ms": statistics.median(COMMON["ANALYSIS"]["cpu"](p[arm]) for p in rerun_pairs.values()),
            }
            for arm in arms
        },
        "rerun_v7_v4_paired_throughput": {
            label: statistics.median(p[prefix + "_v7"]["result"]["ops_per_sec"] / p[prefix + "_v4"]["result"]["ops_per_sec"] for p in rerun_pairs.values())
            for label, prefix in (("retained", "old"), ("prototype", "new"))
        },
        "pooled_all_15_batch_pairs": {
            "stems": PREVIOUS + [RERUN],
            "pair_count": len(pairs),
            "median_changes": {
                metric: statistics.median(pair[metric] for pair in pairs)
                for metric in cells[0]["median_changes"]
            },
            "pairs_below_baseline": {
                metric: sum(pair[metric] < 1 for pair in pairs)
                for metric in ("v7_throughput_change", "normalized_v7_v4_change")
            },
        },
        "prior_highest_comparable_batch_scores": {
            "v4_parallel": COMMON["prior_peaks"]("rfc-025-v7-small-extent-init-ext4-batch64-8writers-20261025", "old_v4")[0],
            "retained_v7_parallel": COMMON["prior_peaks"]("rfc-025-v7-cached-extents-ext4-batch64-8writers-20261025", "new_v7")[0],
        },
        "analysis_sha256": digest(Path(__file__)),
        "analysis_dependencies_sha256": {
            name: digest(ROOT / name)
            for name in ("rfc-025-v7-maintenance-snapshot-20261025-analyze.py", "rfc-025-v7-mvcc-preparation-20261025-analyze.py")
        },
    }
    assert len(pairs) == 15
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
