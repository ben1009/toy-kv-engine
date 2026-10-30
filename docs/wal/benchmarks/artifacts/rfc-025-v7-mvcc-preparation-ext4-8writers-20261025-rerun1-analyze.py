#!/usr/bin/env python3
"""Verify and compare the original ext4 measurements with the complete rerun."""
import json
import runpy
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = "rfc-025-v7-mvcc-preparation-ext4-8writers-20261025"
analysis = runpy.run_path(str(ROOT / "rfc-025-v7-mvcc-preparation-20261025-analyze.py"))
previous = analysis["compare"](BASE)
rerun = analysis["compare"](BASE + "-rerun1")
assert rerun["binary_sha256"] == previous["binary_sha256"], "rerun binaries changed"
pairs, _ = analysis["load"](BASE + "-rerun1")
rerun["median_latency_ms"] = {
    arm: {
        metric: statistics.median(pair[arm]["latency"][metric] for pair in pairs.values())
        for metric in ("p50_ms", "p99_ms", "max_ms")
    }
    for arm in ("old_v4", "new_v4", "old_v7", "new_v7")
}
rerun["candidate_v7_over_v4"] = {
    "throughput": statistics.median(
        analysis["throughput"](pair["new_v7"]) / analysis["throughput"](pair["new_v4"])
        for pair in pairs.values()
    ),
    "p50": statistics.median(
        pair["new_v7"]["latency"]["p50_ms"] / pair["new_v4"]["latency"]["p50_ms"]
        for pair in pairs.values()
    ),
    "p99": statistics.median(
        pair["new_v7"]["latency"]["p99_ms"] / pair["new_v4"]["latency"]["p99_ms"]
        for pair in pairs.values()
    ),
}
combined = previous["pairs"] + rerun["pairs"]
output = {
    "previous": previous,
    "rerun": rerun,
    "combined_ten_pair_normalized_v7_v4_change": statistics.median(
        pair["normalized_v7_v4_change"] for pair in combined
    ),
}
print(json.dumps(output, indent=2))
