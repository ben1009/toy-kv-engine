#!/usr/bin/env python3
"""Run a same-binary V8 Point direct-frame A/B with Batch guardrails."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from statistics import median
from typing import Any

from gcp_v8_point_borrowed_input_runner import _run_one, _summary_fields
from gcp_wal_matrix import (
    POINT_DIRECT_FRAME_WRITERS,
    build_point_direct_frame_plan,
    build_point_direct_frame_schedule,
    validate_point_direct_frame_rows,
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _summary(rows: list[dict[str, Any]], writers: tuple[int, ...], planned: int) -> dict[str, Any]:
    validation = validate_point_direct_frame_rows(rows, writers)
    cells = [(writer, "point") for writer in writers] + [(16, "batch16"), (16, "batch64")]
    comparison: list[dict[str, Any]] = []
    for writer, workload in cells:
        selected = {
            arm: [
                row for row in rows
                if row["writer"] == writer and row["workload"] == workload and row["arm"] == arm
                and row["exit_code"] == 0
            ]
            for arm in ("direct_frame_off", "direct_frame_on")
        }
        metrics = (
            "puts_per_second", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms", "sync_count",
            "puts_per_sync", "initialized_bytes", "preparation_wait_ms", "direct_frame_encoded_count",
        )
        stats = {
            arm: {
                "runs": len(arm_rows),
                **{
                    metric: median(float(row[metric]) for row in arm_rows) if arm_rows else None
                    for metric in metrics
                },
            }
            for arm, arm_rows in selected.items()
        }
        paired_changes: list[float] = []
        for repetition in range(1, 6):
            pair = {
                arm: next(
                    (
                        row for row in rows
                        if row["writer"] == writer and row["workload"] == workload
                        and row["repetition"] == repetition and row["arm"] == arm
                        and row["exit_code"] == 0
                    ),
                    None,
                )
                for arm in ("direct_frame_off", "direct_frame_on")
            }
            baseline = pair["direct_frame_off"]
            candidate = pair["direct_frame_on"]
            if baseline is not None and candidate is not None and baseline["puts_per_second"]:
                paired_changes.append((candidate["puts_per_second"] / baseline["puts_per_second"] - 1) * 100)
        baseline_median = stats["direct_frame_off"]["puts_per_second"]
        candidate_median = stats["direct_frame_on"]["puts_per_second"]
        comparison.append({
            "writer": writer,
            "workload": workload,
            "baseline": stats["direct_frame_off"],
            "candidate": stats["direct_frame_on"],
            "median_throughput_change_pct": (
                (candidate_median / baseline_median - 1) * 100
                if baseline_median and candidate_median else None
            ),
            "paired_throughput_changes_pct": paired_changes,
            "candidate_pair_wins": sum(change > 0 for change in paired_changes),
        })
    return {
        "suite": "v8-point-direct-frame",
        "complete": len(rows) == planned and validation["complete"],
        "runs_completed": len(rows),
        "runs_planned": planned,
        "writers": list(writers),
        "workloads": ["point", "batch16", "batch64"],
        "arms": ["direct_frame_off", "direct_frame_on"],
        "results": rows,
        "matrix_validation": validation,
        "comparison": comparison,
        "qualification_note": (
            "Same-host, same-source, same-binary A/B. Only TOYKV_BENCH_V8_POINT_DIRECT_FRAME changes. "
            "The runtime counter must equal Point ops in the enabled arm and remain zero in all other runs."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        raise ValueError("commit must be a full lowercase 40-character SHA")
    if not re.fullmatch(r"[0-9a-f]{64}", args.binary_sha256):
        raise ValueError("binary SHA-256 must be 64 lowercase hex characters")
    actual_sha = _sha256_file(args.binary)
    if actual_sha != args.binary_sha256:
        raise ValueError("write-perf binary SHA-256 does not match the supplied identity")
    if args.machine_type != "c4-standard-16-lssd" or args.ssd_count != 2 or args.ssd_gib != 750:
        raise ValueError("direct-frame qualification requires c4-standard-16-lssd and its two-SSD layout")

    results_dir = args.results_dir
    db_root = args.db_root
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)
    writers = tuple(int(value) for value in args.writers.split(","))
    if writers != POINT_DIRECT_FRAME_WRITERS:
        raise ValueError(f"direct-frame qualification requires writers {POINT_DIRECT_FRAME_WRITERS}")

    machine_spec = {"vcpus": 16, "ssd_count": args.ssd_count, "ssd_gib": args.ssd_gib}
    schedule = build_point_direct_frame_schedule(writers)
    plan = build_point_direct_frame_plan(
        writers=writers,
        machine_type=args.machine_type,
        machine_spec=machine_spec,
        commit=args.commit,
        binary_sha256=args.binary_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    (results_dir / "commit.txt").write_text(args.commit + "\n", encoding="utf-8")
    (results_dir / "write-perf.sha256").write_text(args.binary_sha256 + "\n", encoding="utf-8")
    with (results_dir / "point-direct-frame-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows({**row, "batch_size": row["batch_size"] or "none"} for row in schedule)

    fields = _summary_fields(schedule[0])
    with (results_dir / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
        csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n").writeheader()

    completed: list[dict[str, Any]] = []
    failed = False
    for index, row in enumerate(schedule, start=1):
        try:
            metadata = _run_one(
                binary=args.binary,
                binary_sha256=args.binary_sha256,
                row={**row, "source_commit": args.commit},
                results_dir=results_dir,
                db_root=db_root,
                safe_deadline_epoch=args.safe_deadline_epoch,
                verify_direct_frame=True,
            )
        except (OSError, TimeoutError, subprocess.SubprocessError) as error:
            failed = True
            (results_dir / "runner-error.txt").write_text(
                f"run {index} did not finish: {error}\n", encoding="utf-8",
            )
            break
        completed.append(metadata)
        with (results_dir / "summary.tsv").open("a", encoding="utf-8", newline="") as stream:
            csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n").writerow(metadata)
        if metadata["exit_code"] != 0:
            failed = True
            break

    summary = _summary(completed, writers, len(schedule))
    summary.update({"source_commit": args.commit, "binary_sha256": args.binary_sha256})
    summary["complete"] = summary["complete"] and not failed
    (results_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (results_dir / "remote-exit.json").write_text(
        json.dumps({"failed": failed or len(completed) != len(schedule), "runs_started": len(completed)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    (results_dir / "finished_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    return 1 if failed or len(completed) != len(schedule) else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--writers", required=True)
    parser.add_argument("--safe-deadline-epoch", type=int, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--machine-type", required=True)
    parser.add_argument("--ssd-count", type=int, required=True)
    parser.add_argument("--ssd-gib", type=int, required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run_suite(parse_args()))
