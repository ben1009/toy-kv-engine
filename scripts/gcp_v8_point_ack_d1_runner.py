#!/usr/bin/env python3
"""Run and verify the same-host V8 Point D1 Coordinator ACK source A/B."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from statistics import median
from typing import Any

from gcp_v8_point_progress_ring_runner import (
    _expected_environment_text,
    _run_one,
    _summary_fields,
    _validate_point_resource_retirement_stage_a_record,
)
from gcp_wal_matrix import (
    POINT_ACK_D1_ARMS,
    POINT_ACK_D1_WRITERS,
    build_point_ack_d1_plan,
    build_point_ack_d1_schedule,
    validate_point_ack_d1_rows,
)


def _run_id(row: dict[str, Any]) -> str:
    return f"r{row['repetition']}-w{row['writer']}-{row['workload']}-{row['arm']}"


def _summary(rows: list[dict[str, Any]], planned: int) -> dict[str, Any]:
    validation = validate_point_ack_d1_rows(rows)
    metrics = (
        "puts_per_second", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms",
        "sync_count", "puts_per_sync", "point_buffer_preallocation_hits",
        "point_buffer_preallocation_fallbacks",
    )
    comparisons: list[dict[str, Any]] = []
    for writer, workload in [*((writer, "point") for writer in POINT_ACK_D1_WRITERS), (16, "batch64")]:
        arm_rows = {
            arm: [
                row for row in rows
                if row.get("writer") == writer and row.get("workload") == workload
                and row.get("arm") == arm and row.get("exit_code") == 0
            ]
            for arm in POINT_ACK_D1_ARMS
        }
        stats = {
            arm: {
                "runs": len(selected),
                **{
                    metric: median(float(row[metric]) for row in selected)
                    if selected and all(row.get(metric) is not None for row in selected) else None
                    for metric in metrics
                },
            }
            for arm, selected in arm_rows.items()
        }
        paired: list[dict[str, float | int]] = []
        for repetition in range(1, 6):
            before = next((row for row in arm_rows["d1_before"] if row.get("repetition") == repetition), None)
            after = next((row for row in arm_rows["d1_after"] if row.get("repetition") == repetition), None)
            if before is None or after is None or float(before["puts_per_second"]) <= 0:
                continue
            paired.append({
                "repetition": repetition,
                "throughput_change_pct": (float(after["puts_per_second"]) / float(before["puts_per_second"]) - 1) * 100,
                "p99_change_pct": (
                    (float(after["p99_ms"]) / float(before["p99_ms"]) - 1) * 100
                    if workload == "point" and float(before["p99_ms"]) > 0 else 0.0
                ),
            })
        comparisons.append({
            "writer": writer,
            "workload": workload,
            "arms": stats,
            "paired_throughput_changes_pct": [item["throughput_change_pct"] for item in paired],
            "paired_throughput_change_median_pct": median(
                item["throughput_change_pct"] for item in paired
            ) if paired else None,
            "d1_pair_wins": sum(item["throughput_change_pct"] > 0 for item in paired),
            "paired_p99_changes_pct": [item["p99_change_pct"] for item in paired] if workload == "point" else [],
            "paired_p99_change_median_pct": median(
                item["p99_change_pct"] for item in paired
            ) if workload == "point" and paired else None,
        })
    return {
        "suite": "v8-point-ack-d1",
        "complete": len(rows) == planned == validation["planned"] and validation["complete"],
        "runs_started": len(rows),
        "runs_completed": sum(row.get("exit_code") == 0 for row in rows),
        "runs_planned": planned,
        "writers": list(POINT_ACK_D1_WRITERS),
        "workloads": ["point", "batch64"],
        "arms": list(POINT_ACK_D1_ARMS),
        "results": rows,
        "matrix_validation": validation,
        "comparison": comparisons,
        "qualification_note": (
            "Same-host, same-toolchain source A/B of D1's consolidated Coordinator ACK checks. Both arms use "
            "Stage A Point buffer preallocation and resource-retirement mode 0. No profiling is enabled."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    schedule = build_point_ack_d1_schedule()
    writers = tuple(int(value) for value in args.writers.split(","))
    if writers != POINT_ACK_D1_WRITERS:
        raise ValueError(f"D1 qualification requires writers {POINT_ACK_D1_WRITERS}")
    if args.machine_type != "c4-standard-16-lssd" or args.ssd_count != 2 or args.ssd_gib != 750:
        raise ValueError("D1 qualification requires c4-standard-16-lssd and two Local SSDs")
    binaries = {
        "d1_before": (args.baseline_binary, args.baseline_binary_sha256, args.baseline_commit),
        "d1_after": (args.candidate_binary, args.candidate_binary_sha256, args.candidate_commit),
    }
    for binary, expected_sha, commit in binaries.values():
        if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
            raise ValueError("source commits must be full lowercase SHA-1 identities")
        if len(expected_sha) != 64 or any(character not in "0123456789abcdef" for character in expected_sha):
            raise ValueError("binary checksums must be full lowercase SHA-256 identities")
        digest = hashlib.sha256(binary.read_bytes()).hexdigest()
        if digest != expected_sha:
            raise ValueError(f"binary checksum mismatch for {binary}")

    results_dir, db_root = args.results_dir, args.db_root
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)
    plan = build_point_ack_d1_plan(
        machine_type=args.machine_type,
        machine_spec={"vcpus": 16, "ssd_count": args.ssd_count, "ssd_gib": args.ssd_gib},
        baseline_commit=args.baseline_commit,
        candidate_commit=args.candidate_commit,
        source_patch_sha256=args.source_patch_sha256,
        baseline_binary_sha256=args.baseline_binary_sha256,
        candidate_binary_sha256=args.candidate_binary_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with (results_dir / "point-ack-d1-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows({**row, "batch_size": row["batch_size"] or "none"} for row in schedule)
    (results_dir / "baseline-commit.txt").write_text(args.baseline_commit + "\n", encoding="utf-8")
    (results_dir / "candidate-commit.txt").write_text(args.candidate_commit + "\n", encoding="utf-8")
    (results_dir / "source-patch.sha256").write_text(args.source_patch_sha256 + "\n", encoding="utf-8")
    (results_dir / "baseline-binary.sha256").write_text(args.baseline_binary_sha256 + "\n", encoding="utf-8")
    (results_dir / "candidate-binary.sha256").write_text(args.candidate_binary_sha256 + "\n", encoding="utf-8")
    fields = _summary_fields(schedule)
    (results_dir / "summary.tsv").write_text("\t".join(fields) + "\n", encoding="utf-8")

    rows: list[dict[str, Any]] = []
    failure: str | None = None
    attempted = started = 0
    for row in schedule:
        attempted += 1
        binary, binary_sha, commit = binaries[str(row["arm"])]

        def record_start() -> None:
            nonlocal started
            started += 1

        try:
            metadata = _run_one(
                binary=binary,
                binary_sha256=binary_sha,
                row={**row, "source_commit": commit},
                results_dir=results_dir,
                db_root=db_root,
                safe_deadline_epoch=args.safe_deadline_epoch,
                candidate_mode="point-resource-retirement-stage-a",
                on_subprocess_started=record_start,
            )
        except (OSError, TimeoutError, subprocess.SubprocessError, ValueError) as error:
            failure = f"run {attempted} did not complete: {error}"
            (results_dir / "runner-error.txt").write_text(failure + "\n", encoding="utf-8")
            break
        rows.append(metadata)
        with (results_dir / "summary.tsv").open("a", encoding="utf-8", newline="") as stream:
            csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n").writerow(
                {key: json.dumps(value, sort_keys=True) if isinstance(value, dict) else value
                 for key, value in metadata.items()}
            )
        if metadata["exit_code"] != 0:
            failure = f"run {attempted} failed validation or exited nonzero"
            break
    summary = _summary(rows, len(schedule))
    summary.update({
        "source_commit": args.candidate_commit,
        "binary_sha256": args.candidate_binary_sha256,
        "baseline_commit": args.baseline_commit,
        "candidate_commit": args.candidate_commit,
        "source_patch_sha256": args.source_patch_sha256,
        "baseline_binary_sha256": args.baseline_binary_sha256,
        "candidate_binary_sha256": args.candidate_binary_sha256,
        "runs_started": started,
        "runs_attempted": attempted,
    })
    summary["complete"] = summary["complete"] and failure is None and attempted == started == len(schedule)
    if failure:
        summary["failure"] = failure
    (results_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (results_dir / "remote-exit.json").write_text(json.dumps({
        "failed": summary["complete"] is not True,
        "runs_started": started,
        "runs_attempted": attempted,
    }, indent=2) + "\n", encoding="utf-8")
    (results_dir / "finished_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    return 0 if summary["complete"] is True else 1


def verify_results_directory(results: Path) -> dict[str, Any]:
    """Verify D1 identities, paired schedule, runtime flags, and raw metrics."""
    errors: list[str] = []
    try:
        summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
        remote_exit = json.loads((results / "remote-exit.json").read_text(encoding="utf-8"))
        plan = json.loads((results / "benchmark-plan.json").read_text(encoding="utf-8"))
        baseline_commit = (results / "baseline-commit.txt").read_text(encoding="utf-8").strip()
        candidate_commit = (results / "candidate-commit.txt").read_text(encoding="utf-8").strip()
        source_patch_sha = (results / "source-patch.sha256").read_text(encoding="utf-8").strip()
        baseline_sha = (results / "baseline-binary.sha256").read_text(encoding="utf-8").strip()
        candidate_sha = (results / "candidate-binary.sha256").read_text(encoding="utf-8").strip()
        schedule = build_point_ack_d1_schedule()
        expected_plan = build_point_ack_d1_plan(
            machine_type="c4-standard-16-lssd",
            machine_spec={"vcpus": 16, "ssd_count": 2, "ssd_gib": 750},
            baseline_commit=baseline_commit,
            candidate_commit=candidate_commit,
            source_patch_sha256=source_patch_sha,
            baseline_binary_sha256=baseline_sha,
            candidate_binary_sha256=candidate_sha,
        )
        if plan != expected_plan:
            errors.append("benchmark plan differs from the fixed D1 suite")
        if summary.get("suite") != "v8-point-ack-d1" or summary.get("complete") is not True:
            errors.append("summary does not report a completed D1 suite")
        if (
            summary.get("source_commit") != candidate_commit
            or summary.get("binary_sha256") != candidate_sha
            or summary.get("baseline_commit") != baseline_commit
            or summary.get("source_patch_sha256") != source_patch_sha
            or summary.get("baseline_binary_sha256") != baseline_sha
            or summary.get("candidate_binary_sha256") != candidate_sha
        ):
            errors.append("summary source or binary identity differs from the archived plan")
        if summary.get("runs_started") != len(schedule) or summary.get("runs_attempted") != len(schedule):
            errors.append("summary counts do not match the plan")
        if summary.get("runs_planned") != len(schedule) or summary.get("runs_completed") != len(schedule):
            errors.append("summary planned/completed counts do not match the plan")
        if remote_exit.get("failed") is not False or remote_exit.get("runs_started") != len(schedule):
            errors.append("remote exit does not confirm all planned runs")
        if remote_exit.get("runs_attempted") != len(schedule):
            errors.append("remote exit attempted count does not match the plan")
        if len(source_patch_sha) != 64 or any(character not in "0123456789abcdef" for character in source_patch_sha):
            errors.append("source patch checksum is invalid")
        source_patch_path = results / "source.patch"
        if (
            not source_patch_path.is_file()
            or hashlib.sha256(source_patch_path.read_bytes()).hexdigest() != source_patch_sha
        ):
            errors.append("archived source patch is missing or its checksum does not match")
        with (results / "point-ack-d1-schedule.tsv").open(encoding="utf-8", newline="") as stream:
            actual_schedule = list(csv.DictReader(stream, delimiter="\t"))
        expected_schedule = [
            {key: "none" if key == "batch_size" and value is None else "" if value is None else str(value)
             for key, value in row.items()}
            for row in schedule
        ]
        if actual_schedule != expected_schedule:
            errors.append("schedule file differs from the fixed D1 matrix")
        summary_tsv = results / "summary.tsv"
        if not summary_tsv.is_file():
            errors.append("summary.tsv is missing")
        else:
            with summary_tsv.open(encoding="utf-8", newline="") as stream:
                actual_summary = list(csv.DictReader(stream, delimiter="\t"))
            if len(actual_summary) != len(schedule):
                errors.append("summary.tsv does not contain exactly one row per planned run")
        rows = summary.get("results")
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise ValueError("summary results are missing or malformed")
        validation = validate_point_ack_d1_rows(rows)
        if validation["complete"] is not True:
            errors.append("run rows do not prove the complete D1 matrix and Stage A path")
        if summary.get("matrix_validation") != validation:
            errors.append("summary matrix validation differs from the archived rows")
        expected_ids = {_run_id(row) for row in schedule}
        actual_ids = {path.name for path in results.iterdir() if path.is_dir() and path.name.startswith("r")}
        if actual_ids != expected_ids:
            errors.append("run directories do not exactly match the D1 schedule")
        rows_by_id = {_run_id(row): row for row in rows}
        summary_fields = _summary_fields(schedule)
        expected_summary = [
            {
                key: json.dumps(rows_by_id[_run_id(row)][key], sort_keys=True)
                if isinstance(rows_by_id[_run_id(row)].get(key), dict)
                else "" if rows_by_id[_run_id(row)].get(key) is None
                else str(rows_by_id[_run_id(row)].get(key))
                for key in summary_fields
            }
            for row in schedule
        ]
        if actual_summary != expected_summary:
            errors.append("summary.tsv rows differ from per-run metadata")
        for row in schedule:
            run_id = _run_id(row)
            run_dir = results / run_id
            result_path, metadata_path = run_dir / "result.json", run_dir / "run-metadata.json"
            if not result_path.is_file() or not metadata_path.is_file():
                errors.append(f"{run_id} is missing result or metadata")
                continue
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata != rows_by_id.get(run_id):
                errors.append(f"{run_id} metadata differs from summary")
            expected_commit = baseline_commit if row["arm"] == "d1_before" else candidate_commit
            expected_sha = baseline_sha if row["arm"] == "d1_before" else candidate_sha
            if metadata.get("source_commit") != expected_commit or metadata.get("binary_sha256") != expected_sha:
                errors.append(f"{run_id} source/binary identity differs from plan")
            environment_path = run_dir / "environment.txt"
            expected_environment = _expected_environment_text(
                True,
                True,
                False,
                True,
                False,
                # The Stage A runner sets this switch only for Point rows;
                # Batch64 inherits the default-off behavior and records it as
                # absent from the controlled environment.
                point_buffer_preallocation_enabled=(True if row["workload"] == "point" else None),
                point_resource_retirement_mode=0,
            )
            if (
                not environment_path.is_file()
                or environment_path.read_text(encoding="utf-8") != expected_environment
            ):
                errors.append(f"{run_id} runtime environment differs from the fixed D1 plan")
            try:
                raw = json.loads(result_path.read_text(encoding="utf-8"))
                expected_metrics = _validate_point_resource_retirement_stage_a_record(raw, row)
                if any(metadata.get(key) != value for key, value in expected_metrics.items()):
                    errors.append(f"{run_id} metrics differ from raw result")
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                errors.append(f"{run_id} raw result is invalid: {error}")
        return {"verified": not errors, "planned": len(schedule), "observed": len(rows), "errors": errors}
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        return {"verified": False, "errors": [str(error)]}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--writers", required=True)
    parser.add_argument("--safe-deadline-epoch", type=int, required=True)
    parser.add_argument("--baseline-commit", required=True)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--source-patch-sha256", required=True)
    parser.add_argument("--baseline-binary-sha256", required=True)
    parser.add_argument("--candidate-binary-sha256", required=True)
    parser.add_argument("--machine-type", required=True)
    parser.add_argument("--ssd-count", type=int, required=True)
    parser.add_argument("--ssd-gib", type=int, required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run_suite(parse_args()))
