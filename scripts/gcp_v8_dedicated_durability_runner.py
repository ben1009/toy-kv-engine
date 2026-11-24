#!/usr/bin/env python3
"""Run and verify D3a CallerDriven/Dedicated durability qualification."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
from statistics import median
from typing import Any

from gcp_v8_point_progress_ring_runner import (
    _expected_environment_text,
    _run_id,
    _run_one,
    _summary_fields,
    _validate_dedicated_durability_record,
)
from gcp_wal_matrix import (
    DURABILITY_DRIVER_ARMS,
    DURABILITY_DRIVER_DIAGNOSTIC_ORDER,
    DURABILITY_DRIVER_NUM,
    DURABILITY_DRIVER_WRITERS,
    build_dedicated_durability_driver_plan,
    build_dedicated_durability_driver_schedule,
    validate_dedicated_durability_driver_rows,
)


def _summary(rows: list[dict[str, Any]], planned: int) -> dict[str, Any]:
    validation = validate_dedicated_durability_driver_rows(rows)
    score_rows = [row for row in rows if row.get("phase") == "score"]
    diagnostic_rows = [row for row in rows if row.get("phase") == "diagnostic"]
    comparisons: list[dict[str, Any]] = []
    for writer, workload in [*((writer, "point") for writer in DURABILITY_DRIVER_WRITERS), (16, "batch64")]:
        arms = {
            arm: [
                row for row in score_rows
                if row.get("writer") == writer and row.get("workload") == workload
                and row.get("arm") == arm and row.get("exit_code") == 0
            ]
            for arm in DURABILITY_DRIVER_ARMS
        }
        metrics = (
            "puts_per_second", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms",
            "sync_count", "puts_per_sync",
        )
        arm_summary = {
            arm: {
                "runs": len(selected),
                **{
                    metric: median(float(row[metric]) for row in selected)
                    if selected and all(row.get(metric) is not None for row in selected) else None
                    for metric in metrics
                },
            }
            for arm, selected in arms.items()
        }
        paired: list[dict[str, float | int]] = []
        for repetition in range(1, 6):
            caller = next((row for row in arms["caller_driven"] if row.get("repetition") == repetition), None)
            dedicated = next((row for row in arms["dedicated"] if row.get("repetition") == repetition), None)
            if caller is None or dedicated is None or float(caller["puts_per_second"]) <= 0:
                continue
            paired.append({
                "repetition": repetition,
                "throughput_change_pct": (
                    float(dedicated["puts_per_second"]) / float(caller["puts_per_second"]) - 1
                ) * 100,
                "p99_change_pct": (
                    (float(dedicated["p99_ms"]) / float(caller["p99_ms"]) - 1) * 100
                    if workload == "point" and float(caller["p99_ms"]) > 0 else 0.0
                ),
            })
        changes = [float(pair["throughput_change_pct"]) for pair in paired]
        p99_changes = [float(pair["p99_change_pct"]) for pair in paired] if workload == "point" else []
        comparisons.append({
            "writer": writer,
            "workload": workload,
            "arms": arm_summary,
            "paired_throughput_changes_pct": changes,
            "paired_throughput_change_median_pct": median(changes) if changes else None,
            "dedicated_pair_wins": sum(change > 0 for change in changes),
            "paired_p99_changes_pct": p99_changes,
            "paired_p99_change_median_pct": median(p99_changes) if p99_changes else None,
        })
    return {
        "suite": "v8-dedicated-durability-driver",
        "complete": len(rows) == planned == validation["planned"] and validation["complete"],
        "runs_started": len(rows),
        "runs_completed": sum(row.get("exit_code") == 0 for row in rows),
        "runs_planned": planned,
        "score_runs_planned": 40,
        "score_runs_completed": sum(row.get("exit_code") == 0 for row in score_rows),
        "diagnostic_runs_planned": 4,
        "diagnostic_runs_completed": sum(row.get("exit_code") == 0 for row in diagnostic_rows),
        "writers": list(DURABILITY_DRIVER_WRITERS),
        "workloads": ["point", "batch64"],
        "arms": list(DURABILITY_DRIVER_ARMS),
        "results": rows,
        "matrix_validation": validation,
        "comparison": comparisons,
        "diagnostics": diagnostic_rows,
        "qualification_note": (
            "Scoring excludes all detailed dedicated-driver counters. The four separate diagnostic runs enable "
            "only the lightweight driver counters and are not performance scores."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    schedule = build_dedicated_durability_driver_schedule()
    writers = tuple(int(value) for value in args.writers.split(","))
    if writers != DURABILITY_DRIVER_WRITERS:
        raise ValueError(f"D3a qualification requires writers {DURABILITY_DRIVER_WRITERS}")
    if args.machine_type != "c4-standard-16-lssd" or args.ssd_count != 2 or args.ssd_gib != 750:
        raise ValueError("D3a qualification requires c4-standard-16-lssd and two Local SSDs")
    if len(args.commit) != 40 or any(character not in "0123456789abcdef" for character in args.commit):
        raise ValueError("source commit must be a full lowercase SHA-1 identity")
    if len(args.binary_sha256) != 64 or any(character not in "0123456789abcdef" for character in args.binary_sha256):
        raise ValueError("binary checksum must be a full lowercase SHA-256 identity")
    results_dir, db_root = args.results_dir, args.db_root
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)
    binary_digest = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    if binary_digest != args.binary_sha256:
        raise ValueError("release binary checksum does not match the supplied identity")
    plan = build_dedicated_durability_driver_plan(
        writers=writers,
        machine_type=args.machine_type,
        machine_spec={"vcpus": 16, "ssd_count": args.ssd_count, "ssd_gib": args.ssd_gib},
        commit=args.commit,
        binary_sha256=args.binary_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with (results_dir / "dedicated-durability-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows({**row, "batch_size": row["batch_size"] or "none"} for row in schedule)
    fields = _summary_fields(schedule)
    (results_dir / "summary.tsv").write_text("\t".join(fields) + "\n", encoding="utf-8")

    rows: list[dict[str, Any]] = []
    failure: str | None = None
    attempted = started = 0
    for row in schedule:
        attempted += 1
        scheduled = {**row, "source_commit": args.commit}

        def record_start() -> None:
            nonlocal started
            started += 1

        try:
            metadata = _run_one(
                binary=args.binary,
                binary_sha256=args.binary_sha256,
                row=scheduled,
                results_dir=results_dir,
                db_root=db_root,
                safe_deadline_epoch=args.safe_deadline_epoch,
                candidate_mode="dedicated-durability",
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
    summary.update({"source_commit": args.commit, "binary_sha256": args.binary_sha256,
                    "runs_attempted": attempted})
    summary["complete"] = summary["complete"] and failure is None and attempted == started == len(schedule)
    if failure:
        summary["failure"] = failure
    (results_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    (results_dir / "remote-exit.json").write_text(json.dumps({
        "failed": summary["complete"] is not True,
        "runs_started": started,
        "runs_attempted": attempted,
    }, indent=2) + "\n", encoding="utf-8")
    (results_dir / "finished_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    return 0 if summary["complete"] is True else 1


def verify_results_directory(results: Path) -> dict[str, Any]:
    """Validate the fixed D3a schedule, run artifacts, environment and runtime proofs."""
    errors: list[str] = []
    try:
        summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
        remote_exit = json.loads((results / "remote-exit.json").read_text(encoding="utf-8"))
        plan = json.loads((results / "benchmark-plan.json").read_text(encoding="utf-8"))
        schedule = build_dedicated_durability_driver_schedule()
        commit = summary.get("source_commit")
        binary_sha256 = summary.get("binary_sha256")
        expected_plan = build_dedicated_durability_driver_plan(
            writers=DURABILITY_DRIVER_WRITERS,
            machine_type="c4-standard-16-lssd",
            machine_spec={"vcpus": 16, "ssd_count": 2, "ssd_gib": 750},
            commit=commit,
            binary_sha256=binary_sha256,
        )
        if plan != expected_plan:
            errors.append("benchmark plan differs from the fixed D3a suite")
        if summary.get("suite") != "v8-dedicated-durability-driver" or summary.get("complete") is not True:
            errors.append("summary does not report a complete D3a suite")
        if len(str(commit)) != 40 or len(str(binary_sha256)) != 64:
            errors.append("source or binary identity is malformed")
        if any(summary.get(key) != len(schedule) for key in ("runs_started", "runs_attempted", "runs_planned", "runs_completed")):
            errors.append("summary run counts differ from the planned 44")
        if summary.get("score_runs_planned") != 40 or summary.get("score_runs_completed") != 40:
            errors.append("scoring run count differs from the planned 40")
        if summary.get("diagnostic_runs_planned") != 4 or summary.get("diagnostic_runs_completed") != 4:
            errors.append("diagnostic run count differs from the planned four")
        if remote_exit.get("failed") is not False or remote_exit.get("runs_started") != len(schedule):
            errors.append("remote exit does not confirm all 44 runs")
        if remote_exit.get("runs_attempted") != len(schedule):
            errors.append("remote attempted count does not match the plan")
        rows = summary.get("results")
        if not isinstance(rows, list) or len(rows) != len(schedule):
            errors.append("summary result rows do not match the plan")
            rows = []
        matrix = validate_dedicated_durability_driver_rows(rows)
        if not matrix["complete"]:
            errors.append("D3a result rows do not cover the exact schedule")
        if summary.get("matrix_validation") != matrix:
            errors.append("summary matrix validation differs from recomputed validation")

        expected_ids = {_run_id(row) for row in schedule}
        actual_ids = {path.name for path in results.iterdir() if path.is_dir() and path.name.startswith(("r", "w"))}
        if actual_ids != expected_ids:
            errors.append("run directories differ from the exact planned IDs")
        rows_by_cell = {
            (row.get("phase"), row.get("writer"), row.get("workload"), row.get("repetition"), row.get("arm")): row
            for row in rows if isinstance(row, dict)
        }
        for expected in schedule:
            key = (expected["phase"], expected["writer"], expected["workload"], expected["repetition"], expected["arm"])
            row = rows_by_cell.get(key)
            if row is None:
                continue
            run_dir = results / _run_id(expected)
            for name in ("result.json", "run-metadata.json", "environment.txt"):
                if not (run_dir / name).is_file():
                    errors.append(f"{run_dir.name} is missing {name}")
            try:
                record = json.loads((run_dir / "result.json").read_text(encoding="utf-8"))
                metadata = json.loads((run_dir / "run-metadata.json").read_text(encoding="utf-8"))
                expected_row = {**expected, "source_commit": commit}
                metrics = _validate_dedicated_durability_record(record, expected_row)
                expected_environment = _expected_environment_text(
                    True, True, False, True, False,
                    point_buffer_preallocation_enabled=bool(expected["point_buffer_preallocation_enabled"]),
                    point_resource_retirement_mode=0,
                    dedicated_durability_driver_enabled=bool(expected["durability_driver_enabled"]),
                    dedicated_durability_diagnostics_enabled=bool(expected["durability_driver_diagnostics_enabled"]),
                )
                if (run_dir / "environment.txt").read_text(encoding="utf-8") != expected_environment:
                    errors.append(f"{run_dir.name} environment differs from the planned arm")
                if row.get("exit_code") != 0 or metadata.get("exit_code") != 0:
                    errors.append(f"{run_dir.name} did not succeed")
                if metadata.get("source_commit") != commit or metadata.get("binary_sha256") != binary_sha256:
                    errors.append(f"{run_dir.name} source or binary identity differs")
                for name, value in metrics.items():
                    if row.get(name) != value or metadata.get(name) != value:
                        errors.append(f"{run_dir.name} has inconsistent {name}")
                        break
            except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                errors.append(f"{run_dir.name} could not be revalidated: {error}")
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        errors.append(f"could not load D3a result bundle: {error}")
    return {"verified": not errors, "errors": errors}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--writers", required=True)
    parser.add_argument("--safe-deadline-epoch", type=int, required=True)
    parser.add_argument("--machine-type", required=True)
    parser.add_argument("--ssd-count", type=int, required=True)
    parser.add_argument("--ssd-gib", type=int, required=True)
    args = parser.parse_args(argv)
    return run_suite(args)


if __name__ == "__main__":
    raise SystemExit(main())
