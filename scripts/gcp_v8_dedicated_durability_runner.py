#!/usr/bin/env python3
"""Run and verify the three-arm D3b ACK waiter qualification."""

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

D3B_RUNNER_BUNDLE_COMPONENT_NAMES = {
    "gcp-free-trial-wal-bench-remote.sh",
    "gcp_wal_matrix.py",
    "gcp_v8_point_progress_ring_runner.py",
    "gcp_v8_dedicated_durability_runner.py",
}


def _validate_runner_bundle_verification(results: Path, expected_sha256: str) -> list[str]:
    errors: list[str] = []
    try:
        receipt = json.loads((results / "runner-bundle-verification.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return [f"remote runner bundle verification receipt is missing or invalid: {error}"]
    if not isinstance(receipt, dict):
        return ["remote runner bundle verification receipt must be a JSON object"]
    if receipt.get("schema_version") != 1 or receipt.get("verified") is not True:
        errors.append("remote runner bundle receipt does not report a successful verification")
    if receipt.get("expected_bundle_sha256") != expected_sha256:
        errors.append("remote runner bundle receipt has a different expected checksum")
    if receipt.get("actual_bundle_sha256") != expected_sha256:
        errors.append("remote uploaded runner bundle checksum differs from the plan")
    components = receipt.get("components")
    if not isinstance(components, dict) or set(components) != D3B_RUNNER_BUNDLE_COMPONENT_NAMES:
        errors.append("remote runner bundle receipt has an unexpected component set")
    elif any(
        not isinstance(checksum, str)
        or len(checksum) != 64
        or any(character not in "0123456789abcdef" for character in checksum)
        for checksum in components.values()
    ):
        errors.append("remote runner bundle receipt contains a malformed component checksum")
    return errors


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
        paired_comparisons: dict[str, list[dict[str, float | int]]] = {}
        for arm in DURABILITY_DRIVER_ARMS:
            if arm == "caller_legacy_ack":
                continue
            paired: list[dict[str, float | int]] = []
            for repetition in range(1, 6):
                baseline = next(
                    (row for row in arms["caller_legacy_ack"] if row.get("repetition") == repetition), None,
                )
                candidate = next(
                    (row for row in arms[arm] if row.get("repetition") == repetition), None,
                )
                if baseline is None or candidate is None or float(baseline["puts_per_second"]) <= 0:
                    continue
                paired.append({
                    "repetition": repetition,
                    "throughput_change_pct": (
                        float(candidate["puts_per_second"]) / float(baseline["puts_per_second"]) - 1
                    ) * 100,
                    "p99_change_pct": (
                        (float(candidate["p99_ms"]) / float(baseline["p99_ms"]) - 1) * 100
                        if workload == "point" and float(baseline["p99_ms"]) > 0 else 0.0
                    ),
                })
            paired_comparisons[arm] = paired
        changes = {
            arm: [float(pair["throughput_change_pct"]) for pair in paired]
            for arm, paired in paired_comparisons.items()
        }
        p99_changes = {
            arm: [float(pair["p99_change_pct"]) for pair in paired]
            for arm, paired in paired_comparisons.items()
        }
        comparisons.append({
            "writer": writer,
            "workload": workload,
            "arms": arm_summary,
            "baseline_arm": "caller_legacy_ack",
            "paired_comparisons": {
                arm: {
                    "throughput_changes_pct": changes[arm],
                    "throughput_change_median_pct": median(changes[arm]) if changes[arm] else None,
                    "wins": sum(change > 0 for change in changes[arm]),
                    "p99_changes_pct": p99_changes[arm] if workload == "point" else [],
                    "p99_change_median_pct": (
                        median(p99_changes[arm]) if workload == "point" and p99_changes[arm] else None
                    ),
                }
                for arm in paired_comparisons
            },
        })
    return {
        "suite": "v8-d3b-independent-ack",
        "complete": len(rows) == planned == validation["planned"] and validation["complete"],
        "runs_started": len(rows),
        "runs_completed": sum(row.get("exit_code") == 0 for row in rows),
        "runs_planned": planned,
        "score_runs_planned": 60,
        "score_runs_completed": sum(row.get("exit_code") == 0 for row in score_rows),
        "diagnostic_runs_planned": 0,
        "diagnostic_runs_completed": 0,
        "writers": list(DURABILITY_DRIVER_WRITERS),
        "workloads": ["point", "batch64"],
        "arms": list(DURABILITY_DRIVER_ARMS),
        "results": rows,
        "matrix_validation": validation,
        "comparison": comparisons,
        "diagnostics": diagnostic_rows,
        "qualification_note": (
            "All three ACK/driver modes share the same source commit, release binary, Cursor/Direct/Stage A "
            "configuration, and unprofiled scoring plan."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    schedule = build_dedicated_durability_driver_schedule()
    writers = tuple(int(value) for value in args.writers.split(","))
    if writers != DURABILITY_DRIVER_WRITERS:
        raise ValueError(f"D3b ACK qualification requires writers {DURABILITY_DRIVER_WRITERS}")
    if args.machine_type != "c4-standard-16-lssd" or args.ssd_count != 2 or args.ssd_gib != 750:
        raise ValueError("D3b qualification requires c4-standard-16-lssd and two Local SSDs")
    if len(args.commit) != 40 or any(character not in "0123456789abcdef" for character in args.commit):
        raise ValueError("source commit must be a full lowercase SHA-1 identity")
    if len(args.binary_sha256) != 64 or any(character not in "0123456789abcdef" for character in args.binary_sha256):
        raise ValueError("binary checksum must be a full lowercase SHA-256 identity")
    runner_bundle_sha256 = args.runner_bundle_sha256
    if len(runner_bundle_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in runner_bundle_sha256
    ):
        raise ValueError("runner bundle checksum must be a full lowercase SHA-256 identity")
    results_dir, db_root = args.results_dir, args.db_root
    receipt_errors = _validate_runner_bundle_verification(results_dir, runner_bundle_sha256)
    if receipt_errors:
        raise ValueError("; ".join(receipt_errors))
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
        runner_bundle_sha256=runner_bundle_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    (results_dir / "runner-bundle.sha256").write_text(runner_bundle_sha256 + "\n", encoding="utf-8")
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
                    "runner_bundle_sha256": runner_bundle_sha256,
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
    """Validate the fixed D3b schedule, run artifacts, environment and runtime proofs."""
    errors: list[str] = []
    try:
        summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
        remote_exit = json.loads((results / "remote-exit.json").read_text(encoding="utf-8"))
        plan = json.loads((results / "benchmark-plan.json").read_text(encoding="utf-8"))
        schedule = build_dedicated_durability_driver_schedule()
        commit = summary.get("source_commit")
        binary_sha256 = summary.get("binary_sha256")
        runner_bundle_sha256 = summary.get("runner_bundle_sha256")
        if not isinstance(runner_bundle_sha256, str) or len(runner_bundle_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in runner_bundle_sha256
        ):
            errors.append("runner bundle checksum is malformed")
            runner_bundle_sha256 = "0" * 64
        expected_plan = build_dedicated_durability_driver_plan(
            writers=DURABILITY_DRIVER_WRITERS,
            machine_type="c4-standard-16-lssd",
            machine_spec={"vcpus": 16, "ssd_count": 2, "ssd_gib": 750},
            commit=commit,
            binary_sha256=binary_sha256,
            runner_bundle_sha256=runner_bundle_sha256,
        )
        if plan != expected_plan:
            errors.append("benchmark plan differs from the fixed D3b suite")
        if summary.get("suite") != "v8-d3b-independent-ack" or summary.get("complete") is not True:
            errors.append("summary does not report a complete D3b suite")
        if len(str(commit)) != 40 or len(str(binary_sha256)) != 64:
            errors.append("source or binary identity is malformed")
        try:
            if (results / "runner-bundle.sha256").read_text(encoding="utf-8") != runner_bundle_sha256 + "\n":
                errors.append("runner bundle checksum sidecar differs from summary")
        except OSError as error:
            errors.append(f"runner bundle checksum sidecar is missing: {error}")
        errors.extend(_validate_runner_bundle_verification(results, runner_bundle_sha256))
        if any(summary.get(key) != len(schedule) for key in ("runs_started", "runs_attempted", "runs_planned", "runs_completed")):
            errors.append("summary run counts differ from the planned 60")
        if summary.get("score_runs_planned") != 60 or summary.get("score_runs_completed") != 60:
            errors.append("scoring run count differs from the planned 60")
        if summary.get("diagnostic_runs_planned") != 0 or summary.get("diagnostic_runs_completed") != 0:
            errors.append("D3b scoring bundle unexpectedly contains diagnostic runs")
        if remote_exit.get("failed") is not False or remote_exit.get("runs_started") != len(schedule):
            errors.append("remote exit does not confirm all 60 runs")
        if remote_exit.get("runs_attempted") != len(schedule):
            errors.append("remote attempted count does not match the plan")
        rows = summary.get("results")
        if not isinstance(rows, list) or len(rows) != len(schedule):
            errors.append("summary result rows do not match the plan")
            rows = []
        matrix = validate_dedicated_durability_driver_rows(rows)
        if not matrix["complete"]:
            errors.append("D3b result rows do not cover the exact schedule")
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
                    independent_ack_waiter_enabled=bool(expected["independent_ack_waiter_enabled"]),
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
        recomputed = _summary(rows, len(schedule))
        if summary.get("comparison") != recomputed["comparison"]:
            errors.append("summary comparison differs from recomputed run results")
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        errors.append(f"could not load D3b result bundle: {error}")
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
    parser.add_argument("--runner-bundle-sha256", required=True)
    parser.parse_args(argv)
    parser.error(
        "D3 dedicated durability/independent ACK qualification is a retired historical experiment; "
        "archived results remain verifiable but cannot be executed"
    )


if __name__ == "__main__":
    raise SystemExit(main())
