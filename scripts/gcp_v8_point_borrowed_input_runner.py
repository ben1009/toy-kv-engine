#!/usr/bin/env python3
"""Run same-host V8 Point borrowed-input A/B with Point and Batch guardrails."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import shlex
import subprocess
import sys
import time
from statistics import median
from typing import Any

from gcp_wal_matrix import (
    POINT_BORROWED_INPUT_NUM,
    POINT_LOOKAHEAD_WINDOW_BYTES,
    build_point_borrowed_input_plan,
    build_point_borrowed_input_schedule,
    validate_point_borrowed_input_rows,
)

RUN_TIMEOUT_SECONDS = 300


def _run_id(row: dict[str, Any]) -> str:
    return f"w{row['writer']}-{row['workload']}-r{row['repetition']}-{row['arm']}"


def _summary_fields(schedule_row: dict[str, Any]) -> tuple[str, ...]:
    """Return all planned-run and measured-result columns in stable order."""
    return (
        *schedule_row.keys(),
        "source_commit", "binary_sha256", "exit_code", "ops", "puts_per_second", "cpu_seconds",
        "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms", "sync_count", "puts_per_sync",
        "initialized_bytes", "preparation_wait_ms", "direct_frame_encoded_count", "duration_seconds",
    )


def _run_one(
    *, binary: Path, binary_sha256: str, row: dict[str, Any], results_dir: Path,
    db_root: Path, safe_deadline_epoch: int, verify_direct_frame: bool = False,
) -> dict[str, Any]:
    name = _run_id(row)
    run_dir = results_dir / name
    run_dir.mkdir(parents=True, exist_ok=False)
    command = [
        str(binary), "--bench", str(row["bench"]), "--wal", "--pitr", "--pitr-format", "v8",
        "--wal-io-mode", "parallel", "--num", str(row["num"]), "--threads", str(row["writer"]),
        "--value-size", "1024", "--target-sst-size", "2147483648", "--compaction", "none",
        "--output", "json", "--latency-sample-every", str(row["sample_every"]),
        "--path", str(db_root / name),
    ]
    if row["workload"] != "point":
        command.extend(("--wal-batch-size", str(row["batch_entries"])))
    env = {
        "TOYKV_BENCH_V8_EXTENT_PREP": str(row["extent_mode"]),
        "TOYKV_BENCH_V8_EXTENT_PREP_BYTES": str(row["extent_window_bytes"]),
        "TOYKV_WAL_V8_FRONTIER_MODE": str(row["frontier_mode"]),
        "TOYKV_BENCH_V8_POINT_DIRECT_FRAME": "1" if row.get("point_direct_frame_enabled", False) else "0",
    }
    (run_dir / "command.sh").write_text(shlex.join(command) + "\n", encoding="utf-8")
    (run_dir / "environment.txt").write_text(
        "\n".join(f"{key}={value}" for key, value in env.items()) + "\n", encoding="utf-8",
    )
    (run_dir / "start_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    if int(time.time()) + RUN_TIMEOUT_SECONDS + 330 >= safe_deadline_epoch:
        raise TimeoutError("stopping before the VM safety deadline")

    started = time.monotonic()
    completed = subprocess.run(
        ["timeout", "--signal=INT", "--kill-after=30s", str(RUN_TIMEOUT_SECONDS), "env",
         *(f"{key}={value}" for key, value in env.items()), *command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    duration = time.monotonic() - started
    (run_dir / "end_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    (run_dir / "result.json").write_text(completed.stdout, encoding="utf-8")
    (run_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")

    exit_code = completed.returncode
    metrics: dict[str, Any] = {
        "ops": "", "puts_per_second": "", "p50_ms": "", "p95_ms": "", "p99_ms": "",
        "cpu_seconds": "", "cpu_us_per_put": "", "sync_count": "", "puts_per_sync": "",
        "initialized_bytes": "", "preparation_wait_ms": "", "direct_frame_encoded_count": "",
    }
    if exit_code == 0:
        try:
            record = json.loads(completed.stdout)
            result = record["result"]
            pipeline = result["wal_pipeline"]
            cpu_seconds = (
                float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])
            ) / 1000
            start_preparation = pipeline["wal_io_start"]["sliding_extent_preparation"]
            end_preparation = pipeline["wal_io_end"]["sliding_extent_preparation"]

            def counter_delta(name: str) -> int:
                start_value = int(start_preparation[name])
                end_value = int(end_preparation[name])
                if end_value < start_value:
                    raise ValueError(f"extent preparation counter {name} moved backwards")
                return end_value - start_value

            initialized_bytes = counter_delta("initialized_bytes")
            sync_count = int(pipeline["sync_count"])
            metrics = {
                "ops": int(result["ops"]),
                "puts_per_second": float(result["ops_per_sec"]),
                "p50_ms": float(record["latency"]["p50_ms"]),
                "p95_ms": float(record["latency"]["p95_ms"]),
                "p99_ms": float(record["latency"]["p99_ms"]),
                "cpu_seconds": cpu_seconds,
                "cpu_us_per_put": cpu_seconds * 1_000_000 / int(result["ops"]),
                "sync_count": sync_count,
                "puts_per_sync": int(result["ops"]) / sync_count if sync_count else 0,
                "initialized_bytes": initialized_bytes,
                "preparation_wait_ms": counter_delta("wait_ns") / 1_000_000,
            }
            if verify_direct_frame:
                direct_frame_enabled = bool(row["point_direct_frame_enabled"])
                direct_frame_encoded_count = int(pipeline["v8_point_direct_frame_encoded_count"])
                metrics["direct_frame_encoded_count"] = direct_frame_encoded_count
                if pipeline.get("v8_point_direct_frame_enabled") is not direct_frame_enabled:
                    raise ValueError("benchmark direct-frame flag does not match the planned run")
                expected_direct_frame_count = (
                    metrics["ops"] if direct_frame_enabled and row["workload"] == "point" else 0
                )
                if direct_frame_encoded_count != expected_direct_frame_count:
                    raise ValueError(
                        "direct-frame encode count does not match the planned Point/Batch workload "
                        f"(expected={expected_direct_frame_count}, actual={direct_frame_encoded_count})"
                    )
            expected_params = {
                "num": row["num"], "threads": row["writer"], "value_size": 1024,
                "batch_size": row["batch_size"], "wal_format_version": 8, "wal_io_mode": "parallel",
            }
            if metrics["ops"] != row["num"] or any(
                record["params"].get(key) != value for key, value in expected_params.items()
            ):
                raise ValueError("benchmark parameters do not match the planned run")
            if pipeline.get("wal_io_stable") is not True:
                raise ValueError("WAL I/O metadata changed during the run")
            for boundary in ("wal_io_start", "wal_io_end"):
                preparation = pipeline[boundary]["sliding_extent_preparation"]
                if preparation.get("mode") != row["extent_mode"] or (
                    preparation.get("window_bytes") != POINT_LOOKAHEAD_WINDOW_BYTES
                ):
                    raise ValueError(f"{boundary} does not report the planned extent preparation")
            should_initialize = row["workload"] == "point"
            if (initialized_bytes > 0) != should_initialize:
                raise ValueError("initialization does not match the planned Point/Batch workload")
            if metrics["sync_count"] <= 0:
                raise ValueError("benchmark reported no WAL syncs")
            if any(not math.isfinite(float(metrics[key])) or float(metrics[key]) < 0 for key in (
                "puts_per_second", "p50_ms", "p95_ms", "p99_ms", "cpu_seconds", "cpu_us_per_put",
                "puts_per_sync", "preparation_wait_ms",
            )):
                raise ValueError("benchmark returned a non-finite or negative metric")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            exit_code = 65
            (run_dir / "validation-error.txt").write_text(f"invalid benchmark result: {error}\n", encoding="utf-8")

    metadata = {
        key: row[key] for key in (
            "writer", "workload", "repetition", "arm", "bench", "batch_entries", "batch_size",
            "num", "sample_every", "extent_mode", "extent_window_bytes", "frontier_mode",
        )
    }
    metadata.update({
        "source_commit": row["source_commit"], "binary_sha256": binary_sha256,
        "exit_code": int(exit_code), **metrics, "duration_seconds": duration,
    })
    if "point_direct_frame_enabled" in row:
        metadata["point_direct_frame_enabled"] = bool(row["point_direct_frame_enabled"])
    (run_dir / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"run={name} source={row['source_commit'][:12]} exit={exit_code} duration={duration:.3f}s", flush=True)
    return metadata


def _summary(rows: list[dict[str, Any]], writers: tuple[int, ...], planned: int) -> dict[str, Any]:
    validation = validate_point_borrowed_input_rows(rows, writers)
    comparison: list[dict[str, Any]] = []
    comparison_by_cell: dict[str, Any] = {}
    cells = [(writer, "point") for writer in writers] + [(16, "batch16"), (16, "batch64")]
    arms = ("v8_owned_baseline", "v8_borrowed_candidate")
    for writer, workload in cells:
        selected = {
            arm: [row for row in rows if row["writer"] == writer and row["workload"] == workload
                  and row["arm"] == arm and row["exit_code"] == 0]
            for arm in arms
        }
        stats: dict[str, Any] = {}
        metrics = (
            "puts_per_second", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms",
            "sync_count", "puts_per_sync", "initialized_bytes", "preparation_wait_ms",
        )
        for arm, arm_rows in selected.items():
            stats[arm] = {
                "runs": len(arm_rows),
                **{metric: median(float(row[metric]) for row in arm_rows) if arm_rows else None for metric in metrics},
            }
        base = stats[arms[0]]["puts_per_second"]
        candidate = stats[arms[1]]["puts_per_second"]
        deltas: list[float] = []
        for repetition in range(1, 6):
            pair = {
                arm: next((row for row in rows if row["writer"] == writer and row["workload"] == workload
                           and row["repetition"] == repetition and row["arm"] == arm and row["exit_code"] == 0), None)
                for arm in arms
            }
            if pair[arms[0]] and pair[arms[1]] and pair[arms[0]]["puts_per_second"]:
                deltas.append((pair[arms[1]]["puts_per_second"] / pair[arms[0]]["puts_per_second"] - 1) * 100)
        cell = {
            "writer": writer, "workload": workload, "baseline": stats[arms[0]],
            "candidate": stats[arms[1]],
            "median_throughput_change_pct": ((candidate / base) - 1) * 100 if base and candidate else None,
            "paired_throughput_changes_pct": deltas,
            "candidate_pair_wins": sum(delta > 0 for delta in deltas),
        }
        comparison.append(cell)
        comparison_by_cell[f"w{writer}-{workload}"] = cell
    return {
        "suite": "v8-point-borrowed-input", "complete": len(rows) == planned and validation["complete"],
        "runs_completed": len(rows), "runs_planned": planned, "writers": list(writers),
        "workloads": ["point", "batch16", "batch64"], "arms": list(arms), "results": rows,
        "matrix_validation": validation, "comparison_by_cell": comparison_by_cell, "comparison": comparison,
        "qualification_note": (
            "Same-host, same-toolchain source comparison. Only the V8 Point borrowed-input preparation differs; "
            "both arms use 1 MiB Point lookahead, Direct FRONTIER, dynamic coalescing, and the same Batch path."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    results_dir, db_root = args.results_dir, args.db_root
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)
    writers = tuple(int(value) for value in args.writers.split(","))
    schedule = build_point_borrowed_input_schedule(writers)
    machine_spec = {
        "vcpus": int(args.machine_type.split("-")[2]), "ssd_count": args.ssd_count, "ssd_gib": args.ssd_gib,
    }
    plan = build_point_borrowed_input_plan(
        writers=writers, machine_type=args.machine_type, machine_spec=machine_spec,
        baseline_commit=args.baseline_commit, candidate_commit=args.candidate_commit,
        source_patch_sha256=args.source_patch_sha256,
        baseline_binary_sha256=args.baseline_binary_sha256,
        candidate_binary_sha256=args.candidate_binary_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with (results_dir / "point-borrowed-input-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows({**row, "batch_size": row["batch_size"] or "none"} for row in schedule)
    fields = _summary_fields(schedule[0])
    with (results_dir / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
        csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n").writeheader()

    binaries = {
        "v8_owned_baseline": (args.baseline_binary, args.baseline_binary_sha256, args.baseline_commit),
        "v8_borrowed_candidate": (args.candidate_binary, args.candidate_binary_sha256, args.candidate_commit),
    }
    completed: list[dict[str, Any]] = []
    failed = False
    for index, row in enumerate(schedule, start=1):
        try:
            binary, binary_hash, source_commit = binaries[row["arm"]]
            result = _run_one(
                binary=binary, binary_sha256=binary_hash,
                row={**row, "source_commit": source_commit}, results_dir=results_dir,
                db_root=db_root, safe_deadline_epoch=args.safe_deadline_epoch,
            )
        except (OSError, TimeoutError, subprocess.SubprocessError) as error:
            failed = True
            (results_dir / "runner-error.txt").write_text(f"run {index} did not finish: {error}\n", encoding="utf-8")
            break
        completed.append(result)
        with (results_dir / "summary.tsv").open("a", encoding="utf-8", newline="") as stream:
            csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n").writerow(result)
        if result["exit_code"] != 0:
            failed = True
            break

    summary = _summary(completed, writers, len(schedule))
    summary.update({
        "source_commit": args.candidate_commit,
        "baseline_commit": args.baseline_commit,
        "source_patch_sha256": args.source_patch_sha256,
        "baseline_binary_sha256": args.baseline_binary_sha256,
        "candidate_binary_sha256": args.candidate_binary_sha256,
    })
    summary["complete"] = summary["complete"] and not failed
    (results_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (results_dir / "remote-exit.json").write_text(
        json.dumps({"failed": failed or len(completed) != len(schedule), "runs_started": len(completed)}, indent=2)
        + "\n", encoding="utf-8",
    )
    (results_dir / "finished_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    return 1 if failed or len(completed) != len(schedule) else 0


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
