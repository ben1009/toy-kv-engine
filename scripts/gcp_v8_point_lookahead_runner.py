#!/usr/bin/env python3
"""Run the focused V8 Point-only lookahead A/B and batch guardrail matrix."""

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
    POINT_LOOKAHEAD_WINDOW_BYTES,
    build_point_lookahead_plan,
    build_point_lookahead_schedule,
    validate_point_lookahead_rows,
)

RUN_TIMEOUT_SECONDS = 900


def _run_id(row: dict[str, Any]) -> str:
    return f"w{row['writer']}-{row['workload']}-r{row['repetition']}-{row['arm']}"


def _run_one(
    *, binary: Path, row: dict[str, Any], results_dir: Path, db_root: Path,
    safe_deadline_epoch: int,
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
        "fallocate_calls": "", "allocated_bytes": "", "initialization_chunks": "",
        "initialized_bytes": "", "fallocate_elapsed_ms": "", "initialization_elapsed_ms": "",
        "preparation_wait_calls": "", "preparation_wait_ms": "",
    }
    if exit_code == 0:
        try:
            record = json.loads(completed.stdout)
            result = record["result"]
            pipeline = result["wal_pipeline"]
            cpu_seconds = (float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])) / 1000
            start_preparation = pipeline["wal_io_start"]["sliding_extent_preparation"]
            end_preparation = pipeline["wal_io_end"]["sliding_extent_preparation"]

            def counter_delta(name: str) -> int:
                start_value = int(start_preparation[name])
                end_value = int(end_preparation[name])
                if end_value < start_value:
                    raise ValueError(f"extent preparation counter {name} moved backwards")
                return end_value - start_value

            initialized_bytes = counter_delta("initialized_bytes")
            metrics = {
                "ops": int(result["ops"]),
                "puts_per_second": float(result["ops_per_sec"]),
                "p50_ms": float(record["latency"]["p50_ms"]),
                "p95_ms": float(record["latency"]["p95_ms"]),
                "p99_ms": float(record["latency"]["p99_ms"]),
                "cpu_seconds": cpu_seconds,
                "cpu_us_per_put": cpu_seconds * 1_000_000 / int(result["ops"]),
                "sync_count": int(pipeline["sync_count"]),
                "puts_per_sync": int(result["ops"]) / int(pipeline["sync_count"]),
                "fallocate_calls": counter_delta("fallocate_calls"),
                "allocated_bytes": counter_delta("allocated_bytes"),
                "initialization_chunks": counter_delta("initialization_chunks"),
                "initialized_bytes": initialized_bytes,
                "fallocate_elapsed_ms": counter_delta("fallocate_elapsed_ns") / 1_000_000,
                "initialization_elapsed_ms": counter_delta("initialization_elapsed_ns") / 1_000_000,
                "preparation_wait_calls": counter_delta("wait_calls"),
                "preparation_wait_ms": counter_delta("wait_ns") / 1_000_000,
            }
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
            for key in ("wal_io_start", "wal_io_end"):
                prep = pipeline[key]["sliding_extent_preparation"]
                if prep.get("mode") != row["extent_mode"] or prep.get("window_bytes") != POINT_LOOKAHEAD_WINDOW_BYTES:
                    raise ValueError(f"{key} does not report the expected extent preparation")
            should_initialize = row["workload"] == "point" and row["arm"] == "v8_point_lookahead"
            if (initialized_bytes > 0) != should_initialize:
                raise ValueError(
                    "extent initialization did not match the Point-only policy "
                    f"(workload={row['workload']}, arm={row['arm']}, bytes={initialized_bytes})"
                )
            if metrics["allocated_bytes"] <= 0:
                raise ValueError("sliding fallocate did not allocate any bytes during the run")
            if any(not math.isfinite(float(metrics[key])) or float(metrics[key]) < 0 for key in (
                "puts_per_second", "p50_ms", "p95_ms", "p99_ms", "cpu_seconds", "cpu_us_per_put",
                "puts_per_sync", "fallocate_elapsed_ms", "initialization_elapsed_ms", "preparation_wait_ms",
            )):
                raise ValueError("benchmark returned a non-finite or negative metric")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            exit_code = 65
            (run_dir / "validation-error.txt").write_text(f"invalid benchmark result: {error}\n", encoding="utf-8")

    metadata = {
        key: row[key] for key in (
            "writer", "workload", "repetition", "arm", "bench", "batch_entries", "batch_size",
            "num", "sample_every", "extent_mode", "extent_window_bytes",
        )
    }
    metadata.update({"exit_code": int(exit_code), **metrics, "duration_seconds": duration})
    (run_dir / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"run={name} exit={exit_code} duration={duration:.3f}s", flush=True)
    return metadata


def _summary(results: list[dict[str, Any]], writers: tuple[int, ...], planned: int) -> dict[str, Any]:
    validation = validate_point_lookahead_rows(results, writers)
    comparison: list[dict[str, Any]] = []
    cells: dict[str, Any] = {}
    workloads = ("point", "batch8", "batch16", "batch64")
    arms = ("v8_fallocate", "v8_point_lookahead")
    for writer in writers:
        for workload in workloads:
            selected = {
                arm: [row for row in results if row["writer"] == writer and row["workload"] == workload
                      and row["arm"] == arm and row["exit_code"] == 0]
                for arm in arms
            }
            stats: dict[str, Any] = {}
            for arm, rows in selected.items():
                stats[arm] = {
                    "runs": len(rows),
                    **{
                        metric: median(float(row[metric]) for row in rows) if rows else None
                        for metric in (
                            "puts_per_second", "p50_ms", "p95_ms", "p99_ms", "cpu_seconds", "cpu_us_per_put",
                            "sync_count", "puts_per_sync", "fallocate_calls", "allocated_bytes",
                            "initialization_chunks", "initialized_bytes", "fallocate_elapsed_ms",
                            "initialization_elapsed_ms", "preparation_wait_calls", "preparation_wait_ms",
                        )
                    },
                }
            base = stats["v8_fallocate"]["puts_per_second"]
            candidate = stats["v8_point_lookahead"]["puts_per_second"]
            delta = ((candidate / base) - 1) * 100 if base not in (None, 0) and candidate is not None else None
            pair_deltas = []
            for repetition in range(1, 6):
                pair = {
                    arm: next((row for row in results if row["writer"] == writer and row["workload"] == workload
                               and row["repetition"] == repetition and row["arm"] == arm and row["exit_code"] == 0), None)
                    for arm in arms
                }
                if pair[arms[0]] and pair[arms[1]] and pair[arms[0]]["puts_per_second"]:
                    pair_deltas.append(
                        (pair[arms[1]]["puts_per_second"] / pair[arms[0]]["puts_per_second"] - 1) * 100
                    )
            comparison.append({
                "writer": writer, "workload": workload, "baseline": stats["v8_fallocate"],
                "candidate": stats["v8_point_lookahead"], "median_throughput_change_pct": delta,
                "paired_throughput_changes_pct": pair_deltas,
                "candidate_pair_wins": sum(change > 0 for change in pair_deltas),
            })
            cells[f"w{writer}-{workload}"] = stats
    return {
        "complete": len(results) == planned and validation["complete"],
        "runs_completed": len(results), "runs_planned": planned, "writers": list(writers),
        "workloads": list(workloads), "arms": list(arms), "results": results,
        "matrix_validation": validation, "comparison_by_cell": cells, "comparison": comparison,
        "qualification_note": (
            "Same-host V8 comparison. Point-only lookahead is selected per admitted write; Batch8/16/64 "
            "are guardrails and must remain on fallocate-only extent preparation."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    results_dir = args.results_dir
    db_root = args.db_root
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)
    writers = tuple(int(value) for value in args.writers.split(","))
    schedule = build_point_lookahead_schedule(writers)
    plan = build_point_lookahead_plan(
        writers=writers,
        machine_type=args.machine_type,
        machine_spec={"vcpus": int(args.machine_type.split("-")[2]), "ssd_count": args.ssd_count,
                      "ssd_gib": args.ssd_gib},
        commit=args.commit,
        binary_sha256=args.binary_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with (results_dir / "point-lookahead-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows({**row, "batch_size": row["batch_size"] or "none"} for row in schedule)
    (results_dir / "summary.tsv").write_text(
        "writer\tworkload\trepetition\tarm\tbench\tbatch_entries\tbatch_size\tnum\tsample_every\t"
        "extent_mode\t"
        "extent_window_bytes\texit_code\tops\tputs_per_second\tp50_ms\tp95_ms\tp99_ms\tcpu_seconds\t"
        "cpu_us_per_put\tsync_count\tputs_per_sync\tfallocate_calls\tallocated_bytes\tinitialization_chunks\t"
        "initialized_bytes\tfallocate_elapsed_ms\tinitialization_elapsed_ms\tpreparation_wait_calls\t"
        "preparation_wait_ms\tduration_seconds\n", encoding="utf-8",
    )
    completed: list[dict[str, Any]] = []
    failed = False
    for index, row in enumerate(schedule, start=1):
        try:
            metadata = _run_one(
                binary=args.binary, row=row, results_dir=results_dir, db_root=db_root,
                safe_deadline_epoch=args.safe_deadline_epoch,
            )
        except (OSError, TimeoutError, subprocess.SubprocessError) as error:
            failed = True
            (results_dir / "runner-error.txt").write_text(f"run {index} did not finish: {error}\n", encoding="utf-8")
            break
        completed.append(metadata)
        with (results_dir / "summary.tsv").open("a", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=metadata.keys(), delimiter="\t", lineterminator="\n")
            writer.writerow(metadata)
        if metadata["exit_code"] != 0:
            failed = True
            break

    summary = _summary(completed, writers, len(schedule))
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
