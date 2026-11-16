#!/usr/bin/env python3
"""Run the V8 Point DATA group-join budget experiment and Batch guardrails."""

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
    build_point_group_policy_plan,
    build_point_group_policy_schedule,
    validate_point_group_policy_rows,
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
        str(binary), "--bench", str(row["bench"]), "--pitr", "--pitr-format", "v8",
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
        "TOYKV_BENCH_V8_POINT_GROUP_JOIN_US": str(row["point_group_join_us"]),
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
        "data_submit_groups": "", "data_submit_tickets": "", "data_submit_avg_tickets": "",
        "data_submit_max_tickets": "", "fallocate_calls": "", "allocated_bytes": "",
        "initialized_bytes": "", "preparation_wait_ms": "",
    }
    if exit_code == 0:
        try:
            record = json.loads(completed.stdout)
            result = record["result"]
            pipeline = result["wal_pipeline"]
            cpu_seconds = (float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])) / 1000
            groups = int(pipeline["v8_data_submit_groups"])
            tickets = int(pipeline["v8_data_submit_tickets"])
            max_tickets = int(pipeline["v8_data_submit_max_tickets"])
            start_preparation = pipeline["wal_io_start"]["sliding_extent_preparation"]
            end_preparation = pipeline["wal_io_end"]["sliding_extent_preparation"]

            def counter_delta(counter: str) -> int:
                before, after = start_preparation[counter], end_preparation[counter]
                if type(before) is not int or type(after) is not int or before < 0 or after < 0:
                    raise ValueError(f"extent preparation counter {counter} is invalid")
                if after < before:
                    raise ValueError(f"extent preparation counter {counter} moved backwards")
                return after - before

            ops = int(result["ops"])
            sync_count = int(pipeline["sync_count"])
            metrics = {
                "ops": ops,
                "puts_per_second": float(result["ops_per_sec"]),
                "p50_ms": float(record["latency"]["p50_ms"]),
                "p95_ms": float(record["latency"]["p95_ms"]),
                "p99_ms": float(record["latency"]["p99_ms"]),
                "cpu_seconds": cpu_seconds,
                "cpu_us_per_put": cpu_seconds * 1_000_000 / ops,
                "sync_count": sync_count,
                "puts_per_sync": ops / sync_count if sync_count else 0,
                "data_submit_groups": groups,
                "data_submit_tickets": tickets,
                "data_submit_avg_tickets": tickets / groups if groups else 0,
                "data_submit_max_tickets": max_tickets,
                "fallocate_calls": counter_delta("fallocate_calls"),
                "allocated_bytes": counter_delta("allocated_bytes"),
                "initialized_bytes": counter_delta("initialized_bytes"),
                "preparation_wait_ms": counter_delta("wait_ns") / 1_000_000,
            }
            expected_params = {
                "num": row["num"], "threads": row["writer"], "value_size": 1024,
                "batch_size": row["batch_size"], "wal_format_version": 8, "wal_io_mode": "parallel",
            }
            if ops != row["num"] or any(record["params"].get(key) != value for key, value in expected_params.items()):
                raise ValueError("benchmark parameters do not match the planned run")
            if pipeline.get("wal_io_stable") is not True:
                raise ValueError("WAL I/O metadata changed during the run")
            for boundary in ("wal_io_start", "wal_io_end"):
                preparation = pipeline[boundary]["sliding_extent_preparation"]
                if preparation.get("mode") != row["extent_mode"]:
                    raise ValueError(f"{boundary} does not report the planned extent preparation mode")
                if preparation.get("window_bytes") != POINT_LOOKAHEAD_WINDOW_BYTES:
                    raise ValueError(f"{boundary} does not report the planned extent window")
            if metrics["data_submit_groups"] <= 0 or metrics["data_submit_tickets"] <= 0:
                raise ValueError("V8 DATA submission group counters are missing")
            if metrics["data_submit_groups"] > metrics["data_submit_tickets"]:
                raise ValueError("V8 DATA group count exceeds its submitted ticket count")
            if metrics["data_submit_max_tickets"] <= 0 or (
                metrics["data_submit_max_tickets"] > metrics["data_submit_tickets"]
            ):
                raise ValueError("V8 DATA maximum group size is inconsistent with its ticket count")
            if metrics["data_submit_max_tickets"] > 32:
                raise ValueError("a V8 DATA submission group exceeded the admission limit")
            should_initialize = row["workload"] == "point"
            if (metrics["initialized_bytes"] > 0) != should_initialize:
                raise ValueError("extent initialization does not match the planned Point/Batch policy")
            if metrics["allocated_bytes"] <= 0:
                raise ValueError("sliding extent preparation did not allocate any bytes")
            if any(not math.isfinite(float(metrics[key])) or float(metrics[key]) < 0 for key in (
                "puts_per_second", "p50_ms", "p95_ms", "p99_ms", "cpu_seconds", "cpu_us_per_put",
                "puts_per_sync", "data_submit_avg_tickets", "preparation_wait_ms",
            )):
                raise ValueError("benchmark returned a non-finite or negative metric")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, ZeroDivisionError) as error:
            exit_code = 65
            (run_dir / "validation-error.txt").write_text(f"invalid benchmark result: {error}\n", encoding="utf-8")

    metadata = {
        key: row[key] for key in (
            "writer", "workload", "repetition", "arm", "bench", "batch_entries", "batch_size", "num",
            "sample_every", "point_group_join_us", "extent_mode", "extent_window_bytes",
        )
    }
    metadata.update({"exit_code": int(exit_code), **metrics, "duration_seconds": duration})
    (run_dir / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"run={name} exit={exit_code} duration={duration:.3f}s", flush=True)
    return metadata


def _summary(results: list[dict[str, Any]], writers: tuple[int, ...], planned: int) -> dict[str, Any]:
    validation = validate_point_group_policy_rows(results, writers)
    workloads = ("point", "batch16", "batch64")
    arms = tuple(
        [f"point_join_{budget}us" for budget in (0, 5, 10, 20)]
        + [f"batch_group_{budget}us" for budget in (0, 20)]
    )
    comparisons: list[dict[str, Any]] = []
    cells: dict[str, Any] = {}
    for workload in workloads:
        selected_writers = writers if workload == "point" else (16,)
        relevant_arms = arms[:4] if workload == "point" else arms[4:]
        for writer in selected_writers:
            selected = {
                arm: [row for row in results if row["writer"] == writer and row["workload"] == workload
                      and row["arm"] == arm and row["exit_code"] == 0]
                for arm in relevant_arms
            }
            stats = {
                arm: {
                    "runs": len(rows),
                    **{
                        metric: median(float(row[metric]) for row in rows) if rows else None
                        for metric in (
                            "puts_per_second", "p50_ms", "p95_ms", "p99_ms", "cpu_seconds", "cpu_us_per_put",
                            "sync_count", "puts_per_sync", "data_submit_groups", "data_submit_avg_tickets",
                            "data_submit_max_tickets", "fallocate_calls", "allocated_bytes", "initialized_bytes",
                            "preparation_wait_ms",
                        )
                    },
                }
                for arm, rows in selected.items()
            }
            baseline_arm = "point_join_0us" if workload == "point" else "batch_group_0us"
            for arm in relevant_arms:
                if arm == baseline_arm:
                    continue
                deltas = []
                for repetition in range(1, 6):
                    base = next((row for row in results if row["writer"] == writer and row["workload"] == workload
                                 and row["repetition"] == repetition and row["arm"] == baseline_arm
                                 and row["exit_code"] == 0), None)
                    candidate = next((row for row in results if row["writer"] == writer and row["workload"] == workload
                                      and row["repetition"] == repetition and row["arm"] == arm
                                      and row["exit_code"] == 0), None)
                    if base and candidate and base["puts_per_second"]:
                        deltas.append((candidate["puts_per_second"] / base["puts_per_second"] - 1) * 100)
                baseline = stats[baseline_arm]["puts_per_second"]
                candidate_rate = stats[arm]["puts_per_second"]
                comparisons.append({
                    "writer": writer, "workload": workload, "baseline_arm": baseline_arm, "candidate_arm": arm,
                    "median_throughput_change_pct": (
                        (candidate_rate / baseline - 1) * 100 if baseline not in (None, 0) and candidate_rate is not None
                        else None
                    ),
                    "paired_throughput_changes_pct": deltas,
                    "candidate_pair_wins": sum(delta > 0 for delta in deltas),
                })
            cells[f"w{writer}-{workload}"] = stats
    return {
        "suite": "v8-point-data-group",
        "complete": len(results) == planned and validation["complete"],
        "runs_completed": len(results), "runs_planned": planned, "writers": list(writers),
        "workloads": list(workloads), "arms": list(arms), "results": results,
        "matrix_validation": validation, "comparison_by_cell": cells, "comparison": comparisons,
        "qualification_note": (
            "Point runs vary only the benchmark-only bounded DATA join budget while keeping 1 MiB Point Lookahead "
            "fixed. Batch16/64 use Fallocate-only and verify that nonzero Point join settings do not delay Batch."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    results_dir, db_root = args.results_dir, args.db_root
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)
    writers = tuple(int(value) for value in args.writers.split(","))
    schedule = build_point_group_policy_schedule(writers)
    machine_spec = {"vcpus": int(args.machine_type.split("-")[2]), "ssd_count": args.ssd_count,
                    "ssd_gib": args.ssd_gib}
    plan = build_point_group_policy_plan(
        writers=writers, machine_type=args.machine_type, machine_spec=machine_spec,
        commit=args.commit, binary_sha256=args.binary_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with (results_dir / "point-group-policy-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows({**row, "batch_size": row["batch_size"] or "none"} for row in schedule)
    summary_fields = (
        "writer", "workload", "repetition", "arm", "bench", "batch_entries", "batch_size", "num",
        "sample_every", "point_group_join_us", "extent_mode", "extent_window_bytes", "exit_code", "ops",
        "puts_per_second", "p50_ms", "p95_ms", "p99_ms", "cpu_seconds", "cpu_us_per_put", "sync_count",
        "puts_per_sync", "data_submit_groups", "data_submit_tickets", "data_submit_avg_tickets",
        "data_submit_max_tickets", "fallocate_calls", "allocated_bytes", "initialized_bytes",
        "preparation_wait_ms", "duration_seconds",
    )
    with (results_dir / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
        csv.DictWriter(stream, fieldnames=summary_fields, delimiter="\t", lineterminator="\n").writeheader()
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
            writer = csv.DictWriter(stream, fieldnames=summary_fields, delimiter="\t", lineterminator="\n")
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
