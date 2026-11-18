#!/usr/bin/env python3
"""Run paired V7/V8 Point diagnostics with sampled hot-mutex statistics."""

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
    V8_MUTEX_DIAGNOSTIC_NUM,
    V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY,
    V8_MUTEX_DIAGNOSTIC_WRITERS,
    build_v8_mutex_diagnostic_plan,
    build_v8_mutex_diagnostic_schedule,
    validate_v8_mutex_diagnostic_rows,
)
from gcp_wal_profile_runner import _validate_wal_io_metadata

RUN_TIMEOUT_SECONDS = 900
LOCK_NAMES = (
    "resource_ledger",
    "reservation_owner",
    "v8_extent_state",
    "v8_extent_preparation",
    "mvcc_publication",
)


def _run_id(row: dict[str, Any]) -> str:
    return f"w{row['writer']}-{row['workload']}-r{row['repetition']}-{row['arm']}"


def _build_command(binary: Path, row: dict[str, Any], db_root: Path) -> list[str]:
    version = 7 if row["arm"] == "v7_parallel" else 8
    return [
        str(binary), "--bench", "wal_concurrent", "--wal", "--pitr", "--pitr-format", f"v{version}",
        "--wal-io-mode", "parallel", "--num", str(V8_MUTEX_DIAGNOSTIC_NUM), "--threads", str(row["writer"]),
        "--value-size", "1024", "--target-sst-size", "2147483648", "--compaction", "none",
        "--output", "json", "--latency-sample-every", "100",
        "--mutex-lock-sample-every", str(V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY),
        "--path", str(db_root / _run_id(row)),
    ]


def _parse_record(stdout: str) -> dict[str, Any]:
    records = []
    for line in stdout.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and all(key in value for key in ("result", "params", "latency")):
            records.append(value)
    if len(records) != 1:
        raise ValueError(f"expected one benchmark JSON record, found {len(records)}")
    return records[0]


def _validate_record(record: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    result = record["result"]
    params = record["params"]
    pipeline = result["wal_pipeline"]
    version = 7 if row["arm"] == "v7_parallel" else 8
    expected = {
        "num": V8_MUTEX_DIAGNOSTIC_NUM,
        "threads": row["writer"],
        "value_size": 1024,
        "wal_format_version": version,
        "wal_io_mode": "parallel",
    }
    if int(result["ops"]) != V8_MUTEX_DIAGNOSTIC_NUM or any(params.get(key) != value for key, value in expected.items()):
        raise ValueError("benchmark parameters do not match the planned run")
    if version == 8:
        if pipeline.get("wal_io_stable") is not True:
            raise ValueError("WAL I/O metadata changed during the run")
        _validate_wal_io_metadata(pipeline, 8, expected_frontier_mode="direct")
        for boundary in ("wal_io_start", "wal_io_end"):
            preparation = pipeline[boundary]["sliding_extent_preparation"]
            if preparation.get("mode") != "sliding-point-lookahead" or preparation.get("window_bytes") != 1 << 20:
                raise ValueError(f"{boundary} does not report the planned Point lookahead preparation")

    diagnostic = result.get("mutex_lock_diagnostics")
    if not isinstance(diagnostic, dict) or diagnostic.get("sample_every") != V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY:
        raise ValueError("mutex diagnostics are missing or have the wrong sampling interval")
    if diagnostic.get("histogram") != "log2_upper_bound_nanoseconds":
        raise ValueError("mutex histogram format is not the planned log2 upper-bound format")
    if diagnostic.get("acquisition_scope") != (
        "instrumented synchronous WAL resource/extent and MVCC publication call sites"
    ):
        raise ValueError("mutex instrumentation scope changed")
    locks = diagnostic.get("locks")
    if not isinstance(locks, list) or {lock.get("name") for lock in locks if isinstance(lock, dict)} != set(LOCK_NAMES):
        raise ValueError("mutex diagnostics do not contain exactly the planned lock classes")
    for lock in locks:
        if not isinstance(lock, dict):
            raise ValueError("mutex diagnostic entry is not an object")
        for name in ("acquisition_count", "sampled_acquisition_count", "sampled_contended_count",
                     "estimated_contended_count", "sampled_wait_ns_total", "estimated_wait_ns_total",
                     "sampled_hold_ns_total", "estimated_hold_ns_total"):
            if type(lock.get(name)) is not int or lock[name] < 0:
                raise ValueError(f"mutex counter {lock.get('name')}.{name} is invalid")
        if lock["sampled_acquisition_count"] > lock["acquisition_count"]:
            raise ValueError(f"mutex sample count exceeds acquisitions for {lock['name']}")
        if lock["sampled_contended_count"] > lock["sampled_acquisition_count"]:
            raise ValueError(f"mutex contention sample count is invalid for {lock['name']}")

    throughput = float(result["ops_per_sec"])
    cpu_seconds = (float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])) / 1000
    values = {
        "ops": int(result["ops"]),
        "puts_per_second": throughput,
        "cpu_seconds": cpu_seconds,
        "cpu_us_per_put": cpu_seconds * 1_000_000 / int(result["ops"]),
        "sync_count": int(pipeline["sync_count"]),
        "puts_per_sync": int(result["ops"]) / max(1, int(pipeline["sync_count"])),
        "p50_ms": float(record["latency"]["p50_ms"]),
        "p95_ms": float(record["latency"]["p95_ms"]),
        "p99_ms": float(record["latency"]["p99_ms"]),
        "mutex_lock_diagnostics": diagnostic,
    }
    if any(not math.isfinite(value) or value < 0 for key, value in values.items() if key != "mutex_lock_diagnostics"):
        raise ValueError("benchmark metrics contain a non-finite or negative value")
    return values


def _run_one(
    *, binary: Path, row: dict[str, Any], results_dir: Path, db_root: Path, safe_deadline_epoch: int,
) -> dict[str, Any]:
    run_id = _run_id(row)
    run_dir = results_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    command = _build_command(binary, row, db_root)
    env: dict[str, str] = {}
    if row["arm"] == "v8_point_lookahead_direct":
        env = {
            "TOYKV_BENCH_V8_EXTENT_PREP": "sliding-point-lookahead",
            "TOYKV_BENCH_V8_EXTENT_PREP_BYTES": "1048576",
            "TOYKV_WAL_V8_FRONTIER_MODE": "direct",
        }
    (run_dir / "command.sh").write_text(shlex.join(command) + "\n", encoding="utf-8")
    (run_dir / "environment.txt").write_text(
        ("\n".join(f"{key}={value}" for key, value in env.items()) + "\n") if env else "<V7 defaults>\n",
        encoding="utf-8",
    )
    (run_dir / "start_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    if int(time.time()) + RUN_TIMEOUT_SECONDS + 330 >= safe_deadline_epoch:
        raise TimeoutError("stopping before the VM safety deadline")

    started = time.monotonic()
    completed = subprocess.run(
        ["timeout", "--signal=INT", "--kill-after=30s", str(RUN_TIMEOUT_SECONDS), "env",
         *(f"{key}={value}" for key, value in env.items()), *command],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False,
    )
    duration = time.monotonic() - started
    (run_dir / "end_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    (run_dir / "result.json").write_text(completed.stdout, encoding="utf-8")
    (run_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")

    values: dict[str, Any] = {}
    exit_code = completed.returncode
    if exit_code == 0:
        try:
            values = _validate_record(_parse_record(completed.stdout), row)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            exit_code = 65
            (run_dir / "validation-error.txt").write_text(f"invalid benchmark result: {error}\n", encoding="utf-8")
    metadata = {
        **{key: row[key] for key in ("writer", "workload", "repetition", "arm", "bench", "num", "sample_every")},
        "exit_code": int(exit_code), **values, "duration_seconds": duration,
    }
    (run_dir / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"run={run_id} exit={exit_code} duration={duration:.3f}s", flush=True)
    return metadata


def _summarize(rows: list[dict[str, Any]], planned: int) -> dict[str, Any]:
    validation = validate_v8_mutex_diagnostic_rows(rows)
    by_arm: dict[str, Any] = {}
    for arm in ("v7_parallel", "v8_point_lookahead_direct"):
        runs = [row for row in rows if row.get("arm") == arm and row.get("exit_code") == 0]
        metrics: dict[str, Any] = {}
        for name in ("puts_per_second", "cpu_us_per_put", "sync_count", "puts_per_sync", "p50_ms", "p95_ms", "p99_ms"):
            values = [float(row[name]) for row in runs if name in row]
            metrics[name] = median(values) if values else None
        locks: dict[str, Any] = {}
        for lock_name in LOCK_NAMES:
            lock_runs = [
                next(lock for lock in row["mutex_lock_diagnostics"]["locks"] if lock["name"] == lock_name)
                for row in runs if isinstance(row.get("mutex_lock_diagnostics"), dict)
            ]
            locks[lock_name] = {
                metric: median(float(lock[metric]) for lock in lock_runs if lock.get(metric) is not None)
                if any(lock.get(metric) is not None for lock in lock_runs)
                else None
                for metric in (
                    "acquisition_count", "sampled_acquisition_count", "sampled_contended_count",
                    "estimated_contended_count", "wait_p50_ns", "wait_p95_ns", "wait_p99_ns",
                    "estimated_wait_ns_total", "hold_p50_ns", "hold_p95_ns", "hold_p99_ns",
                    "estimated_hold_ns_total",
                )
            }
        by_arm[arm] = {"runs": len(runs), **metrics, "locks": locks}

    paired: dict[str, Any] = {}
    v7_by_rep = {row["repetition"]: row for row in rows if row.get("arm") == "v7_parallel"}
    v8_by_rep = {row["repetition"]: row for row in rows if row.get("arm") == "v8_point_lookahead_direct"}
    for metric in ("puts_per_second", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms"):
        changes = []
        for repetition in sorted(set(v7_by_rep) & set(v8_by_rep)):
            left, right = v7_by_rep[repetition], v8_by_rep[repetition]
            if left.get("exit_code") == right.get("exit_code") == 0 and float(left[metric]) != 0:
                changes.append((float(right[metric]) / float(left[metric]) - 1) * 100)
        paired[metric] = {"changes_pct": changes, "median_change_pct": median(changes) if changes else None}
    return {
        "suite": "v8-mutex-diagnostic", "diagnostic_only": True,
        "complete": len(rows) == planned and validation["complete"],
        "runs_completed": len(rows), "runs_planned": planned,
        "writers": list(V8_MUTEX_DIAGNOSTIC_WRITERS),
        "sample_every": V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY,
        "results": rows, "matrix_validation": validation, "by_arm": by_arm, "paired_comparison": paired,
        "interpretation_note": (
            "Acquisition counts are exact for the instrumented synchronous call sites. Contended counts and "
            "wait/hold totals are sample-scaled estimates; percentile histograms are log2 bucket upper bounds. "
            "This is an instrumented diagnostic, not an unprofiled throughput score. The V7/V8 implementations "
            "do not exercise identical lock classes, so compare only semantically shared locks directly."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    args.results_dir.mkdir(parents=True, exist_ok=True)
    args.db_root.mkdir(parents=True, exist_ok=True)
    schedule = build_v8_mutex_diagnostic_schedule()
    machine_spec = {
        "vcpus": int(args.machine_type.split("-")[2]), "ssd_count": args.ssd_count, "ssd_gib": args.ssd_gib,
    }
    plan = build_v8_mutex_diagnostic_plan(
        machine_type=args.machine_type, machine_spec=machine_spec,
        commit=args.commit, binary_sha256=args.binary_sha256,
    )
    (args.results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with (args.results_dir / "v8-mutex-diagnostic-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(schedule)

    rows: list[dict[str, Any]] = []
    failed = False
    for index, item in enumerate(schedule, start=1):
        try:
            row = _run_one(
                binary=args.binary, row=item, results_dir=args.results_dir, db_root=args.db_root,
                safe_deadline_epoch=args.safe_deadline_epoch,
            )
        except (OSError, TimeoutError, subprocess.SubprocessError) as error:
            failed = True
            (args.results_dir / "runner-error.txt").write_text(f"run {index} did not finish: {error}\n", encoding="utf-8")
            break
        rows.append(row)
        if row["exit_code"] != 0:
            failed = True
            break

    summary = _summarize(rows, len(schedule))
    summary["complete"] = summary["complete"] and not failed
    summary["source_commit"] = args.commit
    summary["binary_sha256"] = args.binary_sha256
    (args.results_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    with (args.results_dir / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
        fields = ("writer", "workload", "repetition", "arm", "exit_code", "ops", "puts_per_second",
                  "cpu_us_per_put", "sync_count", "puts_per_sync", "p50_ms", "p95_ms", "p99_ms", "duration_seconds")
        writer = csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows({key: row.get(key, "") for key in fields} for row in rows)
    (args.results_dir / "remote-exit.json").write_text(
        json.dumps({"failed": failed or len(rows) != len(schedule), "runs_started": len(rows)}, indent=2) + "\n",
        encoding="utf-8",
    )
    (args.results_dir / "finished_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    return 0 if summary["complete"] else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--safe-deadline-epoch", type=int, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--machine-type", required=True)
    parser.add_argument("--ssd-count", type=int, required=True)
    parser.add_argument("--ssd-gib", type=int, required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run_suite(parse_args()))
