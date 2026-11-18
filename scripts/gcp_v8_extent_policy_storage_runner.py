#!/usr/bin/env python3
"""Compare V8 extent-policy BTreeMap and Vec storage on one GCP host."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
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
    V8_EXTENT_POLICY_STORAGE_NUM,
    V8_EXTENT_POLICY_STORAGE_WRITERS,
    build_v8_extent_policy_storage_plan,
    build_v8_extent_policy_storage_schedule,
    validate_v8_extent_policy_storage_rows,
)

RUN_TIMEOUT_SECONDS = 300


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_id(row: dict[str, Any]) -> str:
    return (
        f"w{row['writer']}-{row['workload']}-{row['phase']}-r{row['repetition']}-{row['arm']}"
    )


def _summary_fields(row: dict[str, Any]) -> tuple[str, ...]:
    return (
        *row.keys(), "source_commit", "binary_sha256", "exit_code", "ops", "puts_per_second",
        "cpu_seconds", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms", "sync_count",
        "puts_per_sync", "initialized_bytes", "preparation_wait_ms", "extent_policy_storage",
        "scan_diagnostics_enabled", "lookahead_check_calls", "extent_policy_entries_scanned",
        "entries_scanned_per_check", "extent_policy_map_max_entries", "duration_seconds",
    )


def _run_one(
    *, binary: Path, binary_sha256: str, row: dict[str, Any], results_dir: Path,
    db_root: Path, safe_deadline_epoch: int, source_commit: str,
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
    environment = {
        "TOYKV_BENCH_V8_EXTENT_PREP": row["extent_mode"],
        "TOYKV_BENCH_V8_EXTENT_PREP_BYTES": str(row["extent_window_bytes"]),
        "TOYKV_WAL_V8_FRONTIER_MODE": row["frontier_mode"],
        "TOYKV_BENCH_V8_POINT_DIRECT_FRAME": "0",
        "TOYKV_BENCH_V8_EXTENT_POLICY_STORAGE": row["arm"],
        "TOYKV_BENCH_V8_EXTENT_POLICY_SCAN_DIAGNOSTICS": (
            "1" if row["scan_diagnostics_enabled"] else "0"
        ),
    }
    (run_dir / "command.sh").write_text(shlex.join(command) + "\n", encoding="utf-8")
    (run_dir / "environment.txt").write_text(
        "\n".join(f"{key}={value}" for key, value in environment.items()) + "\n", encoding="utf-8",
    )
    (run_dir / "start_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    if int(time.time()) + RUN_TIMEOUT_SECONDS + 330 >= safe_deadline_epoch:
        raise TimeoutError("stopping before the VM safety deadline")

    started = time.monotonic()
    completed = subprocess.run(
        ["timeout", "--signal=INT", "--kill-after=30s", str(RUN_TIMEOUT_SECONDS), "env",
         *(f"{key}={value}" for key, value in environment.items()), *command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    duration = time.monotonic() - started
    (run_dir / "end_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    (run_dir / "result.json").write_text(completed.stdout, encoding="utf-8")
    (run_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")

    metrics: dict[str, Any] = {
        "ops": "", "puts_per_second": "", "cpu_seconds": "", "cpu_us_per_put": "",
        "p50_ms": "", "p95_ms": "", "p99_ms": "", "sync_count": "", "puts_per_sync": "",
        "initialized_bytes": "", "preparation_wait_ms": "", "extent_policy_storage": "",
        "scan_diagnostics_enabled": bool(row["scan_diagnostics_enabled"]),
        "lookahead_check_calls": "", "extent_policy_entries_scanned": "",
        "entries_scanned_per_check": "", "extent_policy_map_max_entries": "",
    }
    exit_code = completed.returncode
    if exit_code == 0:
        try:
            record = json.loads(completed.stdout)
            result = record["result"]
            pipeline = result["wal_pipeline"]
            cpu_seconds = (
                float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])
            ) / 1000
            start_prep = pipeline["wal_io_start"]["sliding_extent_preparation"]
            end_prep = pipeline["wal_io_end"]["sliding_extent_preparation"]
            diagnostics_enabled = bool(row["scan_diagnostics_enabled"])
            check_calls = int(end_prep["lookahead_check_calls"]) - int(start_prep["lookahead_check_calls"])
            entries_scanned = (
                int(end_prep["extent_policy_entries_scanned"])
                - int(start_prep["extent_policy_entries_scanned"])
            )
            initialized_bytes = int(end_prep["initialized_bytes"]) - int(start_prep["initialized_bytes"])
            sync_count = int(pipeline["sync_count"])
            metrics = {
                "ops": int(result["ops"]),
                "puts_per_second": float(result["ops_per_sec"]),
                "cpu_seconds": cpu_seconds,
                "cpu_us_per_put": cpu_seconds * 1_000_000 / int(result["ops"]),
                "p50_ms": float(record["latency"]["p50_ms"]),
                "p95_ms": float(record["latency"]["p95_ms"]),
                "p99_ms": float(record["latency"]["p99_ms"]),
                "sync_count": sync_count,
                "puts_per_sync": int(result["ops"]) / sync_count if sync_count else 0,
                "initialized_bytes": initialized_bytes,
                "preparation_wait_ms": (
                    int(end_prep["wait_ns"]) - int(start_prep["wait_ns"])
                ) / 1_000_000,
                "extent_policy_storage": end_prep["extent_policy_storage"],
                "scan_diagnostics_enabled": end_prep["extent_policy_scan_diagnostics_enabled"],
                "lookahead_check_calls": check_calls,
                "extent_policy_entries_scanned": entries_scanned,
                "entries_scanned_per_check": entries_scanned / check_calls if check_calls else 0,
                "extent_policy_map_max_entries": int(end_prep["extent_policy_map_max_entries"]),
            }
            if int(result["ops"]) != row["num"] or any(
                record["params"].get(key) != value for key, value in {
                    "num": row["num"],
                    "threads": row["writer"],
                    "value_size": 1024,
                    "batch_size": row["batch_size"],
                    "wal_format_version": 8,
                    "wal_io_mode": "parallel",
                }.items()
            ):
                raise ValueError("benchmark parameters do not match the scheduled run")
            if pipeline.get("wal_io_stable") is not True:
                raise ValueError("WAL I/O metadata changed during the run")
            for boundary in ("wal_io_start", "wal_io_end"):
                preparation = pipeline[boundary]["sliding_extent_preparation"]
                if (
                    preparation.get("mode") != row["extent_mode"]
                    or preparation.get("window_bytes") != POINT_LOOKAHEAD_WINDOW_BYTES
                    or preparation.get("extent_policy_storage") != row["arm"]
                    or preparation.get("extent_policy_scan_diagnostics_enabled")
                    is not diagnostics_enabled
                ):
                    raise ValueError(f"{boundary} extent-policy configuration differs from the plan")
            if (initialized_bytes > 0) != (row["workload"] == "point"):
                raise ValueError("extent initialization does not match Point/Batch workload")
            if sync_count <= 0:
                raise ValueError("benchmark reported no WAL syncs")
            for metric in ("puts_per_second", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms"):
                if not math.isfinite(float(metrics[metric])) or float(metrics[metric]) < 0:
                    raise ValueError(f"benchmark returned invalid {metric}")
            if not diagnostics_enabled and (check_calls != 0 or entries_scanned != 0):
                raise ValueError("score run unexpectedly collected extent-policy scan counters")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            exit_code = 65
            (run_dir / "validation-error.txt").write_text(f"invalid benchmark result: {error}\n", encoding="utf-8")

    metadata = {
        **row,
        "source_commit": source_commit,
        "binary_sha256": binary_sha256,
        "exit_code": int(exit_code),
        **metrics,
        "duration_seconds": duration,
    }
    (run_dir / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"run={name} source={source_commit[:12]} exit={exit_code} duration={duration:.3f}s", flush=True)
    return metadata


def _summary(rows: list[dict[str, Any]], writers: tuple[int, ...], planned: int) -> dict[str, Any]:
    validation = validate_v8_extent_policy_storage_rows(rows, writers)
    comparison: list[dict[str, Any]] = []
    cells = [(writer, "point") for writer in writers] + [(16, "batch16"), (16, "batch64")]
    metrics = (
        "puts_per_second", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms", "sync_count",
        "puts_per_sync", "initialized_bytes", "preparation_wait_ms",
    )
    for writer, workload in cells:
        selected = {
            arm: [
                row for row in rows
                if row["phase"] == "score" and row["writer"] == writer and row["workload"] == workload
                and row["arm"] == arm and row["exit_code"] == 0
            ]
            for arm in ("btree", "vec")
        }
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
        paired_changes = []
        for repetition in range(1, 6):
            pair = {
                arm: next((row for row in rows if row["phase"] == "score" and row["writer"] == writer
                           and row["workload"] == workload and row["repetition"] == repetition
                           and row["arm"] == arm and row["exit_code"] == 0), None)
                for arm in ("btree", "vec")
            }
            if pair["btree"] and pair["vec"]:
                paired_changes.append(
                    (pair["vec"]["puts_per_second"] / pair["btree"]["puts_per_second"] - 1) * 100
                )
        btree_rate = stats["btree"]["puts_per_second"]
        vec_rate = stats["vec"]["puts_per_second"]
        comparison.append({
            "writer": writer,
            "workload": workload,
            "btree": stats["btree"],
            "vec": stats["vec"],
            "median_throughput_change_pct": (vec_rate / btree_rate - 1) * 100 if btree_rate and vec_rate else None,
            "paired_throughput_changes_pct": paired_changes,
            "vec_pair_wins": sum(change > 0 for change in paired_changes),
        })

    diagnostics = []
    for writer in writers:
        pair = {
            arm: next((row for row in rows if row["phase"] == "diagnostic" and row["writer"] == writer
                       and row["arm"] == arm and row["exit_code"] == 0), None)
            for arm in ("btree", "vec")
        }
        btree = pair["btree"]
        vector = pair["vec"]
        diagnostics.append({
            "writer": writer,
            "btree": btree,
            "vec": vector,
            "entries_scanned_per_check_change_pct": (
                (vector["entries_scanned_per_check"] / btree["entries_scanned_per_check"] - 1) * 100
                if btree and vector and btree["entries_scanned_per_check"] else None
            ),
        })
    return {
        "suite": "v8-extent-policy-storage",
        "complete": len(rows) == planned and validation["complete"],
        "runs_completed": len(rows),
        "runs_planned": planned,
        "writers": list(writers),
        "arms": ["btree", "vec"],
        "results": rows,
        "matrix_validation": validation,
        "comparison": comparison,
        "scan_diagnostics": diagnostics,
        "qualification_note": (
            "Throughput score runs disable scan counters; separate diagnostic runs enable them. "
            "BTreeMap and Vec share one source revision, binary, host, extent strategy, FRONTIER mode, "
            "and dynamic coalescing configuration."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    if len(args.commit) != 40 or any(char not in "0123456789abcdef" for char in args.commit):
        raise ValueError("commit must be a full lowercase 40-character SHA")
    actual_sha = _sha256_file(args.binary)
    if actual_sha != args.binary_sha256:
        raise ValueError("write-perf binary SHA-256 does not match the supplied identity")
    if args.machine_type != "c4-standard-16-lssd" or args.ssd_count != 2 or args.ssd_gib != 750:
        raise ValueError("extent-policy storage qualification requires c4-standard-16-lssd and its two-SSD layout")
    writers = tuple(int(value) for value in args.writers.split(","))
    if writers != V8_EXTENT_POLICY_STORAGE_WRITERS:
        raise ValueError(f"extent-policy storage qualification requires writers {V8_EXTENT_POLICY_STORAGE_WRITERS}")

    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    args.db_root.mkdir(parents=True, exist_ok=True)
    machine_spec = {"vcpus": 16, "ssd_count": args.ssd_count, "ssd_gib": args.ssd_gib}
    schedule = build_v8_extent_policy_storage_schedule(writers)
    plan = build_v8_extent_policy_storage_plan(
        writers=writers,
        machine_type=args.machine_type,
        machine_spec=machine_spec,
        commit=args.commit,
        binary_sha256=args.binary_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    (results_dir / "commit.txt").write_text(args.commit + "\n", encoding="utf-8")
    (results_dir / "write-perf.sha256").write_text(args.binary_sha256 + "\n", encoding="utf-8")
    with (results_dir / "extent-policy-storage-schedule.tsv").open(
        "w", encoding="utf-8", newline="",
    ) as stream:
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
                row={**row},
                results_dir=results_dir,
                db_root=args.db_root,
                safe_deadline_epoch=args.safe_deadline_epoch,
                source_commit=args.commit,
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
