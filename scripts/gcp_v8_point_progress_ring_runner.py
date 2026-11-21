#!/usr/bin/env python3
"""Run the RFC 027 F0.5 same-binary V8 Point progress-ring qualification."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
from statistics import median
from typing import Any

from gcp_wal_matrix import (
    POINT_LOOKAHEAD_WINDOW_BYTES,
    POINT_PROGRESS_RING_NUM,
    POINT_PROGRESS_RING_WRITERS,
    build_point_progress_ring_plan,
    build_point_progress_ring_schedule,
    validate_point_progress_ring_rows,
)

RUN_TIMEOUT_SECONDS = 300
UNSET_EXPERIMENT_VARIABLES = (
    "TOYKV_WAL_V8_COALESCE_BUDGET_US",
    "TOYKV_BENCH_WAL_CPU_ATTRIBUTION_SAMPLE_EVERY",
    "TOYKV_BENCH_WAL_SYNC_DIAGNOSTICS",
    "TOYKV_BENCH_WAL_HOL_DIAGNOSTICS",
    "TOYKV_BENCH_WAL_COMPLETION_TIMELINE_SAMPLE_EVERY",
    "TOYKV_BENCH_WAL_V8_DURABILITY_SAMPLE_EVERY",
    "TOYKV_BENCH_AFFINITY_MODE",
    "TOYKV_BENCH_IOWQ_AFFINITY",
    "TOYKV_BENCH_WAL_IO_WORKER_CPU",
)
FIXED_ENVIRONMENT = {
    "TOYKV_BENCH_V8_EXTENT_PREP": "sliding-point-lookahead",
    "TOYKV_BENCH_V8_EXTENT_PREP_BYTES": str(POINT_LOOKAHEAD_WINDOW_BYTES),
    "TOYKV_WAL_V8_FRONTIER_MODE": "direct",
    "TOYKV_BENCH_V8_POINT_DIRECT_FRAME": "0",
    "TOYKV_BENCH_V8_POINT_FRONTIER_CREDIT_BATCH": "0",
    "TOYKV_BENCH_V8_EXTENT_POLICY_STORAGE": "vec",
    "TOYKV_BENCH_V8_EXTENT_POLICY_SCAN_DIAGNOSTICS": "0",
    "TOYKV_BENCH_V8_DATA_SUBMIT_ORDER": "ordered",
    "TOYKV_BENCH_WAL_WORKER_GROUP_HISTOGRAM": "0",
}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_id(row: dict[str, Any]) -> str:
    return f"r{row['repetition']}-w{row['writer']}-{row['workload']}-{row['arm']}"


def _summary_fields(schedule: list[dict[str, Any]]) -> tuple[str, ...]:
    return (
        *schedule[0].keys(), "source_commit", "binary_sha256", "exit_code", "ops",
        "puts_per_second", "cpu_seconds", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms",
        "latency_sample_every", "latency_samples", "latency_unsampled_operations", "sync_count",
        "puts_per_sync",
        "ordered_link_hash_cpu_ns", "data_o_direct", "frontier_o_direct", "sync_o_direct",
        "progress_ring_start", "progress_ring_end", "extent_scan_diagnostics_start",
        "extent_scan_diagnostics_end", "initialized_bytes", "duration_seconds",
    )


def _environment(progress_ring_enabled: bool) -> dict[str, str]:
    # An allowlist makes the candidate switch the only inherited experiment variable.
    return {
        "PATH": os.environ.get("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"),
        "HOME": os.environ.get("HOME", "/home/bench"),
        "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
        "LC_ALL": "C",
        **FIXED_ENVIRONMENT,
        "TOYKV_BENCH_V8_POINT_PROGRESS_RING": "1" if progress_ring_enabled else "0",
    }


def _expected_environment_text(progress_ring_enabled: bool) -> str:
    controlled = {
        key: value for key, value in _environment(progress_ring_enabled).items()
        if key not in ("PATH", "HOME", "TMPDIR", "LC_ALL")
    }
    controlled.update({key: "<unset>" for key in UNSET_EXPERIMENT_VARIABLES})
    return "\n".join(f"{key}={value}" for key, value in sorted(controlled.items())) + "\n"


def _validate_record(record: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    result = record["result"]
    params = record["params"]
    expected_params = {
        "num": POINT_PROGRESS_RING_NUM,
        "threads": row["writer"],
        "value_size": 1024,
        "batch_size": row["batch_size"],
        "wal_format_version": 8,
        "wal_io_mode": "parallel",
    }
    if result.get("ops") != POINT_PROGRESS_RING_NUM or any(
        params.get(key) != value for key, value in expected_params.items()
    ):
        raise ValueError("benchmark parameters do not match the fixed progress-ring plan")

    pipeline = result["wal_pipeline"]
    if pipeline.get("wal_io_stable") is not True:
        raise ValueError("WAL I/O metadata changed during the run")
    io_by_boundary: dict[str, dict[str, Any]] = {}
    for boundary in ("wal_io_start", "wal_io_end"):
        snapshot = pipeline[boundary]
        if snapshot.get("progress_ring_enabled") is not bool(row["progress_ring_enabled"]):
            raise ValueError(f"{boundary} progress-ring mode differs from the planned arm")
        preparation = snapshot.get("sliding_extent_preparation")
        if not isinstance(preparation, dict) or any((
            preparation.get("mode") != "sliding-point-lookahead",
            preparation.get("window_bytes") != POINT_LOOKAHEAD_WINDOW_BYTES,
            preparation.get("extent_policy_storage") != "vec",
            preparation.get("extent_policy_scan_diagnostics_enabled") is not False,
        )):
            raise ValueError(f"{boundary} extent settings do not match the fixed plan")
        handles: dict[str, Any] = {}
        for name in ("data", "frontier", "sync"):
            handle = snapshot.get(name)
            if not isinstance(handle, dict):
                raise ValueError(f"{boundary} is missing {name} I/O metadata")
            if type(handle.get("o_direct")) is not bool or any(
                type(handle.get(key)) is not int or handle[key] <= 0 for key in ("device", "inode")
            ):
                raise ValueError(f"{boundary} {name} I/O metadata is invalid")
            handles[name] = handle
        if not handles["data"]["o_direct"] or not handles["frontier"]["o_direct"]:
            raise ValueError(f"{boundary} did not use O_DIRECT for DATA and Direct FRONTIER")
        io_by_boundary[boundary] = handles
    if io_by_boundary["wal_io_start"] != io_by_boundary["wal_io_end"]:
        raise ValueError("DATA, FRONTIER, or sync descriptor metadata changed during the run")

    if pipeline.get("worker_group_sizes") is not None:
        raise ValueError("DATA worker-group histogram must be disabled for scored runs")

    digest = result.get("wal_v8_digest")
    if not isinstance(digest, dict) or digest.get("cpu_timing_sample_rate") != 64:
        raise ValueError("ordered-link hash CPU sample metadata is missing or incorrect")
    hash_cpu = digest.get("ordered_link_hash_cpu_ns")
    sampled_tickets = digest.get("cpu_timing_sampled_tickets")
    if type(hash_cpu) is not int or hash_cpu < 0 or type(sampled_tickets) is not int or sampled_tickets <= 0:
        raise ValueError("ordered-link hash CPU metrics are invalid")

    cpu_ms = float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])
    ops_per_second = float(result["ops_per_sec"])
    if not math.isfinite(cpu_ms) or cpu_ms < 0 or not math.isfinite(ops_per_second) or ops_per_second <= 0:
        raise ValueError("process CPU or throughput metric is invalid")
    sync_count = pipeline.get("sync_count")
    if type(sync_count) is not int or sync_count <= 0:
        raise ValueError("WAL sync count is invalid")

    is_point = row["workload"] == "point"
    latency = record.get("latency")
    if is_point:
        if not isinstance(latency, dict):
            raise ValueError("Point result is missing latency samples")
        samples = latency.get("samples")
        unsampled = latency.get("unsampled_completed_operations")
        if (
            latency.get("sample_every") != 10
            or type(samples) is not int
            or samples != POINT_PROGRESS_RING_NUM // 10
            or type(unsampled) is not int
            or unsampled + samples != POINT_PROGRESS_RING_NUM
        ):
            raise ValueError("Point latency sample count does not match the 1-in-10 plan")
        latency_metrics = {key: float(latency[key]) for key in ("p50_ms", "p95_ms", "p99_ms")}
        if any(not math.isfinite(value) or value <= 0 for value in latency_metrics.values()):
            raise ValueError("Point latency percentiles are invalid")
        sample_every, sample_count, unsampled_count = 10, samples, unsampled
    else:
        if latency is not None:
            raise ValueError("Batch64 should not collect per-Put latency samples")
        latency_metrics = {"p50_ms": None, "p95_ms": None, "p99_ms": None}
        sample_every = sample_count = unsampled_count = None

    start_prep = pipeline["wal_io_start"]["sliding_extent_preparation"]
    end_prep = pipeline["wal_io_end"]["sliding_extent_preparation"]
    initialized = end_prep.get("initialized_bytes", 0) - start_prep.get("initialized_bytes", 0)
    if type(initialized) is not int or initialized < 0 or (initialized > 0) is not is_point:
        raise ValueError("Lookahead initialization differs from the Point/Batch plan")

    return {
        "ops": int(result["ops"]),
        "puts_per_second": ops_per_second,
        "cpu_seconds": cpu_ms / 1000,
        "cpu_us_per_put": cpu_ms * 1000 / POINT_PROGRESS_RING_NUM,
        **latency_metrics,
        "latency_sample_every": sample_every,
        "latency_samples": sample_count,
        "latency_unsampled_operations": unsampled_count,
        "sync_count": sync_count,
        "puts_per_sync": POINT_PROGRESS_RING_NUM / sync_count,
        "ordered_link_hash_cpu_ns": hash_cpu,
        "data_o_direct": io_by_boundary["wal_io_end"]["data"]["o_direct"],
        "frontier_o_direct": io_by_boundary["wal_io_end"]["frontier"]["o_direct"],
        "sync_o_direct": io_by_boundary["wal_io_end"]["sync"]["o_direct"],
        "progress_ring_start": pipeline["wal_io_start"]["progress_ring_enabled"],
        "progress_ring_end": pipeline["wal_io_end"]["progress_ring_enabled"],
        "extent_scan_diagnostics_start": start_prep["extent_policy_scan_diagnostics_enabled"],
        "extent_scan_diagnostics_end": end_prep["extent_policy_scan_diagnostics_enabled"],
        "initialized_bytes": initialized,
    }


def _run_one(
    *, binary: Path, binary_sha256: str, row: dict[str, Any], results_dir: Path,
    db_root: Path, safe_deadline_epoch: int,
) -> dict[str, Any]:
    name = _run_id(row)
    run_dir = results_dir / name
    run_dir.mkdir(parents=True, exist_ok=False)
    command = [
        str(binary), "--bench", str(row["bench"]), "--wal", "--pitr", "--pitr-format", "v8",
        "--wal-io-mode", "parallel", "--num", str(row["num"]), "--threads", str(row["writer"]),
        "--value-size", "1024", "--target-sst-size", "2147483648", "--compaction", "none",
        "--output", "json", "--path", str(db_root / name),
    ]
    if row["workload"] == "point":
        command.extend(("--latency-sample-every", str(row["sample_every"])))
    else:
        command.extend(("--wal-batch-size", str(row["batch_entries"])))
    environment = _environment(bool(row["progress_ring_enabled"]))
    (run_dir / "command.sh").write_text(shlex.join(command) + "\n", encoding="utf-8")
    (run_dir / "environment.txt").write_text(
        _expected_environment_text(bool(row["progress_ring_enabled"])), encoding="utf-8",
    )
    (run_dir / "start_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    if int(time.time()) + RUN_TIMEOUT_SECONDS + 330 >= safe_deadline_epoch:
        raise TimeoutError("stopping before the VM safety deadline")

    started = time.monotonic()
    completed = subprocess.run(
        ["timeout", "--signal=INT", "--kill-after=30s", str(RUN_TIMEOUT_SECONDS), *command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
        env=environment,
    )
    duration = time.monotonic() - started
    (run_dir / "end_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    (run_dir / "result.json").write_text(completed.stdout, encoding="utf-8")
    (run_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")
    metadata: dict[str, Any] = {
        **row,
        "source_commit": row["source_commit"],
        "binary_sha256": binary_sha256,
        "exit_code": completed.returncode,
        "duration_seconds": duration,
    }
    try:
        if completed.returncode != 0:
            raise ValueError(f"write-perf exited with status {completed.returncode}")
        metadata.update(_validate_record(json.loads(completed.stdout), row))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        metadata["exit_code"] = 65 if completed.returncode == 0 else completed.returncode
        (run_dir / "validation-error.txt").write_text(f"invalid benchmark result: {error}\n", encoding="utf-8")
    (run_dir / "run-metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    print(f"run={name} exit={metadata['exit_code']} duration={duration:.3f}s", flush=True)
    return metadata


def _summary(rows: list[dict[str, Any]], writers: tuple[int, ...], planned: int) -> dict[str, Any]:
    validation = validate_point_progress_ring_rows(rows, writers)
    cells = [(writer, "point") for writer in writers] + [(16, "batch64")]
    comparisons: list[dict[str, Any]] = []
    for writer, workload in cells:
        selected = {
            arm: [
                row for row in rows
                if row.get("writer") == writer and row.get("workload") == workload
                and row.get("arm") == arm and row.get("exit_code") == 0
            ]
            for arm in ("eager", "progress_ring")
        }
        metrics = (
            "puts_per_second", "cpu_us_per_put", "p50_ms", "p95_ms", "p99_ms", "sync_count",
            "puts_per_sync", "ordered_link_hash_cpu_ns",
        )
        stats = {
            arm: {
                "runs": len(arm_rows),
                **{
                    metric: median(float(row[metric]) for row in arm_rows)
                    if arm_rows and all(row.get(metric) is not None for row in arm_rows) else None
                    for metric in metrics
                },
            }
            for arm, arm_rows in selected.items()
        }
        paired: list[float] = []
        paired_p99: list[float] = []
        for repetition in range(1, 6):
            pair = {
                arm: next((row for row in rows if row.get("writer") == writer
                           and row.get("workload") == workload and row.get("repetition") == repetition
                           and row.get("arm") == arm and row.get("exit_code") == 0), None)
                for arm in ("eager", "progress_ring")
            }
            baseline, candidate = pair["eager"], pair["progress_ring"]
            if baseline and candidate and baseline["puts_per_second"] > 0:
                paired.append((candidate["puts_per_second"] / baseline["puts_per_second"] - 1) * 100)
                if workload == "point":
                    paired_p99.append((candidate["p99_ms"] / baseline["p99_ms"] - 1) * 100)
        baseline_median = stats["eager"]["puts_per_second"]
        candidate_median = stats["progress_ring"]["puts_per_second"]
        comparisons.append({
            "writer": writer,
            "workload": workload,
            "baseline": stats["eager"],
            "candidate": stats["progress_ring"],
            "median_throughput_change_pct": (
                (candidate_median / baseline_median - 1) * 100
                if baseline_median and candidate_median else None
            ),
            "paired_throughput_changes_pct": paired,
            "paired_throughput_change_median_pct": median(paired) if paired else None,
            "candidate_pair_wins": sum(value > 0 for value in paired),
            "paired_p99_changes_pct": paired_p99,
            "paired_p99_change_median_pct": median(paired_p99) if paired_p99 else None,
        })
    by_cell = {f"w{row['writer']}-{row['workload']}": row for row in comparisons}
    point_gates = [
        by_cell[f"w{writer}-point"]["paired_throughput_change_median_pct"] is not None
        and by_cell[f"w{writer}-point"]["paired_throughput_change_median_pct"] >= 5
        and by_cell[f"w{writer}-point"]["candidate_pair_wins"] >= 4
        and by_cell[f"w{writer}-point"]["paired_p99_change_median_pct"] is not None
        and by_cell[f"w{writer}-point"]["paired_p99_change_median_pct"] <= 5
        for writer in (8, 16)
    ]
    point1 = by_cell["w1-point"]["paired_throughput_change_median_pct"]
    batch64 = by_cell["w16-batch64"]["paired_throughput_change_median_pct"]
    guardrails = point1 is not None and point1 >= -2 and batch64 is not None and batch64 >= -2
    return {
        "suite": "v8-point-progress-ring",
        "complete": len(rows) == planned and validation["complete"],
        "runs_started": len(rows),
        "runs_completed": sum(row.get("exit_code") == 0 for row in rows),
        "runs_planned": planned,
        "writers": list(writers),
        "workloads": ["point", "batch64"],
        "arms": ["eager", "progress_ring"],
        "results": rows,
        "matrix_validation": validation,
        "comparison": comparisons,
        "comparison_by_cell": by_cell,
        "qualification_gates": {
            "point8_and_point16_each_gain_at_least_5pct_and_win_4_of_5": all(point_gates),
            "point8_and_point16_p99_regression_at_most_5pct": all(
                by_cell[f"w{writer}-point"]["paired_p99_change_median_pct"] is not None
                and by_cell[f"w{writer}-point"]["paired_p99_change_median_pct"] <= 5
                for writer in (8, 16)
            ),
            "point1_and_batch64_paired_median_regression_at_most_2pct": guardrails,
            "candidate_qualified": bool(validation["complete"] and all(point_gates) and guardrails),
        },
        "qualification_note": (
            "Same-host, same-source, same-release-binary A/B; the progress-ring switch is the only experiment "
            "variable. Local performance screening was skipped at the user's direction because local SSD results "
            "are unstable. Passing this qualification does not change production defaults."
        ),
    }


def run_suite(args: argparse.Namespace) -> int:
    if len(args.commit) != 40 or any(char not in "0123456789abcdef" for char in args.commit):
        raise ValueError("commit must be a full lowercase 40-character SHA")
    if len(args.binary_sha256) != 64 or any(char not in "0123456789abcdef" for char in args.binary_sha256):
        raise ValueError("binary SHA-256 must be 64 lowercase hexadecimal characters")
    if _sha256_file(args.binary) != args.binary_sha256:
        raise ValueError("write-perf binary SHA-256 does not match its recorded identity")
    if args.machine_type != "c4-standard-16-lssd" or args.ssd_count != 2 or args.ssd_gib != 750:
        raise ValueError("progress-ring qualification requires c4-standard-16-lssd and two Local SSDs")
    writers = tuple(int(value) for value in args.writers.split(","))
    if writers != POINT_PROGRESS_RING_WRITERS:
        raise ValueError(f"progress-ring qualification requires writers {POINT_PROGRESS_RING_WRITERS}")

    results_dir, db_root = args.results_dir, args.db_root
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)
    schedule = build_point_progress_ring_schedule(writers)
    plan = build_point_progress_ring_plan(
        writers=writers,
        machine_type=args.machine_type,
        machine_spec={"vcpus": 16, "ssd_count": args.ssd_count, "ssd_gib": args.ssd_gib},
        commit=args.commit,
        binary_sha256=args.binary_sha256,
    )
    (results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    (results_dir / "commit.txt").write_text(args.commit + "\n", encoding="utf-8")
    (results_dir / "write-perf.sha256").write_text(args.binary_sha256 + "\n", encoding="utf-8")
    with (results_dir / "point-progress-ring-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows({**row, "batch_size": row["batch_size"] or "none"} for row in schedule)
    summary_fields = _summary_fields(schedule)
    (results_dir / "summary.tsv").write_text("\t".join(summary_fields) + "\n", encoding="utf-8")

    rows: list[dict[str, Any]] = []
    attempted = 0
    failure: str | None = None
    for row in schedule:
        attempted += 1
        try:
            metadata = _run_one(
                binary=args.binary, binary_sha256=args.binary_sha256,
                row={**row, "source_commit": args.commit}, results_dir=results_dir,
                db_root=db_root, safe_deadline_epoch=args.safe_deadline_epoch,
            )
        except (OSError, TimeoutError, subprocess.SubprocessError, ValueError) as error:
            failure = f"run {attempted} did not complete: {error}"
            (results_dir / "runner-error.txt").write_text(failure + "\n", encoding="utf-8")
            break
        rows.append(metadata)
        with (results_dir / "summary.tsv").open("a", encoding="utf-8", newline="") as stream:
            csv.DictWriter(stream, fieldnames=summary_fields, delimiter="\t", lineterminator="\n").writerow(
                {key: json.dumps(value, sort_keys=True) if isinstance(value, dict) else value
                 for key, value in metadata.items()}
            )
        if metadata["exit_code"] != 0:
            failure = f"run {attempted} failed validation or exited nonzero"
            break

    summary = _summary(rows, writers, len(schedule))
    summary.update({"source_commit": args.commit, "binary_sha256": args.binary_sha256})
    summary["complete"] = summary["complete"] and failure is None and attempted == len(schedule)
    if failure:
        summary["failure"] = failure
    (results_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    (results_dir / "remote-exit.json").write_text(
        json.dumps({"failed": failure is not None or attempted != len(schedule), "runs_started": attempted}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    (results_dir / "finished_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    return 1 if failure is not None or attempted != len(schedule) else 0


def verify_results_directory(results: Path) -> dict[str, Any]:
    """Strictly validate all locally collected F0.5 artifacts before cloud cleanup."""
    errors: list[str] = []
    try:
        summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
        remote_exit = json.loads((results / "remote-exit.json").read_text(encoding="utf-8"))
        plan = json.loads((results / "benchmark-plan.json").read_text(encoding="utf-8"))
        commit = (results / "commit.txt").read_text(encoding="utf-8").strip()
        binary_sha = (results / "write-perf.sha256").read_text(encoding="utf-8").strip()
        if not all(isinstance(value, dict) for value in (summary, remote_exit, plan)):
            raise ValueError("summary, remote-exit, and plan must be JSON objects")
        schedule = build_point_progress_ring_schedule(plan.get("writers", ()))
        expected_plan = build_point_progress_ring_plan(
            writers=plan.get("writers", ()),
            machine_type="c4-standard-16-lssd",
            machine_spec={"vcpus": 16, "ssd_count": 2, "ssd_gib": 750},
            commit=commit,
            binary_sha256=binary_sha,
        )
        for key, value in expected_plan.items():
            # Older F0.5 archives called the ordered policy "ticket_prefix".
            # Keep that descriptive alias readable, but still verify every
            # run's captured environment against the actual supported value.
            if key == "data_submit_order" and plan.get(key) == "ticket_prefix" and value == "ordered":
                continue
            if plan.get(key) != value:
                errors.append(f"plan field {key} differs from fixed F0.5 suite")
        if len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
            errors.append("source commit identity is invalid")
        if len(binary_sha) != 64 or any(char not in "0123456789abcdef" for char in binary_sha):
            errors.append("binary checksum is invalid")
        if (
            summary.get("suite") != "v8-point-progress-ring"
            or summary.get("source_commit") != commit
            or summary.get("binary_sha256") != binary_sha
            or summary.get("complete") is not True
            or summary.get("runs_started") != len(schedule)
            or summary.get("runs_completed") != len(schedule)
            or summary.get("runs_planned") != len(schedule)
        ):
            errors.append("summary metadata or run counts are incomplete")
        if remote_exit.get("failed") is not False or remote_exit.get("runs_started") != len(schedule):
            errors.append("remote-exit does not confirm all planned runs")
        with (results / "point-progress-ring-schedule.tsv").open(encoding="utf-8", newline="") as stream:
            schedule_rows = list(csv.DictReader(stream, delimiter="\t"))
        expected_schedule_rows = [
            {
                key: (
                    "none" if key == "batch_size" and value is None
                    else "" if value is None
                    else str(value)
                )
                for key, value in planned_row.items()
            }
            for planned_row in schedule
        ]
        if schedule_rows != expected_schedule_rows:
            errors.append("schedule file differs from the exact planned run order or configuration")
        rows = summary.get("results")
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            errors.append("summary results are missing or malformed")
            rows = []
        validation = validate_point_progress_ring_rows(rows, plan.get("writers", ()))
        if validation["complete"] is not True:
            errors.append("run rows do not match the fixed F0.5 matrix")
        recomputed = _summary(rows, tuple(plan.get("writers", ())), len(schedule))
        for key in (
            "suite", "complete", "runs_started", "runs_completed", "runs_planned", "writers",
            "workloads", "arms", "matrix_validation", "comparison", "comparison_by_cell",
            "qualification_gates",
        ):
            if summary.get(key) != recomputed.get(key):
                errors.append(f"summary field {key} does not match archived run rows")
        expected_ids = {_run_id(row) for row in schedule}
        actual_ids = {
            path.name for path in results.iterdir()
            if path.is_dir() and path.name.startswith(("r", "w"))
        }
        if actual_ids != expected_ids:
            errors.append("run directories do not exactly match the schedule")
        rows_by_id = {_run_id(row): row for row in rows if isinstance(row, dict)}
        for row in schedule:
            name = _run_id(row)
            run_dir = results / name
            metadata_path, result_path = run_dir / "run-metadata.json", run_dir / "result.json"
            if not metadata_path.is_file() or not result_path.is_file():
                errors.append(f"{name} is missing its result or metadata")
                continue
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if not isinstance(metadata, dict):
                errors.append(f"{name} run metadata is malformed")
                continue
            if rows_by_id.get(name) != metadata:
                errors.append(f"{name} summary row differs from run metadata")
            identity = {key: row[key] for key in row}
            identity.update({"source_commit": commit, "binary_sha256": binary_sha})
            if any(metadata.get(key) != value for key, value in identity.items()):
                errors.append(f"{name} metadata identity differs from the schedule")
            environment_path = run_dir / "environment.txt"
            if not environment_path.is_file() or environment_path.read_text(encoding="utf-8") != (
                _expected_environment_text(bool(row["progress_ring_enabled"]))
            ):
                errors.append(f"{name} environment does not match the fixed A/B configuration")
            try:
                raw = json.loads(result_path.read_text(encoding="utf-8"))
                expected_metrics = _validate_record(raw, row)
                if any(metadata.get(key) != value for key, value in expected_metrics.items()):
                    errors.append(f"{name} metadata metrics differ from raw result")
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                errors.append(f"{name} raw result is invalid: {error}")
        summary_tsv_path = results / "summary.tsv"
        if not summary_tsv_path.is_file():
            errors.append("summary.tsv is missing")
        else:
            with summary_tsv_path.open(encoding="utf-8", newline="") as stream:
                summary_tsv_rows = list(csv.DictReader(stream, delimiter="\t"))
            metadata_by_id = {_run_id(row): row for row in rows if isinstance(row, dict)}
            expected_summary_tsv_rows = []
            for planned_row in schedule:
                metadata = metadata_by_id.get(_run_id(planned_row), {})
                expected_summary_tsv_rows.append({
                    key: (
                        json.dumps(metadata[key], sort_keys=True)
                        if isinstance(metadata.get(key), dict)
                        else "" if metadata.get(key) is None else str(metadata.get(key))
                    )
                    for key in _summary_fields(schedule)
                })
            if summary_tsv_rows != expected_summary_tsv_rows:
                errors.append("summary.tsv rows differ from run metadata")
        if errors:
            return {"verified": False, "planned": len(schedule), "errors": errors}
        return {"verified": True, "planned": len(schedule), "observed": len(rows), "errors": []}
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        return {"verified": False, "errors": [str(error)]}


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
