#!/usr/bin/env python3
"""Run a small-ticket V7/V8 CPU-attribution suite on the disposable GCP VM."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time
from typing import Any

from gcp_wal_matrix import RFC026_WORKLOADS, validate_writer_counts


def build_profile_schedule(writers: tuple[int, ...]) -> list[dict[str, Any]]:
    """Profile each V7/V8 workload cell once; add V8 Leader for one writer."""
    writer_counts = validate_writer_counts(writers)
    schedule: list[dict[str, Any]] = []
    for writer in writer_counts:
        for workload_index, workload in enumerate(RFC026_WORKLOADS):
            arms = ["v7_parallel", "v8_parallel"]
            if writer == 1:
                arms.append("v8_leader")
            # Reverse adjacent pairs to reduce fixed order bias in this diagnostic
            # capture. This is not a scored or state-matched benchmark.
            if (writer + workload_index) % 2 == 0:
                arms.reverse()
            for arm in arms:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload["name"],
                    "bench": workload["bench"],
                    "batch_entries": workload["batch_entries"],
                    "batch_size": workload["batch_size"],
                    "num": workload["num"],
                    "sample_every": workload["sample_every"],
                    "arm": arm,
                    "perf_capture": workload["name"] == "point",
                })
    return schedule


def build_profile_plan(
    writers: tuple[int, ...], *, machine_type: str, machine_spec: dict[str, int], commit: str,
) -> dict[str, Any]:
    schedule = build_profile_schedule(writers)
    return {
        "suite": "v8-small-ticket-profile",
        "diagnostic_only": True,
        "writers": list(writers),
        "workloads": [dict(workload) for workload in RFC026_WORKLOADS],
        "arms": ["v7_parallel", "v8_parallel", "v8_leader_at_one_writer"],
        "runs_planned": len(schedule),
        "perf_captures_planned": sum(bool(row["perf_capture"]) for row in schedule),
        "hotpath_sampling_rate": 0.1,
        "perf_frequency_hz": 199,
        "perf_stack_mode": "DWARF, 4096-byte stack dump",
        "machine_type": machine_type,
        "vcpus": machine_spec["vcpus"],
        "local_ssd_count": machine_spec["ssd_count"],
        "local_ssd_total_gib": machine_spec["ssd_gib"],
        "commit": commit,
        "note": "Instrumented diagnostics only; do not compare their rates with unprofiled benchmark scores.",
    }


def validate_profile_rows(rows: list[dict[str, Any]], writers: tuple[int, ...]) -> dict[str, Any]:
    expected = {
        (row["writer"], row["workload"], row["arm"]): row["num"]
        for row in build_profile_schedule(writers)
    }
    observed: dict[tuple[int, str, str], int] = {}
    invalid: list[dict[str, Any]] = []
    for row in rows:
        try:
            writer = row["writer"]
            exit_code = row["exit_code"]
            ops = row["ops"]
            if any(type(value) is not int for value in (writer, exit_code, ops)):
                invalid.append(row)
                continue
            cell = (writer, str(row["workload"]), str(row["arm"]))
        except (KeyError, TypeError, ValueError):
            invalid.append(row)
            continue
        observed[cell] = observed.get(cell, 0) + 1
        if exit_code != 0 or (cell in expected and ops != expected[cell]):
            invalid.append(row)

    expected_cells = set(expected)
    observed_cells = set(observed)
    missing = sorted(expected_cells - observed_cells)
    unexpected = sorted(observed_cells - expected_cells)
    duplicates = sorted(cell for cell, count in observed.items() if count > 1)
    return {
        "complete": not (missing or unexpected or duplicates or invalid),
        "planned": len(expected),
        "observed": len(rows),
        "missing_cells": [list(cell) for cell in missing],
        "unexpected_cells": [list(cell) for cell in unexpected],
        "duplicate_cells": [list(cell) for cell in duplicates],
        "invalid_rows": invalid,
    }


def _build_command(binary: Path, row: dict[str, Any], db_root: Path) -> tuple[list[str], int, str]:
    arm = row["arm"]
    writer = int(row["writer"])
    wal_mode = "leader" if arm == "v8_leader" else "parallel"
    wal_version = 7 if arm == "v7_parallel" else 8
    command = [
        str(binary), "--bench", str(row["bench"]), "--wal", "--wal-io-mode", wal_mode,
        "--num", str(row["num"]), "--threads", str(writer), "--value-size", "1024",
        "--target-sst-size", "2147483648", "--compaction", "none", "--output", "json",
        "--latency-sample-every", str(row["sample_every"]), "--path",
        str(db_root / f"w{writer}-{row['workload']}-{arm}"), "--profile",
    ]
    if row["workload"] != "point":
        command.extend(("--wal-batch-size", str(row["batch_entries"])))
    command.extend(("--pitr", "--pitr-format", f"v{wal_version}"))
    expected_batch = None if row["batch_size"] is None else min(
        int(row["batch_size"]), max(1, (int(row["num"]) + writer - 1) // writer)
    )
    return command, expected_batch, wal_mode


def _load_benchmark_record(result_path: Path) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    with result_path.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict) and all(key in value for key in ("result", "params", "latency")):
                records.append(value)

    if len(records) != 1:
        raise ValueError(f"expected one benchmark JSON record, found {len(records)}")
    return records[0]


def _validate_wal_io_metadata(
    pipeline: dict[str, Any], expected_version: int, expected_frontier_mode: str = "buffered",
    expected_data_o_direct: bool = True,
) -> dict[str, Any]:
    if expected_version != 8:
        return {}
    if expected_frontier_mode not in ("buffered", "direct"):
        raise ValueError(f"unsupported expected V8 FRONTIER mode: {expected_frontier_mode!r}")

    start = pipeline.get("wal_io_start")
    end = pipeline.get("wal_io_end")
    if not isinstance(start, dict) or not isinstance(end, dict):
        raise ValueError("V8 benchmark record is missing start/end WAL I/O metadata")
    handle_names = ("data", "frontier", "sync")
    start_handles = {name: start.get(name) for name in handle_names}
    end_handles = {name: end.get(name) for name in handle_names}
    if pipeline.get("wal_io_stable") is not True or start_handles != end_handles:
        raise ValueError("V8 active WAL I/O metadata changed during the benchmark")

    identities: set[tuple[int, int]] = set()
    for snapshot in (start, end):
        for handle_name in ("data", "frontier", "sync"):
            handle = snapshot.get(handle_name)
            if not isinstance(handle, dict):
                raise ValueError(f"V8 WAL I/O metadata is missing the {handle_name} handle")
            if type(handle.get("o_direct")) is not bool:
                raise ValueError(f"V8 WAL {handle_name} handle is missing its O_DIRECT flag")
            device = handle.get("device")
            inode = handle.get("inode")
            if type(device) is not int or type(inode) is not int or device < 0 or inode <= 0:
                raise ValueError(f"V8 WAL {handle_name} handle has invalid device/inode metadata")
            identities.add((device, inode))

    if len(identities) != 1:
        raise ValueError("V8 DATA, FRONTIER, and sync handles do not identify one WAL inode")
    if start["data"]["o_direct"] is not expected_data_o_direct:
        raise ValueError(f"expected V8 DATA O_DIRECT={expected_data_o_direct}")
    if start["sync"]["o_direct"] is not False:
        raise ValueError("expected V8 sync O_DIRECT=False")
    expected_frontier_direct = expected_frontier_mode == "direct"
    if start["frontier"]["o_direct"] is not expected_frontier_direct:
        raise ValueError(
            f"expected V8 FRONTIER O_DIRECT={expected_frontier_direct}, "
            f"got {start['frontier']['o_direct']}"
        )

    device, inode = next(iter(identities))
    return {
        "wal_data_o_direct": start["data"]["o_direct"],
        "wal_frontier_o_direct": start["frontier"]["o_direct"],
        "wal_sync_o_direct": start["sync"]["o_direct"],
        "wal_io_device": device,
        "wal_io_inode": inode,
    }


def _expected_frontier_mode_for_arm(arm: str, configured_mode: str) -> str:
    return configured_mode if arm == "v8_parallel" else "buffered"


def _run_one(
    row: dict[str, Any], *, binary: Path, result_root: Path, db_root: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    writer = int(row["writer"])
    run_dir = result_root / f"w{writer}-{row['workload']}-{row['arm']}"
    run_dir.mkdir(parents=True, exist_ok=False)
    command, expected_batch, wal_mode = _build_command(binary, row, db_root)
    timeline = run_dir / "timeline.json"
    env = os.environ.copy()
    env.update({
        "HOTPATH_TIME_SAMPLING_RATE": "0.1",
        "HOTPATH_METRICS_SERVER_OFF": "true",
        "TOYKV_WAL_TIMELINE": str(timeline),
        "TOYKV_WAL_TIMELINE_INTERVAL_MS": "100",
    })
    (run_dir / "command.sh").write_text(shlex.join(command) + "\n", encoding="utf-8")
    (run_dir / "start_epoch.txt").write_text(f"{time.time():.6f}\n", encoding="utf-8")
    started = time.monotonic()
    execution_deadline = started + timeout_seconds

    def run_benchmark_process(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        remaining = execution_deadline - time.monotonic()
        if remaining <= 0:
            return subprocess.CompletedProcess(argv, 124)
        return subprocess.run(
            ["timeout", "--signal=INT", "--kill-after=30s", str(remaining), *argv],
            timeout=remaining + 45, check=False, **kwargs,
        )

    perf_status = "not_requested"
    result_path = run_dir / "result.json"
    stderr_path = run_dir / "stderr.log"
    if row["perf_capture"]:
        perf = shutil.which("perf")
        if perf is None:
            perf_status = "unavailable: perf executable not found"
            with result_path.open("w", encoding="utf-8") as output, stderr_path.open("w", encoding="utf-8") as log:
                process = run_benchmark_process(
                    command, env=env, stdout=output, stderr=log,
                )
            exit_code = process.returncode
        else:
            perf_data = run_dir / "perf.data"
            perf_log = run_dir / "perf-record.log"
            # Keep the recorder privileged while the benchmark creates files
            # as the caller, including databases left behind by a crash.
            perf_command = [
                "sudo", "-n", perf, "record", "-F", "199", "--call-graph", "dwarf,4096",
                "-o", str(perf_data), "--", "setpriv", "--reuid", str(os.getuid()),
                "--regid", str(os.getgid()), "--init-groups", "--reset-env", "--", "env",
                *[f"{key}={value}" for key, value in env.items()
                    if key.startswith("HOTPATH_") or key.startswith("TOYKV_WAL_")], *command,
            ]
            (run_dir / "perf-command.sh").write_text(shlex.join(perf_command) + "\n", encoding="utf-8")
            try:
                with result_path.open("w", encoding="utf-8") as output, perf_log.open("w", encoding="utf-8") as log:
                    perf_result = run_benchmark_process(
                        perf_command, stdout=output, stderr=log,
                    )
                if perf_result.returncode == 0 and perf_data.is_file() and perf_data.stat().st_size:
                    perf_status = "captured"
                else:
                    perf_status = f"record_failed:{perf_result.returncode}"
                    if perf_data.exists() and perf_data.stat().st_size == 0:
                        perf_data.unlink()
            except (OSError, subprocess.TimeoutExpired) as error:
                perf_status = f"record_failed:{error}"
                exit_code = 70
            else:
                exit_code = perf_result.returncode
            # Retry an empty capture in a fresh database only within the
            # original per-cell budget shared by both attempts.
            if not result_path.is_file() or not result_path.stat().st_size:
                if exit_code != 70 and time.monotonic() < execution_deadline:
                    db_path = db_root / f"w{writer}-{row['workload']}-{row['arm']}"
                    if db_path.exists():
                        shutil.rmtree(db_path)
                    with result_path.open("w", encoding="utf-8") as output, stderr_path.open("w", encoding="utf-8") as log:
                        fallback = run_benchmark_process(
                            command, env=env, stdout=output, stderr=log,
                        )
                    exit_code = fallback.returncode

            if perf_status == "captured":
                try:
                    report = subprocess.run(
                        ["sudo", "-n", perf, "report", "--stdio", "--call-graph", "fractal,0.5,caller",
                         "--percent-limit", "0.5", "-i", str(perf_data)],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                        timeout=120, check=False,
                    )
                except (OSError, subprocess.TimeoutExpired) as error:
                    perf_status = f"report_failed:{error}"
                    (run_dir / "perf-report.txt").write_text(f"perf report failed: {error}\n", encoding="utf-8")
                else:
                    (run_dir / "perf-report.txt").write_text(report.stdout + report.stderr, encoding="utf-8")
                    if report.returncode:
                        perf_status = f"report_failed:{report.returncode}"
    else:
        try:
            with result_path.open("w", encoding="utf-8") as output, stderr_path.open("w", encoding="utf-8") as log:
                process = run_benchmark_process(
                    command, env=env, stdout=output, stderr=log,
                )
            exit_code = process.returncode
        except (OSError, subprocess.TimeoutExpired) as error:
            exit_code = 70
            stderr_path.write_text(f"benchmark launch failed: {error}\n", encoding="utf-8")

    elapsed = time.monotonic() - started
    result_path = run_dir / "result.json"
    record: dict[str, Any] | None = None
    # Keep usable counters from abnormal exits without reporting the run as
    # successful: perf also propagates benchmark and timeout exit statuses.
    if exit_code == 0 or (result_path.is_file() and result_path.stat().st_size):
        try:
            record = _load_benchmark_record(result_path)
        except (OSError, ValueError) as error:
            if exit_code == 0:
                exit_code = 65
            (run_dir / "validation-error.txt").write_text(f"invalid benchmark JSON: {error}\n", encoding="utf-8")

    metrics: dict[str, Any] = {
        "ops": "", "puts_per_second": "", "p99_ms": "", "cpu_seconds": "", "sync_count": "",
        "wal_data_o_direct": "", "wal_frontier_o_direct": "", "wal_sync_o_direct": "",
        "wal_io_device": "", "wal_io_inode": "",
    }
    if record is not None:
        try:
            result = record["result"]
            params = record["params"]
            latency = record["latency"]
            for name, value in (("result", result), ("params", params), ("latency", latency)):
                if not isinstance(value, dict):
                    raise ValueError(f"benchmark {name} must be an object")
            ops = result["ops"]
            if type(ops) is not int:
                raise ValueError("benchmark ops must be an integer")
            pipeline = result.get("wal_pipeline")
            if pipeline is None:
                pipeline = {}
            elif not isinstance(pipeline, dict):
                raise ValueError("benchmark wal_pipeline must be an object or null")
            cpu_seconds = (float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])) / 1000
            expected_version = 7 if row["arm"] == "v7_parallel" else 8
            expected_frontier_mode = _expected_frontier_mode_for_arm(
                row["arm"], os.environ.get("TOYKV_WAL_V8_FRONTIER_MODE", "buffered")
            )
            parsed_metrics = {
                "ops": ops,
                "puts_per_second": float(result["ops_per_sec"]),
                "p99_ms": float(latency["p99_ms"]),
                "cpu_seconds": cpu_seconds,
                "sync_count": pipeline.get("sync_count", ""),
                **_validate_wal_io_metadata(
                    pipeline,
                    expected_version,
                    expected_frontier_mode,
                    expected_data_o_direct=row["arm"] == "v8_parallel",
                ),
            }
            if any(not math.isfinite(parsed_metrics[key]) or parsed_metrics[key] < 0
                   for key in ("puts_per_second", "p99_ms", "cpu_seconds")):
                raise ValueError("benchmark emitted non-finite or negative metrics")
            metrics = parsed_metrics

            expected_params = {
                "num": int(row["num"]), "threads": writer, "value_size": 1024,
                "batch_size": expected_batch, "wal_format_version": expected_version,
                "wal_io_mode": wal_mode,
            }
            actual_params = {key: params.get(key) for key in expected_params}
            if metrics["ops"] != int(row["num"]) or actual_params != expected_params:
                raise ValueError(
                    f"expected ops={row['num']} params={expected_params}; "
                    f"got ops={result.get('ops')} params={actual_params}"
                )
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            if exit_code == 0:
                exit_code = 65
            (run_dir / "validation-error.txt").write_text(f"invalid benchmark metrics: {error}\n", encoding="utf-8")

    metadata = {
        "writer": writer, "workload": row["workload"], "arm": row["arm"],
        "perf_status": perf_status, "exit_code": exit_code, **metrics,
        "duration_seconds": elapsed,
    }
    (run_dir / "run-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return metadata


def run_suite(
    *, binary: Path, results_dir: Path, db_root: Path, writers: tuple[int, ...],
    safe_deadline_epoch: int, timeout_seconds: int = 900,
) -> dict[str, Any]:
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)
    schedule = build_profile_schedule(writers)
    with (results_dir / "profile-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(schedule)

    summary_fields = [
        "writer", "workload", "arm", "perf_status", "exit_code", "ops", "puts_per_second",
        "p99_ms", "cpu_seconds", "sync_count", "duration_seconds", "error",
        "wal_data_o_direct", "wal_frontier_o_direct", "wal_sync_o_direct", "wal_io_device", "wal_io_inode",
    ]
    with (results_dir / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
        csv.DictWriter(stream, fieldnames=summary_fields, delimiter="\t").writeheader()

    rows: list[dict[str, Any]] = []
    failed = False
    for item in schedule:
        if time.time() + timeout_seconds + 330 >= safe_deadline_epoch:
            failed = True
            break
        try:
            row = _run_one(
                item, binary=binary, result_root=results_dir, db_root=db_root,
                timeout_seconds=timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired, FileExistsError) as error:
            row = {
                "writer": item["writer"], "workload": item["workload"], "arm": item["arm"],
                "perf_status": "not_run", "exit_code": 70, "ops": "", "puts_per_second": "",
                "p99_ms": "", "cpu_seconds": "", "sync_count": "", "duration_seconds": "",
                "error": str(error),
            }
            run_dir = results_dir / f"w{item['writer']}-{item['workload']}-{item['arm']}"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "run-metadata.json").write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
        rows.append(row)
        failed = failed or row["exit_code"] != 0
        with (results_dir / "summary.tsv").open("a", encoding="utf-8", newline="") as stream:
            csv.DictWriter(stream, fieldnames=summary_fields, delimiter="\t").writerow(row)
        print(f"profile_run=w{row['writer']}-{row['workload']}-{row['arm']} exit={row['exit_code']} perf={row['perf_status']}", flush=True)

    validation = validate_profile_rows(rows, writers)
    perf_requested = sum(bool(item["perf_capture"]) for item in schedule)
    perf_captured = sum(
        row.get("perf_status") == "captured" or str(row.get("perf_status", "")).startswith("report_failed:")
        for row in rows
    )
    summary = {
        "complete": not failed and validation["complete"],
        "suite": "v8-small-ticket-profile",
        "diagnostic_only": True,
        "runs_completed": len(rows),
        "runs_planned": len(schedule),
        "writers": list(writers),
        "workloads": [row["name"] for row in RFC026_WORKLOADS],
        "arms": ["v7_parallel", "v8_parallel", "v8_leader_at_one_writer"],
        "perf_captures_planned": perf_requested,
        "perf_captures_completed": perf_captured,
        "perf_capture_complete": perf_captured == perf_requested,
        "matrix_validation": validation,
        "results": rows,
        "note": "Profiles are instrumented diagnostic runs, not throughput qualification; do not compare their rates with unprofiled matrix scores.",
    }
    (results_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (results_dir / "remote-exit.json").write_text(
        json.dumps({"failed": failed or not validation["complete"], "runs_started": len(rows)}, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--writers", required=True, help="Comma-separated supported writer counts")
    parser.add_argument("--safe-deadline-epoch", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        writers = validate_writer_counts(int(value) for value in args.writers.split(","))
    except ValueError as error:
        parser.error(str(error))
    summary = run_suite(
        binary=args.binary, results_dir=args.results_dir, db_root=args.db_root,
        writers=writers, safe_deadline_epoch=args.safe_deadline_epoch,
    )
    return 0 if summary["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
