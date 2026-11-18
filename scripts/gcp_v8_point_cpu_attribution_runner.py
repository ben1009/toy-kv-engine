#!/usr/bin/env python3
"""Run sampled V7/V8 Point CPU attribution at four writer counts."""

from __future__ import annotations

import csv
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import shlex
import subprocess
import time
from typing import Any

from gcp_wal_matrix import (
    V8_POINT_CPU_ATTRIBUTION_NUM,
    V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
    build_v8_point_cpu_attribution_plan,
    build_v8_point_cpu_attribution_schedule,
    validate_v8_point_cpu_attribution_rows,
)
from gcp_wal_profile_runner import _validate_wal_io_metadata

RUN_TIMEOUT_SECONDS = 900
PRODUCER_PHASE_FIELDS = (
    "input_build_cpu_ns", "preflight_cpu_ns", "sequencer_wait_cpu_ns", "commit_setup_cpu_ns",
    "admission_cpu_ns", "frame_finish_cpu_ns", "v8_logical_encode_cpu_ns", "v8_digest_ready_cpu_ns",
    "v8_frame_encode_cpu_ns", "v8_frames_ready_submit_cpu_ns", "producer_total_cpu_ns",
)
THREAD_ROLES = {
    "io_worker_completion_processing",
    "v8_completion_reaper",
    "v7_durability_coordinator",
}
PRODUCER_CPU_FIELDS = tuple(field for field in PRODUCER_PHASE_FIELDS)
V8_DURABILITY_FIELDS = (
    "driver_cpu_ns", "driver_wall_ns", "tickets_covered", "preparation_cpu_ns",
    "frontier_write_cpu_ns", "fdatasync_cpu_ns", "coalescing_cpu_ns",
    "resource_retire_before_publish_cpu_ns", "resource_retire_after_publish_cpu_ns",
    "coordinator_publish_cpu_ns",
)


def _run_id(row: dict[str, Any]) -> str:
    return f"w{row['writer']}-point-{row['arm']}"


def _build_command(binary: Path, row: dict[str, Any], db_root: Path) -> list[str]:
    version = 7 if row["arm"] == "v7_parallel" else 8
    return [
        str(binary), "--bench", "wal_concurrent", "--wal", "--pitr", "--pitr-format", f"v{version}",
        "--wal-io-mode", "parallel", "--num", str(V8_POINT_CPU_ATTRIBUTION_NUM),
        "--threads", str(row["writer"]), "--value-size", "1024", "--target-sst-size", "2147483648",
        "--compaction", "none", "--output", "json", "--latency-sample-every", "100",
        "--wal-cpu-attribution-sample-every", str(V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY),
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


def _valid_nonnegative_int(value: Any) -> bool:
    return type(value) is int and value >= 0


def _validate_v8_durability_phase_samples(durability: dict[str, Any]) -> None:
    phases = durability.get("phases")
    if (
        durability.get("sample_every") != V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY
        or durability.get("sampled_advances", 0) <= 0
        or durability.get("dropped_samples") != 0
        or not isinstance(phases, list)
        or len(phases) != durability.get("sampled_advances")
        or any(not isinstance(sample, dict) for sample in phases)
    ):
        raise ValueError("V8 durability-driver samples are missing or incomplete")
    # Rust emits phase records only after the full durability advance succeeds;
    # failed or interrupted samples are counted as dropped instead. Older
    # records therefore have no per-phase `succeeded` field. If a future schema
    # provides it, reject only an explicit failure.
    if any(sample.get("succeeded") is False for sample in phases):
        raise ValueError("V8 durability-driver sample reports a failed advance")


def _validate_record(record: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    result = record["result"]
    params = record["params"]
    pipeline = result["wal_pipeline"]
    version = 7 if row["arm"] == "v7_parallel" else 8
    expected_params = {
        "num": V8_POINT_CPU_ATTRIBUTION_NUM,
        "threads": row["writer"],
        "value_size": 1024,
        "wal_format_version": version,
        "wal_io_mode": "parallel",
    }
    if int(result["ops"]) != V8_POINT_CPU_ATTRIBUTION_NUM or any(
        params.get(key) != value for key, value in expected_params.items()
    ):
        raise ValueError("benchmark parameters do not match the planned Point run")

    attribution = pipeline.get("wal_cpu_attribution")
    if not isinstance(attribution, dict):
        raise ValueError("WAL CPU attribution payload is missing")
    expected_samples = V8_POINT_CPU_ATTRIBUTION_NUM // V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY
    if (
        attribution.get("sample_every") != V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY
        or attribution.get("operations_seen") != V8_POINT_CPU_ATTRIBUTION_NUM
        or attribution.get("selected_producer_samples") != expected_samples
        or attribution.get("dropped_producer_samples") != 0
        or attribution.get("dropped_thread_samples") != 0
        or attribution.get("thread_cpu_time_supported") is not True
    ):
        raise ValueError("WAL CPU attribution counters are incomplete or inconsistent")
    producer_samples = attribution.get("producer_samples")
    thread_samples = attribution.get("thread_samples")
    if not isinstance(producer_samples, list) or len(producer_samples) != expected_samples:
        raise ValueError("producer sample count differs from the planned cadence")
    if not isinstance(thread_samples, list) or not thread_samples:
        raise ValueError("thread-role CPU samples are missing")
    for sample in producer_samples:
        if (
            not isinstance(sample, dict)
            or sample.get("wal_version") != version
            or sample.get("entry_count") != 1
            or sample.get("completed") is not True
            or not _valid_nonnegative_int(sample.get("ticket"))
            or not _valid_nonnegative_int(sample.get("commit_ts"))
            or any(not _valid_nonnegative_int(sample.get(field)) for field in PRODUCER_PHASE_FIELDS)
        ):
            raise ValueError("producer attribution sample is malformed or incomplete")
    roles = set()
    for sample in thread_samples:
        if (
            not isinstance(sample, dict)
            or sample.get("role") not in THREAD_ROLES
            or not _valid_nonnegative_int(sample.get("sample_sequence"))
            or not _valid_nonnegative_int(sample.get("work_items"))
            or not _valid_nonnegative_int(sample.get("thread_cpu_ns"))
            or not _valid_nonnegative_int(sample.get("wall_ns"))
            or sample.get("completed") is not True
        ):
            raise ValueError("thread-role attribution sample is malformed or incomplete")
        roles.add(sample["role"])
    required_roles = {"io_worker_completion_processing", "v7_durability_coordinator"} if version == 7 else {
        "io_worker_completion_processing", "v8_completion_reaper",
    }
    if not required_roles.issubset(roles):
        raise ValueError(f"required WAL thread roles are missing: {sorted(required_roles - roles)}")

    durability = pipeline.get("v8_durability_diagnostics")
    if version == 8:
        if not isinstance(durability, dict):
            raise ValueError("V8 durability-driver samples are missing or incomplete")
        _validate_v8_durability_phase_samples(durability)
        if pipeline.get("wal_io_stable") is not True:
            raise ValueError("WAL I/O metadata changed during the run")
        _validate_wal_io_metadata(pipeline, 8, expected_frontier_mode="direct")
        for boundary in ("wal_io_start", "wal_io_end"):
            prep = pipeline[boundary]["sliding_extent_preparation"]
            if prep.get("mode") != "sliding-point-lookahead" or prep.get("window_bytes") != 1 << 20:
                raise ValueError(f"{boundary} does not report the planned 1 MiB Point lookahead")
    elif durability is not None:
        raise ValueError("V7 run unexpectedly reports V8 durability-driver diagnostics")

    throughput = float(result["ops_per_sec"])
    cpu_seconds = (float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])) / 1000
    if not all(math.isfinite(value) and value >= 0 for value in (throughput, cpu_seconds)):
        raise ValueError("benchmark result contains a non-finite or negative metric")
    return {
        "ops": int(result["ops"]),
        "puts_per_second": throughput,
        "process_cpu_seconds": cpu_seconds,
        "process_cpu_us_per_put": cpu_seconds * 1_000_000 / int(result["ops"]),
        "sync_count": int(pipeline["sync_count"]),
        "p50_ms": float(record["latency"]["p50_ms"]),
        "p95_ms": float(record["latency"]["p95_ms"]),
        "p99_ms": float(record["latency"]["p99_ms"]),
        "wal_cpu_attribution_valid": True,
        "wal_cpu_attribution": attribution,
        "v8_durability_diagnostics": durability,
        "v8_digest": record.get("wal_v8_digest"),
    }


def _distribution(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "mean": None}
    ordered = sorted(values)

    def percentile(fraction: float) -> float:
        index = max(0, math.ceil(fraction * len(ordered)) - 1)
        return ordered[index]

    return {
        "count": len(ordered), "p50": percentile(0.50), "p95": percentile(0.95),
        "p99": percentile(0.99), "mean": sum(ordered) / len(ordered),
    }


def _producer_phase_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summaries: dict[str, Any] = {}
    for row in rows:
        attribution = row.get("wal_cpu_attribution")
        if not isinstance(attribution, dict):
            continue
        samples = attribution["producer_samples"]
        summaries[row["arm"]] = {
            field: _distribution([float(sample[field]) for sample in samples])
            for field in PRODUCER_CPU_FIELDS
        }
    return summaries


def _thread_role_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summaries: dict[str, Any] = {}
    for row in rows:
        attribution = row.get("wal_cpu_attribution")
        if not isinstance(attribution, dict):
            continue
        for sample in attribution["thread_samples"]:
            role = sample["role"]
            entry = summaries.setdefault(role, {
                "sample_count": 0, "completed_samples": 0, "sampled_thread_cpu_ns": 0,
                "sampled_wall_ns": 0, "sampled_work_items": 0,
            })
            entry["sample_count"] += 1
            entry["completed_samples"] += int(sample["completed"] is True)
            entry["sampled_thread_cpu_ns"] += sample["thread_cpu_ns"]
            entry["sampled_wall_ns"] += sample["wall_ns"]
            entry["sampled_work_items"] += sample["work_items"]
    for entry in summaries.values():
        items = entry["sampled_work_items"]
        entry["sampled_thread_cpu_ns_per_work_item"] = (
            entry["sampled_thread_cpu_ns"] / items if items else None
        )
        entry["unit_note"] = "Work-item units are role-specific; these are sampled sums, not extrapolated totals."
    return summaries


def _durability_summary(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    phases = [
        phase
        for row in rows
        for phase in (row.get("v8_durability_diagnostics") or {}).get("phases", [])
    ]
    if not phases:
        return None
    summary: dict[str, Any] = {
        field: _distribution([float(phase[field]) for phase in phases])
        for field in V8_DURABILITY_FIELDS
    }
    summary["driver_cpu_ns_per_ticket"] = _distribution([
        float(phase["driver_cpu_ns"]) / int(phase["tickets_covered"])
        for phase in phases if int(phase["tickets_covered"]) > 0
    ])
    summary["sampled_advances"] = len(phases)
    summary["unit_note"] = (
        "Distribution is over sampled completed V8 advances only. Phase durations can overlap or omit work; "
        "do not add their percentiles."
    )
    return summary


def _digest_summary(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    digest = next((row.get("v8_digest") for row in rows if isinstance(row.get("v8_digest"), dict)), None)
    if digest is None:
        return None
    sampled_tickets = int(digest.get("cpu_timing_sampled_tickets", 0))
    return {
        **digest,
        "payload_hash_cpu_ns_per_sampled_ticket": (
            digest["payload_hash_cpu_ns"] / sampled_tickets if sampled_tickets else None
        ),
        "ordered_link_hash_cpu_ns_per_sampled_ticket": (
            digest["ordered_link_hash_cpu_ns"] / sampled_tickets if sampled_tickets else None
        ),
        "note": "Raw sampled CPU totals; digest hashing overlaps producer digest-ready timing.",
    }


def _run_one(
    *, binary: Path, row: dict[str, Any], results_dir: Path, db_root: Path, safe_deadline_epoch: int,
) -> dict[str, Any]:
    run_id = _run_id(row)
    run_dir = results_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    command = _build_command(binary, row, db_root)
    env = {} if row["arm"] == "v7_parallel" else {
        "TOYKV_BENCH_V8_EXTENT_PREP": "sliding-point-lookahead",
        "TOYKV_BENCH_V8_EXTENT_PREP_BYTES": "1048576",
        "TOYKV_WAL_V8_FRONTIER_MODE": "direct",
    }
    (run_dir / "command.sh").write_text(shlex.join(command) + "\n", encoding="utf-8")
    (run_dir / "environment.txt").write_text(
        "\n".join(f"{key}={value}" for key, value in env.items()) + "\n" if env else "<V7 defaults>\n",
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
    validation = validate_v8_point_cpu_attribution_rows(rows)
    by_writer: dict[str, Any] = {}
    for writer in (1, 4, 8, 16):
        by_writer[str(writer)] = {}
        for arm in ("v7_parallel", "v8_point_lookahead_direct"):
            run = next((row for row in rows if row.get("writer") == writer and row.get("arm") == arm), None)
            if run is None:
                by_writer[str(writer)][arm] = None
                continue
            by_writer[str(writer)][arm] = {
                **{key: run.get(key) for key in (
                    "puts_per_second", "process_cpu_us_per_put", "sync_count", "p50_ms", "p95_ms", "p99_ms",
                )},
                "producer_cpu_ns": _producer_phase_summary([run])[arm],
                "thread_roles": _thread_role_summary([run]),
                "durability_advance_cpu_ns": _durability_summary([run]),
                "digest_cpu": _digest_summary([run]),
            }
    return {
        "suite": "v8-point-cpu-attribution",
        "diagnostic_only": True,
        "complete": len(rows) == planned and validation["complete"],
        "runs_completed": len(rows),
        "runs_planned": planned,
        "writers": [1, 4, 8, 16],
        "sample_every": V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
        "results": rows,
        "by_writer": by_writer,
        "matrix_validation": validation,
        "interpretation_note": (
            "Do not report throughput or latency from these instrumented runs as unprofiled scores. "
            "Producer phase fields can overlap: V8 frame-finish contains the listed V8 subphases. Thread-role "
            "work_items differ by role: completion count, reaper ticket count, or tickets covered by a V7 sync. "
            "V8 durability phase samples measure the elected writer's advance; V7 coordinator samples measure "
            "the receive-to-sync coordinator loop."
        ),
    }


def run_suite(args: Any) -> int:
    args.results_dir.mkdir(parents=True, exist_ok=True)
    args.db_root.mkdir(parents=True, exist_ok=True)
    if not isinstance(args.commit, str) or len(args.commit) != 40 or any(
        character not in "0123456789abcdef" for character in args.commit
    ):
        raise ValueError("source commit must be a full lowercase Git SHA")
    binary_digest = hashlib.sha256()
    with args.binary.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b""):
            binary_digest.update(chunk)
    if binary_digest.hexdigest() != args.binary_sha256:
        raise ValueError("write-perf binary SHA-256 does not match the benchmark plan")
    (args.results_dir / "commit.txt").write_text(args.commit + "\n", encoding="utf-8")
    (args.results_dir / "write-perf.sha256").write_text(args.binary_sha256 + "\n", encoding="utf-8")
    schedule = build_v8_point_cpu_attribution_schedule()
    machine_spec = {
        "vcpus": int(args.machine_type.split("-")[2]), "ssd_count": args.ssd_count, "ssd_gib": args.ssd_gib,
    }
    plan = build_v8_point_cpu_attribution_plan(
        machine_type=args.machine_type, machine_spec=machine_spec,
        commit=args.commit, binary_sha256=args.binary_sha256,
    )
    (args.results_dir / "benchmark-plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with (args.results_dir / "v8-point-cpu-attribution-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(schedule)

    rows: list[dict[str, Any]] = []
    failed = False
    runs_started = 0
    for index, item in enumerate(schedule, start=1):
        runs_started += 1
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
        if row.get("exit_code") != 0:
            failed = True
            break

    summary = _summarize(rows, len(schedule))
    summary["source_commit"] = args.commit
    summary["binary_sha256"] = args.binary_sha256
    summary["results_complete"] = summary["complete"] and not failed
    (args.results_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    with (args.results_dir / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
        fields = ("writer", "arm", "exit_code", "ops", "puts_per_second", "process_cpu_us_per_put",
                  "sync_count", "duration_seconds")
        writer = csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})
    failed = failed or not summary["complete"]
    (args.results_dir / "remote-exit.json").write_text(
        json.dumps({"failed": failed, "runs_started": runs_started}, indent=2) + "\n",
        encoding="utf-8",
    )
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
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
    return run_suite(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
