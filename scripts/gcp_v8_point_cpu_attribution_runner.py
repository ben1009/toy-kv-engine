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
from typing import Any, Callable

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
V8_DIGEST_COUNTER_FIELDS = (
    "payload_hash_bytes",
    "payload_hash_cpu_ns",
    "header_batch_hash_cpu_ns",
    "ordered_link_hash_cpu_ns",
    "post_encode_hash_reread_bytes",
    "cpu_timing_sample_rate",
    "cpu_timing_sampled_tickets",
)
# Keep in sync with wal::v8::digest::WAL_V8_HASH_CPU_SAMPLE_RATE and
# write-perf.rs::WAL_V8_HASH_CPU_SAMPLE_RATE.
V8_DIGEST_CPU_SAMPLE_RATE = 64
V8_COMPLETION_TIMELINE_SAMPLE_EVERY = 128


def _run_id(row: dict[str, Any]) -> str:
    return f"w{row['writer']}-point-{row['arm']}"


def _build_command(binary: Path, row: dict[str, Any], db_root: Path) -> list[str]:
    version = 7 if row["arm"] == "v7_parallel" else 8
    command = [
        str(binary), "--bench", "wal_concurrent", "--wal", "--pitr", "--pitr-format", f"v{version}",
        "--wal-io-mode", "parallel", "--num", str(V8_POINT_CPU_ATTRIBUTION_NUM),
        "--threads", str(row["writer"]), "--value-size", "1024", "--target-sst-size", "2147483648",
        "--compaction", "none", "--output", "json", "--latency-sample-every", "100",
        "--wal-cpu-attribution-sample-every", str(V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY),
        "--wal-sync-diagnostics",
        "--path", str(db_root / _run_id(row)),
    ]
    command.append("--wal-completion-timeline")
    return command


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


def _validate_v8_digest(digest: Any) -> dict[str, Any]:
    if not isinstance(digest, dict):
        raise ValueError("V8 digest metrics are missing or are not an object")
    missing = [field for field in V8_DIGEST_COUNTER_FIELDS if field not in digest]
    if missing:
        raise ValueError(f"V8 digest metrics are missing fields: {', '.join(missing)}")
    invalid = [field for field in V8_DIGEST_COUNTER_FIELDS if not _valid_nonnegative_int(digest[field])]
    if invalid:
        raise ValueError(f"V8 digest metrics have invalid counters: {', '.join(invalid)}")
    if digest["cpu_timing_sample_rate"] != V8_DIGEST_CPU_SAMPLE_RATE:
        raise ValueError("V8 digest CPU timing sample rate does not match the expected rate")
    return digest


def _extract_v8_digest(result: dict[str, Any], wal_version: int) -> dict[str, Any] | None:
    digest = result.get("wal_v8_digest")
    if wal_version == 7:
        if digest is not None:
            raise ValueError("V7 result unexpectedly contains V8 digest metrics")
        return None
    if wal_version != 8:
        raise ValueError(f"unsupported WAL version for digest validation: {wal_version}")
    return _validate_v8_digest(digest)


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
    if any(
        not isinstance(sample, dict)
        or any(not _valid_nonnegative_int(sample.get(field)) for field in V8_DURABILITY_FIELDS)
        for sample in phases
    ):
        raise ValueError("V8 durability-driver phase has a missing or invalid summary field")


def _sync_decision_summary(observations: Any) -> dict[str, Any]:
    if not isinstance(observations, list) or not observations:
        raise ValueError("WAL sync decision observations are missing or empty")
    prefix_fields = (
        "admitted_ticket_count",
        "frames_ready_prefix_tickets",
        "data_complete_prefix_tickets",
        "durability_ready_prefix_tickets",
    )
    bool_fields = (
        "next_ticket_present",
        "next_ticket_frames_ready",
        "next_ticket_data_complete",
        "succeeded",
    )
    for index, observation in enumerate(observations):
        if not isinstance(observation, dict):
            raise ValueError(f"WAL sync decision observation {index} is not an object")
        if any(not _valid_nonnegative_int(observation.get(field)) for field in prefix_fields):
            raise ValueError(f"WAL sync decision observation {index} has missing prefix counts")
        if any(type(observation.get(field)) is not bool for field in bool_fields):
            raise ValueError(f"WAL sync decision observation {index} has invalid readiness state")
        if not _valid_nonnegative_int(observation.get("captured_target")):
            raise ValueError(f"WAL sync decision observation {index} has no captured target")
        if not _valid_nonnegative_int(observation.get("decision_ticket_start")):
            raise ValueError(f"WAL sync decision observation {index} has no decision ticket start")
        if not _valid_nonnegative_int(observation.get("written_frontier_start")) or not _valid_nonnegative_int(
            observation.get("written_frontier_end")
        ):
            raise ValueError(f"WAL sync decision observation {index} has invalid ticket frontiers")
        admitted = observation["admitted_ticket_count"]
        decision_start = observation["decision_ticket_start"]
        target = observation["captured_target"]
        frames_ready = observation["frames_ready_prefix_tickets"]
        data_complete = observation["data_complete_prefix_tickets"]
        durability_ready = observation["durability_ready_prefix_tickets"]
        if max(frames_ready, data_complete, durability_ready) > admitted:
            raise ValueError(f"WAL sync decision observation {index} prefix exceeds admitted tickets")
        if target < decision_start or durability_ready != target - decision_start:
            raise ValueError(f"WAL sync decision observation {index} target does not match its decision prefix")
        if durability_ready > min(frames_ready, data_complete):
            raise ValueError(f"WAL sync decision observation {index} durability prefix exceeds an input prefix")
        if observation["succeeded"] is not True:
            raise ValueError(f"WAL sync decision observation {index} reports a failed sync")
        if not observation["next_ticket_present"] and any(
            observation[field] for field in ("next_ticket_frames_ready", "next_ticket_data_complete")
        ):
            raise ValueError(f"WAL sync decision observation {index} reports readiness without a later ticket")

    return {
        "count": len(observations),
        **{
            field: _distribution([float(observation[field]) for observation in observations])
            for field in prefix_fields
        },
        "later_ticket_present_count": sum(observation["next_ticket_present"] for observation in observations),
        "later_ticket_frames_ready_count": sum(
            observation["next_ticket_frames_ready"] for observation in observations
        ),
        "later_ticket_data_complete_count": sum(
            observation["next_ticket_data_complete"] for observation in observations
        ),
        "unit_note": (
            "Prefix counts start at decision_ticket_start, the durable frontier when the target was selected. "
            "written_frontier_start/end describe the separate sync event window. Later-ticket readiness records "
            "the first admitted ticket beyond the selected group; it does not by itself prove that ticket was "
            "waiting for the current sync."
        ),
    }


def _completion_timeline_summary(timeline: Any) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        raise ValueError("V8 completion timeline is missing or is not an object")
    samples = timeline.get("samples")
    if (
        timeline.get("sample_every") != V8_COMPLETION_TIMELINE_SAMPLE_EVERY
        or timeline.get("dropped_samples") != 0
        or not isinstance(samples, list)
        or not samples
    ):
        raise ValueError("V8 completion timeline samples are missing or incomplete")

    duration_fields = (
        "submit_to_cqe_observed_ns",
        "cqe_observed_to_group_finished_ns",
        "group_finished_to_result_send_start_ns",
        "result_send_start_to_reaper_recv_ns",
        "reaper_recv_to_data_complete_publish_ns",
        "data_complete_lock_wait_wall_ns",
        "data_complete_lock_wait_cpu_ns",
        "data_complete_lock_hold_wall_ns",
        "data_complete_lock_hold_cpu_ns",
        "reaper_recv_to_prefix_ready_ns",
        "prefix_ready_to_durable_ns",
    )
    for index, sample in enumerate(samples):
        if not isinstance(sample, dict):
            raise ValueError(f"V8 completion timeline sample {index} is not an object")
        required_ints = (
            "group_id", "ticket_start", "ticket_end", "request_id", "request_start", "request_end",
            "data_complete_prefix_end_before", "data_complete_prefix_end_after", *duration_fields,
        )
        if any(not _valid_nonnegative_int(sample.get(field)) for field in required_ints):
            raise ValueError(f"V8 completion timeline sample {index} has missing or invalid counters")
        if (
            sample["ticket_start"] >= sample["ticket_end"]
            or sample["request_start"] > sample["request_id"]
            or sample["request_id"] >= sample["request_end"]
            or sample["request_id"] % V8_COMPLETION_TIMELINE_SAMPLE_EVERY != 0
            or sample["data_complete_prefix_end_after"] < sample["data_complete_prefix_end_before"]
        ):
            raise ValueError(f"V8 completion timeline sample {index} has inconsistent identities or prefix data")
        blocker_ticket = sample.get("hol_blocker_ticket")
        blocker_frames_ready = sample.get("hol_blocker_frames_ready")
        blocker_digest_folded = sample.get("hol_blocker_digest_folded")
        blocker_write_elapsed = sample.get("hol_blocker_data_write_elapsed_ns")
        if blocker_ticket is None:
            if any(value is not None for value in (blocker_frames_ready, blocker_digest_folded, blocker_write_elapsed)):
                raise ValueError(f"V8 completion timeline sample {index} reports blocker attributes without a blocker")
        elif (
            not _valid_nonnegative_int(blocker_ticket)
            or type(blocker_frames_ready) is not bool
            or type(blocker_digest_folded) is not bool
            or (blocker_write_elapsed is not None and not _valid_nonnegative_int(blocker_write_elapsed))
        ):
            raise ValueError(f"V8 completion timeline sample {index} has invalid blocker attributes")
    return {
        "sample_every": V8_COMPLETION_TIMELINE_SAMPLE_EVERY,
        "sample_count": len(samples),
        **{field: _distribution([float(sample[field]) for sample in samples]) for field in duration_fields},
        "prefix_advanced_count": sum(
            sample["data_complete_prefix_end_after"] > sample["data_complete_prefix_end_before"]
            for sample in samples
        ),
        "hol_blocker_count": sum(sample["hol_blocker_ticket"] is not None for sample in samples),
        "hol_blocker_frames_ready_count": sum(sample["hol_blocker_frames_ready"] is True for sample in samples),
        "hol_blocker_digest_folded_count": sum(sample["hol_blocker_digest_folded"] is True for sample in samples),
        "data_complete_prefix_growth_tickets": _distribution([
            float(sample["data_complete_prefix_end_after"] - sample["data_complete_prefix_end_before"])
            for sample in samples
        ]),
        "hol_blocker_data_write_elapsed_ns": _distribution([
            float(sample["hol_blocker_data_write_elapsed_ns"])
            for sample in samples
            if sample["hol_blocker_data_write_elapsed_ns"] is not None
        ]),
        "unit_note": (
            "One request is sampled per cadence and carries its worker group ticket range. Submit-to-CQE is when "
            "userspace observes completion; it includes kernel completion plus notification and scheduling delay, "
            "so it is not an SSD-only latency. The blocker write age starts when the producer begins publishing DATA "
            "to the ordered worker submitter, before SQE submission, so it includes queueing. Reaper-to-prefix-ready "
            "includes readiness and predecessor ordering."
        ),
    }


def _completion_handoff_timeline_summary(
    timeline: Any,
    expected_operations: int | None = None,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        raise ValueError("parallel completion handoff timeline is missing or is not an object")
    samples = timeline.get("samples")
    if (
        timeline.get("sample_every") != V8_COMPLETION_TIMELINE_SAMPLE_EVERY
        or timeline.get("dropped_samples") != 0
        or not isinstance(samples, list)
        or not samples
    ):
        raise ValueError("parallel completion handoff samples are missing or incomplete")

    duration_fields = (
        "frame_ready_to_submit_request_ns",
        "submit_request_to_worker_queue_ns",
        "worker_queue_to_worker_accept_ns",
        "worker_accept_to_sqe_submit_ns",
        "submit_to_cqe_observed_ns",
        "cqe_observed_to_group_finished_ns",
        "group_finished_to_result_send_start_ns",
        "result_send_start_to_receiver_ns",
        "receiver_to_data_complete_publish_ns",
    )
    sampled_tickets: set[int] = set()
    for index, sample in enumerate(samples):
        if not isinstance(sample, dict):
            raise ValueError(f"completion handoff sample {index} is not an object")
        required_ints = (
            "group_id", "ticket_start", "ticket_end", "request_id", "request_start", "request_end",
            "sampled_ticket", "data_complete_prefix_end_before", "data_complete_prefix_end_after",
            *duration_fields,
        )
        if any(not _valid_nonnegative_int(sample.get(field)) for field in required_ints):
            raise ValueError(f"completion handoff sample {index} has missing or invalid fields")
        if (
            sample["ticket_start"] >= sample["ticket_end"]
            or not sample["ticket_start"] <= sample["sampled_ticket"] < sample["ticket_end"]
            or sample["sampled_ticket"] % V8_COMPLETION_TIMELINE_SAMPLE_EVERY != 0
            or sample["request_start"] > sample["request_id"]
            or sample["request_id"] >= sample["request_end"]
            or sample["data_complete_prefix_end_after"] < sample["data_complete_prefix_end_before"]
        ):
            raise ValueError(f"completion handoff sample {index} has inconsistent identities")
        if sample["sampled_ticket"] in sampled_tickets:
            raise ValueError(f"completion handoff sample {index} duplicates a sampled ticket")
        sampled_tickets.add(sample["sampled_ticket"])
        blocker_ticket = sample.get("hol_blocker_ticket")
        if blocker_ticket is not None and not _valid_nonnegative_int(blocker_ticket):
            raise ValueError(f"completion handoff sample {index} has an invalid HOL blocker ticket")
    if expected_operations is not None:
        expected_tickets = set(
            range(0, expected_operations, V8_COMPLETION_TIMELINE_SAMPLE_EVERY)
        )
        if sampled_tickets != expected_tickets:
            raise ValueError("completion handoff samples do not cover every planned ticket sample")
    return {
        "sample_every": V8_COMPLETION_TIMELINE_SAMPLE_EVERY,
        "sample_count": len(samples),
        "unique_sampled_ticket_count": len(sampled_tickets),
        **{field: _distribution([float(sample[field]) for sample in samples]) for field in duration_fields},
        "hol_blocker_count": sum(sample["hol_blocker_ticket"] is not None for sample in samples),
        "data_complete_prefix_growth_tickets": _distribution([
            float(sample["data_complete_prefix_end_after"] - sample["data_complete_prefix_end_before"])
            for sample in samples
        ]),
    }


def _validate_record(record: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    result = record["result"]
    params = record["params"]
    pipeline = result["wal_pipeline"]
    version = 7 if row["arm"] == "v7_parallel" else 8
    sync_decision = _sync_decision_summary(pipeline.get("sync_observations"))
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
    operations_seen = attribution.get("operations_seen")
    retry_attempts = (
        operations_seen - V8_POINT_CPU_ATTRIBUTION_NUM
        if _valid_nonnegative_int(operations_seen)
        else -1
    )
    expected_samples = operations_seen // V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY if retry_attempts >= 0 else -1
    if (
        attribution.get("sample_every") != V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY
        or retry_attempts < 0
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
    completed_sample_count = 0
    for sample in producer_samples:
        if not isinstance(sample, dict) or sample.get("wal_version") != version or sample.get("entry_count") != 1:
            raise ValueError("producer attribution sample is malformed or incomplete")
        if (
            not _valid_nonnegative_int(sample.get("sample_sequence"))
            or sample["sample_sequence"] % V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY != 0
            or any(not _valid_nonnegative_int(sample.get(field)) for field in PRODUCER_PHASE_FIELDS)
        ):
            raise ValueError("producer attribution sample is malformed or incomplete")
        if sample.get("completed") is True:
            completed_sample_count += 1
            if not _valid_nonnegative_int(sample.get("ticket")) or not _valid_nonnegative_int(sample.get("commit_ts")):
                raise ValueError("completed producer attribution sample is missing its ticket identity")
        elif sample.get("completed") is False and sample.get("ticket") is None and sample.get("commit_ts") is None:
            # Admission-barrier retries are rejected before timestamp or ticket assignment.
            # The successful benchmark run proves the logical operation was retried.
            continue
        else:
            raise ValueError("producer attribution sample is malformed or incomplete")
    if completed_sample_count < max(0, expected_samples - retry_attempts):
        raise ValueError("too few completed producer attribution samples after admission retries")
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
        completion_timeline = _completion_timeline_summary(pipeline.get("v8_completion_timeline"))
    elif durability is not None:
        raise ValueError("V7 run unexpectedly reports V8 durability-driver diagnostics")
    elif pipeline.get("v8_completion_timeline") is not None:
        raise ValueError("V7 run unexpectedly reports a V8 completion timeline")
    else:
        completion_timeline = None

    completion_handoff_timeline = _completion_handoff_timeline_summary(
        pipeline.get("completion_handoff_timeline"),
        int(result["ops"]),
    )

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
        "wal_sync_decision_summary": sync_decision,
        "v8_completion_timeline_summary": completion_timeline,
        "completion_handoff_timeline_summary": completion_handoff_timeline,
        "p50_ms": float(record["latency"]["p50_ms"]),
        "p95_ms": float(record["latency"]["p95_ms"]),
        "p99_ms": float(record["latency"]["p99_ms"]),
        "wal_cpu_attribution_valid": True,
        "wal_cpu_attribution_retry_attempts": retry_attempts,
        "wal_cpu_attribution": attribution,
        "v8_durability_diagnostics": durability,
        "v8_digest": _extract_v8_digest(result, version),
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
        samples = [sample for sample in attribution["producer_samples"] if sample.get("completed") is True]
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
    digest_values = [row["v8_digest"] for row in rows if row.get("v8_digest") is not None]
    if not digest_values:
        return None
    digests = [_validate_v8_digest(value) for value in digest_values]
    digest = digests[0]
    sampled_tickets = digest["cpu_timing_sampled_tickets"]
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
    on_launch: Callable[[], None] | None = None,
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

    command = [
        "timeout", "--signal=INT", "--kill-after=30s", str(RUN_TIMEOUT_SECONDS), "env",
        *(f"{key}={value}" for key, value in env.items()), *command,
    ]
    started = time.monotonic()
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        if on_launch is not None:
            on_launch()
        stdout, stderr = process.communicate()
    completed = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    duration = time.monotonic() - started
    (run_dir / "end_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    (run_dir / "result.json").write_text(completed.stdout, encoding="utf-8")
    (run_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")

    values: dict[str, Any] = {}
    exit_code = completed.returncode
    if exit_code == 0:
        try:
            record = _parse_record(completed.stdout)
            sync_observations = record.get("result", {}).get("wal_pipeline", {}).get("sync_observations")
            if isinstance(sync_observations, list):
                (run_dir / "sync-observations.json").write_text(
                    json.dumps(sync_observations, indent=2) + "\n", encoding="utf-8",
                )
            completion_timeline = record.get("result", {}).get("wal_pipeline", {}).get(
                "v8_completion_timeline"
            )
            if isinstance(completion_timeline, dict):
                (run_dir / "completion-timeline.json").write_text(
                    json.dumps(completion_timeline, indent=2) + "\n", encoding="utf-8",
                )
            completion_handoff_timeline = record.get("result", {}).get("wal_pipeline", {}).get(
                "completion_handoff_timeline"
            )
            if isinstance(completion_handoff_timeline, dict):
                (run_dir / "completion-handoff-timeline.json").write_text(
                    json.dumps(completion_handoff_timeline, indent=2) + "\n", encoding="utf-8",
                )
            values = _validate_record(record, row)
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
                "producer_cpu_ns": _producer_phase_summary([run]).get(arm),
                "thread_roles": _thread_role_summary([run]),
                "durability_advance_cpu_ns": _durability_summary([run]),
                "digest_cpu": _digest_summary([run]),
                "sync_decisions": run.get("wal_sync_decision_summary"),
                "data_completion_timeline": run.get("v8_completion_timeline_summary"),
                "completion_handoff_timeline": run.get("completion_handoff_timeline_summary"),
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
        def record_launch() -> None:
            nonlocal runs_started
            runs_started += 1

        try:
            row = _run_one(
                binary=args.binary, row=item, results_dir=args.results_dir, db_root=args.db_root,
                safe_deadline_epoch=args.safe_deadline_epoch, on_launch=record_launch,
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
