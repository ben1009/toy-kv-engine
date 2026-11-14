#!/usr/bin/env python3
"""Run bounded V7/V8 commit-order traces and untraced Point pairs on a GCP host."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from itertools import groupby
from typing import Any

from gcp_wal_matrix import (
    COMMIT_ORDER_TRACE_LIMIT,
    COMMIT_ORDER_TRACE_NUM,
    COMMIT_ORDER_TRACE_WRITERS,
    build_commit_order_trace_plan,
    build_commit_order_trace_schedule,
    validate_commit_order_trace_rows,
)

RUN_TIMEOUT_SECONDS = 300
V8_ENV = {
    "TOYKV_WAL_V8_FRONTIER_MODE": "direct",
    "TOYKV_WAL_V8_COALESCE_BUDGET_US": "400",
    "TOYKV_BENCH_V8_EXTENT_PREP": "sliding-fallocate",
    "TOYKV_BENCH_V8_EXTENT_PREP_BYTES": "4194304",
}
TRACE_STAGES = (
    "timestamp_reserved",
    "wal_ticket_admitted",
    "durable_frontier_published",
    "wal_return",
    "memtable_publish_complete",
    "mvcc_publication_entry",
    "mvcc_publication_complete",
)


def _validate_trace_result(
    record: dict[str, Any], *, expected_trace_limit: int, expected_arm: str,
) -> None:
    params = record.get("params")
    result = record.get("result")
    if not isinstance(params, dict) or not isinstance(result, dict):
        raise ValueError("benchmark result is missing params or result objects")
    expected_params = {
        "num": COMMIT_ORDER_TRACE_NUM,
        "threads": COMMIT_ORDER_TRACE_WRITERS,
        "value_size": 1024,
        "wal_format_version": 7 if expected_arm == "v7_parallel" else 8,
        "wal_io_mode": "parallel",
    }
    if result.get("ops") != COMMIT_ORDER_TRACE_NUM or any(
        params.get(key) != value for key, value in expected_params.items()
    ):
        raise ValueError("run parameters do not match the planned Point cell")

    pipeline = result.get("wal_pipeline")
    if not isinstance(pipeline, dict):
        raise ValueError("WAL pipeline metrics are missing")
    trace = pipeline.get("commit_order_trace")
    if expected_trace_limit:
        if not isinstance(trace, dict):
            raise ValueError("commit-order trace payload is missing")
        if (
            trace.get("limit") != expected_trace_limit
            or trace.get("selected_records") != expected_trace_limit
            or not isinstance(trace.get("records"), list)
            or len(trace["records"]) != expected_trace_limit
            or trace.get("dropped_records") != 0
            or trace.get("dropped_frontier_events") != 0
            or trace.get("incomplete_records") != 0
        ):
            raise ValueError("commit-order trace is incomplete")
        analyze_trace(trace)
    elif trace is not None:
        raise ValueError("commit-order tracing was enabled for a score run")


def _run_id(row: dict[str, Any]) -> str:
    return (
        f"w{row['writer']}-{row['workload']}-{row['phase']}-r{row['repetition']}-"
        f"{row['arm']}"
    )


def _inversion_count(values: list[int]) -> int:
    ranks = {value: rank for rank, value in enumerate(sorted(set(values)), start=1)}
    tree = [0] * (len(ranks) + 1)
    inversions = 0
    seen = 0
    for value in values:
        index = ranks[value]
        prefix = 0
        cursor = index
        while cursor > 0:
            prefix += tree[cursor]
            cursor -= cursor & -cursor
        inversions += seen - prefix
        cursor = index
        while cursor < len(tree):
            tree[cursor] += 1
            cursor += cursor & -cursor
        seen += 1
    return inversions


def _percentile(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1)
    return ordered[max(0, index)]


def _return_pair_inversion_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(records, key=lambda record: int(record["commit_ts"]))
    inversions = 0
    comparable_pairs = 0
    tie_pairs = 0
    for index, earlier in enumerate(ordered):
        earlier_return_ns = int(earlier["wal_return"]["monotonic_ns"])
        for later in ordered[index + 1:]:
            later_return_ns = int(later["wal_return"]["monotonic_ns"])
            if earlier_return_ns == later_return_ns:
                tie_pairs += 1
            else:
                comparable_pairs += 1
                inversions += int(earlier_return_ns > later_return_ns)
    return {
        "timestamp_inversion_pairs": inversions,
        "timestamp_pair_count": comparable_pairs,
        "timestamp_tie_pairs_excluded": tie_pairs,
        "timestamp_inversion_rate": inversions / comparable_pairs if comparable_pairs else None,
    }


def analyze_trace(trace: dict[str, Any]) -> dict[str, Any]:
    records = trace.get("records")
    if not isinstance(records, list):
        raise ValueError("trace records must be a list")
    complete = [
        record for record in records
        if all(isinstance(record.get(stage), dict) for stage in TRACE_STAGES)
    ]
    if len(complete) != len(records):
        raise ValueError("trace contains an incomplete commit-order record")
    commit_timestamps = [record.get("commit_ts") for record in complete]
    if any(type(commit_ts) is not int or commit_ts <= 0 for commit_ts in commit_timestamps):
        raise ValueError("trace contains an invalid commit timestamp")
    if len(set(commit_timestamps)) != len(commit_timestamps):
        raise ValueError("trace contains duplicate commit timestamps")
    tickets = [record.get("wal_ticket") for record in complete]
    if any(type(ticket) is not int or ticket < 0 for ticket in tickets):
        raise ValueError("trace contains an invalid WAL ticket")
    if len(set(tickets)) != len(tickets):
        raise ValueError("trace contains duplicate WAL tickets")

    for record in complete:
        frontier_ticket_end = record.get("frontier_ticket_end")
        if type(frontier_ticket_end) is not int or frontier_ticket_end <= int(record["wal_ticket"]):
            raise ValueError("trace contains an invalid frontier ticket end")
        for stage in TRACE_STAGES:
            event = record[stage]
            if any(type(event.get(field)) is not int for field in ("sequence", "monotonic_ns", "thread_id")):
                raise ValueError(f"trace stage {stage} has invalid event metadata")
            if event["sequence"] <= 0 or event["monotonic_ns"] <= 0 or event["thread_id"] <= 0:
                raise ValueError(f"trace stage {stage} has a zero event field")

    stage_results: dict[str, Any] = {}
    for stage in TRACE_STAGES:
        ordered_events = sorted(
            (
                int(record[stage]["monotonic_ns"]),
                int(record["commit_ts"]),
            )
            for record in complete
        )
        ranks = {value: rank for rank, value in enumerate(sorted(set(commit_timestamps)), start=1)}
        tree = [0] * (len(ranks) + 1)
        inversions = 0
        comparable_pairs = 0
        seen = 0
        for _, grouped_events in groupby(ordered_events, key=lambda event: event[0]):
            group = list(grouped_events)
            comparable_pairs += seen * len(group)
            for _, commit_ts in group:
                index = ranks[commit_ts]
                prefix = 0
                cursor = index
                while cursor > 0:
                    prefix += tree[cursor]
                    cursor -= cursor & -cursor
                inversions += seen - prefix
            for _, commit_ts in group:
                cursor = ranks[commit_ts]
                while cursor < len(tree):
                    tree[cursor] += 1
                    cursor += cursor & -cursor
            seen += len(group)
        all_pairs = len(complete) * (len(complete) - 1) // 2
        stage_results[stage] = {
            "records": len(complete),
            "timestamp_inversion_pairs": inversions,
            "timestamp_pair_count": comparable_pairs,
            "timestamp_tie_pairs_excluded": all_pairs - comparable_pairs,
            "timestamp_inversion_rate": inversions / comparable_pairs if comparable_pairs else None,
        }

    interval_results: dict[str, Any] = {}
    for first, second in zip(TRACE_STAGES, TRACE_STAGES[1:]):
        key = f"{first}_to_{second}"
        durations = [
            int(record[second]["monotonic_ns"]) - int(record[first]["monotonic_ns"])
            for record in complete
        ]
        interval_results[key] = {
            "p50_ns": _percentile(durations, 0.50),
            "p95_ns": _percentile(durations, 0.95),
            "negative_duration_records": sum(duration < 0 for duration in durations),
        }

    ordered_records = sorted(complete, key=lambda record: int(record["commit_ts"]))
    frontier_groups: dict[int, list[dict[str, Any]]] = {}
    for record in complete:
        frontier_groups.setdefault(int(record["frontier_ticket_end"]), []).append(record)

    category_counts = {
        category: {
            "timestamp_inversion_pairs": 0,
            "timestamp_pair_count": 0,
            "timestamp_tie_pairs_excluded": 0,
        }
        for category in ("intra_group", "inter_group")
    }
    for index, earlier in enumerate(ordered_records):
        for later in ordered_records[index + 1:]:
            category = (
                "intra_group"
                if earlier["frontier_ticket_end"] == later["frontier_ticket_end"]
                else "inter_group"
            )
            earlier_return_ns = int(earlier["wal_return"]["monotonic_ns"])
            later_return_ns = int(later["wal_return"]["monotonic_ns"])
            counts = category_counts[category]
            if earlier_return_ns == later_return_ns:
                counts["timestamp_tie_pairs_excluded"] += 1
            else:
                counts["timestamp_pair_count"] += 1
                counts["timestamp_inversion_pairs"] += int(earlier_return_ns > later_return_ns)

    return_ordering: dict[str, Any] = {"frontier_groups": len(frontier_groups)}
    for category, counts in category_counts.items():
        comparable_pairs = counts["timestamp_pair_count"]
        return_ordering[category] = {
            **counts,
            "timestamp_inversion_rate": (
                counts["timestamp_inversion_pairs"] / comparable_pairs if comparable_pairs else None
            ),
        }

    size_summaries: dict[int, dict[str, Any]] = {}
    incomplete_groups = 0
    incomplete_group_details = []
    previous_ticket_end = 0
    for frontier_ticket_end in sorted(frontier_groups):
        group_records = sorted(frontier_groups[frontier_ticket_end], key=lambda record: int(record["wal_ticket"]))
        expected_size = frontier_ticket_end - previous_ticket_end
        is_complete_group = expected_size > 0 and len(group_records) == expected_size and all(
            int(record["wal_ticket"]) == previous_ticket_end + index
            for index, record in enumerate(group_records)
        )
        if is_complete_group:
            group_size = expected_size
            group_summary = _return_pair_inversion_summary(group_records)
            size_summary = size_summaries.setdefault(
                group_size,
                {
                    "groups": 0,
                    "records": 0,
                    "timestamp_inversion_pairs": 0,
                    "timestamp_pair_count": 0,
                    "timestamp_tie_pairs_excluded": 0,
                    "group_inversion_rates": [],
                },
            )
            size_summary["groups"] += 1
            size_summary["records"] += len(group_records)
            for field in (
                "timestamp_inversion_pairs",
                "timestamp_pair_count",
                "timestamp_tie_pairs_excluded",
            ):
                size_summary[field] += group_summary[field]
            if group_summary["timestamp_inversion_rate"] is not None:
                size_summary["group_inversion_rates"].append(group_summary["timestamp_inversion_rate"])
        else:
            incomplete_groups += 1
            incomplete_group_details.append({
                "frontier_ticket_end": frontier_ticket_end,
                "expected_group_size": expected_size,
                "observed_record_count": len(group_records),
            })
        previous_ticket_end = frontier_ticket_end

    complete_group_summaries: dict[str, dict[str, Any]] = {}
    for group_size, summary in sorted(size_summaries.items()):
        comparable_pairs = summary["timestamp_pair_count"]
        complete_group_summaries[str(group_size)] = {
            **{
                field: value
                for field, value in summary.items()
                if field != "group_inversion_rates"
            },
            "timestamp_inversion_rate": (
                summary["timestamp_inversion_pairs"] / comparable_pairs if comparable_pairs else None
            ),
            "median_group_inversion_rate": _median(summary["group_inversion_rates"]),
        }

    group_size_ordering = {
        "complete_groups": sum(summary["groups"] for summary in size_summaries.values()),
        "incomplete_groups": incomplete_groups,
        "incomplete_group_details": incomplete_group_details,
        "by_group_size": complete_group_summaries,
        "note": (
            "Group-size summaries include only contiguous, fully observed ticket ranges. The trace may end "
            "partway through a durable group; such groups are excluded from size-stratified rates."
        ),
    }

    return {
        "complete_records": len(complete),
        "pairwise_timestamp_inversions": stage_results,
        "stage_intervals": interval_results,
        "wal_return_inversions_by_frontier_group": return_ordering,
        "wal_return_inversions_by_complete_group_size": group_size_ordering,
        "ordering_note": (
            "Inversion rates compare only distinct monotonic clock readings. Events sharing one clock reading "
            "are unordered ties and excluded from that stage's denominator; this includes tickets covered by "
            "one durable-frontier publication. Negative within-record intervals are reported rather than "
            "silently discarded."
        ),
    }


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def summarize_score_pairs(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [row for row in rows if row["phase"] == "score" and row["exit_code"] == 0]
    by_repetition: dict[int, dict[str, dict[str, float]]] = {}
    for row in scores:
        metrics = by_repetition.setdefault(int(row["repetition"]), {})
        metrics[str(row["arm"])] = {
            "puts_per_second": float(row["puts_per_second"]),
            "p99_ms": float(row["p99_ms"]),
            "cpu_seconds": float(row["cpu_seconds"]),
        }

    paired_changes: dict[str, list[float]] = {
        "throughput_pct": [],
        "p99_pct": [],
        "cpu_seconds_pct": [],
    }
    paired_wins = 0
    paired_runs = 0
    for arms in by_repetition.values():
        v7 = arms.get("v7_parallel")
        v8 = arms.get("v8_parallel")
        if not v7 or not v8:
            continue
        paired_runs += 1
        paired_wins += int(v8["puts_per_second"] > v7["puts_per_second"])
        for metric, output in (
            ("puts_per_second", "throughput_pct"),
            ("p99_ms", "p99_pct"),
            ("cpu_seconds", "cpu_seconds_pct"),
        ):
            if v7[metric] > 0:
                paired_changes[output].append((v8[metric] / v7[metric] - 1) * 100)

    summary: dict[str, Any] = {
        "paired_runs": paired_runs,
        "v8_throughput_wins": paired_wins,
        "median_paired_change_pct_v8_vs_v7": {
            key: _median(values) for key, values in paired_changes.items()
        },
    }
    for arm in ("v7_parallel", "v8_parallel"):
        arm_rows = [row for row in scores if row["arm"] == arm]
        summary[arm] = {
            "runs": len(arm_rows),
            **{
                f"median_{field}": _median([float(row[field]) for row in arm_rows])
                for field in ("puts_per_second", "p99_ms", "cpu_seconds")
            },
        }
    return summary


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    results_dir: Path = args.results_dir
    db_root: Path = args.db_root
    if not re.fullmatch(r"[0-9a-f]{64}", args.source_patch_sha256):
        raise ValueError("source patch SHA-256 must be a 64-character lowercase hex digest")
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)

    machine_spec = {"vcpus": 16, "ssd_count": 2, "ssd_gib": 750}
    plan = build_commit_order_trace_plan(
        machine_type="c4-standard-16-lssd",
        machine_spec=machine_spec,
        commit=args.commit,
        binary_sha256=args.binary_sha256,
        source_patch_sha256=args.source_patch_sha256,
    )
    _write_json(results_dir / "benchmark-plan.json", plan)
    schedule = build_commit_order_trace_schedule()
    schedule_path = results_dir / "commit-order-trace-schedule.tsv"
    with schedule_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(schedule[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(schedule)

    summary_columns = (
        "run_index", "writer", "workload", "phase", "repetition", "arm", "trace_limit",
        "exit_code", "ops", "puts_per_second", "p99_ms", "cpu_seconds", "sync_count", "duration_seconds",
    )
    summary_path = results_dir / "summary.tsv"
    with summary_path.open("w", encoding="utf-8", newline="") as stream:
        csv.DictWriter(stream, fieldnames=summary_columns, delimiter="\t").writeheader()

    rows: list[dict[str, Any]] = []
    traces: dict[str, Any] = {}
    failed = False
    for row in schedule:
        now = int(time.time())
        if now + RUN_TIMEOUT_SECONDS + 330 >= args.safe_deadline_epoch:
            failed = True
            break

        run_id = _run_id(row)
        run_dir = results_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        command = [
            str(args.binary), "--bench", str(row["bench"]), "--wal", "--wal-io-mode", "parallel",
            "--num", str(row["num"]), "--threads", str(row["writer"]), "--value-size", "1024",
            "--target-sst-size", "2147483648", "--compaction", "none", "--output", "json",
            "--latency-sample-every", str(row["sample_every"]), "--path", str(db_root / run_id),
            "--pitr", "--pitr-format", "v7" if row["arm"] == "v7_parallel" else "v8",
        ]
        if row["trace_limit"]:
            command.extend(("--commit-order-trace-limit", str(row["trace_limit"])))

        environment: dict[str, str] = {}
        if row["arm"] == "v8_parallel":
            environment.update(V8_ENV)

        (run_dir / "command.sh").write_text(
            shlex.join(["env", *(f"{key}={value}" for key, value in environment.items()), *command]) + "\n",
            encoding="utf-8",
        )
        start_utc = datetime.now(timezone.utc).isoformat()
        (run_dir / "start_utc.txt").write_text(start_utc + "\n", encoding="utf-8")
        start = time.monotonic()
        wrapped = [
            "timeout", "--signal=INT", "--kill-after=30s", str(RUN_TIMEOUT_SECONDS),
            "env", *(f"{key}={value}" for key, value in environment.items()), *command,
        ]
        if row["run_index"] == 1:
            print(f"WAL_BENCHMARK_STARTED {start_utc}", flush=True)
        with (run_dir / "result.json").open("wb") as stdout, (run_dir / "stderr.log").open("wb") as stderr:
            try:
                completed = subprocess.run(
                    wrapped, stdout=stdout, stderr=stderr, check=False,
                    timeout=RUN_TIMEOUT_SECONDS + 35,
                )
                exit_code = completed.returncode
            except subprocess.TimeoutExpired:
                exit_code = 124
        duration = time.monotonic() - start
        (run_dir / "end_utc.txt").write_text(
            datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8"
        )

        metrics: dict[str, Any] = {
            "ops": "", "puts_per_second": "", "p99_ms": "", "cpu_seconds": "", "sync_count": "",
        }
        record = None
        if exit_code == 0:
            try:
                record = json.loads((run_dir / "result.json").read_text(encoding="utf-8"))
                result = record["result"]
                pipeline = result["wal_pipeline"]
                metrics = {
                    "ops": int(result["ops"]),
                    "puts_per_second": float(result["ops_per_sec"]),
                    "p99_ms": float(record["latency"]["p99_ms"]),
                    "cpu_seconds": (float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])) / 1000,
                    "sync_count": int(pipeline["sync_count"]),
                }
                if any(
                    not math.isfinite(float(metrics[field])) or float(metrics[field]) < 0
                    for field in ("puts_per_second", "p99_ms", "cpu_seconds", "sync_count")
                ):
                    raise ValueError("run metrics contain non-finite or negative values")
                _validate_trace_result(
                    record,
                    expected_trace_limit=int(row["trace_limit"]),
                    expected_arm=str(row["arm"]),
                )
                if row["trace_limit"]:
                    trace = pipeline.get("commit_order_trace")
                    assert isinstance(trace, dict)
                    traces[run_id] = analyze_trace(trace)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                (run_dir / "validation-error.txt").write_text(f"{error}\n", encoding="utf-8")
                exit_code = 65

        metadata = {
            key: row[key] for key in ("writer", "workload", "phase", "repetition", "arm", "trace_limit")
        }
        metadata.update({"exit_code": int(exit_code), **metrics, "duration_seconds": duration})
        _write_json(run_dir / "run-metadata.json", metadata)
        summary_row = {**metadata, "run_index": row["run_index"]}
        rows.append(summary_row)
        with summary_path.open("a", encoding="utf-8", newline="") as stream:
            csv.DictWriter(stream, fieldnames=summary_columns, delimiter="\t").writerow(summary_row)
        print(f"run={run_id} exit={exit_code} duration={duration:.2f}s", flush=True)
        if exit_code != 0:
            failed = True
            break

    validation = validate_commit_order_trace_rows(rows)
    planned = len(schedule)
    complete = not failed and len(rows) == planned and validation["complete"]
    summary = {
        "suite": "commit-order-trace",
        "source_commit": args.commit,
        "source_patch_sha256": args.source_patch_sha256,
        "binary_sha256": args.binary_sha256,
        "complete": complete,
        "runs_completed": len(rows),
        "runs_planned": planned,
        "writer": COMMIT_ORDER_TRACE_WRITERS,
        "workload": "point",
        "results": rows,
        "matrix_validation": validation,
        "trace_analysis": traces,
        "score_comparison": summarize_score_pairs(rows),
        "qualification_note": plan["qualification_note"],
    }
    _write_json(results_dir / "summary.json", summary)
    _write_json(results_dir / "commit-order-trace-analysis.json", {
        "commit": args.commit,
        "binary_sha256": args.binary_sha256,
        "source_patch_sha256": args.source_patch_sha256,
        "traced_runs": traces,
        "score_comparison": summary["score_comparison"],
    })
    _write_json(results_dir / "remote-exit.json", {"failed": failed or not complete, "runs_started": len(rows)})
    (results_dir / "finished_utc.txt").write_text(
        datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8"
    )
    print(f"commit-order trace suite finished; runs={len(rows)}/{planned} complete={complete}", flush=True)
    return 0 if complete else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--safe-deadline-epoch", type=int, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--source-patch-sha256", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
