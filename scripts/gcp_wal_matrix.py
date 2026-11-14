"""Shared benchmark plans and exact matrix-completeness validation."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping


SUPPORTED_WRITERS = (1, 2, 4, 8, 16)
LEGACY_DEFAULT_WRITERS = (1, 4, 8)
FULL_DEFAULT_WRITERS = SUPPORTED_WRITERS

RFC026_WORKLOADS = (
    {"name": "point", "bench": "wal_concurrent", "batch_entries": 1, "batch_size": None,
     "num": 100_000, "sample_every": 100},
    {"name": "batch8", "bench": "wal_batch_concurrent", "batch_entries": 8, "batch_size": 8,
     "num": 300_000, "sample_every": 10},
    {"name": "batch16", "bench": "wal_batch_concurrent", "batch_entries": 16, "batch_size": 16,
     "num": 500_000, "sample_every": 10},
    {"name": "batch64", "bench": "wal_batch_concurrent", "batch_entries": 64, "batch_size": 64,
     "num": 750_000, "sample_every": 10},
)
RFC026_ARMS = ("v4_parallel", "v7_parallel", "v8_parallel", "v8_leader")
RFC026_ORDERS = (
    ("v4_parallel", "v7_parallel", "v8_parallel", "v8_leader"),
    ("v7_parallel", "v8_parallel", "v8_leader", "v4_parallel"),
    ("v8_parallel", "v8_leader", "v4_parallel", "v7_parallel"),
    ("v8_leader", "v4_parallel", "v7_parallel", "v8_parallel"),
)
LEGACY_ARMS = ("v4", "v7", "v8")
LEGACY_ORDERS = (
    ("v4", "v7", "v8", "v8", "v7", "v4"),
    ("v8", "v7", "v4", "v4", "v7", "v8"),
)
COMMIT_ORDER_TRACE_LIMIT = 1_024
COMMIT_ORDER_TRACE_REPETITIONS = 5
COMMIT_ORDER_TRACE_WRITERS = 16
COMMIT_ORDER_TRACE_NUM = 100_000
COMMIT_ORDER_TRACE_ORDERS = (("v7_parallel", "v8_parallel"), ("v8_parallel", "v7_parallel"))


def validate_writer_counts(values: Iterable[int]) -> tuple[int, ...]:
    writers = tuple(int(value) for value in values)
    if not writers:
        raise ValueError("at least one writer count is required")
    unsupported = sorted(set(writers) - set(SUPPORTED_WRITERS))
    if unsupported:
        raise ValueError(f"unsupported writer counts: {unsupported}")
    duplicates = sorted(writer for writer, count in Counter(writers).items() if count > 1)
    if duplicates:
        raise ValueError(f"duplicate writer counts are not allowed: {duplicates}")
    return writers


def build_rfc026_schedule(writers: Iterable[int]) -> list[dict[str, object]]:
    """Return the exact ordered RFC 026 run plan consumed by the remote runner."""
    writer_counts = validate_writer_counts(writers)
    schedule: list[dict[str, object]] = []
    for writer in writer_counts:
        for workload in RFC026_WORKLOADS:
            for repetition, order in enumerate(RFC026_ORDERS, start=1):
                for arm in order:
                    schedule.append({
                        "run_index": len(schedule) + 1,
                        "writer": writer,
                        "workload": workload["name"],
                        "repetition": repetition,
                        "arm": arm,
                        **{key: value for key, value in workload.items() if key != "name"},
                    })
    return schedule


def build_batch64_schedule(writers: Iterable[int]) -> list[dict[str, int | str]]:
    writer_counts = validate_writer_counts(writers)
    schedule: list[dict[str, int | str]] = []
    for writer in writer_counts:
        for block, order in enumerate(LEGACY_ORDERS, start=1):
            for position, arm in enumerate(order, start=1):
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "block": block,
                    "position": position,
                    "arm": arm,
                })
    return schedule


def build_commit_order_trace_schedule() -> list[dict[str, int | str]]:
    """Return two trace captures and five counterbalanced, untraced Point pairs."""
    schedule: list[dict[str, int | str]] = []
    for arm in ("v7_parallel", "v8_parallel"):
        schedule.append({
            "run_index": len(schedule) + 1,
            "writer": COMMIT_ORDER_TRACE_WRITERS,
            "workload": "point",
            "phase": "trace",
            "repetition": 0,
            "arm": arm,
            "trace_limit": COMMIT_ORDER_TRACE_LIMIT,
            "bench": "wal_concurrent",
            "batch_entries": 1,
            "num": COMMIT_ORDER_TRACE_NUM,
            "sample_every": 100,
        })

    for repetition in range(1, COMMIT_ORDER_TRACE_REPETITIONS + 1):
        order = COMMIT_ORDER_TRACE_ORDERS[(repetition - 1) % len(COMMIT_ORDER_TRACE_ORDERS)]
        for arm in order:
            schedule.append({
                "run_index": len(schedule) + 1,
                "writer": COMMIT_ORDER_TRACE_WRITERS,
                "workload": "point",
                "phase": "score",
                "repetition": repetition,
                "arm": arm,
                "trace_limit": 0,
                "bench": "wal_concurrent",
                "batch_entries": 1,
                "num": COMMIT_ORDER_TRACE_NUM,
                "sample_every": 100,
            })
    return schedule


def build_commit_order_trace_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str | None = None,
    binary_sha256: str | None = None, source_patch_sha256: str | None = None,
) -> dict[str, object]:
    schedule = build_commit_order_trace_schedule()
    plan: dict[str, object] = {
        "suite": "commit-order-trace",
        "writers": [COMMIT_ORDER_TRACE_WRITERS],
        "formats": ["v7_parallel", "v8_parallel"],
        "workloads": [{
            "name": "point", "bench": "wal_concurrent", "batch_entries": 1,
            "num": COMMIT_ORDER_TRACE_NUM, "sample_every": 100,
        }],
        "trace_records_per_diagnostic_run": COMMIT_ORDER_TRACE_LIMIT,
        "untraced_pairs_planned": COMMIT_ORDER_TRACE_REPETITIONS,
        "runs_planned": len(schedule),
        "score_runs_planned": COMMIT_ORDER_TRACE_REPETITIONS * 2,
        "counterbalanced": True,
        "qualification_note": (
            "Trace runs are diagnostic only; throughput comparisons use separate runs without the "
            "commit-order trace."
        ),
    }
    if commit is not None:
        plan["commit"] = commit
    if binary_sha256 is not None:
        plan["binary_sha256"] = binary_sha256
    if source_patch_sha256 is not None:
        plan["source_patch_sha256"] = source_patch_sha256
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan.update({
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}",
        "value_bytes": 1024,
        "compaction": "none",
    })
    return plan


def build_benchmark_plan(
    *, full_matrix: bool, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    num: int = 750_000, commit: str | None = None, binary_sha256: str | None = None,
) -> dict[str, object]:
    writer_counts = validate_writer_counts(writers)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    if full_matrix:
        workloads = [dict(workload) for workload in RFC026_WORKLOADS]
        arms = list(RFC026_ARMS)
        schedule = build_rfc026_schedule(writer_counts)
        plan: dict[str, object] = {
            "suite": "rfc026-full",
            "writers": list(writer_counts),
            "formats": arms,
            "workloads": workloads,
            "repetitions_per_cell": len(RFC026_ORDERS),
            "format_orders": [list(order) for order in RFC026_ORDERS],
            "score_runs_planned": len(schedule),
            "runs_planned": len(schedule),
            "counterbalanced": True,
        }
    else:
        workloads = [{
            "name": "batch64", "bench": "wal_batch_concurrent", "batch_entries": 64,
            "batch_size": 64, "num": num, "sample_every": 10,
        }]
        arms = list(LEGACY_ARMS)
        schedule = build_batch64_schedule(writer_counts)
        plan = {
            "suite": "batch64",
            "writers": list(writer_counts),
            "formats": arms,
            "workloads": workloads,
            "writes_per_run": num,
            "batch_entries": 64,
            "order_blocks": [list(order) for order in LEGACY_ORDERS],
            "repetitions_per_cell": 4,
            "score_runs_planned": len(schedule),
            "runs_planned": len(schedule),
            "counterbalanced": True,
        }

    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan.update({
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}",
        "value_bytes": 1024,
        "compaction": "none",
        "qualification_note": "Same-host matrix only; no cross-machine or storage-state qualification is implied.",
    })
    if commit is not None:
        plan["commit"] = commit
    if binary_sha256 is not None:
        plan["binary_sha256"] = binary_sha256
    return plan


def _cell(row: Mapping[str, object], fields: tuple[str, ...]) -> tuple[object, ...] | None:
    try:
        return tuple(int(row[field]) if field in ("writer", "block", "position", "repetition")
                     else str(row[field]) for field in fields)
    except (KeyError, TypeError, ValueError):
        return None


def _validate_rows(
    rows: Iterable[Mapping[str, object]], expected: Mapping[tuple[object, ...], int],
    fields: tuple[str, ...],
) -> dict[str, object]:
    observed: Counter[tuple[object, ...]] = Counter()
    invalid_rows = 0
    failed_cells: list[dict[str, object]] = []
    ops_mismatches: list[dict[str, object]] = []
    for row in rows:
        cell = _cell(row, fields)
        if cell is None:
            invalid_rows += 1
            continue
        observed[cell] += 1
        if str(row.get("exit_code", "")) != "0":
            failed_cells.append({"cell": list(cell), "exit_code": row.get("exit_code")})
        try:
            actual_ops = int(row.get("ops", ""))
        except (TypeError, ValueError):
            actual_ops = -1
        if cell in expected and actual_ops != expected[cell]:
            ops_mismatches.append({"cell": list(cell), "expected": expected[cell], "actual": actual_ops})

    expected_cells = set(expected)
    observed_cells = set(observed)
    missing = sorted(expected_cells - observed_cells, key=repr)
    unexpected = sorted(observed_cells - expected_cells, key=repr)
    duplicates = sorted((cell for cell, count in observed.items() if count > 1), key=repr)
    complete = not (missing or unexpected or duplicates or failed_cells or ops_mismatches or invalid_rows)
    return {
        "complete": complete,
        "planned": len(expected),
        "observed": sum(observed.values()) + invalid_rows,
        "unique_cells": len(observed),
        "missing_cells": [list(cell) for cell in missing],
        "unexpected_cells": [list(cell) for cell in unexpected],
        "duplicate_cells": [list(cell) for cell in duplicates],
        "failed_cells": failed_cells,
        "ops_mismatches": ops_mismatches,
        "invalid_rows": invalid_rows,
    }


def validate_rfc026_rows(rows: Iterable[Mapping[str, object]], writers: Iterable[int]) -> dict[str, object]:
    schedule = build_rfc026_schedule(writers)
    expected = {
        (int(item["writer"]), str(item["workload"]), int(item["repetition"]), str(item["arm"])): int(item["num"])
        for item in schedule
    }
    return _validate_rows(rows, expected, ("writer", "workload", "repetition", "arm"))


def validate_batch64_rows(rows: Iterable[Mapping[str, object]], writers: Iterable[int], num: int) -> dict[str, object]:
    schedule = build_batch64_schedule(writers)
    expected = {
        (int(item["writer"]), int(item["block"]), int(item["position"]), str(item["arm"])): num
        for item in schedule
    }
    return _validate_rows(rows, expected, ("writer", "block", "position", "format"))


def validate_commit_order_trace_rows(rows: Iterable[Mapping[str, object]]) -> dict[str, object]:
    schedule = build_commit_order_trace_schedule()
    expected = {
        (int(item["writer"]), str(item["phase"]), int(item["repetition"]), str(item["arm"])):
        int(item["num"])
        for item in schedule
    }
    return _validate_rows(rows, expected, ("writer", "phase", "repetition", "arm"))
