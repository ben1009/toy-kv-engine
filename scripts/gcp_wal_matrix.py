"""Shared benchmark plans and exact matrix-completeness validation."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
import math


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

POINT_LOOKAHEAD_WORKLOADS = (
    {"name": "point", "bench": "wal_concurrent", "batch_entries": 1, "batch_size": None,
     "num": 100_000, "sample_every": 100},
    {"name": "batch8", "bench": "wal_batch_concurrent", "batch_entries": 8, "batch_size": 8,
     "num": 300_000, "sample_every": 10},
    {"name": "batch16", "bench": "wal_batch_concurrent", "batch_entries": 16, "batch_size": 16,
     "num": 500_000, "sample_every": 10},
    {"name": "batch64", "bench": "wal_batch_concurrent", "batch_entries": 64, "batch_size": 64,
     "num": 750_000, "sample_every": 10},
)
POINT_LOOKAHEAD_ORDERS = (
    ("v8_fallocate", "v8_point_lookahead"),
    ("v8_point_lookahead", "v8_fallocate"),
)
POINT_LOOKAHEAD_REPETITIONS = 5
POINT_LOOKAHEAD_WRITERS = 16
POINT_LOOKAHEAD_WINDOW_BYTES = 1 << 20

POINT_BORROWED_INPUT_WRITERS = (1, 8, 16)
POINT_BORROWED_INPUT_REPETITIONS = 5
POINT_BORROWED_INPUT_NUM = 100_000
POINT_BORROWED_INPUT_WORKLOADS = (
    {"name": "point", "bench": "wal_concurrent", "batch_entries": 1, "batch_size": None},
    {"name": "batch16", "bench": "wal_batch_concurrent", "batch_entries": 16, "batch_size": 16},
    {"name": "batch64", "bench": "wal_batch_concurrent", "batch_entries": 64, "batch_size": 64},
)
POINT_BORROWED_INPUT_ARMS = ("v8_owned_baseline", "v8_borrowed_candidate")
POINT_BORROWED_INPUT_ORDERS = (
    ("v8_owned_baseline", "v8_borrowed_candidate"),
    ("v8_borrowed_candidate", "v8_owned_baseline"),
)

POINT_DIRECT_FRAME_WRITERS = (1, 8, 16)
POINT_DIRECT_FRAME_REPETITIONS = 5
POINT_DIRECT_FRAME_NUM = 100_000
POINT_DIRECT_FRAME_ARMS = ("direct_frame_off", "direct_frame_on")
POINT_DIRECT_FRAME_ORDERS = (
    ("direct_frame_off", "direct_frame_on"),
    ("direct_frame_on", "direct_frame_off"),
)

POINT_PROGRESS_RING_WRITERS = (1, 8, 16)
POINT_PROGRESS_RING_REPETITIONS = 5
POINT_PROGRESS_RING_NUM = 100_000
POINT_PREALLOCATION_WRITERS = (1, 8, 16)
POINT_PREALLOCATION_REPETITIONS = 5
POINT_PREALLOCATION_NUM = 100_000
POINT_PREALLOCATION_ARMS = ("v7_parallel", "v8_prealloc_off", "v8_prealloc_on")
POINT_PREALLOCATION_POINT_ORDERS = (
    ("v7_parallel", "v8_prealloc_off", "v8_prealloc_on"),
    ("v8_prealloc_off", "v8_prealloc_on", "v7_parallel"),
    ("v8_prealloc_on", "v7_parallel", "v8_prealloc_off"),
    ("v7_parallel", "v8_prealloc_on", "v8_prealloc_off"),
    ("v8_prealloc_on", "v8_prealloc_off", "v7_parallel"),
)
POINT_PREALLOCATION_BATCH_ORDERS = (
    ("v8_prealloc_off", "v8_prealloc_on"),
    ("v8_prealloc_on", "v8_prealloc_off"),
)
V7_V8_READY_TARGET_FINAL_WRITERS = (1, 8, 16)
V7_V8_READY_TARGET_FINAL_REPETITIONS = 5
V7_V8_READY_TARGET_FINAL_NUM = 100_000
V7_V8_READY_TARGET_FINAL_ARMS = ("v7_parallel", "v8_dynamic", "v8_target4", "v8_target8")
V7_V8_READY_TARGET_FINAL_ORDERS = (
    ("v7_parallel", "v8_dynamic", "v8_target4", "v8_target8"),
    ("v8_dynamic", "v8_target4", "v8_target8", "v7_parallel"),
    ("v8_target4", "v8_target8", "v7_parallel", "v8_dynamic"),
    ("v8_target8", "v7_parallel", "v8_dynamic", "v8_target4"),
    ("v8_target8", "v8_target4", "v8_dynamic", "v7_parallel"),
)
V7_V8_READY_TARGET_FINAL_CASES = (
    (1, "point"),
    (8, "point"),
    (16, "point"),
    (16, "batch64"),
)
POINT_PROGRESS_PLAN_SCHEMA_V1 = 1
POINT_PROGRESS_PLAN_SCHEMA_V2 = 2
POINT_PROGRESS_PLAN_SCHEMA_V3 = 3
CURSOR_DIRECT_COMPLETION_PLAN_SCHEMA_V1 = 1
CURSOR_DIRECT_COMPLETION_WAKE_PLAN_SCHEMA_V1 = 1
CURSOR_SINGLETON_SUBMISSION_PLAN_SCHEMA_V1 = 1
POINT_PROGRESS_RING_ARMS = ("eager", "progress_ring")
POINT_PROGRESS_CURSOR_ARMS = ("eager", "cursor")
POINT_PROGRESS_CURSOR_V2_ARMS = ("eager", "progress_ring", "cursor")
CURSOR_DIRECT_COMPLETION_ARMS = ("cursor_relay", "cursor_direct")
CURSOR_SINGLETON_SUBMISSION_ARMS = ("cursor_control", "cursor_singleton")
CURSOR_PUBLICATION_WRITERS = (1, 8, 16)
CURSOR_PUBLICATION_REPETITIONS = 5
CURSOR_PUBLICATION_NUM = 100_000
CURSOR_PUBLICATION_PLAN_SCHEMA_V2 = 2
CURSOR_PUBLICATION_ARMS = ("baseline", "admission_fast_path")
CURSOR_PUBLICATION_ORDERS = (
    ("baseline", "admission_fast_path"),
    ("admission_fast_path", "baseline"),
)
POINT_RESOURCE_RETIREMENT_WRITERS = (8, 16)
POINT_RESOURCE_RETIREMENT_REPETITIONS = 5
POINT_RESOURCE_RETIREMENT_NUM = 100_000
POINT_RESOURCE_RETIREMENT_ARMS = ("per_ticket", "detach", "batch")
POINT_RESOURCE_RETIREMENT_MODES = {"per_ticket": 0, "detach": 1, "batch": 2}
POINT_RESOURCE_RETIREMENT_ORDERS = (
    ("per_ticket", "detach", "batch"),
    ("detach", "batch", "per_ticket"),
    ("batch", "per_ticket", "detach"),
    ("per_ticket", "batch", "detach"),
    ("batch", "detach", "per_ticket"),
)
POINT_ACK_D1_WRITERS = (8, 16)
POINT_ACK_D1_REPETITIONS = 5
POINT_ACK_D1_NUM = 100_000
POINT_ACK_D1_ARMS = ("d1_before", "d1_after")
POINT_ACK_D1_ORDERS = (
    ("d1_before", "d1_after"),
    ("d1_after", "d1_before"),
)
DURABILITY_DRIVER_WRITERS = (1, 8, 16)
DURABILITY_DRIVER_REPETITIONS = 5
DURABILITY_DRIVER_NUM = 100_000
DURABILITY_DRIVER_ARMS = (
    "caller_legacy_ack",
    "caller_independent_ack",
    "dedicated_independent_ack",
)
DURABILITY_DRIVER_SCORE_ORDERS = (
    ("caller_legacy_ack", "caller_independent_ack", "dedicated_independent_ack"),
    ("caller_independent_ack", "dedicated_independent_ack", "caller_legacy_ack"),
    ("dedicated_independent_ack", "caller_legacy_ack", "caller_independent_ack"),
    ("caller_legacy_ack", "dedicated_independent_ack", "caller_independent_ack"),
    ("dedicated_independent_ack", "caller_independent_ack", "caller_legacy_ack"),
)
DURABILITY_DRIVER_DIAGNOSTIC_ORDER: tuple[tuple[int, str], ...] = ()
CURSOR_COALESCING_BUDGET_WRITERS = (8, 16)
CURSOR_COALESCING_BUDGET_REPETITIONS = 5
CURSOR_COALESCING_BUDGET_NUM = 100_000
CURSOR_COALESCING_BUDGET_ARMS = ("dynamic", "fixed_100", "fixed_400")
CURSOR_COALESCING_BUDGET_ORDERS = (
    ("dynamic", "fixed_100", "fixed_400"),
    ("fixed_100", "fixed_400", "dynamic"),
    ("fixed_400", "dynamic", "fixed_100"),
)
CURSOR_COALESCING_READY_TARGET_WRITERS = (1, 8, 16)
CURSOR_COALESCING_READY_TARGET_REPETITIONS = 5
CURSOR_COALESCING_READY_TARGET_NUM = 100_000
CURSOR_COALESCING_READY_TARGET_ARMS = ("dynamic", "target_8", "target_12")
CURSOR_COALESCING_READY_TARGET_ORDERS = (
    ("dynamic", "target_8", "target_12"),
    ("target_8", "target_12", "dynamic"),
    ("target_12", "dynamic", "target_8"),
)
CURSOR_COALESCING_READY_TARGET_CASES = (
    (1, "point"),
    (8, "point"),
    (16, "point"),
    (16, "batch64"),
)
CURSOR_COALESCING_READY_TARGET_SCAN_WRITERS = (8, 16)
CURSOR_COALESCING_READY_TARGET_SCAN_REPETITIONS = 5
CURSOR_COALESCING_READY_TARGET_SCAN_NUM = 100_000
CURSOR_COALESCING_READY_TARGET_SCAN_ARMS = ("dynamic", "target_4", "target_6", "target_8")
CURSOR_COALESCING_READY_TARGET_SCAN_ORDERS = (
    ("dynamic", "target_4", "target_6", "target_8"),
    ("target_4", "target_6", "target_8", "dynamic"),
    ("target_6", "target_8", "dynamic", "target_4"),
    ("target_8", "dynamic", "target_4", "target_6"),
)
CURSOR_COALESCING_READY_TARGET_SCAN_CASES = ((8, "point"), (16, "point"))
CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_WRITERS = (8, 16)
CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_REPETITIONS = 5
CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_NUM = 100_000
CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_ARMS = (
    "dynamic", "target_1", "target_2", "target_3", "target_4",
)
CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_ORDERS = (
    ("dynamic", "target_1", "target_2", "target_3", "target_4"),
    ("target_1", "target_2", "target_3", "target_4", "dynamic"),
    ("target_2", "target_3", "target_4", "dynamic", "target_1"),
    ("target_3", "target_4", "dynamic", "target_1", "target_2"),
    ("target_4", "dynamic", "target_1", "target_2", "target_3"),
)
CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_CASES = ((8, "point"), (16, "point"))
POINT_PROGRESS_RING_ORDERS = (
    ("eager", "progress_ring"),
    ("progress_ring", "eager"),
)
POINT_PROGRESS_CURSOR_V2_ORDERS = (
    ("eager", "progress_ring", "cursor"),
    ("progress_ring", "cursor", "eager"),
    ("cursor", "eager", "progress_ring"),
    ("eager", "cursor", "progress_ring"),
    ("cursor", "progress_ring", "eager"),
)
POINT_PROGRESS_CURSOR_ORDERS = tuple(
    tuple("cursor" if arm == "progress_ring" else arm for arm in order)
    for order in POINT_PROGRESS_RING_ORDERS
)
CURSOR_DIRECT_COMPLETION_ORDERS = (
    ("cursor_relay", "cursor_direct"),
    ("cursor_direct", "cursor_relay"),
)
CURSOR_DIRECT_COMPLETION_WAKE_ARMS = ("token", "select")
CURSOR_DIRECT_COMPLETION_WAKE_ORDERS = (
    ("token", "select"),
    ("select", "token"),
)
CURSOR_SINGLETON_SUBMISSION_ORDERS = (
    ("cursor_control", "cursor_singleton"),
    ("cursor_singleton", "cursor_control"),
)
POINT_PROGRESS_RING_CELLS = (
    (1, "point", "wal_concurrent", 1, None, 10),
    (8, "point", "wal_concurrent", 1, None, 10),
    (16, "point", "wal_concurrent", 1, None, 10),
    (16, "batch64", "wal_batch_concurrent", 64, 64, None),
)
V8_EXTENT_POLICY_STORAGE_WRITERS = (8, 16)
V8_EXTENT_POLICY_STORAGE_REPETITIONS = 5
V8_EXTENT_POLICY_STORAGE_NUM = 100_000
V8_EXTENT_POLICY_STORAGE_ARMS = ("btree", "vec")
V8_EXTENT_POLICY_STORAGE_ORDERS = (("btree", "vec"), ("vec", "btree"))
V8_EXTENT_POLICY_STORAGE_WORKLOADS = (
    {"name": "point", "bench": "wal_concurrent", "batch_entries": 1, "batch_size": None},
    {"name": "batch16", "bench": "wal_batch_concurrent", "batch_entries": 16, "batch_size": 16},
    {"name": "batch64", "bench": "wal_batch_concurrent", "batch_entries": 64, "batch_size": 64},
)

V8_MUTEX_DIAGNOSTIC_WRITERS = (16,)
V8_MUTEX_DIAGNOSTIC_REPETITIONS = 5
V8_MUTEX_DIAGNOSTIC_NUM = 100_000
V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY = 64
V8_MUTEX_DIAGNOSTIC_ORDERS = (
    ("v7_parallel", "v8_point_lookahead_direct"),
    ("v8_point_lookahead_direct", "v7_parallel"),
)

V8_POINT_CPU_ATTRIBUTION_WRITERS = (1, 4, 8, 16)
V8_POINT_CPU_ATTRIBUTION_NUM = 100_000
V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY = 64
V8_POINT_CPU_ATTRIBUTION_ARMS = ("v7_parallel", "v8_point_lookahead_direct")


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


def build_point_lookahead_schedule(writers: Iterable[int]) -> list[dict[str, object]]:
    """Compare V8 fallocate with Point-only lookahead, including batch guardrails."""
    writer_counts = validate_writer_counts(writers)
    schedule: list[dict[str, object]] = []
    for writer in writer_counts:
        for workload in POINT_LOOKAHEAD_WORKLOADS:
            for repetition in range(1, POINT_LOOKAHEAD_REPETITIONS + 1):
                order = POINT_LOOKAHEAD_ORDERS[(repetition - 1) % len(POINT_LOOKAHEAD_ORDERS)]
                for arm in order:
                    schedule.append({
                        "run_index": len(schedule) + 1,
                        "writer": writer,
                        "workload": workload["name"],
                        "repetition": repetition,
                        "arm": arm,
                        **{key: value for key, value in workload.items() if key != "name"},
                        "extent_mode": (
                            "sliding-point-lookahead"
                            if arm == "v8_point_lookahead" and workload["name"] == "point"
                            else "sliding-fallocate"
                        ),
                        "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    })
    return schedule


def build_point_lookahead_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str | None = None, binary_sha256: str | None = None,
) -> dict[str, object]:
    writer_counts = validate_writer_counts(writers)
    schedule = build_point_lookahead_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "suite": "v8-point-lookahead",
        "writers": list(writer_counts),
        "workloads": [dict(workload) for workload in POINT_LOOKAHEAD_WORKLOADS],
        "arms": ["v8_fallocate", "v8_point_lookahead"],
        "repetitions_per_cell": POINT_LOOKAHEAD_REPETITIONS,
        "arm_orders": [list(order) for order in POINT_LOOKAHEAD_ORDERS],
        "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}",
        "value_bytes": 1024,
        "compaction": "none",
        "qualification_note": (
            "Same-VM V8 comparison. Point uses one-entry DATA writes; batch cells verify that the "
            "point-only lookahead mode leaves multi-entry batches on fallocate-only preparation."
        ),
    }
    if commit is not None:
        plan["commit"] = commit
    if binary_sha256 is not None:
        plan["binary_sha256"] = binary_sha256
    return plan


def build_point_borrowed_input_schedule(writers: Iterable[int]) -> list[dict[str, object]]:
    """Build paired Point and Batch guardrail runs for the borrowed-input change."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_BORROWED_INPUT_WRITERS:
        raise ValueError(f"borrowed-input suite requires writers {POINT_BORROWED_INPUT_WRITERS}")
    schedule: list[dict[str, object]] = []
    cells = [(writer, POINT_BORROWED_INPUT_WORKLOADS[0]) for writer in writer_counts]
    cells.extend((16, workload) for workload in POINT_BORROWED_INPUT_WORKLOADS[1:])
    for writer, workload in cells:
        for repetition in range(1, POINT_BORROWED_INPUT_REPETITIONS + 1):
            order = POINT_BORROWED_INPUT_ORDERS[(repetition - 1) % len(POINT_BORROWED_INPUT_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload["name"],
                    "repetition": repetition,
                    "arm": arm,
                    "bench": workload["bench"],
                    "batch_entries": workload["batch_entries"],
                    "batch_size": workload["batch_size"],
                    "num": POINT_BORROWED_INPUT_NUM,
                    "sample_every": 100 if workload["name"] == "point" else 10,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                })
    return schedule


def build_point_borrowed_input_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    baseline_commit: str, candidate_commit: str, source_patch_sha256: str,
    baseline_binary_sha256: str | None = None, candidate_binary_sha256: str | None = None,
) -> dict[str, object]:
    writer_counts = validate_writer_counts(writers)
    schedule = build_point_borrowed_input_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "suite": "v8-point-borrowed-input",
        "writers": list(writer_counts),
        "workloads": [dict(workload, num=POINT_BORROWED_INPUT_NUM) for workload in POINT_BORROWED_INPUT_WORKLOADS],
        "arms": list(POINT_BORROWED_INPUT_ARMS),
        "repetitions_per_cell": POINT_BORROWED_INPUT_REPETITIONS,
        "arm_orders": [list(order) for order in POINT_BORROWED_INPUT_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; both revisions built with the pinned toolchain",
        "value_bytes": 1024,
        "compaction": "none",
        "baseline_commit": baseline_commit,
        "candidate_commit": candidate_commit,
        "source_patch_sha256": source_patch_sha256,
        "extent_preparation": "sliding-point-lookahead, 1 MiB",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "qualification_note": (
            "Only V8 Point preparation source differs. The candidate borrows the Point key/value during WAL "
            "preparation; Point/Batch admission, frame jobs, durability, extent policy and frontier settings "
            "are held fixed. Batch16/64 are paired regression guardrails."
        ),
    }
    if baseline_binary_sha256 is not None:
        plan["baseline_binary_sha256"] = baseline_binary_sha256
    if candidate_binary_sha256 is not None:
        plan["candidate_binary_sha256"] = candidate_binary_sha256
    return plan


def validate_point_borrowed_input_rows(rows: list[Mapping[str, object]], writers: Iterable[int]) -> dict[str, object]:
    """Check every paired run identity, count and successful operation total."""
    schedule = build_point_borrowed_input_schedule(writers)
    expected = {
        (
            int(row["writer"]),
            str(row["workload"]),
            int(row["repetition"]),
            str(row["arm"]),
        ): int(row["num"])
        for row in schedule
    }
    return _validate_rows(rows, expected, ("writer", "workload", "repetition", "arm"))


def build_point_direct_frame_schedule(writers: Iterable[int]) -> list[dict[str, object]]:
    """Build same-binary direct-frame A/B runs and Point/Batch guardrails."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_DIRECT_FRAME_WRITERS:
        raise ValueError(f"direct-frame suite requires writers {POINT_DIRECT_FRAME_WRITERS}")
    schedule: list[dict[str, object]] = []
    cells = [(writer, POINT_BORROWED_INPUT_WORKLOADS[0]) for writer in writer_counts]
    cells.extend((16, workload) for workload in POINT_BORROWED_INPUT_WORKLOADS[1:])
    for writer, workload in cells:
        for repetition in range(1, POINT_DIRECT_FRAME_REPETITIONS + 1):
            order = POINT_DIRECT_FRAME_ORDERS[(repetition - 1) % len(POINT_DIRECT_FRAME_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload["name"],
                    "repetition": repetition,
                    "arm": arm,
                    "bench": workload["bench"],
                    "batch_entries": workload["batch_entries"],
                    "batch_size": workload["batch_size"],
                    "num": POINT_DIRECT_FRAME_NUM,
                    "sample_every": 100 if workload["name"] == "point" else 10,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "point_direct_frame_enabled": arm == "direct_frame_on",
                })
    return schedule


def build_point_direct_frame_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    writer_counts = validate_writer_counts(writers)
    schedule = build_point_direct_frame_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "suite": "v8-point-direct-frame",
        "writers": list(writer_counts),
        "workloads": [dict(workload, num=POINT_DIRECT_FRAME_NUM) for workload in POINT_BORROWED_INPUT_WORKLOADS],
        "arms": list(POINT_DIRECT_FRAME_ARMS),
        "repetitions_per_cell": POINT_DIRECT_FRAME_REPETITIONS,
        "arm_orders": [list(order) for order in POINT_DIRECT_FRAME_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same binary and source for both arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "extent_preparation": "sliding-point-lookahead, 1 MiB",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "point_direct_frame_ab": {
            "environment_variable": "TOYKV_BENCH_V8_POINT_DIRECT_FRAME",
            "baseline_value": "0",
            "candidate_value": "1",
        },
        "qualification_note": (
            "The direct-frame flag is the only A/B difference; both arms use the same bench binary. "
            "Every Point put must report one completed direct-frame encode. Batch16 and Batch64 must report zero."
        ),
    }
    return plan


def validate_point_direct_frame_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int]
) -> dict[str, object]:
    """Check planned run identities, success counts, and actual encoder hits."""
    schedule = build_point_direct_frame_schedule(writers)
    expected = {
        (row["writer"], row["workload"], row["repetition"], row["arm"]): row for row in schedule
    }
    observed: dict[tuple[object, object, object, object], int] = {}
    invalid: list[Mapping[str, object]] = []
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "repetition", "arm"))
        observed[identity] = observed.get(identity, 0) + 1
        expected_row = expected.get(identity)
        should_encode = bool(expected_row and expected_row["point_direct_frame_enabled"])
        expected_hits = POINT_DIRECT_FRAME_NUM if should_encode and row.get("workload") == "point" else 0
        if (
            expected_row is None
            or row.get("exit_code") != 0
            or row.get("ops") != POINT_DIRECT_FRAME_NUM
            or row.get("point_direct_frame_enabled") != should_encode
            or row.get("direct_frame_encoded_count") != expected_hits
        ):
            invalid.append(row)

    expected_cells = set(expected)
    observed_cells = set(observed)
    return {
        "complete": not (
            expected_cells != observed_cells or any(count != 1 for count in observed.values()) or invalid
        ),
        "planned": len(schedule),
        "observed": len(rows),
        "missing_cells": [list(cell) for cell in sorted(expected_cells - observed_cells)],
        "unexpected_cells": [list(cell) for cell in sorted(observed_cells - expected_cells, key=str)],
        "duplicate_cells": [list(cell) for cell, count in observed.items() if count != 1],
        "invalid_rows": [dict(row) for row in invalid],
    }


def build_point_progress_ring_schedule(writers: Iterable[int]) -> list[dict[str, object]]:
    """Build five counterbalanced eager/F0 pairs for Point and Batch64."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_PROGRESS_RING_WRITERS:
        raise ValueError(f"progress-ring qualification requires writers {POINT_PROGRESS_RING_WRITERS}")

    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS:
        if workload == "point" and writer not in writer_counts:
            continue
        for repetition in range(1, POINT_PROGRESS_RING_REPETITIONS + 1):
            order = POINT_PROGRESS_RING_ORDERS[(repetition - 1) % len(POINT_PROGRESS_RING_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_PROGRESS_RING_NUM,
                    "sample_every": sample_every,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                    "progress_ring_enabled": arm == "progress_ring",
                })
    return schedule


def build_point_progress_cursor_schedule(writers: Iterable[int]) -> list[dict[str, object]]:
    """Build a same-host Eager/Cursor comparison; retired Ring mode is not runnable."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_PROGRESS_RING_WRITERS:
        raise ValueError(f"progress-cursor qualification requires writers {POINT_PROGRESS_RING_WRITERS}")

    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS:
        if workload == "point" and writer not in writer_counts:
            continue
        for repetition in range(1, POINT_PROGRESS_RING_REPETITIONS + 1):
            order = POINT_PROGRESS_CURSOR_ORDERS[(repetition - 1) % len(POINT_PROGRESS_CURSOR_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_PROGRESS_RING_NUM,
                    "sample_every": sample_every,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                    "progress_ring_enabled": arm == "cursor",
                    "progress_cursor_enabled": arm == "cursor",
                    "progress_counters_enabled": False,
                })
    return schedule


def build_v7_v8_point_preallocation_schedule() -> list[dict[str, object]]:
    """Build the V7/V8/preallocation comparison and Batch64 guardrail schedule."""
    point_cells = tuple(
        (writer, "point", "wal_concurrent", 1, None, 10)
        for writer in POINT_PREALLOCATION_WRITERS
    )
    cells = (*point_cells, (16, "batch64", "wal_batch_concurrent", 64, 64, None))
    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in cells:
        for repetition in range(1, POINT_PREALLOCATION_REPETITIONS + 1):
            if workload == "point":
                order = POINT_PREALLOCATION_POINT_ORDERS[repetition - 1]
                arms = order
            else:
                arms = POINT_PREALLOCATION_BATCH_ORDERS[(repetition - 1) % 2]
            for arm in arms:
                is_v7 = arm == "v7_parallel"
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_PREALLOCATION_NUM,
                    "sample_every": sample_every,
                    "format_version": 7 if is_v7 else 8,
                    "point_buffer_preallocation_enabled": None if is_v7 else arm == "v8_prealloc_on",
                    "progress_ring_enabled": not is_v7,
                    "progress_cursor_enabled": not is_v7,
                    "progress_counters_enabled": False if not is_v7 else None,
                    "cursor_direct_completion_enabled": not is_v7 if not is_v7 else None,
                    "cursor_singleton_submission_enabled": False if not is_v7 else None,
                    "extent_mode": "sliding-point-lookahead" if not is_v7 else None,
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES if not is_v7 else 0,
                    "frontier_mode": "direct" if not is_v7 else None,
                })
    return schedule


def build_v7_v8_point_preallocation_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str,
    binary_sha256: str | None = None,
) -> dict[str, object]:
    schedule = build_v7_v8_point_preallocation_schedule()
    return {
        "plan_schema_version": 1,
        "suite": "v7-v8-point-preallocation",
        "writers": list(POINT_PREALLOCATION_WRITERS),
        "workloads": [
            {"writer": writer, "name": "point", "bench": "wal_concurrent", "batch_size": None,
             "num": POINT_PREALLOCATION_NUM, "latency_sample_every": 10}
            for writer in POINT_PREALLOCATION_WRITERS
        ] + [
            {"writer": 16, "name": "batch64", "bench": "wal_batch_concurrent", "batch_size": 64,
             "num": POINT_PREALLOCATION_NUM, "latency_sample_every": None}
        ],
        "arms": list(POINT_PREALLOCATION_ARMS),
        "repetitions_per_point_cell": POINT_PREALLOCATION_REPETITIONS,
        "repetitions_per_batch_guardrail": POINT_PREALLOCATION_REPETITIONS,
        "point_arm_orders": [list(order) for order in POINT_PREALLOCATION_POINT_ORDERS],
        "batch_arm_orders": [list(order) for order in POINT_PREALLOCATION_BATCH_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": int(machine_spec["vcpus"]),
        "local_ssd_count": int(machine_spec["ssd_count"]),
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": "single C4 Local SSD host; same source commit and binary for all arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "v8_configuration": {
            "progress_mode": "cursor (mode 2)",
            "completion": "direct",
            "frontier": "direct",
            "coalescing": "dynamic",
            "extent_preparation": "1 MiB sliding-point-lookahead for Point; Batch64 remains fallocate-only",
            "singleton_submission": False,
            "point_buffer_preallocation": "sole V8 A/B variable",
        },
        "v7_configuration": "V7 Parallel; no V8-only environment variables",
        "local_screen": {
            "status": "skipped_by_user",
            "reason": "Local SSD results were excluded at the user's direction because the local SSD is unstable.",
        },
        "qualification_note": (
            "Point1/8/16 each compare V7 Parallel, V8 preallocation off, and V8 preallocation on in five "
            "counterbalanced repetitions. Batch64 is a five-pair V8 off/on regression guardrail. Same host, "
            "source commit, release bench binary, and V8 settings are used throughout. Point latency is sampled "
            "every 10 puts; Batch64 latency sampling is disabled."
        ),
    }


def build_v7_v8_ready_target_final_schedule() -> list[dict[str, object]]:
    """Build the final same-binary V7/V8 Dynamic/Target4/Target8 matrix."""
    schedule: list[dict[str, object]] = []
    for repetition in range(1, V7_V8_READY_TARGET_FINAL_REPETITIONS + 1):
        case_order = V7_V8_READY_TARGET_FINAL_CASES[repetition % len(V7_V8_READY_TARGET_FINAL_CASES):] + (
            V7_V8_READY_TARGET_FINAL_CASES[:repetition % len(V7_V8_READY_TARGET_FINAL_CASES)]
        )
        for writer, workload in case_order:
            case_index = V7_V8_READY_TARGET_FINAL_CASES.index((writer, workload))
            arm_order = V7_V8_READY_TARGET_FINAL_ORDERS[(repetition - 1 + case_index) % len(
                V7_V8_READY_TARGET_FINAL_ORDERS
            )]
            is_batch = workload == "batch64"
            bench = "wal_batch_concurrent" if is_batch else "wal_concurrent"
            batch_entries = 64 if is_batch else 1
            batch_size = 64 if is_batch else None
            sample_every = None if is_batch else 10
            for arm in arm_order:
                is_v7 = arm == "v7_parallel"
                target = 4 if arm == "v8_target4" else 8 if arm == "v8_target8" else None
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": V7_V8_READY_TARGET_FINAL_NUM,
                    "sample_every": sample_every,
                    "format_version": 7 if is_v7 else 8,
                    "coalescing_ready_target": target,
                    "point_buffer_preallocation_enabled": None if is_v7 else not is_batch,
                    "progress_ring_enabled": not is_v7,
                    "progress_cursor_enabled": not is_v7,
                    "progress_counters_enabled": False if not is_v7 else None,
                    "cursor_direct_completion_enabled": True if not is_v7 else None,
                    "cursor_singleton_submission_enabled": False if not is_v7 else None,
                    "extent_mode": "sliding-point-lookahead" if not is_v7 else None,
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES if not is_v7 else 0,
                    "frontier_mode": "direct" if not is_v7 else None,
                })
    return schedule


def build_v7_v8_ready_target_final_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str,
    binary_sha256: str | None = None,
) -> dict[str, object]:
    schedule = build_v7_v8_ready_target_final_schedule()
    return {
        "plan_schema_version": 1,
        "suite": "v7-v8-ready-target-final",
        "writers": list(V7_V8_READY_TARGET_FINAL_WRITERS),
        "workloads": [
            {"writer": writer, "name": workload, "bench": "wal_concurrent" if workload == "point"
             else "wal_batch_concurrent", "batch_size": 64 if workload == "batch64" else None,
             "num": V7_V8_READY_TARGET_FINAL_NUM, "latency_sample_every": 10 if workload == "point" else None}
            for writer, workload in V7_V8_READY_TARGET_FINAL_CASES
        ],
        "arms": list(V7_V8_READY_TARGET_FINAL_ARMS),
        "repetitions_per_cell": V7_V8_READY_TARGET_FINAL_REPETITIONS,
        "arm_orders": [list(order) for order in V7_V8_READY_TARGET_FINAL_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": int(machine_spec["vcpus"]),
        "local_ssd_count": int(machine_spec["ssd_count"]),
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": "single C4 Local SSD host; same source commit and release binary for all four arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "v8_configuration": {
            "progress_mode": "cursor (mode 2)",
            "completion": "direct",
            "frontier": "direct",
            "driver": "caller-driven",
            "ack": "legacy",
            "coalescing": "dynamic deadline with ready target unset, or explicit target 4/8",
            "extent_preparation": "1 MiB sliding-point-lookahead for Point; Batch64 remains fallocate-only",
            "singleton_submission": False,
            "point_buffer_preallocation": "enabled for all V8 Point arms only",
        },
        "v7_configuration": "V7 Parallel; no V8-only environment variables",
        "qualification_note": (
            "Point1, Point8, Point16, and Batch64/w16 each run five counterbalanced repetitions across V7 Parallel, "
            "V8 Dynamic, V8 Target4, and V8 Target8. Stage A is enabled for all V8 Point arms and disabled for "
            "Batch64. All arms use the same source commit and release binary; Point latency is sampled every 10 puts."
        ),
    }


def build_cursor_direct_completion_schedule(
    writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> list[dict[str, object]]:
    """Build same-binary Cursor relay/direct-completion score pairs."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_PROGRESS_RING_WRITERS:
        raise ValueError(
            f"Cursor direct-completion qualification requires writers {POINT_PROGRESS_RING_WRITERS}"
        )

    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS:
        for repetition in range(1, POINT_PROGRESS_RING_REPETITIONS + 1):
            order = CURSOR_DIRECT_COMPLETION_ORDERS[(repetition - 1) % len(CURSOR_DIRECT_COMPLETION_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_PROGRESS_RING_NUM,
                    "sample_every": sample_every,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": arm == "cursor_direct",
                })
    return schedule


def build_cursor_direct_completion_wake_schedule(
    writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> list[dict[str, object]]:
    """Build a same-binary Token/Select wake-mode score matrix."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_PROGRESS_RING_WRITERS:
        raise ValueError(
            f"Cursor direct-completion wake qualification requires writers {POINT_PROGRESS_RING_WRITERS}"
        )

    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS:
        for repetition in range(1, POINT_PROGRESS_RING_REPETITIONS + 1):
            order = CURSOR_DIRECT_COMPLETION_WAKE_ORDERS[(repetition - 1) % len(CURSOR_DIRECT_COMPLETION_WAKE_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_PROGRESS_RING_NUM,
                    "sample_every": sample_every,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_direct_completion_wake_mode": arm,
                    "point_buffer_preallocation_enabled": workload == "point",
                })
    return schedule


def build_cursor_singleton_submission_schedule(
    writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> list[dict[str, object]]:
    """Build same-binary Cursor singleton-submission score pairs."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_PROGRESS_RING_WRITERS:
        raise ValueError(
            f"Cursor singleton-submission qualification requires writers {POINT_PROGRESS_RING_WRITERS}"
        )

    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS:
        for repetition in range(1, POINT_PROGRESS_RING_REPETITIONS + 1):
            order = CURSOR_SINGLETON_SUBMISSION_ORDERS[(repetition - 1) % len(CURSOR_SINGLETON_SUBMISSION_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_PROGRESS_RING_NUM,
                    "sample_every": sample_every,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": False,
                    "cursor_singleton_submission_enabled": arm == "cursor_singleton",
                })
    return schedule


def build_cursor_publication_schedule(
    writers: Iterable[int] = CURSOR_PUBLICATION_WRITERS,
) -> list[dict[str, object]]:
    """Build the final 40-run D2a-only, same-binary cursor-publication matrix."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != CURSOR_PUBLICATION_WRITERS:
        raise ValueError(f"cursor-publication qualification requires writers {CURSOR_PUBLICATION_WRITERS}")

    switches = {"baseline": False, "admission_fast_path": True}
    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS:
        for repetition in range(1, CURSOR_PUBLICATION_REPETITIONS + 1):
            order = CURSOR_PUBLICATION_ORDERS[(repetition - 1) % len(CURSOR_PUBLICATION_ORDERS)]
            for arm in order:
                admission_fast_path = switches[arm]
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": CURSOR_PUBLICATION_NUM,
                    "sample_every": sample_every,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_singleton_submission_enabled": False,
                    "point_buffer_preallocation_enabled": workload == "point",
                    "point_resource_retirement_mode": 0,
                    "admission_cursor_publish_fast_path_enabled": admission_fast_path,
                })
    return schedule


def build_cursor_publication_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    """Describe the isolated D2a requalification with Stage A and retirement fixed."""
    writer_counts = validate_writer_counts(writers)
    schedule = build_cursor_publication_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    return {
        "plan_schema_version": CURSOR_PUBLICATION_PLAN_SCHEMA_V2,
        "suite": "v8-cursor-publication",
        "writers": list(writer_counts),
        "workloads": [
            {"writer": writer, "name": workload, "bench": bench,
             "batch_entries": entries, "batch_size": batch_size,
             "num": CURSOR_PUBLICATION_NUM, "sample_every": sample_every}
            for writer, workload, bench, entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS
        ],
        "arms": list(CURSOR_PUBLICATION_ARMS),
        "repetitions_per_cell": CURSOR_PUBLICATION_REPETITIONS,
        "arm_orders": [list(order) for order in CURSOR_PUBLICATION_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same source and release binary for all arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "progress_mode": "cursor (mode 2)",
        "direct_completion_enabled": True,
        "singleton_submission_enabled": False,
        "point_buffer_preallocation_enabled": True,
        "point_resource_retirement_mode": 0,
        "extent_preparation": "sliding-point-lookahead, 1 MiB for Point; Batch64 remains fallocate-only",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "data_submit_order": "ordered",
        "extent_policy_storage": "vec",
        "extent_policy_scan_diagnostics_enabled": False,
        "latency_sample_every": 10,
        "d2a": {
            "admission_fast_path_env": "TOYKV_BENCH_V8_ADMISSION_CURSOR_PUBLISH_FAST_PATH",
            "runtime_proof_field": "admission_cursor_publish_fast_path_enabled",
            "default": False,
            "stage_a_preallocation": "enabled for Point, disabled for Batch64",
            "retirement_mode": 0,
            "d1": False,
            "d2b": "removed",
        },
        "qualification_note": (
            "Two-arm same-host, same-source, same-release-binary D2a-only comparison. Cursor, Direct Completion, "
            "Direct FRONTIER, dynamic coalescing, 1 MiB Point lookahead, Stage A Point preallocation, retirement "
            "mode 0, and D1 disabled are fixed. D2b was removed. Point1/8/16 and Batch64 each have five paired "
            "repetitions; latency is sampled every 10 Point puts."
        ),
    }


def build_point_resource_retirement_schedule(
    writers: Iterable[int] = POINT_RESOURCE_RETIREMENT_WRITERS,
    *,
    point_buffer_preallocation_enabled: bool = False,
) -> list[dict[str, object]]:
    """Build the same-binary Cursor Direct comparison for retirement modes 0/1/2."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_RESOURCE_RETIREMENT_WRITERS:
        raise ValueError(
            f"Point resource-retirement qualification requires writers {POINT_RESOURCE_RETIREMENT_WRITERS}"
        )

    cells = [
        (writer, "point", "wal_concurrent", 1, None, 10)
        for writer in writer_counts
    ]
    cells.append((16, "batch64", "wal_batch_concurrent", 64, 64, None))
    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in cells:
        for repetition in range(1, POINT_RESOURCE_RETIREMENT_REPETITIONS + 1):
            order = POINT_RESOURCE_RETIREMENT_ORDERS[(repetition - 1) % len(POINT_RESOURCE_RETIREMENT_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_RESOURCE_RETIREMENT_NUM,
                    "sample_every": sample_every,
                    "point_resource_retirement_mode": POINT_RESOURCE_RETIREMENT_MODES[arm],
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_singleton_submission_enabled": False,
                    "point_buffer_preallocation_enabled": point_buffer_preallocation_enabled and workload == "point",
                    "scan_diagnostics_enabled": False,
                })
    return schedule


def build_point_resource_retirement_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str,
    binary_sha256: str | None = None,
    point_buffer_preallocation_enabled: bool = False,
) -> dict[str, object]:
    """Describe the strict C1/C2 score matrix and its runtime mode proof."""
    schedule = build_point_resource_retirement_schedule(
        point_buffer_preallocation_enabled=point_buffer_preallocation_enabled,
    )
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "plan_schema_version": 1,
        "suite": "v8-point-resource-retirement",
        "writers": list(POINT_RESOURCE_RETIREMENT_WRITERS),
        "workloads": [
            {"writer": writer, "name": workload, "bench": bench,
             "batch_entries": entries, "batch_size": batch_size,
             "num": POINT_RESOURCE_RETIREMENT_NUM, "sample_every": sample_every}
            for writer, workload, bench, entries, batch_size, sample_every in (
                *[(writer, "point", "wal_concurrent", 1, None, 10)
                  for writer in POINT_RESOURCE_RETIREMENT_WRITERS],
                (16, "batch64", "wal_batch_concurrent", 64, 64, None),
            )
        ],
        "arms": list(POINT_RESOURCE_RETIREMENT_ARMS),
        "mode_values": dict(POINT_RESOURCE_RETIREMENT_MODES),
        "repetitions_per_cell": POINT_RESOURCE_RETIREMENT_REPETITIONS,
        "arm_orders": [list(order) for order in POINT_RESOURCE_RETIREMENT_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same source and binary for all arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "progress_mode": "cursor (mode 2)",
        "direct_completion_enabled": True,
        "point_buffer_preallocation_enabled": point_buffer_preallocation_enabled,
        "point_frontier_credit_batch_enabled": False,
        "singleton_submission_enabled": False,
        "extent_preparation": "sliding-point-lookahead, 1 MiB; Point-only policy keeps Batch64 on fallocate",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "extent_policy_storage": "vec",
        "data_submit_order": "ordered",
        "latency_sample_every": 10,
        "resource_retirement_ab": {
            "environment_variable": "TOYKV_BENCH_V8_POINT_RESOURCE_RETIREMENT_MODE",
            "values": {"per_ticket": "0", "detach": "1", "batch": "2"},
            "runtime_proof_fields": [
                "point_resource_retirement_mode",
                "point_resource_retirement_detached_prefixes",
                "point_resource_retirement_batched_prefixes",
            ],
        },
        "qualification_note": (
            "Same-host, same-source, same-release-binary comparison of per-ticket retirement (mode 0), "
            "C1 detached Point prefixes (mode 1), and C2 batch retirement (mode 2). All arms use Cursor, "
            "Direct Completion, Direct FRONTIER, dynamic coalescing, 1 MiB Point lookahead, "
            + (
                "Point buffer preallocation for Point runs, and no legacy credit batching. "
                if point_buffer_preallocation_enabled else
                "and no Point buffer preallocation or legacy credit batching. "
            )
            + "Batch64 is a no-detach regression guardrail. "
            "Scored runs have no detailed profiling, and mode 2 must report successful batch-prefix counts."
        ),
    }
    return plan


def build_point_ack_d1_schedule() -> list[dict[str, object]]:
    """Build paired pre-D1/D1 runs with Stage A and retirement mode 0 fixed."""
    cells = [
        (writer, "point", "wal_concurrent", 1, None, 10)
        for writer in POINT_ACK_D1_WRITERS
    ]
    cells.append((16, "batch64", "wal_batch_concurrent", 64, 64, None))
    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in cells:
        for repetition in range(1, POINT_ACK_D1_REPETITIONS + 1):
            order = POINT_ACK_D1_ORDERS[(repetition - 1) % len(POINT_ACK_D1_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_ACK_D1_NUM,
                    "sample_every": sample_every,
                    "point_buffer_preallocation_enabled": workload == "point",
                    "point_resource_retirement_mode": 0,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_singleton_submission_enabled": False,
                    "scan_diagnostics_enabled": False,
                })
    return schedule


def build_point_ack_d1_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], baseline_commit: str,
    candidate_commit: str, source_patch_sha256: str,
    baseline_binary_sha256: str | None = None, candidate_binary_sha256: str | None = None,
) -> dict[str, object]:
    """Describe the D1 source A/B while holding Stage A and retirement fixed."""
    schedule = build_point_ack_d1_schedule()
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "plan_schema_version": 1,
        "suite": "v8-point-ack-d1",
        "writers": list(POINT_ACK_D1_WRITERS),
        "workloads": [
            {"writer": writer, "name": "point", "bench": "wal_concurrent", "batch_size": None,
             "num": POINT_ACK_D1_NUM, "latency_sample_every": 10}
            for writer in POINT_ACK_D1_WRITERS
        ] + [
            {"writer": 16, "name": "batch64", "bench": "wal_batch_concurrent", "batch_size": 64,
             "num": POINT_ACK_D1_NUM, "latency_sample_every": None}
        ],
        "arms": list(POINT_ACK_D1_ARMS),
        "repetitions_per_cell": POINT_ACK_D1_REPETITIONS,
        "arm_orders": [list(order) for order in POINT_ACK_D1_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; both revisions built with the pinned toolchain",
        "value_bytes": 1024,
        "compaction": "none",
        "baseline_commit": baseline_commit,
        "candidate_commit": candidate_commit,
        "source_patch_sha256": source_patch_sha256,
        "progress_mode": "cursor (mode 2)",
        "direct_completion_enabled": True,
        "point_buffer_preallocation_enabled": True,
        "point_resource_retirement_mode": 0,
        "extent_preparation": "sliding-point-lookahead, 1 MiB",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "singleton_submission_enabled": False,
        "v8_configuration": {
            "data_submission": "ordered",
            "point_preallocation": "Stage A enabled",
            "retirement_mode": 0,
        },
        "qualification_note": (
            "The only source difference is D1's consolidated Coordinator ACK checks. Both builds use the same "
            "toolchain and fixed Cursor + Direct Completion + Direct FRONTIER + Point lookahead configuration; "
            "Stage A is enabled and retirement mode 0 is fixed. Batch64 is a regression guardrail."
        ),
    }
    if baseline_binary_sha256 is not None:
        plan["baseline_binary_sha256"] = baseline_binary_sha256
    if candidate_binary_sha256 is not None:
        plan["candidate_binary_sha256"] = candidate_binary_sha256
    return plan


def build_dedicated_durability_driver_schedule() -> list[dict[str, object]]:
    """Build 60 uninstrumented D3b ACK-mode score runs."""
    cells = [
        (writer, "point", "wal_concurrent", 1, None, 10)
        for writer in DURABILITY_DRIVER_WRITERS
    ]
    cells.append((16, "batch64", "wal_batch_concurrent", 64, 64, None))
    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in cells:
        for repetition in range(1, DURABILITY_DRIVER_REPETITIONS + 1):
            for arm in DURABILITY_DRIVER_SCORE_ORDERS[(repetition - 1) % len(DURABILITY_DRIVER_SCORE_ORDERS)]:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "phase": "score",
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": DURABILITY_DRIVER_NUM,
                    "sample_every": sample_every,
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_singleton_submission_enabled": False,
                    "point_buffer_preallocation_enabled": workload == "point",
                    "point_resource_retirement_mode": 0,
                    "durability_driver_enabled": arm == "dedicated_independent_ack",
                    "independent_ack_waiter_enabled": arm != "caller_legacy_ack",
                    "durability_driver_diagnostics_enabled": False,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                })
    return schedule


def build_dedicated_durability_driver_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None, runner_bundle_sha256: str | None = None,
) -> dict[str, object]:
    writer_counts = validate_writer_counts(writers)
    if writer_counts != DURABILITY_DRIVER_WRITERS:
        raise ValueError(f"D3b ACK qualification requires writers {DURABILITY_DRIVER_WRITERS}")
    schedule = build_dedicated_durability_driver_schedule()
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "plan_schema_version": 3,
        "suite": "v8-d3b-independent-ack",
        "writers": list(writer_counts),
        "workloads": [
            {"writer": writer, "name": "point", "bench": "wal_concurrent", "batch_size": None,
             "num": DURABILITY_DRIVER_NUM, "latency_sample_every": 10}
            for writer in writer_counts
        ] + [
            {"writer": 16, "name": "batch64", "bench": "wal_batch_concurrent", "batch_size": 64,
             "num": DURABILITY_DRIVER_NUM, "latency_sample_every": None}
        ],
        "arms": list(DURABILITY_DRIVER_ARMS),
        "repetitions_per_cell": DURABILITY_DRIVER_REPETITIONS,
        "arm_orders": [list(order) for order in DURABILITY_DRIVER_SCORE_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": 60,
        "diagnostic_runs_planned": 0,
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same source and release binary for all arms",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "value_bytes": 1024,
        "compaction": "none",
        "progress_mode": "cursor (mode 2)",
        "direct_completion_enabled": True,
        "singleton_submission_enabled": False,
        "point_buffer_preallocation_enabled": True,
        "point_resource_retirement_mode": 0,
        "ack_modes": {
            "caller_legacy_ack": {"durability_driver": False, "independent_ack_waiter": False},
            "caller_independent_ack": {"durability_driver": False, "independent_ack_waiter": True},
            "dedicated_independent_ack": {"durability_driver": True, "independent_ack_waiter": True},
        },
        "extent_preparation": "sliding-point-lookahead, 1 MiB for Point; Batch64 remains fallocate-only",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "data_submit_order": "ordered",
        "instrumentation": {
            "score_runs": "all detailed profiling and driver diagnostics disabled",
            "diagnostic_runs": "none",
        },
        "runner_bundle_sha256": runner_bundle_sha256,
        "runner_bundle_verification": "remote-component-bytes-v1",
        "qualification_note": (
            "Same-host, same-source, same-release-binary comparison of CallerDriven + legacy ACK, "
            "CallerDriven + independent ACK, and Dedicated Driver + independent ACK. Stage A is enabled "
            "for Point, retirement mode 0 is fixed, and D1/D2/C1/C2 remain disabled. All 60 runs are "
            "unprofiled: five counterbalanced repetitions for Point1/8/16 and Batch64."
        ),
    }
    if binary_sha256 is not None:
        plan["binary_sha256"] = binary_sha256
    return plan


def validate_dedicated_durability_driver_rows(rows: list[Mapping[str, object]]) -> dict[str, object]:
    schedule = build_dedicated_durability_driver_schedule()
    fields = ("phase", "writer", "workload", "repetition", "arm")
    expected = {tuple(row[field] for field in fields): row for row in schedule}
    observed: Counter[tuple[object, ...]] = Counter()
    failed: list[dict[str, object]] = []
    invalid: list[dict[str, object]] = []
    for row in rows:
        try:
            key = (str(row["phase"]), int(row["writer"]), str(row["workload"]), int(row["repetition"]), str(row["arm"]))
        except (KeyError, TypeError, ValueError):
            invalid.append({"row": dict(row)})
            continue
        observed[key] += 1
        if key not in expected or row.get("exit_code") != 0 or row.get("ops") != DURABILITY_DRIVER_NUM:
            failed.append({"cell": list(key), "exit_code": row.get("exit_code"), "ops": row.get("ops")})
    expected_cells = set(expected)
    observed_cells = set(observed)
    missing = sorted(expected_cells - observed_cells, key=repr)
    unexpected = sorted(observed_cells - expected_cells, key=repr)
    duplicates = sorted((cell for cell, count in observed.items() if count > 1), key=repr)
    return {
        "complete": not (missing or unexpected or duplicates or failed or invalid),
        "planned": len(expected),
        "observed": sum(observed.values()) + len(invalid),
        "missing_cells": [list(cell) for cell in missing],
        "unexpected_cells": [list(cell) for cell in unexpected],
        "duplicate_cells": [list(cell) for cell in duplicates],
        "failed_cells": failed,
        "invalid_rows": len(invalid),
    }


def validate_point_ack_d1_rows(rows: list[Mapping[str, object]]) -> dict[str, object]:
    """Validate the D1 pair matrix and prove Stage A/mode 0 were exercised."""
    schedule = build_point_ack_d1_schedule()
    expected = {
        (row["writer"], row["workload"], row["repetition"], row["arm"]): row
        for row in schedule
    }
    observed: dict[tuple[object, object, object, object], int] = {}
    invalid: list[Mapping[str, object]] = []
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "repetition", "arm"))
        observed[identity] = observed.get(identity, 0) + 1
        planned = expected.get(identity)
        counter_names = (
            "point_buffer_preallocation_attempts",
            "point_buffer_preallocation_hits",
            "point_buffer_preallocation_fallbacks",
            "point_resource_retirement_detached_prefixes",
            "point_resource_retirement_detached_tickets",
            "point_resource_retirement_batched_prefixes",
        )
        counters_valid = all(type(row.get(name)) is int and row[name] >= 0 for name in counter_names)
        point_preallocation_valid = (
            counters_valid
            and row.get("point_buffer_preallocation_attempts")
            == row.get("point_buffer_preallocation_hits") + row.get("point_buffer_preallocation_fallbacks")
            and (row.get("point_buffer_preallocation_hits", 0) > 0 if planned and planned["workload"] == "point" else True)
        )
        batch_preallocation_disabled = counters_valid and all(
            row.get(name) == 0 for name in counter_names[:3]
        )
        if (
            planned is None
            or row.get("exit_code") != 0
            or row.get("ops") != POINT_ACK_D1_NUM
            or row.get("point_resource_retirement_mode") != 0
            or row.get("point_buffer_preallocation_enabled") is not (planned["workload"] == "point")
            or not point_preallocation_valid
            or (planned["workload"] == "batch64" and not batch_preallocation_disabled)
            or any(type(row.get(key)) is not int or row[key] != 0 for key in (
                "point_resource_retirement_detached_prefixes",
                "point_resource_retirement_detached_tickets",
                "point_resource_retirement_batched_prefixes",
            ))
        ):
            invalid.append(row)
    expected_cells = set(expected)
    observed_cells = set(observed)
    return {
        "complete": not (
            expected_cells != observed_cells or any(count != 1 for count in observed.values()) or invalid
        ),
        "planned": len(schedule),
        "observed": len(rows),
        "missing_cells": [list(cell) for cell in sorted(expected_cells - observed_cells, key=str)],
        "unexpected_cells": [list(cell) for cell in sorted(observed_cells - expected_cells, key=str)],
        "duplicate_cells": [list(cell) for cell, count in observed.items() if count != 1],
        "invalid_rows": [dict(row) for row in invalid],
    }


def build_cursor_coalescing_budget_schedule(
    writers: Iterable[int] = CURSOR_COALESCING_BUDGET_WRITERS,
) -> list[dict[str, object]]:
    """Build a same-binary Dynamic/100/400 us Cursor Direct Point score sweep."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != CURSOR_COALESCING_BUDGET_WRITERS:
        raise ValueError(
            f"Cursor coalescing-budget qualification requires writers {CURSOR_COALESCING_BUDGET_WRITERS}"
        )

    schedule: list[dict[str, object]] = []
    for repetition in range(1, CURSOR_COALESCING_BUDGET_REPETITIONS + 1):
        writer_order = CURSOR_COALESCING_BUDGET_WRITERS if repetition % 2 else tuple(
            reversed(CURSOR_COALESCING_BUDGET_WRITERS)
        )
        for writer_index, writer in enumerate(writer_order):
            rotation = (repetition + writer_index - 1) % len(CURSOR_COALESCING_BUDGET_ORDERS)
            order = CURSOR_COALESCING_BUDGET_ORDERS[rotation]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": "point",
                    "repetition": repetition,
                    "arm": arm,
                    "bench": "wal_concurrent",
                    "batch_entries": 1,
                    "batch_size": None,
                    "num": CURSOR_COALESCING_BUDGET_NUM,
                    "sample_every": 10,
                    "coalescing_budget_us": {
                        "dynamic": None,
                        "fixed_100": 100,
                        "fixed_400": 400,
                    }[arm],
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_singleton_submission_enabled": False,
                    "scan_diagnostics_enabled": False,
                })
    return schedule


def build_cursor_coalescing_budget_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str,
) -> dict[str, object]:
    return {
        "plan_schema_version": 1,
        "suite": "v8-cursor-coalescing-budget",
        "commit": commit,
        "machine_type": machine_type,
        "machine_spec": dict(machine_spec),
        "writers": list(CURSOR_COALESCING_BUDGET_WRITERS),
        "workloads": ["point"],
        "arms": list(CURSOR_COALESCING_BUDGET_ARMS),
        "repetitions": CURSOR_COALESCING_BUDGET_REPETITIONS,
        "runs_planned": len(build_cursor_coalescing_budget_schedule()),
        "num": CURSOR_COALESCING_BUDGET_NUM,
        "value_size_bytes": 1024,
        "latency_sample_every": 10,
        "runtime_protocol": {
            "progress_mode": "cursor (2)",
            "completion": "direct",
            "singleton_submission": False,
            "frontier_mode": "direct",
            "extent_preparation": "1 MiB sliding-point-lookahead",
            "extent_policy_scan_diagnostics": False,
            "coalescing_budget_us": {
                "dynamic": "unset; preserve runtime adaptive policy",
                "fixed_100": 100,
                "fixed_400": 400,
            },
        },
        "qualification_note": (
            "Same-host, same-source, same-release-binary Cursor Direct Point comparison. Only the V8 coalescing "
            "budget differs; no phase profiling is enabled. Five counterbalanced repetitions at 8 and 16 writers."
        ),
    }


def validate_cursor_coalescing_budget_rows(
    rows: list[Mapping[str, object]],
) -> dict[str, object]:
    schedule = build_cursor_coalescing_budget_schedule()
    validation = _validate_point_progress_rows(rows, schedule)
    invalid_rows: list[Mapping[str, object]] = []
    for row in rows:
        if (
            row.get("progress_cursor_enabled") is not True
            or row.get("progress_counters_enabled") is not False
            or row.get("cursor_direct_completion_enabled") is not True
            or row.get("cursor_singleton_submission_enabled") is not False
            or row.get("coalescing_budget_us") != next(
                (expected["coalescing_budget_us"] for expected in schedule
                 if expected["run_index"] == row.get("run_index")),
                object(),
            )
        ):
            invalid_rows.append(row)
    if invalid_rows:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid_rows)
    return validation


def build_cursor_coalescing_ready_target_schedule() -> list[dict[str, object]]:
    """Build the 60-run Dynamic/Target8/Target12 E3 qualification matrix."""
    schedule: list[dict[str, object]] = []
    cases = CURSOR_COALESCING_READY_TARGET_CASES
    for repetition in range(1, CURSOR_COALESCING_READY_TARGET_REPETITIONS + 1):
        ordered_cases = cases if repetition % 2 else tuple(reversed(cases))
        for case_index, (writer, workload) in enumerate(ordered_cases):
            rotation = (repetition + case_index - 1) % len(CURSOR_COALESCING_READY_TARGET_ORDERS)
            for arm in CURSOR_COALESCING_READY_TARGET_ORDERS[rotation]:
                batch = workload == "batch64"
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": "wal_concurrent" if workload == "point" else "wal_batch_concurrent",
                    "batch_entries": 1 if workload == "point" else 64,
                    "batch_size": None if workload == "point" else 64,
                    "num": CURSOR_COALESCING_READY_TARGET_NUM,
                    "sample_every": 10,
                    "coalescing_ready_target": {
                        "dynamic": None,
                        "target_8": 8,
                        "target_12": 12,
                    }[arm],
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_singleton_submission_enabled": False,
                    "point_buffer_preallocation_enabled": not batch,
                    "scan_diagnostics_enabled": False,
                })
    return schedule


def build_cursor_coalescing_ready_target_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str,
) -> dict[str, object]:
    schedule = build_cursor_coalescing_ready_target_schedule()
    return {
        "plan_schema_version": 1,
        "suite": "v8-cursor-coalescing-ready-target",
        "commit": commit,
        "machine_type": machine_type,
        "machine_spec": dict(machine_spec),
        "writers": list(CURSOR_COALESCING_READY_TARGET_WRITERS),
        "workloads": ["point1", "point8", "point16", "batch64-w16"],
        "arms": list(CURSOR_COALESCING_READY_TARGET_ARMS),
        "repetitions": CURSOR_COALESCING_READY_TARGET_REPETITIONS,
        "runs_planned": len(schedule),
        "num": CURSOR_COALESCING_READY_TARGET_NUM,
        "value_size_bytes": 1024,
        "latency_sample_every": 10,
        "runtime_protocol": {
            "progress_mode": "cursor (2)",
            "completion": "direct",
            "singleton_submission": False,
            "frontier_mode": "direct",
            "extent_preparation": "1 MiB sliding-point-lookahead",
            "extent_policy_scan_diagnostics": False,
            "point_buffer_preallocation": "on for Point; off for Batch64",
            "coalescing_ready_target": {
                "dynamic": "unset; preserve runtime adaptive policy",
                "target_8": 8,
                "target_12": 12,
            },
        },
        "qualification_note": (
            "Same-host, same-source, same-release-binary E3 comparison across Point1/8/16 and Batch64/16. "
            "Five counterbalanced repetitions compare Dynamic, ready-prefix target 8, and target 12. "
            "Scoring runs have no detailed profiling; runtime target-hit counters prove configured behavior."
        ),
    }


def validate_cursor_coalescing_ready_target_rows(
    rows: list[Mapping[str, object]],
) -> dict[str, object]:
    schedule = build_cursor_coalescing_ready_target_schedule()
    validation = _validate_point_progress_rows(rows, schedule)
    expected_by_index = {row["run_index"]: row for row in schedule}
    invalid_rows: list[Mapping[str, object]] = []
    for row in rows:
        expected = expected_by_index.get(row.get("run_index"))
        if (
            expected is None
            or row.get("progress_cursor_enabled") is not True
            or row.get("progress_counters_enabled") is not False
            or row.get("cursor_direct_completion_enabled") is not True
            or row.get("cursor_singleton_submission_enabled") is not False
            or row.get("coalescing_ready_target") != expected["coalescing_ready_target"]
            or row.get("point_buffer_preallocation_enabled") is not expected["point_buffer_preallocation_enabled"]
        ):
            invalid_rows.append(row)
    if invalid_rows:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid_rows)
    return validation


def build_cursor_coalescing_ready_target_scan_schedule() -> list[dict[str, object]]:
    """Build the 40-run Point8/16 Dynamic/Target4/6/8 E3b scan."""
    schedule: list[dict[str, object]] = []
    for repetition in range(1, CURSOR_COALESCING_READY_TARGET_SCAN_REPETITIONS + 1):
        cases = (
            CURSOR_COALESCING_READY_TARGET_SCAN_CASES
            if repetition % 2
            else tuple(reversed(CURSOR_COALESCING_READY_TARGET_SCAN_CASES))
        )
        for writer, workload in cases:
            case_index = CURSOR_COALESCING_READY_TARGET_SCAN_CASES.index((writer, workload))
            order = CURSOR_COALESCING_READY_TARGET_SCAN_ORDERS[
                (repetition - 1 + case_index) % len(CURSOR_COALESCING_READY_TARGET_SCAN_ORDERS)
            ]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": "wal_concurrent",
                    "batch_entries": 1,
                    "batch_size": None,
                    "num": CURSOR_COALESCING_READY_TARGET_SCAN_NUM,
                    "sample_every": 10,
                    "coalescing_ready_target": {
                        "dynamic": None,
                        "target_4": 4,
                        "target_6": 6,
                        "target_8": 8,
                    }[arm],
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_singleton_submission_enabled": False,
                    "point_buffer_preallocation_enabled": True,
                    "scan_diagnostics_enabled": False,
                })
    return schedule


def build_cursor_coalescing_ready_target_scan_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str,
) -> dict[str, object]:
    schedule = build_cursor_coalescing_ready_target_scan_schedule()
    return {
        "plan_schema_version": 1,
        "suite": "v8-cursor-coalescing-ready-target-scan",
        "commit": commit,
        "machine_type": machine_type,
        "machine_spec": dict(machine_spec),
        "writers": list(CURSOR_COALESCING_READY_TARGET_SCAN_WRITERS),
        "workloads": ["point8", "point16"],
        "arms": list(CURSOR_COALESCING_READY_TARGET_SCAN_ARMS),
        "repetitions": CURSOR_COALESCING_READY_TARGET_SCAN_REPETITIONS,
        "runs_planned": len(schedule),
        "num": CURSOR_COALESCING_READY_TARGET_SCAN_NUM,
        "value_size_bytes": 1024,
        "latency_sample_every": 10,
        "runtime_protocol": {
            "progress_mode": "cursor (2)",
            "completion": "direct",
            "singleton_submission": False,
            "frontier_mode": "direct",
            "extent_preparation": "1 MiB sliding-point-lookahead",
            "extent_policy_scan_diagnostics": False,
            "point_buffer_preallocation": "on",
            "coalescing_ready_target": {
                "dynamic": "unset; preserve runtime adaptive policy",
                "target_4": 4,
                "target_6": 6,
                "target_8": 8,
            },
        },
        "qualification_note": (
            "Same-host, same-source, same-release-binary E3b Point8/Point16 parameter scan. Five counterbalanced "
            "repetitions compare Dynamic, ready-prefix targets 4, 6, and 8. Scoring runs have no detailed profiling; "
            "runtime target-hit counters prove configured behavior. This is an exploratory scan, not final qualification."
        ),
    }


def validate_cursor_coalescing_ready_target_scan_rows(
    rows: list[Mapping[str, object]],
) -> dict[str, object]:
    schedule = build_cursor_coalescing_ready_target_scan_schedule()
    validation = _validate_point_progress_rows(rows, schedule)
    expected_by_index = {row["run_index"]: row for row in schedule}
    invalid_rows: list[Mapping[str, object]] = []
    for row in rows:
        expected = expected_by_index.get(row.get("run_index"))
        if (
            expected is None
            or row.get("progress_cursor_enabled") is not True
            or row.get("progress_counters_enabled") is not False
            or row.get("cursor_direct_completion_enabled") is not True
            or row.get("cursor_singleton_submission_enabled") is not False
            or row.get("coalescing_ready_target") != expected["coalescing_ready_target"]
            or row.get("point_buffer_preallocation_enabled") is not True
        ):
            invalid_rows.append(row)
    if invalid_rows:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid_rows)
    return validation


def build_cursor_coalescing_ready_target_small_scan_schedule() -> list[dict[str, object]]:
    """Build a 50-run Point8/16 Dynamic/Target1–4 parameter scan."""
    schedule: list[dict[str, object]] = []
    cases = CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_CASES
    for repetition in range(1, CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_REPETITIONS + 1):
        ordered_cases = cases if repetition % 2 else tuple(reversed(cases))
        for writer, workload in ordered_cases:
            case_index = cases.index((writer, workload))
            order_index = (repetition - 1 + case_index) % len(
                CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_ORDERS
            )
            for arm in CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_ORDERS[order_index]:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": "wal_concurrent",
                    "batch_entries": 1,
                    "batch_size": None,
                    "num": CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_NUM,
                    "sample_every": 10,
                    "coalescing_ready_target": {
                        "dynamic": None,
                        "target_1": 1,
                        "target_2": 2,
                        "target_3": 3,
                        "target_4": 4,
                    }[arm],
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "progress_ring_enabled": True,
                    "progress_cursor_enabled": True,
                    "progress_counters_enabled": False,
                    "cursor_direct_completion_enabled": True,
                    "cursor_singleton_submission_enabled": False,
                    "point_buffer_preallocation_enabled": True,
                    "scan_diagnostics_enabled": False,
                })
    return schedule


def build_cursor_coalescing_ready_target_small_scan_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str,
) -> dict[str, object]:
    schedule = build_cursor_coalescing_ready_target_small_scan_schedule()
    return {
        "plan_schema_version": 1,
        "suite": "v8-cursor-coalescing-ready-target-small-scan",
        "commit": commit,
        "machine_type": machine_type,
        "machine_spec": dict(machine_spec),
        "writers": list(CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_WRITERS),
        "workloads": ["point8", "point16"],
        "arms": list(CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_ARMS),
        "repetitions": CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_REPETITIONS,
        "runs_planned": len(schedule),
        "num": CURSOR_COALESCING_READY_TARGET_SMALL_SCAN_NUM,
        "value_size_bytes": 1024,
        "latency_sample_every": 10,
        "runtime_protocol": {
            "progress_mode": "cursor (2)",
            "completion": "direct",
            "singleton_submission": False,
            "frontier_mode": "direct",
            "extent_preparation": "1 MiB sliding-point-lookahead",
            "extent_policy_scan_diagnostics": False,
            "point_buffer_preallocation": "on",
            "coalescing_ready_target": {
                "dynamic": "unset; preserve runtime adaptive policy",
                "target_1": 1,
                "target_2": 2,
                "target_3": 3,
                "target_4": 4,
            },
        },
        "qualification_note": (
            "Same-host, same-source, same-release-binary exploratory Point8/Point16 scan. Five counterbalanced "
            "repetitions compare Dynamic and ready-prefix targets 1–4 without profiling. Target1 exits once the "
            "current ready prefix is nonempty and still submits the entire ready prefix; it does not force "
            "single-ticket groups. Runtime target-hit counters prove the selected arm."
        ),
    }


def validate_cursor_coalescing_ready_target_small_scan_rows(
    rows: list[Mapping[str, object]],
) -> dict[str, object]:
    schedule = build_cursor_coalescing_ready_target_small_scan_schedule()
    validation = _validate_point_progress_rows(rows, schedule)
    expected_by_index = {row["run_index"]: row for row in schedule}
    invalid_rows: list[Mapping[str, object]] = []
    for row in rows:
        expected = expected_by_index.get(row.get("run_index"))
        if (
            expected is None
            or row.get("progress_cursor_enabled") is not True
            or row.get("progress_counters_enabled") is not False
            or row.get("cursor_direct_completion_enabled") is not True
            or row.get("cursor_singleton_submission_enabled") is not False
            or row.get("coalescing_ready_target") != expected["coalescing_ready_target"]
            or row.get("point_buffer_preallocation_enabled") is not True
        ):
            invalid_rows.append(row)
    if invalid_rows:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid_rows)
    return validation


def build_point_progress_cursor_v2_schedule(writers: Iterable[int]) -> list[dict[str, object]]:
    """Build the historical Eager/Ring/Cursor schedule for archived V2 plans."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != POINT_PROGRESS_RING_WRITERS:
        raise ValueError(f"historical progress-cursor qualification requires writers {POINT_PROGRESS_RING_WRITERS}")

    schedule: list[dict[str, object]] = []
    for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS:
        for repetition in range(1, POINT_PROGRESS_RING_REPETITIONS + 1):
            order = POINT_PROGRESS_CURSOR_V2_ORDERS[(repetition - 1) % len(POINT_PROGRESS_CURSOR_V2_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload,
                    "repetition": repetition,
                    "arm": arm,
                    "bench": bench,
                    "batch_entries": batch_entries,
                    "batch_size": batch_size,
                    "num": POINT_PROGRESS_RING_NUM,
                    "sample_every": sample_every,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                    "progress_ring_enabled": arm != "eager",
                    "progress_cursor_enabled": arm == "cursor",
                    "progress_counters_enabled": False,
                })
    return schedule


def build_point_progress_ring_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    writer_counts = validate_writer_counts(writers)
    schedule = build_point_progress_ring_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "plan_schema_version": POINT_PROGRESS_PLAN_SCHEMA_V1,
        "suite": "v8-point-progress-ring",
        "writers": list(writer_counts),
        "workloads": [
            {"writer": writer, "name": workload, "bench": bench,
             "batch_entries": batch_entries, "batch_size": batch_size,
             "num": POINT_PROGRESS_RING_NUM, "sample_every": sample_every}
            for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS
        ],
        "arms": list(POINT_PROGRESS_RING_ARMS),
        "repetitions_per_cell": POINT_PROGRESS_RING_REPETITIONS,
        "arm_orders": [list(order) for order in POINT_PROGRESS_RING_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same source and binary for both arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "extent_preparation": "sliding-point-lookahead, 1 MiB for both arms; Point-only policy keeps Batch64 on fallocate",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "point_direct_frame_enabled": False,
        "point_frontier_credit_batch_enabled": False,
        "extent_policy_storage": "vec",
        "extent_policy_scan_diagnostics_enabled": False,
        "data_submit_order": "ordered",
        "latency_sample_every": 10,
        "hash_cpu_sample_every": 64,
        "worker_group_histogram": "disabled for score runs; opt-in diagnostics only",
        "progress_ring_ab": {
            "environment_variable": "TOYKV_BENCH_V8_POINT_PROGRESS_RING",
            "baseline_value": "0",
            "candidate_value": "1",
        },
        "local_screen": {
            "status": "skipped_by_user",
            "reason": "Local SSD results were excluded at the user's direction because the local SSD is unstable.",
        },
        "qualification_note": (
            "Only the per-ticket progress-ring switch changes between arms. Both use the same release bench binary, "
            "Direct FRONTIER, dynamic coalescing, 1 MiB Point lookahead, and existing digest sampling. "
            "Point latency is sampled every 10 completed puts; Batch64 is throughput-only."
        ),
    }
    if binary_sha256 is not None:
        plan["binary_sha256"] = binary_sha256
    return plan


def build_point_progress_cursor_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    """Build a Cursor-specific plan without changing the historical F0.5 plan schema."""
    writer_counts = validate_writer_counts(writers)
    schedule = build_point_progress_cursor_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan = build_point_progress_ring_plan(
        writers=writer_counts,
        machine_type=machine_type,
        machine_spec=machine_spec,
        commit=commit,
        binary_sha256=binary_sha256,
    )
    plan.pop("progress_ring_ab")
    plan.update({
        "plan_schema_version": POINT_PROGRESS_PLAN_SCHEMA_V3,
        "suite": "v8-point-progress-cursor",
        "arms": list(POINT_PROGRESS_CURSOR_ARMS),
        "arm_orders": [list(order) for order in POINT_PROGRESS_CURSOR_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "runtime_protocol": (
            f"single {vcpus}-vCPU host; {layout}; same source and binary for two arms"
        ),
        "progress_cursor_ab": {
            "environment_variable": "TOYKV_BENCH_V8_POINT_PROGRESS_RING",
            "baseline_value": "0",
            "candidate_value": "2",
        },
        "progress_counters_enabled": False,
        "qualification_note": (
            "Same-host, same-source, same-release-binary two-arm qualification of Eager (mode 0) "
            "and Cursor (mode 2); the only changing factor is the progress mode. All arms use "
            "Direct FRONTIER, dynamic coalescing, 1 MiB Point lookahead, and existing digest sampling. Point latency "
            "is sampled every 10 completed puts; Batch64 is throughput-only. The historical F0.5 Ring "
            "plan and verifier remain available for archived results. Production defaults remain Eager."
        ),
    })
    return plan


def build_cursor_direct_completion_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    """Build the independent Cursor relay/direct-completion experiment plan."""
    writer_counts = validate_writer_counts(writers)
    schedule = build_cursor_direct_completion_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    return {
        "plan_schema_version": CURSOR_DIRECT_COMPLETION_PLAN_SCHEMA_V1,
        "suite": "v8-cursor-direct-completion",
        "writers": list(writer_counts),
        "workloads": [
            {"writer": writer, "name": workload, "bench": bench,
             "batch_entries": batch_entries, "batch_size": batch_size,
             "num": POINT_PROGRESS_RING_NUM, "sample_every": sample_every}
            for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS
        ],
        "arms": list(CURSOR_DIRECT_COMPLETION_ARMS),
        "repetitions_per_cell": POINT_PROGRESS_RING_REPETITIONS,
        "arm_orders": [list(order) for order in CURSOR_DIRECT_COMPLETION_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same source and binary for both arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "extent_preparation": "sliding-point-lookahead, 1 MiB for both arms; Point-only policy keeps Batch64 on fallocate",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "progress_mode": "cursor (mode 2) for both arms",
        "direct_completion_ab": {
            "environment_variable": "TOYKV_BENCH_V8_CURSOR_DIRECT_COMPLETION",
            "baseline_value": "0",
            "candidate_value": "1",
        },
        "point_direct_frame_enabled": False,
        "point_frontier_credit_batch_enabled": False,
        "extent_policy_storage": "vec",
        "extent_policy_scan_diagnostics_enabled": False,
        "data_submit_order": "ordered",
        "latency_sample_every": 10,
        "hash_cpu_sample_every": 64,
        "worker_group_histogram": "disabled for score runs; opt-in diagnostics only",
        "qualification_note": (
            "Independent same-host, same-source, same-release-binary Cursor-only experiment. Both arms use progress "
            "mode 2; only the benchmark-only completion relay bypass changes. Point latency is sampled every 10 "
            "completed puts; Batch64 is a regression guardrail. This experiment does not change production defaults."
        ),
    }


def build_cursor_direct_completion_wake_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    """Build the isolated same-binary Token/Select direct-completion plan."""
    writer_counts = validate_writer_counts(writers)
    schedule = build_cursor_direct_completion_wake_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    return {
        "plan_schema_version": CURSOR_DIRECT_COMPLETION_WAKE_PLAN_SCHEMA_V1,
        "suite": "v8-cursor-direct-completion-wake",
        "writers": list(writer_counts),
        "workloads": [
            {"writer": writer, "name": workload, "bench": bench,
             "batch_entries": batch_entries, "batch_size": batch_size,
             "num": POINT_PROGRESS_RING_NUM, "sample_every": sample_every}
            for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS
        ],
        "arms": list(CURSOR_DIRECT_COMPLETION_WAKE_ARMS),
        "repetitions_per_cell": POINT_PROGRESS_RING_REPETITIONS,
        "arm_orders": [list(order) for order in CURSOR_DIRECT_COMPLETION_WAKE_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same source and binary for both arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "extent_preparation": "sliding-point-lookahead, 1 MiB for both arms; Point-only policy keeps Batch64 on fallocate",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "progress_mode": "cursor (mode 2) for both arms",
        "direct_completion_enabled": True,
        "completion_wake_ab": {
            "environment_variable": "TOYKV_BENCH_V8_DIRECT_COMPLETION_WAKE",
            "baseline_value": "token",
            "candidate_value": "select",
        },
        "point_buffer_preallocation": "enabled for Point only (Stage A); disabled for Batch64",
        "point_direct_frame_enabled": False,
        "point_frontier_credit_batch_enabled": False,
        "extent_policy_storage": "vec",
        "extent_policy_scan_diagnostics_enabled": False,
        "data_submit_order": "ordered",
        "latency_sample_every": 10,
        "hash_cpu_sample_every": 64,
        "worker_group_histogram": "disabled for score runs; opt-in diagnostics only",
        "qualification_note": (
            "Same-host, same-source, same-release-binary E1 wake-mode comparison. Both arms use Cursor progress, "
            "Direct Completion, CallerDriven durability, Legacy ACK, Stage A Point buffer preallocation, dynamic "
            "coalescing, Direct FRONTIER, and 1 MiB Point lookahead. Only direct-completion wake delivery changes "
            "from control token to select over the completion queue and control channel. Scored runs disable detailed "
            "diagnostics; Batch64 is a regression guardrail. This experiment does not change production defaults."
        ),
    }


def build_cursor_singleton_submission_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    """Build the independent Cursor singleton-submission experiment plan."""
    writer_counts = validate_writer_counts(writers)
    schedule = build_cursor_singleton_submission_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    return {
        "plan_schema_version": CURSOR_SINGLETON_SUBMISSION_PLAN_SCHEMA_V1,
        "suite": "v8-cursor-singleton-submission",
        "writers": list(writer_counts),
        "workloads": [
            {"writer": writer, "name": workload, "bench": bench,
             "batch_entries": batch_entries, "batch_size": batch_size,
             "num": POINT_PROGRESS_RING_NUM, "sample_every": sample_every}
            for writer, workload, bench, batch_entries, batch_size, sample_every in POINT_PROGRESS_RING_CELLS
        ],
        "arms": list(CURSOR_SINGLETON_SUBMISSION_ARMS),
        "repetitions_per_cell": POINT_PROGRESS_RING_REPETITIONS,
        "arm_orders": [list(order) for order in CURSOR_SINGLETON_SUBMISSION_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same source and binary for both arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "extent_preparation": "sliding-point-lookahead, 1 MiB for both arms; Point-only policy keeps Batch64 on fallocate",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "progress_mode": "cursor (mode 2) for both arms",
        "direct_completion_enabled": False,
        "singleton_submission_ab": {
            "environment_variable": "TOYKV_BENCH_V8_CURSOR_SINGLETON_SUBMISSION",
            "baseline_value": "0",
            "candidate_value": "1",
        },
        "point_direct_frame_enabled": False,
        "point_frontier_credit_batch_enabled": False,
        "extent_policy_storage": "vec",
        "extent_policy_scan_diagnostics_enabled": False,
        "data_submit_order": "ordered",
        "latency_sample_every": 10,
        "hash_cpu_sample_every": 64,
        "worker_group_histogram": "disabled for score runs; opt-in diagnostics only",
        "qualification_note": (
            "Same-host, same-source, same-release-binary Cursor-only comparison. The only changing factor is "
            "the allocation-free representation for singleton DATA groups; group formation, submission order, "
            "and multi-ticket group representation are unchanged. Score runs disable detailed diagnostics. "
            "This experiment does not change production defaults."
        ),
    }


def build_point_progress_cursor_v2_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    """Build the historical three-arm plan used by 60-run Cursor archives."""
    writer_counts = validate_writer_counts(writers)
    schedule = build_point_progress_cursor_v2_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan = build_point_progress_ring_plan(
        writers=writer_counts,
        machine_type=machine_type,
        machine_spec=machine_spec,
        commit=commit,
        binary_sha256=binary_sha256,
    )
    plan.pop("progress_ring_ab")
    plan.update({
        "plan_schema_version": POINT_PROGRESS_PLAN_SCHEMA_V2,
        "suite": "v8-point-progress-cursor",
        "arms": list(POINT_PROGRESS_CURSOR_V2_ARMS),
        "arm_orders": [list(order) for order in POINT_PROGRESS_CURSOR_V2_ORDERS],
        "runs_planned": len(schedule),
        "score_runs_planned": len(schedule),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same source and binary for three arms",
        "progress_ring_control": {
            "environment_variable": "TOYKV_BENCH_V8_POINT_PROGRESS_RING",
            "candidate_value": "1",
        },
        "progress_cursor_ab": {
            "environment_variable": "TOYKV_BENCH_V8_POINT_PROGRESS_RING",
            "baseline_value": "0",
            "candidate_value": "2",
        },
        "progress_counters_enabled": False,
        "qualification_note": (
            "Same-host, same-source, same-release-binary three-arm qualification of Eager (mode 0), Ring (mode 1), "
            "and Cursor (mode 2); the only changing factor is the progress mode. All arms use "
            "Direct FRONTIER, dynamic coalescing, 1 MiB Point lookahead, and existing digest sampling. Point latency "
            "is sampled every 10 completed puts; Batch64 is throughput-only. The independent Cursor matrix keeps "
            "the historical F0.5 Ring plan and verifier unchanged, and does not change production defaults."
        ),
    })
    return plan


def _validate_point_progress_rows(
    rows: list[Mapping[str, object]], schedule: list[dict[str, object]],
) -> dict[str, object]:
    """Validate run identities, planned mode flags, fixed work, and Point samples."""
    expected = {
        (row["writer"], row["workload"], row["repetition"], row["arm"]): row for row in schedule
    }
    observed: dict[tuple[object, object, object, object], int] = {}
    invalid: list[Mapping[str, object]] = []
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "repetition", "arm"))
        observed[identity] = observed.get(identity, 0) + 1
        expected_row = expected.get(identity)
        histogram_disabled = row.get("worker_group_sizes") is None
        is_point = row.get("workload") == "point"
        metrics_valid = all(
            isinstance(row.get(key), (int, float))
            and math.isfinite(float(row[key]))
            and float(row[key]) > 0
            for key in (("puts_per_second", "cpu_us_per_put", "p99_ms") if is_point
                        else ("puts_per_second", "cpu_us_per_put"))
        )
        samples_valid = True
        if is_point:
            samples = row.get("latency_samples")
            unsampled = row.get("latency_unsampled_operations")
            samples_valid = (
                row.get("latency_sample_every") == 10
                and type(samples) is int
                and samples == POINT_PROGRESS_RING_NUM // 10
                and type(unsampled) is int
                and unsampled + samples == POINT_PROGRESS_RING_NUM
            )
        digest_cpu = row.get("ordered_link_hash_cpu_ns")
        if (
            expected_row is None
            or type(row.get("exit_code")) is not int
            or row.get("exit_code") != 0
            or row.get("ops") != POINT_PROGRESS_RING_NUM
            or row.get("progress_ring_enabled") is not bool(expected_row["progress_ring_enabled"])
            or not histogram_disabled
            or not metrics_valid
            or not samples_valid
            or type(digest_cpu) is not int
            or digest_cpu < 0
        ):
            invalid.append(row)

    expected_cells = set(expected)
    observed_cells = set(observed)
    return {
        "complete": not (
            expected_cells != observed_cells or any(count != 1 for count in observed.values()) or invalid
        ),
        "planned": len(schedule),
        "observed": len(rows),
        "missing_cells": [list(cell) for cell in sorted(expected_cells - observed_cells)],
        "unexpected_cells": [list(cell) for cell in sorted(observed_cells - expected_cells, key=str)],
        "duplicate_cells": [list(cell) for cell, count in observed.items() if count != 1],
        "invalid_rows": [dict(row) for row in invalid],
    }


def validate_point_progress_ring_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> dict[str, object]:
    """Validate the historical two-arm F0.5 Ring matrix."""
    return _validate_point_progress_rows(rows, build_point_progress_ring_schedule(writers))


def validate_point_progress_cursor_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> dict[str, object]:
    """Validate the Eager/Cursor matrix and counter switch."""
    schedule = build_point_progress_cursor_schedule(writers)
    validation = _validate_point_progress_rows(rows, schedule)
    invalid_mode_flags: list[Mapping[str, object]] = []
    for row in rows:
        arm = row.get("arm")
        if (
            type(row.get("progress_cursor_enabled")) is not bool
            or row.get("progress_cursor_enabled") is not (arm == "cursor")
            or row.get("progress_counters_enabled") is not False
        ):
            invalid_mode_flags.append(row)
    if invalid_mode_flags:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid_mode_flags)
    return validation


def validate_cursor_direct_completion_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> dict[str, object]:
    """Validate the Cursor relay/direct-completion score matrix and actual mode flags."""
    schedule = build_cursor_direct_completion_schedule(writers)
    validation = _validate_point_progress_rows(rows, schedule)
    invalid_mode_flags: list[Mapping[str, object]] = []
    for row in rows:
        if (
            row.get("progress_cursor_enabled") is not True
            or row.get("progress_counters_enabled") is not False
            or type(row.get("cursor_direct_completion_enabled")) is not bool
            or row.get("cursor_direct_completion_enabled") is not (row.get("arm") == "cursor_direct")
        ):
            invalid_mode_flags.append(row)
    if invalid_mode_flags:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid_mode_flags)
    return validation


def validate_cursor_direct_completion_wake_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> dict[str, object]:
    """Validate Token/Select mode flags and the Stage A Point-only setting."""
    schedule = build_cursor_direct_completion_wake_schedule(writers)
    validation = _validate_point_progress_rows(rows, schedule)
    expected = {
        (row["writer"], row["workload"], row["repetition"], row["arm"]): row
        for row in schedule
    }
    invalid: list[Mapping[str, object]] = []
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "repetition", "arm"))
        planned = expected.get(identity)
        if planned is None:
            invalid.append(row)
            continue
        expected_select = planned["arm"] == "select"
        if any((
            row.get("progress_cursor_enabled") is not True,
            row.get("progress_counters_enabled") is not False,
            row.get("cursor_direct_completion_enabled") is not True,
            row.get("cursor_direct_completion_wake_mode") != planned["arm"],
            row.get("cursor_direct_completion_select_wake_enabled_start") is not expected_select,
            row.get("cursor_direct_completion_select_wake_enabled_end") is not expected_select,
            row.get("point_buffer_preallocation_enabled") is not planned["point_buffer_preallocation_enabled"],
        )):
            invalid.append(row)
    if invalid:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid)
    return validation


def validate_cursor_singleton_submission_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> dict[str, object]:
    """Validate the singleton-submission score matrix and actual mode flags."""
    schedule = build_cursor_singleton_submission_schedule(writers)
    validation = _validate_point_progress_rows(rows, schedule)
    invalid_mode_flags: list[Mapping[str, object]] = []
    for row in rows:
        arm = row.get("arm")
        if (
            row.get("progress_cursor_enabled") is not True
            or row.get("progress_counters_enabled") is not False
            or row.get("cursor_direct_completion_enabled") is not False
            or type(row.get("cursor_singleton_submission_enabled")) is not bool
            or row.get("cursor_singleton_submission_enabled") is not (arm == "cursor_singleton")
        ):
            invalid_mode_flags.append(row)
    if invalid_mode_flags:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid_mode_flags)
    return validation


def validate_cursor_publication_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int] = CURSOR_PUBLICATION_WRITERS,
) -> dict[str, object]:
    """Validate each planned D2a/D2b arm against captured runtime settings."""
    schedule = build_cursor_publication_schedule(writers)
    validation = _validate_point_progress_rows(rows, schedule)
    expected = {
        (row["writer"], row["workload"], row["repetition"], row["arm"]): row
        for row in schedule
    }
    invalid: list[Mapping[str, object]] = []
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "repetition", "arm"))
        planned = expected.get(identity)
        if planned is None:
            invalid.append(row)
            continue
        for key in (
            "progress_cursor_enabled", "progress_counters_enabled", "cursor_direct_completion_enabled",
            "cursor_singleton_submission_enabled", "point_buffer_preallocation_enabled",
            "point_resource_retirement_mode", "admission_cursor_publish_fast_path_enabled",
        ):
            if row.get(key) != planned[key]:
                invalid.append(row)
                break
    if invalid:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid)
    return validation


def validate_point_resource_retirement_rows(
    rows: list[Mapping[str, object]],
    writers: Iterable[int] = POINT_RESOURCE_RETIREMENT_WRITERS,
    *,
    point_buffer_preallocation_enabled: bool = False,
) -> dict[str, object]:
    """Validate mode selection and prove C1/C2 were actually exercised."""
    schedule = build_point_resource_retirement_schedule(
        writers,
        point_buffer_preallocation_enabled=point_buffer_preallocation_enabled,
    )
    validation = _validate_point_progress_rows(rows, schedule)
    expected = {
        (row["writer"], row["workload"], row["repetition"], row["arm"]): row
        for row in schedule
    }
    invalid: list[Mapping[str, object]] = []
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "repetition", "arm"))
        planned = expected.get(identity)
        mode = row.get("point_resource_retirement_mode")
        detached = row.get("point_resource_retirement_detached_prefixes")
        detached_tickets = row.get("point_resource_retirement_detached_tickets")
        batched = row.get("point_resource_retirement_batched_prefixes")
        valid = (
            planned is not None
            and mode == planned["point_resource_retirement_mode"]
            and type(detached) is int and detached >= 0
            and type(detached_tickets) is int and detached_tickets >= 0
            and type(batched) is int and batched >= 0
            and batched <= detached
        )
        if valid and row.get("workload") == "point":
            expected_mode = int(planned["point_resource_retirement_mode"])
            valid = (
                (expected_mode == 0 and detached == 0 and detached_tickets == 0 and batched == 0)
                or (expected_mode == 1 and detached > 0 and detached_tickets > 0 and batched == 0)
                or (expected_mode == 2 and detached > 0 and detached_tickets > 0 and batched > 0)
            )
        elif valid:
            valid = detached == 0 and detached_tickets == 0 and batched == 0
        if not valid:
            invalid.append(row)
    if invalid:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid)
    return validation


def validate_point_progress_cursor_v2_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int] = POINT_PROGRESS_RING_WRITERS,
) -> dict[str, object]:
    """Validate archived three-arm Eager/Ring/Cursor rows, without running Ring mode."""
    schedule = build_point_progress_cursor_v2_schedule(writers)
    validation = _validate_point_progress_rows(rows, schedule)
    invalid_mode_flags: list[Mapping[str, object]] = []
    for row in rows:
        arm = row.get("arm")
        if (
            type(row.get("progress_cursor_enabled")) is not bool
            or row.get("progress_cursor_enabled") is not (arm == "cursor")
            or row.get("progress_counters_enabled") is not False
        ):
            invalid_mode_flags.append(row)
    if invalid_mode_flags:
        validation["complete"] = False
        validation["invalid_rows"].extend(dict(row) for row in invalid_mode_flags)
    return validation


def build_v8_extent_policy_storage_schedule(
    writers: Iterable[int],
) -> list[dict[str, object]]:
    """Build uninstrumented BTreeMap/Vec score pairs and separate scan diagnostics."""
    writer_counts = validate_writer_counts(writers)
    if writer_counts != V8_EXTENT_POLICY_STORAGE_WRITERS:
        raise ValueError(
            f"extent-policy storage suite requires writers {V8_EXTENT_POLICY_STORAGE_WRITERS}"
        )

    cells = [(writer, V8_EXTENT_POLICY_STORAGE_WORKLOADS[0]) for writer in writer_counts]
    cells.extend((16, workload) for workload in V8_EXTENT_POLICY_STORAGE_WORKLOADS[1:])
    schedule: list[dict[str, object]] = []
    for writer, workload in cells:
        for repetition in range(1, V8_EXTENT_POLICY_STORAGE_REPETITIONS + 1):
            order = V8_EXTENT_POLICY_STORAGE_ORDERS[(repetition - 1) % len(V8_EXTENT_POLICY_STORAGE_ORDERS)]
            for arm in order:
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": workload["name"],
                    "phase": "score",
                    "repetition": repetition,
                    "arm": arm,
                    "bench": workload["bench"],
                    "batch_entries": workload["batch_entries"],
                    "batch_size": workload["batch_size"],
                    "num": V8_EXTENT_POLICY_STORAGE_NUM,
                    "sample_every": 100 if workload["name"] == "point" else 10,
                    "extent_mode": "sliding-point-lookahead",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "frontier_mode": "direct",
                    "scan_diagnostics_enabled": False,
                })

    for writer in writer_counts:
        order = V8_EXTENT_POLICY_STORAGE_ORDERS[(writer // 8 - 1) % len(V8_EXTENT_POLICY_STORAGE_ORDERS)]
        for arm in order:
            schedule.append({
                "run_index": len(schedule) + 1,
                "writer": writer,
                "workload": "point",
                "phase": "diagnostic",
                "repetition": 0,
                "arm": arm,
                "bench": "wal_concurrent",
                "batch_entries": 1,
                "batch_size": None,
                "num": V8_EXTENT_POLICY_STORAGE_NUM,
                "sample_every": 100,
                "extent_mode": "sliding-point-lookahead",
                "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                "frontier_mode": "direct",
                "scan_diagnostics_enabled": True,
            })
    return schedule


def build_v8_extent_policy_storage_plan(
    *, writers: Iterable[int], machine_type: str, machine_spec: Mapping[str, int],
    commit: str, binary_sha256: str | None = None,
) -> dict[str, object]:
    writer_counts = validate_writer_counts(writers)
    schedule = build_v8_extent_policy_storage_schedule(writer_counts)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "suite": "v8-extent-policy-storage",
        "writers": list(writer_counts),
        "workloads": [dict(workload, num=V8_EXTENT_POLICY_STORAGE_NUM)
                      for workload in V8_EXTENT_POLICY_STORAGE_WORKLOADS],
        "arms": list(V8_EXTENT_POLICY_STORAGE_ARMS),
        "repetitions_per_cell": V8_EXTENT_POLICY_STORAGE_REPETITIONS,
        "score_runs_planned": sum(row["phase"] == "score" for row in schedule),
        "diagnostic_runs_planned": sum(row["phase"] == "diagnostic" for row in schedule),
        "runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same binary and source for both arms",
        "value_bytes": 1024,
        "compaction": "none",
        "commit": commit,
        "binary_sha256": binary_sha256,
        "extent_preparation": "sliding-point-lookahead, 1 MiB",
        "frontier_mode": "direct",
        "coalescing": "dynamic",
        "storage_ab": {
            "environment_variable": "TOYKV_BENCH_V8_EXTENT_POLICY_STORAGE",
            "baseline": "btree",
            "candidate": "vec",
            "score_diagnostics": False,
            "diagnostic_runs_per_storage": 1,
        },
        "qualification_note": (
            "The BTreeMap and Vec score arms use one bench binary and differ only in extent-policy storage. "
            "Scan counters are disabled for throughput runs; four separate Point runs enable them to verify "
            "the work reduction. Batch16/64 remain independent regression guardrails."
        ),
    }
    return plan


def validate_v8_extent_policy_storage_rows(
    rows: list[Mapping[str, object]], writers: Iterable[int],
) -> dict[str, object]:
    schedule = build_v8_extent_policy_storage_schedule(writers)
    expected = {
        (row["writer"], row["workload"], row["phase"], row["repetition"], row["arm"]): row
        for row in schedule
    }
    observed: dict[tuple[object, ...], int] = {}
    invalid: list[Mapping[str, object]] = []
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "phase", "repetition", "arm"))
        observed[identity] = observed.get(identity, 0) + 1
        planned = expected.get(identity)
        if (
            planned is None
            or row.get("exit_code") != 0
            or row.get("ops") != V8_EXTENT_POLICY_STORAGE_NUM
            or row.get("extent_policy_storage") != row.get("arm")
            or row.get("scan_diagnostics_enabled") != bool(planned["scan_diagnostics_enabled"])
        ):
            invalid.append(row)
            continue
        if planned["phase"] == "score":
            if row.get("lookahead_check_calls") != 0 or row.get("extent_policy_entries_scanned") != 0:
                invalid.append(row)
        elif (
            row.get("workload") != "point"
            or row.get("lookahead_check_calls", 0) <= 0
            or row.get("extent_policy_entries_scanned", 0) <= 0
        ):
            invalid.append(row)
        if row.get("workload") != "point" and row.get("initialized_bytes") != 0:
            invalid.append(row)

    expected_cells = set(expected)
    observed_cells = set(observed)
    return {
        "complete": not (
            expected_cells != observed_cells or any(count != 1 for count in observed.values()) or invalid
        ),
        "planned": len(schedule),
        "observed": len(rows),
        "missing_cells": [list(cell) for cell in sorted(expected_cells - observed_cells, key=str)],
        "unexpected_cells": [list(cell) for cell in sorted(observed_cells - expected_cells, key=str)],
        "duplicate_cells": [list(cell) for cell, count in observed.items() if count != 1],
        "invalid_rows": [dict(row) for row in invalid],
    }
def build_v8_mutex_diagnostic_schedule() -> list[dict[str, object]]:
    """Build five counterbalanced V7/V8 Point mutex-diagnostic pairs."""
    schedule: list[dict[str, object]] = []
    for repetition in range(1, V8_MUTEX_DIAGNOSTIC_REPETITIONS + 1):
        order = V8_MUTEX_DIAGNOSTIC_ORDERS[(repetition - 1) % len(V8_MUTEX_DIAGNOSTIC_ORDERS)]
        for arm in order:
            is_v8 = arm == "v8_point_lookahead_direct"
            schedule.append({
                "run_index": len(schedule) + 1,
                "writer": V8_MUTEX_DIAGNOSTIC_WRITERS[0],
                "workload": "point",
                "repetition": repetition,
                "arm": arm,
                "bench": "wal_concurrent",
                "batch_entries": 1,
                "batch_size": None,
                "num": V8_MUTEX_DIAGNOSTIC_NUM,
                "sample_every": V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY,
                "extent_mode": "sliding-point-lookahead" if is_v8 else "none",
                "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES if is_v8 else 0,
                "frontier_mode": "direct" if is_v8 else "n/a",
            })
    return schedule


def build_v8_mutex_diagnostic_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str | None = None,
    binary_sha256: str | None = None,
) -> dict[str, object]:
    schedule = build_v8_mutex_diagnostic_schedule()
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "suite": "v8-mutex-diagnostic",
        "diagnostic_only": True,
        "writers": list(V8_MUTEX_DIAGNOSTIC_WRITERS),
        "workloads": [{"name": "point", "bench": "wal_concurrent", "num": V8_MUTEX_DIAGNOSTIC_NUM}],
        "arms": list(V8_MUTEX_DIAGNOSTIC_ORDERS[0]),
        "repetitions_per_pair": V8_MUTEX_DIAGNOSTIC_REPETITIONS,
        "arm_orders": [list(order) for order in V8_MUTEX_DIAGNOSTIC_ORDERS],
        "mutex_sample_every": V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY,
        "runs_planned": len(schedule),
        "counterbalanced": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}",
        "value_bytes": 1024,
        "compaction": "none",
        "v8_configuration": {
            "extent_preparation": "sliding-point-lookahead",
            "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
            "frontier_mode": "direct",
            "coalescing": "dynamic",
        },
        "qualification_note": (
            "Diagnostic-only paired runs; sampled mutex timings and estimates are not uninstrumented "
            "throughput scores. V7 and V8 use the same binary and host."
        ),
    }
    if commit is not None:
        plan["commit"] = commit
    if binary_sha256 is not None:
        plan["binary_sha256"] = binary_sha256
    return plan


def validate_v8_mutex_diagnostic_rows(rows: list[Mapping[str, object]]) -> dict[str, object]:
    """Validate each planned diagnostic run and its sampled mutex payload."""
    schedule = build_v8_mutex_diagnostic_schedule()
    expected = {
        (row["writer"], row["workload"], row["repetition"], row["arm"]): row for row in schedule
    }
    observed: dict[tuple[object, object, object, object], int] = {}
    invalid: list[Mapping[str, object]] = []
    lock_names = {
        "resource_ledger", "reservation_owner", "v8_extent_state", "v8_extent_preparation",
        "mvcc_publication",
    }
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "repetition", "arm"))
        observed[identity] = observed.get(identity, 0) + 1
        try:
            if identity not in expected or row.get("exit_code") != 0 or row.get("ops") != V8_MUTEX_DIAGNOSTIC_NUM:
                invalid.append(row)
                continue
            diagnostics = row["mutex_lock_diagnostics"]
            if not isinstance(diagnostics, Mapping):
                invalid.append(row)
                continue
            if diagnostics.get("sample_every") != V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY:
                invalid.append(row)
                continue
            locks = diagnostics.get("locks")
            if not isinstance(locks, list) or {entry.get("name") for entry in locks if isinstance(entry, Mapping)} != lock_names:
                invalid.append(row)
                continue
        except (KeyError, TypeError):
            invalid.append(row)

    expected_cells = set(expected)
    observed_cells = set(observed)
    return {
        "complete": not (
            expected_cells != observed_cells
            or any(count != 1 for count in observed.values())
            or invalid
        ),
        "planned": len(schedule),
        "observed": len(rows),
        "missing_cells": [list(cell) for cell in sorted(expected_cells - observed_cells)],
        "unexpected_cells": [list(cell) for cell in sorted(observed_cells - expected_cells, key=str)],
        "duplicate_cells": [list(cell) for cell, count in observed.items() if count != 1],
        "invalid_rows": [dict(row) for row in invalid],
    }


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
        "diagnostic_sampling": {
            "mvcc_publication_sample_every": 64,
            "v8_durability_sample_every": 64,
        },
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


def validate_point_lookahead_rows(
    rows: Iterable[Mapping[str, object]], writers: Iterable[int],
) -> dict[str, object]:
    schedule = build_point_lookahead_schedule(writers)
    expected = {
        (int(item["writer"]), str(item["workload"]), int(item["repetition"]), str(item["arm"])):
        int(item["num"])
        for item in schedule
    }
    return _validate_rows(rows, expected, ("writer", "workload", "repetition", "arm"))


def build_v8_point_cpu_attribution_schedule(
    writers: Iterable[int] = V8_POINT_CPU_ATTRIBUTION_WRITERS,
    repetitions: int = 1,
) -> list[dict[str, object]]:
    """Build a counterbalanced diagnostic-only V7/V8 Point schedule."""
    schedule: list[dict[str, object]] = []
    writer_counts = validate_writer_counts(writers)
    if not set(writer_counts).issubset(V8_POINT_CPU_ATTRIBUTION_WRITERS):
        raise ValueError(f"CPU attribution writers must be selected from {V8_POINT_CPU_ATTRIBUTION_WRITERS}")
    if type(repetitions) is not int or repetitions <= 0:
        raise ValueError("CPU attribution repetitions must be a positive integer")
    for repetition in range(1, repetitions + 1):
        for index, writer in enumerate(writer_counts):
            order = (
                V8_POINT_CPU_ATTRIBUTION_ARMS
                if (index + repetition - 1) % 2 == 0
                else tuple(reversed(V8_POINT_CPU_ATTRIBUTION_ARMS))
            )
            for arm in order:
                is_v8 = arm == "v8_point_lookahead_direct"
                schedule.append({
                    "run_index": len(schedule) + 1,
                    "writer": writer,
                    "workload": "point",
                    "repetition": repetition,
                    "arm": arm,
                    "bench": "wal_concurrent",
                    "batch_entries": 1,
                    "batch_size": None,
                    "num": V8_POINT_CPU_ATTRIBUTION_NUM,
                    "sample_every": V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
                    "extent_mode": "sliding-point-lookahead" if is_v8 else "none",
                    "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES if is_v8 else 0,
                    "frontier_mode": "direct" if is_v8 else "n/a",
                })
    return schedule


def build_v8_point_cpu_attribution_plan(
    *, machine_type: str, machine_spec: Mapping[str, int], commit: str | None = None,
    binary_sha256: str | None = None, writers: Iterable[int] = V8_POINT_CPU_ATTRIBUTION_WRITERS,
    repetitions: int = 1,
) -> dict[str, object]:
    writer_counts = validate_writer_counts(writers)
    schedule = build_v8_point_cpu_attribution_schedule(writer_counts, repetitions)
    vcpus = int(machine_spec["vcpus"])
    ssd_count = int(machine_spec["ssd_count"])
    layout = "single Local SSD" if ssd_count == 1 else f"RAID0 over {ssd_count} Local SSDs"
    plan: dict[str, object] = {
        "suite": "v8-point-cpu-attribution",
        "diagnostic_only": True,
        "writers": list(writer_counts),
        "repetitions_per_cell": repetitions,
        "workloads": [{"name": "point", "bench": "wal_concurrent", "num": V8_POINT_CPU_ATTRIBUTION_NUM}],
        "arms": list(V8_POINT_CPU_ATTRIBUTION_ARMS),
        "runs_per_writer": 2 * repetitions,
        "runs_per_writer_per_repetition": 2,
        "runs_planned": len(schedule),
        "sample_every": V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
        "counterbalanced_order": True,
        "machine_type": machine_type,
        "vcpus": vcpus,
        "local_ssd_count": ssd_count,
        "local_ssd_total_gib": int(machine_spec["ssd_gib"]),
        "runtime_protocol": f"single {vcpus}-vCPU host; {layout}; same binary and source for V7/V8",
        "value_bytes": 1024,
        "compaction": "none",
        "v8_configuration": {
            "extent_preparation": "sliding-point-lookahead",
            "extent_window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
            "frontier_mode": "direct",
            "coalescing": "dynamic",
        },
        "qualification_note": (
            "Instrumented diagnostic runs use the declared repetition count per V7/V8/writer cell. "
            "These are CPU-attribution diagnostics, "
            "not uninstrumented throughput scores. Producer samples are every 64th Point operation; selected "
            "worker and durability samples have role-specific units. V7 and V8 phase boundaries are not all "
            "identical and must be interpreted from their field names."
        ),
    }
    if commit is not None:
        plan["commit"] = commit
    if binary_sha256 is not None:
        plan["binary_sha256"] = binary_sha256
    return plan


def validate_v8_point_cpu_attribution_rows(
    rows: Iterable[Mapping[str, object]],
    writers: Iterable[int] = V8_POINT_CPU_ATTRIBUTION_WRITERS,
    repetitions: int = 1,
) -> dict[str, object]:
    """Validate each successful diagnostic result in the planned schedule."""
    schedule = build_v8_point_cpu_attribution_schedule(writers, repetitions)
    expected = {
        (int(row["writer"]), str(row["workload"]), int(row["repetition"]), str(row["arm"])):
        int(row["num"])
        for row in schedule
    }
    observed: dict[tuple[object, ...], int] = {}
    invalid: list[Mapping[str, object]] = []
    for row in rows:
        identity = tuple(row.get(key) for key in ("writer", "workload", "repetition", "arm"))
        observed[identity] = observed.get(identity, 0) + 1
        if (
            identity not in expected
            or row.get("exit_code") != 0
            or row.get("ops") != V8_POINT_CPU_ATTRIBUTION_NUM
            or row.get("sample_every") != V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY
            or row.get("wal_cpu_attribution_valid") is not True
        ):
            invalid.append(row)

    expected_cells = set(expected)
    observed_cells = set(observed)
    return {
        "complete": not (
            expected_cells != observed_cells or any(count != 1 for count in observed.values()) or invalid
        ),
        "planned": len(schedule),
        "observed": len(rows),
        "missing_cells": [list(cell) for cell in sorted(expected_cells - observed_cells, key=str)],
        "unexpected_cells": [list(cell) for cell in sorted(observed_cells - expected_cells, key=str)],
        "duplicate_cells": [list(cell) for cell, count in observed.items() if count != 1],
        "invalid_rows": [dict(row) for row in invalid],
    }
