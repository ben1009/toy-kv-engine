"""Unit tests for safety-sensitive pieces of the GCP benchmark runner."""

from __future__ import annotations

import base64
import csv
import hashlib
import importlib.util
from contextlib import redirect_stderr
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import shutil
import stat
import struct
import subprocess
import tempfile
import tarfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from gcp_wal_matrix import (
    COMMIT_ORDER_TRACE_NUM,
    COMMIT_ORDER_TRACE_WRITERS,
    FULL_DEFAULT_WRITERS,
    LEGACY_DEFAULT_WRITERS,
    POINT_LOOKAHEAD_WRITERS,
    POINT_LOOKAHEAD_WINDOW_BYTES,
    POINT_BORROWED_INPUT_NUM,
    POINT_BORROWED_INPUT_WRITERS,
    POINT_DIRECT_FRAME_NUM,
    POINT_DIRECT_FRAME_WRITERS,
    POINT_PROGRESS_CURSOR_ORDERS,
    POINT_PROGRESS_RING_NUM,
    POINT_PROGRESS_RING_WRITERS,
    V8_EXTENT_POLICY_STORAGE_NUM,
    V8_EXTENT_POLICY_STORAGE_WRITERS,
    V8_MUTEX_DIAGNOSTIC_NUM,
    V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY,
    V8_MUTEX_DIAGNOSTIC_WRITERS,
    V8_POINT_CPU_ATTRIBUTION_NUM,
    V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
    V8_POINT_CPU_ATTRIBUTION_WRITERS,
    build_benchmark_plan,
    build_batch64_schedule,
    build_commit_order_trace_plan,
    build_commit_order_trace_schedule,
    build_point_lookahead_plan,
    build_point_lookahead_schedule,
    build_point_borrowed_input_plan,
    build_point_borrowed_input_schedule,
    build_point_direct_frame_plan,
    build_point_direct_frame_schedule,
    build_point_progress_ring_plan,
    build_point_progress_ring_schedule,
    build_point_progress_cursor_plan,
    build_point_progress_cursor_schedule,
    build_v8_extent_policy_storage_plan,
    build_v8_extent_policy_storage_schedule,
    build_v8_mutex_diagnostic_plan,
    build_v8_mutex_diagnostic_schedule,
    build_v8_point_cpu_attribution_plan,
    build_v8_point_cpu_attribution_schedule,
    build_rfc026_schedule,
    validate_commit_order_trace_rows,
    validate_batch64_rows,
    validate_point_lookahead_rows,
    validate_point_borrowed_input_rows,
    validate_point_direct_frame_rows,
    validate_point_progress_ring_rows,
    validate_v8_extent_policy_storage_rows,
    validate_v8_mutex_diagnostic_rows,
    validate_v8_point_cpu_attribution_rows,
    validate_rfc026_rows,
)
from analyze_gcp_commit_order_trace import extract_trace
from gcp_commit_order_trace_runner import (
    TRACE_STAGES,
    _inversion_count,
    analyze_trace,
    summarize_score_pairs,
)
from gcp_wal_profile_runner import (
    _expected_frontier_mode_for_arm,
    _load_benchmark_record,
    _run_one as run_profile_one,
    _validate_wal_io_metadata,
    build_profile_plan,
    build_profile_schedule,
    run_suite as run_profile_suite,
    validate_profile_rows,
)
from gcp_v8_mutex_diagnostic_runner import (
    LOCK_NAMES,
    _build_command as build_mutex_command,
    _validate_record as validate_mutex_record,
)
from gcp_v8_point_borrowed_input_runner import _summary_fields
import gcp_v8_point_cpu_attribution_runner as cpu_attribution_runner
import gcp_v8_point_progress_ring_runner as progress_ring_runner
from gcp_v8_point_cpu_attribution_runner import (
    _build_command as build_cpu_attribution_command,
    _completion_handoff_timeline_summary,
    _completion_timeline_summary,
    _run_id as cpu_attribution_run_id,
    _validate_v8_durability_phase_samples,
    run_suite as run_cpu_attribution_suite,
)


SCRIPT = Path(__file__).with_name("gcp-free-trial-wal-bench.py")
SPEC = importlib.util.spec_from_file_location("gcp_free_trial_wal_bench", SCRIPT)
assert SPEC and SPEC.loader
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


class SshStreamTransferTests(unittest.TestCase):
    def test_upload_streams_bytes_over_strict_ssh(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            local = Path(temporary) / "runner.sh"
            local.write_bytes(b"runner bytes\x00\xff")
            observed = {}

            def run(command, **kwargs):
                observed["command"] = command
                observed["bytes"] = kwargs["stdin"].read()
                return subprocess.CompletedProcess(command, 0, "", "")

            with patch.object(BENCH.subprocess, "run", side_effect=run):
                result = BENCH.stream_file_to_vm(
                    "192.0.2.10", local, "/tmp/runner with space.sh",
                    Path("/tmp/key"), Path("/tmp/known-hosts"), timeout=30,
                )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(observed["bytes"], b"runner bytes\x00\xff")
        command = observed["command"]
        self.assertEqual(command[0], "ssh")
        self.assertEqual(command[-1], "umask 077 && cat > '/tmp/runner with space.sh'")
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn("GlobalKnownHostsFile=/dev/null", command)

    def test_upload_retries_transient_disconnect_and_reaudits_before_reconnect(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            local = Path(temporary) / "runner.sh"
            payload = b"runner bytes"
            local.write_bytes(payload)
            order: list[str] = []
            results = [
                subprocess.CompletedProcess([], 255, "", "Connection closed by 192.0.2.10 port 22"),
                subprocess.CompletedProcess([], 0, "", ""),
            ]

            def run(command, **kwargs):
                order.append("ssh")
                self.assertEqual(kwargs["stdin"].read(), payload)
                return results.pop(0)

            def audit_before_retry() -> None:
                order.append("audit")

            with (
                patch.object(BENCH.subprocess, "run", side_effect=run),
                patch.object(BENCH.time, "sleep") as sleep,
            ):
                # The caller audits before attempt one; the callback is used
                # for each reconnect after a transient transport failure.
                order.append("audit")
                result = BENCH.stream_file_to_vm(
                    "192.0.2.10", local, "/tmp/runner.sh", Path("/tmp/key"),
                    Path("/tmp/known-hosts"), timeout=30, before_attempt=audit_before_retry,
                )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(order, ["audit", "ssh", "audit", "ssh"])
        self.assertIn("transient transport error", result.stderr)
        sleep.assert_called_once_with(1)

    def test_upload_does_not_retry_authentication_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            local = Path(temporary) / "runner.sh"
            local.write_bytes(b"runner bytes")
            before_retry = Mock()
            failure = subprocess.CompletedProcess([], 255, "", "Permission denied (publickey)")
            with (
                patch.object(BENCH.subprocess, "run", return_value=failure) as run,
                patch.object(BENCH.time, "sleep") as sleep,
            ):
                result = BENCH.stream_file_to_vm(
                    "192.0.2.10", local, "/tmp/runner.sh", Path("/tmp/key"),
                    Path("/tmp/known-hosts"), timeout=30, before_attempt=before_retry,
                )

        self.assertEqual(result.returncode, 255)
        run.assert_called_once()
        before_retry.assert_not_called()
        sleep.assert_not_called()

    def test_download_streams_binary_file_to_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "nested" / "results.tar.gz"
            observed = {}

            def run(command, **kwargs):
                observed["command"] = command
                kwargs["stdout"].write(b"archive bytes\x00\xff")
                return subprocess.CompletedProcess(command, 0, None, "")

            with patch.object(BENCH.subprocess, "run", side_effect=run):
                result = BENCH.stream_file_from_vm(
                    "192.0.2.10", "/tmp/results;echo unsafe", destination,
                    Path("/tmp/key"), Path("/tmp/known-hosts"), timeout=30,
                )
            self.assertEqual(result.returncode, 0)
            self.assertEqual(destination.read_bytes(), b"archive bytes\x00\xff")
            command = observed["command"]
            self.assertEqual(command[0], "ssh")
            self.assertEqual(command[-1], "cat -- '/tmp/results;echo unsafe'")
            self.assertIn("StrictHostKeyChecking=yes", command)
            self.assertIn("GlobalKnownHostsFile=/dev/null", command)


class PreflightGcloud:
    """Read-only API fixtures using Compute Engine's actual response fields."""

    def __init__(self, *, family_scoped=True):
        self.machine = {"guestCpus": 8, "bundledLocalSsds": {"partitionCount": 1, "defaultInterface": "NVME"}}
        self.instances = []
        self.disks = []
        self.family_scoped = family_scoped
        self.ssd_limit = 750
        self.json_calls = []

    def run(self, command):
        if command != ["auth", "list", "--filter=status:ACTIVE", "--format=value(account)"]:
            raise AssertionError(command)
        return subprocess.CompletedProcess(command, 0, "bench@example.com\n", "")

    def json(self, command, *, timeout=120):
        self.json_calls.append(command)
        if command[:2] == ["projects", "describe"]:
            return {"projectId": "test-project", "lifecycleState": "ACTIVE"}
        if command[:3] == ["compute", "zones", "describe"]:
            return {"status": "UP"}
        if command[:3] == ["compute", "machine-types", "describe"] and command[3] in BENCH.MACHINE_SPECS:
            return self.machine
        if command[:3] == ["compute", "images", "describe-from-family"]:
            return {"status": "READY", "name": "ubuntu-2404-pinned-image"}
        if command[:3] == ["compute", "networks", "describe"]:
            return {"name": "default", "autoCreateSubnetworks": True}
        if command[:3] == ["quotas", "info", "list"]:
            ssd_dimensions = {"region": "us-central1"}
            if self.family_scoped:
                ssd_dimensions["vm_family"] = "C4"
            return [
                {"quotaId": "CPUS_PER_VM_FAMILY", "dimensionsInfo": [{
                    "dimensions": {"region": "us-central1", "vm_family": "C4"}, "details": {"quotaValue": 64},
                }]},
                {"quotaId": "LOCAL_SSD_TOTAL_GB", "dimensionsInfo": [{
                    "dimensions": ssd_dimensions, "details": {"quotaValue": self.ssd_limit},
                }]},
                {"quotaId": "HDB-TOTAL-GB-per-project-region",
                 "metric": "compute.googleapis.com/hyperdisk_balanced_total_storage",
                 "dimensionsInfos": [{
                     "dimensions": {"region": "us-central1"},
                     "applicableLocations": ["us-central1"],
                     "details": {"value": "100"},
                 }]},
            ]
        if command[:3] == ["compute", "instances", "list"]:
            return self.instances
        if command[:3] == ["compute", "disks", "list"]:
            return self.disks
        if command[:3] == ["compute", "reservations", "list"]:
            return []
        if command[:3] == ["compute", "regions", "describe"]:
            return {"status": "UP", "quotas": [
                {"metric": name, "limit": limit, "usage": 0}
                for name, limit in (("INSTANCES", 10), ("IN_USE_ADDRESSES", 10))
            ]}
        if command[:3] == ["compute", "project-info", "describe"]:
            return {"quotas": [{"metric": "FIREWALLS", "limit": 10, "usage": 0}]}
        raise AssertionError(command)


class ProvisionGcloud:
    def __init__(self):
        self.calls = []

    def run(self, command, **_kwargs):
        self.calls.append(command)
        return subprocess.CompletedProcess(command, 0, "--max-run-duration --instance-termination-action", "")

    def json(self, command):
        return {
            "creationTimestamp": BENCH.datetime.now(BENCH.timezone.utc).isoformat(),
            "networkInterfaces": [{"accessConfigs": [{"natIP": "192.0.2.1"}]}],
        }


def fake_keygen(command, **_kwargs):
    key = Path(command[-1])
    key.write_text("private-key", encoding="utf-8")
    key.with_suffix(".pub").write_text("public-key", encoding="utf-8")
    return subprocess.CompletedProcess(command, 0, "", "")


class PointProgressRingQualificationTests(unittest.TestCase):
    @staticmethod
    def _raw_record(row: dict[str, object]) -> dict[str, object]:
        ring_enabled = bool(row["progress_ring_enabled"])
        cursor_enabled = bool(row.get("progress_cursor_enabled", False))
        start_preparation = {
            "mode": "sliding-point-lookahead",
            "window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
            "extent_policy_storage": "vec",
            "extent_policy_scan_diagnostics_enabled": False,
            "initialized_bytes": 0,
        }
        end_preparation = {
            **start_preparation,
            "initialized_bytes": POINT_LOOKAHEAD_WINDOW_BYTES if row["workload"] == "point" else 0,
        }
        handles = {
            "data": {"o_direct": True, "device": 8, "inode": 101},
            "frontier": {"o_direct": True, "device": 8, "inode": 101},
            "sync": {"o_direct": False, "device": 8, "inode": 101},
        }
        start = {
            **handles,
            "progress_ring_enabled": ring_enabled,
            "progress_cursor_enabled": cursor_enabled,
            "sliding_extent_preparation": start_preparation,
        }
        end = {
            **handles,
            "progress_ring_enabled": ring_enabled,
            "progress_cursor_enabled": cursor_enabled,
            "sliding_extent_preparation": end_preparation,
        }
        point = row["workload"] == "point"
        speed = 100_000.0
        if ring_enabled and row["writer"] in (8, 16) and point:
            speed *= 1.06
        elif ring_enabled and row["writer"] == 1 and point:
            speed *= 0.99
        elif ring_enabled and row["workload"] == "batch64":
            speed *= 0.995
        result = {
            "ops": POINT_PROGRESS_RING_NUM,
            "ops_per_sec": speed,
            "process_cpu_user_ms": 6000.0,
            "process_cpu_system_ms": 1000.0,
            "wal_pipeline": {
                "wal_io_stable": True,
                "wal_io_start": start,
                "wal_io_end": end,
                "sync_count": 1000,
            },
            "wal_v8_digest": {
                "cpu_timing_sample_rate": 64,
                "ordered_link_hash_cpu_ns": 200_000,
                "cpu_timing_sampled_tickets": 1563,
            },
        }
        record: dict[str, object] = {
            "params": {
                "num": POINT_PROGRESS_RING_NUM,
                "threads": row["writer"],
                "value_size": 1024,
                "batch_size": row["batch_size"],
                "wal_format_version": 8,
                "wal_io_mode": "parallel",
            },
            "result": result,
        }
        if point:
            record["latency"] = {
                "sample_every": 10,
                "samples": POINT_PROGRESS_RING_NUM // 10,
                "unsampled_completed_operations": POINT_PROGRESS_RING_NUM - POINT_PROGRESS_RING_NUM // 10,
                "p50_ms": 0.35,
                "p95_ms": 0.70,
                "p99_ms": 1.0,
            }
        return record

    @classmethod
    def _make_fixture(cls, root: Path, *, cursor: bool = False) -> Path:
        results = root / "results"
        results.mkdir()
        commit = "a" * 40
        binary_sha = "b" * 64
        schedule_builder = build_point_progress_cursor_schedule if cursor else build_point_progress_ring_schedule
        plan_builder = build_point_progress_cursor_plan if cursor else build_point_progress_ring_plan
        schedule = schedule_builder(POINT_PROGRESS_RING_WRITERS)
        plan = plan_builder(
            writers=POINT_PROGRESS_RING_WRITERS,
            machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            commit=commit,
            binary_sha256=binary_sha,
        )
        (results / "benchmark-plan.json").write_text(json.dumps(plan), encoding="utf-8")
        (results / "commit.txt").write_text(commit + "\n", encoding="utf-8")
        (results / "write-perf.sha256").write_text(binary_sha + "\n", encoding="utf-8")
        schedule_name = "point-progress-cursor-schedule.tsv" if cursor else "point-progress-ring-schedule.tsv"
        with (results / schedule_name).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t", lineterminator="\n")
            writer.writeheader()
            writer.writerows({**row, "batch_size": row["batch_size"] or "none"} for row in schedule)

        rows = []
        for row in schedule:
            run_id = progress_ring_runner._run_id(row)
            run_dir = results / run_id
            run_dir.mkdir()
            raw = cls._raw_record(row)
            metadata = {
                **row,
                "source_commit": commit,
                "binary_sha256": binary_sha,
                "exit_code": 0,
                "duration_seconds": 5.0,
                **progress_ring_runner._validate_record(raw, row),
            }
            (run_dir / "result.json").write_text(json.dumps(raw), encoding="utf-8")
            (run_dir / "run-metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
            (run_dir / "environment.txt").write_text(
                progress_ring_runner._expected_environment_text(
                    bool(row["progress_ring_enabled"]), bool(row.get("progress_cursor_enabled", False)),
                    row.get("progress_counters_enabled"),
                ),
                encoding="utf-8",
            )
            rows.append(metadata)
        summary = (
            progress_ring_runner._cursor_summary(rows, POINT_PROGRESS_RING_WRITERS, len(schedule))
            if cursor else progress_ring_runner._summary(rows, POINT_PROGRESS_RING_WRITERS, len(schedule))
        )
        summary.update({
            "source_commit": commit,
            "binary_sha256": binary_sha,
            "runs_attempted": len(schedule),
        })
        (results / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
        (results / "remote-exit.json").write_text(
            json.dumps({"failed": False, "runs_started": len(schedule), "runs_attempted": len(schedule)}),
            encoding="utf-8",
        )
        with (results / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
            fields = progress_ring_runner._summary_fields(schedule)
            writer = csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    key: json.dumps(value, sort_keys=True) if isinstance(value, dict) else value
                    for key, value in row.items()
                })
        return results

    def test_environment_uses_supported_ordered_data_submit_mode(self) -> None:
        for ring_enabled, cursor_enabled, counters_enabled, mode in (
            (False, False, None, "0"), (True, False, None, "1"), (True, True, False, "2"),
        ):
            environment = progress_ring_runner._environment(ring_enabled, cursor_enabled, counters_enabled)
            self.assertEqual(environment["TOYKV_BENCH_V8_DATA_SUBMIT_ORDER"], "ordered")
            self.assertEqual(environment["TOYKV_BENCH_V8_POINT_PROGRESS_RING"], mode)
            self.assertEqual(
                environment.get("TOYKV_BENCH_V8_PROGRESS_COUNTERS"),
                "0" if counters_enabled is False else None,
            )
        with self.assertRaisesRegex(ValueError, "requires progress-ring mode"):
            progress_ring_runner._environment(False, True)

    def test_cursor_fixture_uses_mode_two_and_keeps_f05_verifier_separate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results = self._make_fixture(Path(temporary), cursor=True)
            schedule = build_point_progress_cursor_schedule(POINT_PROGRESS_RING_WRITERS)
            self.assertEqual(len(schedule), 60)
            self.assertEqual(
                {row["arm"] for row in schedule},
                {"eager", "progress_ring", "cursor"},
            )
            self.assertTrue(progress_ring_runner.verify_cursor_results_directory(results)["verified"])
            self.assertFalse(progress_ring_runner.verify_results_directory(results)["verified"])
            candidate = next(row for row in schedule if row["arm"] == "cursor")
            environment = (results / progress_ring_runner._run_id(candidate) / "environment.txt").read_text(
                encoding="utf-8",
            )
            self.assertIn("TOYKV_BENCH_V8_POINT_PROGRESS_RING=2\n", environment)
            self.assertIn("TOYKV_BENCH_V8_PROGRESS_COUNTERS=0\n", environment)
            plan = json.loads((results / "benchmark-plan.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["suite"], "v8-point-progress-cursor")
            self.assertEqual(plan["arms"], ["eager", "progress_ring", "cursor"])
            self.assertEqual(plan["arm_orders"], [list(order) for order in POINT_PROGRESS_CURSOR_ORDERS])
            self.assertEqual(plan["runs_planned"], 60)
            self.assertEqual(
                plan["runtime_protocol"],
                "single 16-vCPU host; RAID0 over 2 Local SSDs; same source and binary for three arms",
            )
            self.assertEqual(plan["progress_cursor_ab"]["candidate_value"], "2")
            self.assertFalse(plan["progress_counters_enabled"])
            self.assertTrue(BENCH._verify_result_set(results)["verified"])

    def test_verifier_accepts_legacy_plan_label_only_with_ordered_run_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results = self._make_fixture(Path(temporary))
            plan_path = results / "benchmark-plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["data_submit_order"] = "ticket_prefix"
            plan_path.write_text(json.dumps(plan), encoding="utf-8")
            summary_path = results / "summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary.pop("runs_attempted")
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            remote_exit_path = results / "remote-exit.json"
            remote_exit = json.loads(remote_exit_path.read_text(encoding="utf-8"))
            remote_exit.pop("runs_attempted")
            remote_exit_path.write_text(json.dumps(remote_exit), encoding="utf-8")
            self.assertTrue(progress_ring_runner.verify_results_directory(results)["verified"])

            first = build_point_progress_ring_schedule(POINT_PROGRESS_RING_WRITERS)[0]
            run_id = progress_ring_runner._run_id(first)
            environment_path = results / run_id / "environment.txt"
            environment_path.write_text(
                environment_path.read_text(encoding="utf-8").replace(
                    "TOYKV_BENCH_V8_DATA_SUBMIT_ORDER=ordered",
                    "TOYKV_BENCH_V8_DATA_SUBMIT_ORDER=ready",
                ),
                encoding="utf-8",
            )
            verification = progress_ring_runner.verify_results_directory(results)
            self.assertFalse(verification["verified"])
            self.assertTrue(any("environment" in error for error in verification["errors"]))

    def test_schedule_and_cli_are_fixed_counterbalanced_and_note_local_screen_waiver(self) -> None:
        schedule = build_point_progress_ring_schedule(POINT_PROGRESS_RING_WRITERS)
        self.assertEqual(len(schedule), 40)
        self.assertEqual({row["workload"] for row in schedule}, {"point", "batch64"})
        for writer in POINT_PROGRESS_RING_WRITERS:
            for repetition in range(1, 6):
                arms = [row["arm"] for row in schedule
                        if row["writer"] == writer and row["workload"] == "point"
                        and row["repetition"] == repetition]
                self.assertEqual(arms, ["eager", "progress_ring"] if repetition % 2 else ["progress_ring", "eager"])
        plan = build_point_progress_ring_plan(
            writers=POINT_PROGRESS_RING_WRITERS,
            machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            commit="a" * 40,
        )
        self.assertEqual(plan["runs_planned"], 40)
        self.assertEqual(plan["data_submit_order"], "ordered")
        self.assertEqual(plan["local_screen"]["status"], "skipped_by_user")
        valid_args = [
            "--project", "test-project", "--zone", "us-east1-b", "--commit", "a" * 40,
            "--network", "walbench-isolated", "--subnet", "walbench-subnet",
            "--ssh-source-range", "192.0.2.10/32",
            "--max-runtime-minutes", "60", "--estimated-hourly-cost-usd", "1.5",
            "--max-estimated-spend-usd", "2",
            "--v8-point-progress-ring-ab", "--preflight-only",
        ]
        args = BENCH.parse_args(valid_args)
        self.assertEqual(args.writers, POINT_PROGRESS_RING_WRITERS)
        self.assertEqual(args.num, POINT_PROGRESS_RING_NUM)
        self.assertEqual(args.machine_type, "c4-standard-16-lssd")
        cursor_args = BENCH.parse_args([
            *[argument for argument in valid_args if argument != "--v8-point-progress-ring-ab"],
            "--v8-point-progress-cursor-ab",
        ])
        self.assertEqual(cursor_args.writers, POINT_PROGRESS_RING_WRITERS)
        self.assertEqual(cursor_args.num, POINT_PROGRESS_RING_NUM)
        self.assertEqual(cursor_args.machine_type, "c4-standard-16-lssd")
        stderr = StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit):
            BENCH.parse_args([*valid_args, "--writers", "16"])
        self.assertIn("requires writers (1, 8, 16)", stderr.getvalue())

    def test_progress_ring_requires_isolated_network_subnet_and_ssh_cidr(self) -> None:
        base = [
            "--project", "test-project", "--zone", "us-east1-b", "--commit", "a" * 40,
            "--max-runtime-minutes", "60", "--estimated-hourly-cost-usd", "1.5",
            "--max-estimated-spend-usd", "2",
            "--v8-point-progress-ring-ab", "--preflight-only",
        ]
        invalid_arguments = (
            base,
            [*base, "--network", "default", "--subnet", "default", "--ssh-source-range", "192.0.2.10/32"],
            [*base, "--network", "walbench-isolated", "--ssh-source-range", "192.0.2.10/32"],
            [*base, "--network", "walbench-isolated", "--subnet", "walbench-subnet"],
        )
        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit):
                BENCH.parse_args(arguments)

    def test_progress_cursor_requires_isolated_network_subnet_and_ssh_cidr(self) -> None:
        base = [
            "--project", "test-project", "--zone", "us-east1-b", "--commit", "a" * 40,
            "--max-runtime-minutes", "60", "--estimated-hourly-cost-usd", "1.5",
            "--max-estimated-spend-usd", "2",
            "--v8-point-progress-cursor-ab", "--preflight-only",
        ]
        invalid_arguments = (
            base,
            [*base, "--network", "default", "--subnet", "default", "--ssh-source-range", "192.0.2.10/32"],
            [*base, "--network", "walbench-isolated", "--ssh-source-range", "192.0.2.10/32"],
            [*base, "--network", "walbench-isolated", "--subnet", "walbench-subnet"],
        )
        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit):
                BENCH.parse_args(arguments)

    def test_progress_ring_preflight_audits_isolated_ingress(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = root / "preflight-1"
            audit_tags = []

            def audit_and_capture(*args: object) -> None:
                audit_tags.append(args[4])

            with (
                patch.object(BENCH, "Gcloud", return_value=Mock()),
                patch.object(BENCH, "preflight", return_value={"zone": "us-east1-b"}),
                patch.object(BENCH, "audit_ssh_firewall_rules", side_effect=audit_and_capture),
            ):
                for index in (1, 2):
                    results = root / f"preflight-{index}"
                    status = BENCH.main([
                        "--project", "test-project", "--zone", "us-east1-b", "--commit", "a" * 40,
                        "--network", "walbench-isolated", "--subnet", "walbench-subnet",
                        "--ssh-source-range", "192.0.2.10/32", "--skip-ssh-egress-check",
                        "--max-runtime-minutes", "60", "--estimated-hourly-cost-usd", "1.5",
                        "--max-estimated-spend-usd", "2", "--v8-point-progress-ring-ab",
                        "--preflight-only", "--results-dir", str(results),
                    ])
                    self.assertEqual(status, 0)
                    report = json.loads((results / "preflight.json").read_text(encoding="utf-8"))
                    self.assertEqual(report["isolated_network_audit"]["vm_tag"], audit_tags[-1])
                    self.assertTrue(audit_tags[-1].startswith(BENCH.POINT_PROGRESS_RING_VM_TAG + "-"))
            self.assertEqual(len(audit_tags), 2)
            self.assertNotEqual(audit_tags[0], audit_tags[1])

    def test_cursor_preflight_audits_isolated_ingress_and_records_cursor_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results = Path(temporary) / "preflight"
            audit_tags = []

            def audit_and_capture(*args: object) -> None:
                audit_tags.append(args[4])

            with (
                patch.object(BENCH, "Gcloud", return_value=Mock()),
                patch.object(BENCH, "preflight", return_value={"zone": "us-east1-b"}),
                patch.object(BENCH, "audit_ssh_firewall_rules", side_effect=audit_and_capture),
            ):
                status = BENCH.main([
                    "--project", "test-project", "--zone", "us-east1-b", "--commit", "a" * 40,
                    "--network", "walbench-isolated", "--subnet", "walbench-subnet",
                    "--ssh-source-range", "192.0.2.10/32", "--skip-ssh-egress-check",
                    "--max-runtime-minutes", "60", "--estimated-hourly-cost-usd", "1.5",
                    "--max-estimated-spend-usd", "2", "--v8-point-progress-cursor-ab",
                    "--preflight-only", "--results-dir", str(results),
                ])
            self.assertEqual(status, 0)
            self.assertEqual(len(audit_tags), 1)
            self.assertTrue(audit_tags[0].startswith(BENCH.POINT_PROGRESS_RING_VM_TAG + "-"))
            report = json.loads((results / "preflight.json").read_text(encoding="utf-8"))
            self.assertEqual(report["benchmark_suite"], "v8-point-progress-cursor")
            self.assertEqual(report["benchmark_plan"]["suite"], "v8-point-progress-cursor")
            self.assertEqual(report["benchmark_plan"]["progress_cursor_ab"]["candidate_value"], "2")

    def test_archive_round_trip_verifies_all_40_raw_runs_and_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = self._make_fixture(root)
            temp_root = root / "archive-tmp"
            temp_root.mkdir()
            command = BENCH._build_remote_archive_command(root=str(results), max_bytes=64 * 1024 * 1024)
            completed = subprocess.run(
                ["bash", "-euc", command], capture_output=True, text=True, check=False,
                env={**os.environ, "TMPDIR": str(temp_root)},
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            archive_values = self._archive_output(completed.stdout)
            archive = Path(archive_values["ARCHIVE_PATH"])
            archive_copy = root / "bundle.tar.gz"
            shutil.copyfile(archive, archive_copy)
            self.assertEqual(self._sha256(archive_copy), archive_values["ARCHIVE_SHA256"])
            extracted = root / "extracted"
            extracted.mkdir()
            with tarfile.open(archive_copy, "r:gz") as bundle:
                bundle.extractall(extracted)
            unpacked = extracted / "results"
            self.assertTrue((unpacked / "point-progress-ring-schedule.tsv").is_file())
            self.assertEqual(progress_ring_runner.verify_results_directory(unpacked)["verified"], True)

    def test_verifier_rejects_missing_extra_bad_samples_and_scores_not_matching_raw(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = self._make_fixture(root)
            self.assertTrue(progress_ring_runner.verify_results_directory(results)["verified"])

            first = build_point_progress_ring_schedule(POINT_PROGRESS_RING_WRITERS)[0]
            run_id = progress_ring_runner._run_id(first)
            (results / run_id / "run-metadata.json").unlink()
            rejected = progress_ring_runner.verify_results_directory(results)
            self.assertFalse(rejected["verified"])
            # Each independent corruption case starts with a fresh fixture.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = self._make_fixture(root)
            extra = results / "r99-w16-extra"
            extra.mkdir()
            self.assertFalse(progress_ring_runner.verify_results_directory(results)["verified"])

    def test_summary_uses_unrounded_ratios_for_inclusive_promotion_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results = self._make_fixture(Path(temporary))
            rows = json.loads((results / "summary.json").read_text(encoding="utf-8"))["results"]

        def score(point_ratio: float, p99_ratio: float, guardrail_ratio: float) -> dict[str, object]:
            adjusted = json.loads(json.dumps(rows))
            for row in adjusted:
                writer, workload, arm = row["writer"], row["workload"], row["arm"]
                baseline = arm == "eager"
                ratio = 1.0
                if workload == "point" and writer in (8, 16):
                    ratio = 1.0 if baseline else point_ratio
                    row["p99_ms"] = 1.0 if baseline else p99_ratio
                elif workload == "point" and writer == 1 or workload == "batch64":
                    ratio = 1.0 if baseline else guardrail_ratio
                row["puts_per_second"] = 100_000.0 * ratio
            return progress_ring_runner._summary(adjusted, POINT_PROGRESS_RING_WRITERS, len(adjusted))

        exact = score(1.05, 1.05, 0.98)
        self.assertTrue(exact["qualification_gates"]["point8_and_point16_each_gain_at_least_5pct_and_win_4_of_5"])
        self.assertTrue(exact["qualification_gates"]["point8_and_point16_p99_regression_at_most_5pct"])
        self.assertTrue(exact["qualification_gates"]["point1_and_batch64_paired_median_regression_at_most_2pct"])
        self.assertTrue(exact["qualification_gates"]["candidate_qualified"])

        throughput_outside = score(1.04999, 1.05, 0.98)
        self.assertFalse(
            throughput_outside["qualification_gates"][
                "point8_and_point16_each_gain_at_least_5pct_and_win_4_of_5"
            ]
        )
        self.assertFalse(throughput_outside["qualification_gates"]["candidate_qualified"])

        p99_outside = score(1.05, 1.05001, 0.98)
        self.assertTrue(
            p99_outside["qualification_gates"][
                "point8_and_point16_each_gain_at_least_5pct_and_win_4_of_5"
            ]
        )
        self.assertFalse(p99_outside["qualification_gates"]["point8_and_point16_p99_regression_at_most_5pct"])
        self.assertFalse(p99_outside["qualification_gates"]["candidate_qualified"])

        guardrail_outside = score(1.05, 1.05, 0.97999)
        self.assertFalse(
            guardrail_outside["qualification_gates"][
                "point1_and_batch64_paired_median_regression_at_most_2pct"
            ]
        )
        self.assertFalse(guardrail_outside["qualification_gates"]["candidate_qualified"])

    def test_malformed_nested_benchmark_objects_raise_value_error_and_verifier_survives(self) -> None:
        row = build_point_progress_ring_schedule(POINT_PROGRESS_RING_WRITERS)[0]
        malformed_records = []
        malformed_records.append(None)
        malformed_records.append({"result": None, "params": {}})
        malformed_records.append({"result": {}, "params": []})
        malformed_records.append({"result": {"wal_pipeline": None}, "params": {}})
        malformed_records.append({
            "result": {"wal_pipeline": {"wal_io_start": None}}, "params": {},
        })
        for record in malformed_records:
            with self.subTest(record=record), self.assertRaises(ValueError):
                progress_ring_runner._validate_record(record, row)

        with tempfile.TemporaryDirectory() as temporary:
            results = self._make_fixture(Path(temporary))
            raw_path = results / progress_ring_runner._run_id(row) / "result.json"
            raw = json.loads(raw_path.read_text(encoding="utf-8"))
            raw["result"]["wal_pipeline"] = None
            raw_path.write_text(json.dumps(raw), encoding="utf-8")
            verification = progress_ring_runner.verify_results_directory(results)
            self.assertFalse(verification["verified"])
            self.assertTrue(any("WAL pipeline metadata" in error for error in verification["errors"]))

    def test_verifier_skips_score_recomputation_for_invalid_matrix_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results = self._make_fixture(Path(temporary))
            summary_path = results / "summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            point = next(row for row in summary["results"] if row["workload"] == "point")
            point["p99_ms"] = 0
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            verification = progress_ring_runner.verify_results_directory(results)
            self.assertFalse(verification["verified"])
            self.assertTrue(any("run rows" in error for error in verification["errors"]))

    def test_progress_ring_qualification_rejects_cursor_or_progress_diagnostics(self) -> None:
        row = build_point_progress_ring_schedule(POINT_PROGRESS_RING_WRITERS)[0]
        for field, value, error in (
            ("progress_cursor_enabled", True, "Cursor mode differs from the planned arm"),
            ("progress_diagnostics", {"owner_scans": 0}, "diagnostics must be disabled"),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, error):
                raw = self._raw_record(row)
                raw["result"]["wal_pipeline"]["wal_io_start"][field] = value
                progress_ring_runner._validate_record(raw, row)

    def test_matrix_validator_rejects_boolean_exit_status(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results = self._make_fixture(Path(temporary))
            summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
        summary["results"][0]["exit_code"] = False
        validation = validate_point_progress_ring_rows(summary["results"], POINT_PROGRESS_RING_WRITERS)
        self.assertFalse(validation["complete"])

    def test_run_suite_uses_resolved_binary_and_reports_launches_separately(self) -> None:
        binary = Path("scripts/gcp_v8_point_progress_ring_runner.py")
        binary_sha256 = self._sha256(binary)
        commit = "a" * 40
        seen_binary_paths: list[Path] = []

        def fake_run_one(**kwargs: object) -> dict[str, object]:
            row = kwargs["row"]
            seen_binary_paths.append(kwargs["binary"])
            kwargs["on_subprocess_started"]()
            raw = self._raw_record(row)
            if row["arm"] == "progress_ring" and row["writer"] in (8, 16) and row["workload"] == "point":
                raw["result"]["ops_per_sec"] = 101_000.0
            return {
                **row,
                "source_commit": commit,
                "binary_sha256": binary_sha256,
                "exit_code": 0,
                "duration_seconds": 5.0,
                **progress_ring_runner._validate_record(raw, row),
            }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(
                commit=commit,
                binary=binary,
                binary_sha256=binary_sha256,
                machine_type="c4-standard-16-lssd",
                ssd_count=2,
                ssd_gib=750,
                writers=",".join(map(str, POINT_PROGRESS_RING_WRITERS)),
                results_dir=root / "results",
                db_root=root / "db",
                safe_deadline_epoch=4_000_000_000,
            )
            with patch.object(progress_ring_runner, "_run_one", side_effect=fake_run_one):
                status = progress_ring_runner.run_suite(args)

            self.assertEqual(status, 0, "complete matrices return success even when a promotion gate fails")
            self.assertTrue(all(path == binary.resolve(strict=True) for path in seen_binary_paths))
            summary = json.loads((args.results_dir / "summary.json").read_text(encoding="utf-8"))
            remote_exit = json.loads((args.results_dir / "remote-exit.json").read_text(encoding="utf-8"))
            self.assertTrue(summary["complete"])
            self.assertFalse(summary["qualification_gates"]["candidate_qualified"])
            self.assertEqual(summary["runs_started"], 40)
            self.assertEqual(summary["runs_attempted"], 40)
            self.assertEqual(remote_exit["runs_started"], 40)
            self.assertEqual(remote_exit["runs_attempted"], 40)
            self.assertFalse(remote_exit["failed"])

    def test_run_suite_writes_separate_cursor_plan_and_schedule(self) -> None:
        binary = Path("scripts/gcp_v8_point_progress_ring_runner.py")
        binary_sha256 = self._sha256(binary)
        commit = "a" * 40

        def fake_run_one(**kwargs: object) -> dict[str, object]:
            row = kwargs["row"]
            kwargs["on_subprocess_started"]()
            raw = self._raw_record(row)
            return {
                **row,
                "source_commit": commit,
                "binary_sha256": binary_sha256,
                "exit_code": 0,
                "duration_seconds": 5.0,
                **progress_ring_runner._validate_record(raw, row),
            }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(
                commit=commit,
                binary=binary,
                binary_sha256=binary_sha256,
                machine_type="c4-standard-16-lssd",
                ssd_count=2,
                ssd_gib=750,
                writers=",".join(map(str, POINT_PROGRESS_RING_WRITERS)),
                results_dir=root / "results",
                db_root=root / "db",
                safe_deadline_epoch=4_000_000_000,
                candidate_mode="cursor",
            )
            with patch.object(progress_ring_runner, "_run_one", side_effect=fake_run_one):
                status = progress_ring_runner.run_suite(args)
            self.assertEqual(status, 0)
            self.assertTrue((args.results_dir / "point-progress-cursor-schedule.tsv").is_file())
            self.assertFalse((args.results_dir / "point-progress-ring-schedule.tsv").exists())
            plan = json.loads((args.results_dir / "benchmark-plan.json").read_text(encoding="utf-8"))
            summary = json.loads((args.results_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["suite"], "v8-point-progress-cursor")
            self.assertEqual(summary["suite"], "v8-point-progress-cursor")
            self.assertEqual(summary["arms"], ["eager", "progress_ring", "cursor"])
            self.assertEqual(summary["runs_planned"], 60)
            self.assertEqual(summary["runs_started"], 60)
            self.assertTrue(summary["complete"])

    def test_launch_count_does_not_include_deadline_rejected_attempt(self) -> None:
        binary = Path("scripts/gcp_v8_point_progress_ring_runner.py")
        binary_sha256 = self._sha256(binary)
        commit = "a" * 40
        started = 0

        def fake_run_one(**kwargs: object) -> dict[str, object]:
            nonlocal started
            if started == 12:
                raise TimeoutError("stopping before the VM safety deadline")
            kwargs["on_subprocess_started"]()
            started += 1
            return {
                **kwargs["row"], "source_commit": commit, "binary_sha256": binary_sha256,
                "exit_code": 0, "puts_per_second": 1.0, "cpu_us_per_put": 1.0,
                "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0,
                "sync_count": 1, "puts_per_sync": 1.0, "ordered_link_hash_cpu_ns": 0,
            }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(
                commit=commit, binary=binary, binary_sha256=binary_sha256,
                machine_type="c4-standard-16-lssd", ssd_count=2, ssd_gib=750,
                writers=",".join(map(str, POINT_PROGRESS_RING_WRITERS)),
                results_dir=root / "results", db_root=root / "db", safe_deadline_epoch=4_000_000_000,
            )
            with patch.object(progress_ring_runner, "_run_one", side_effect=fake_run_one):
                status = progress_ring_runner.run_suite(args)
            self.assertEqual(status, 1)
            summary = json.loads((args.results_dir / "summary.json").read_text(encoding="utf-8"))
            remote_exit = json.loads((args.results_dir / "remote-exit.json").read_text(encoding="utf-8"))
            self.assertFalse(summary["complete"])
            self.assertEqual(summary["runs_started"], 12)
            self.assertEqual(summary["runs_attempted"], 13)
            self.assertEqual(remote_exit["runs_started"], 12)
            self.assertEqual(remote_exit["runs_attempted"], 13)
            self.assertTrue(remote_exit["failed"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = self._make_fixture(root)
            point = next(row for row in build_point_progress_ring_schedule(POINT_PROGRESS_RING_WRITERS)
                         if row["workload"] == "point")
            raw_path = results / progress_ring_runner._run_id(point) / "result.json"
            raw = json.loads(raw_path.read_text(encoding="utf-8"))
            raw["result"]["wal_pipeline"]["worker_group_sizes"] = {"one_ticket": 1}
            raw_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertFalse(progress_ring_runner.verify_results_directory(results)["verified"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = self._make_fixture(root)
            point = next(row for row in build_point_progress_ring_schedule(POINT_PROGRESS_RING_WRITERS)
                         if row["workload"] == "point")
            raw_path = results / progress_ring_runner._run_id(point) / "result.json"
            raw = json.loads(raw_path.read_text(encoding="utf-8"))
            raw["latency"]["samples"] -= 1
            raw_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertFalse(progress_ring_runner.verify_results_directory(results)["verified"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = self._make_fixture(root)
            schedule = build_point_progress_ring_schedule(POINT_PROGRESS_RING_WRITERS)
            rows = json.loads((results / "summary.json").read_text(encoding="utf-8"))["results"]
            first = rows[0]
            run_id = progress_ring_runner._run_id(first)
            first["puts_per_second"] *= 2
            (results / run_id / "run-metadata.json").write_text(json.dumps(first), encoding="utf-8")
            summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
            summary["results"] = rows
            recomputed = progress_ring_runner._summary(rows, POINT_PROGRESS_RING_WRITERS, len(schedule))
            summary.update({key: recomputed[key] for key in (
                "complete", "runs_started", "runs_completed", "runs_planned", "writers", "workloads",
                "arms", "matrix_validation", "comparison", "comparison_by_cell", "qualification_gates",
            )})
            (results / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
            summary_tsv = results / "summary.tsv"
            with summary_tsv.open(encoding="utf-8", newline="") as stream:
                tsv_rows = list(csv.DictReader(stream, delimiter="\t"))
                fieldnames = tuple(tsv_rows[0])
            tsv_row = next(row for row in tsv_rows if row["run_index"] == str(first["run_index"]))
            tsv_row["puts_per_second"] = str(first["puts_per_second"])
            with summary_tsv.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
                writer.writeheader()
                writer.writerows(tsv_rows)
            self.assertFalse(progress_ring_runner.verify_results_directory(results)["verified"])

    @staticmethod
    def _archive_output(output: str) -> dict[str, str]:
        return dict(line.split("=", 1) for line in output.splitlines() if "=" in line)

    @staticmethod
    def _sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()


class GcpFreeTrialWalBenchTests(unittest.TestCase):
    def test_ssh_readiness_retries_within_a_fixed_deadline(self) -> None:
        now = [0.0]
        timeouts = []

        def monotonic() -> float:
            return now[0]

        def sleep(seconds: float) -> None:
            now[0] += seconds

        def probe(timeout: int) -> subprocess.CompletedProcess[str]:
            timeouts.append(timeout)
            return subprocess.CompletedProcess([], 255, "", "timed out during banner exchange")

        with (
            patch.object(BENCH.time, "monotonic", side_effect=monotonic),
            patch.object(BENCH.time, "sleep", side_effect=sleep) as sleep_mock,
            self.assertRaisesRegex(
                BENCH.BenchError,
                "did not become ready within 21 seconds.*banner exchange",
            ),
        ):
            BENCH.wait_for_ssh_ready(probe, timeout_seconds=21, poll_interval_seconds=10)

        self.assertEqual(timeouts, [12, 11, 1])
        self.assertEqual(sleep_mock.call_args_list, [unittest.mock.call(10), unittest.mock.call(10), unittest.mock.call(1)])

    def test_ssh_readiness_returns_when_a_retry_succeeds(self) -> None:
        now = [0.0]
        attempts = [
            subprocess.CompletedProcess([], 255, "", "connection closed"),
            subprocess.CompletedProcess([], 0, "", ""),
        ]

        def monotonic() -> float:
            return now[0]

        def sleep(seconds: float) -> None:
            now[0] += seconds

        def probe(_timeout: int) -> subprocess.CompletedProcess[str]:
            return attempts.pop(0)

        with (
            patch.object(BENCH.time, "monotonic", side_effect=monotonic),
            patch.object(BENCH.time, "sleep", side_effect=sleep) as sleep_mock,
        ):
            BENCH.wait_for_ssh_ready(probe, timeout_seconds=30, poll_interval_seconds=5)

        self.assertEqual(now[0], 5)
        sleep_mock.assert_called_once_with(5)

    def test_stale_ssh_source_stops_before_cloud_preflight(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temp,
            patch.object(BENCH.urllib.request, "urlopen", return_value=BytesIO(b"8.8.8.8")),
            patch.object(BENCH, "preflight") as preflight,
        ):
            results = Path(temp) / "results"
            code = BENCH.main([
                "--project", "project", "--zone", "us-central1-a", "--commit", "a" * 40,
                "--ssh-source-range", "9.9.9.9/32", "--preflight-only",
                "--results-dir", str(results),
            ])
            self.assertEqual(code, 2)
            preflight.assert_not_called()
            failure = json.loads((results / "preflight-failure.json").read_text())
            self.assertIn("outside --ssh-source-range", failure["error"])
            self.assertFalse(failure["vm_created"])

    def test_auto_ssh_source_range_resolves_current_public_ipv4_as_host_rule(self) -> None:
        with patch.object(BENCH.urllib.request, "urlopen", return_value=BytesIO(b"8.8.8.8\n")) as lookup:
            args = BENCH.parse_args([
                "--project", "project", "--zone", "us-central1-a", "--commit", "a" * 40,
                "--ssh-source-range", "auto", "--preflight-only",
            ])

        self.assertEqual(args.ssh_source_range, "8.8.8.8/32")
        lookup.assert_called_once()
        self.assertEqual(lookup.call_args.args[0].full_url, BENCH.PUBLIC_IPV4_LOOKUP_URL)
        self.assertEqual(lookup.call_args.kwargs["timeout"], 10)

    def test_auto_ssh_source_range_rejects_non_global_or_non_ipv4_response(self) -> None:
        for response in (b"2001:4860:4860::8888", b"192.168.1.2", b"224.0.0.1", b"not-an-ip"):
            with self.subTest(response=response), patch.object(
                BENCH.urllib.request, "urlopen", return_value=BytesIO(response),
            ), self.assertRaises(BENCH.BenchError):
                BENCH.resolve_ssh_source_range("auto")

    def test_auto_ssh_source_range_wraps_incomplete_lookup_response(self) -> None:
        class IncompleteReadResponse(BytesIO):
            def read(self, _size: int = -1) -> bytes:
                raise BENCH.http.client.IncompleteRead(b"8.8.")

        with patch.object(
            BENCH.urllib.request, "urlopen", return_value=IncompleteReadResponse(),
        ), self.assertRaisesRegex(BENCH.BenchError, "no cloud resources were created"):
            BENCH.resolve_ssh_source_range("auto")

    def test_auto_ssh_source_range_lookup_failure_fails_closed(self) -> None:
        with patch.object(BENCH.urllib.request, "urlopen", side_effect=TimeoutError("lookup timed out")):
            with self.assertRaisesRegex(BENCH.BenchError, "no cloud resources were created"):
                BENCH.resolve_ssh_source_range("auto")

    def test_point_borrowed_summary_columns_cover_run_metadata(self) -> None:
        schedule_row = build_point_borrowed_input_schedule((1, 8, 16))[0]
        metadata = {
            **schedule_row,
            "source_commit": "candidate",
            "binary_sha256": "binary",
            "exit_code": 0,
            "ops": 100_000,
            "puts_per_second": 1.0,
            "cpu_seconds": 1.0,
            "cpu_us_per_put": 10.0,
            "p50_ms": 1.0,
            "p95_ms": 2.0,
            "p99_ms": 3.0,
            "sync_count": 1,
            "puts_per_sync": 100_000,
            "initialized_bytes": 0,
            "preparation_wait_ms": 0.0,
            "duration_seconds": 1.0,
        }
        fields = _summary_fields(schedule_row)
        self.assertFalse(metadata.keys() - set(fields))
        writer = csv.DictWriter(StringIO(), fieldnames=fields, delimiter="\t")
        writer.writerow(metadata)

    def _make_result_bundle(
        self, root: Path, *, complete: bool, include_run_metadata: bool = True,
    ) -> Path:
        # Match the VM layout so archive paths are rooted at "results/".
        results = root / "results"
        results.mkdir(parents=True)
        plan = build_benchmark_plan(
            full_matrix=False, writers=(16,), machine_type=BENCH.MACHINE_TYPE,
            machine_spec=BENCH.MACHINE_SPECS[BENCH.MACHINE_TYPE], num=64,
        )
        (results / "benchmark-plan.json").write_text(json.dumps(plan), encoding="utf-8")
        (results / "rfc026-schedule.tsv").write_text("run_index\n", encoding="utf-8")
        (results / "summary.tsv").write_text("writer\tblock\tposition\tformat\texit_code\n", encoding="utf-8")
        schedule = build_batch64_schedule((16,))
        rows = []
        for run in schedule:
            run_id = BENCH._run_identifier(run, "batch64")
            run_dir = results / run_id
            run_dir.mkdir()
            exit_code = 0 if complete or run != schedule[-1] else 1
            row = {
                "writer": run["writer"], "block": run["block"], "position": run["position"],
                "format": run["arm"], "exit_code": exit_code,
            }
            rows.append(row)
            (run_dir / "result.json").write_text(json.dumps({"result": {"ops_per_sec": 123}}), encoding="utf-8")
            if include_run_metadata:
                (run_dir / "run-metadata.json").write_text(json.dumps(row), encoding="utf-8")
            (run_dir / "stderr.log").write_text("", encoding="utf-8")
            (run_dir / "timeline.json").write_text("{}\n", encoding="utf-8")
        (results / "summary.json").write_text(json.dumps({
            "complete": complete, "runs_planned": len(schedule), "runs_completed": len(schedule),
            "matrix_validation": {
                "complete": complete, "planned": len(schedule), "observed": len(schedule),
            },
            "results": [{key: str(value) for key, value in row.items()} for row in rows],
        }), encoding="utf-8")
        (results / "remote-exit.json").write_text(json.dumps({
            "failed": not complete, "runs_started": len(schedule),
        }), encoding="utf-8")
        (results / "profile-schedule.tsv").write_text("writer\tworkload\n", encoding="utf-8")
        (results / "rust-toolchain.txt").write_text("nightly\n", encoding="utf-8")
        return results

    def _run_remote_bundle(self, command: str, results_root: Path, temp_root: Path) -> subprocess.CompletedProcess[str]:
        remote_root = "GCP_WAL_RESULTS_ROOT=/mnt/gcp-wal-bench/results"
        command = command.replace(remote_root, f"GCP_WAL_RESULTS_ROOT={BENCH.shlex.quote(str(results_root))}")
        bundle_tmp = temp_root / "remote-tmp"
        bundle_tmp.mkdir(exist_ok=True)
        environment = {**os.environ, "TMPDIR": str(bundle_tmp)}
        return subprocess.run(
            ["bash", "-euc", command], capture_output=True, text=True, check=False, env=environment,
        )

    @staticmethod
    def _archive_metadata(output: str) -> dict[str, str]:
        return dict(line.split("=", 1) for line in output.splitlines() if "=" in line)

    def test_benchmark_record_loader_requires_exactly_one_valid_json_record(self) -> None:
        valid = json.dumps({"result": {}, "params": {}, "latency": {}})
        with tempfile.TemporaryDirectory() as temp:
            result_path = Path(temp) / "result.json"
            for contents, expected_count in (("diagnostic only\n", 0), (f"{valid}\n{valid}\n", 2)):
                result_path.write_text(contents, encoding="utf-8")
                with self.subTest(expected_count=expected_count), self.assertRaisesRegex(
                    ValueError, f"found {expected_count}"
                ):
                    _load_benchmark_record(result_path)

            result_path.write_text(f"{valid}\n[hotpath] details\n", encoding="utf-8")
            self.assertEqual(_load_benchmark_record(result_path), {
                "result": {}, "params": {}, "latency": {},
            })

    def test_v8_io_metadata_requires_stable_active_inode_and_reports_modes(self) -> None:
        handles = {
            name: {"o_direct": name == "data", "device": 7, "inode": 11}
            for name in ("data", "frontier", "sync")
        }
        pipeline = {"wal_io_start": handles, "wal_io_end": handles, "wal_io_stable": True}
        self.assertEqual(_validate_wal_io_metadata(pipeline, 8), {
            "wal_data_o_direct": True,
            "wal_frontier_o_direct": False,
            "wal_sync_o_direct": False,
            "wal_io_device": 7,
            "wal_io_inode": 11,
        })
        evolving_extent_metadata = {
            "wal_io_start": {
                **handles,
                "sliding_extent_preparation": {"fallocate_calls": 0, "allocated_bytes": 0},
            },
            "wal_io_end": {
                **handles,
                "sliding_extent_preparation": {"fallocate_calls": 104, "allocated_bytes": 436207616},
            },
            "wal_io_stable": True,
        }
        self.assertEqual(_validate_wal_io_metadata(evolving_extent_metadata, 8), {
            "wal_data_o_direct": True,
            "wal_frontier_o_direct": False,
            "wal_sync_o_direct": False,
            "wal_io_device": 7,
            "wal_io_inode": 11,
        })
        with self.assertRaisesRegex(ValueError, "expected V8 FRONTIER O_DIRECT=True"):
            _validate_wal_io_metadata(pipeline, 8, "direct")

        direct_handles = {
            **handles,
            "frontier": {**handles["frontier"], "o_direct": True},
        }
        direct_pipeline = {
            "wal_io_start": direct_handles,
            "wal_io_end": direct_handles,
            "wal_io_stable": True,
        }
        self.assertTrue(_validate_wal_io_metadata(direct_pipeline, 8, "direct")["wal_frontier_o_direct"])
        self.assertEqual(_validate_wal_io_metadata({}, 7), {})
        self.assertEqual(_expected_frontier_mode_for_arm("v8_parallel", "direct"), "direct")
        self.assertEqual(_expected_frontier_mode_for_arm("v8_leader", "direct"), "buffered")
        leader_handles = {
            **handles,
            "data": {**handles["data"], "o_direct": False},
        }
        leader_pipeline = {
            "wal_io_start": leader_handles,
            "wal_io_end": leader_handles,
            "wal_io_stable": True,
        }
        self.assertFalse(
            _validate_wal_io_metadata(
                leader_pipeline,
                8,
                expected_data_o_direct=False,
            )["wal_data_o_direct"]
        )

        buffered_data = {
            **handles,
            "data": {**handles["data"], "o_direct": False},
        }
        direct_sync = {
            **handles,
            "sync": {**handles["sync"], "o_direct": True},
        }
        for invalid_handles, message in (
            (buffered_data, "expected V8 DATA O_DIRECT=True"),
            (direct_sync, "expected V8 sync O_DIRECT=False"),
        ):
            invalid = {
                "wal_io_start": invalid_handles,
                "wal_io_end": invalid_handles,
                "wal_io_stable": True,
            }
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                _validate_wal_io_metadata(invalid, 8)

        changed_inode = {**handles, "frontier": {**handles["frontier"], "inode": 12}}
        invalid_pipelines = (
            {**pipeline, "wal_io_stable": False},
            {**pipeline, "wal_io_end": changed_inode},
        )
        for invalid in invalid_pipelines:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                _validate_wal_io_metadata(invalid, 8)

    def test_writer_defaults_are_suite_specific_and_duplicates_are_rejected(self) -> None:
        common = ["--project", "project", "--zone", "us-central1-a", "--commit", "a" * 40, "--preflight-only"]
        legacy = BENCH.parse_args(common)
        self.assertEqual(legacy.num, 750_000)
        full = BENCH.parse_args([*common, "--full-rfc026-matrix"])
        self.assertEqual(legacy.writers, LEGACY_DEFAULT_WRITERS)
        self.assertEqual(full.writers, FULL_DEFAULT_WRITERS)
        explicit = BENCH.parse_args([*common, "--writers", "2", "16"])
        self.assertEqual(explicit.writers, (2, 16))
        profile = BENCH.parse_args([*common, "--v8-small-ticket-profile"])
        self.assertEqual(profile.writers, (1, 8, 16))
        retained_profile = BENCH.parse_args([
            *common, "--v8-small-ticket-profile", "--keep-vm-after-run",
        ])
        self.assertTrue(retained_profile.keep_vm_after_run)
        profile_subset = BENCH.parse_args([*common, "--v8-small-ticket-profile", "--writers", "8"])
        self.assertEqual(profile_subset.writers, (8,))
        trace_args = [
            *common, "--commit-order-trace", "--num", "100000", "--source-patch-sha256", "a" * 64,
        ]
        trace = BENCH.parse_args(trace_args)
        self.assertEqual(trace.num, COMMIT_ORDER_TRACE_NUM)
        self.assertEqual(trace.writers, (16,))
        self.assertEqual(trace.machine_type, "c4-standard-16-lssd")
        trace_default = BENCH.parse_args([
            *common, "--commit-order-trace", "--source-patch-sha256", "a" * 64,
        ])
        self.assertEqual(trace_default.num, COMMIT_ORDER_TRACE_NUM)
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*trace_args, "--writers", "8"])
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--commit-order-trace", "--num", "100000"])
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--writers", "2", "2"])
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--v8-small-ticket-profile", "--full-rfc026-matrix"])

        point_lookahead = BENCH.parse_args([*common, "--v8-point-lookahead-ab"])
        self.assertEqual(point_lookahead.writers, (POINT_LOOKAHEAD_WRITERS,))
        self.assertEqual(point_lookahead.machine_type, "c4-standard-16-lssd")
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--v8-point-lookahead-ab", "--writers", "8"])

        mutex_diagnostic = BENCH.parse_args([*common, "--v8-mutex-diagnostic"])
        self.assertEqual(mutex_diagnostic.num, V8_MUTEX_DIAGNOSTIC_NUM)
        self.assertEqual(mutex_diagnostic.writers, V8_MUTEX_DIAGNOSTIC_WRITERS)
        self.assertEqual(mutex_diagnostic.machine_type, "c4-standard-16-lssd")
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--v8-mutex-diagnostic", "--writers", "8"])
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--v8-mutex-diagnostic", "--num", "750000"])

        cpu_attribution = BENCH.parse_args([*common, "--v8-point-cpu-attribution"])
        self.assertEqual(cpu_attribution.num, V8_POINT_CPU_ATTRIBUTION_NUM)
        self.assertEqual(cpu_attribution.writers, V8_POINT_CPU_ATTRIBUTION_WRITERS)
        self.assertEqual(cpu_attribution.machine_type, "c4-standard-16-lssd")
        cpu_attribution_subset = BENCH.parse_args([
            *common, "--v8-point-cpu-attribution", "--writers", "8", "16",
        ])
        self.assertEqual(cpu_attribution_subset.writers, (8, 16))
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--v8-point-cpu-attribution", "--writers", "2"])
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--v8-point-cpu-attribution", "--num", "750000"])

    def test_v8_point_cpu_attribution_plan_and_commands_are_fixed_and_counterbalanced(self) -> None:
        schedule = build_v8_point_cpu_attribution_schedule()
        self.assertEqual(len(schedule), 8)
        self.assertEqual({row["writer"] for row in schedule}, set(V8_POINT_CPU_ATTRIBUTION_WRITERS))
        self.assertEqual({row["num"] for row in schedule}, {V8_POINT_CPU_ATTRIBUTION_NUM})
        self.assertEqual({row["sample_every"] for row in schedule}, {V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY})
        for index, writer in enumerate(V8_POINT_CPU_ATTRIBUTION_WRITERS):
            pair = [row["arm"] for row in schedule if row["writer"] == writer]
            expected = (
                ["v7_parallel", "v8_point_lookahead_direct"] if index % 2 == 0 else
                ["v8_point_lookahead_direct", "v7_parallel"]
            )
            self.assertEqual(pair, expected)
            for arm in pair:
                row = next(row for row in schedule if row["writer"] == writer and row["arm"] == arm)
                command = build_cpu_attribution_command(Path("write-perf"), row, Path("dbs"))
                self.assertIn("--wal-cpu-attribution-sample-every", command)
                self.assertIn(str(V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY), command)
                self.assertIn("--wal-sync-diagnostics", command)
                self.assertIn(f"--threads", command)
                self.assertIn(str(writer), command)
                self.assertIn("--wal-completion-timeline", command)

        plan = build_v8_point_cpu_attribution_plan(
            machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            commit="a" * 40,
            binary_sha256="b" * 64,
        )
        self.assertEqual(plan["runs_planned"], 8)
        self.assertTrue(plan["diagnostic_only"])
        self.assertEqual(plan["v8_configuration"]["frontier_mode"], "direct")

        rows = [{
            "writer": row["writer"], "workload": row["workload"], "repetition": row["repetition"],
            "arm": row["arm"], "ops": row["num"], "sample_every": row["sample_every"],
            "exit_code": 0, "wal_cpu_attribution_valid": True,
        } for row in schedule]
        self.assertTrue(validate_v8_point_cpu_attribution_rows(rows)["complete"])
        self.assertFalse(validate_v8_point_cpu_attribution_rows(rows[:-1])["complete"])

        repeated_schedule = build_v8_point_cpu_attribution_schedule((8, 16), repetitions=3)
        self.assertEqual(len(repeated_schedule), 12)
        self.assertEqual({row["repetition"] for row in repeated_schedule}, {1, 2, 3})
        self.assertEqual(len({cpu_attribution_run_id(row) for row in repeated_schedule}), 12)
        for row in repeated_schedule:
            expected_run_id = f"r{row['repetition']}-w{row['writer']}-point-{row['arm']}"
            self.assertEqual(BENCH._run_identifier(row, "v8-point-cpu-attribution"), expected_run_id)
            self.assertEqual(BENCH._summary_row_identifier(row, "v8-point-cpu-attribution"), expected_run_id)
        repeated_rows = [{
            "writer": row["writer"], "workload": row["workload"], "repetition": row["repetition"],
            "arm": row["arm"], "ops": row["num"], "sample_every": row["sample_every"],
            "exit_code": 0, "wal_cpu_attribution_valid": True,
        } for row in repeated_schedule]
        self.assertTrue(validate_v8_point_cpu_attribution_rows(repeated_rows, (8, 16), 3)["complete"])
        repeated_plan = build_v8_point_cpu_attribution_plan(
            machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            writers=(8, 16),
            repetitions=3,
        )
        self.assertEqual(repeated_plan["runs_planned"], 12)
        with self.assertRaises(ValueError):
            build_v8_point_cpu_attribution_schedule((8, 16), repetitions=0)

    def test_v8_durability_phase_validation_matches_serialized_schema(self) -> None:
        # Rust phase records are emitted only after completed advances and use integer timing/count fields.
        phase = {field: 10 for field in cpu_attribution_runner.V8_DURABILITY_FIELDS}
        diagnostics = {
            "sample_every": V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
            "sampled_advances": 1,
            "dropped_samples": 0,
            "phases": [phase],
        }
        _validate_v8_durability_phase_samples(diagnostics)

        diagnostics["phases"] = ["not a phase record"]
        with self.assertRaisesRegex(ValueError, "missing or incomplete"):
            _validate_v8_durability_phase_samples(diagnostics)

        diagnostics["phases"] = [{key: value for key, value in phase.items() if key != "driver_cpu_ns"}]
        with self.assertRaisesRegex(ValueError, "missing or invalid summary field"):
            _validate_v8_durability_phase_samples(diagnostics)

        diagnostics["phases"] = [{**phase, "driver_cpu_ns": True}]
        with self.assertRaisesRegex(ValueError, "missing or invalid summary field"):
            _validate_v8_durability_phase_samples(diagnostics)

        diagnostics["phases"] = [{**phase, "succeeded": False}]
        with self.assertRaisesRegex(ValueError, "reports a failed advance"):
            _validate_v8_durability_phase_samples(diagnostics)

    def test_sync_decision_validation_requires_complete_prefix_snapshot(self) -> None:
        observation = {
            "captured_target": 11,
            "decision_ticket_start": 10,
            "sync_decision_monotonic_ns": 123456,
            "written_frontier_start": 10,
            "written_frontier_end": 11,
            "admitted_ticket_count": 3,
            "frames_ready_prefix_tickets": 3,
            "data_complete_prefix_tickets": 1,
            "durability_ready_prefix_tickets": 1,
            "data_hol_blocker_ticket": 11,
            "data_hol_blocker_frames_ready": True,
            "data_hol_blocker_digest_folded": None,
            "data_hol_blocker_data_write_elapsed_ns": None,
            "next_ticket_present": True,
            "next_ticket_frames_ready": True,
            "next_ticket_data_complete": False,
            "succeeded": True,
        }
        summary = cpu_attribution_runner._sync_decision_summary([observation])
        self.assertEqual(summary["count"], 1)
        self.assertEqual(summary["later_ticket_present_count"], 1)
        self.assertEqual(summary["later_ticket_frames_ready_count"], 1)
        self.assertEqual(summary["later_ticket_data_complete_count"], 0)

        for invalid in (None, [], ["bad"], [{**observation, "decision_ticket_start": None}],
                        [{**observation, "frames_ready_prefix_tickets": None}],
                        [{**observation, "durability_ready_prefix_tickets": 2}],
                        [{**observation, "next_ticket_present": False}],
                        [{**observation, "succeeded": False}]):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                cpu_attribution_runner._sync_decision_summary(invalid)

    def test_v8_completion_timeline_validation_and_summary(self) -> None:
        timeline = {
            "sample_every": cpu_attribution_runner.V8_COMPLETION_TIMELINE_SAMPLE_EVERY,
            "dropped_samples": 0,
            "samples": [{
                "group_id": 7,
                "ticket_start": 106,
                "ticket_end": 108,
                "request_id": 128,
                "request_start": 128,
                "request_end": 130,
                "submit_to_cqe_observed_ns": 1_000,
                "cqe_observed_to_group_finished_ns": 200,
                "group_finished_to_result_send_start_ns": 100,
                "result_send_start_to_reaper_recv_ns": 300,
                "reaper_recv_to_data_complete_publish_ns": 400,
                "data_complete_lock_wait_wall_ns": 20,
                "data_complete_lock_wait_cpu_ns": 5,
                "data_complete_lock_hold_wall_ns": 60,
                "data_complete_lock_hold_cpu_ns": 40,
                "data_complete_prefix_end_before": 100,
                "data_complete_prefix_end_after": 100,
                "frame_ready_monotonic_ns": 10,
                "sqe_submitted_monotonic_ns": 20,
                "cqe_observed_monotonic_ns": 30,
                "data_complete_published_monotonic_ns": 40,
                "hol_blocker_ticket": 100,
                "hol_blocker_frames_ready": True,
                "hol_blocker_digest_folded": True,
                "hol_blocker_data_write_elapsed_ns": 2_000,
                "reaper_recv_to_prefix_ready_ns": 600,
                "prefix_ready_to_durable_ns": 3_000,
            }],
        }
        summary = _completion_timeline_summary(timeline)
        self.assertEqual(summary["sample_count"], 1)
        self.assertEqual(summary["hol_blocker_count"], 1)
        self.assertEqual(summary["hol_blocker_frames_ready_count"], 1)
        self.assertEqual(summary["submit_to_cqe_observed_ns"]["p50"], 1_000)
        self.assertEqual(summary["data_complete_prefix_growth_tickets"]["p50"], 0)
        self.assertEqual(summary["hol_blocker_data_write_elapsed_ns"]["p50"], 2_000)

        no_blocker_sample = {
            key: value for key, value in timeline["samples"][0].items()
            if not key.startswith("hol_blocker_")
        }
        no_blocker_summary = _completion_timeline_summary({
            **timeline, "samples": [no_blocker_sample],
        })
        self.assertEqual(no_blocker_summary["hol_blocker_count"], 0)
        self.assertEqual(no_blocker_summary["hol_blocker_frames_ready_count"], 0)
        self.assertEqual(no_blocker_summary["hol_blocker_digest_folded_count"], 0)
        self.assertEqual(no_blocker_summary["hol_blocker_data_write_elapsed_ns"]["count"], 0)

        for invalid in (
            None,
            {**timeline, "dropped_samples": 1},
            {**timeline, "samples": [{**timeline["samples"][0], "request_id": 129}]},
            {**timeline, "samples": [{**timeline["samples"][0], "data_complete_lock_wait_cpu_ns": True}]},
            {**timeline, "samples": [{**timeline["samples"][0], "hol_blocker_frames_ready": None}]},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                _completion_timeline_summary(invalid)

    def test_v8_cpu_attribution_failed_launch_writes_partial_summary_and_zero_started(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results_dir = Path(temporary) / "results"
            binary = Path(temporary) / "write-perf"
            binary.write_bytes(b"fake benchmark executable")
            binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
            args = SimpleNamespace(
                binary=binary,
                results_dir=results_dir,
                db_root=Path(temporary) / "databases",
                safe_deadline_epoch=9_999_999_999,
                commit="a" * 40,
                binary_sha256=binary_sha256,
                machine_type="c4-standard-16-lssd",
                ssd_count=2,
                ssd_gib=750,
                writers=V8_POINT_CPU_ATTRIBUTION_WRITERS,
            )
            with patch.object(
                cpu_attribution_runner,
                "_run_one",
                side_effect=TimeoutError("stopping before launch"),
            ):
                exit_code = run_cpu_attribution_suite(args)

            summary = json.loads((results_dir / "summary.json").read_text(encoding="utf-8"))
            remote_exit = json.loads((results_dir / "remote-exit.json").read_text(encoding="utf-8"))
            self.assertEqual(exit_code, 1)
            self.assertFalse(summary["complete"])
            self.assertFalse(summary["results_complete"])
            self.assertEqual(summary["runs_completed"], 0)
            self.assertEqual(remote_exit, {"failed": True, "runs_started": 0})
            self.assertTrue((results_dir / "summary.tsv").is_file())

    def test_v8_cpu_attribution_failed_run_uses_real_summarizer_and_counts_launch(self) -> None:
        for failed_exit_code in (75, 65):
            with self.subTest(exit_code=failed_exit_code), tempfile.TemporaryDirectory() as temporary:
                results_dir = Path(temporary) / "results"
                binary = Path(temporary) / "write-perf"
                binary.write_bytes(b"fake benchmark executable")
                binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
                args = SimpleNamespace(
                    binary=binary,
                    results_dir=results_dir,
                    db_root=Path(temporary) / "databases",
                    safe_deadline_epoch=9_999_999_999,
                    commit="a" * 40,
                    binary_sha256=binary_sha256,
                    machine_type="c4-standard-16-lssd",
                    ssd_count=2,
                    ssd_gib=750,
                    writers=V8_POINT_CPU_ATTRIBUTION_WRITERS,
                )
                scheduled = build_v8_point_cpu_attribution_schedule()[0]

                def failed_run(**kwargs):
                    kwargs["on_launch"]()
                    return {**kwargs["row"], "exit_code": failed_exit_code}

                with patch.object(cpu_attribution_runner, "_run_one", side_effect=failed_run):
                    exit_code = run_cpu_attribution_suite(args)

                summary = json.loads((results_dir / "summary.json").read_text(encoding="utf-8"))
                remote_exit = json.loads((results_dir / "remote-exit.json").read_text(encoding="utf-8"))
                self.assertEqual(exit_code, 1)
                self.assertFalse(summary["complete"])
                self.assertFalse(summary["results_complete"])
                self.assertEqual(summary["runs_completed"], 1)
                self.assertEqual(summary["results"][0]["arm"], scheduled["arm"])
                self.assertEqual(summary["results"][0]["exit_code"], failed_exit_code)
                self.assertEqual(remote_exit, {"failed": True, "runs_started": 1})

    def test_v8_cpu_attribution_success_writes_complete_remote_exit_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results_dir = Path(temporary) / "results"
            binary = Path(temporary) / "write-perf"
            binary.write_bytes(b"fake benchmark executable")
            binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
            args = SimpleNamespace(
                binary=binary,
                results_dir=results_dir,
                db_root=Path(temporary) / "databases",
                safe_deadline_epoch=9_999_999_999,
                commit="a" * 40,
                binary_sha256=binary_sha256,
                machine_type="c4-standard-16-lssd",
                ssd_count=2,
                ssd_gib=750,
                writers=V8_POINT_CPU_ATTRIBUTION_WRITERS,
            )

            def successful_run(**kwargs):
                kwargs["on_launch"]()
                row = kwargs["row"]
                return {
                    **row,
                    "ops": row["num"],
                    "exit_code": 0,
                    "wal_cpu_attribution_valid": True,
                }

            with patch.object(cpu_attribution_runner, "_run_one", side_effect=successful_run):
                exit_code = run_cpu_attribution_suite(args)

            summary = json.loads((results_dir / "summary.json").read_text(encoding="utf-8"))
            remote_exit = json.loads((results_dir / "remote-exit.json").read_text(encoding="utf-8"))
            planned = len(build_v8_point_cpu_attribution_schedule())
            self.assertEqual(exit_code, 0)
            self.assertTrue(summary["complete"])
            self.assertTrue(summary["results_complete"])
            self.assertEqual(summary["runs_completed"], planned)
            self.assertEqual(remote_exit, {"failed": False, "runs_started": planned})

    def test_v8_cpu_attribution_accepts_retried_admission_barrier_sample(self) -> None:
        sequence = list(range(
            V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
            V8_POINT_CPU_ATTRIBUTION_NUM + 1,
            V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
        ))
        producer_samples = []
        for sample_sequence in sequence:
            sample = {
                "sample_sequence": sample_sequence,
                "wal_version": 7,
                "entry_count": 1,
                "completed": sample_sequence != sequence[0],
                **{field: 10 for field in cpu_attribution_runner.PRODUCER_PHASE_FIELDS},
            }
            if sample["completed"]:
                sample.update({"ticket": sample_sequence, "commit_ts": sample_sequence})
            producer_samples.append(sample)

        completion_handoff_samples = [
            {
                "group_id": ticket,
                "ticket_start": ticket,
                "ticket_end": ticket + 1,
                "worker_tid": 42,
                "request_id": ticket,
                "request_start": ticket,
                "request_end": ticket + 1,
                "sampled_ticket": ticket,
                "frame_ready_to_submit_request_ns": 5,
                "submit_request_to_worker_queue_ns": 6,
                "worker_queue_to_worker_accept_ns": 7,
                "worker_accept_to_sqe_submit_ns": 8,
                "submit_to_cqe_observed_ns": 10,
                "cqe_observed_to_group_finished_ns": 20,
                "group_finished_to_result_send_start_ns": 30,
                "result_send_start_to_receiver_ns": 40,
                "receiver_to_data_complete_publish_ns": 9,
                "data_complete_prefix_end_before": ticket,
                "data_complete_prefix_end_after": ticket + 1,
                "frame_ready_monotonic_ns": ticket + 1,
                "sqe_submitted_monotonic_ns": ticket + 2,
                "cqe_observed_monotonic_ns": ticket + 3,
                "data_complete_published_monotonic_ns": ticket + 4,
                "hol_blocker_ticket": None,
                "publication_phases": {
                    "v7_durability_state_lock_wait_wall_ns": 1,
                    "v7_durability_state_lock_wait_cpu_ns": 1,
                    "v7_durability_state_lock_hold_wall_ns": 1,
                    "v7_durability_state_lock_hold_cpu_ns": 1,
                    "v7_group_registration_wall_ns": 1,
                    "v7_group_registration_cpu_ns": 1,
                    "v7_written_frontier_advance_wall_ns": 1,
                    "v7_written_frontier_advance_cpu_ns": 1,
                },
            }
            for ticket in range(
                0,
                V8_POINT_CPU_ATTRIBUTION_NUM,
                cpu_attribution_runner.V8_COMPLETION_TIMELINE_SAMPLE_EVERY,
            )
        ]

        record = {
            "params": {
                "num": V8_POINT_CPU_ATTRIBUTION_NUM,
                "threads": 16,
                "value_size": 1024,
                "wal_format_version": 7,
                "wal_io_mode": "parallel",
            },
            "result": {
                "ops": V8_POINT_CPU_ATTRIBUTION_NUM,
                "ops_per_sec": 20_000,
                "process_cpu_user_ms": 1_000,
                "process_cpu_system_ms": 100,
                "wal_pipeline": {
                    "sync_count": 1,
                    "sync_observations": [{
                        "captured_target": 2,
                        "decision_ticket_start": 1,
                        "written_frontier_start": 1,
                        "written_frontier_end": 2,
                        "admitted_ticket_count": 2,
                        "frames_ready_prefix_tickets": 2,
                        "data_complete_prefix_tickets": 1,
                        "durability_ready_prefix_tickets": 1,
                        "next_ticket_present": True,
                        "next_ticket_frames_ready": True,
                        "next_ticket_data_complete": False,
                        "sync_decision_monotonic_ns": 100,
                        "data_hol_blocker_ticket": 2,
                        "data_hol_blocker_frames_ready": True,
                        "data_hol_blocker_digest_folded": None,
                        "data_hol_blocker_data_write_elapsed_ns": None,
                        "succeeded": True,
                    }],
                    "completion_handoff_timeline": {
                        "sample_every": cpu_attribution_runner.V8_COMPLETION_TIMELINE_SAMPLE_EVERY,
                        "dropped_samples": 0,
                        "samples": completion_handoff_samples,
                    },
                    "wal_cpu_attribution": {
                        "sample_every": V8_POINT_CPU_ATTRIBUTION_SAMPLE_EVERY,
                        "operations_seen": V8_POINT_CPU_ATTRIBUTION_NUM + 1,
                        "selected_producer_samples": len(producer_samples),
                        "dropped_producer_samples": 0,
                        "dropped_thread_samples": 0,
                        "thread_cpu_time_supported": True,
                        "producer_samples": producer_samples,
                        "thread_samples": [
                            {
                                "role": role,
                                "sample_sequence": 64,
                                "work_items": 1,
                                "thread_cpu_ns": 10,
                                "wall_ns": 10,
                                "completed": True,
                            }
                            for role in (
                                "io_worker_completion_processing",
                                "v7_durability_coordinator",
                            )
                        ],
                    },
                },
            },
            "latency": {"p50_ms": 1, "p95_ms": 2, "p99_ms": 3},
        }
        row = {"writer": 16, "arm": "v7_parallel"}

        validated = cpu_attribution_runner._validate_record(record, row)

        self.assertEqual(validated["wal_cpu_attribution_retry_attempts"], 1)
        self.assertEqual(
            validated["completion_handoff_timeline_summary"]["sample_count"],
            len(completion_handoff_samples),
        )
        producer_summary = cpu_attribution_runner._producer_phase_summary([{
            **validated, "arm": "v7_parallel",
        }])
        self.assertEqual(
            producer_summary["v7_parallel"]["producer_total_cpu_ns"]["count"],
            len(sequence) - 1,
        )

    def test_completion_handoff_timeline_requires_matched_complete_samples(self) -> None:
        valid = {
            "sample_every": cpu_attribution_runner.V8_COMPLETION_TIMELINE_SAMPLE_EVERY,
            "dropped_samples": 0,
            "samples": [{
                "group_id": 7,
                "ticket_start": 120,
                "ticket_end": 130,
                "worker_tid": 42,
                "request_id": 128,
                "request_start": 120,
                "request_end": 131,
                "sampled_ticket": 128,
                "frame_ready_monotonic_ns": 10,
                "sqe_submitted_monotonic_ns": 20,
                "cqe_observed_monotonic_ns": 30,
                "data_complete_published_monotonic_ns": 40,
                "frame_ready_to_submit_request_ns": 5,
                "submit_request_to_worker_queue_ns": 6,
                "worker_queue_to_worker_accept_ns": 7,
                "worker_accept_to_sqe_submit_ns": 8,
                "submit_to_cqe_observed_ns": 10,
                "cqe_observed_to_group_finished_ns": 20,
                "group_finished_to_result_send_start_ns": 30,
                "result_send_start_to_receiver_ns": 40,
                "receiver_to_data_complete_publish_ns": 9,
                "data_complete_prefix_end_before": 120,
                "data_complete_prefix_end_after": 130,
                "hol_blocker_ticket": 130,
            }],
        }
        summary = _completion_handoff_timeline_summary(valid)
        self.assertEqual(summary["sample_count"], 1)
        self.assertEqual(summary["result_send_start_to_receiver_ns"]["p50"], 40)
        self.assertEqual(summary["frame_ready_to_submit_request_ns"]["p50"], 5)
        self.assertEqual(summary["unique_sampled_ticket_count"], 1)
        no_blocker_sample = {
            key: value for key, value in valid["samples"][0].items()
            if key != "hol_blocker_ticket"
        }
        no_blocker_summary = _completion_handoff_timeline_summary({
            **valid, "samples": [no_blocker_sample],
        })
        self.assertEqual(no_blocker_summary["hol_blocker_count"], 0)
        with self.assertRaises(ValueError):
            _completion_handoff_timeline_summary(valid, expected_operations=100_000)

        invalid_timelines = (
            None,
            {**valid, "sample_every": 64},
            {**valid, "dropped_samples": 1},
            {**valid, "samples": []},
            {**valid, "samples": [{**valid["samples"][0], "sampled_ticket": 127}]},
            {**valid, "samples": [valid["samples"][0], valid["samples"][0]]},
            {**valid, "samples": [{**valid["samples"][0], "ticket_end": 120}]},
            {**valid, "samples": [{**valid["samples"][0], "receiver_to_data_complete_publish_ns": None}]},
            {**valid, "samples": [{**valid["samples"][0], "data_complete_prefix_end_after": 119}]},
            {**valid, "samples": [{**valid["samples"][0], "result_send_start_to_receiver_ns": -1}]},
        )
        for timeline in invalid_timelines:
            with self.subTest(timeline=timeline), self.assertRaises(ValueError):
                _completion_handoff_timeline_summary(timeline)

    def test_same_ticket_hol_join_classifies_stage_at_sync_snapshot(self) -> None:
        observation = {
            "data_hol_blocker_ticket": 32,
            "sync_decision_monotonic_ns": 25,
            "decision_ticket_start": 16,
            "data_complete_prefix_tickets": 16,
        }
        timeline = {
            "samples": [{
                "sampled_ticket": 32,
                "frame_ready_monotonic_ns": 10,
                "sqe_submitted_monotonic_ns": 20,
                "cqe_observed_monotonic_ns": 30,
                "data_complete_published_monotonic_ns": 40,
            }],
        }

        summary = cpu_attribution_runner._same_ticket_hol_summary([observation], timeline)

        self.assertEqual(summary["matched_blocker_decision_count"], 1)
        self.assertEqual(summary["stage_at_sync_decision"]["sqe_submitted_waiting_for_cqe"], 1)
        self.assertEqual(summary["frame_ready_age_at_decision_ns"]["p50"], 15)

    def test_v8_cpu_attribution_validates_digest_schema(self) -> None:
        # The digest is serialized beside wal_pipeline inside result, not at the JSON record root.
        digest = {field: 7 for field in cpu_attribution_runner.V8_DIGEST_COUNTER_FIELDS}
        digest["cpu_timing_sample_rate"] = cpu_attribution_runner.V8_DIGEST_CPU_SAMPLE_RATE
        self.assertEqual(
            cpu_attribution_runner._extract_v8_digest({"wal_v8_digest": digest}, 8),
            digest,
        )
        self.assertIsNone(cpu_attribution_runner._extract_v8_digest({}, 7))

        invalid_results = (
            None,
            [],
            {"payload_hash_cpu_ns": 7},
            {**digest, "payload_hash_cpu_ns": True},
            {**digest, "ordered_link_hash_cpu_ns": -1},
            {**digest, "cpu_timing_sample_rate": 32},
        )
        for invalid_digest in invalid_results:
            with self.subTest(invalid_digest=invalid_digest), self.assertRaises(ValueError):
                cpu_attribution_runner._extract_v8_digest(
                    {"wal_v8_digest": invalid_digest}, 8,
                )

        with self.assertRaisesRegex(ValueError, "missing fields"):
            cpu_attribution_runner._digest_summary([{"v8_digest": {"payload_hash_cpu_ns": 7}}])

        summary = cpu_attribution_runner._digest_summary([{"v8_digest": digest}])
        self.assertEqual(summary["payload_hash_cpu_ns_per_sampled_ticket"], 1)

    def test_v8_mutex_diagnostic_schedule_and_validation_are_counterbalanced(self) -> None:
        schedule = build_v8_mutex_diagnostic_schedule()
        self.assertEqual(len(schedule), 10)
        self.assertEqual({row["writer"] for row in schedule}, {16})
        self.assertEqual({row["num"] for row in schedule}, {100_000})
        self.assertEqual({row["sample_every"] for row in schedule}, {64})
        for repetition in range(1, 6):
            pair = [row["arm"] for row in schedule if row["repetition"] == repetition]
            expected = (
                ["v7_parallel", "v8_point_lookahead_direct"] if repetition % 2 else
                ["v8_point_lookahead_direct", "v7_parallel"]
            )
            self.assertEqual(pair, expected)

        diagnostics = {
            "sample_every": V8_MUTEX_DIAGNOSTIC_SAMPLE_EVERY,
            "locks": [{"name": name} for name in LOCK_NAMES],
        }
        rows = [{
            "writer": row["writer"], "workload": row["workload"], "repetition": row["repetition"],
            "arm": row["arm"], "ops": row["num"], "exit_code": 0,
            "mutex_lock_diagnostics": diagnostics,
        } for row in schedule]
        self.assertTrue(validate_v8_mutex_diagnostic_rows(rows)["complete"])
        self.assertFalse(validate_v8_mutex_diagnostic_rows(rows[:-1])["complete"])
        broken = [{**row, "mutex_lock_diagnostics": {**diagnostics, "sample_every": 32}} for row in rows]
        self.assertFalse(validate_v8_mutex_diagnostic_rows(broken)["complete"])

        plan = build_v8_mutex_diagnostic_plan(
            machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            commit="a" * 40,
        )
        self.assertEqual(plan["runs_planned"], 10)
        self.assertTrue(plan["diagnostic_only"])
        self.assertEqual(plan["mutex_sample_every"], 64)

    def test_mutex_diagnostic_command_pins_latest_v8_point_candidate(self) -> None:
        row = next(row for row in build_v8_mutex_diagnostic_schedule()
                   if row["arm"] == "v8_point_lookahead_direct")
        command = build_mutex_command(Path("write-perf"), row, Path("dbs"))
        self.assertIn("--mutex-lock-sample-every", command)
        self.assertIn("64", command)
        self.assertIn("v8", command)
        v7_row = next(row for row in build_v8_mutex_diagnostic_schedule() if row["arm"] == "v7_parallel")
        self.assertIn("v7", build_mutex_command(Path("write-perf"), v7_row, Path("dbs")))

    def test_mutex_diagnostic_record_validation_checks_v7_and_v8_paths(self) -> None:
        diagnostics = {
            "sample_every": 64,
            "histogram": "log2_upper_bound_nanoseconds",
            "acquisition_scope": "instrumented synchronous WAL resource/extent and MVCC publication call sites",
            "locks": [{
                "name": name,
                "acquisition_count": 640,
                "sampled_acquisition_count": 10,
                "sampled_contended_count": 1,
                "estimated_contended_count": 64,
                "sampled_wait_ns_total": 100,
                "estimated_wait_ns_total": 6400,
                "sampled_hold_ns_total": 200,
                "estimated_hold_ns_total": 12800,
            } for name in LOCK_NAMES],
        }
        handles = {
            "data": {"o_direct": True, "device": 1, "inode": 7},
            "frontier": {"o_direct": True, "device": 1, "inode": 7},
            "sync": {"o_direct": False, "device": 1, "inode": 7},
        }
        preparation = {"mode": "sliding-point-lookahead", "window_bytes": 1 << 20}
        for version, arm in ((7, "v7_parallel"), (8, "v8_point_lookahead_direct")):
            row = {
                "writer": 16, "workload": "point", "repetition": 1, "arm": arm,
            }
            pipeline = {"sync_count": 10}
            if version == 8:
                pipeline.update({
                    "wal_io_stable": True,
                    "wal_io_start": {**handles, "sliding_extent_preparation": preparation},
                    "wal_io_end": {**handles, "sliding_extent_preparation": preparation},
                })
            record = {
                "params": {
                    "num": 100_000, "threads": 16, "value_size": 1024,
                    "wal_format_version": version, "wal_io_mode": "parallel",
                },
                "result": {
                    "ops": 100_000, "ops_per_sec": 20_000.0,
                    "process_cpu_user_ms": 1000.0, "process_cpu_system_ms": 1000.0,
                    "wal_pipeline": pipeline, "mutex_lock_diagnostics": diagnostics,
                },
                "latency": {"p50_ms": 0.1, "p95_ms": 0.2, "p99_ms": 0.3},
            }
            self.assertEqual(validate_mutex_record(record, row)["ops"], 100_000)

            if version == 8:
                bad = json.loads(json.dumps(record))
                bad["result"]["wal_pipeline"]["wal_io_start"]["frontier"]["o_direct"] = False
                bad["result"]["wal_pipeline"]["wal_io_end"]["frontier"]["o_direct"] = False
                with self.assertRaisesRegex(ValueError, "FRONTIER O_DIRECT"):
                    validate_mutex_record(bad, row)

    def test_point_lookahead_schedule_has_counterbalanced_point_and_batch_guardrails(self) -> None:
        schedule = build_point_lookahead_schedule((16,))
        self.assertEqual(len(schedule), 40)
        self.assertEqual(
            {row["extent_mode"] for row in schedule if row["arm"] == "v8_fallocate"},
            {"sliding-fallocate"},
        )
        self.assertEqual(
            {row["extent_mode"] for row in schedule if row["arm"] == "v8_point_lookahead"},
            {"sliding-fallocate", "sliding-point-lookahead"},
        )
        for workload in ("point", "batch8", "batch16", "batch64"):
            selected = [row for row in schedule if row["workload"] == workload]
            self.assertEqual(len(selected), 10)
            expected_extent_mode = (
                "sliding-point-lookahead" if workload == "point" else "sliding-fallocate"
            )
            self.assertEqual(
                {row["extent_mode"] for row in selected if row["arm"] == "v8_point_lookahead"},
                {expected_extent_mode},
            )
            for repetition in range(1, 6):
                pair = [row["arm"] for row in selected if row["repetition"] == repetition]
                expected = (
                    ["v8_fallocate", "v8_point_lookahead"] if repetition % 2 else
                    ["v8_point_lookahead", "v8_fallocate"]
                )
                self.assertEqual(pair, expected)
        rows = [{
            "writer": row["writer"], "workload": row["workload"], "repetition": row["repetition"],
            "arm": row["arm"], "ops": row["num"], "exit_code": 0,
        } for row in schedule]
        self.assertTrue(validate_point_lookahead_rows(rows, (16,))["complete"])
        self.assertFalse(validate_point_lookahead_rows(rows[:-1], (16,))["complete"])
        plan = build_point_lookahead_plan(
            writers=(16,), machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
        )
        self.assertEqual(plan["runs_planned"], 40)
        self.assertEqual(plan["suite"], "v8-point-lookahead")

    def test_point_borrowed_input_schedule_is_counterbalanced_and_limited_to_point_and_batch_guardrails(self) -> None:
        schedule = build_point_borrowed_input_schedule(POINT_BORROWED_INPUT_WRITERS)
        self.assertEqual(len(schedule), 50)
        self.assertTrue(all("point_direct_frame_enabled" not in row for row in schedule))
        for writer in POINT_BORROWED_INPUT_WRITERS:
            point_rows = [row for row in schedule if row["writer"] == writer and row["workload"] == "point"]
            self.assertEqual(len(point_rows), 10)
            for repetition in range(1, 6):
                pair = [row["arm"] for row in point_rows if row["repetition"] == repetition]
                expected = (
                    ["v8_owned_baseline", "v8_borrowed_candidate"] if repetition % 2 else
                    ["v8_borrowed_candidate", "v8_owned_baseline"]
                )
                self.assertEqual(pair, expected)
        self.assertEqual(
            {(row["writer"], row["workload"]) for row in schedule if row["workload"] != "point"},
            {(16, "batch16"), (16, "batch64")},
        )
        rows = [{
            "writer": row["writer"], "workload": row["workload"], "repetition": row["repetition"],
            "arm": row["arm"], "ops": POINT_BORROWED_INPUT_NUM, "exit_code": 0,
        } for row in schedule]
        self.assertTrue(validate_point_borrowed_input_rows(rows, POINT_BORROWED_INPUT_WRITERS)["complete"])
        self.assertFalse(
            validate_point_borrowed_input_rows(rows[:-1], POINT_BORROWED_INPUT_WRITERS)["complete"]
        )
        with self.assertRaisesRegex(ValueError, "requires writers"):
            build_point_borrowed_input_schedule((16,))
        plan = build_point_borrowed_input_plan(
            writers=POINT_BORROWED_INPUT_WRITERS, machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            baseline_commit="1" * 40, candidate_commit="2" * 40, source_patch_sha256="a" * 64,
        )
        self.assertEqual(plan["runs_planned"], 50)
        self.assertEqual(plan["suite"], "v8-point-borrowed-input")

    def test_point_borrowed_input_verifier_accepts_legacy_artifact_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            results.mkdir()
            baseline_commit = "1" * 40
            candidate_commit = "2" * 40
            source_patch_sha256 = "a" * 64
            baseline_binary_sha256 = "b" * 64
            candidate_binary_sha256 = "c" * 64
            machine_spec = BENCH.MACHINE_SPECS["c4-standard-16-lssd"]
            plan = build_point_borrowed_input_plan(
                writers=POINT_BORROWED_INPUT_WRITERS,
                machine_type="c4-standard-16-lssd",
                machine_spec=machine_spec,
                baseline_commit=baseline_commit,
                candidate_commit=candidate_commit,
                source_patch_sha256=source_patch_sha256,
                baseline_binary_sha256=baseline_binary_sha256,
                candidate_binary_sha256=candidate_binary_sha256,
            )
            (results / "benchmark-plan.json").write_text(json.dumps(plan), encoding="utf-8")
            (results / "source-patch.sha256").write_text(f"{source_patch_sha256}\n", encoding="utf-8")
            (results / "baseline-binary.sha256").write_text(f"{baseline_binary_sha256}\n", encoding="utf-8")
            (results / "candidate-binary.sha256").write_text(f"{candidate_binary_sha256}\n", encoding="utf-8")

            legacy_environment = (
                "TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead\n"
                "TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576\n"
                "TOYKV_WAL_V8_FRONTIER_MODE=direct\n"
            )
            schedule = build_point_borrowed_input_schedule(POINT_BORROWED_INPUT_WRITERS)
            summary_rows = []
            for item in schedule:
                run_id = BENCH._run_identifier(item, "v8-point-borrowed-input")
                run_dir = results / run_id
                run_dir.mkdir()
                source_commit, binary_sha256 = (
                    (baseline_commit, baseline_binary_sha256)
                    if item["arm"] == "v8_owned_baseline"
                    else (candidate_commit, candidate_binary_sha256)
                )
                row = {
                    **BENCH._expected_run_metadata(item, "v8-point-borrowed-input"),
                    "source_commit": source_commit,
                    "binary_sha256": binary_sha256,
                    "exit_code": 0,
                    "ops": item["num"],
                }
                summary_rows.append(row)
                preparation = {
                    "mode": "sliding-point-lookahead",
                    "window_bytes": POINT_LOOKAHEAD_WINDOW_BYTES,
                    "initialized_bytes": int(item["workload"] == "point"),
                }
                record = {
                    "params": {
                        "num": item["num"],
                        "threads": item["writer"],
                        "value_size": 1024,
                        "batch_size": item["batch_size"],
                        "wal_format_version": 8,
                        "wal_io_mode": "parallel",
                    },
                    "result": {
                        "ops": item["num"],
                        "wal_pipeline": {
                            "wal_io_stable": True,
                            "wal_io_start": {"sliding_extent_preparation": preparation},
                            "wal_io_end": {"sliding_extent_preparation": preparation},
                        },
                    },
                }
                (run_dir / "run-metadata.json").write_text(json.dumps(row), encoding="utf-8")
                (run_dir / "result.json").write_text(json.dumps(record), encoding="utf-8")
                (run_dir / "environment.txt").write_text(legacy_environment, encoding="utf-8")

            matrix_validation = validate_point_borrowed_input_rows(
                summary_rows, POINT_BORROWED_INPUT_WRITERS,
            )
            (results / "summary.json").write_text(json.dumps({
                "suite": "v8-point-borrowed-input",
                "complete": True,
                "runs_planned": len(schedule),
                "runs_completed": len(schedule),
                "source_commit": candidate_commit,
                "baseline_commit": baseline_commit,
                "source_patch_sha256": source_patch_sha256,
                "baseline_binary_sha256": baseline_binary_sha256,
                "candidate_binary_sha256": candidate_binary_sha256,
                "matrix_validation": matrix_validation,
                "results": summary_rows,
            }), encoding="utf-8")
            (results / "remote-exit.json").write_text(json.dumps({
                "failed": False,
                "runs_started": len(schedule),
            }), encoding="utf-8")

            self.assertTrue(BENCH._verify_result_set(results)["verified"])

    def test_point_borrowed_input_cli_requires_fixed_pair_and_source_identity(self) -> None:
        args = BENCH.parse_args([
            "--project", "test-project", "--zone", "us-east1-b", "--commit", "2" * 40,
            "--baseline-commit", "1" * 40, "--source-patch-sha256", "a" * 64,
            "--v8-point-borrowed-input-ab", "--preflight-only",
        ])
        self.assertEqual(args.num, POINT_BORROWED_INPUT_NUM)
        self.assertEqual(args.writers, POINT_BORROWED_INPUT_WRITERS)
        self.assertEqual(args.machine_type, "c4-standard-16-lssd")
        with self.assertRaises(SystemExit):
            BENCH.parse_args([
                "--project", "test-project", "--zone", "us-east1-b", "--commit", "2" * 40,
                "--v8-point-borrowed-input-ab", "--preflight-only",
            ])

    def test_point_direct_frame_schedule_is_counterbalanced_and_checks_fast_path_hits(self) -> None:
        schedule = build_point_direct_frame_schedule(POINT_DIRECT_FRAME_WRITERS)
        self.assertEqual(len(schedule), 50)
        for writer in POINT_DIRECT_FRAME_WRITERS:
            point_rows = [row for row in schedule if row["writer"] == writer and row["workload"] == "point"]
            self.assertEqual(len(point_rows), 10)
            for repetition in range(1, 6):
                pair = [row["arm"] for row in point_rows if row["repetition"] == repetition]
                expected = (
                    ["direct_frame_off", "direct_frame_on"] if repetition % 2 else
                    ["direct_frame_on", "direct_frame_off"]
                )
                self.assertEqual(pair, expected)
        rows = [{
            **row,
            "ops": POINT_DIRECT_FRAME_NUM,
            "exit_code": 0,
            "direct_frame_encoded_count": (
                POINT_DIRECT_FRAME_NUM
                if row["point_direct_frame_enabled"] and row["workload"] == "point"
                else 0
            ),
        } for row in schedule]
        self.assertTrue(validate_point_direct_frame_rows(rows, POINT_DIRECT_FRAME_WRITERS)["complete"])
        bad_row = next(row for row in rows if row["point_direct_frame_enabled"] and row["workload"] == "point")
        bad_row["direct_frame_encoded_count"] = 0
        self.assertFalse(validate_point_direct_frame_rows(rows, POINT_DIRECT_FRAME_WRITERS)["complete"])
        with self.assertRaisesRegex(ValueError, "requires writers"):
            build_point_direct_frame_schedule((16,))

        plan = build_point_direct_frame_plan(
            writers=POINT_DIRECT_FRAME_WRITERS,
            machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            commit="c" * 40,
            binary_sha256="d" * 64,
        )
        self.assertEqual(plan["runs_planned"], 50)
        self.assertEqual(plan["suite"], "v8-point-direct-frame")
        self.assertEqual(plan["binary_sha256"], "d" * 64)

    def test_point_direct_frame_cli_uses_fixed_same_binary_matrix(self) -> None:
        args = BENCH.parse_args([
            "--project", "test-project", "--zone", "us-east1-b", "--commit", "c" * 40,
            "--v8-point-direct-frame-ab", "--preflight-only",
        ])
        self.assertEqual(args.num, POINT_DIRECT_FRAME_NUM)
        self.assertEqual(args.writers, POINT_DIRECT_FRAME_WRITERS)
        self.assertEqual(args.machine_type, "c4-standard-16-lssd")
        with self.assertRaises(SystemExit):
            BENCH.parse_args([
                "--project", "test-project", "--zone", "us-east1-b", "--commit", "c" * 40,
                "--writers", "16", "--v8-point-direct-frame-ab", "--preflight-only",
            ])

    def test_extent_policy_storage_schedule_separates_scores_and_scan_diagnostics(self) -> None:
        schedule = build_v8_extent_policy_storage_schedule(V8_EXTENT_POLICY_STORAGE_WRITERS)
        self.assertEqual(len(schedule), 44)
        score_rows = [row for row in schedule if row["phase"] == "score"]
        diagnostic_rows = [row for row in schedule if row["phase"] == "diagnostic"]
        self.assertEqual(len(score_rows), 40)
        self.assertEqual(len(diagnostic_rows), 4)
        self.assertTrue(all(not row["scan_diagnostics_enabled"] for row in score_rows))
        self.assertTrue(all(row["scan_diagnostics_enabled"] for row in diagnostic_rows))

        for writer in V8_EXTENT_POLICY_STORAGE_WRITERS:
            point_rows = [
                row for row in score_rows if row["writer"] == writer and row["workload"] == "point"
            ]
            self.assertEqual(len(point_rows), 10)
            for repetition in range(1, 6):
                arms = [row["arm"] for row in point_rows if row["repetition"] == repetition]
                expected = ["btree", "vec"] if repetition % 2 else ["vec", "btree"]
                self.assertEqual(arms, expected)

        rows = []
        for row in schedule:
            diagnostic = row["phase"] == "diagnostic"
            rows.append({
                **row,
                "exit_code": 0,
                "ops": V8_EXTENT_POLICY_STORAGE_NUM,
                "extent_policy_storage": row["arm"],
                "scan_diagnostics_enabled": diagnostic,
                "lookahead_check_calls": 100 if diagnostic else 0,
                "extent_policy_entries_scanned": 1_000 if diagnostic else 0,
                "initialized_bytes": 1_048_576 if row["workload"] == "point" else 0,
            })
        self.assertTrue(
            validate_v8_extent_policy_storage_rows(rows, V8_EXTENT_POLICY_STORAGE_WRITERS)["complete"]
        )

        invalid_diagnostic = next(row for row in rows if row["phase"] == "diagnostic")
        invalid_diagnostic["extent_policy_entries_scanned"] = 0
        self.assertFalse(
            validate_v8_extent_policy_storage_rows(rows, V8_EXTENT_POLICY_STORAGE_WRITERS)["complete"]
        )

        plan = build_v8_extent_policy_storage_plan(
            writers=V8_EXTENT_POLICY_STORAGE_WRITERS,
            machine_type="c4-standard-16-lssd",
            machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            commit="c" * 40,
            binary_sha256="d" * 64,
        )
        self.assertEqual(plan["runs_planned"], 44)
        self.assertEqual(plan["score_runs_planned"], 40)
        self.assertEqual(plan["diagnostic_runs_planned"], 4)
        self.assertEqual(plan["binary_sha256"], "d" * 64)

    def test_extent_policy_storage_cli_uses_fixed_same_binary_matrix(self) -> None:
        args = BENCH.parse_args([
            "--project", "test-project", "--zone", "us-east1-b", "--commit", "c" * 40,
            "--v8-extent-policy-storage-ab", "--preflight-only",
        ])
        self.assertEqual(args.num, V8_EXTENT_POLICY_STORAGE_NUM)
        self.assertEqual(args.writers, V8_EXTENT_POLICY_STORAGE_WRITERS)
        self.assertEqual(args.machine_type, "c4-standard-16-lssd")
        with self.assertRaises(SystemExit):
            BENCH.parse_args([
                "--project", "test-project", "--zone", "us-east1-b", "--commit", "c" * 40,
                "--writers", "16", "--v8-extent-policy-storage-ab", "--preflight-only",
            ])

    def test_point_direct_frame_result_verifier_requires_actual_point_hits_and_batch_misses(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            results.mkdir()
            commit = "c" * 40
            binary_sha = "d" * 64
            writers = POINT_DIRECT_FRAME_WRITERS
            schedule = build_point_direct_frame_schedule(writers)
            plan = build_point_direct_frame_plan(
                writers=writers,
                machine_type="c4-standard-16-lssd",
                machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
                commit=commit,
                binary_sha256=binary_sha,
            )
            (results / "benchmark-plan.json").write_text(json.dumps(plan), encoding="utf-8")
            (results / "commit.txt").write_text(commit + "\n", encoding="utf-8")
            (results / "write-perf.sha256").write_text(binary_sha + "\n", encoding="utf-8")
            rows = []
            for row in schedule:
                run_id = BENCH._run_identifier(row, "v8-point-direct-frame")
                run_dir = results / run_id
                run_dir.mkdir()
                initialized_bytes = 1_048_576 if row["workload"] == "point" else 0
                encoded_count = (
                    POINT_DIRECT_FRAME_NUM
                    if row["point_direct_frame_enabled"] and row["workload"] == "point"
                    else 0
                )
                metadata = {
                    **BENCH._expected_run_metadata(row, "v8-point-direct-frame"),
                    "source_commit": commit,
                    "binary_sha256": binary_sha,
                    "exit_code": 0,
                    "ops": POINT_DIRECT_FRAME_NUM,
                    "puts_per_second": 10_000.0,
                    "cpu_seconds": 1.0,
                    "cpu_us_per_put": 10.0,
                    "p50_ms": 0.1,
                    "p95_ms": 0.2,
                    "p99_ms": 0.3,
                    "sync_count": 10,
                    "puts_per_sync": POINT_DIRECT_FRAME_NUM / 10,
                    "initialized_bytes": initialized_bytes,
                    "preparation_wait_ms": 0.0,
                    "direct_frame_encoded_count": encoded_count,
                    "duration_seconds": 10.0,
                }
                (run_dir / "run-metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
                (run_dir / "environment.txt").write_text(
                    "TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead\n"
                    "TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576\n"
                    "TOYKV_WAL_V8_FRONTIER_MODE=direct\n"
                    f"TOYKV_BENCH_V8_POINT_DIRECT_FRAME={int(row['point_direct_frame_enabled'])}\n",
                    encoding="utf-8",
                )
                preparation = {
                    "mode": row["extent_mode"],
                    "window_bytes": row["extent_window_bytes"],
                    "initialized_bytes": initialized_bytes,
                }
                record = {
                    "params": {
                        "num": POINT_DIRECT_FRAME_NUM,
                        "threads": row["writer"],
                        "value_size": 1024,
                        "batch_size": row["batch_size"],
                        "wal_format_version": 8,
                        "wal_io_mode": "parallel",
                    },
                    "result": {
                        "ops": POINT_DIRECT_FRAME_NUM,
                        "wal_pipeline": {
                            "wal_io_stable": True,
                            "v8_point_direct_frame_enabled": row["point_direct_frame_enabled"],
                            "v8_point_direct_frame_encoded_count": encoded_count,
                            "wal_io_start": {"sliding_extent_preparation": {**preparation, "initialized_bytes": 0}},
                            "wal_io_end": {"sliding_extent_preparation": preparation},
                        },
                    },
                }
                (run_dir / "result.json").write_text(json.dumps(record), encoding="utf-8")
                rows.append(metadata)

            validation = validate_point_direct_frame_rows(rows, writers)
            (results / "summary.json").write_text(json.dumps({
                "suite": "v8-point-direct-frame",
                "source_commit": commit,
                "binary_sha256": binary_sha,
                "complete": True,
                "runs_planned": len(schedule),
                "runs_completed": len(schedule),
                "results": rows,
                "matrix_validation": validation,
            }), encoding="utf-8")
            (results / "remote-exit.json").write_text(json.dumps({
                "failed": False, "runs_started": len(schedule),
            }), encoding="utf-8")
            self.assertTrue(BENCH._verify_result_set(results)["verified"])

            batch_on = next(
                row for row in rows
                if row["workload"] == "batch16" and row["point_direct_frame_enabled"]
            )
            batch_on["direct_frame_encoded_count"] = 1
            (results / BENCH._run_identifier(batch_on, "v8-point-direct-frame") / "run-metadata.json").write_text(
                json.dumps(batch_on), encoding="utf-8",
            )
            rejected = BENCH._verify_result_set(results)
            self.assertFalse(rejected["verified"])
            self.assertTrue(any("direct-frame hit count" in error for error in rejected["errors"]))

    def test_point_lookahead_result_verifier_checks_workload_specific_initialization(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            results.mkdir()
            schedule = build_point_lookahead_schedule((16,))
            plan = build_point_lookahead_plan(
                writers=(16,), machine_type="c4-standard-16-lssd",
                machine_spec=BENCH.MACHINE_SPECS["c4-standard-16-lssd"],
            )
            (results / "benchmark-plan.json").write_text(json.dumps(plan), encoding="utf-8")
            rows = []
            for row in schedule:
                run_id = BENCH._run_identifier(row, "v8-point-lookahead")
                run_dir = results / run_id
                run_dir.mkdir()
                should_initialize = row["workload"] == "point" and row["arm"] == "v8_point_lookahead"
                initialized_bytes = 1_048_576 if should_initialize else 0
                extent_start = {
                    "mode": row["extent_mode"], "window_bytes": row["extent_window_bytes"],
                    "initialized_bytes": 0,
                }
                extent_end = {
                    "mode": row["extent_mode"], "window_bytes": row["extent_window_bytes"],
                    "initialized_bytes": initialized_bytes,
                }
                record = {
                    "params": {
                        "num": row["num"], "threads": row["writer"], "value_size": 1024,
                        "batch_size": row["batch_size"], "wal_format_version": 8,
                        "wal_io_mode": "parallel",
                    },
                    "result": {
                        "ops": row["num"], "ops_per_sec": 10_000.0,
                        "process_cpu_user_ms": 1.0, "process_cpu_system_ms": 1.0,
                        "wal_pipeline": {
                            "sync_count": 10, "wal_io_stable": True,
                            "wal_io_start": {"sliding_extent_preparation": extent_start},
                            "wal_io_end": {"sliding_extent_preparation": extent_end},
                        },
                    },
                    "latency": {"p50_ms": 0.1, "p95_ms": 0.2, "p99_ms": 0.3},
                }
                (run_dir / "result.json").write_text(json.dumps(record), encoding="utf-8")
                metadata = {**BENCH._expected_run_metadata(row, "v8-point-lookahead"), "exit_code": 0}
                (run_dir / "run-metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
                rows.append({
                    **metadata, "ops": row["num"], "puts_per_second": 10_000.0,
                    "p50_ms": 0.1, "p95_ms": 0.2, "p99_ms": 0.3, "cpu_seconds": 0.002,
                    "cpu_us_per_put": 0.02, "sync_count": 10, "puts_per_sync": row["num"] / 10,
                })
            validation = validate_point_lookahead_rows(rows, (16,))
            (results / "summary.json").write_text(json.dumps({
                "complete": True, "runs_planned": len(schedule), "runs_completed": len(schedule),
                "results": rows, "matrix_validation": validation,
            }), encoding="utf-8")
            (results / "remote-exit.json").write_text(json.dumps({
                "failed": False, "runs_started": len(schedule),
            }), encoding="utf-8")
            self.assertTrue(BENCH._verify_result_set(results)["verified"])

            batch_row = next(row for row in schedule if row["workload"] == "batch8"
                             and row["arm"] == "v8_point_lookahead")
            batch_record_path = results / BENCH._run_identifier(batch_row, "v8-point-lookahead") / "result.json"
            batch_record = json.loads(batch_record_path.read_text(encoding="utf-8"))
            batch_record["result"]["wal_pipeline"]["wal_io_end"]["sliding_extent_preparation"][
                "initialized_bytes"
            ] = 4096
            batch_record_path.write_text(json.dumps(batch_record), encoding="utf-8")
            rejected = BENCH._verify_result_set(results)
            self.assertFalse(rejected["verified"])
            self.assertTrue(any("Point-only policy" in error for error in rejected["errors"]))

    def test_vm_retention_keeps_unverified_benchmark_for_artifact_recovery(self) -> None:
        args = BENCH.argparse.Namespace(keep_vm_after_run=True)
        complete = {
            "created": True,
            "benchmark_attempted": True,
            "benchmark_started": True,
            "collected": True,
            "results_verified": True,
        }
        self.assertTrue(BENCH.should_retain_vm(args, 0, complete))
        self.assertTrue(BENCH.should_retain_vm(args, 1, complete))
        self.assertFalse(BENCH.should_retain_vm(args, 0, {**complete, "collected": False}))
        self.assertFalse(BENCH.should_retain_vm(
            BENCH.argparse.Namespace(keep_vm_after_run=False), 0, complete,
        ))
        self.assertTrue(BENCH.should_retain_vm(
            BENCH.argparse.Namespace(keep_vm_after_run=False),
            1,
            {"created": True, "benchmark_attempted": True, "results_verified": False},
        ))
        self.assertTrue(BENCH.should_retain_vm(
            BENCH.argparse.Namespace(keep_vm_after_run=False),
            1,
            {"created": True, "remote_execution_uncertain": True, "results_verified": False},
        ))
        self.assertFalse(BENCH.should_retain_vm(
            BENCH.argparse.Namespace(keep_vm_after_run=False),
            1,
            {"created": True, "benchmark_attempted": False, "results_verified": False},
        ))

    def test_retained_vm_reconnect_instructions_include_quoted_firewall_cleanup(self) -> None:
        project = "project name"
        firewall = "temporary ssh rule; echo unsafe"
        instructions = BENCH.retained_vm_reconnect_instructions(
            termination_timestamp="2026-11-10T08:00:00Z",
            network="default",
            subnet="default",
            firewall=firewall,
            project=project,
            connect_command="ssh -i retained-key bench@192.0.2.1",
        )

        cleanup_prefix = "Delete the temporary SSH firewall rule after the VM is gone: "
        cleanup_line = next(line for line in instructions.splitlines() if line.startswith(cleanup_prefix))
        self.assertEqual(BENCH.shlex.split(cleanup_line.removeprefix(cleanup_prefix)), [
            "gcloud", "compute", "firewall-rules", "delete", firewall, f"--project={project}", "--quiet",
        ])

    def test_benchmark_plan_matches_suite_and_machine_shape(self) -> None:
        full = build_benchmark_plan(
            full_matrix=True, writers=FULL_DEFAULT_WRITERS, machine_type="c4-standard-16-lssd",
            machine_spec={"vcpus": 16, "ssd_count": 2, "ssd_gib": 750},
        )
        self.assertEqual(full["score_runs_planned"], 320)
        self.assertEqual(full["runtime_protocol"], "single 16-vCPU host; RAID0 over 2 Local SSDs")
        small = build_benchmark_plan(
            full_matrix=False, writers=(2,), machine_type="c4-standard-8-lssd",
            machine_spec={"vcpus": 8, "ssd_count": 1, "ssd_gib": 375}, num=128,
        )
        self.assertEqual(small["score_runs_planned"], 12)
        self.assertEqual(small["runtime_protocol"], "single 8-vCPU host; single Local SSD")

        profile = build_profile_plan(
            (1, 8, 16), machine_type="c4-standard-16-lssd",
            machine_spec={"vcpus": 16, "ssd_count": 2, "ssd_gib": 750}, commit="a" * 40,
        )
        self.assertEqual(profile["runs_planned"], 28)
        self.assertEqual(profile["perf_captures_planned"], 7)
        self.assertTrue(profile["diagnostic_only"])

    def test_commit_order_trace_schedule_is_bounded_and_counterbalanced(self) -> None:
        schedule = build_commit_order_trace_schedule()
        self.assertEqual(len(schedule), 12)
        self.assertEqual(sum(row["phase"] == "trace" for row in schedule), 2)
        score_rows = [row for row in schedule if row["phase"] == "score"]
        self.assertEqual(len(score_rows), 10)
        self.assertTrue(all(row["writer"] == COMMIT_ORDER_TRACE_WRITERS for row in schedule))
        self.assertTrue(all(row["num"] == COMMIT_ORDER_TRACE_NUM for row in schedule))
        self.assertEqual(
            [tuple(row["arm"] for row in score_rows[index:index + 2]) for index in range(0, 10, 2)],
            [
                ("v7_parallel", "v8_parallel"),
                ("v8_parallel", "v7_parallel"),
                ("v7_parallel", "v8_parallel"),
                ("v8_parallel", "v7_parallel"),
                ("v7_parallel", "v8_parallel"),
            ],
        )
        validation = validate_commit_order_trace_rows([{
            "writer": row["writer"], "workload": row["workload"], "phase": row["phase"],
            "repetition": row["repetition"], "arm": row["arm"], "ops": row["num"], "exit_code": 0,
        } for row in schedule])
        self.assertTrue(validation["complete"])
        plan = build_commit_order_trace_plan(
            machine_type="c4-standard-16-lssd",
            machine_spec={"vcpus": 16, "ssd_count": 2, "ssd_gib": 750},
        )
        self.assertEqual(plan["runs_planned"], 12)
        self.assertEqual(plan["trace_records_per_diagnostic_run"], 1024)
        self.assertEqual(plan["diagnostic_sampling"]["mvcc_publication_sample_every"], 64)

    def test_commit_order_trace_analyzer_counts_stage_inversions_and_pairs(self) -> None:
        self.assertEqual(_inversion_count([1, 3, 2]), 1)
        records = []
        for commit_ts in (1, 2, 3):
            record = {
                "commit_ts": commit_ts, "wal_ticket": commit_ts - 1,
                "frontier_ticket_end": commit_ts,
            }
            for stage_index, stage in enumerate(TRACE_STAGES):
                order = (3 - commit_ts) if stage_index >= 3 else commit_ts
                record[stage] = {
                    "monotonic_ns": stage_index * 100_000 + order * 1_000 + commit_ts,
                    "sequence": commit_ts * 10 + stage_index,
                    "thread_id": 1,
                }
            records.append(record)
        analysis = analyze_trace({"records": records})
        self.assertEqual(analysis["complete_records"], 3)
        self.assertEqual(analysis["pairwise_timestamp_inversions"]["wal_return"]["timestamp_inversion_pairs"], 3)
        self.assertEqual(analysis["pairwise_timestamp_inversions"]["put_return"]["timestamp_inversion_pairs"], 3)
        self.assertEqual(analysis["pairwise_timestamp_inversions"]["timestamp_reserved"]["timestamp_inversion_pairs"], 0)
        self.assertEqual(analysis["pairwise_timestamp_inversions"]["wal_return"]["timestamp_tie_pairs_excluded"], 0)
        self.assertIn("mvcc_publication_complete_to_put_return", analysis["stage_intervals"])

        tied_records = []
        for commit_ts in (1, 2, 3):
            record = {
                "commit_ts": commit_ts, "wal_ticket": commit_ts - 1,
                "frontier_ticket_end": commit_ts,
            }
            for stage_index, stage in enumerate(TRACE_STAGES):
                monotonic_ns = 1 if stage == "durable_frontier_published" else stage_index + 1
                record[stage] = {
                    "monotonic_ns": monotonic_ns, "sequence": commit_ts * 10 + stage_index, "thread_id": 1,
                }
            tied_records.append(record)
        tied_analysis = analyze_trace({"records": tied_records})
        frontier_order = tied_analysis["pairwise_timestamp_inversions"]["durable_frontier_published"]
        self.assertEqual(frontier_order["timestamp_pair_count"], 0)
        self.assertEqual(frontier_order["timestamp_tie_pairs_excluded"], 3)
        self.assertIsNone(frontier_order["timestamp_inversion_rate"])

        with self.assertRaisesRegex(ValueError, "incomplete"):
            analyze_trace({"records": [{"commit_ts": 1}]})

        score_rows = [
            {"phase": "score", "repetition": 1, "arm": "v7_parallel", "exit_code": 0,
             "puts_per_second": 100.0, "p99_ms": 1.0, "cpu_seconds": 1.2},
            {"phase": "score", "repetition": 1, "arm": "v8_parallel", "exit_code": 0,
             "puts_per_second": 110.0, "p99_ms": 0.9, "cpu_seconds": 1.1},
        ]
        paired = summarize_score_pairs(score_rows)
        self.assertEqual(paired["paired_runs"], 1)
        self.assertEqual(paired["v8_throughput_wins"], 1)
        self.assertAlmostEqual(paired["median_paired_change_pct_v8_vs_v7"]["throughput_pct"], 10.0)

    def test_commit_order_trace_analyzer_splits_wal_returns_by_group_and_size(self) -> None:
        records = []
        for commit_ts, ticket, frontier_end, return_order in (
            (1, 0, 3, 40),
            (2, 1, 3, 10),
            (3, 2, 3, 10),
            (4, 3, 5, 20),
            (5, 4, 5, 30),
            (6, 5, 7, 50),
        ):
            record = {
                "commit_ts": commit_ts,
                "wal_ticket": ticket,
                "frontier_ticket_end": frontier_end,
            }
            for stage_index, stage in enumerate(TRACE_STAGES):
                monotonic_ns = stage_index * 100_000 + commit_ts * 10
                if stage == "wal_return":
                    monotonic_ns = 300_000 + return_order * 1_000
                record[stage] = {
                    "monotonic_ns": monotonic_ns,
                    "sequence": commit_ts * 10 + stage_index,
                    "thread_id": 1,
                }
            records.append(record)

        analysis = analyze_trace({"records": records})
        grouped = analysis["wal_return_inversions_by_frontier_group"]
        self.assertEqual(grouped["frontier_groups"], 3)
        self.assertEqual(grouped["intra_group"]["timestamp_inversion_pairs"], 2)
        self.assertEqual(grouped["intra_group"]["timestamp_pair_count"], 3)
        self.assertEqual(grouped["intra_group"]["timestamp_tie_pairs_excluded"], 1)
        self.assertEqual(grouped["inter_group"]["timestamp_inversion_pairs"], 2)
        self.assertEqual(grouped["inter_group"]["timestamp_pair_count"], 11)

        by_size = analysis["wal_return_inversions_by_complete_group_size"]
        self.assertEqual(by_size["complete_groups"], 2)
        self.assertEqual(by_size["incomplete_groups"], 1)
        self.assertEqual(by_size["incomplete_group_details"], [{
            "frontier_ticket_end": 7,
            "expected_group_size": 2,
            "observed_record_count": 1,
        }])
        size_two = by_size["by_group_size"]["2"]
        self.assertEqual(size_two["groups"], 1)
        self.assertEqual(size_two["records"], 2)
        self.assertEqual(size_two["timestamp_inversion_pairs"], 0)
        self.assertEqual(size_two["timestamp_pair_count"], 1)
        size_three = by_size["by_group_size"]["3"]
        self.assertEqual(size_three["timestamp_inversion_pairs"], 2)
        self.assertEqual(size_three["timestamp_pair_count"], 2)
        self.assertEqual(size_three["timestamp_tie_pairs_excluded"], 1)
        self.assertEqual(size_three["timestamp_inversion_rate"], 1.0)
        self.assertEqual(size_three["median_group_inversion_rate"], 1.0)

    def test_commit_order_trace_analyzer_excludes_ties_in_complete_16_ticket_group(self) -> None:
        records = []
        for ticket in range(16):
            record = {
                "commit_ts": ticket + 1,
                "wal_ticket": ticket,
                "frontier_ticket_end": 16,
            }
            for stage_index, stage in enumerate(TRACE_STAGES):
                return_order = 0 if ticket < 2 else ticket
                monotonic_ns = stage_index * 100_000 + ticket + 1
                if stage == "wal_return":
                    monotonic_ns = 300_000 + return_order * 1_000
                record[stage] = {
                    "monotonic_ns": monotonic_ns,
                    "sequence": (ticket + 1) * 10 + stage_index,
                    "thread_id": ticket + 1,
                }
            records.append(record)

        analysis = analyze_trace({"records": records})
        size_sixteen = analysis["wal_return_inversions_by_complete_group_size"]["by_group_size"]["16"]
        self.assertEqual(size_sixteen["groups"], 1)
        self.assertEqual(size_sixteen["timestamp_pair_count"], 119)
        self.assertEqual(size_sixteen["timestamp_tie_pairs_excluded"], 1)
        self.assertEqual(size_sixteen["timestamp_inversion_pairs"], 0)

    def test_offline_commit_order_trace_extractor_accepts_write_perf_json(self) -> None:
        trace = {"records": []}
        self.assertIs(extract_trace(trace), trace)
        self.assertIs(
            extract_trace({"result": {"wal_pipeline": {"commit_order_trace": trace}}}),
            trace,
        )
        with self.assertRaisesRegex(ValueError, "does not contain"):
            extract_trace({"result": {"wal_pipeline": {}}})

    def test_commit_order_trace_analyzer_rejects_invalid_frontier_group(self) -> None:
        record = {"commit_ts": 1, "wal_ticket": 0, "frontier_ticket_end": 0}
        for stage_index, stage in enumerate(TRACE_STAGES):
            record[stage] = {
                "monotonic_ns": stage_index + 1,
                "sequence": stage_index + 1,
                "thread_id": 1,
            }

        with self.assertRaisesRegex(ValueError, "frontier ticket end"):
            analyze_trace({"records": [record]})

    def test_commit_order_trace_result_verifier_checks_trace_and_score_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            results.mkdir()
            schedule = build_commit_order_trace_schedule()
            plan = build_commit_order_trace_plan(
                machine_type="c4-standard-16-lssd",
                machine_spec={"vcpus": 16, "ssd_count": 2, "ssd_gib": 750},
                commit="a" * 40,
                binary_sha256="b" * 64,
                source_patch_sha256="c" * 64,
            )
            (results / "benchmark-plan.json").write_text(json.dumps(plan), encoding="utf-8")
            (results / "source-patch.sha256").write_text("c" * 64 + "\n", encoding="utf-8")
            (results / "write-perf.sha256").write_text("b" * 64 + "\n", encoding="utf-8")
            summary_rows = []
            for item in schedule:
                run_id = BENCH._run_identifier(item, "commit-order-trace")
                run_dir = results / run_id
                run_dir.mkdir()
                version = 7 if item["arm"] == "v7_parallel" else 8
                trace = None
                if item["trace_limit"]:
                    trace = {
                        "limit": item["trace_limit"], "selected_records": item["trace_limit"],
                        "dropped_records": 0, "dropped_frontier_events": 0, "incomplete_records": 0,
                        "records": [{
                            "commit_ts": value, "wal_ticket": value, "frontier_ticket_end": value + 1,
                            **{
                                stage: {"sequence": value + index, "monotonic_ns": value + index, "thread_id": 1}
                                for index, stage in enumerate(TRACE_STAGES)
                            },
                        } for value in range(1, item["trace_limit"] + 1)],
                    }
                record = {
                    "params": {
                        "num": COMMIT_ORDER_TRACE_NUM, "threads": COMMIT_ORDER_TRACE_WRITERS,
                        "value_size": 1024, "wal_format_version": version, "wal_io_mode": "parallel",
                    },
                    "result": {
                        "ops": COMMIT_ORDER_TRACE_NUM,
                        "wal_pipeline": {"commit_order_trace": trace},
                    },
                }
                row = {
                    **{key: item[key] for key in (
                        "writer", "workload", "phase", "repetition", "arm", "trace_limit",
                    )},
                    "exit_code": 0, "ops": COMMIT_ORDER_TRACE_NUM,
                }
                summary_rows.append(row)
                (run_dir / "result.json").write_text(json.dumps(record), encoding="utf-8")
                (run_dir / "run-metadata.json").write_text(json.dumps(row), encoding="utf-8")

            validation = validate_commit_order_trace_rows(summary_rows)
            score_comparison = summarize_score_pairs([
                {**row, "puts_per_second": 1.0, "p99_ms": 1.0, "cpu_seconds": 1.0}
                for row in summary_rows if row["phase"] == "score"
            ])
            (results / "summary.json").write_text(json.dumps({
                "suite": "commit-order-trace", "complete": True,
                "runs_planned": len(schedule), "runs_completed": len(schedule),
                "source_commit": "a" * 40, "source_patch_sha256": "c" * 64,
                "binary_sha256": "b" * 64, "trace_analysis": {}, "score_comparison": score_comparison,
                "matrix_validation": validation, "results": summary_rows,
            }), encoding="utf-8")
            (results / "commit-order-trace-analysis.json").write_text(json.dumps({
                "commit": "a" * 40, "source_patch_sha256": "c" * 64,
                "binary_sha256": "b" * 64, "traced_runs": {}, "score_comparison": score_comparison,
            }), encoding="utf-8")
            (results / "remote-exit.json").write_text(json.dumps({
                "failed": False, "runs_started": len(schedule),
            }), encoding="utf-8")
            self.assertTrue(BENCH._verify_result_set(results)["verified"])

            trace_run = next(item for item in schedule if item["trace_limit"])
            run_id = BENCH._run_identifier(trace_run, "commit-order-trace")
            result_path = results / run_id / "result.json"
            record = json.loads(result_path.read_text(encoding="utf-8"))
            record["result"]["wal_pipeline"]["commit_order_trace"]["dropped_frontier_events"] = 1
            result_path.write_text(json.dumps(record), encoding="utf-8")
            rejected = BENCH._verify_result_set(results)
            self.assertFalse(rejected["verified"])
            self.assertTrue(any("dropped or incomplete" in error for error in rejected["errors"]))

    def test_small_ticket_profile_schedule_and_completeness_validation(self) -> None:
        schedule = build_profile_schedule((1, 8, 16))
        self.assertEqual(len(schedule), 28)
        self.assertEqual(sum(row["perf_capture"] for row in schedule), 7)
        self.assertTrue(all(row["writer"] == 1 for row in schedule if row["arm"] == "v8_leader"))
        rows = [{
            "writer": row["writer"], "workload": row["workload"], "arm": row["arm"],
            "exit_code": 0, "ops": row["num"],
        } for row in schedule]
        self.assertTrue(validate_profile_rows(rows, (1, 8, 16))["complete"])
        self.assertFalse(validate_profile_rows(rows[:-1], (1, 8, 16))["complete"])
        duplicate = [*rows[:-1], rows[0]]
        validation = validate_profile_rows(duplicate, (1, 8, 16))
        self.assertFalse(validation["complete"])
        self.assertTrue(validation["duplicate_cells"])
        invalid_updates = [{"exit_code": 1}, {"ops": rows[0]["ops"] - 1}]
        for field in ("writer", "exit_code", "ops"):
            value = rows[0][field]
            invalid_updates.extend({field: invalid} for invalid in (
                str(value), float(value), value + 0.5, float("inf"), bool(value),
            ))
        for invalid_fields in invalid_updates:
            with self.subTest(invalid_fields=invalid_fields):
                invalid_row = {**rows[0], **invalid_fields}
                invalid_rows = [invalid_row, *rows[1:]]
                validation = validate_profile_rows(invalid_rows, (1, 8, 16))
                self.assertFalse(validation["complete"])
                self.assertEqual(validation["observed"], len(invalid_rows))
                self.assertEqual(validation["invalid_rows"], [invalid_row])

    def test_small_ticket_profile_runner_executes_schedule_without_gcp(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_binary = root / "write-perf"
            fake_binary.write_text("""#!/usr/bin/env python3
import json
import sys

args = sys.argv[1:]
def value(name, default=None):
    try: return args[args.index(name) + 1]
    except ValueError: return default

version = value('--pitr-format', 'v4')
wal_mode = value('--wal-io-mode', 'parallel')
handles = {
    name: {'o_direct': name == 'data' and wal_mode == 'parallel', 'device': 7, 'inode': 11}
    for name in ('data', 'frontier', 'sync')
}
pipeline = {'sync_count': 12}
if version == 'v8':
    pipeline.update({
        'wal_io_start': handles, 'wal_io_end': handles, 'wal_io_stable': True,
    })
record = {
    'params': {
        'num': int(value('--num')), 'threads': int(value('--threads')),
        'value_size': int(value('--value-size')), 'batch_size': int(value('--wal-batch-size'))
            if value('--wal-batch-size') else None,
        'wal_format_version': int(version[1:]), 'wal_io_mode': value('--wal-io-mode'),
    },
    'result': {
        'ops': int(value('--num')), 'ops_per_sec': 1000.0,
        'process_cpu_user_ms': 10, 'process_cpu_system_ms': 5,
        'wal_pipeline': pipeline,
    },
    'latency': {'p99_ms': 1.25},
}
print(json.dumps(record))
print('[hotpath] diagnostic report follows')
""", encoding="utf-8")
            fake_binary.chmod(0o755)
            results = root / "results"
            with patch("gcp_wal_profile_runner.shutil.which", return_value=None):
                summary = run_profile_suite(
                    binary=fake_binary, results_dir=results, db_root=root / "dbs",
                    writers=(1,), safe_deadline_epoch=int(time.time()) + 3600,
                    timeout_seconds=5,
                )
            self.assertTrue(summary["complete"])
            self.assertEqual(summary["runs_planned"], 12)
            self.assertEqual(summary["runs_completed"], 12)
            self.assertEqual(summary["perf_captures_completed"], 0)
            with (results / "summary.tsv").open(encoding="utf-8") as stream:
                self.assertEqual(len(list(csv.DictReader(stream, delimiter="\t"))), 12)

    def test_remote_profile_suite_builds_hotpath_binary_and_invokes_uploaded_runner(self) -> None:
        source = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh").read_text(encoding="utf-8")
        self.assertIn('INSTALL_PACKAGES+=(linux-tools-common linux-tools-gcp)', source)
        self.assertIn('BENCH_FEATURES=bench,hotpath-profile', source)
        self.assertIn('cp /tmp/gcp-wal-profile-runner.py "$REPO/scripts/gcp_wal_profile_runner.py"', source)
        self.assertIn('PYTHONPATH="$REPO/scripts${PYTHONPATH:+:$PYTHONPATH}"', source)
        self.assertIn('python3 "$REPO/scripts/gcp_wal_profile_runner.py"', source)
        self.assertIn('"v8-small-ticket-profile"', source)

    def test_remote_profile_probe_allows_missing_or_broken_perf(self) -> None:
        source = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh").read_text(encoding="utf-8")
        start = source.index(
            'if [[ "$BENCH_SUITE" == "v8-small-ticket-profile" ]]; then\n',
            source.index('BENCH_FEATURES=bench,hotpath-profile'),
        )
        finish = source.index('\ncargo "+$TOOLCHAIN" build', start)
        probe = source[start:finish]

        for perf_status in (None, 1, 0):
            with self.subTest(perf_status=perf_status), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                bin_dir = root / "bin"
                bin_dir.mkdir()
                if perf_status is not None:
                    perf = bin_dir / "perf"
                    perf.write_text(
                        f"#!/bin/sh\nprintf 'perf probe output\\n'\nexit {perf_status}\n",
                        encoding="utf-8",
                    )
                    perf.chmod(0o755)
                result = subprocess.run(
                    ["/bin/bash", "-euc", probe],
                    env={
                        "PATH": str(bin_dir), "BENCH_RESULTS": str(root),
                        "BENCH_SUITE": "v8-small-ticket-profile",
                    },
                    text=True, capture_output=True, check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue((root / "perf-version.txt").read_text(encoding="utf-8").strip())
                if perf_status != 0:
                    self.assertIn("continuing", result.stderr)

    def test_small_ticket_profile_preserves_failure_status_with_usable_json(self) -> None:
        row = build_profile_schedule((1,))[0]
        record = {
            "params": {
                "num": row["num"], "threads": 1, "value_size": 1024,
                "batch_size": None, "wal_format_version": 7, "wal_io_mode": "parallel",
            },
            "result": {
                "ops": row["num"], "ops_per_sec": 1000.0,
                "process_cpu_user_ms": 10, "process_cpu_system_ms": 5,
                "wal_pipeline": {"sync_count": 12},
            },
            "latency": {"p99_ms": 1.25},
        }
        for process_status in (1, 124):
            with self.subTest(process_status=process_status), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)

                def fail_after_json(command, **kwargs):
                    json.dump(record, kwargs["stdout"])
                    return subprocess.CompletedProcess(command, process_status)

                with (
                    patch("gcp_wal_profile_runner.shutil.which", return_value="/fake/perf"),
                    patch("gcp_wal_profile_runner.subprocess.run", side_effect=fail_after_json),
                ):
                    metadata = run_profile_one(
                        row, binary=root / "write-perf", result_root=root / "results",
                        db_root=root / "dbs", timeout_seconds=5,
                    )
                self.assertEqual(metadata["exit_code"], process_status)
                self.assertEqual(metadata["perf_status"], f"record_failed:{process_status}")
                self.assertEqual(metadata["ops"], row["num"])
                self.assertEqual(metadata["puts_per_second"], 1000.0)
                self.assertEqual(metadata["p99_ms"], 1.25)
                self.assertEqual(metadata["cpu_seconds"], 0.015)
                self.assertEqual(metadata["sync_count"], 12)
                self.assertEqual(validate_profile_rows([metadata], (1,))["invalid_rows"], [metadata])

    def test_small_ticket_profile_preserves_capture_after_report_failure(self) -> None:
        row = build_profile_schedule((1,))[0]
        record = {
            "params": {
                "num": row["num"], "threads": 1, "value_size": 1024,
                "batch_size": None, "wal_format_version": 7, "wal_io_mode": "parallel",
            },
            "result": {
                "ops": row["num"], "ops_per_sec": 1000.0,
                "process_cpu_user_ms": 10, "process_cpu_system_ms": 5,
                "wal_pipeline": {"sync_count": 12},
            },
            "latency": {"p99_ms": 1.25},
        }
        for failure in ("timeout", "os_error", "nonzero_exit"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)

                def record_then_fail_report(command, **kwargs):
                    if "record" in command:
                        json.dump(record, kwargs["stdout"])
                        Path(command[command.index("-o") + 1]).write_bytes(b"perf capture")
                        return subprocess.CompletedProcess(command, 0)
                    self.assertIn("report", command)
                    if failure == "timeout":
                        raise subprocess.TimeoutExpired(command, 120)
                    if failure == "os_error":
                        raise OSError("perf report unavailable")
                    return subprocess.CompletedProcess(command, 1, "", "report failed")

                with (
                    patch("gcp_wal_profile_runner.build_profile_schedule", return_value=[row]),
                    patch("gcp_wal_profile_runner.shutil.which", return_value="/fake/perf"),
                    patch("gcp_wal_profile_runner.subprocess.run", side_effect=record_then_fail_report),
                    patch("builtins.print"),
                ):
                    summary = run_profile_suite(
                        binary=root / "write-perf", results_dir=root / "results", db_root=root / "dbs",
                        writers=(1,), safe_deadline_epoch=int(time.time()) + 3600, timeout_seconds=5,
                    )
                self.assertTrue(summary["complete"])
                self.assertEqual(summary["perf_captures_completed"], 1)
                self.assertTrue(summary["perf_capture_complete"])
                metadata = summary["results"][0]
                self.assertEqual(metadata["exit_code"], 0)
                self.assertTrue(metadata["perf_status"].startswith("report_failed:"))
                self.assertEqual(metadata["ops"], row["num"])
                self.assertEqual(metadata["puts_per_second"], 1000.0)
                self.assertEqual(metadata["cpu_seconds"], 0.015)
                run_dir = root / "results" / "w1-point-v7_parallel"
                self.assertTrue((run_dir / "perf.data").stat().st_size)
                self.assertTrue((run_dir / "perf-report.txt").read_text(encoding="utf-8"))

    def test_small_ticket_profile_finishes_suite_after_malformed_json(self) -> None:
        schedule = build_profile_schedule((1,))[:2]
        first = schedule[0]
        valid_record = {
            "params": {
                "num": first["num"], "threads": 1, "value_size": 1024,
                "batch_size": None, "wal_format_version": 7, "wal_io_mode": "parallel",
            },
            "result": {
                "ops": first["num"], "ops_per_sec": 1000.0,
                "process_cpu_user_ms": 10, "process_cpu_system_ms": 5,
                "wal_pipeline": {"sync_count": 12},
            },
            "latency": {"p99_ms": 1.25},
        }
        next_record = {
            **valid_record,
            "params": {**valid_record["params"], "wal_format_version": 8},
            "result": {
                **valid_record["result"],
                "wal_pipeline": {
                    "sync_count": 12,
                    "wal_io_start": {
                        name: {"o_direct": name == "data", "device": 7, "inode": 11}
                        for name in ("data", "frontier", "sync")
                    },
                    "wal_io_end": {
                        name: {"o_direct": name == "data", "device": 7, "inode": 11}
                        for name in ("data", "frontier", "sync")
                    },
                    "wal_io_stable": True,
                },
            },
        }
        invalid_records = [
            ("null", None),
            ("array", []),
            ("result", {**valid_record, "result": []}),
            ("params", {**valid_record, "params": []}),
            ("latency", {**valid_record, "latency": []}),
            ("pipeline", {**valid_record, "result": {**valid_record["result"], "wal_pipeline": [1]}}),
            ("ops_string", {**valid_record, "result": {**valid_record["result"], "ops": "invalid"}}),
            ("ops_numeric_string", {**valid_record, "result": {**valid_record["result"], "ops": str(first["num"])}}),
            ("ops_float", {**valid_record, "result": {**valid_record["result"], "ops": float(first["num"])}}),
            ("ops_fractional", {**valid_record, "result": {**valid_record["result"], "ops": first["num"] + 0.5}}),
            ("ops_bool", {**valid_record, "result": {**valid_record["result"], "ops": True}}),
            ("ops_null", {**valid_record, "result": {**valid_record["result"], "ops": None}}),
            ("ops_overflow", {**valid_record, "result": {**valid_record["result"], "ops": float("inf")}}),
            ("cpu_overflow", {**valid_record, "result": {**valid_record["result"], "process_cpu_user_ms": 10 ** 400}}),
        ]
        for label, invalid_record in invalid_records:
            for process_status in (0, 124):
                with self.subTest(payload=label, process_status=process_status), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    records = iter(((invalid_record, process_status), (next_record, 0)))

                    def emit_records(command, **kwargs):
                        record, status = next(records)
                        json.dump(record, kwargs["stdout"])
                        return subprocess.CompletedProcess(command, status)

                    with (
                        patch("gcp_wal_profile_runner.build_profile_schedule", return_value=schedule),
                        patch("gcp_wal_profile_runner.shutil.which", return_value=None),
                        patch("gcp_wal_profile_runner.subprocess.run", side_effect=emit_records),
                        patch("builtins.print"),
                    ):
                        summary = run_profile_suite(
                            binary=root / "write-perf", results_dir=root / "results", db_root=root / "dbs",
                            writers=(1,), safe_deadline_epoch=int(time.time()) + 3600, timeout_seconds=5,
                        )
                    self.assertFalse(summary["complete"])
                    self.assertEqual(summary["runs_completed"], 2)
                    self.assertEqual(summary["results"][0]["exit_code"], process_status or 65)
                    self.assertEqual(summary["results"][1]["exit_code"], 0)
                    self.assertEqual(summary["results"][1]["ops"], schedule[1]["num"])
                    result_root = root / "results"
                    validation_error = result_root / "w1-point-v7_parallel" / "validation-error.txt"
                    self.assertTrue(validation_error.read_text(encoding="utf-8"))
                    saved_summary = json.loads((result_root / "summary.json").read_text(encoding="utf-8"))
                    self.assertEqual(saved_summary["runs_completed"], 2)
                    remote_exit = json.loads((result_root / "remote-exit.json").read_text(encoding="utf-8"))
                    self.assertEqual(remote_exit, {"failed": True, "runs_started": 2})

    def test_small_ticket_profile_perf_child_uses_caller_identity_and_fresh_fallback(self) -> None:
        row = build_profile_schedule((1,))[0]
        record = {
            "params": {
                "num": row["num"], "threads": 1, "value_size": 1024,
                "batch_size": None, "wal_format_version": 7, "wal_io_mode": "parallel",
            },
            "result": {
                "ops": row["num"], "ops_per_sec": 1000.0,
                "process_cpu_user_ms": 10, "process_cpu_system_ms": 5,
                "wal_pipeline": {"sync_count": 12},
            },
            "latency": {"p99_ms": 1.25},
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / "dbs" / "w1-point-v7_parallel"

            def record_then_retry(command, **kwargs):
                if "record" in command:
                    child = command[command.index("--") + 1:]
                    expected = [
                        "setpriv", "--reuid", "1234", "--regid", "2345",
                        "--init-groups", "--reset-env", "--", "env",
                    ]
                    self.assertEqual(child[:len(expected)], expected)
                    self.assertIn(str(root / "write-perf"), child)
                    db_path.mkdir(parents=True)
                    (db_path / "partial.wal").write_bytes(b"unfinished batch")
                    return subprocess.CompletedProcess(command, 1)
                self.assertNotIn("sudo", command)
                self.assertFalse(db_path.exists(), "fallback must start with a fresh database")
                json.dump(record, kwargs["stdout"])
                return subprocess.CompletedProcess(command, 0)

            with (
                patch("gcp_wal_profile_runner.shutil.which", return_value="/fake/perf"),
                patch("gcp_wal_profile_runner.os.getuid", return_value=1234),
                patch("gcp_wal_profile_runner.os.getgid", return_value=2345),
                patch("gcp_wal_profile_runner.subprocess.run", side_effect=record_then_retry) as execute,
            ):
                metadata = run_profile_one(
                    row, binary=root / "write-perf", result_root=root / "results",
                    db_root=root / "dbs", timeout_seconds=5,
                )
            self.assertEqual(execute.call_count, 2)
            self.assertEqual(metadata["exit_code"], 0)
            self.assertEqual(metadata["ops"], row["num"])
            self.assertEqual(metadata["perf_status"], "record_failed:1")

    def test_small_ticket_profile_stops_fallback_when_database_cleanup_fails(self) -> None:
        row = build_profile_schedule((1,))[0]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / "dbs" / "w1-point-v7_parallel"

            def interrupted_record(command, **kwargs):
                self.assertIn("record", command)
                db_path.mkdir(parents=True)
                (db_path / "partial.wal").write_bytes(b"unfinished batch")
                return subprocess.CompletedProcess(command, 1)

            def fail_to_remove(path, ignore_errors=False):
                self.assertEqual(path, db_path)
                if not ignore_errors:
                    raise PermissionError("partial database cleanup denied")

            with (
                patch("gcp_wal_profile_runner.build_profile_schedule", return_value=[row]),
                patch("gcp_wal_profile_runner.shutil.which", return_value="/fake/perf"),
                patch("gcp_wal_profile_runner.shutil.rmtree", side_effect=fail_to_remove),
                patch("gcp_wal_profile_runner.subprocess.run", side_effect=interrupted_record) as execute,
                patch("builtins.print"),
            ):
                summary = run_profile_suite(
                    binary=root / "write-perf", results_dir=root / "results", db_root=root / "dbs",
                    writers=(1,), safe_deadline_epoch=int(time.time()) + 3600, timeout_seconds=5,
                )
            self.assertEqual(execute.call_count, 1)
            self.assertFalse(summary["complete"])
            self.assertEqual(summary["results"][0]["exit_code"], 70)
            self.assertIn("partial database cleanup denied", summary["results"][0]["error"])
            self.assertTrue((root / "results" / "remote-exit.json").is_file())

    def test_small_ticket_profile_fallback_shares_the_original_timeout(self) -> None:
        row = build_profile_schedule((1,))[0]
        for perf_duration in (3, 900, 930):
            with self.subTest(perf_duration=perf_duration), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                clock = [1000.0]
                attempts = []

                def fail_command(command, **kwargs):
                    attempts.append((command, kwargs["timeout"]))
                    if len(attempts) == 1:
                        clock[0] += perf_duration
                        status = 124 if perf_duration >= 900 else 1
                    else:
                        status = 2
                    return subprocess.CompletedProcess(command, status)

                with (
                    patch("gcp_wal_profile_runner.shutil.which", return_value="/fake/perf"),
                    patch("gcp_wal_profile_runner.time.monotonic", side_effect=lambda: clock[0]),
                    patch("gcp_wal_profile_runner.subprocess.run", side_effect=fail_command),
                ):
                    metadata = run_profile_one(
                        row, binary=root / "write-perf", result_root=root / "results",
                        db_root=root / "dbs", timeout_seconds=900,
                    )
                self.assertEqual(float(attempts[0][0][3]), 900.0)
                self.assertEqual(attempts[0][1], 945.0)
                if perf_duration < 900:
                    self.assertEqual(len(attempts), 2)
                    self.assertEqual(float(attempts[1][0][3]), 900.0 - perf_duration)
                    self.assertEqual(attempts[1][1], 945.0 - perf_duration)
                    self.assertEqual(metadata["exit_code"], 2)
                else:
                    self.assertEqual(len(attempts), 1)
                    self.assertEqual(metadata["exit_code"], 124)
                self.assertEqual(metadata["perf_status"], f"record_failed:{124 if perf_duration >= 900 else 1}")

    def test_matrix_validators_reject_missing_duplicate_unexpected_failed_and_wrong_ops(self) -> None:
        full_schedule = build_rfc026_schedule((1,))
        full_rows = [{
            "writer": cell["writer"], "workload": cell["workload"], "repetition": cell["repetition"],
            "arm": cell["arm"], "ops": cell["num"], "exit_code": 0,
        } for cell in full_schedule]
        self.assertTrue(validate_rfc026_rows(full_rows, (1,))["complete"])
        self.assertFalse(validate_rfc026_rows(full_rows[:-1], (1,))["complete"])
        forked = [*full_rows[:-1], full_rows[0]]
        result = validate_rfc026_rows(forked, (1,))
        self.assertFalse(result["complete"])
        self.assertTrue(result["duplicate_cells"])
        self.assertTrue(result["missing_cells"])
        wrong_ops = [*full_rows]
        wrong_ops[0] = {**wrong_ops[0], "ops": 99}
        self.assertFalse(validate_rfc026_rows(wrong_ops, (1,))["complete"])
        unexpected = [*full_rows, {**full_rows[0], "writer": 2}]
        self.assertFalse(validate_rfc026_rows(unexpected, (1,))["complete"])
        failed = [*full_rows]
        failed[0] = {**failed[0], "exit_code": 65}
        self.assertFalse(validate_rfc026_rows(failed, (1,))["complete"])

        legacy_schedule = build_batch64_schedule((2,))
        legacy_rows = [{
            "writer": cell["writer"], "block": cell["block"], "position": cell["position"],
            "format": cell["arm"], "ops": 128, "exit_code": 0,
        } for cell in legacy_schedule]
        self.assertTrue(validate_batch64_rows(legacy_rows, (2,), 128)["complete"])
        self.assertFalse(validate_batch64_rows(legacy_rows[:-1], (2,), 128)["complete"])
        legacy_duplicate = [*legacy_rows[:-1], legacy_rows[0]]
        self.assertFalse(validate_batch64_rows(legacy_duplicate, (2,), 128)["complete"])

    def preflight(self, gcloud, machine_type=BENCH.MACHINE_TYPE):
        with patch.object(BENCH.shutil, "which", return_value="/usr/bin/gcloud"):
            return BENCH.preflight(gcloud, "test-project", "us-central1-a", "default", None, machine_type)

    def test_preflight_accepts_compute_bundled_local_ssd_metadata(self) -> None:
        gcloud = PreflightGcloud()
        report = self.preflight(gcloud)
        self.assertEqual(report["bundled_local_ssds"], {"partitionCount": 1, "defaultInterface": "NVME"})
        self.assertEqual(report["local_ssd_capacity_gib"], 375)
        self.assertTrue(report["machine_type_local_ssd_verified"])
        self.assertIn(["compute", "zones", "describe", "us-central1-a", "--project=test-project"],
                      gcloud.json_calls)
        self.assertTrue(any(call[:3] == ["quotas", "info", "list"] for call in gcloud.json_calls))
        self.assertIn(["compute", "disks", "list", "--project=test-project"], gcloud.json_calls)
        self.assertFalse(any(call[:2] == ["services", "list"] for call in gcloud.json_calls))
        self.assertEqual(report["quotas"]["hyperdisk_balanced_gb"]["limit"], 100)
        self.assertEqual(report["quotas"]["hyperdisk_balanced_gb"]["inventory_used"], 0)

    def test_preflight_accepts_16_vcpu_two_ssd_machine(self) -> None:
        gcloud = PreflightGcloud()
        gcloud.machine = {
            "guestCpus": 16,
            "bundledLocalSsds": {"partitionCount": 2, "defaultInterface": "NVME"},
        }
        gcloud.ssd_limit = 1500
        report = self.preflight(gcloud, "c4-standard-16-lssd")
        self.assertEqual(report["guest_vcpus"], 16)
        self.assertEqual(report["local_ssd_count"], 2)
        self.assertEqual(report["local_ssd_capacity_gib"], 750)
        self.assertEqual(report["quotas"]["c4_cpu"]["required"], 16)
        self.assertEqual(report["quotas"]["c4_local_ssd_gb"]["required"], 750)

    def test_preflight_fails_closed_when_compute_api_is_unavailable(self) -> None:
        class ComputeUnavailable(PreflightGcloud):
            def json(self, command, *, timeout=120):
                if command[:3] == ["compute", "zones", "describe"]:
                    raise BENCH.BenchError("API [compute.googleapis.com] is disabled")
                return super().json(command, timeout=timeout)

        with self.assertRaisesRegex(BENCH.BenchError, "compute.googleapis.com.*disabled"):
            self.preflight(ComputeUnavailable())

    def test_preflight_fails_closed_when_cloud_quotas_api_is_unavailable(self) -> None:
        class CloudQuotasUnavailable(PreflightGcloud):
            def json(self, command, *, timeout=120):
                if command[:3] == ["quotas", "info", "list"]:
                    raise BENCH.BenchError("API [cloudquotas.googleapis.com] is disabled")
                return super().json(command, timeout=timeout)

        with self.assertRaisesRegex(BENCH.BenchError, "cloudquotas.googleapis.com.*disabled"):
            self.preflight(CloudQuotasUnavailable())

    def test_preflight_rejects_unverified_local_ssd_configuration(self) -> None:
        for bundled in (None, {}, {"partitionCount": 2, "defaultInterface": "NVME"},
                        {"partitionCount": 1, "defaultInterface": "SCSI"}):
            with self.subTest(bundled=bundled):
                gcloud = PreflightGcloud()
                gcloud.machine["bundledLocalSsds"] = bundled
                with self.assertRaisesRegex(BENCH.BenchError, "exactly 1 bundled NVMe Local SSD"):
                    self.preflight(gcloud)

    def test_preflight_counts_attached_local_ssds_for_family_and_regional_quotas(self) -> None:
        for family_scoped in (True, False):
            with self.subTest(family_scoped=family_scoped):
                gcloud = PreflightGcloud(family_scoped=family_scoped)
                gcloud.instances = [{
                    "name": "existing", "zone": "us-central1-a", "status": "RUNNING",
                    "machineType": BENCH.MACHINE_TYPE if family_scoped else "n2-standard-4",
                    "disks": [{"type": "PERSISTENT", "diskSizeGb": "50"},
                              {"type": "SCRATCH", "diskSizeGb": "375"}],
                }]
                report = self.preflight(gcloud)
                self.assertEqual(report["quotas"]["c4_local_ssd_gb"]["inventory_used"], 375)
                self.assertEqual(report["quotas"]["c4_local_ssd_gb"]["available"], 375)
                gcloud.ssd_limit = 500
                with self.assertRaisesRegex(BENCH.BenchError, "Insufficient C4 Local SSD quota"):
                    self.preflight(gcloud)

    def test_preflight_rejects_missing_attached_local_ssd_capacity(self) -> None:
        gcloud = PreflightGcloud(family_scoped=False)
        gcloud.instances = [{
            "name": "existing", "zone": "us-central1-a", "machineType": "n2-standard-4",
            "disks": [{"type": "SCRATCH"}],
        }]
        with self.assertRaisesRegex(BENCH.BenchError, "Local SSD diskSizeGb"):
            self.preflight(gcloud)

    def test_quota_family_is_selected_from_dimensions(self) -> None:
        quotas = [{
            "quotaId": "LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY",
            "metric": "compute.googleapis.com/local_ssd_total_storage_per_vm_family",
            "dimensionsInfo": [
                {"dimensions": {"region": "us-central1", "vm_family": "C4"},
                 "details": {"quotaValue": 750}},
                {"dimensions": {"region": "us-central1", "vm_family": "N2"},
                 "details": {"quotaValue": 9000}},
                {"dimensions": {"region": "us-east1", "vm_family": "C4"},
                 "details": {"quotaValue": 8000}},
            ],
        }]
        self.assertEqual(BENCH.quota_limit(quotas, "local_ssd", "us-central1")[0], 750)

    def test_quota_limit_accepts_live_plural_dimensions_and_prefers_family_limit(self) -> None:
        quotas = {"quotaInfos": [
            {
                "quotaId": "CPUS_PER_VM_FAMILY-per-project-region",
                "metric": "compute.googleapis.com/cpus",
                "dimensionsInfos": [{
                    "dimensions": {"vm_family": "C4"},
                    "applicableLocations": ["us-central1"],
                    "details": {"value": "6000"},
                }],
            },
            {
                "quotaId": "LOCAL-SSD-TOTAL-GB-PER-VM-FAMILY-per-project-region",
                "metric": "compute.googleapis.com/local_ssd_total_storage_per_vm_family",
                "dimensionsInfos": [
                    {"dimensions": {"vm_family": "C4"}, "applicableLocations": ["us-central1"],
                     "details": {"value": "6000"}},
                    {"dimensions": {"vm_family": "N2"}, "applicableLocations": ["us-central1"],
                     "details": {"value": "6000"}},
                ],
            },
            {
                "quotaId": "LOCAL-SSD-TOTAL-GB-per-project-region",
                "metric": "compute.googleapis.com/local_ssd_total_storage",
                "dimensionsInfos": [{
                    "applicableLocations": ["us-central1"],
                    "details": {"value": "-1"},
                }],
            },
            {
                "quotaId": "HDB-TOTAL-GB-per-project-region",
                "metric": "compute.googleapis.com/hyperdisk_balanced_total_storage",
                "dimensionsInfos": [{
                    "dimensions": {"region": "us-central1"},
                    "applicableLocations": ["us-central1"],
                    "details": {"value": "500"},
                }],
            },
        ]}

        self.assertEqual(BENCH.quota_limit(quotas, "c4_cpu", "us-central1")[0], 6000)
        self.assertEqual(BENCH.quota_limit(quotas, "local_ssd", "us-central1")[0], 6000)
        self.assertEqual(BENCH.quota_limit(quotas, "hyperdisk_balanced", "us-central1")[0], 500)

    def test_hyperdisk_inventory_counts_balanced_and_doubles_ha_capacity(self) -> None:
        disks = [
            {"name": "zonal", "zone": "https://compute/zones/us-central1-a",
             "type": "https://compute/diskTypes/hyperdisk-balanced", "sizeGb": "30"},
            {"name": "regional-ha", "region": "https://compute/regions/us-central1",
             "type": "https://compute/diskTypes/hyperdisk-balanced-high-availability", "sizeGb": "20"},
            {"name": "other-region", "zone": "https://compute/zones/us-east1-b",
             "type": "https://compute/diskTypes/hyperdisk-balanced", "sizeGb": "500"},
            {"name": "local-ssd", "zone": "https://compute/zones/us-central1-a",
             "type": "https://compute/diskTypes/local-ssd", "sizeGb": "375"},
        ]
        self.assertEqual(BENCH._hyperdisk_balanced_usage_gb(disks, "us-central1"), 70)

    def test_preflight_rejects_hyperdisk_quota_without_enough_inventory_headroom(self) -> None:
        gcloud = PreflightGcloud()
        gcloud.disks = [{
            "name": "existing",
            "zone": "https://compute/zones/us-central1-a",
            "type": "https://compute/diskTypes/hyperdisk-balanced",
            "sizeGb": "60",
        }]
        with self.assertRaisesRegex(BENCH.BenchError, "available=40 GB, need=50 GB"):
            self.preflight(gcloud)

    def test_legacy_regional_local_ssd_quota_is_accepted(self) -> None:
        quotas = [{
            "quotaId": "LOCAL_SSD_TOTAL_GB",
            "metric": "compute.googleapis.com/local_ssd_total_gb",
            "dimensionsInfo": [{
                "dimensions": {"region": "us-central1"},
                "details": {"quotaValue": 750},
            }],
        }]
        _, source = BENCH.quota_limit(quotas, "local_ssd", "us-central1")
        self.assertEqual(source["dimensions"], {"region": "us-central1"})

    def test_existing_benchmark_vm_blocks_a_retry_but_terminated_vm_does_not(self) -> None:
        instances = [
            {"name": "walbench-old-run", "status": "RUNNING"},
            {"name": "walbench-finished", "status": "TERMINATED"},
            {"name": "unrelated", "status": "RUNNING"},
        ]
        self.assertEqual(BENCH._active_benchmark_instances(instances), ["walbench-old-run"])

    def test_resource_deletion_verification_distinguishes_absent_present_and_unknown(self) -> None:
        class FakeGcloud:
            def __init__(self, result):
                self.result = result

            def run(self, _args, *, check, timeout):
                if check or timeout != 120:
                    raise AssertionError((check, timeout))
                if isinstance(self.result, Exception):
                    raise self.result
                return self.result

        absent = BENCH.verify_resource_absent(
            FakeGcloud(subprocess.CompletedProcess([], 1, "", "resource was not found")), ["describe"]
        )
        present = BENCH.verify_resource_absent(
            FakeGcloud(subprocess.CompletedProcess([], 0, "{}", "")), ["describe"]
        )
        unknown = BENCH.verify_resource_absent(
            FakeGcloud(subprocess.CompletedProcess([], 1, "", "permission denied")), ["describe"]
        )
        failed = BENCH.verify_resource_absent(FakeGcloud(BENCH.BenchError("API unavailable")), ["describe"])

        self.assertEqual(absent["state"], "absent")
        self.assertEqual(present["state"], "present")
        self.assertEqual(unknown["state"], "unknown")
        self.assertEqual(failed["state"], "unknown")

    def test_cleanup_deletes_and_verifies_vm_and_firewall(self) -> None:
        class FakeGcloud:
            def __init__(self):
                self.calls = []
                self.responses = [
                    subprocess.CompletedProcess([], 0, "", ""),
                    subprocess.CompletedProcess([], 1, "", "resource not found"),
                    subprocess.CompletedProcess([], 0, "", ""),
                    subprocess.CompletedProcess([], 1, "", "resource not found"),
                ]

            def run(self, args, *, check, timeout):
                self.calls.append(args)
                if check:
                    raise AssertionError("cleanup commands must not short-circuit")
                return self.responses.pop(0)

        gcloud = FakeGcloud()
        messages, verification, failed = BENCH.cleanup_gcp_resources(
            gcloud,
            project="test-project",
            zone="us-central1-a",
            instance="walbench-test",
            firewall="walbench-ssh-test",
            vm_created=True,
            firewall_created=True,
        )

        self.assertFalse(failed)
        self.assertEqual(verification["vm"]["state"], "absent")
        self.assertEqual(verification["firewall"]["state"], "absent")
        self.assertIn("VM deletion verification: absent", messages[1])
        self.assertIn("temporary firewall deletion verification: absent", messages[3])
        self.assertEqual([call[2] for call in gcloud.calls], ["delete", "describe", "delete", "describe"])

    def test_cleanup_reports_unverified_resources_as_failure(self) -> None:
        class FakeGcloud:
            def __init__(self):
                self.responses = [
                    subprocess.CompletedProcess([], 1, "", "permission denied"),
                    subprocess.CompletedProcess([], 0, "{}", ""),
                    subprocess.CompletedProcess([], 0, "", ""),
                    subprocess.CompletedProcess([], 1, "", "resource not found"),
                ]

            def run(self, _args, *, check, timeout):
                if check:
                    raise AssertionError("cleanup commands must not short-circuit")
                return self.responses.pop(0)

        messages, verification, failed = BENCH.cleanup_gcp_resources(
            FakeGcloud(),
            project="test-project",
            zone="us-central1-a",
            instance="walbench-test",
            firewall="walbench-ssh-test",
            vm_created=True,
            firewall_created=True,
        )

        self.assertTrue(failed)
        self.assertEqual(verification["vm"]["state"], "present")
        self.assertEqual(verification["firewall"]["state"], "absent")
        self.assertTrue(any("VM deletion verification: present" in message for message in messages))

    def test_interrupted_vm_verification_does_not_skip_firewall_cleanup(self) -> None:
        class FakeGcloud:
            def __init__(self):
                self.calls = []
                self.responses = [
                    subprocess.CompletedProcess([], 0, "", ""),
                    KeyboardInterrupt(),
                    subprocess.CompletedProcess([], 0, "", ""),
                    subprocess.CompletedProcess([], 1, "", "resource not found"),
                ]

            def run(self, args, *, check, timeout):
                self.calls.append(args)
                result = self.responses.pop(0)
                if isinstance(result, BaseException):
                    raise result
                return result

        gcloud = FakeGcloud()
        _, verification, failed = BENCH.cleanup_gcp_resources(
            gcloud, project="test-project", zone="us-central1-a", instance="walbench-test",
            firewall="walbench-ssh-test", vm_created=True, firewall_created=True,
        )
        self.assertEqual([call[2] for call in gcloud.calls], ["delete", "describe", "delete", "describe"])
        self.assertEqual(verification["vm"], {"state": "unknown", "detail": "KeyboardInterrupt"})
        self.assertEqual(verification["firewall"]["state"], "absent")
        self.assertTrue(failed)

    def test_local_lifecycle_write_failure_cannot_skip_cloud_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp)
            args = BENCH.argparse.Namespace(
                project="test-project", zone="us-central1-a", network="default", subnet=None,
                ssh_source_range="203.0.113.10/32", max_runtime_minutes=60, commit="a" * 40, num=64,
                machine_type=BENCH.MACHINE_TYPE, writers=(1, 4, 8), full_rfc026_matrix=False,
            )
            disk_full = False

            def collect(*_args, **_kwargs):
                nonlocal disk_full
                disk_full = True
                return {
                    "results": True, "results_verified": True,
                    "bootstrap_log": True, "summary_complete": True,
                }

            def fsync(_fd):
                if disk_full:
                    raise OSError("No space left on device")

            success = subprocess.CompletedProcess([], 0, "", "")

            def make_known_hosts(*args, **_kwargs):
                Path(args[5]).write_text("host-key\n", encoding="utf-8")

            with (
                patch.object(BENCH.shutil, "which", return_value="/usr/bin/tool"),
                patch.object(BENCH, "audit_ssh_firewall_rules"),
                patch.object(BENCH, "wait_for_verified_host_keys", side_effect=make_known_hosts),
                patch.object(BENCH.subprocess, "run", side_effect=fake_keygen),
                patch.object(BENCH, "ssh_command", return_value=success),
                patch.object(BENCH, "stream_file_to_vm", return_value=success),
                patch.object(BENCH, "collect_artifacts", side_effect=collect),
                patch.object(BENCH.os, "fsync", side_effect=fsync),
                patch.object(BENCH, "cleanup_gcp_resources", return_value=([], {}, False)) as cleanup,
            ):
                with self.assertRaisesRegex(OSError, "No space left on device"):
                    BENCH.provision_and_run(args, ProvisionGcloud(), results, {"boot_image": {"name": "pinned"}})
                cleanup.assert_called_once()
                self.assertTrue(cleanup.call_args.kwargs["vm_created"])
                self.assertTrue(cleanup.call_args.kwargs["firewall_created"])

    def test_host_key_failure_skips_collection_and_still_deletes_vm(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            args = BENCH.argparse.Namespace(
                project="test-project", zone="us-central1-a", network="default", subnet=None,
                ssh_source_range="203.0.113.10/32", max_runtime_minutes=60, commit="a" * 40, num=64,
                machine_type=BENCH.MACHINE_TYPE, writers=(1, 4, 8), full_rfc026_matrix=False,
            )
            gcloud = ProvisionGcloud()
            with (
                patch.object(BENCH.shutil, "which", return_value="/usr/bin/tool"),
                patch.object(BENCH, "audit_ssh_firewall_rules"),
                patch.object(BENCH, "wait_for_verified_host_keys", side_effect=BENCH.BenchError("host keys unavailable")),
                patch.object(BENCH.subprocess, "run", side_effect=fake_keygen),
                patch.object(BENCH, "ssh_command") as ssh,
                patch.object(BENCH, "collect_artifacts") as collection,
                patch.object(BENCH, "cleanup_gcp_resources", return_value=([], {}, False)) as cleanup,
            ):
                result = BENCH.provision_and_run(
                    args, gcloud, Path(temp), {"boot_image": {"name": "pinned"}}
                )
                self.assertEqual(result, 1)
                create = next(
                    call for call in gcloud.calls
                    if call[:4] == ["beta", "compute", "instances", "create"] and "--help" not in call
                )
                self.assertIn("--no-service-account", create)
                self.assertIn("--no-scopes", create)
                self.assertIn("--boot-disk-provisioned-iops=3000", create)
                self.assertIn("--boot-disk-provisioned-throughput=140", create)
                ssh.assert_not_called()
                collection.assert_not_called()
                cleanup.assert_called_once()
                self.assertTrue(cleanup.call_args.kwargs["vm_created"])
                self.assertFalse(cleanup.call_args.kwargs["firewall_created"])

    def test_incomplete_result_collection_retains_vm_for_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            args = BENCH.argparse.Namespace(
                project="test-project", zone="us-central1-a", network="walbench-net", subnet="walbench-subnet",
                ssh_source_range="203.0.113.10/32", max_runtime_minutes=60, commit="a" * 40, num=64,
                machine_type=BENCH.MACHINE_TYPE, writers=(16,), full_rfc026_matrix=True,
                v8_small_ticket_profile=False, commit_order_trace=False, source_patch_sha256=None,
                keep_vm_after_run=False,
            )
            gcloud = ProvisionGcloud()
            successful_ssh = subprocess.CompletedProcess([], 0, "", "")

            def ssh(_key, _known_hosts, _ip, command, *, timeout):
                if command == "true":
                    return successful_ssh
                return subprocess.CompletedProcess([], 1, "", "summary generation failed")

            def wait_for_host_keys(_gcloud, _instance, _project, _zone, _ip, known_hosts, _deadline):
                known_hosts.write_text("verified host key\n", encoding="utf-8")

            def fail_collection(_ip, key, known_hosts, _results_dir, *, before_connect):
                self.assertTrue(key.is_file())
                self.assertTrue(known_hosts.is_file())
                raise BENCH.BenchError("firewall audit failed before artifact transfer")

            with (
                patch.object(BENCH.shutil, "which", return_value="/usr/bin/tool"),
                patch.object(BENCH, "audit_ssh_firewall_rules"),
                patch.object(BENCH, "wait_for_verified_host_keys", side_effect=wait_for_host_keys),
                patch.object(BENCH.subprocess, "run", side_effect=fake_keygen),
                patch.object(BENCH, "ssh_command", side_effect=ssh),
                patch.object(BENCH, "stream_file_to_vm", return_value=successful_ssh),
                patch.object(BENCH, "collect_artifacts", side_effect=fail_collection),
                patch.object(BENCH, "cleanup_gcp_resources", return_value=([], {}, False)) as cleanup,
            ):
                result = BENCH.provision_and_run(
                    args, gcloud, results, {"boot_image": {"name": "pinned-image"}}
                )

            self.assertEqual(result, 1)
            cleanup.assert_not_called()
            lifecycle = json.loads((results / "lifecycle.json").read_text(encoding="utf-8"))
            self.assertTrue(lifecycle["benchmark_attempted"])
            self.assertTrue(lifecycle["retained"])
            self.assertTrue((results / "retained-ssh-key").is_file())
            self.assertTrue((results / "retained-known-hosts").is_file())
            reconnect = (results / "reconnect.txt").read_text(encoding="utf-8")
            self.assertIn("Do not destroy the VM, firewall, VPC, subnet, route, or Terraform state", reconnect)

    def test_reconnect_instruction_failure_keeps_vm_for_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            args = BENCH.argparse.Namespace(
                project="test-project", zone="us-central1-a", network="walbench-net", subnet="walbench-subnet",
                ssh_source_range="203.0.113.10/32", max_runtime_minutes=60, commit="a" * 40, num=64,
                machine_type=BENCH.MACHINE_TYPE, writers=(16,), full_rfc026_matrix=True,
                v8_small_ticket_profile=False, commit_order_trace=False, source_patch_sha256=None,
                keep_vm_after_run=False,
            )
            gcloud = ProvisionGcloud()
            successful_ssh = subprocess.CompletedProcess([], 0, "", "")

            def ssh(_key, _known_hosts, _ip, command, *, timeout):
                if command == "true":
                    return successful_ssh
                return subprocess.CompletedProcess([], 1, "", "benchmark did not complete")

            def wait_for_host_keys(_gcloud, _instance, _project, _zone, _ip, known_hosts, _deadline):
                known_hosts.write_text("verified host key\n", encoding="utf-8")

            def fail_collection(*_args, **_kwargs):
                raise BENCH.BenchError("firewall audit failed before artifact transfer")

            with (
                patch.object(BENCH.shutil, "which", return_value="/usr/bin/tool"),
                patch.object(BENCH, "audit_ssh_firewall_rules"),
                patch.object(BENCH, "wait_for_verified_host_keys", side_effect=wait_for_host_keys),
                patch.object(BENCH.subprocess, "run", side_effect=fake_keygen),
                patch.object(BENCH, "ssh_command", side_effect=ssh),
                patch.object(BENCH, "stream_file_to_vm", return_value=successful_ssh),
                patch.object(BENCH, "collect_artifacts", side_effect=fail_collection),
                patch.object(BENCH, "retained_vm_reconnect_instructions", side_effect=OSError("disk full")),
                patch.object(BENCH, "cleanup_gcp_resources", return_value=([], {}, False)) as cleanup,
            ):
                result = BENCH.provision_and_run(
                    args, gcloud, results, {"boot_image": {"name": "pinned-image"}}
                )

            self.assertEqual(result, 1)
            cleanup.assert_not_called()
            lifecycle = json.loads((results / "lifecycle.json").read_text(encoding="utf-8"))
            self.assertTrue(lifecycle["retained"])
            self.assertTrue((results / "retained-ssh-key").is_file())
            self.assertFalse((results / "reconnect.txt").exists())

    def test_firewall_audit_rejects_broad_ssh_allow(self) -> None:
        rule = {
            "name": "allow-ssh-from-anywhere",
            "network": "https://compute.googleapis.com/compute/v1/projects/p/global/networks/default",
            "direction": "INGRESS",
            "allowed": [{"IPProtocol": "tcp", "ports": ["22"]}],
            "sourceRanges": ["0.0.0.0/0"],
        }
        self.assertTrue(BENCH._ssh_rule_exceeds_source_range(
            rule, "default", "walbench-12345678", "203.0.113.10/32"
        ))

    def test_firewall_audit_accepts_narrow_or_nonmatching_rules(self) -> None:
        narrow = {
            "network": "https://compute.googleapis.com/compute/v1/projects/p/global/networks/default",
            "direction": "INGRESS",
            "allowed": [{"IPProtocol": "tcp", "ports": ["22"]}],
            "sourceRanges": ["203.0.113.10/32"],
        }
        self.assertFalse(BENCH._ssh_rule_exceeds_source_range(
            narrow, "default", "walbench-12345678", "203.0.113.0/24"
        ))
        broad_tagged = {**narrow, "sourceRanges": ["0.0.0.0/0"], "targetTags": ["other-vm"]}
        self.assertFalse(BENCH._ssh_rule_exceeds_source_range(
            broad_tagged, "default", "walbench-12345678", "203.0.113.10/32"
        ))

    def test_firewall_rule_with_omitted_ports_allows_ssh(self) -> None:
        rule = {
            "network": "default",
            "direction": "INGRESS",
            "allowed": [{"IPProtocol": "tcp"}],
            "sourceRanges": ["0.0.0.0/0"],
        }
        self.assertTrue(BENCH._ssh_rule_exceeds_source_range(
            rule, "default", "walbench-12345678", "203.0.113.10/32"
        ))

    def test_firewall_audit_handles_numeric_tcp_and_effective_policy_rules(self) -> None:
        rule = {
            "action": "allow",
            "direction": "INGRESS",
            "match": {
                "srcIpRanges": ["0.0.0.0/0"],
                "layer4Configs": [{"ipProtocol": "6", "ports": ["22"]}],
            },
        }
        self.assertTrue(BENCH._effective_ssh_rule_exceeds_source_range(
            rule, "default", "walbench-12345678", "203.0.113.10/32"
        ))
        narrow = {**rule, "match": {**rule["match"], "srcIpRanges": ["203.0.113.10/32"]}}
        self.assertFalse(BENCH._effective_ssh_rule_exceeds_source_range(
            narrow, "default", "walbench-12345678", "203.0.113.0/24"
        ))

    def test_effective_firewall_parser_accepts_empty_inventory_and_policy_rules(self) -> None:
        self.assertEqual(BENCH._effective_firewall_rules({"firewalls": [], "firewallPolicys": []}), [])
        self.assertEqual(BENCH._effective_firewall_rules({}), [])
        policy = {"firewallPolicys": [{"rules": [{"action": "deny"}]}]}
        self.assertEqual(BENCH._effective_firewall_rules(policy), [{"action": "deny"}])

    def test_firewall_audit_handles_snake_case_policy_output(self) -> None:
        flattened_policy_rule = {
            "action": "allow",
            "direction": "INGRESS",
            "src_ip_ranges": ["0.0.0.0/0"],
            "layer4_configs": [{"ip_protocol": "6", "ports": ["22"]}],
        }
        self.assertTrue(BENCH._effective_ssh_rule_exceeds_source_range(
            flattened_policy_rule, "default", "walbench-12345678", "203.0.113.10/32"
        ))

    def test_firewall_audit_checks_regional_effective_policy(self) -> None:
        broad_rule = {
            "action": "allow",
            "direction": "INGRESS",
            "match": {
                "srcIpRanges": ["0.0.0.0/0"],
                "layer4Configs": [{"ipProtocol": "6", "ports": ["22"]}],
            },
        }

        class FakeGcloud:
            def json(self, command):
                if command[:3] == ["compute", "firewall-rules", "list"]:
                    return []
                if command[:3] == ["compute", "networks", "get-effective-firewalls"]:
                    return {"firewalls": [], "firewallPolicys": []}
                if command[:3] == ["compute", "network-firewall-policies", "get-effective-firewalls"]:
                    return {"firewallPolicys": [{"rules": [broad_rule]}]}
                raise AssertionError(command)

        with self.assertRaises(BENCH.BenchError):
            BENCH.audit_ssh_firewall_rules(
                FakeGcloud(), "project", "default", "us-central1", "walbench-12345678", "203.0.113.10/32"
            )

    def test_host_key_parser_accepts_guest_attribute_and_openssh_forms(self) -> None:
        algorithm = b"ssh-ed25519"
        blob = struct.pack(">I", len(algorithm)) + algorithm + b"public-key-body"
        encoded = base64.b64encode(blob).decode("ascii")
        guest_response = {"guestAttributes": [
            {"queryPath": "hostkeys/ssh-ed25519", "value": encoded},
        ]}
        openssh_response = {"guestAttributes": [
            {"queryPath": "hostkeys/ssh-ed25519/0", "value": f"ssh-ed25519 {encoded} vm"},
        ]}
        gcloud_list_response = [{"namespace": "hostkeys", "key": "ssh-ed25519", "value": encoded}]
        expected = [f"192.0.2.1 ssh-ed25519 {encoded}"]
        self.assertEqual(BENCH._host_key_lines(guest_response, "192.0.2.1"), expected)
        self.assertEqual(BENCH._host_key_lines(openssh_response, "192.0.2.1"), expected)
        self.assertEqual(BENCH._host_key_lines(gcloud_list_response, "192.0.2.1"), expected)

    def test_host_key_parser_rejects_algorithm_mismatch(self) -> None:
        algorithm = b"ssh-ed25519"
        blob = struct.pack(">I", len(algorithm)) + algorithm + b"public-key-body"
        encoded = base64.b64encode(blob).decode("ascii")
        response = {"guestAttributes": [
            {"queryPath": "hostkeys/ssh-rsa", "value": encoded},
        ]}
        self.assertEqual(BENCH._host_key_lines(response, "192.0.2.1"), [])

    def test_gcloud_command_log_does_not_store_response_bodies(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "gcloud.jsonl"
            gcloud = BENCH.Gcloud("project", path)
            completed = subprocess.CompletedProcess(
                args=["gcloud", "compute", "instances", "list"],
                returncode=0,
                stdout='[{"metadata":{"private":"secret"}}]',
                stderr="diagnostic-secret",
            )
            with patch.object(BENCH.subprocess, "run", return_value=completed) as mock_run:
                gcloud.run(["compute", "instances", "list"])
            child_env = mock_run.call_args.kwargs["env"]
            self.assertEqual(child_env["CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API"], "false")
            logged = json.loads(path.read_text(encoding="utf-8"))
            self.assertNotIn("secret", path.read_text(encoding="utf-8"))
            self.assertEqual(logged["stdout_bytes"], len(completed.stdout.encode("utf-8")))

    def test_gcloud_json_retries_transient_read_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            gcloud = BENCH.Gcloud("project", Path(temp) / "gcloud.jsonl")
            responses = [
                subprocess.CompletedProcess([], 1, "", "ConnectionError: RemoteDisconnected"),
                subprocess.CompletedProcess([], 0, '{"status":"UP"}', ""),
            ]
            with (
                patch.object(BENCH.subprocess, "run", side_effect=responses) as mock_run,
                patch.object(BENCH.time, "sleep") as sleep,
            ):
                self.assertEqual(gcloud.json(["compute", "zones", "describe", "zone-a"]), {"status": "UP"})

            self.assertEqual(mock_run.call_count, 2)
            sleep.assert_called_once_with(1)

    def test_gcloud_json_does_not_retry_nontransient_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            gcloud = BENCH.Gcloud("project", Path(temp) / "gcloud.jsonl")
            failure = subprocess.CompletedProcess([], 403, "", "Permission denied")
            with (
                patch.object(BENCH.subprocess, "run", return_value=failure) as mock_run,
                patch.object(BENCH.time, "sleep") as sleep,
            ):
                with self.assertRaisesRegex(BENCH.BenchError, "Permission denied"):
                    gcloud.json(["compute", "zones", "describe", "zone-a"])

            mock_run.assert_called_once()
            sleep.assert_not_called()

    def test_remote_archive_script_runs_and_keeps_diagnostics_and_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            results = root / "remote" / "results"
            run_dir = results / "r1-w16-point-v8_parallel"
            run_dir.mkdir(parents=True)
            for name in (
                "profile-schedule.tsv", "rust-toolchain.txt", "write-perf.sha256", "perf-version.txt",
            ):
                (results / name).write_text("fixture\n", encoding="utf-8")
            for name in (
                "stderr.log", "timeline.json", "perf.data", "perf-report.txt", "sync-observations.json",
                "completion-timeline.json", "completion-handoff-timeline.json",
            ):
                (run_dir / name).write_bytes(b"capture")
            (run_dir / "ignored.db").write_bytes(b"should not be bundled")

            command = BENCH._build_remote_archive_command(root=str(results), max_bytes=1024 * 1024)
            result = self._run_remote_bundle(command, results, root)
            self.assertEqual(result.returncode, 0, result.stderr)
            metadata = self._archive_metadata(result.stdout)
            self.assertEqual(metadata["ARCHIVE_FILES"], "11")
            archive = Path(metadata["ARCHIVE_PATH"])
            self.assertEqual(archive.parent, root / "remote-tmp")
            with tarfile.open(archive, "r:gz") as bundle:
                names = set(bundle.getnames())
            for name in (
                "results/profile-schedule.tsv", "results/rust-toolchain.txt",
                "results/write-perf.sha256", "results/perf-version.txt",
                "results/r1-w16-point-v8_parallel/stderr.log", "results/r1-w16-point-v8_parallel/timeline.json",
                "results/r1-w16-point-v8_parallel/perf.data", "results/r1-w16-point-v8_parallel/perf-report.txt",
                "results/r1-w16-point-v8_parallel/sync-observations.json",
                "results/r1-w16-point-v8_parallel/completion-timeline.json",
                "results/r1-w16-point-v8_parallel/completion-handoff-timeline.json",
            ):
                self.assertIn(name, names)
            self.assertFalse(any(name.endswith("ignored.db") for name in names))

            too_small = BENCH._build_remote_archive_command(root=str(results), max_bytes=1)
            rejected = self._run_remote_bundle(too_small, results, root)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("exceeds 1 byte limit", rejected.stderr)

    def test_remote_cpu_attribution_runner_receives_requested_repetitions(self) -> None:
        remote_script = Path(BENCH.__file__).with_name("gcp-free-trial-wal-bench-remote.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('--repetitions "$BENCH_REPETITIONS"', remote_script)

    def test_collection_retries_and_preserves_partial_bundle_for_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            remote_results = self._make_result_bundle(root, complete=False)
            key = root / "key"
            known_hosts = root / "known_hosts"
            key.write_text("private", encoding="utf-8")
            known_hosts.write_text("host-key", encoding="utf-8")
            calls = 0
            events: list[str] = []
            archives: dict[str, Path] = {}

            def pack(_key, _known_hosts, _ip, command, *, timeout):
                self.assertEqual(events[-1], "audit")
                events.append("ssh")
                packed = self._run_remote_bundle(command, remote_results, root)
                self.assertEqual(packed.returncode, 0, packed.stderr)
                metadata = self._archive_metadata(packed.stdout)
                archives[metadata["ARCHIVE_PATH"]] = Path(metadata["ARCHIVE_PATH"])
                return packed

            def transfer(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                nonlocal calls
                self.assertFalse(recursive)
                self.assertEqual(events[-1], "audit")
                events.append("ssh-stream")
                if remote.endswith(".tar.gz"):
                    calls += 1
                    if calls == 1:
                        return subprocess.CompletedProcess([], 1, "", "temporary transfer failure")
                    shutil.copyfile(archives[remote], destination)
                    return subprocess.CompletedProcess([], 0, "", "")
                destination.write_text("bootstrap details", encoding="utf-8")
                return subprocess.CompletedProcess([], 0, "", "")

            with (
                patch.object(BENCH, "ssh_command", side_effect=pack),
                patch.object(BENCH, "stream_file_from_vm", side_effect=transfer),
                patch.object(BENCH.time, "sleep"),
            ):
                result = BENCH.collect_artifacts(
                    "192.0.2.1", key, known_hosts, root, before_connect=lambda: events.append("audit")
                )
            self.assertTrue(result["results"])
            self.assertFalse(result["summary_complete"])
            self.assertFalse(result["results_verified"])
            self.assertTrue((root / "collected" / "results" / "summary.json").is_file())
            self.assertTrue((root / "collection-attempt-02" / "results.tar.gz").is_file())
            self.assertTrue((root / "collection-attempt-02" / "bundle-metadata.json").is_file())
            self.assertEqual(calls, 2)
            self.assertEqual(
                events,
                ["audit", "ssh", "audit", "ssh-stream", "audit", "ssh", "audit", "ssh-stream", "audit", "ssh-stream"],
            )

    def test_collection_rejects_complete_summary_with_missing_run_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            remote_results = self._make_result_bundle(root, complete=True, include_run_metadata=False)
            archives: dict[str, Path] = {}

            def pack(_key, _known_hosts, _ip, command, *, timeout):
                packed = self._run_remote_bundle(command, remote_results, root)
                metadata = self._archive_metadata(packed.stdout)
                archives[metadata["ARCHIVE_PATH"]] = Path(metadata["ARCHIVE_PATH"])
                return packed

            def transfer(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                self.assertFalse(recursive)
                if remote in archives:
                    shutil.copyfile(archives[remote], destination)
                else:
                    destination.write_text("bootstrap details", encoding="utf-8")
                return subprocess.CompletedProcess([], 0, "", "")

            with (
                patch.object(BENCH, "ssh_command", side_effect=pack),
                patch.object(BENCH, "stream_file_from_vm", side_effect=transfer),
                patch.object(BENCH.time, "sleep"),
            ):
                result = BENCH.collect_artifacts(
                    "192.0.2.1", root / "key", root / "known_hosts", root, before_connect=lambda: None
                )

            self.assertTrue(result["results"])
            self.assertFalse(result["results_verified"])
            attempts = json.loads((root / "artifact-collection.json").read_text(encoding="utf-8"))["attempts"]
            self.assertEqual(len(attempts), 2)
            self.assertIn(
                "missing result.json or run-metadata.json",
                " ".join(attempts[0]["validation"]["errors"]),
            )
            self.assertTrue((root / "collection-attempt-01" / "results.tar.gz").is_file())

    def test_collection_retries_after_copying_an_unverified_result_set(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            remote_results = self._make_result_bundle(root, complete=False)
            archives: dict[str, Path] = {}
            pack_count = 0

            def pack(_key, _known_hosts, _ip, command, *, timeout):
                nonlocal pack_count
                pack_count += 1
                if pack_count == 2:
                    summary_path = remote_results / "summary.json"
                    summary = json.loads(summary_path.read_text(encoding="utf-8"))
                    summary["complete"] = True
                    summary["matrix_validation"]["complete"] = True
                    for row in summary["results"]:
                        row["exit_code"] = 0
                        run_id = BENCH._summary_row_identifier(row, "batch64")
                        metadata_path = remote_results / run_id / "run-metadata.json"
                        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                        metadata["exit_code"] = 0
                        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
                    summary_path.write_text(json.dumps(summary), encoding="utf-8")
                    (remote_results / "remote-exit.json").write_text(
                        json.dumps({"failed": False, "runs_started": summary["runs_planned"]}),
                        encoding="utf-8",
                    )
                packed = self._run_remote_bundle(command, remote_results, root)
                self.assertEqual(packed.returncode, 0, packed.stderr)
                metadata = self._archive_metadata(packed.stdout)
                archives[metadata["ARCHIVE_PATH"]] = Path(metadata["ARCHIVE_PATH"])
                return packed

            def transfer(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                self.assertFalse(recursive)
                if remote in archives:
                    shutil.copyfile(archives[remote], destination)
                else:
                    destination.write_text("bootstrap details", encoding="utf-8")
                return subprocess.CompletedProcess([], 0, "", "")

            with (
                patch.object(BENCH, "ssh_command", side_effect=pack),
                patch.object(BENCH, "stream_file_from_vm", side_effect=transfer),
                patch.object(BENCH.time, "sleep"),
            ):
                result = BENCH.collect_artifacts(
                    "192.0.2.1", root / "key", root / "known_hosts", root, before_connect=lambda: None
                )

            self.assertTrue(result["results_verified"])
            self.assertEqual(pack_count, 2)
            collection = json.loads((root / "artifact-collection.json").read_text(encoding="utf-8"))
            self.assertEqual([attempt["results_verified"] for attempt in collection["attempts"]], [False, True])
            for attempt in (1, 2):
                staging = root / f"collection-attempt-{attempt:02d}"
                self.assertTrue((staging / "results.tar.gz").is_file())
                bundle_metadata = json.loads((staging / "bundle-metadata.json").read_text(encoding="utf-8"))
                self.assertTrue(bundle_metadata["bundle_checksum_verified"])

    def test_result_verifier_checks_per_run_pairing_and_remote_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = self._make_result_bundle(Path(temp), complete=True)
            schedule = build_batch64_schedule((16,))
            first_id = BENCH._run_identifier(schedule[0], "batch64")
            extra_dir = results / "w99-extra"
            extra_dir.mkdir()
            shutil.move(results / first_id / "run-metadata.json", extra_dir / "run-metadata.json")
            invalid_pairing = BENCH._verify_result_set(results)
            self.assertFalse(invalid_pairing["verified"])
            self.assertTrue(any("run directories do not match schedule" in error for error in invalid_pairing["errors"]))

            shutil.move(extra_dir / "run-metadata.json", results / first_id / "run-metadata.json")
            extra_dir.rmdir()
            (results / "remote-exit.json").write_text(json.dumps({"failed": True, "runs_started": 12}), encoding="utf-8")
            invalid_exit = BENCH._verify_result_set(results)
            self.assertFalse(invalid_exit["verified"])
            self.assertTrue(any("remote exit record reports failure" in error for error in invalid_exit["errors"]))

            (results / "remote-exit.json").write_text(
                json.dumps({"failed": False, "runs_started": len(schedule) - 1}), encoding="utf-8"
            )
            mismatched_exit = BENCH._verify_result_set(results)
            self.assertFalse(mismatched_exit["verified"])
            self.assertTrue(any("remote exit run count does not match the plan" in error
                                for error in mismatched_exit["errors"]))

    def test_run_directory_validation_accepts_w_and_r_ids_and_reports_missing_and_extra(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp)
            expected_ids = {
                "w16-point-r1-v7_parallel",
                "r1-w8-point-v7_parallel",
            }
            for run_id in expected_ids:
                (results / run_id).mkdir()

            self.assertEqual(BENCH._run_directory_delta(results, expected_ids), ([], []))

            (results / "r1-w8-point-v7_parallel").rmdir()
            (results / "r2-w16-point-v8_candidate").mkdir()
            missing, unexpected = BENCH._run_directory_delta(results, expected_ids)
            self.assertEqual(missing, ["r1-w8-point-v7_parallel"])
            self.assertEqual(unexpected, ["r2-w16-point-v8_candidate"])

    def test_result_verifier_accepts_csv_string_exit_code_in_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = self._make_result_bundle(Path(temp), complete=True)
            summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
            self.assertTrue(all(row["exit_code"] == "0" for row in summary["results"]))

            schedule = build_batch64_schedule((16,))
            run_id = BENCH._run_identifier(schedule[0], "batch64")
            metadata = json.loads((results / run_id / "run-metadata.json").read_text(encoding="utf-8"))
            self.assertIs(type(metadata["exit_code"]), int)
            self.assertTrue(BENCH._verify_result_set(results)["verified"])

    def test_profile_result_verification_uses_schedule_and_requires_perf_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            results.mkdir()
            schedule = build_profile_schedule((1,))
            with (results / "profile-schedule.tsv").open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t")
                writer.writeheader()
                writer.writerows(schedule)
            rows = []
            captures = 0
            for item in schedule:
                run_id = BENCH._run_identifier(item, "v8-small-ticket-profile")
                run_dir = results / run_id
                run_dir.mkdir()
                status = "captured" if item["perf_capture"] else "not_requested"
                if item["perf_capture"]:
                    captures += 1
                    (run_dir / "perf.data").write_bytes(b"perf")
                    (run_dir / "perf-report.txt").write_text("report", encoding="utf-8")
                (run_dir / "result.json").write_text(
                    json.dumps({"result": {}, "params": {}, "latency": {}}) + "\n[hotpath] sample\n",
                    encoding="utf-8",
                )
                row = {
                    "writer": item["writer"], "workload": item["workload"], "arm": item["arm"],
                    "exit_code": 0, "ops": item["num"], "perf_status": status,
                }
                rows.append(row)
                (run_dir / "run-metadata.json").write_text(json.dumps(row), encoding="utf-8")
            (results / "summary.json").write_text(json.dumps({
                "complete": True, "suite": "v8-small-ticket-profile", "runs_planned": len(schedule),
                "runs_completed": len(schedule), "results": rows,
                "matrix_validation": {"complete": True, "planned": len(schedule), "observed": len(schedule)},
                "perf_captures_planned": captures, "perf_captures_completed": captures,
                "perf_capture_complete": True,
            }), encoding="utf-8")
            (results / "remote-exit.json").write_text(
                json.dumps({"failed": False, "runs_started": len(schedule)}), encoding="utf-8"
            )
            self.assertTrue(BENCH._verify_result_set(results)["verified"])
            capture_row = next(row for row in schedule if row["perf_capture"])
            capture_id = BENCH._run_identifier(capture_row, "v8-small-ticket-profile")
            (results / capture_id / "perf.data").unlink()
            rejected = BENCH._verify_result_set(results)
            self.assertFalse(rejected["verified"])
            self.assertTrue(any("profile capture artifacts are incomplete" in error for error in rejected["errors"]))

    def test_collection_rejects_corrupt_archive_transfer(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            remote_results = self._make_result_bundle(root, complete=True)
            archives: dict[str, Path] = {}

            def pack(_key, _known_hosts, _ip, command, *, timeout):
                packed = self._run_remote_bundle(command, remote_results, root)
                metadata = self._archive_metadata(packed.stdout)
                archives[metadata["ARCHIVE_PATH"]] = Path(metadata["ARCHIVE_PATH"])
                return packed

            def transfer(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                if remote in archives:
                    shutil.copyfile(archives[remote], destination)
                    with destination.open("ab") as stream:
                        stream.write(b"corrupted transfer")
                else:
                    destination.write_text("bootstrap details", encoding="utf-8")
                return subprocess.CompletedProcess([], 0, "", "")

            with (
                patch.object(BENCH, "ssh_command", side_effect=pack),
                patch.object(BENCH, "stream_file_from_vm", side_effect=transfer),
                patch.object(BENCH.time, "sleep"),
            ):
                result = BENCH.collect_artifacts(
                    "192.0.2.1", root / "key", root / "known_hosts", root, before_connect=lambda: None
                )
            self.assertFalse(result["results_verified"])
            attempts = json.loads((root / "artifact-collection.json").read_text(encoding="utf-8"))["attempts"]
            self.assertEqual(len(attempts), 2)
            self.assertFalse(attempts[0]["bundle_checksum_verified"])
            self.assertTrue((root / "collection-attempt-01" / "results.tar.gz").is_file())

    def test_collection_keeps_bootstrap_log_when_results_are_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            key = root / "key"
            known_hosts = root / "known_hosts"
            key.write_text("private", encoding="utf-8")
            known_hosts.write_text("host-key", encoding="utf-8")

            def transfer(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                self.assertFalse(recursive)
                self.assertEqual(remote, "/var/log/gcp-free-trial-wal-bench.log")
                destination.write_text(
                    "bootstrap details\nWAL_BENCHMARK_STARTED 2026-11-04T10:00:00Z\n", encoding="utf-8"
                )
                return subprocess.CompletedProcess([], 0, "", "")

            with (
                patch.object(
                    BENCH,
                    "ssh_command",
                    return_value=subprocess.CompletedProcess([], 1, "", "Local SSD setup failed"),
                ),
                patch.object(BENCH, "stream_file_from_vm", side_effect=transfer),
                patch.object(BENCH.time, "sleep"),
            ):
                result = BENCH.collect_artifacts("192.0.2.1", key, known_hosts, root, before_connect=lambda: None)

            self.assertFalse(result["results"])
            self.assertTrue(result["bootstrap_log"])
            self.assertTrue(result["benchmark_started"])
            self.assertIn("WAL_BENCHMARK_STARTED", (root / "bootstrap.log").read_text(encoding="utf-8"))

    def test_collection_stops_connecting_when_firewall_audit_rejects_access(self) -> None:
        for rejected_audit in (1, 2):
            with self.subTest(rejected_audit=rejected_audit), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                events = []
                results = self._make_result_bundle(root, complete=True)
                archives: dict[str, Path] = {}

                def audit():
                    events.append("audit")
                    if events.count("audit") == rejected_audit:
                        raise BENCH.BenchError("SSH firewall policy changed")

                def pack(_key, _known_hosts, _ip, command, *, timeout):
                    events.append("ssh")
                    result = self._run_remote_bundle(command, results, root)
                    metadata = self._archive_metadata(result.stdout)
                    archives[metadata["ARCHIVE_PATH"]] = Path(metadata["ARCHIVE_PATH"])
                    return result

                def transfer(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                    events.append("ssh-stream")
                    self.assertFalse(recursive)
                    if remote in archives:
                        shutil.copyfile(archives[remote], destination)
                    else:
                        destination.write_text("boot log", encoding="utf-8")
                    return subprocess.CompletedProcess([], 0, "", "")

                with (
                    patch.object(BENCH, "ssh_command", side_effect=pack),
                    patch.object(BENCH, "stream_file_from_vm", side_effect=transfer),
                ):
                    with self.assertRaisesRegex(BENCH.BenchError, "SSH firewall policy changed"):
                        BENCH.collect_artifacts(
                            "192.0.2.1", root / "key", root / "known_hosts", root, before_connect=audit
                        )
                self.assertEqual(
                    events,
                    ["audit"] if rejected_audit == 1 else ["audit", "ssh", "audit"],
                )
                if rejected_audit == 2:
                    self.assertFalse((root / "collected" / "results" / "summary.json").exists())

    def test_results_directory_is_private_under_permissive_umask(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            BENCH.create_private_results_directory(results)
            self.assertEqual(stat.S_IMODE(results.stat().st_mode), 0o700)

    def test_remote_run_parser_allows_missing_profile_counters(self) -> None:
        remote_script = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh")
        source = remote_script.read_text(encoding="utf-8")
        self.assertIn('echo "WAL_BENCHMARK_STARTED $(date --iso-8601=seconds)" | tee -a "$BENCH_BOOT_LOG"', source)
        marker = 'python3 - "$RUN_DIR" "$WRITERS" "$BLOCK" "$POSITION" "$FORMAT" "$EXIT_CODE" '
        parser_start = source.index(marker)
        heredoc_start = source.index("<<'PY'\n", parser_start) + len("<<'PY'\n")
        parser_end = source.index("\nPY\n", heredoc_start)
        parser = source[heredoc_start:parser_end]
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp) / "run"
            run_dir.mkdir()
            (run_dir / "result.json").write_text(json.dumps({
                "result": {
                    "ops": 64,
                    "ops_per_sec": 1000,
                    "process_cpu_user_ms": 10,
                    "process_cpu_system_ms": 5,
                },
                "params": {
                    "num": 64, "threads": 8, "value_size": 1024, "batch_size": 8,
                    "wal_format_version": 8, "wal_io_mode": "parallel",
                },
                "latency": {"p99_ms": 1.2},
            }), encoding="utf-8")
            with (run_dir.parent / "summary.tsv").open("w", encoding="utf-8") as stream:
                stream.write(
                    "writer\tblock\tposition\tformat\texit_code\tops\tputs_per_second\t"
                    "p99_ms\tcpu_seconds\tsync_count\tduration_seconds\n"
                )
            result = subprocess.run(
                ["python3", "-", str(run_dir), "8", "1", "1", "v8", "0", "1.0", "64"],
                input=parser,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            row = (run_dir.parent / "summary.tsv").read_text(encoding="utf-8").splitlines()[1]
            fields = row.split("\t")
            self.assertEqual(fields[5], "64")
            self.assertEqual(float(fields[6]), 1000.0)
            self.assertEqual(float(fields[7]), 1.2)
            self.assertAlmostEqual(float(fields[8]), 0.015)
            self.assertEqual(fields[9], "")
            self.assertAlmostEqual(float(fields[10]), 1.0)

            # Batch size is capped by the largest per-writer operation partition.
            record_path = run_dir / "result.json"
            record = json.loads(record_path.read_text(encoding="utf-8"))
            record["params"]["batch_size"] = 64
            record_path.write_text(json.dumps(record), encoding="utf-8")
            rejected = subprocess.run(
                ["python3", "-", str(run_dir), "8", "1", "2", "v8", "0", "1.0", "64"],
                input=parser,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(rejected.returncode, 65)

    def test_remote_summary_completes_with_only_the_benchmark_environment(self) -> None:
        source = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh").read_text(encoding="utf-8")
        start = source.index(
            'python3 - "$BENCH_RESULTS" "$FAILED" "$RUN_INDEX" "$BENCH_WRITERS" "$BENCH_NUM" <<\'PY\''
        )
        finish = source.index("\nBENCH_USER_SCRIPT\n", start)
        script = "if false; then :; else\n" + source[start:finish]
        orders = (("v4", "v7", "v8", "v8", "v7", "v4"), ("v8", "v7", "v4", "v4", "v7", "v8"))
        rows = []
        for writers in LEGACY_DEFAULT_WRITERS:
            repetitions = dict.fromkeys(("v4", "v7", "v8"), 0)
            for block, order in enumerate(orders, 1):
                for position, fmt in enumerate(order, 1):
                    repetitions[fmt] += 1
                    factor = {"v4": 1, "v7": 2, "v8": 3}[fmt]
                    rows.append({
                        "writer": writers, "block": block, "position": position, "format": fmt,
                        "exit_code": 0, "ops": 64,
                        "puts_per_second": 100 * repetitions[fmt] * factor,
                        "p99_ms": {"v4": 8, "v7": 4, "v8": 2}[fmt],
                        "cpu_seconds": {"v4": 0.8, "v7": 0.6, "v8": 0.4}[fmt],
                        "sync_count": "", "duration_seconds": 1,
                    })

        for failed in (False, True):
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as temp:
                results = Path(temp)
                selected = rows if not failed else [*rows[:4], {**rows[4], "exit_code": 65}]
                with (results / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=rows[0], delimiter="\t")
                    writer.writeheader()
                    writer.writerows(selected)
                result = subprocess.run(
                    ["bash", "-euc", script],
                    env={
                        "PATH": os.environ["PATH"], "BENCH_RESULTS": str(results),
                        "FAILED": str(int(failed)), "RUN_INDEX": str(len(selected)),
                        "BENCH_WRITERS": "1,4,8", "BENCH_NUM": "64",
                        "PYTHONPATH": str(Path(__file__).parent),
                    },
                    capture_output=True, text=True, check=False,
                )
                self.assertEqual(result.returncode, int(failed), result.stderr)
                summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
                self.assertEqual(summary["complete"], not failed)
                self.assertEqual(summary["runs_completed"], len(selected))
                stats = summary["comparison_by_writer"]["1"]
                self.assertEqual(stats["formats"]["v4"]["median_puts_per_second"], 250 if not failed else 100)
                self.assertIsNone(stats["formats"]["v8"]["median_sync_count"])
                if not failed:
                    self.assertAlmostEqual(stats["relative_to_v4"]["v7"]["throughput_change_pct_vs_v4"], 100)
                    self.assertAlmostEqual(stats["relative_to_v4"]["v8"]["p99_change_pct_vs_v4"], -75)
                    self.assertAlmostEqual(stats["relative_to_v4"]["v8"]["cpu_change_pct_vs_v4"], -50)
                else:
                    self.assertEqual(stats["formats"]["v7"]["runs"], 1)
                with (results / "comparison.tsv").open(encoding="utf-8") as stream:
                    comparisons = list(csv.DictReader(stream, delimiter="\t"))
                self.assertEqual(len(comparisons), 9)
                self.assertTrue((results / "finished_utc.txt").is_file())
                self.assertEqual(
                    json.loads((results / "remote-exit.json").read_text(encoding="utf-8")),
                    {"failed": failed, "runs_started": len(selected)},
                )

    def test_rfc026_run_plan_uses_lf_for_shell_reader(self) -> None:
        source = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh").read_text(encoding="utf-8")
        start = source.index('python3 - "$RUN_PLAN" "$BENCH_WRITERS" <<\'PY\'')
        finish = source.index("\nPY\n", start) + len("\nPY")
        script = source[start:finish]

        with tempfile.TemporaryDirectory() as temp:
            plan_path = Path(temp) / "rfc026-schedule.tsv"
            result = subprocess.run(
                ["bash", "-euc", script],
                env={
                    "PATH": os.environ["PATH"],
                    "PYTHONPATH": str(Path(__file__).parent),
                    "RUN_PLAN": str(plan_path),
                    "BENCH_WRITERS": "16",
                },
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            content = plan_path.read_text(encoding="utf-8")
            self.assertNotIn("\r", content)
            rows = list(csv.DictReader(content.splitlines(), delimiter="\t"))
            self.assertEqual(len(rows), len(build_rfc026_schedule((16,))))
            self.assertEqual(rows[0]["sample_every"], "100")

    def test_full_rfc026_summary_accepts_all_320_runs(self) -> None:
        source = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh").read_text(encoding="utf-8")
        self.assertIn('IFS=\',\' read -r -a WRITER_COUNTS <<<"$BENCH_WRITERS"', source)
        self.assertIn('while IFS=$\'\\t\' read -r PLAN_INDEX WRITER_COUNT WORKLOAD REPETITION ARM', source)
        self.assertIn("build_rfc026_schedule", source)
        self.assertIn('if (( RUN_INDEX != EXPECTED_RUNS )); then', source)
        start = source.index('python3 - "$BENCH_RESULTS" "$FAILED" "$RUN_INDEX" "$BENCH_WRITERS"')
        finish = source.index('\nPY\n    date -u +%Y-%m-%dT%H:%M:%SZ >"$BENCH_RESULTS/finished_utc.txt"', start)
        script = source[start:finish + len("\nPY")]
        schedule = build_rfc026_schedule(FULL_DEFAULT_WRITERS)
        self.assertEqual(len(schedule), 320)
        rows = []
        for item in schedule:
            arm_index = ("v4_parallel", "v7_parallel", "v8_parallel", "v8_leader").index(item["arm"]) + 1
            rows.append({
                "writer": item["writer"], "workload": item["workload"],
                "repetition": item["repetition"], "arm": item["arm"],
                "exit_code": 0, "ops": item["num"], "puts_per_second": 100 * arm_index,
                "p99_ms": 4 / arm_index, "cpu_seconds": 1 / arm_index,
                "sync_count": 10 * arm_index, "duration_seconds": 1,
            })

        scenarios = (
            ("complete", rows, 320, True),
            ("missing", rows[:-1], 319, False),
            ("duplicate", [*rows[:-1], rows[0]], 320, False),
        )
        for name, selected, run_index, expected_complete in scenarios:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                results = Path(temp)
                with (results / "summary.tsv").open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=rows[0], delimiter="\t")
                    writer.writeheader()
                    writer.writerows(selected)
                result = subprocess.run(
                    ["bash", "-euc", script],
                    env={
                        "PATH": os.environ["PATH"], "PYTHONPATH": str(Path(__file__).parent),
                        "BENCH_RESULTS": str(results), "FAILED": "0",
                        "RUN_INDEX": str(run_index), "BENCH_WRITERS": "1,2,4,8,16",
                    },
                    capture_output=True, text=True, check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
                self.assertEqual(summary["complete"], expected_complete)
                self.assertEqual(summary["runs_planned"], 320)
                self.assertEqual(summary["runs_completed"], len(selected))
                self.assertEqual(summary["writers"], [1, 2, 4, 8, 16])
                if expected_complete:
                    cell = summary["comparison_by_cell"]["w16-batch64"]
                    self.assertEqual(cell["v4_parallel"]["puts_per_second"], 100)
                    self.assertEqual(cell["v8_parallel"]["puts_per_second"], 300)
                else:
                    self.assertFalse(summary["matrix_validation"]["complete"])
                    if name == "duplicate":
                        self.assertTrue(summary["matrix_validation"]["duplicate_cells"])
                        self.assertTrue(summary["matrix_validation"]["missing_cells"])

    def test_remote_rfc026_scheduler_emits_exact_tabular_plan(self) -> None:
        source = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh").read_text(encoding="utf-8")
        marker = 'python3 - "$RUN_PLAN" "$BENCH_WRITERS" <<\'PY\'\n'
        start = source.index(marker) + len(marker)
        finish = source.index("\nPY\n", start)
        generator = source[start:finish]
        with tempfile.TemporaryDirectory() as temp:
            plan_path = Path(temp) / "schedule.tsv"
            result = subprocess.run(
                ["python3", "-", str(plan_path), ",".join(map(str, FULL_DEFAULT_WRITERS))],
                input=generator,
                text=True,
                capture_output=True,
                check=False,
                env={"PATH": os.environ["PATH"], "PYTHONPATH": str(Path(__file__).parent)},
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            with plan_path.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream, delimiter="\t"))
            self.assertEqual(len(rows), 320)
            self.assertEqual(rows[0]["workload"], "point")
            self.assertEqual(rows[0]["batch_size"], "none")
            self.assertEqual(rows[-1]["writer"], "16")
            self.assertEqual(rows[-1]["workload"], "batch64")
            self.assertEqual(rows[-1]["batch_size"], "64")


if __name__ == "__main__":
    unittest.main()
