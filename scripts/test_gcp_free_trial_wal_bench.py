"""Unit tests for safety-sensitive pieces of the GCP benchmark runner."""

from __future__ import annotations

import base64
import csv
import hashlib
import importlib.util
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
import unittest
from unittest.mock import patch

from gcp_wal_matrix import (
    FULL_DEFAULT_WRITERS,
    LEGACY_DEFAULT_WRITERS,
    build_benchmark_plan,
    build_batch64_schedule,
    build_rfc026_schedule,
    validate_batch64_rows,
    validate_rfc026_rows,
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


SCRIPT = Path(__file__).with_name("gcp-free-trial-wal-bench.py")
SPEC = importlib.util.spec_from_file_location("gcp_free_trial_wal_bench", SCRIPT)
assert SPEC and SPEC.loader
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


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


class GcpFreeTrialWalBenchTests(unittest.TestCase):
    def _make_result_bundle(
        self, root: Path, *, complete: bool, include_run_metadata: bool = True,
    ) -> tuple[Path, str]:
        source_root = root / "source"
        results = source_root / "results"
        run_dir = results / "w16-point-r1-v7_parallel"
        run_dir.mkdir(parents=True)
        (results / "summary.json").write_text(
            json.dumps({"complete": complete, "runs_planned": 1, "runs_completed": int(complete)}),
            encoding="utf-8",
        )
        (results / "summary.tsv").write_text("writer\tworkload\n16\tpoint\n", encoding="utf-8")
        (results / "benchmark-plan.json").write_text('{"runs_planned":1}\n', encoding="utf-8")
        (results / "remote-exit.json").write_text(
            json.dumps({"failed": not complete, "runs_started": 1}), encoding="utf-8"
        )
        (results / "rfc026-schedule.tsv").write_text("", encoding="utf-8")
        (run_dir / "result.json").write_text('{"result":{"ops_per_sec":123}}\n', encoding="utf-8")
        if include_run_metadata:
            (run_dir / "run-metadata.json").write_text(
                json.dumps({"exit_code": 0 if complete else 1}), encoding="utf-8"
            )
        archive = root / "results.tar.gz"
        with tarfile.open(archive, "w:gz") as bundle:
            for path in results.rglob("*"):
                if path.is_file():
                    bundle.add(path, arcname=path.relative_to(source_root).as_posix())
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        return archive, digest

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
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--writers", "2", "2"])
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--v8-small-ticket-profile", "--full-rfc026-matrix"])

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
            with (
                patch.object(BENCH.shutil, "which", return_value="/usr/bin/tool"),
                patch.object(BENCH, "audit_ssh_firewall_rules"),
                patch.object(BENCH, "wait_for_verified_host_keys"),
                patch.object(BENCH.subprocess, "run", side_effect=fake_keygen),
                patch.object(BENCH, "ssh_command", return_value=success),
                patch.object(BENCH, "scp_to_vm", return_value=success),
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
                v8_small_ticket_profile=False, keep_vm_after_run=False,
            )
            gcloud = ProvisionGcloud()
            successful_ssh = subprocess.CompletedProcess([], 0, "", "")

            def ssh(_key, _known_hosts, _ip, command, *, timeout):
                if command == "true":
                    return successful_ssh
                return subprocess.CompletedProcess([], 1, "", "summary generation failed")

            def wait_for_host_keys(_gcloud, _instance, _project, _zone, _ip, known_hosts, _deadline):
                known_hosts.write_text("verified host key\n", encoding="utf-8")

            incomplete = {
                "results": False,
                "results_verified": False,
                "summary_complete": False,
                "bootstrap_log": True,
                "benchmark_started": True,
            }
            with (
                patch.object(BENCH.shutil, "which", return_value="/usr/bin/tool"),
                patch.object(BENCH, "audit_ssh_firewall_rules"),
                patch.object(BENCH, "wait_for_verified_host_keys", side_effect=wait_for_host_keys),
                patch.object(BENCH.subprocess, "run", side_effect=fake_keygen),
                patch.object(BENCH, "ssh_command", side_effect=ssh),
                patch.object(BENCH, "scp_to_vm", return_value=successful_ssh),
                patch.object(BENCH, "collect_artifacts", return_value=incomplete),
                patch.object(BENCH, "cleanup_gcp_resources") as cleanup,
            ):
                result = BENCH.provision_and_run(
                    args, gcloud, results, {"boot_image": {"name": "pinned-image"}}
                )

            self.assertEqual(result, 1)
            cleanup.assert_not_called()
            lifecycle = json.loads((results / "lifecycle.json").read_text(encoding="utf-8"))
            self.assertTrue(lifecycle["benchmark_attempted"])
            self.assertTrue(lifecycle["benchmark_started"])
            self.assertTrue(lifecycle["retained"])
            self.assertTrue((results / "retained-ssh-key").is_file())
            reconnect = (results / "reconnect.txt").read_text(encoding="utf-8")
            self.assertIn("Do not destroy the VM, firewall, VPC, subnet, route, or Terraform state", reconnect)

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

    def test_collection_retries_and_keeps_a_transferred_partial_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            key = root / "key"
            known_hosts = root / "known_hosts"
            key.write_text("private", encoding="utf-8")
            known_hosts.write_text("host-key", encoding="utf-8")
            archive, digest = self._make_result_bundle(root, complete=False)
            calls = 0
            events = []

            def pack(_key, _known_hosts, _ip, _command, *, timeout):
                self.assertEqual(events[-1], "audit")
                events.append("ssh")
                return subprocess.CompletedProcess([], 0, f"ARCHIVE_SHA256={digest}\n", "")

            def scp(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                nonlocal calls
                self.assertFalse(recursive)
                self.assertEqual(events[-1], "ssh" if remote.endswith(".tar.gz") else "audit")
                events.append("scp")
                if remote.endswith(".tar.gz"):
                    calls += 1
                    if calls == 1:
                        return subprocess.CompletedProcess([], 1, "", "temporary transfer failure")
                    shutil.copyfile(archive, destination)
                    return subprocess.CompletedProcess([], 0, "", "")
                destination.write_text("boot log", encoding="utf-8")
                return subprocess.CompletedProcess([], 0, "", "")

            with (
                patch.object(BENCH, "ssh_command", side_effect=pack),
                patch.object(BENCH, "scp_from_vm", side_effect=scp),
                patch.object(BENCH.time, "sleep"),
            ):
                result = BENCH.collect_artifacts(
                    "192.0.2.1", key, known_hosts, root, before_connect=lambda: events.append("audit")
                )
            self.assertTrue(result["results"])
            self.assertFalse(result["summary_complete"])
            self.assertFalse(result["results_verified"])
            self.assertTrue((root / "collected" / "results" / "w16-point-r1-v7_parallel" / "result.json").is_file())
            self.assertEqual(calls, 2)
            self.assertEqual(events, ["audit", "ssh", "scp", "audit", "ssh", "scp", "audit", "scp"])

    def test_collection_rejects_complete_summary_with_missing_run_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive, digest = self._make_result_bundle(root, complete=True, include_run_metadata=False)

            def pack(_key, _known_hosts, _ip, _command, *, timeout):
                return subprocess.CompletedProcess([], 0, f"ARCHIVE_SHA256={digest}\n", "")

            def scp(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                self.assertFalse(recursive)
                if remote.endswith(".tar.gz"):
                    shutil.copyfile(archive, destination)
                else:
                    destination.write_text("bootstrap details", encoding="utf-8")
                return subprocess.CompletedProcess([], 0, "", "")

            with (
                patch.object(BENCH, "ssh_command", side_effect=pack),
                patch.object(BENCH, "scp_from_vm", side_effect=scp),
                patch.object(BENCH.time, "sleep"),
            ):
                result = BENCH.collect_artifacts(
                    "192.0.2.1", root / "key", root / "known_hosts", root, before_connect=lambda: None
                )

            self.assertTrue(result["results"])
            self.assertFalse(result["results_verified"])
            attempt = json.loads((root / "artifact-collection.json").read_text(encoding="utf-8"))["attempts"][0]
            self.assertEqual(attempt["run_metadata_count"], 0)
            self.assertEqual(attempt["result_json_count"], 1)

    def test_collection_keeps_bootstrap_log_when_results_are_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            key = root / "key"
            known_hosts = root / "known_hosts"
            key.write_text("private", encoding="utf-8")
            known_hosts.write_text("host-key", encoding="utf-8")

            def scp(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
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
                patch.object(BENCH, "scp_from_vm", side_effect=scp),
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
                archive, digest = self._make_result_bundle(root, complete=True)

                def audit():
                    events.append("audit")
                    if events.count("audit") == rejected_audit:
                        raise BENCH.BenchError("SSH firewall policy changed")

                def pack(_key, _known_hosts, _ip, _command, *, timeout):
                    events.append("ssh")
                    return subprocess.CompletedProcess([], 0, f"ARCHIVE_SHA256={digest}\n", "")

                def scp(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                    events.append("scp")
                    self.assertFalse(recursive)
                    if remote.endswith(".tar.gz"):
                        shutil.copyfile(archive, destination)
                    else:
                        destination.write_text("boot log", encoding="utf-8")
                    return subprocess.CompletedProcess([], 0, "", "")

                with (
                    patch.object(BENCH, "ssh_command", side_effect=pack),
                    patch.object(BENCH, "scp_from_vm", side_effect=scp),
                ):
                    with self.assertRaisesRegex(BENCH.BenchError, "SSH firewall policy changed"):
                        BENCH.collect_artifacts(
                            "192.0.2.1", root / "key", root / "known_hosts", root, before_connect=audit
                        )
                self.assertEqual(
                    events,
                    ["audit"] if rejected_audit == 1 else ["audit", "ssh", "scp", "audit"],
                )
                if rejected_audit == 2:
                    self.assertTrue((root / "collected" / "results" / "summary.json").is_file())

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
