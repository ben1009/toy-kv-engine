"""Unit tests for safety-sensitive pieces of the GCP benchmark runner."""

from __future__ import annotations

import base64
import csv
import importlib.util
import json
import os
from pathlib import Path
import struct
import subprocess
import stat
import tempfile
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
    def test_writer_defaults_are_suite_specific_and_duplicates_are_rejected(self) -> None:
        common = ["--project", "project", "--zone", "us-central1-a", "--commit", "a" * 40, "--preflight-only"]
        legacy = BENCH.parse_args(common)
        full = BENCH.parse_args([*common, "--full-rfc026-matrix"])
        self.assertEqual(legacy.writers, LEGACY_DEFAULT_WRITERS)
        self.assertEqual(full.writers, FULL_DEFAULT_WRITERS)
        explicit = BENCH.parse_args([*common, "--writers", "2", "16"])
        self.assertEqual(explicit.writers, (2, 16))
        with self.assertRaises(SystemExit):
            BENCH.parse_args([*common, "--writers", "2", "2"])

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
                return {"results": True, "bootstrap_log": True, "summary_complete": True}

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
                ssh.assert_not_called()
                collection.assert_not_called()
                cleanup.assert_called_once()
                self.assertTrue(cleanup.call_args.kwargs["vm_created"])
                self.assertFalse(cleanup.call_args.kwargs["firewall_created"])

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

    def test_collection_retries_and_keeps_a_transferred_partial_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            key = root / "key"
            known_hosts = root / "known_hosts"
            key.write_text("private", encoding="utf-8")
            known_hosts.write_text("host-key", encoding="utf-8")
            calls = 0
            events = []

            def scp(_ip, _remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                nonlocal calls
                self.assertEqual(events[-1], "audit")
                events.append("scp")
                if recursive:
                    calls += 1
                    if calls == 1:
                        return subprocess.CompletedProcess([], 1, "", "temporary transfer failure")
                    result_dir = destination / "results"
                    result_dir.mkdir(parents=True)
                    (result_dir / "summary.json").write_text('{"complete": false}\n', encoding="utf-8")
                    return subprocess.CompletedProcess([], 0, "", "")
                destination.write_text("boot log", encoding="utf-8")
                return subprocess.CompletedProcess([], 0, "", "")

            with patch.object(BENCH, "scp_from_vm", side_effect=scp), patch.object(BENCH.time, "sleep"):
                result = BENCH.collect_artifacts(
                    "192.0.2.1", key, known_hosts, root, before_connect=lambda: events.append("audit")
                )
            self.assertTrue(result["results"])
            self.assertFalse(result["summary_complete"])
            self.assertTrue((root / "collected" / "results" / "summary.json").is_file())
            self.assertEqual(calls, 2)
            self.assertEqual(events, ["audit", "scp"] * 3)

    def test_collection_keeps_bootstrap_log_when_results_are_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            key = root / "key"
            known_hosts = root / "known_hosts"
            key.write_text("private", encoding="utf-8")
            known_hosts.write_text("host-key", encoding="utf-8")

            def scp(_ip, remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                if recursive:
                    return subprocess.CompletedProcess([], 1, "", "Local SSD setup failed")
                self.assertEqual(remote, "/var/log/gcp-free-trial-wal-bench.log")
                destination.write_text(
                    "bootstrap details\nWAL_BENCHMARK_STARTED 2026-11-04T10:00:00Z\n", encoding="utf-8"
                )
                return subprocess.CompletedProcess([], 0, "", "")

            with patch.object(BENCH, "scp_from_vm", side_effect=scp), patch.object(BENCH.time, "sleep"):
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

                def audit():
                    events.append("audit")
                    if events.count("audit") == rejected_audit:
                        raise BENCH.BenchError("SSH firewall policy changed")

                def scp(_ip, _remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                    events.append("scp")
                    self.assertTrue(recursive)
                    result_dir = destination / "results"
                    result_dir.mkdir()
                    (result_dir / "summary.json").write_text('{"complete": true}\n', encoding="utf-8")
                    return subprocess.CompletedProcess([], 0, "", "")

                with patch.object(BENCH, "scp_from_vm", side_effect=scp):
                    with self.assertRaisesRegex(BENCH.BenchError, "SSH firewall policy changed"):
                        BENCH.collect_artifacts(
                            "192.0.2.1", root / "key", root / "known_hosts", root, before_connect=audit
                        )
                self.assertEqual(events, ["audit"] if rejected_audit == 1 else ["audit", "scp", "audit"])
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
