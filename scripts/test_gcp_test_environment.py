#!/usr/bin/env python3
"""Exercise the generic test-host preflight without GCP credentials or requests."""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "infra/gcp/test-environment/preflight.py"
SPEC = importlib.util.spec_from_file_location("gcp_test_environment_preflight", SCRIPT)
PREFLIGHT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREFLIGHT)
REGION = "us-central1"
ZONE = "us-central1-a"


def quota(quota_id, value, *, family=None):
    dimensions = {"region": REGION}
    if family:
        dimensions["vm_family"] = family
    return {"quotaId": quota_id, "dimensionsInfos": [
        {"dimensions": dimensions, "details": {"quotaValue": value}},
    ]}


def instance(name, machine, *, status="RUNNING", zone=ZONE, ssd_count=1):
    return {"name": name, "machineType": machine, "status": status,
            "zone": f"projects/test-project/zones/{zone}",
            "disks": [{"type": "PERSISTENT"}] + [
                {"type": "SCRATCH", "diskSizeGb": "375"} for _ in range(ssd_count)
            ]}


class GcloudInventory:
    """Allow only the read-only requests used by the preflight contract."""

    def __init__(self):
        self.commands = []
        self.account = "test@example.com"
        self.project = {"projectId": "test-project", "lifecycleState": "ACTIVE"}
        self.zone = {"status": "UP"}
        self.machines = {
            f"c4-standard-{cpus}-lssd": {
                "guestCpus": cpus,
                "bundledLocalSsds": {"partitionCount": cpus // 8, "defaultInterface": "NVME"},
            } for cpus in (8, 16)
        }
        self.quotas = [quota("CPUS-per-project-region-vm-family", 32, family="C4"),
                       quota("LOCAL_SSD_TOTAL_GB-per-project-region-vm-family", 2250, family="C4"),
                       quota("HDB-TOTAL-GB-per-project-region", 500)]
        self.instances = []
        self.reservations = []
        self.disks = []
        self.region = {"status": "UP", "quotas": [
            {"metric": "INSTANCES", "limit": 10, "usage": 0},
            {"metric": "IN_USE_ADDRESSES", "limit": 4, "usage": 0},
        ]}
        self.global_info = {"quotas": [
            {"metric": "NETWORKS", "limit": 5, "usage": 0},
            {"metric": "FIREWALLS", "limit": 10, "usage": 0},
            {"metric": "CPUS_ALL_REGIONS", "limit": 32, "usage": 0},
        ]}

    def __call__(self, command, **kwargs):
        self.commands.append(command)
        if command[:2] == ["auth", "list"]:
            return self.account
        if command[:2] == ["projects", "describe"]:
            return deepcopy(self.project)
        if command[:3] == ["compute", "machine-types", "describe"]:
            return deepcopy(self.machines[command[3]])
        replies = {
            ("compute", "zones", "describe"): self.zone,
            ("quotas", "info", "list"): {"quotaInfos": self.quotas},
            ("compute", "instances", "list"): self.instances,
            ("compute", "reservations", "list"): self.reservations,
            ("compute", "regions", "describe"): self.region,
            ("compute", "disks", "list"): self.disks,
            ("compute", "project-info", "describe"): self.global_info,
        }
        key = tuple(command[:3])
        if key not in replies:
            raise AssertionError(f"unexpected or mutating gcloud request: {command}")
        return deepcopy(replies[key])


class GcpTestEnvironmentPreflightTests(unittest.TestCase):
    def run_preflight(self, inventory, machine="c4-standard-16-lssd"):
        with patch.object(PREFLIGHT, "_run_gcloud", side_effect=inventory):
            return PREFLIGHT.preflight("test-project", ZONE, machine, 50)

    def test_both_machine_shapes_pass_with_read_only_inventory(self):
        for cpus, disks in ((8, 1), (16, 2)):
            with self.subTest(cpus=cpus):
                inventory = GcloudInventory()
                report = self.run_preflight(inventory, f"c4-standard-{cpus}-lssd")
                self.assertEqual(report["guest_vcpus"], cpus)
                self.assertEqual(report["local_ssd"]["count"], disks)
                self.assertEqual(report["local_ssd"]["required_quota_gb"], disks * 375)
                self.assertEqual(report["quotas"]["hyperdisk_balanced"]["required_gb"], 50)
                self.assertFalse(report["billing_mutated"])
                self.assertFalse(report["actual_free_host_capacity_guaranteed"])
                self.assertNotIn("subnetworks", report["quotas"])
                quota_command = next(cmd for cmd in inventory.commands if cmd[:2] == ["quotas", "info"])
                self.assertIn("--billing-project=test-project", quota_command)

    def test_unlimited_quota_sentinels_are_reported_without_numeric_limits(self):
        """Verify unlimited quota reports use explicit flags and null numeric values."""
        inventory = GcloudInventory()
        for record in inventory.quotas:
            record["dimensionsInfos"][0]["details"]["quotaValue"] = -1
        for response in (inventory.region, inventory.global_info):
            for item in response["quotas"]:
                item["limit"] = -1

        report = self.run_preflight(inventory)

        for quota in ("c4_cpu", "local_ssd", "hyperdisk_balanced", "instances",
                      "in_use_addresses", "networks", "firewalls", "cpus_all_regions"):
            with self.subTest(quota=quota):
                details = report["quotas"][quota]
                self.assertTrue(details["unlimited"])
                limit_key = "limit_gb" if quota == "hyperdisk_balanced" else "limit"
                available_key = "available_gb" if quota == "hyperdisk_balanced" else "available"
                self.assertIsNone(details[limit_key])
                self.assertIsNone(details[available_key])

    def test_insufficient_quota_rejects_provisioning_for_each_resource(self):
        cases = [("cloud", 0, "C4 CPU"), ("cloud", 1, "Local SSD"),
                 ("cloud", 2, "Hyperdisk"), ("regional", 0, "INSTANCES"),
                 ("regional", 1, "IN_USE_ADDRESSES"), ("global", 0, "NETWORKS"),
                 ("global", 1, "FIREWALLS"), ("global", 2, "global CPU")]
        for scope, index, message in cases:
            with self.subTest(scope=scope, index=index):
                inventory = GcloudInventory()
                if scope == "cloud":
                    inventory.quotas[index]["dimensionsInfos"][0]["details"]["quotaValue"] = 0
                else:
                    response = inventory.region if scope == "regional" else inventory.global_info
                    response["quotas"][index]["usage"] = response["quotas"][index]["limit"]
                with self.assertRaisesRegex(PREFLIGHT.PreflightError, message):
                    self.run_preflight(inventory)

    def test_missing_ambiguous_and_invalid_quota_responses_fail_closed(self):
        """Reject missing, ambiguous, and invalid quota responses."""
        for case in ("missing_cpu", "wrong_family", "negative", "nan", "missing_limit", "duplicate_regional"):
            with self.subTest(case=case):
                inventory = GcloudInventory()
                if case == "missing_cpu":
                    inventory.quotas.pop(0)
                elif case == "wrong_family":
                    inventory.quotas[0]["dimensionsInfos"][0]["dimensions"]["vm_family"] = "N2"
                elif case in {"negative", "nan", "missing_limit"}:
                    inventory.quotas[0]["dimensionsInfos"][0]["details"]["quotaValue"] = {
                    "negative": -2, "nan": "NaN", "missing_limit": None,
                    }[case]
                else:
                    inventory.region["quotas"].append(deepcopy(inventory.region["quotas"][0]))
                with self.assertRaises(PREFLIGHT.PreflightError):
                    self.run_preflight(inventory)

    def test_strictest_matching_family_quota_wins(self):
        inventory = GcloudInventory()
        inventory.quotas += [quota("CPUS", 1000, family="N2"), quota("CPUS", 8, family="C4")]
        with self.assertRaisesRegex(PREFLIGHT.PreflightError, "C4 CPU"):
            self.run_preflight(inventory)

    def test_family_ssd_quota_is_not_replaced_by_a_larger_general_limit(self):
        inventory = GcloudInventory()
        inventory.quotas[1] = quota("LOCAL_SSD_TOTAL_GB", 500, family="C4")
        inventory.quotas.append(quota("LOCAL_SSD_TOTAL_GB", 10000))
        with self.assertRaisesRegex(PREFLIGHT.PreflightError, "Local SSD"):
            self.run_preflight(inventory)

    def test_only_the_global_all_regions_cpu_quota_can_be_absent(self):
        inventory = GcloudInventory()
        inventory.global_info["quotas"].pop()
        self.assertFalse(self.run_preflight(inventory)["quotas"]["cpus_all_regions"]["applicable"])
        inventory.global_info["quotas"].pop()
        with self.assertRaisesRegex(PREFLIGHT.PreflightError, "FIREWALLS"):
            self.run_preflight(inventory)

    def test_region_locations_can_supply_quota_when_region_dimension_is_absent(self):
        inventory = GcloudInventory()
        for item in inventory.quotas:
            dimension = item["dimensionsInfos"][0]
            del dimension["dimensions"]["region"]
            dimension["applicableLocations"] = [REGION]
        self.assertEqual(self.run_preflight(inventory)["guest_vcpus"], 16)

    def test_inventory_counts_running_cpu_and_retained_local_ssds(self):
        inventory = GcloudInventory()
        inventory.instances = [instance("running", "c4-standard-8-lssd"),
                               instance("stopped", "c4-standard-16-lssd", status="TERMINATED", ssd_count=2),
                               instance("elsewhere", "c4-standard-16-lssd", zone="us-east1-b", ssd_count=2)]
        report = self.run_preflight(inventory)
        self.assertEqual(report["existing_region_instance_count"], 2)
        self.assertEqual(report["quotas"]["c4_cpu"]["inventory_used"], 8)
        self.assertEqual(report["quotas"]["local_ssd"]["inventory_used"], 1125)

    def test_shared_local_ssd_quota_includes_other_vm_families(self):
        inventory = GcloudInventory()
        inventory.quotas[1] = quota("LOCAL_SSD_TOTAL_GB-per-project-region", 750)
        inventory.instances = [instance("other-family", "n2-standard-8")]
        with self.assertRaisesRegex(PREFLIGHT.PreflightError, "Local SSD"):
            self.run_preflight(inventory)
        inventory.quotas[1] = quota("LOCAL_SSD_TOTAL_GB-per-project-region-vm-family", 750, family="C4")
        self.assertEqual(self.run_preflight(inventory)["quotas"]["local_ssd"]["inventory_used"], 0)

    def test_hyperdisk_inventory_counts_regional_replicas_and_ignores_other_regions(self):
        inventory = GcloudInventory()
        inventory.disks = [
            {"type": "hyperdisk-balanced", "zone": ZONE, "sizeGb": "50"},
            {"type": "hyperdisk-balanced-high-availability", "region": REGION, "sizeGb": "60"},
            {"type": "hyperdisk-balanced", "zone": "us-east1-b", "sizeGb": "1000"},
            {"type": "pd-balanced", "zone": ZONE, "sizeGb": "1000"},
        ]
        self.assertEqual(self.run_preflight(inventory)["quotas"]["hyperdisk_balanced"]["inventory_used_gb"], 170)

    def test_ambiguous_reservations_reject_preflight(self):
        for family, shared_ssd in (("c4-standard-8-lssd", False), ("n2-standard-8", True), (None, False)):
            with self.subTest(family=family, shared_ssd=shared_ssd):
                inventory = GcloudInventory()
                properties = {"machineType": family} if family else {}
                inventory.reservations = [{"name": "reserved", "zone": ZONE,
                                           "specificReservation": {"instanceProperties": properties}}]
                if shared_ssd:
                    inventory.quotas[1] = quota("LOCAL_SSD_TOTAL_GB-per-project-region", 2250)
                with self.assertRaises(PREFLIGHT.PreflightError):
                    self.run_preflight(inventory)

    def test_machine_and_project_readiness_are_required(self):
        for case in ("accounts", "project", "zone", "region", "no_ssd", "scsi", "no_cpu"):
            with self.subTest(case=case):
                inventory = GcloudInventory()
                if case == "accounts":
                    inventory.account = ""
                elif case == "project":
                    inventory.project["lifecycleState"] = "DELETE_REQUESTED"
                elif case in {"zone", "region"}:
                    getattr(inventory, case)["status"] = "DOWN"
                else:
                    machine = inventory.machines["c4-standard-16-lssd"]
                    if case == "no_ssd":
                        machine["bundledLocalSsds"]["partitionCount"] = 0
                    elif case == "scsi":
                        machine["bundledLocalSsds"]["defaultInterface"] = "SCSI"
                    else:
                        machine["guestCpus"] = 0
                with self.assertRaises(PREFLIGHT.PreflightError):
                    self.run_preflight(inventory)

    def test_missing_inventory_sizes_are_not_assumed_to_be_zero(self):
        for case in ("local_ssd", "hyperdisk"):
            with self.subTest(case=case):
                inventory = GcloudInventory()
                if case == "local_ssd":
                    inventory.instances = [instance("missing-size", "c4-standard-8-lssd")]
                    del inventory.instances[0]["disks"][1]["diskSizeGb"]
                else:
                    inventory.disks = [{"type": "hyperdisk-balanced", "zone": ZONE}]
                with self.assertRaises(PREFLIGHT.PreflightError):
                    self.run_preflight(inventory)

    def test_cli_writes_report_and_returns_failure_without_output_on_error(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "report.json"
            args = ["--project", "test-project", "--zone", ZONE, "--output", str(output)]
            with patch.object(PREFLIGHT, "preflight", return_value={"billing_mutated": False}), redirect_stdout(io.StringIO()):
                self.assertEqual(PREFLIGHT.main(args), 0)
            self.assertEqual(json.loads(output.read_text()), {"billing_mutated": False})
            output.unlink()
            with patch.object(PREFLIGHT, "preflight", side_effect=PREFLIGHT.PreflightError("quota denied")), redirect_stderr(io.StringIO()):
                self.assertEqual(PREFLIGHT.main(args), 2)
            self.assertFalse(output.exists())

    def test_cli_rejects_invalid_boot_sizes_before_requesting_gcp_inventory(self):
        for size in ("19", "101", "50.5"):
            with self.subTest(size=size), patch.object(PREFLIGHT, "preflight") as run, redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    PREFLIGHT.main(["--project", "test-project", "--zone", ZONE, "--boot-disk-gb", size])
                self.assertEqual(error.exception.code, 2)
                run.assert_not_called()

    def test_gcloud_never_uses_shell_or_interactive_api_enable(self):
        completed = subprocess.CompletedProcess([], 0, '{"ok":true}', "")
        argument = "test-project;$(touch /tmp/not-a-command)"
        with patch.object(PREFLIGHT.subprocess, "run", return_value=completed) as run:
            self.assertEqual(PREFLIGHT._run_gcloud(["projects", "describe", argument]), {"ok": True})
        self.assertEqual(run.call_args.args[0], ["gcloud", "projects", "describe", argument, "--format=json"])
        self.assertFalse(run.call_args.kwargs["shell"])
        self.assertEqual(run.call_args.kwargs["env"]["CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API"], "false")

    def test_gcloud_errors_timeouts_and_invalid_json_fail_preflight(self):
        for outcome in (subprocess.CompletedProcess([], 1, "", "permission denied"),
                        subprocess.CompletedProcess([], 0, "not-json", ""),
                        FileNotFoundError("gcloud"), subprocess.TimeoutExpired("gcloud", 1)):
            with self.subTest(outcome=outcome):
                kwargs = {"side_effect": outcome} if isinstance(outcome, Exception) else {"return_value": outcome}
                with patch.object(PREFLIGHT.subprocess, "run", **kwargs):
                    with self.assertRaises(PREFLIGHT.PreflightError):
                        PREFLIGHT._run_gcloud(["compute", "instances", "list"])


if __name__ == "__main__":
    unittest.main()
