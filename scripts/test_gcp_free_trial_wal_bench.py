"""Unit tests for safety-sensitive pieces of the GCP benchmark runner."""

from __future__ import annotations

import base64
import importlib.util
import json
from pathlib import Path
import struct
import subprocess
import stat
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("gcp-free-trial-wal-bench.py")
SPEC = importlib.util.spec_from_file_location("gcp_free_trial_wal_bench", SCRIPT)
assert SPEC and SPEC.loader
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


class GcpFreeTrialWalBenchTests(unittest.TestCase):
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
        expected = [f"192.0.2.1 ssh-ed25519 {encoded}"]
        self.assertEqual(BENCH._host_key_lines(guest_response, "192.0.2.1"), expected)
        self.assertEqual(BENCH._host_key_lines(openssh_response, "192.0.2.1"), expected)

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
            with patch.object(BENCH.subprocess, "run", return_value=completed):
                gcloud.run(["compute", "instances", "list"])
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

            def scp(_ip, _remote, destination, _key, _known_hosts, *, recursive=False, timeout):
                nonlocal calls
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
                result = BENCH.collect_artifacts("192.0.2.1", key, known_hosts, root)
            self.assertTrue(result["results"])
            self.assertFalse(result["summary_complete"])
            self.assertTrue((root / "collected" / "results" / "summary.json").is_file())
            self.assertEqual(calls, 2)

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
                result = BENCH.collect_artifacts("192.0.2.1", key, known_hosts, root)

            self.assertFalse(result["results"])
            self.assertTrue(result["bootstrap_log"])
            self.assertTrue(result["benchmark_started"])
            self.assertIn("WAL_BENCHMARK_STARTED", (root / "bootstrap.log").read_text(encoding="utf-8"))

    def test_results_directory_is_private_under_permissive_umask(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            results = Path(temp) / "results"
            BENCH.create_private_results_directory(results)
            self.assertEqual(stat.S_IMODE(results.stat().st_mode), 0o700)

    def test_remote_run_parser_allows_missing_profile_counters(self) -> None:
        remote_script = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh")
        source = remote_script.read_text(encoding="utf-8")
        self.assertIn('echo "WAL_BENCHMARK_STARTED $(date --iso-8601=seconds)" | tee -a "$BOOT_LOG"', source)
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
                "params": {"wal_format_version": 8},
                "latency": {"p99_ms": 1.2},
            }), encoding="utf-8")
            with (run_dir.parent / "summary.tsv").open("w", encoding="utf-8") as stream:
                stream.write(
                    "writer\tblock\tposition\tformat\texit_code\tops\tputs_per_second\t"
                    "p99_ms\tcpu_seconds\tsync_count\tduration_seconds\n"
                )
            result = subprocess.run(
                ["python3", "-", str(run_dir), "1", "1", "1", "v8", "0", "1.0", "64"],
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


if __name__ == "__main__":
    unittest.main()
