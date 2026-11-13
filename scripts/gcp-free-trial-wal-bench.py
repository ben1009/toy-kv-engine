#!/usr/bin/env python3
"""Run the V4/V7/V8 WAL benchmark matrix on a disposable GCP C4 Local SSD VM.

This tool never changes Cloud Billing, enables APIs, or requests quota changes.
It refuses to create a VM unless the operator explicitly attests that the
project is still on a Free Trial and the read-only preflight passes.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import csv
from collections.abc import Callable
from datetime import datetime, timezone
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from typing import Any
import uuid

from gcp_wal_matrix import (
    FULL_DEFAULT_WRITERS,
    LEGACY_DEFAULT_WRITERS,
    build_batch64_schedule,
    build_benchmark_plan,
    build_rfc026_schedule,
    validate_writer_counts,
)
MACHINE_TYPE = "c4-standard-8-lssd"
MACHINE_SPECS = {
    "c4-standard-8-lssd": {"vcpus": 8, "ssd_count": 1, "ssd_gb": 375, "ssd_gib": 375},
    "c4-standard-16-lssd": {"vcpus": 16, "ssd_count": 2, "ssd_gb": 750, "ssd_gib": 750},
}
VM_INSTANCE_QUOTA = "INSTANCES"
RUNNERS = Path(__file__).with_name("gcp-free-trial-wal-bench-remote.sh")
MATRIX_MODULE = Path(__file__).with_name("gcp_wal_matrix.py")
PROFILE_RUNNER = Path(__file__).with_name("gcp_wal_profile_runner.py")
REPO_URL = "https://github.com/ben1009/toy-kv-engine.git"
MAX_RUNTIME_MINUTES = 180
MIN_RUNTIME_MINUTES = 60
MAX_ARTIFACT_BUNDLE_BYTES = 512 * 1024 * 1024

REMOTE_ARCHIVE_SCRIPT = r'''
import hashlib
import os
from pathlib import Path
import tempfile
import tarfile

root = Path(os.environ["GCP_WAL_RESULTS_ROOT"])
max_bytes = int(os.environ["GCP_WAL_MAX_ARCHIVE_BYTES"])
if not root.is_dir():
    raise SystemExit("benchmark results directory is missing")
root_names = {
    "summary.json", "summary.tsv", "comparison.tsv", "benchmark-plan.json",
    "rfc026-schedule.tsv", "profile-schedule.tsv", "commit.txt", "remote-exit.json",
    "finished_utc.txt", "remote-finished-utc.txt", "remote-status.txt", "host.txt",
    "os-release.txt", "uname.txt", "lscpu.txt", "lsblk.txt", "findmnt.txt", "df.txt",
    "setup.log", "rust-toolchain.txt", "rust-toolchain.toml", "write-perf.sha256", "perf-version.txt",
}
run_names = {
    "result.json", "run-metadata.json", "environment.txt", "start_utc.txt", "end_utc.txt",
    "start_epoch.txt", "command.sh", "stderr.log", "timeline.json", "perf.data",
    "perf-report.txt", "perf-record.log", "perf-command.sh", "validation-error.txt",
}
selected = [path for path in root.iterdir() if path.is_file() and path.name in root_names]
for run_dir in sorted(root.glob("w*")):
    if run_dir.is_dir():
        selected.extend(path for path in run_dir.iterdir() if path.is_file() and path.name in run_names)

descriptor, archive_name = tempfile.mkstemp(prefix="gcp-wal-bench-results-", suffix=".tar.gz")
os.close(descriptor)
archive = Path(archive_name)
try:
    with tarfile.open(archive, "w:gz") as bundle:
        for path in sorted(selected):
            bundle.add(path, arcname=path.relative_to(root.parent).as_posix())
    archive_bytes = archive.stat().st_size
    if archive_bytes > max_bytes:
        raise SystemExit(f"compact result bundle exceeds {max_bytes} byte limit: {archive_bytes}")
    digest = hashlib.sha256()
    with archive.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    print(f"ARCHIVE_PATH={archive}")
    print(f"ARCHIVE_SHA256={digest.hexdigest()}")
    print(f"ARCHIVE_BYTES={archive_bytes}")
    print(f"ARCHIVE_FILES={len(selected)}")
except BaseException:
    archive.unlink(missing_ok=True)
    raise
'''


def _build_remote_archive_command(
    *, root: str = "/mnt/gcp-wal-bench/results", max_bytes: int = MAX_ARTIFACT_BUNDLE_BYTES
) -> str:
    return " ".join((
        f"GCP_WAL_RESULTS_ROOT={shlex.quote(root)}",
        f"GCP_WAL_MAX_ARCHIVE_BYTES={max_bytes}",
        "python3 -c",
        shlex.quote(REMOTE_ARCHIVE_SCRIPT),
    ))


class BenchError(RuntimeError):
    """Raised for a failed preflight or benchmark orchestration step."""


def _is_transient_gcloud_error(detail: str) -> bool:
    lowered = detail.lower()
    transient_markers = (
        "connectionerror",
        "connection aborted",
        "connection reset",
        "remote end closed connection",
        "remotedisconnected",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "service unavailable",
        "rate limit",
    )
    return any(marker in lowered for marker in transient_markers) or bool(
        re.search(r"\b(?:429|500|502|503|504)\b", lowered)
    )


class Gcloud:
    def __init__(self, project: str, log_path: Path):
        self.project = project
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
        timeout: int = 120,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = ["gcloud", *args]
        started = datetime.now(timezone.utc).isoformat()
        try:
            # argv items are not shell-evaluated; this is intentionally not shell=True.
            # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
            env = os.environ.copy()
            # Keep the runner read-only with respect to API activation, even
            # when invoked from an interactive shell. Required APIs are
            # verified by their own read-only requests during preflight.
            env["CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API"] = "false"
            result = subprocess.run(
                command,
                input=input_text,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
                shell=False,
                env=env,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            self._log({"started": started, "command": command, "error": str(error)})
            raise BenchError(f"gcloud command failed to run: {shlex.join(command)}: {error}") from error
        self._log({
            "started": started,
            "command": command,
            "returncode": result.returncode,
            # gcloud JSON responses can contain project or instance metadata.
            # Keep command/status diagnostics without persisting response bodies.
            "stdout_bytes": len(result.stdout.encode("utf-8")),
            "stderr_bytes": len(result.stderr.encode("utf-8")),
        })
        if check and result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise BenchError(f"gcloud command failed ({result.returncode}): {shlex.join(command)}\n{detail}")
        return result

    def json(self, args: list[str], *, timeout: int = 120) -> Any:
        command_args = [*args, "--format=json"]
        for attempt in range(3):
            try:
                result = self.run(command_args, check=False, timeout=timeout)
            except BenchError as error:
                if attempt == 2 or not _is_transient_gcloud_error(str(error)):
                    raise
                time.sleep(2**attempt)
                continue

            if result.returncode:
                detail = result.stderr.strip() or result.stdout.strip()
                if attempt == 2 or not _is_transient_gcloud_error(detail):
                    raise BenchError(
                        f"gcloud command failed ({result.returncode}): {shlex.join(['gcloud', *command_args])}\n{detail}"
                    )
                time.sleep(2**attempt)
                continue

            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError as error:
                raise BenchError(f"gcloud returned invalid JSON for {shlex.join(args)}") from error

        raise BenchError(f"gcloud command failed after retries: {shlex.join(['gcloud', *command_args])}")

    def _log(self, record: dict[str, Any]) -> None:
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def verify_resource_absent(gcloud: Gcloud, describe_args: list[str]) -> dict[str, str]:
    """Check whether a GCP resource is absent after a delete attempt."""
    try:
        result = gcloud.run(describe_args, check=False, timeout=120)
    except BaseException as error:
        # SIGTERM becomes KeyboardInterrupt in this runner. Teardown must
        # continue to the other resource even if a verification is interrupted.
        return {"state": "unknown", "detail": str(error) or type(error).__name__}
    if result.returncode == 0:
        return {"state": "present", "detail": "describe command still found the resource"}

    detail = (result.stderr.strip() or result.stdout.strip())[-2000:]
    if re.search(r"\b(?:not found|not_found|does not exist)\b", detail, re.IGNORECASE):
        return {"state": "absent", "detail": detail or "describe reported not found"}
    return {"state": "unknown", "detail": detail or "describe failed without a not-found response"}


def cleanup_gcp_resources(
    gcloud: Gcloud,
    *,
    project: str,
    zone: str,
    instance: str,
    firewall: str,
    vm_created: bool,
    firewall_created: bool,
) -> tuple[list[str], dict[str, dict[str, str]], bool]:
    """Attempt and verify cleanup for every resource that may have been created."""
    cleanup: list[str] = []
    verification: dict[str, dict[str, str]] = {}
    failed = False

    if vm_created:
        try:
            result = gcloud.run([
                "compute", "instances", "delete", instance,
                f"--project={project}", f"--zone={zone}", "--quiet",
            ], check=False, timeout=300)
            cleanup.append(
                "VM delete completed" if result.returncode == 0
                else f"VM delete failed: {result.stderr.strip()}"
            )
        except BaseException as error:
            cleanup.append(f"VM delete failed: {error}")
        verification["vm"] = verify_resource_absent(gcloud, [
            "compute", "instances", "describe", instance,
            f"--project={project}", f"--zone={zone}", "--format=json",
        ])
        vm_state = verification["vm"]
        cleanup.append(f"VM deletion verification: {vm_state['state']} ({vm_state['detail']})")
        failed = failed or vm_state["state"] != "absent"

    if firewall_created:
        try:
            result = gcloud.run([
                "compute", "firewall-rules", "delete", firewall,
                f"--project={project}", "--quiet",
            ], check=False, timeout=180)
            cleanup.append(
                "temporary firewall rule deleted" if result.returncode == 0
                else f"firewall delete failed: {result.stderr.strip()}"
            )
        except BaseException as error:
            cleanup.append(f"firewall delete failed: {error}")
        verification["firewall"] = verify_resource_absent(gcloud, [
            "compute", "firewall-rules", "describe", firewall,
            f"--project={project}", "--format=json",
        ])
        firewall_state = verification["firewall"]
        cleanup.append(
            "temporary firewall deletion verification: "
            f"{firewall_state['state']} ({firewall_state['detail']})"
        )
        failed = failed or firewall_state["state"] != "absent"

    return cleanup, verification, failed


def zone_region(zone: str) -> str:
    region, separator, zone_suffix = zone.rpartition("-")
    if not separator or not zone_suffix or not re.fullmatch(r"[a-z]", zone_suffix):
        raise BenchError(f"Invalid GCP zone {zone!r}; expected a zone such as us-central1-a")
    return region


def _number(value: Any, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise BenchError(f"Quota metadata is missing a numeric {label}") from error
    if not math.isfinite(result) or result < 0:
        raise BenchError(f"Quota metadata has an invalid {label}")
    return result


def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def _quota_kind(info: dict[str, Any]) -> str | None:
    names = _norm(info.get("quotaId", "")) + _norm(info.get("metric", ""))
    # Cloud Quotas may identify the C4 family only in dimensionsInfo, rather
    # than in quotaId/metric. quota_limit() filters that dimension below.
    if "localssd" in names:
        return "local_ssd"
    if "hdbtotalgb" in names or "hyperdiskbalancedtotalstorage" in names:
        return "hyperdisk_balanced"
    if "cpus" in names:
        return "c4_cpu"
    if "instancesperprojectregion" in names or names.endswith("instances"):
        return "instances"
    return None


def quota_limit(
    quota_infos: Any,
    kind: str,
    region: str,
    family: str = "C4",
) -> tuple[float, dict[str, Any]]:
    """Return the most-specific applicable quota value; fail closed if absent."""
    if isinstance(quota_infos, dict):
        quota_infos = quota_infos.get("quotaInfos")
    if not isinstance(quota_infos, list):
        raise BenchError("Cloud Quotas response does not contain a quotaInfos list")

    matches: list[tuple[int, Any, dict[str, Any]]] = []
    for info in quota_infos:
        if not isinstance(info, dict) or _quota_kind(info) != kind:
            continue
        # The live Cloud Quotas API returns `dimensionsInfos`; older fixtures
        # and SDK responses may use the singular spelling.
        dimension_infos = info.get("dimensionsInfos", info.get("dimensionsInfo", []))
        if not isinstance(dimension_infos, list):
            continue
        for item in dimension_infos:
            if not isinstance(item, dict):
                continue
            dimensions = {str(key).lower(): str(value) for key, value in (item.get("dimensions") or {}).items()}
            applicable = item.get("applicableLocations")
            has_region = dimensions.get("region") == region
            has_applicable_region = isinstance(applicable, list) and region in applicable
            if not has_region and not has_applicable_region:
                continue
            vm_family = dimensions.get("vm_family")
            if vm_family and vm_family.upper() != family.upper():
                continue
            # The deprecated LOCAL_SSD_TOTAL_GB quota is regional rather than
            # family-scoped. Accept it only as a fallback for Local SSD; callers
            # must account for all regional Local SSD usage in that case.
            if kind == "c4_cpu" and not vm_family:
                continue
            details = item.get("details") or {}
            raw_limit = details.get("quotaValue", details.get("value", details.get("limit")))
            if raw_limit is None:
                continue
            specificity = int("region" in dimensions) + int("vm_family" in dimensions)
            matches.append((specificity, raw_limit, {
                "quota_id": info.get("quotaId"),
                "metric": info.get("metric"),
                "dimensions": dimensions,
                "applicable_locations": applicable,
                "details": details,
                "limit": raw_limit,
            }))

    if not matches:
        raise BenchError(
            f"Could not verify the {kind} quota for region={region}, vm_family={family}. "
            "The Cloud Quotas API must be enabled and expose an applicable quota; no VM will be created."
        )
    specificity = max(match[0] for match in matches)
    strongest = [match for match in matches if match[0] == specificity]
    # If the service returns overlapping equally-specific entries, use the
    # smallest value as the conservative effective limit.
    parsed = [(_number(raw_limit, "limit"), details) for _, raw_limit, details in strongest]
    selected_limit, selected_source = min(parsed, key=lambda match: match[0])
    return selected_limit, selected_source


def _resource_zone(instance: dict[str, Any]) -> str:
    return str(instance.get("zone", "")).rstrip("/").split("/")[-1]


def _machine_type_name(instance: dict[str, Any]) -> str:
    return str(instance.get("machineType", "")).rstrip("/").split("/")[-1]


def _instance_is_running(instance: dict[str, Any]) -> bool:
    # Treat unfamiliar non-terminated states as consuming CPU. This is
    # intentionally conservative when estimating remaining family quota.
    return str(instance.get("status", "")).upper() != "TERMINATED"


def _instance_local_ssd_gib(instance: dict[str, Any]) -> int:
    name = instance.get("name", "<unnamed>")
    disks = instance.get("disks")
    if not isinstance(disks, list):
        raise BenchError(f"Cannot verify the attached disk inventory for instance {name}")
    total = 0
    for disk in disks:
        if not isinstance(disk, dict) or disk.get("type") not in {"PERSISTENT", "SCRATCH"}:
            raise BenchError(f"Cannot verify an attached disk type for instance {name}")
        if disk["type"] == "SCRATCH":
            size = _number(disk.get("diskSizeGb"), f"Local SSD diskSizeGb for instance {name}")
            if size == 0 or not size.is_integer():
                raise BenchError(f"Invalid Local SSD diskSizeGb for instance {name}")
            total += int(size)
    return total


def _disk_region(disk: dict[str, Any]) -> str | None:
    region = str(disk.get("region", "")).rstrip("/").split("/")[-1]
    if region:
        return region
    zone = str(disk.get("zone", "")).rstrip("/").split("/")[-1]
    return zone_region(zone) if zone else None


def _hyperdisk_balanced_usage_gb(disks: Any, region: str) -> float:
    """Estimate HDB-TOTAL-GB use from existing balanced and HA disks."""
    if not isinstance(disks, list):
        raise BenchError("Cannot verify the existing Hyperdisk inventory")

    usage_gb = 0.0
    for disk in disks:
        if not isinstance(disk, dict):
            raise BenchError("Cannot verify an existing disk entry")
        disk_type = str(disk.get("type", "")).rstrip("/").split("/")[-1]
        if disk_type not in {"hyperdisk-balanced", "hyperdisk-balanced-high-availability"}:
            continue
        disk_region = _disk_region(disk)
        if disk_region is None:
            raise BenchError(f"Cannot determine the region for Hyperdisk {disk.get('name', '<unnamed>')}")
        if disk_region != region:
            continue
        size_gb = _number(disk.get("sizeGb"), f"Hyperdisk size for {disk.get('name', '<unnamed>')}")
        if not size_gb.is_integer() or size_gb <= 0:
            raise BenchError(f"Invalid Hyperdisk size for {disk.get('name', '<unnamed>')}")
        # Hyperdisk Balanced High Availability consumes twice its capacity
        # against the shared HDB-TOTAL-GB regional quota.
        usage_gb += size_gb * (2 if disk_type == "hyperdisk-balanced-high-availability" else 1)
    return usage_gb


def _active_benchmark_instances(instances: list[dict[str, Any]]) -> list[str]:
    return sorted(
        str(item.get("name", "<unnamed>"))
        for item in instances
        if str(item.get("name", "")).startswith("walbench-") and _instance_is_running(item)
    )


def _firewall_allows_ssh(rule: dict[str, Any]) -> bool:
    if str(rule.get("direction", "INGRESS")).upper() != "INGRESS":
        return False
    if str(rule.get("disabled", "false")).lower() == "true":
        return False
    allowed_rules = rule.get("allowed")
    if not isinstance(allowed_rules, list):
        return False
    for allowed in allowed_rules:
        if not isinstance(allowed, dict):
            continue
        protocol = str(allowed.get("IPProtocol", "")).lower()
        ports = allowed.get("ports")
        if protocol == "all":
            return True
        if protocol not in {"tcp", "6"}:
            continue
        # An omitted TCP port list means all TCP ports in a Compute firewall
        # rule, including SSH.
        if not ports:
            return True
        for value in ports:
            try:
                if "-" in str(value):
                    first, last = (int(part) for part in str(value).split("-", 1))
                    if first <= 22 <= last:
                        return True
                elif int(value) == 22:
                    return True
            except ValueError:
                continue
    return False


def _effective_firewall_rules(response: Any) -> list[dict[str, Any]]:
    """Normalize the network effective-firewalls response across gcloud shapes."""
    if isinstance(response, list):
        rules = response
    elif isinstance(response, dict):
        if not any(key in response for key in ("firewalls", "firewallPolicys", "firewallPolicies")):
            if not response:
                # Compute returns `{}` when a network has no effective
                # firewall rules or policies. The API request itself has
                # already succeeded, so this is a valid empty inventory.
                rules = []
            elif any(key in response for key in ("allowed", "action", "match")):
                rules = [response]
            else:
                raise BenchError("Could not safely interpret effective firewall rules; no VM will be created")
        else:
            raw_firewalls = response.get("firewalls", [])
            raw_policies = response.get("firewallPolicys", response.get("firewallPolicies", []))
            if not isinstance(raw_firewalls, list) or not isinstance(raw_policies, list):
                raise BenchError("Could not safely interpret effective firewall rules; no VM will be created")
            rules = list(raw_firewalls)
        for policy in response.get("firewallPolicys", response.get("firewallPolicies", [])):
            if isinstance(policy, dict):
                policy_rules = policy.get("rules", [])
                if not isinstance(policy_rules, list):
                    raise BenchError("Could not safely interpret effective firewall rules; no VM will be created")
                rules.extend(policy_rules)
            else:
                raise BenchError("Could not safely interpret effective firewall rules; no VM will be created")
    else:
        raise BenchError("Could not safely interpret effective firewall rules; no VM will be created")
    # Empty firewall and policy lists are valid; malformed response shapes fail closed.
    if any(not isinstance(rule, dict) for rule in rules):
        raise BenchError("Could not safely interpret effective firewall rules; no VM will be created")
    return rules


def _effective_ssh_rule_exceeds_source_range(
    rule: dict[str, Any], network_name: str, vm_tag: str, source_range: str
) -> bool:
    """Conservatively detect broad SSH allows in effective VPC or policy rules."""
    action = str(rule.get("action", "allow" if rule.get("allowed") else "")).lower()
    if action not in {"allow", "allowed"} or str(rule.get("disabled", "false")).lower() == "true":
        return False
    if str(rule.get("direction", "INGRESS")).upper() != "INGRESS":
        return False
    if rule.get("targetServiceAccounts") or rule.get("target_service_accounts") or rule.get("target_svc_acct"):
        # The VM has no service account, so these rules cannot match it.
        return False
    target_tags = rule.get("targetTags") or rule.get("target_tags") or []
    if target_tags and vm_tag not in target_tags:
        return False
    if rule.get("targetSecureTags") or rule.get("target_secure_tags"):
        # This VM is not assigned secure tags.
        return False
    target_networks = rule.get("targetNetworks") or rule.get("target_networks") or []
    if target_networks:
        network_names = {str(value).rstrip("/").split("/")[-1] for value in target_networks}
        if network_name not in network_names:
            return False
    target_resources = rule.get("targetResources") or rule.get("target_resources") or []
    network_targets = [str(value).rstrip("/").split("/")[-1]
                       for value in target_resources if "/networks/" in str(value)]
    if network_targets and network_name not in network_targets:
        return False
    if str(rule.get("targetType", "INSTANCES")).upper() not in {"", "INSTANCES"}:
        return False

    match = rule.get("match") or {}
    allowed = rule.get("allowed")
    if isinstance(allowed, list):
        if not _firewall_allows_ssh(rule):
            return False
    else:
        layer4 = match.get("layer4Configs") or match.get("layer4_configs") or rule.get("layer4_configs") or []
        if not layer4:
            protocol = str(rule.get("IPProtocol", rule.get("ipProtocol", rule.get("ip_protocol", "")))).lower()
            if protocol and protocol not in {"all", "tcp", "6"}:
                return False
            if rule.get("ports") and not _ports_allow_ssh(rule["ports"]):
                return False
        elif not any(
            str(config.get("ipProtocol", config.get("IPProtocol", config.get("ip_protocol", "")))).lower()
            in {"all", "tcp", "6"}
            and _ports_allow_ssh(config.get("ports"))
            for config in layer4 if isinstance(config, dict)
        ):
            return False

    source_ranges = (
        rule.get("sourceRanges") or rule.get("source_ranges") or rule.get("ipRanges")
        or rule.get("ip_ranges") or rule.get("srcIpRanges") or rule.get("src_ip_ranges")
        or match.get("srcIpRanges") or match.get("src_ip_ranges") or []
    )
    other_source_selectors = (
        rule.get("sourceTags") or rule.get("source_tags") or rule.get("src_tags")
        or rule.get("sourceServiceAccounts") or rule.get("source_service_accounts") or rule.get("src_svc_acct")
        or match.get("srcSecureTags") or match.get("srcNetworks") or match.get("srcNetworkType")
        or match.get("srcNetworkContext") or match.get("srcAddressGroups") or match.get("srcFqdns")
        or match.get("srcRegionCodes") or match.get("srcThreatIntelligences")
    )
    if other_source_selectors or not source_ranges:
        return True
    requested = ipaddress.ip_network(source_range, strict=False)
    for raw_source in source_ranges:
        try:
            existing = ipaddress.ip_network(str(raw_source), strict=False)
        except ValueError:
            return True
        if existing.version != requested.version or not existing.subnet_of(requested):
            return True
    return False


def _ports_allow_ssh(ports: Any) -> bool:
    if not ports:
        return True
    for value in ports:
        try:
            if "-" in str(value):
                first, last = (int(part) for part in str(value).split("-", 1))
                if first <= 22 <= last:
                    return True
            elif int(value) == 22:
                return True
        except ValueError:
            continue
    return False


def _ssh_rule_exceeds_source_range(
    rule: dict[str, Any], network_name: str, vm_tag: str, source_range: str
) -> bool:
    if not _firewall_allows_ssh(rule):
        return False
    rule_network = str(rule.get("network", "")).rstrip("/").split("/")[-1]
    if not rule_network:
        return True
    if rule_network != network_name:
        return False
    target_tags = rule.get("targetTags") or []
    if target_tags and vm_tag not in target_tags:
        return False
    # The VM is created with --no-service-account, so target-service-account
    # rules do not apply to it.
    if rule.get("targetServiceAccounts"):
        return False

    requested = ipaddress.ip_network(source_range, strict=False)
    source_ranges = rule.get("sourceRanges") or []
    # Source tags/service accounts are identity-based and cannot be bounded by
    # the operator's CIDR. Missing source ranges are also treated as unknown.
    if not source_ranges or rule.get("sourceTags") or rule.get("sourceServiceAccounts"):
        return True
    for raw_source in source_ranges:
        try:
            existing = ipaddress.ip_network(str(raw_source), strict=False)
        except ValueError:
            return True
        if existing.version != requested.version or not existing.subnet_of(requested):
            return True
    return False


def audit_ssh_firewall_rules(
    gcloud: Gcloud, project: str, network: str, region: str, vm_tag: str, source_range: str
) -> None:
    network_name = str(network).rstrip("/").split("/")[-1]
    rules = gcloud.json(["compute", "firewall-rules", "list", f"--project={project}"])
    if not isinstance(rules, list):
        raise BenchError("Could not safely audit existing VPC firewall rules; no VM will be created")
    effective = _effective_firewall_rules(gcloud.json([
        "compute", "networks", "get-effective-firewalls", network_name, f"--project={project}"
    ]))
    regional_effective = _effective_firewall_rules(gcloud.json([
        "compute", "network-firewall-policies", "get-effective-firewalls",
        f"--network={network_name}", f"--region={region}", f"--project={project}"
    ]))
    conflicts = [
        str(rule.get("name", "<unnamed>"))
        for rule in rules
        if isinstance(rule, dict)
        and _ssh_rule_exceeds_source_range(rule, network_name, vm_tag, source_range)
    ]
    conflicts.extend(
        str(rule.get("name") or rule.get("firewallPolicyName") or rule.get("firewall_policy_name") or "<unnamed>")
        for rule in [*effective, *regional_effective]
        if _effective_ssh_rule_exceeds_source_range(rule, network_name, vm_tag, source_range)
    )
    if conflicts:
        names = ", ".join(sorted(conflicts))
        raise BenchError(
            "Existing VPC firewall ALLOW rules can expose SSH beyond --ssh-source-range "
            f"({names}). Narrow or remove those rules, or use an isolated VPC, before provisioning."
        )


def preflight(
    gcloud: Gcloud, project: str, zone: str, network: str, subnet: str | None,
    machine_type: str = MACHINE_TYPE,
) -> dict[str, Any]:
    machine_spec = MACHINE_SPECS[machine_type]
    expected_vcpus = machine_spec["vcpus"]
    expected_ssd_count = machine_spec["ssd_count"]
    expected_ssd_gb = machine_spec["ssd_gb"]
    expected_ssd_gib = machine_spec["ssd_gib"]
    region = zone_region(zone)
    if not shutil.which("gcloud"):
        raise BenchError("gcloud is not installed; install the Google Cloud CLI and authenticate first")
    active_accounts = gcloud.run(["auth", "list", "--filter=status:ACTIVE", "--format=value(account)"])
    account = active_accounts.stdout.strip().splitlines()
    if len(account) != 1:
        raise BenchError("Expected exactly one active gcloud account; authenticate with gcloud auth login first")

    project_info = gcloud.json(["projects", "describe", project])
    if project_info.get("projectId") != project or project_info.get("lifecycleState") != "ACTIVE":
        raise BenchError(f"Project {project!r} is not active or did not match the requested project")

    # Verify the required APIs by making the read-only Compute and Cloud
    # Quotas requests used below. This avoids depending on the separate
    # Service Usage API endpoint; Gcloud.run disables implicit API enablement,
    # so disabled or inaccessible APIs still fail closed.
    zone_info = gcloud.json(["compute", "zones", "describe", zone, f"--project={project}"])
    if zone_info.get("status") != "UP":
        raise BenchError(f"Zone {zone} is not UP")

    machine = gcloud.json([
        "compute", "machine-types", "describe", machine_type, f"--zone={zone}", f"--project={project}"
    ])
    if int(machine.get("guestCpus", -1)) != expected_vcpus:
        raise BenchError(f"{machine_type} did not report exactly {expected_vcpus} vCPUs")
    bundled_local_ssds = machine.get("bundledLocalSsds")
    if (not isinstance(bundled_local_ssds, dict)
            or bundled_local_ssds.get("partitionCount") != expected_ssd_count
            or bundled_local_ssds.get("defaultInterface") != "NVME"):
        raise BenchError(
            f"{machine_type} in {zone} must expose exactly {expected_ssd_count} bundled NVMe Local SSD(s); "
            f"machine type returned bundledLocalSsds={bundled_local_ssds!r}"
        )
    # MachineType exposes the bundled Local SSD count/interface; the guest
    # verifies each device before assembling the benchmark filesystem.

    # Resolve the mutable image family once, then create from its immutable
    # image name so every host record can identify the exact boot image.
    boot_image = gcloud.json([
        "compute", "images", "describe-from-family", "ubuntu-2404-lts-amd64",
        "--project=ubuntu-os-cloud",
    ])
    if boot_image.get("status") != "READY" or not boot_image.get("name"):
        raise BenchError("Could not resolve a READY Ubuntu 24.04 boot image")

    network_info = gcloud.json(["compute", "networks", "describe", network, f"--project={project}"])
    subnet_info: dict[str, Any] | None = None
    if subnet:
        subnet_info = gcloud.json([
            "compute", "networks", "subnets", "describe", subnet,
            f"--region={region}", f"--project={project}",
        ])
        subnet_network = str(subnet_info.get("network", "")).rstrip("/").split("/")[-1]
        if subnet_network != network_info.get("name"):
            raise BenchError(f"Subnetwork {subnet!r} does not belong to VPC network {network!r}")
    elif not network_info.get("autoCreateSubnetworks", False):
        raise BenchError("Custom-mode VPC networks require --subnet in this runner")

    quota_infos = gcloud.json([
        "quotas", "info", "list", "--service=compute.googleapis.com", f"--project={project}",
        f"--billing-project={project}",
    ], timeout=180)
    c4_cpu_limit, c4_cpu_source = quota_limit(quota_infos, "c4_cpu", region)
    local_ssd_limit, local_ssd_source = quota_limit(quota_infos, "local_ssd", region)
    hyperdisk_limit, hyperdisk_source = quota_limit(quota_infos, "hyperdisk_balanced", region)
    local_ssd_family_scoped = "vm_family" in local_ssd_source["dimensions"]

    instances = gcloud.json(["compute", "instances", "list", f"--project={project}"], timeout=180)
    regional_instances = [item for item in instances if zone_region(_resource_zone(item)) == region]
    existing_benchmark_instances = _active_benchmark_instances(instances)
    if existing_benchmark_instances:
        raise BenchError(
            "An earlier WAL benchmark VM is still present in this project "
            f"({', '.join(sorted(existing_benchmark_instances))}); clean it up before retrying."
        )
    reservations = gcloud.json(["compute", "reservations", "list", f"--project={project}"], timeout=180)
    regional_c4_reservations: list[str] = []
    regional_ssd_reservations: list[str] = []
    for reservation in reservations:
        reservation_zone = _resource_zone(reservation)
        if not reservation_zone or zone_region(reservation_zone) != region:
            continue
        properties = (reservation.get("specificReservation") or {}).get("instanceProperties") or {}
        reserved_machine_type = str(properties.get("machineType", "")).rstrip("/").split("/")[-1]
        if not reserved_machine_type:
            raise BenchError(
                f"Cannot determine the machine family for reservation {reservation.get('name')} in {region}; "
                "quota headroom cannot be verified safely."
            )
        reservation_name = str(reservation.get("name", reserved_machine_type))
        if reserved_machine_type.startswith("c4-"):
            regional_c4_reservations.append(reservation_name)
        regional_ssd_reservations.append(reservation_name)
    if regional_c4_reservations:
        raise BenchError(
            f"C4 reservations exist in {region} ({', '.join(regional_c4_reservations)}); "
            "the runner cannot safely derive their C4 CPU/Local SSD quota usage, so it will not provision."
        )
    if not local_ssd_family_scoped and regional_ssd_reservations:
        raise BenchError(
            f"Reservations exist in {region} ({', '.join(regional_ssd_reservations)}); "
            "the runner cannot safely derive their regional Local SSD usage, so it will not provision."
        )

    c4_cpu_used = 0
    c4_ssd_used = 0
    regional_ssd_used = 0
    c4_running_names: list[str] = []
    machine_cache: dict[tuple[str, str], dict[str, Any]] = {}
    for instance in regional_instances:
        machine_name = _machine_type_name(instance)
        is_c4 = machine_name.startswith("c4-")
        if not is_c4 and local_ssd_family_scoped:
            continue
        if is_c4 and _instance_is_running(instance):
            instance_zone = _resource_zone(instance)
            cache_key = (machine_name, instance_zone)
            if cache_key not in machine_cache:
                machine_cache[cache_key] = gcloud.json([
                    "compute", "machine-types", "describe", machine_name,
                    f"--zone={instance_zone}", f"--project={project}"
                ])
            machine_metadata = machine_cache[cache_key]
            c4_cpu_used += int(machine_metadata.get("guestCpus", 0))
            c4_running_names.append(str(instance.get("name", machine_name)))
        # Local SSDs can be optional attachments rather than part of a machine
        # type. Count actual SCRATCH disks on every relevant extant instance,
        # including stopped/terminated instances that have not been deleted.
        scratch_gb = _instance_local_ssd_gib(instance)
        regional_ssd_used += scratch_gb
        if is_c4:
            c4_ssd_used += scratch_gb

    c4_cpu_available = c4_cpu_limit - c4_cpu_used
    local_ssd_used = c4_ssd_used if local_ssd_family_scoped else regional_ssd_used
    local_ssd_available = local_ssd_limit - local_ssd_used
    if c4_cpu_available < expected_vcpus:
        raise BenchError(
            f"Insufficient C4 CPU quota in {region}: limit={c4_cpu_limit:g}, "
            f"current C4 use={c4_cpu_used}, available={c4_cpu_available:g}, need={expected_vcpus}."
        )
    if local_ssd_available < expected_ssd_gb:
        raise BenchError(
            f"Insufficient C4 Local SSD quota in {region}: limit={local_ssd_limit:g} GB, "
            f"current Local SSD use={local_ssd_used} GB, available={local_ssd_available:g} GB, "
            f"need={expected_ssd_gb} GB."
        )

    region_info = gcloud.json(["compute", "regions", "describe", region, f"--project={project}"])
    if region_info.get("status") != "UP":
        raise BenchError(f"Region {region} is not UP")
    vm_quota = next((quota for quota in region_info.get("quotas", [])
                     if _norm(quota.get("metric", "")) == _norm(VM_INSTANCE_QUOTA)), None)
    if vm_quota is None:
        raise BenchError(f"Could not verify the regional {VM_INSTANCE_QUOTA} quota; no VM will be created")
    vm_instances_available = _number(vm_quota.get("limit"), "VM instance quota limit") - _number(
        vm_quota.get("usage"), "VM instance quota usage"
    )
    if vm_instances_available < 1:
        raise BenchError(
            f"Insufficient VM instance quota in {region}: limit={vm_quota['limit']}, "
            f"usage={vm_quota['usage']}, need=1."
        )
    address_quota = next((quota for quota in region_info.get("quotas", [])
                          if _norm(quota.get("metric", "")) == _norm("IN_USE_ADDRESSES")), None)
    if address_quota is None:
        raise BenchError("Could not verify the regional IN_USE_ADDRESSES quota; no VM will be created")
    addresses_available = _number(address_quota.get("limit"), "external IPv4 quota limit") - _number(
        address_quota.get("usage"), "external IPv4 quota usage"
    )
    if addresses_available < 1:
        raise BenchError(
            f"Insufficient external IPv4 quota in {region}: limit={address_quota['limit']}, "
            f"usage={address_quota['usage']}, need=1."
        )
    hyperdisks = gcloud.json(["compute", "disks", "list", f"--project={project}"])
    hyperdisk_used = _hyperdisk_balanced_usage_gb(hyperdisks, region)
    hyperdisk_available = hyperdisk_limit - hyperdisk_used
    if hyperdisk_available < 50:
        raise BenchError(
            f"Insufficient Hyperdisk Balanced quota in {region}: limit={hyperdisk_limit:g} GB, "
            f"inventory use={hyperdisk_used:g} GB, available={hyperdisk_available:g} GB, need=50 GB."
        )

    global_project_info = gcloud.json(["compute", "project-info", "describe", f"--project={project}"])
    global_quotas = global_project_info.get("quotas", [])
    global_cpu_quota = next((quota for quota in global_project_info.get("quotas", [])
                             if _norm(quota.get("metric", "")) == _norm("CPUS_ALL_REGIONS")), None)
    global_cpu_report: dict[str, Any]
    if global_cpu_quota is None:
        # This quota only applies to some projects/accounts.
        global_cpu_report = {"applicable": False, "note": "not exposed by Compute Engine project quota data"}
    else:
        global_cpu_available = _number(global_cpu_quota.get("limit"), "global CPU quota limit") - _number(
            global_cpu_quota.get("usage"), "global CPU quota usage"
        )
        if global_cpu_available < expected_vcpus:
            raise BenchError(
                f"Insufficient CPUS_ALL_REGIONS quota: limit={global_cpu_quota['limit']}, "
                f"usage={global_cpu_quota['usage']}, available={global_cpu_available:g}, "
                f"need={expected_vcpus}."
            )
        global_cpu_report = {
            "applicable": True,
            "limit": _number(global_cpu_quota.get("limit"), "global CPU quota limit"),
            "usage": _number(global_cpu_quota.get("usage"), "global CPU quota usage"),
            "available": global_cpu_available,
            "required": expected_vcpus,
        }
    firewall_quota = next((quota for quota in global_quotas
                           if _norm(quota.get("metric", "")) == _norm("FIREWALLS")), None)
    if firewall_quota is None:
        raise BenchError("Could not verify the global FIREWALLS quota; no firewall rule or VM will be created")
    firewall_available = _number(firewall_quota.get("limit"), "firewall quota limit") - _number(
        firewall_quota.get("usage"), "firewall quota usage"
    )
    if firewall_available < 1:
        raise BenchError(
            f"Insufficient global firewall quota: limit={firewall_quota['limit']}, "
            f"usage={firewall_quota['usage']}, need=1."
        )

    return {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "project": project,
        "active_gcloud_account": account[0],
        "zone": zone,
        "region": region,
        "zone_status": zone_info.get("status"),
        "machine_type": machine_type,
        "guest_vcpus": int(machine["guestCpus"]),
        "bundled_local_ssds": bundled_local_ssds,
        "local_ssd_capacity_gib": expected_ssd_gib,
        "local_ssd_count": expected_ssd_count,
        "machine_type_local_ssd_verified": True,
        "boot_image": {
            "project": "ubuntu-os-cloud",
            "name": boot_image["name"],
            "creation_timestamp": boot_image.get("creationTimestamp"),
        },
        "actual_zonal_capacity_guaranteed": False,
        "capacity_note": (
            f"The zone and machine type support {expected_ssd_count} bundled Local SSD(s) "
            f"({expected_ssd_gb} GB total) and project quotas have room. "
            "GCP does not guarantee currently free host capacity through this preflight; instance creation "
            "can still fail if the zone is depleted."
        ),
        "network": network_info.get("name"),
        "subnetwork": subnet_info.get("name") if subnet_info else None,
        "quotas": {
            "c4_cpu": {
                "limit": c4_cpu_limit,
                "inventory_used": c4_cpu_used,
                "available": c4_cpu_available,
                "required": expected_vcpus,
                "source": c4_cpu_source,
            },
            "c4_local_ssd_gb": {
                "limit": local_ssd_limit,
                "inventory_used": local_ssd_used,
                "inventory_scope": "C4" if local_ssd_family_scoped else "region",
                "available": local_ssd_available,
                "required": expected_ssd_gb,
                "source": local_ssd_source,
            },
            "vm_instances": {
                "limit": _number(vm_quota.get("limit"), "VM instance quota limit"),
                "usage": _number(vm_quota.get("usage"), "VM instance quota usage"),
                "available": vm_instances_available,
                "required": 1,
            },
            "external_ipv4_addresses": {
                "limit": _number(address_quota.get("limit"), "external IPv4 quota limit"),
                "usage": _number(address_quota.get("usage"), "external IPv4 quota usage"),
                "available": addresses_available,
                "required": 1,
            },
            "hyperdisk_balanced_gb": {
                "limit": hyperdisk_limit,
                "inventory_used": hyperdisk_used,
                "available": hyperdisk_available,
                "required": 50,
                "source": hyperdisk_source,
            },
            "cpus_all_regions": global_cpu_report,
            "firewalls": {
                "limit": _number(firewall_quota.get("limit"), "firewall quota limit"),
                "usage": _number(firewall_quota.get("usage"), "firewall quota usage"),
                "available": firewall_available,
                "required": 1,
            },
        },
        "existing_c4_running_instances": c4_running_names,
        "existing_benchmark_instances": existing_benchmark_instances,
        "regional_c4_reservations": regional_c4_reservations,
        "existing_region_instance_count": len(regional_instances),
        "billing": {
            "mutation_performed": False,
            "trial_status": "not_available_from_supported_read-only gcloud quota/compute checks",
            "operator_attestation_required_for_run": True,
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="Explicit GCP project ID; no gcloud default is used")
    parser.add_argument("--zone", required=True, help="Zone to verify and use, for example us-central1-a")
    parser.add_argument("--commit", required=True, help="Full 40-character toy-kv-engine commit SHA to benchmark")
    parser.add_argument("--machine-type", choices=tuple(MACHINE_SPECS),
                        help="C4 Local SSD shape (defaults to 8-vCPU for benchmark suites)")
    parser.add_argument("--writers", type=int, nargs="+", choices=(1, 2, 4, 8, 16),
                        help="Writer counts to run (defaults depend on the selected suite)")
    suite_group = parser.add_mutually_exclusive_group()
    suite_group.add_argument("--full-rfc026-matrix", action="store_true",
                             help="Run point/batch8/batch16/batch64 across V4/V7/V8 parallel and V8 Leader")
    suite_group.add_argument("--v8-small-ticket-profile", action="store_true",
                             help="Collect Hotpath/perf diagnostics for V7/V8 point and batch workloads")
    parser.add_argument("--network", help="Existing VPC to use (default: default)")
    parser.add_argument("--subnet", help="Subnetwork name for custom-mode VPCs (must be in the requested region)")
    parser.add_argument("--ssh-source-range", help="Restricted IPv4 CIDR allowed to SSH to this ephemeral VM")
    parser.add_argument("--results-dir", type=Path, help="Local directory for preflight and collected artifacts")
    parser.add_argument("--num", type=int, default=750_000, help="Writes per run (default: 750000)")
    parser.add_argument("--max-runtime-minutes", type=int, default=180,
                        help=f"Server-enforced delete deadline, {MIN_RUNTIME_MINUTES}..{MAX_RUNTIME_MINUTES} minutes (default: 180)")
    parser.add_argument("--estimated-hourly-cost-usd", type=float,
                        help="Operator-supplied hourly cost estimate for VM, disks, IP, and expected egress")
    parser.add_argument("--max-estimated-spend-usd", type=float,
                        help="Refuse the run if hourly estimate × maximum VM runtime exceeds this amount")
    parser.add_argument("--preflight-only", action="store_true", help="Run read-only checks; never create cloud resources")
    parser.add_argument(
        "--keep-vm-after-run", action="store_true",
        help="Retain a successful benchmark VM and its restricted SSH rule until its server-side delete deadline",
    )
    parser.add_argument("--confirm-free-trial", action="store_true",
                        help="Attest that Billing Overview says Free Trial and you will not upgrade to Paid")
    args = parser.parse_args(argv)
    if args.machine_type is None:
        args.machine_type = MACHINE_TYPE
    args.network = args.network or "default"
    if args.writers is None:
        if args.v8_small_ticket_profile:
            args.writers = (1, 8, 16)
        else:
            args.writers = FULL_DEFAULT_WRITERS if args.full_rfc026_matrix else LEGACY_DEFAULT_WRITERS
    try:
        args.writers = validate_writer_counts(args.writers)
    except ValueError as error:
        parser.error(str(error))
    if not re.fullmatch(r"[0-9a-fA-F]{40}", args.commit):
        parser.error("--commit must be a full 40-character SHA")
    if args.num < 64:
        parser.error("--num must be at least 64")
    if (args.full_rfc026_matrix or args.v8_small_ticket_profile) and args.num != 750_000:
        parser.error("the selected multi-workload suite uses fixed operation counts; omit --num overrides")
    if not MIN_RUNTIME_MINUTES <= args.max_runtime_minutes <= MAX_RUNTIME_MINUTES:
        parser.error(f"--max-runtime-minutes must be between {MIN_RUNTIME_MINUTES} and {MAX_RUNTIME_MINUTES}")
    if (args.estimated_hourly_cost_usd is None) != (args.max_estimated_spend_usd is None):
        parser.error("--estimated-hourly-cost-usd and --max-estimated-spend-usd must be provided together")
    if args.estimated_hourly_cost_usd is not None:
        for value, option in (
            (args.estimated_hourly_cost_usd, "--estimated-hourly-cost-usd"),
            (args.max_estimated_spend_usd, "--max-estimated-spend-usd"),
        ):
            if not math.isfinite(value) or value <= 0:
                parser.error(f"{option} must be a finite positive amount")
        estimated_spend = args.estimated_hourly_cost_usd * args.max_runtime_minutes / 60
        if estimated_spend > args.max_estimated_spend_usd:
            parser.error(
                f"estimated maximum spend ${estimated_spend:.2f} exceeds "
                f"--max-estimated-spend-usd ${args.max_estimated_spend_usd:.2f}"
            )
    if not args.preflight_only:
        if not args.confirm_free_trial:
            parser.error("VM creation requires --confirm-free-trial after manually checking Billing Overview")
        if not args.ssh_source_range:
            parser.error("VM creation requires --ssh-source-range; unrestricted SSH is not allowed")
        if args.estimated_hourly_cost_usd is None:
            parser.error(
                "VM creation requires --estimated-hourly-cost-usd and --max-estimated-spend-usd "
                "after checking current regional pricing"
            )
    if args.ssh_source_range:
        try:
            source = ipaddress.ip_network(args.ssh_source_range, strict=False)
        except ValueError as error:
            parser.error(f"invalid --ssh-source-range: {error}")
        if source.version != 4 or source.prefixlen < 24:
            parser.error("--ssh-source-range must be an IPv4 CIDR no broader than /24; use your public IP with /32")
    return args


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def should_retain_vm(args: argparse.Namespace, return_code: int, lifecycle: dict[str, Any]) -> bool:
    explicitly_requested = bool(
        getattr(args, "keep_vm_after_run", False)
        and lifecycle.get("created")
        and lifecycle.get("benchmark_started")
        and lifecycle.get("collected")
        and lifecycle.get("results_verified")
    )
    needs_artifact_recovery = bool(
        lifecycle.get("created")
        and (
            lifecycle.get("benchmark_attempted")
            or lifecycle.get("benchmark_started")
            or lifecycle.get("remote_execution_uncertain")
        )
        and not lifecycle.get("results_verified")
    )
    return explicitly_requested or needs_artifact_recovery


def retained_vm_reconnect_instructions(
    *,
    termination_timestamp: Any,
    network: Any,
    subnet: Any,
    firewall: str,
    project: str,
    connect_command: str,
    cleanup_uncertain: bool = False,
) -> str:
    cleanup_command = (
        "gcloud compute firewall-rules delete "
        f"{shlex.quote(firewall)} --project={shlex.quote(project)} --quiet"
    )

    return "".join((
        (
            "Cloud cleanup could not be verified; the VM may have been deleted or may still be running.\n"
            if cleanup_uncertain else "VM retained until its GCP auto-delete deadline.\n"
        ),
        "Do not destroy the VM, firewall, VPC, subnet, route, or Terraform state until the complete local result set is recovered.\n"
        f"Auto-delete deadline: {termination_timestamp}\n"
        f"VPC: {network}\n"
        f"Subnet: {subnet}\n"
        f"SSH firewall rule: {firewall}\n"
        f"Delete the temporary SSH firewall rule after the VM is gone: {cleanup_command}\n"
        f"{connect_command}\n",
    ))


def create_private_results_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=False, mode=0o700)
    # mkdir's mode is affected by umask; explicitly ensure artifacts stay private
    # even when the runner is invoked with a permissive shared-host umask.
    os.chmod(path, 0o700)


def gcloud_base(project: str, zone: str) -> list[str]:
    return ["--project", project, "--zone", zone]


def _run_identifier(row: dict[str, Any], suite: str) -> str:
    if suite == "v8-small-ticket-profile":
        return f"w{row['writer']}-{row['workload']}-{row['arm']}"
    if suite == "rfc026-full":
        return f"w{row['writer']}-{row['workload']}-r{row['repetition']}-{row['arm']}"
    if suite == "batch64":
        return f"w{row['writer']}-b{row['block']}-p{row['position']}-{row['arm']}"
    raise ValueError(f"unsupported benchmark suite: {suite!r}")


def _summary_row_identifier(row: dict[str, Any], suite: str) -> str:
    if suite == "v8-small-ticket-profile":
        return f"w{int(row['writer'])}-{row['workload']}-{row['arm']}"
    if suite == "rfc026-full":
        return f"w{int(row['writer'])}-{row['workload']}-r{int(row['repetition'])}-{row['arm']}"
    if suite == "batch64":
        return f"w{int(row['writer'])}-b{int(row['block'])}-p{int(row['position'])}-{row['format']}"
    raise ValueError(f"unsupported benchmark suite: {suite!r}")


def _expected_run_metadata(row: dict[str, Any], suite: str) -> dict[str, Any]:
    if suite == "v8-small-ticket-profile":
        return {key: row[key] for key in ("writer", "workload", "arm", "perf_capture")}
    if suite == "rfc026-full":
        return {key: row[key] for key in ("writer", "workload", "repetition", "arm")}
    if suite == "batch64":
        return {
            "writer": row["writer"], "block": row["block"], "position": row["position"], "format": row["arm"],
        }
    raise ValueError(f"unsupported benchmark suite: {suite!r}")


def _verify_result_set(results: Path) -> dict[str, Any]:
    """Validate suite metadata and one successful result/metadata pair per planned run."""
    import json

    errors: list[str] = []
    try:
        summary = json.loads((results / "summary.json").read_text(encoding="utf-8"))
        remote_exit = json.loads((results / "remote-exit.json").read_text(encoding="utf-8"))
        if not isinstance(summary, dict) or not isinstance(remote_exit, dict):
            raise ValueError("summary and remote exit metadata must be JSON objects")
        suite = summary.get("suite")
        if suite == "v8-small-ticket-profile":
            with (results / "profile-schedule.tsv").open(encoding="utf-8", newline="") as stream:
                schedule_rows = list(csv.DictReader(stream, delimiter="\t"))
            schedule: list[dict[str, Any]] = []
            for row in schedule_rows:
                if not {"writer", "workload", "arm", "perf_capture"}.issubset(row):
                    raise ValueError("profile schedule is missing required columns")
                schedule.append({
                    **row,
                    "writer": int(row["writer"]),
                    "perf_capture": row["perf_capture"].strip().lower() == "true",
                })
            planned = len(schedule)
            matrix_validation = summary.get("matrix_validation")
            if not isinstance(matrix_validation, dict) or matrix_validation.get("complete") is not True:
                errors.append("profile matrix validation is incomplete")
            if not isinstance(matrix_validation, dict) or matrix_validation.get("planned") != planned:
                errors.append("profile validation planned count does not match its schedule")
            if not isinstance(matrix_validation, dict) or matrix_validation.get("observed") != summary.get("runs_completed"):
                errors.append("profile validation observed count does not match its summary")
        else:
            plan = json.loads((results / "benchmark-plan.json").read_text(encoding="utf-8"))
            if not isinstance(plan, dict):
                raise ValueError("benchmark plan must be a JSON object")
            suite = plan.get("suite")
            if suite == "rfc026-full":
                schedule = build_rfc026_schedule(plan["writers"])
            elif suite == "batch64":
                schedule = build_batch64_schedule(plan["writers"])
            else:
                raise ValueError(f"unsupported benchmark plan suite: {suite!r}")
            planned = plan.get("runs_planned")
            if type(planned) is not int or planned != len(schedule):
                errors.append("benchmark plan run count does not match its schedule")
            matrix_validation = summary.get("matrix_validation")
            if not isinstance(matrix_validation, dict) or matrix_validation.get("complete") is not True:
                errors.append("benchmark matrix validation is incomplete")
            if not isinstance(matrix_validation, dict) or matrix_validation.get("planned") != planned:
                errors.append("matrix validation planned count does not match its plan")
            if not isinstance(matrix_validation, dict) or matrix_validation.get("observed") != planned:
                errors.append("matrix validation observed count does not match its plan")

        summary_planned = summary.get("runs_planned")
        completed = summary.get("runs_completed")
        if type(summary_planned) is not int or summary_planned != planned:
            errors.append("summary planned count does not match the run schedule")
        if type(completed) is not int or completed != planned:
            errors.append("summary completed count does not match the run schedule")
        if summary.get("complete") is not True:
            errors.append("summary does not report a complete run")
        if remote_exit.get("failed") is not False:
            errors.append("remote exit record reports failure")
        if type(remote_exit.get("runs_started")) is not int or remote_exit.get("runs_started") != planned:
            errors.append("remote exit run count does not match the plan")

        expected: dict[str, dict[str, Any]] = {}
        for row in schedule:
            run_id = _run_identifier(row, suite)
            if run_id in expected:
                errors.append(f"duplicate run identifier in schedule: {run_id}")
            expected[run_id] = _expected_run_metadata(row, suite)
        actual_directories = {path.name for path in results.glob("w*") if path.is_dir()}
        if actual_directories != set(expected):
            missing = sorted(set(expected) - actual_directories)
            unexpected = sorted(actual_directories - set(expected))
            errors.append(f"run directories do not match schedule (missing={missing}, unexpected={unexpected})")

        summary_rows = summary.get("results")
        if not isinstance(summary_rows, list) or len(summary_rows) != planned:
            errors.append("summary does not contain one row for every planned run")
        else:
            summary_ids: list[str] = []
            for row in summary_rows:
                if not isinstance(row, dict):
                    errors.append("summary contains a non-object run row")
                    continue
                try:
                    summary_ids.append(_summary_row_identifier(row, suite))
                except (KeyError, TypeError, ValueError) as error:
                    errors.append(f"summary contains an invalid run identifier: {error}")
                    continue
                if row.get("exit_code") != 0:
                    errors.append(f"summary run {summary_ids[-1]} did not succeed")
            if len(summary_ids) != len(set(summary_ids)) or set(summary_ids) != set(expected):
                errors.append("summary run identifiers do not match the unique planned runs")

        profile_capture_count = 0
        for run_id, identity in expected.items():
            perf_capture_expected = bool(identity.pop("perf_capture", False))
            run_dir = results / run_id
            result_path = run_dir / "result.json"
            metadata_path = run_dir / "run-metadata.json"
            if not result_path.is_file() or not metadata_path.is_file():
                errors.append(f"run {run_id} is missing result.json or run-metadata.json")
                continue
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                if not isinstance(metadata, dict):
                    raise ValueError("run metadata must be a JSON object")
                if any(metadata.get(key) != value for key, value in identity.items()):
                    errors.append(f"run metadata identity does not match {run_id}")
                if type(metadata.get("exit_code")) is not int or metadata["exit_code"] != 0:
                    errors.append(f"run metadata for {run_id} does not report success")
                if suite == "v8-small-ticket-profile":
                    if perf_capture_expected:
                        status = str(metadata.get("perf_status", ""))
                        captured = status == "captured" or status.startswith("report_failed:")
                        perf_path = run_dir / "perf.data"
                        report_path = run_dir / "perf-report.txt"
                        if not captured or not perf_path.is_file() or perf_path.stat().st_size == 0 or not report_path.is_file():
                            errors.append(f"profile capture artifacts are incomplete for {run_id}")
                        else:
                            profile_capture_count += 1
                    elif metadata.get("perf_status") != "not_requested":
                        errors.append(f"unexpected profile capture status for {run_id}")
                try:
                    if suite == "v8-small-ticket-profile":
                        from gcp_wal_profile_runner import _load_benchmark_record

                        _load_benchmark_record(result_path)
                    else:
                        record = json.loads(result_path.read_text(encoding="utf-8"))
                        if not isinstance(record, dict):
                            raise ValueError("benchmark result must be a JSON object")
                except (OSError, json.JSONDecodeError, ValueError) as error:
                    errors.append(f"run {run_id} result is invalid: {error}")
            except (OSError, json.JSONDecodeError, ValueError) as error:
                errors.append(f"run {run_id} metadata is invalid: {error}")

        if suite == "v8-small-ticket-profile":
            captures_planned = sum(bool(row["perf_capture"]) for row in schedule)
            if summary.get("perf_captures_planned") != captures_planned:
                errors.append("profile planned capture count does not match schedule")
            if summary.get("perf_captures_completed") != captures_planned or summary.get("perf_capture_complete") is not True:
                errors.append("profile perf captures are incomplete")
            if profile_capture_count != captures_planned:
                errors.append("profile capture artifacts do not match planned captures")
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError, csv.Error) as error:
        errors.append(f"result metadata is invalid: {error}")
        planned = 0
        completed = 0
        suite = None

    return {
        "verified": not errors,
        "suite": suite,
        "runs_planned": planned,
        "runs_completed": completed,
        "errors": errors,
    }


def _sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_artifacts(
    ip: str, key: Path, known_hosts: Path, results_dir: Path, *, before_connect: Callable[[], None]
) -> dict[str, Any]:
    """Bundle and verify compact benchmark artifacts before teardown."""
    import tarfile
    from pathlib import PurePosixPath

    collected = results_dir / "collected"
    attempts: list[dict[str, Any]] = []
    results_collected = False
    summary_complete = False
    benchmark_started = False
    for attempt_number in range(1, 3):
        staging = results_dir / f"collection-attempt-{attempt_number:02d}"
        staging.mkdir(parents=True, exist_ok=False)
        before_connect()
        attempt: dict[str, Any] = {"number": attempt_number, "results_collected": False}
        remote_archive_path: str | None = None
        phase = "archive"
        try:
            remote_pack = _build_remote_archive_command()
            packed = ssh_command(key, known_hosts, ip, remote_pack, timeout=90)
            if packed.returncode:
                raise BenchError(f"Could not build compact result bundle: {packed.stderr.strip()}")
            remote_path_match = re.search(r"ARCHIVE_PATH=(/[A-Za-z0-9._/-]+)", packed.stdout)
            remote_digest = re.search(r"ARCHIVE_SHA256=([0-9a-f]{64})", packed.stdout)
            remote_bytes = re.search(r"ARCHIVE_BYTES=(\d+)", packed.stdout)
            remote_files = re.search(r"ARCHIVE_FILES=(\d+)", packed.stdout)
            if None in (remote_path_match, remote_digest, remote_bytes, remote_files):
                raise BenchError("Remote compact result bundle did not report complete archive metadata")
            remote_archive_path = remote_path_match.group(1)
            remote_archive = PurePosixPath(remote_archive_path)
            if (
                not remote_archive.is_absolute()
                or ".." in remote_archive.parts
                or not remote_archive.name.startswith("gcp-wal-bench-results-")
                or not remote_archive.name.endswith(".tar.gz")
            ):
                raise BenchError("Remote compact result bundle reported an unsafe archive path")
            archive_bytes = int(remote_bytes.group(1))
            archive_files = int(remote_files.group(1))
            attempt.update({
                "remote_archive_path": remote_archive_path,
                "remote_bundle_sha256": remote_digest.group(1),
                "bundle_bytes": archive_bytes,
                "bundle_files": archive_files,
            })
            if archive_bytes > MAX_ARTIFACT_BUNDLE_BYTES:
                raise BenchError(
                    f"Compact result bundle exceeds the {MAX_ARTIFACT_BUNDLE_BYTES} byte transfer limit; "
                    "preserving the VM for artifact recovery"
                )
            phase = "transfer_firewall_audit"
            before_connect()
            phase = "transfer"
            archive_path = staging / "results.tar.gz"
            transfer = scp_from_vm(
                ip, remote_archive_path, archive_path, key, known_hosts,
                timeout=120,
            )
            if transfer.returncode:
                raise BenchError(f"Could not transfer compact result bundle: {transfer.stderr.strip()}")
            local_digest = _sha256_file(archive_path)
            attempt["local_bundle_sha256"] = local_digest
            attempt["local_bundle_bytes"] = archive_path.stat().st_size
            attempt["bundle_checksum_verified"] = local_digest == remote_digest.group(1)
            write_json(staging / "bundle-metadata.json", attempt)
            if not attempt["bundle_checksum_verified"]:
                raise BenchError("Compact result bundle SHA-256 mismatch")
            if archive_path.stat().st_size != archive_bytes:
                raise BenchError("Compact result bundle byte count mismatch")
            unpacked = staging / "unpacked"
            unpacked.mkdir()
            with tarfile.open(archive_path, "r:gz") as bundle:
                members = bundle.getmembers()
                for member in members:
                    path = PurePosixPath(member.name)
                    if path.is_absolute() or ".." in path.parts or not member.isfile():
                        raise BenchError("Compact result bundle contains an unsafe archive member")
                bundle.extractall(unpacked, members=members)
            local_results = unpacked / "results"
            results_collected = local_results.is_dir()
            validation = _verify_result_set(local_results) if results_collected else {
                "verified": False, "errors": ["results directory is missing"], "runs_planned": 0, "runs_completed": 0,
            }
            summary_complete = bool(validation["verified"])
            results_verified = summary_complete
            attempt.update({
                "returncode": transfer.returncode,
                "stderr": transfer.stderr[-4000:],
                "results_collected": results_collected,
                "results_verified": results_verified,
                "summary_complete": summary_complete,
                "validation": validation,
                "unpacked_bundle_files": len(members),
            })
            if results_collected:
                collected.mkdir(parents=True, exist_ok=True)
                collected_results = collected / "results"
                shutil.rmtree(collected_results, ignore_errors=True)
                shutil.copytree(local_results, collected_results)
                shutil.rmtree(unpacked, ignore_errors=True)
                attempt["local_results"] = str(collected_results)
            attempts.append(attempt)
            write_json(staging / "bundle-metadata.json", attempt)
            if results_verified:
                break
        except (BenchError, OSError, json.JSONDecodeError, tarfile.TarError, subprocess.TimeoutExpired) as error:
            if phase == "transfer_firewall_audit":
                raise
            attempt["error"] = str(error)
            attempt["results_collected"] = results_collected
            attempt["results_verified"] = False
            if remote_archive_path is not None:
                attempt["remote_archive_path"] = remote_archive_path
            attempts.append(attempt)
            try:
                write_json(staging / "bundle-metadata.json", attempt)
            except OSError:
                pass
        if attempt_number < 2:
            time.sleep(5)

    # The bootstrap log lives on the VM root disk and can still be useful when
    # setup failed before the Local SSD results directory was created. Collect
    # it independently so a missing results tree does not suppress diagnostics.
    boot_log_destination = (collected if results_collected else results_dir) / "bootstrap.log"
    boot_log_collected = False
    try:
        before_connect()
        boot_log = scp_from_vm(
            ip, "/var/log/gcp-free-trial-wal-bench.log", boot_log_destination,
            key, known_hosts, timeout=30,
        )
        boot_log_collected = boot_log.returncode == 0 and boot_log_destination.is_file()
        if boot_log_collected:
            try:
                benchmark_started = "WAL_BENCHMARK_STARTED" in boot_log_destination.read_text(encoding="utf-8")
            except OSError:
                benchmark_started = False
    except (OSError, subprocess.TimeoutExpired):
        boot_log_collected = False
    write_json(results_dir / "artifact-collection.json", {
        "attempts": attempts,
        "results_collected": results_collected,
        "results_verified": results_collected and summary_complete,
        "summary_complete": summary_complete,
        "bootstrap_log_collected": boot_log_collected,
        "benchmark_started": benchmark_started,
    })
    return {
        "bootstrap_log": boot_log_collected,
        "results": results_collected,
        "results_verified": results_collected and summary_complete,
        "summary_complete": summary_complete,
        "benchmark_started": benchmark_started,
        "attempts": attempts,
    }


def _host_key_lines(response: Any, ip: str) -> list[str]:
    if isinstance(response, list):
        attributes = []
        for item in response:
            if not isinstance(item, dict):
                continue
            namespace = str(item.get("namespace", "")).strip("/")
            key = str(item.get("key", "")).strip("/")
            if namespace and key:
                attributes.append({"queryPath": f"{namespace}/{key}", "value": item.get("value", "")})
    elif isinstance(response, dict):
        attributes = response.get("guestAttributes", [])
    else:
        return []
    allowed_algorithms = {
        "ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521",
        "ssh-ed25519", "ssh-rsa",
    }
    if not isinstance(attributes, list):
        attributes = []
    query_value = response.get("queryValue") or {} if isinstance(response, dict) else {}
    if isinstance(query_value, dict):
        items = query_value.get("items", [])
        if isinstance(items, list):
            attributes.extend(
                {
                    "queryPath": f"{item.get('namespace', 'hostkeys')}/{item.get('key', '')}",
                    "value": item.get("value", ""),
                }
                for item in items
                if isinstance(item, dict)
            )
    lines: set[str] = set()
    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue
        query_path = str(attribute.get("queryPath", ""))
        path_algorithm = next(
            (part for part in reversed(query_path.split("/")) if part in allowed_algorithms), ""
        )
        value_parts = str(attribute.get("value", "")).strip().split()
        if not value_parts:
            continue
        if value_parts[0] in allowed_algorithms:
            algorithm = value_parts[0]
            if path_algorithm in allowed_algorithms and path_algorithm != algorithm:
                continue
            if len(value_parts) < 2:
                continue
            key_data = value_parts[1]
        else:
            algorithm = path_algorithm
            if algorithm not in allowed_algorithms or len(value_parts) != 1:
                continue
            key_data = value_parts[0]
        if not re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", key_data):
            continue
        try:
            key_blob = base64.b64decode(key_data, validate=True)
        except (binascii.Error, ValueError):
            continue
        if len(key_blob) < 4:
            continue
        algorithm_length = struct.unpack(">I", key_blob[:4])[0]
        if algorithm_length > len(key_blob) - 4:
            continue
        try:
            blob_algorithm = key_blob[4:4 + algorithm_length].decode("ascii")
        except UnicodeDecodeError:
            continue
        if blob_algorithm != algorithm:
            continue
        lines.add(f"{ip} {algorithm} {key_data}")
    return sorted(lines)


def wait_for_verified_host_keys(
    gcloud: Gcloud, instance: str, project: str, zone: str, ip: str, known_hosts: Path, deadline: float
) -> None:
    while time.monotonic() < deadline:
        try:
            response = gcloud.json([
                "compute", "instances", "get-guest-attributes", instance,
                "--query-path=hostkeys/", f"--project={project}", f"--zone={zone}",
            ], timeout=45)
        except BenchError:
            # Guest Attributes queries are rate-limited to 10 requests/minute per VM.
            time.sleep(7)
            continue
        lines = _host_key_lines(response, ip)
        if lines:
            known_hosts.write_text("\n".join(lines) + "\n", encoding="utf-8")
            os.chmod(known_hosts, 0o600)
            return
        # Guest Attributes queries are rate-limited to 10 requests/minute per VM.
        time.sleep(7)
    raise BenchError("The VM did not publish verifiable SSH host keys through GCP guest attributes")


def ssh_command(
    key: Path, known_hosts: Path, ip: str, remote_command: str, timeout: int
) -> subprocess.CompletedProcess[str]:
    command = [
        "ssh", "-i", str(key), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={known_hosts}",
        "-o", "GlobalKnownHostsFile=/dev/null", "-o", "ConnectTimeout=8", "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3", f"bench@{ip}", remote_command,
    ]
    # Local argv is not shell-evaluated; dynamic remote arguments are built with shlex.join.
    # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
    return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, check=False, shell=False)


def scp_from_vm(
    ip: str, remote_path: str, destination: Path, key: Path, known_hosts: Path,
    *, recursive: bool = False, timeout: int,
) -> subprocess.CompletedProcess[str]:
    command = ["scp", "-i", str(key), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
               "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={known_hosts}",
               "-o", "GlobalKnownHostsFile=/dev/null", "-o", "ConnectTimeout=8"]
    if recursive:
        command.append("-r")
    command.extend((f"bench@{ip}:{remote_path}", str(destination)))
    # SCP receives separate argv items and does not pass them through a local shell.
    # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
    return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, check=False, shell=False)


def scp_to_vm(
    ip: str, local_path: Path, remote_path: str, key: Path, known_hosts: Path, timeout: int
) -> subprocess.CompletedProcess[str]:
    command = ["scp", "-i", str(key), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
               "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={known_hosts}",
               "-o", "GlobalKnownHostsFile=/dev/null", "-o", "ConnectTimeout=8",
               str(local_path), f"bench@{ip}:{remote_path}"]
    # SCP receives separate argv items and does not pass them through a local shell.
    # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit
    return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, check=False, shell=False)


def append_local_log(path: Path, text: str) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(text)
        if not text.endswith("\n"):
            stream.write("\n")


def provision_and_run(
    args: argparse.Namespace, gcloud: Gcloud, results_dir: Path, preflight_result: dict[str, Any]
) -> int:
    profile_suite = getattr(args, "v8_small_ticket_profile", False)
    missing_tools = [tool for tool in ("ssh", "scp", "ssh-keygen") if not shutil.which(tool)]
    if missing_tools:
        raise BenchError("Required local tools are missing: " + ", ".join(missing_tools))
    create_help = gcloud.run(["beta", "compute", "instances", "create", "--help"])
    help_text = create_help.stdout + create_help.stderr
    if "--max-run-duration" not in help_text or "--instance-termination-action" not in help_text:
        raise BenchError(
            "Installed gcloud beta compute instances create does not expose server-side deletion deadlines; "
            "update the Google Cloud CLI before provisioning."
        )
    suffix = uuid.uuid4().hex[:8]
    stamp = datetime.now(timezone.utc).strftime("%y%m%d-%H%M%S")
    instance = f"walbench-{stamp}-{suffix}"
    tag = f"walbench-{suffix}"
    firewall = f"walbench-ssh-{suffix}"
    key_log = results_dir / "ssh.log"
    ssh_log = results_dir / "remote-session.log"
    lifecycle = {"project": args.project, "zone": args.zone,
                 "instance": instance, "firewall": firewall, "created": False, "firewall_created": False,
                 "network": args.network, "subnet": args.subnet,
                 "benchmark_attempted": False, "benchmark_started": False,
                 "remote_execution_uncertain": False, "collected": False,
                 "summary_complete": False, "results_verified": False,
                 "retained": False, "cleanup": []}
    results_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(results_dir, 0o700)
    key = results_dir / "retained-ssh-key"
    known_hosts = results_dir / "retained-known-hosts"
    lifecycle["recovery_key"] = str(key)
    lifecycle["recovery_known_hosts"] = str(known_hosts)
    lifecycle_path = results_dir / "lifecycle.json"

    def persist_lifecycle() -> None:
        temporary_path = lifecycle_path.with_name(".lifecycle.json.tmp")
        with temporary_path.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(lifecycle, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, lifecycle_path)
        directory_fd = os.open(results_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    # Save resource names before cloud create requests can be interrupted.
    persist_lifecycle()

    # VPC firewall rules are additive, so check the existing network before
    # creating the benchmark VM or its temporary SSH rule.
    network_region = zone_region(args.zone)
    audit_ssh_firewall_rules(gcloud, args.project, args.network, network_region, tag, args.ssh_source_range)

    with tempfile.TemporaryDirectory(prefix="gcp-wal-bench-") as temporary:
        keygen = subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)],
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
                                shell=False)
        if keygen.returncode:
            raise BenchError(f"ssh-keygen failed: {keygen.stderr.strip()}")
        public_key = Path(str(key) + ".pub").read_text(encoding="utf-8").strip()
        key_metadata = Path(temporary) / "ssh-keys"
        key_metadata.write_text(f"bench:{public_key}\n", encoding="utf-8")
        os.chmod(key, 0o600)
        with key.open("rb") as stream:
            os.fsync(stream.fileno())
        key_directory_fd = os.open(results_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(key_directory_fd)
        finally:
            os.close(key_directory_fd)

        connected = False
        ip: str | None = None

        def save_reconnect_instructions(*, cleanup_uncertain: bool = False) -> None:
            if not ip:
                raise BenchError("cannot write reconnect instructions before the VM address is known")
            if not key.is_file():
                raise BenchError(f"retained SSH private key is unavailable at {key}")
            os.chmod(key, 0o600)
            if known_hosts.is_file():
                os.chmod(known_hosts, 0o600)
                with known_hosts.open("rb") as stream:
                    os.fsync(stream.fileno())
            connect_command = shlex.join([
                "ssh", "-i", str(key), "-o", "IdentitiesOnly=yes",
                "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "ConnectTimeout=8", "-o", "ServerAliveInterval=30",
                "-o", "ServerAliveCountMax=3",
                "-o", f"UserKnownHostsFile={known_hosts}",
                "-o", "GlobalKnownHostsFile=/dev/null", f"bench@{ip}",
            ])
            reconnect_path = results_dir / "reconnect.txt"
            temporary_reconnect = reconnect_path.with_name(".reconnect.txt.tmp")
            temporary_reconnect.write_text(
                retained_vm_reconnect_instructions(
                    termination_timestamp=lifecycle.get("termination_timestamp", "see instance.json"),
                    network=lifecycle.get("network"),
                    subnet=lifecycle.get("subnet"),
                    firewall=firewall,
                    project=args.project,
                    connect_command=connect_command,
                    cleanup_uncertain=cleanup_uncertain,
                ),
                encoding="utf-8",
            )
            os.chmod(temporary_reconnect, 0o600)
            with temporary_reconnect.open("rb") as stream:
                os.fsync(stream.fileno())
            os.replace(temporary_reconnect, reconnect_path)
            directory_fd = os.open(results_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)

        try:
            create = [
                "beta", "compute", "instances", "create", instance,
                f"--project={args.project}", f"--zone={args.zone}", f"--machine-type={args.machine_type}",
                f"--image={preflight_result['boot_image']['name']}", "--image-project=ubuntu-os-cloud",
                "--boot-disk-size=50GB", "--boot-disk-type=hyperdisk-balanced", "--boot-disk-auto-delete",
                "--boot-disk-provisioned-iops=3000", "--boot-disk-provisioned-throughput=140",
                f"--max-run-duration={args.max_runtime_minutes}m", "--instance-termination-action=DELETE",
                "--no-service-account", "--no-scopes",
                f"--network={args.network}", f"--tags={tag}", f"--metadata-from-file=ssh-keys={key_metadata}",
                "--metadata=block-project-ssh-keys=TRUE,enable-guest-attributes=TRUE,enable-oslogin=FALSE",
                "--quiet",
            ]
            if args.subnet:
                create.append(f"--subnet={args.subnet}")
            # Mark intent before invoking gcloud: a transport failure may happen
            # after GCP accepted the insert request.
            lifecycle["created"] = True
            persist_lifecycle()
            gcloud.run(create, timeout=600)
            instance_info = gcloud.json([
                "compute", "instances", "describe", instance,
                f"--project={args.project}", f"--zone={args.zone}",
            ])
            interfaces = instance_info.get("networkInterfaces", [])
            access_configs = interfaces[0].get("accessConfigs", []) if interfaces else []
            ip = access_configs[0].get("natIP") if access_configs else None
            if not ip:
                raise BenchError("Created VM has no external IPv4 address; cannot transfer artifacts over SSH")

            # Recheck effective VPC and inherited policy rules immediately before
            # opening the VM's SSH path, narrowing the preflight/create race.
            audit_ssh_firewall_rules(
                gcloud, args.project, args.network, network_region, tag, args.ssh_source_range
            )

            write_json(results_dir / "instance.json", {
                "project": args.project, "zone": args.zone, "instance": instance,
                "machine_type": args.machine_type, "external_ip": ip,
                "termination_timestamp": instance_info.get("terminationTimestamp"),
                "auto_delete_deadline_minutes": args.max_runtime_minutes,
            })
            lifecycle["external_ip"] = ip
            lifecycle["termination_timestamp"] = instance_info.get("terminationTimestamp")
            persist_lifecycle()

            termination_time = instance_info.get("terminationTimestamp")
            if termination_time:
                deadline_epoch = datetime.fromisoformat(termination_time.replace("Z", "+00:00")).timestamp()
            else:
                creation_time = instance_info.get("creationTimestamp")
                if not creation_time:
                    raise BenchError("GCP did not report a VM creation time or auto-delete deadline")
                creation_epoch = datetime.fromisoformat(creation_time.replace("Z", "+00:00")).timestamp()
                deadline_epoch = creation_epoch + args.max_runtime_minutes * 60

            deadline = time.monotonic() + deadline_epoch - time.time() - 600
            if deadline <= time.monotonic():
                raise BenchError("Less than ten minutes remain before the VM auto-delete deadline")
            wait_for_verified_host_keys(gcloud, instance, args.project, args.zone, ip, known_hosts, deadline)
            os.chmod(known_hosts, 0o600)
            with known_hosts.open("rb") as stream:
                os.fsync(stream.fileno())

            # Create ingress only after the VM exists, so an interruption before
            # VM creation cannot orphan a firewall rule. The server-side VM
            # deadline remains the fallback for a local runner that disappears.
            lifecycle["firewall_created"] = True
            persist_lifecycle()
            gcloud.run([
                "compute", "firewall-rules", "create", firewall, f"--project={args.project}",
                f"--network={args.network}", "--direction=INGRESS", "--priority=900", "--action=ALLOW",
                "--rules=tcp:22", f"--source-ranges={args.ssh_source_range}", f"--target-tags={tag}",
                "--description=Temporary restricted SSH for disposable WAL benchmark VM",
            ], timeout=180)
            while time.monotonic() < deadline:
                # Recheck immediately before each new connection so a changed
                # effective policy fails closed before this runner opens SSH.
                audit_ssh_firewall_rules(
                    gcloud, args.project, args.network, network_region, tag, args.ssh_source_range
                )
                probe = ssh_command(key, known_hosts, ip, "true", timeout=12)
                append_local_log(key_log, probe.stdout + probe.stderr)
                if probe.returncode == 0:
                    connected = True
                    break
                time.sleep(10)
            if not connected:
                raise BenchError("SSH did not become ready before timeout")

            remote_script = RUNNERS
            audit_ssh_firewall_rules(
                gcloud, args.project, args.network, network_region, tag, args.ssh_source_range
            )
            scp_result = scp_to_vm(ip, remote_script, "/tmp/run-wal-bench.sh", key, known_hosts, timeout=120)
            append_local_log(key_log, scp_result.stdout + scp_result.stderr)
            if scp_result.returncode:
                raise BenchError(f"Could not copy remote runner: {scp_result.stderr.strip()}")
            if not MATRIX_MODULE.is_file():
                raise BenchError(f"Benchmark matrix module is not present in this checkout: {MATRIX_MODULE}")
            audit_ssh_firewall_rules(
                gcloud, args.project, args.network, network_region, tag, args.ssh_source_range
            )
            matrix_copy = scp_to_vm(
                ip, MATRIX_MODULE, "/tmp/gcp_wal_matrix.py", key, known_hosts, timeout=120
            )
            append_local_log(key_log, matrix_copy.stdout + matrix_copy.stderr)
            if matrix_copy.returncode:
                raise BenchError(f"Could not copy benchmark matrix module: {matrix_copy.stderr.strip()}")
            if profile_suite:
                if not PROFILE_RUNNER.is_file():
                    raise BenchError(f"Profiling helper is not present in this checkout: {PROFILE_RUNNER}")
                profile_copy = scp_to_vm(
                    ip, PROFILE_RUNNER, "/tmp/gcp-wal-profile-runner.py", key, known_hosts, timeout=120
                )
                append_local_log(key_log, profile_copy.stdout + profile_copy.stderr)
                if profile_copy.returncode:
                    raise BenchError(f"Could not copy profile runner: {profile_copy.stderr.strip()}")

            safe_deadline_epoch = int(deadline_epoch - 600)
            remote_command = "sudo -n bash /tmp/run-wal-bench.sh " + shlex.join([
                args.commit.lower(), str(args.num), args.project, args.zone, instance, str(safe_deadline_epoch),
                args.machine_type, ",".join(str(writer) for writer in args.writers),
                "v8-small-ticket-profile" if profile_suite else (
                    "rfc026-full" if args.full_rfc026_matrix else "batch64"
                ),
            ])
            remaining = max(30, int(deadline - time.monotonic()))
            try:
                audit_ssh_firewall_rules(
                    gcloud, args.project, args.network, network_region, tag, args.ssh_source_range
                )
                lifecycle["benchmark_attempted"] = True
                persist_lifecycle()
                run = ssh_command(key, known_hosts, ip, remote_command, timeout=remaining)
            except subprocess.TimeoutExpired as error:
                lifecycle["remote_execution_uncertain"] = True
                persist_lifecycle()
                append_local_log(ssh_log, f"REMOTE RUN TIMED OUT after {remaining} seconds\n")
                raise BenchError("Remote benchmark exceeded the local run deadline; collecting partial results") from error
            append_local_log(ssh_log, run.stdout + run.stderr)
            if run.returncode:
                raise BenchError(f"Remote benchmark failed with exit code {run.returncode}")
            lifecycle["benchmark_started"] = True
            persist_lifecycle()
            return_code = 0
        except BaseException as error:
            return_code = 1
            lifecycle["error"] = str(error)
            persist_lifecycle()
            append_local_log(results_dir / "orchestrator.log", f"ERROR: {error}\n")
        finally:
            try:
                if lifecycle["created"]:
                    try:
                        # Copy results even when a benchmark or SSH command failed.
                        if connected:
                            collection = collect_artifacts(
                                ip, key, known_hosts, results_dir,
                                before_connect=lambda: audit_ssh_firewall_rules(
                                    gcloud, args.project, args.network, network_region, tag, args.ssh_source_range
                                ),
                            )
                            lifecycle["artifact_collection"] = collection
                            lifecycle["benchmark_started"] = bool(
                                lifecycle["benchmark_started"] or collection.get("benchmark_started")
                            )
                            lifecycle["collected"] = bool(collection.get("results"))
                            lifecycle["summary_complete"] = bool(collection.get("summary_complete"))
                            lifecycle["results_verified"] = bool(collection.get("results_verified"))
                            lifecycle["retained"] = should_retain_vm(args, return_code, lifecycle)
                            persist_lifecycle()
                            if not lifecycle["results_verified"]:
                                return_code = 1
                                lifecycle["cleanup"].append(
                                    "result set is incomplete or unverified; preserving the VM and network for recovery"
                                )
                                persist_lifecycle()
                            elif not collection.get("bootstrap_log"):
                                lifecycle["cleanup"].append("bootstrap log could not be collected; benchmark results are local")
                                persist_lifecycle()
                        if should_retain_vm(args, return_code, lifecycle):
                            lifecycle["retained"] = True
                            save_reconnect_instructions()
                            lifecycle["retained"] = True
                            lifecycle["cleanup"].append(
                                "VM and restricted SSH firewall retained; only the VM has a server-side delete deadline"
                            )
                            try:
                                persist_lifecycle()
                            except BaseException:
                                # The in-memory retention flag still prevents teardown if
                                # the local filesystem cannot persist recovery metadata.
                                raise
                    except BaseException as error:
                        return_code = 1
                        if should_retain_vm(args, return_code, lifecycle):
                            lifecycle["retained"] = True
                            try:
                                save_reconnect_instructions()
                            except BaseException as recovery_error:
                                print(
                                    f"VM cleanup is disabled, but reconnect instructions could not be saved: "
                                    f"{recovery_error}; recovery key path: {key}",
                                    file=sys.stderr,
                                )
                        lifecycle["cleanup"].append(f"local artifact/retention step failed: {error}")
                        persist_lifecycle()
            finally:
                # Do not destroy the only remaining copy of a completed or
                # uncertain benchmark until its full result set is verified locally.
                if lifecycle["retained"]:
                    cleanup_messages, cleanup_verification, cleanup_failed = [], {}, False
                else:
                    cleanup_messages, cleanup_verification, cleanup_failed = cleanup_gcp_resources(
                        gcloud,
                        project=args.project,
                        zone=args.zone,
                        instance=instance,
                        firewall=firewall,
                        vm_created=lifecycle["created"],
                        firewall_created=lifecycle["firewall_created"],
                    )
                lifecycle["cleanup"].extend(cleanup_messages)
                if cleanup_verification:
                    lifecycle["cleanup_verification"] = cleanup_verification
                if cleanup_failed:
                    return_code = 1
                    if lifecycle["created"]:
                        lifecycle["retained"] = True
                        lifecycle["cleanup"].append(
                            "resource cleanup could not be verified; preserving local recovery credentials"
                        )
                        try:
                            save_reconnect_instructions(cleanup_uncertain=True)
                        except BaseException as recovery_error:
                            print(
                                f"Cloud cleanup is uncertain and reconnect instructions could not be saved: "
                                f"{recovery_error}; recovery key path: {key}",
                                file=sys.stderr,
                            )
                elif not lifecycle["retained"]:
                    for credential in (key, Path(str(key) + ".pub"), known_hosts):
                        credential.unlink(missing_ok=True)
                persist_lifecycle()

    return return_code


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.results_dir is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        args.results_dir = Path("bench-results") / f"gcp-wal-{stamp}"
    results_dir: Path = args.results_dir.resolve()
    try:
        # Atomic reservation prevents two local runners from sharing results
        # and overwriting each other's captures.
        create_private_results_directory(results_dir)
    except FileExistsError:
        print(
            f"Results directory already exists; refusing to reuse it: {results_dir}",
            file=sys.stderr,
        )
        return 2
    gcloud = Gcloud(args.project, results_dir / "gcloud-commands.jsonl")

    try:
        report = preflight(
            gcloud, args.project, args.zone, args.network, args.subnet, args.machine_type,
        )
        report["benchmark_commit"] = args.commit.lower()
        report["writer_counts"] = list(args.writers)
        if args.v8_small_ticket_profile:
            report["benchmark_suite"] = "v8-small-ticket-profile"
            try:
                from gcp_wal_profile_runner import build_profile_plan
            except ModuleNotFoundError as error:
                raise BenchError(
                    "The optional V8 profile helper is not present in this checkout; "
                    "use a checkout containing scripts/gcp_wal_profile_runner.py or select another suite."
                ) from error
            report["benchmark_plan"] = build_profile_plan(
                args.writers, machine_type=args.machine_type, machine_spec=MACHINE_SPECS[args.machine_type],
                commit=args.commit.lower(),
            )
        else:
            report["benchmark_suite"] = "rfc026-full" if args.full_rfc026_matrix else "batch64"
            report["benchmark_plan"] = build_benchmark_plan(
                full_matrix=args.full_rfc026_matrix,
                writers=args.writers,
                machine_type=args.machine_type,
                machine_spec=MACHINE_SPECS[args.machine_type],
                num=args.num,
                commit=args.commit.lower(),
            )
        report["machine_type"] = args.machine_type
        report["free_trial_attestation"] = bool(args.confirm_free_trial)
        if args.estimated_hourly_cost_usd is not None:
            report["cost_guard"] = {
                "operator_estimated_hourly_cost_usd": args.estimated_hourly_cost_usd,
                "estimated_max_spend_usd": args.estimated_hourly_cost_usd * args.max_runtime_minutes / 60,
                "maximum_estimated_spend_usd": args.max_estimated_spend_usd,
                "maximum_runtime_minutes": args.max_runtime_minutes,
                "note": (
                    "Admission guard only. The estimate depends on operator-supplied pricing and does not "
                    "cap unmodeled usage or replace the Free Trial billing attestation."
                ),
            }
        write_json(results_dir / "preflight.json", report)
        print(json.dumps(report, indent=2))
    except BaseException as error:
        write_json(results_dir / "preflight-failure.json", {
            "project": args.project,
            "zone": args.zone,
            "error": str(error),
            "vm_created": False,
            "note": "Preflight failed before any VM or firewall resource was created.",
        })
        print(f"Preflight failed; no VM was created: {error}", file=sys.stderr)
        print(f"Preflight diagnostics: {results_dir}", file=sys.stderr)
        return 2

    if args.preflight_only:
        print(f"Read-only preflight passed. No cloud resources were created. Results: {results_dir}")
        return 0

    print("Preflight passed. Starting the benchmark VM; billing, APIs, and quota settings will not be changed.")
    try:
        run_status = provision_and_run(args, gcloud, results_dir, report)
    except BaseException as error:
        write_json(results_dir / "run-failure.json", {
            "project": args.project,
            "zone": args.zone,
            "error": str(error),
            "resource_state": "provisioning may have started; inspect lifecycle.json and gcloud-commands.jsonl",
            "note": "The server-side VM deletion deadline remains the fallback if local cleanup did not complete.",
        })
        print(f"GCP setup/run failed; provisioning may have started: {error}", file=sys.stderr)
        print(f"Run diagnostics: {results_dir}", file=sys.stderr)
        return 2
    print(f"Benchmark artifacts: {results_dir}")
    return run_status


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda _signum, _frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    raise SystemExit(main())
