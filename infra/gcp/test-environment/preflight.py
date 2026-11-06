#!/usr/bin/env python3
"""Read-only GCP quota and Local SSD preflight for the reusable test host."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


LOCAL_SSD_GB_PER_DEVICE = 375
BOOT_DISK_TYPE = "hyperdisk-balanced"


class PreflightError(RuntimeError):
    """A required read-only GCP capacity check failed or was ambiguous."""


def _run_gcloud(args: list[str], *, json_output: bool = True, timeout: int = 180) -> Any:
    command = ["gcloud", *args]
    if json_output:
        command.append("--format=json")
    env = os.environ.copy()
    env["CLOUDSDK_CORE_SHOULD_PROMPT_TO_ENABLE_API"] = "false"
    try:
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            shell=False,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PreflightError(f"could not run {command[0]}: {error}") from error
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise PreflightError(f"gcloud command failed ({result.returncode}): {' '.join(command)}\n{detail}")
    if not json_output:
        return result.stdout.strip()
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise PreflightError(f"gcloud returned invalid JSON for {' '.join(args)}") from error


def _number(value: Any, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise PreflightError(f"quota response is missing numeric {label}") from error
    if not math.isfinite(result) or result < 0:
        raise PreflightError(f"quota response has invalid {label}: {value!r}")
    return result


def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def _zone_region(zone: str) -> str:
    region, separator, suffix = zone.rpartition("-")
    if not separator or not re.fullmatch(r"[a-z]", suffix):
        raise PreflightError(f"invalid zone {zone!r}; expected a zone such as us-central1-a")
    return region


def _resource_region(resource: dict[str, Any]) -> str | None:
    region = str(resource.get("region", "")).rstrip("/").split("/")[-1]
    if region:
        return region
    zone = str(resource.get("zone", "")).rstrip("/").split("/")[-1]
    return _zone_region(zone) if zone else None


def _machine_name(instance: dict[str, Any]) -> str:
    return str(instance.get("machineType", "")).rstrip("/").split("/")[-1]


def _instance_ssd_gb(instance: dict[str, Any]) -> int:
    disks = instance.get("disks")
    if not isinstance(disks, list):
        raise PreflightError(f"cannot read attached-disk inventory for {instance.get('name', '<unnamed>')}")
    total = 0
    for disk in disks:
        if not isinstance(disk, dict) or disk.get("type") not in {"PERSISTENT", "SCRATCH"}:
            raise PreflightError(f"cannot identify an attached disk on {instance.get('name', '<unnamed>')}")
        if disk["type"] == "SCRATCH":
            size = _number(disk.get("diskSizeGb"), "Local SSD diskSizeGb")
            if size <= 0 or not size.is_integer():
                raise PreflightError(f"invalid Local SSD size on {instance.get('name', '<unnamed>')}")
            total += int(size)
    return total


def _quota_records(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, dict):
        response = response.get("quotaInfos")
    if not isinstance(response, list) or any(not isinstance(item, dict) for item in response):
        raise PreflightError("Cloud Quotas did not return a quotaInfos list")
    return response


def _cloud_quota(
    records: list[dict[str, Any]], kind: str, region: str, family: str,
) -> tuple[float, dict[str, Any], bool]:
    matches: list[tuple[int, float, dict[str, Any]]] = []
    for record in records:
        names = _norm(record.get("quotaId", "")) + _norm(record.get("metric", ""))
        if kind == "local_ssd":
            is_match = "localssd" in names
        elif kind == "hyperdisk_balanced":
            is_match = "hdbtotalgb" in names or "hyperdiskbalancedtotalstorage" in names
        else:
            is_match = "cpus" in names
        if not is_match:
            continue
        dimensions_list = record.get("dimensionsInfos", record.get("dimensionsInfo", []))
        if not isinstance(dimensions_list, list):
            continue
        for dimension_info in dimensions_list:
            if not isinstance(dimension_info, dict):
                continue
            dimensions = {
                str(key).lower(): str(value)
                for key, value in (dimension_info.get("dimensions") or {}).items()
            }
            locations = dimension_info.get("applicableLocations")
            if dimensions.get("region") != region and not (isinstance(locations, list) and region in locations):
                continue
            scoped_family = dimensions.get("vm_family")
            if scoped_family and scoped_family.upper() != family.upper():
                continue
            if kind == "c4_cpu" and not scoped_family:
                continue
            details = dimension_info.get("details") or {}
            raw_limit = details.get("quotaValue", details.get("value", details.get("limit")))
            if raw_limit is None:
                continue
            specificity = int("region" in dimensions) + int("vm_family" in dimensions)
            source = {
                "quota_id": record.get("quotaId"),
                "metric": record.get("metric"),
                "dimensions": dimensions,
                "applicable_locations": locations,
            }
            matches.append((specificity, _number(raw_limit, f"{kind} quota limit"), source))
    if not matches:
        raise PreflightError(
            f"cannot verify {kind} quota for region={region}, vm_family={family}; "
            "Cloud Quotas must expose an applicable quota and the API must already be enabled"
        )
    specificity = max(item[0] for item in matches)
    selected = min((item for item in matches if item[0] == specificity), key=lambda item: item[1])
    return selected[1], selected[2], "vm_family" in selected[2]["dimensions"]


def _regional_quota(region: dict[str, Any], metric: str) -> tuple[float, float]:
    matches = [
        item for item in region.get("quotas", [])
        if isinstance(item, dict) and _norm(item.get("metric", "")) == _norm(metric)
    ]
    if len(matches) != 1:
        raise PreflightError(f"cannot uniquely verify regional {metric} quota")
    return _number(matches[0].get("limit"), f"{metric} quota limit"), _number(
        matches[0].get("usage"), f"{metric} quota usage"
    )


def _global_quota(project_info: dict[str, Any], metric: str) -> tuple[float, float]:
    matches = [
        item for item in project_info.get("quotas", [])
        if isinstance(item, dict) and _norm(item.get("metric", "")) == _norm(metric)
    ]
    if len(matches) != 1:
        raise PreflightError(f"cannot uniquely verify global {metric} quota")
    return _number(matches[0].get("limit"), f"{metric} quota limit"), _number(
        matches[0].get("usage"), f"{metric} quota usage"
    )


def _hyperdisk_usage_gb(disks: Any, region: str) -> float:
    if not isinstance(disks, list):
        raise PreflightError("cannot inspect existing disk inventory")
    total = 0.0
    for disk in disks:
        if not isinstance(disk, dict):
            raise PreflightError("cannot interpret an existing disk inventory entry")
        disk_type = str(disk.get("type", "")).rstrip("/").split("/")[-1]
        if disk_type not in {"hyperdisk-balanced", "hyperdisk-balanced-high-availability"}:
            continue
        disk_region = _resource_region(disk)
        if disk_region is None:
            raise PreflightError(f"cannot determine region for disk {disk.get('name', '<unnamed>')}")
        if disk_region != region:
            continue
        size = _number(disk.get("sizeGb"), "Hyperdisk size")
        if size <= 0:
            raise PreflightError(f"invalid size for Hyperdisk {disk.get('name', '<unnamed>')}")
        total += size * (2 if disk_type == "hyperdisk-balanced-high-availability" else 1)
    return total


def preflight(project: str, zone: str, machine_type: str, boot_disk_gb: int) -> dict[str, Any]:
    region = _zone_region(zone)
    account_lines = _run_gcloud(
        ["auth", "list", "--filter=status:ACTIVE", "--format=value(account)"], json_output=False
    ).splitlines()
    if len(account_lines) != 1:
        raise PreflightError("expected exactly one active gcloud account; run gcloud auth login")

    project_info = _run_gcloud(["projects", "describe", project])
    if project_info.get("projectId") != project or project_info.get("lifecycleState") != "ACTIVE":
        raise PreflightError(f"project {project!r} is not active")
    zone_info = _run_gcloud(["compute", "zones", "describe", zone, f"--project={project}"])
    if zone_info.get("status") != "UP":
        raise PreflightError(f"zone {zone} is not UP")
    machine = _run_gcloud([
        "compute", "machine-types", "describe", machine_type,
        f"--zone={zone}", f"--project={project}",
    ])
    bundled = machine.get("bundledLocalSsds")
    if not isinstance(bundled, dict) or bundled.get("defaultInterface") != "NVME":
        raise PreflightError(f"{machine_type} does not expose bundled NVMe Local SSD metadata")
    ssd_count = bundled.get("partitionCount")
    if not isinstance(ssd_count, int) or ssd_count < 1:
        raise PreflightError(f"{machine_type} has no bundled Local SSDs")
    if not machine_type.startswith("c4-"):
        raise PreflightError("quota accounting currently supports the selected C4 Local SSD test-host family only")
    vcpus = int(machine.get("guestCpus", 0))
    if vcpus <= 0:
        raise PreflightError(f"{machine_type} returned an invalid vCPU count")

    quota_response = _run_gcloud([
        "quotas", "info", "list", "--service=compute.googleapis.com",
        f"--project={project}", f"--billing-project={project}",
    ])
    quota_records = _quota_records(quota_response)
    c4_cpu_limit, c4_cpu_source, _ = _cloud_quota(quota_records, "c4_cpu", region, "C4")
    local_ssd_limit, local_ssd_source, family_scoped_ssd = _cloud_quota(
        quota_records, "local_ssd", region, "C4"
    )
    hyperdisk_limit, hyperdisk_source, _ = _cloud_quota(
        quota_records, "hyperdisk_balanced", region, "C4"
    )

    instances = _run_gcloud(["compute", "instances", "list", f"--project={project}"])
    if not isinstance(instances, list):
        raise PreflightError("cannot inspect current instance inventory")
    regional_instances = [
        item for item in instances
        if _resource_region(item) == region
    ]
    reservations = _run_gcloud(["compute", "reservations", "list", f"--project={project}"])
    if not isinstance(reservations, list):
        raise PreflightError("cannot inspect reservation inventory")
    family_reservations = []
    regional_reservations = []
    for reservation in reservations:
        if _resource_region(reservation) != region:
            continue
        regional_reservations.append(str(reservation.get("name", "<unnamed>")))
        properties = (reservation.get("specificReservation") or {}).get("instanceProperties") or {}
        reserved_type = str(properties.get("machineType", "")).rstrip("/").split("/")[-1]
        if not reserved_type:
            raise PreflightError("cannot determine a regional reservation's machine type")
        if reserved_type.startswith("c4-"):
            family_reservations.append(str(reservation.get("name", reserved_type)))
    if family_reservations:
        raise PreflightError(
            "C4 reservations make CPU/Local SSD inventory ambiguous: " + ", ".join(family_reservations)
        )
    if not family_scoped_ssd and regional_reservations:
        raise PreflightError(
            "regional Local SSD quota is not family-scoped and reservations prevent safe accounting: "
            + ", ".join(regional_reservations)
        )

    cpu_used = 0
    c4_ssd_used = 0
    region_ssd_used = 0
    machine_cache: dict[tuple[str, str], int] = {}
    for instance in regional_instances:
        name = _machine_name(instance)
        is_c4 = name.startswith("c4-")
        if is_c4 and str(instance.get("status", "")).upper() != "TERMINATED":
            instance_zone = str(instance.get("zone", "")).rstrip("/").split("/")[-1]
            cache_key = (name, instance_zone)
            if cache_key not in machine_cache:
                details = _run_gcloud([
                    "compute", "machine-types", "describe", name,
                    f"--zone={instance_zone}", f"--project={project}",
                ])
                machine_cache[cache_key] = int(details.get("guestCpus", 0))
            if machine_cache[cache_key] <= 0:
                raise PreflightError(f"cannot account CPU use for instance {instance.get('name', name)}")
            cpu_used += machine_cache[cache_key]
        scratch_gb = _instance_ssd_gb(instance)
        region_ssd_used += scratch_gb
        if is_c4:
            c4_ssd_used += scratch_gb

    family_cpu_available = c4_cpu_limit - cpu_used
    ssd_used = c4_ssd_used if family_scoped_ssd else region_ssd_used
    ssd_available = local_ssd_limit - ssd_used
    required_ssd_gb = ssd_count * LOCAL_SSD_GB_PER_DEVICE
    if family_cpu_available < vcpus:
        raise PreflightError(
            f"insufficient C4 CPU quota in {region}: available={family_cpu_available:g}, need={vcpus}"
        )
    if ssd_available < required_ssd_gb:
        raise PreflightError(
            f"insufficient C4 Local SSD quota in {region}: available={ssd_available:g} GB, "
            f"need={required_ssd_gb} GB"
        )

    region_info = _run_gcloud(["compute", "regions", "describe", region, f"--project={project}"])
    if region_info.get("status") != "UP":
        raise PreflightError(f"region {region} is not UP")
    region_quota_report = {}
    for metric, required in (
        ("INSTANCES", 1),
        ("IN_USE_ADDRESSES", 1),
        ("SUBNETWORKS", 1),
    ):
        limit, usage = _regional_quota(region_info, metric)
        available = limit - usage
        if available < required:
            raise PreflightError(f"insufficient regional {metric} quota: available={available:g}, need={required}")
        region_quota_report[metric.lower()] = {
            "limit": limit, "usage": usage, "available": available, "required": required,
        }

    hdb_usage = _hyperdisk_usage_gb(
        _run_gcloud(["compute", "disks", "list", f"--project={project}"]), region
    )
    hdb_available = hyperdisk_limit - hdb_usage
    if hdb_available < boot_disk_gb:
        raise PreflightError(
            f"insufficient Hyperdisk Balanced quota in {region}: available={hdb_available:g} GB, "
            f"need={boot_disk_gb} GB"
        )

    global_quotas = _run_gcloud(["compute", "project-info", "describe", f"--project={project}"])
    global_quota_report = {}
    for metric, required in (("FIREWALLS", 1), ("NETWORKS", 1)):
        limit, usage = _global_quota(global_quotas, metric)
        available = limit - usage
        if available < required:
            raise PreflightError(f"insufficient global {metric} quota: available={available:g}, need={required}")
        global_quota_report[metric.lower()] = {
            "limit": limit, "usage": usage, "available": available, "required": required,
        }
    global_cpu_records = [
        item for item in global_quotas.get("quotas", [])
        if isinstance(item, dict) and _norm(item.get("metric", "")) == _norm("CPUS_ALL_REGIONS")
    ]
    if not global_cpu_records:
        global_quota_report["cpus_all_regions"] = {"applicable": False}
    else:
        if len(global_cpu_records) != 1:
            raise PreflightError("cannot uniquely verify global CPUS_ALL_REGIONS quota")
        limit = _number(global_cpu_records[0].get("limit"), "global CPU quota limit")
        usage = _number(global_cpu_records[0].get("usage"), "global CPU quota usage")
        available = limit - usage
        if available < vcpus:
            raise PreflightError(
                f"insufficient global CPU quota: available={available:g}, need={vcpus}"
            )
        global_quota_report["cpus_all_regions"] = {
            "applicable": True, "limit": limit, "usage": usage,
            "available": available, "required": vcpus,
        }

    return {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "project": project,
        "active_gcloud_account": account_lines[0],
        "zone": zone,
        "region": region,
        "zone_status": zone_info.get("status"),
        "machine_type": machine_type,
        "guest_vcpus": vcpus,
        "local_ssd": {
            "count": ssd_count,
            "interface": bundled["defaultInterface"],
            "required_quota_gb": required_ssd_gb,
        },
        "boot_disk": {"type": BOOT_DISK_TYPE, "size_gb": boot_disk_gb},
        "quotas": {
            "c4_cpu": {
                "limit": c4_cpu_limit, "inventory_used": cpu_used,
                "available": family_cpu_available, "required": vcpus, "source": c4_cpu_source,
            },
            "local_ssd": {
                "limit": local_ssd_limit, "inventory_used": ssd_used,
                "available": ssd_available, "required": required_ssd_gb,
                "family_scoped": family_scoped_ssd, "source": local_ssd_source,
            },
            "hyperdisk_balanced": {
                "limit_gb": hyperdisk_limit, "inventory_used_gb": hdb_usage,
                "available_gb": hdb_available, "required_gb": boot_disk_gb,
                "source": hyperdisk_source,
            },
            **region_quota_report,
            **global_quota_report,
        },
        "existing_region_instance_count": len(regional_instances),
        "actual_free_host_capacity_guaranteed": False,
        "billing_mutated": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--zone", required=True)
    parser.add_argument("--machine-type", default="c4-standard-16-lssd")
    parser.add_argument("--boot-disk-gb", type=int, default=50)
    parser.add_argument("--output", type=Path, help="Optional path for the JSON report")
    args = parser.parse_args(argv)
    if not 20 <= args.boot_disk_gb <= 100:
        parser.error("--boot-disk-gb must be between 20 and 100")
    try:
        report = preflight(args.project, args.zone, args.machine_type, args.boot_disk_gb)
    except PreflightError as error:
        print(f"Preflight failed; Terraform must not provision: {error}", file=sys.stderr)
        return 2
    output = json.dumps(report, indent=2, sort_keys=True) + "\n"
    print(output, end="")
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
