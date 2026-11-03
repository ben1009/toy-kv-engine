"""Read-only WAL extent snapshots and passive, file-attributed I/O diagnostics.

FIEMAP is issued without SYNC. File data is read through the filesystem only,
after timed writes stop. No raw device access, extra flush or write is used.
"""

import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
import fcntl
import hashlib
import json
import os
from pathlib import Path
import statistics
import struct
import time
import zlib

import wal_trace_regimes as trace


FRAME_BYTES = 4096
FS_IOC_FIEMAP = 0xC020660B
FIEMAP_SAFE_FLAGS = 0x1 | 0x800  # LAST and UNWRITTEN only
EXTRA_EVENTS = ("ext4/ext4_ext_map_blocks_exit", "ext4/ext4_free_blocks")
EXTENT_PROTOCOL = "kv-engine.passive-wal-extents.v1"
EXTENT_POLICY = {
    "counter_interval_ms": 100, "min_interval_ns": 50_000_000,
    "window_samples": 16, "confirm_windows": 5, "guard_ns": 20_000_000,
    "max_sample_gap_ns": 250_000_000, "min_percentile_samples": 100,
    "mode": "calibration_only_no_performance_ratio",
}


def monotonic_ns():
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def fiemap(fd):
    """Page through the UAPI's 32-byte header and 56-byte extent records."""
    records, start = [], 0
    for _ in range(256):
        buffer = bytearray(32 + 256 * 56)
        struct.pack_into("=QQIIII", buffer, 0, start, (1 << 64) - 1 - start, 0, 0, 256, 0)
        fcntl.ioctl(fd, FS_IOC_FIEMAP, buffer, True)
        count = struct.unpack_from("=I", buffer, 20)[0]
        if count > 256 or struct.unpack_from("=I", buffer, 16)[0] != 0:
            raise ValueError("Invalid FIEMAP result or unexpected sync flags")
        for index in range(count):
            logical, physical, length, _, _, flags = struct.unpack_from("=QQQQQI", buffer, 32 + index * 56)
            if not length or logical < start or (records and logical < records[-1]["logical"] + records[-1]["length"]):
                raise ValueError("Unordered or empty FIEMAP extent")
            records.append({"logical": logical, "physical": physical, "length": length, "flags": flags})
        if not count or records[-1]["flags"] & 1:
            return records
        new_start = records[-1]["logical"] + records[-1]["length"]
        if new_start <= start:
            raise ValueError("FIEMAP made no progress")
        start = new_start
    raise ValueError("FIEMAP extent limit exceeded")


def frame_kind(frame, offset, version, header_digest):
    if len(frame) != FRAME_BYTES:
        return "unknown"
    if offset == 0:
        return "HEADER"
    magic = b"TKVW7FR1" if version == 7 else b"TKVW8FR1"
    kind = int.from_bytes(frame[10:12], "big")
    body = int.from_bytes(frame[56:60], "big")
    if (frame[:8] != magic or frame[8:10] != b"\0\1" or kind not in (1, 2)
            or frame[12:16] != b"\0\x40\0\0" or frame[16:48] != header_digest
            or int.from_bytes(frame[48:56], "big") != offset
            or not (24 < body <= 4032 if kind == 1 else body == 104)):
        return "unknown"
    crc = zlib.crc32(frame[64:], zlib.crc32(frame[:60]))
    if crc != int.from_bytes(frame[60:64], "big") or any(frame[64 + body:]):
        return "unknown"
    return "DATA" if kind == 1 else "FRONTIER"


def inventory(fd, size, version, header_digest):
    runs = []
    for chunk_start in range(0, size, 1 << 20):
        wanted = min(1 << 20, size - chunk_start)
        chunk = os.pread(fd, wanted, chunk_start)
        if len(chunk) != wanted:
            raise ValueError("WAL changed during frame inventory")
        for index in range(0, len(chunk), FRAME_BYTES):
            offset = chunk_start + index
            kind = frame_kind(chunk[index:index + FRAME_BYTES], offset, version, header_digest)
            end = min(offset + FRAME_BYTES, size)
            if runs and runs[-1]["kind"] == kind:
                runs[-1]["end"] = end
            else:
                runs.append({"start": offset, "end": end, "kind": kind})
    return runs


def snapshot(database, with_inventory=False):
    start = monotonic_ns()
    files = []
    for path in sorted(database.glob("*.wal")):
        with path.open("rb") as stream:
            fd = stream.fileno()
            before = os.fstat(fd)
            header = os.pread(fd, FRAME_BYTES, 0)
            version = int.from_bytes(header[4:6], "big")
            if len(header) != FRAME_BYTES or header[:4] != b"WAL2" or version not in (7, 8):
                continue
            if zlib.crc32(header[:144]) != int.from_bytes(header[144:148], "big"):
                raise ValueError("WAL header CRC failed")
            digest = hashlib.sha256(header).digest()
            extents = fiemap(fd)
            frames = inventory(fd, before.st_size, version, digest) if with_inventory else []
            after = os.fstat(fd)
            stable = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            if not stable or fiemap(fd) != extents:
                raise ValueError("WAL changed during extent snapshot")
            files.append({"path": str(path), "inode": before.st_ino,
                          "dev_major": os.major(before.st_dev), "dev_minor": os.minor(before.st_dev),
                          "size": before.st_size, "fs_block_bytes": os.fstatvfs(fd).f_frsize,
                          "version": version, "header_sha256": digest.hex(), "extents": extents,
                          "frames": frames, "snapshot_stable": stable})
    return {"schema": "kv-engine.wal-extent-snapshot.v1", "clock": "CLOCK_MONOTONIC",
            "sample_begin_ns": start, "sample_end_ns": monotonic_ns(), "fiemap_flags": 0,
            "inventory_after_timed_writes": with_inventory, "files": files}


def storage_device(path):
    """Support a direct filesystem partition, rejecting stacked block devices."""
    device = os.stat(path).st_dev
    node = Path(f"/sys/dev/block/{os.major(device)}:{os.minor(device)}").resolve(strict=True)
    partition = (node / "partition").exists()
    disk = node.parent if partition else node
    if not disk.name.startswith("nvme") or not (disk / "nsid").exists():
        raise ValueError("Extent tracing requires a directly mounted NVMe namespace")
    logical = int((disk / "queue/logical_block_size").read_text())
    if logical != 512:
        raise ValueError("This protocol requires 512-byte namespace LBAs")
    major, minor = (int(n) for n in (disk / "dev").read_text().split(":"))
    return {"partition_start_bytes": int((node / "start").read_text()) * 512 if partition else 0,
            "fs_major": os.major(device), "fs_minor": os.minor(device),
            "disk_major": major, "disk_minor": minor, "disk": disk.name,
            "nsid": int((disk / "nsid").read_text()), "logical_block_bytes": logical}


def file_key(record):
    return record["dev_major"], record["dev_minor"], record["inode"]


def mapped_blocks(extents):
    for extent in extents:
        if extent["flags"] & ~FIEMAP_SAFE_FLAGS or any(
            extent[field] % FRAME_BYTES for field in ("logical", "physical", "length")
        ):
            continue
        for delta in range(0, extent["length"], FRAME_BYTES):
            yield extent["logical"] + delta, extent["physical"] + delta


def extent_index(timeline, events, device):
    snapshots = timeline["wal_extents"]
    before, after = snapshots["before"], snapshots["after"]
    start = timeline["counter_points"][0]["sample_begin_ns"]
    end = timeline["counter_points"][-1]["sample_end_ns"]
    if not (before["sample_end_ns"] < start < end < after["sample_begin_ns"]):
        raise ValueError("Extent snapshots must bracket all application counters")
    if any(s["fiemap_flags"] or s["clock"] != "CLOCK_MONOTONIC" for s in (before, after)):
        raise ValueError("Unsupported FIEMAP flags or snapshot clock")
    initial = {file_key(f): f for f in before["files"]}
    files, physical, logical, problems = {}, {}, {}, Counter()
    for record in after["files"]:
        key = file_key(record)
        if (key[:2] != (device["fs_major"], device["fs_minor"])
                or record["fs_block_bytes"] != FRAME_BYTES or not record["snapshot_stable"]):
            raise ValueError("Unsupported file device/block size or unstable snapshot")
        previous = initial.get(key)
        if previous and (previous["header_sha256"] != record["header_sha256"] or previous["version"] != record["version"]):
            raise ValueError("WAL identity changed during capture")
        files[key] = record
        old = dict(mapped_blocks(previous["extents"])) if previous else {}
        runs = record["frames"]
        starts = [r["start"] for r in runs]
        for offset, location in mapped_blocks(record["extents"]):
            if offset + FRAME_BYTES > record["size"]:
                continue
            position = bisect_right(starts, offset) - 1
            kind = runs[position]["kind"] if position >= 0 and offset + FRAME_BYTES <= runs[position]["end"] else "unknown"
            absolute = location + device["partition_start_bytes"]
            block = {"file": list(key), "logical": offset, "physical": absolute, "kind": kind,
                     "observed_ns": before["sample_end_ns"] if old.get(offset) == location else None,
                     "conflict": False}
            if absolute in physical:
                physical[absolute]["conflict"] = block["conflict"] = True
                problems["aliased_physical_block"] += 1
            physical[absolute] = block
            logical[key, offset] = block
    for event in events:
        if not event["event"].startswith("ext4/") or not before["sample_begin_ns"] <= event["time_ns"] <= end:
            continue
        key = (event["dev"] >> 20, event["dev"] & ((1 << 20) - 1), event["ino"])
        if key not in files:
            continue
        if event["event"] == "ext4/ext4_free_blocks":
            if event["time_ns"] >= start:
                if event["flags"] & 1:
                    # Extent-tree metadata can be freed without moving file data.
                    problems["wal_metadata_blocks_freed"] += 1
                else:
                    problems["wal_blocks_freed_during_window"] += 1
                    for (owner, _), block in logical.items():
                        if owner == key:
                            block["conflict"] = True
            continue
        if event["ret"] <= 0 or not event["pblk"]:
            continue
        for delta in range(min(event["ret"], event["len"])):
            offset = (event["lblk"] + delta) * FRAME_BYTES
            block = logical.get((key, offset))
            if block is None:
                continue
            absolute = (event["pblk"] + delta) * FRAME_BYTES + device["partition_start_bytes"]
            if absolute != block["physical"]:
                block["conflict"] = True
                problems["mapping_changed"] += 1
            elif block["observed_ns"] is None or event["time_ns"] < block["observed_ns"]:
                block["observed_ns"] = event["time_ns"]
    return physical, dict(problems)


def attribute_commands(commands, events, timeline, device):
    physical, problems = extent_index(timeline, events, device)
    setups = {((e["ctrl_id"], e["qid"], e["cid"]), e["time_ns"]): e
              for e in events if e["event"] == "nvme/nvme_setup_cmd" and e["disk"] == device["disk"]}
    end = timeline["counter_points"][-1]["sample_end_ns"]
    last, overlapping_blocks = {}, set()
    for index, command in enumerate(commands):
        setup = setups[tuple(command["key"]), command["start_ns"]]
        command["physical"] = setup.get("lba", 0) * device["logical_block_bytes"]
        if command["opcode"] != 1 or command["start_ns"] > end:
            continue
        start, size = command["physical"], command["bytes"]
        if start % FRAME_BYTES or size % FRAME_BYTES:
            continue
        for address in range(start, start + size, FRAME_BYTES):
            if address not in physical:
                continue
            if address in last and commands[last[address]]["end_ns"] > command["start_ns"]:
                overlapping_blocks.add(address)
            last[address] = index
    roles = Counter()
    for index, command in enumerate(commands):
        role, owner, logical_offset = "NON_WAL", None, None
        if command["opcode"] == 1:
            start, size = command["physical"], command["bytes"]
            addresses = list(range(start, start + size, FRAME_BYTES))
            blocks = [physical.get(a) for a in addresses]
            found = [b for b in blocks if b is not None]
            if found:
                role = "UNRESOLVED"
                if start % FRAME_BYTES or size % FRAME_BYTES or len(found) != len(blocks):
                    role = "PARTIAL_EXTENT"
                elif len({tuple(b["file"]) for b in found}) != 1:
                    role = "MIXED_FILES"
                elif any(b["conflict"] or a in overlapping_blocks for a, b in zip(addresses, blocks)):
                    role = "MAPPING_CONFLICT"
                elif any(b["observed_ns"] is None or b["observed_ns"] > command["start_ns"] for b in blocks):
                    role = "UNOBSERVED_ALLOCATION"
                elif command["pid"] != timeline["process_id"]:
                    role = "OTHER_ISSUER_IN_WAL_EXTENT"
                elif [b["logical"] for b in blocks] != list(range(found[0]["logical"], found[0]["logical"] + size, FRAME_BYTES)):
                    role = "NONCONTIGUOUS_LOGICAL"
                elif any(last.get(a) != index for a in addresses):
                    role = "OVERWRITTEN_OR_INITIALIZATION"
                else:
                    kinds = {b["kind"] for b in blocks}
                    role = next(iter(kinds)) if len(kinds) == 1 else "MIXED_ROLES"
                    if role == "unknown":
                        role = "UNTYPED"
                    owner, logical_offset = found[0]["file"], found[0]["logical"]
                roles[role] += 1
        command.update({"wal_role": role, "wal_file": owner, "logical_start": logical_offset})
    block_dev = device["disk_major"] << 20 | device["disk_minor"]
    begin = timeline["counter_points"][0]["sample_begin_ns"]
    block_issues = sum(e["event"] == "block/block_rq_issue" and e["dev"] == block_dev
                       and begin <= e["time_ns"] <= end and "W" in e["rwbs"]
                       and e["bytes"] > 0 and e["sector"] * 512 in physical for e in events)
    return {"roles": dict(roles), "mapping_problems": problems, "physical_blocks": len(physical),
            "block_issues_starting_in_wal_extent": block_issues,
            "allocation_observation": "Initial FIEMAP or matching ext4 map event preceding command setup; post-only mappings are not backdated."}


def size_bucket(size):
    for limit in (4096, 16384, 65536, 131072):
        if size <= limit:
            return f"le_{limit}"
    return "gt_131072"


def depth_bucket(depth):
    return "0" if depth == 0 else "1" if depth == 1 else "2-7" if depth < 8 else "8+"


def wal_data_samples(commands, device):
    selected = [c for c in commands if c["nsid"] in (device["nsid"], 0xFFFFFFFF)]
    events = sorted([(c["start_ns"], 1, i) for i, c in enumerate(selected)] +
                    [(c["end_ns"], 0, i) for i, c in enumerate(selected)])
    active, pending, samples = set(), None, []
    for timestamp, kind, index in events:
        command = selected[index]
        if kind == 0:
            active.discard(index)
            if command["opcode"] == 0 and not command["status"] and not command["retries"]:
                pending = {"flush_start_ns": command["start_ns"], "flush_end_ns": timestamp,
                           "intervening_fua": 0, "intervening_other_writes": 0,
                           "intervening_reads": 0,
                           "inflight_at_flush": len(active)}
            continue
        command["peak_inflight"] = len(active) + 1
        for other in active:
            selected[other]["peak_inflight"] = max(selected[other].get("peak_inflight", 1), len(active) + 1)
        if command["opcode"] == 2 and pending is not None:
            pending["intervening_reads"] += 1
        if command["opcode"] == 1 and pending is not None:
            role = command["wal_role"]
            if role not in ("NON_WAL", "HEADER", "FRONTIER", "OVERWRITTEN_OR_INITIALIZATION"):
                samples.append({**pending, "command_index": index, "inflight_at_write": len(active)})
                pending = None
            elif command["control"] & 0x4000:
                pending["intervening_fua"] += 1
            else:
                pending["intervening_other_writes"] += 1
        active.add(index)
    for sample in samples:
        command = selected[sample.pop("command_index")]
        gap_us = (command["start_ns"] - sample["flush_end_ns"]) / 1000
        intervening = "mixed" if sample["intervening_fua"] and sample["intervening_other_writes"] else (
            "fua" if sample["intervening_fua"] else "ordinary" if sample["intervening_other_writes"] else "none")
        shape = {"size": size_bucket(command["bytes"]), "queue": depth_bucket(sample["inflight_at_write"]),
                 "peak": depth_bucket(command["peak_inflight"] - 1), "intervening_writes": intervening,
                 "flush_queue": depth_bucket(sample["inflight_at_flush"]), "intervening_reads": bool(sample["intervening_reads"]),
                 "gap": "le_100us" if gap_us <= 100 else "le_500us" if gap_us <= 500 else "gt_500us"}
        reasons = []
        if command["wal_role"] != "DATA":
            reasons.append(command["wal_role"])
        if command["status"] or command["retries"] or command["control"]:
            reasons.append("error_retry_or_write_control")
        sample.update({"write_start_ns": command["start_ns"], "time_ns": command["end_ns"],
                       "post_flush_write_us": (command["end_ns"] - command["start_ns"]) / 1000,
                       "write_bytes": command["bytes"], "wal_file": command["wal_file"],
                       "logical_start": command["logical_start"], "wal_role": command["wal_role"],
                       "flush_to_write_gap_us": gap_us, "peak_inflight": command["peak_inflight"],
                       "shape": shape, "stratum": json.dumps(shape, sort_keys=True, separators=(",", ":")),
                       "eligible": not reasons, "reasons": reasons})
    return sorted(samples, key=lambda s: s["time_ns"])


def calibration_rows(samples):
    grouped = defaultdict(list)
    for sample in samples:
        if sample["eligible"]:
            grouped[sample["stratum"]].append(sample["post_flush_write_us"])
    output = []
    for stratum, values in sorted(grouped.items()):
        values.sort()
        output.append({"stratum": stratum, "samples": len(values), "p10_us": values[int((len(values) - 1) * .10)],
                       "p50_us": statistics.median(values), "p90_us": values[int((len(values) - 1) * .90)],
                       "p99_us": values[int((len(values) - 1) * .99)] if len(values) >= EXTENT_POLICY["min_percentile_samples"] else None,
                       "calibrated": False, "reason": "Descriptive distribution only; no independent state labels or shared-format calibration."})
    return output


def analyze_run(perf_path, formats, timeline, benchmark, device, policy):
    events, metadata = trace.decode_perf(perf_path, formats)
    commands, anomalies = trace.match_commands(events, device["disk"])
    required = {"nvme/nvme_setup_cmd", "nvme/nvme_complete_rq", "block/block_rq_issue", "block/block_rq_complete",
                "ext4/ext4_ext_map_blocks_exit"}
    missing = sorted(required - metadata["event_counts"].keys())
    quality = metadata["lost"] == 0 and not any(anomalies.values()) and not missing
    attribution = attribute_commands(commands, events, timeline, device)
    begin, end = timeline["counter_points"][0]["sample_begin_ns"], timeline["counter_points"][-1]["sample_end_ns"]
    samples = [sample for sample in wal_data_samples(commands, device)
               if begin <= sample["flush_start_ns"] <= sample["write_start_ns"] < sample["time_ns"] <= end]
    if not quality:
        for sample in samples:
            sample["eligible"] = False
            sample["reasons"].append("trace_quality")
    # This cohort collects calibration distributions; thresholds must be frozen
    # and validated on fresh data before these intervals can receive state labels.
    regimes = [{"start_ns": begin, "end_ns": end, "state": "unknown"}]
    intervals = trace.metric_intervals(timeline, regimes, policy)
    if sum(r["puts"] for r in timeline["requests"]) != benchmark["result"]["ops"]:
        raise ValueError("Request log operation loss")
    observed = [c for c in commands if c["opcode"] == 1 and begin <= c["start_ns"] < c["end_ns"] <= end]
    roles = dict(Counter(c["wal_role"] for c in observed))
    data_bytes = sum(c["bytes"] for c in observed if c["wal_role"] == "DATA")
    inventory_bytes = sum(r["end"] - r["start"] for f in timeline["wal_extents"]["after"]["files"]
                          for r in f["frames"] if r["kind"] == "DATA")
    summary = {"trace_quality_ok": quality, "metadata": metadata, "matching_anomalies": anomalies,
               "missing_events": missing, "attribution": attribution, "window_write_roles": roles,
               "attributed_DATA_bytes": data_bytes, "inventory_DATA_bytes": inventory_bytes,
               "post_flush_wal_observations": len(samples), "eligible_wal_DATA_samples": sum(s["eligible"] for s in samples),
               "ineligible_reasons": dict(Counter(reason for s in samples for reason in s["reasons"])),
               "intervals": len(intervals), "eligible_intervals": 0, "calibration_pending": True,
               "RFC_performance_qualified": False}
    columns = ("key", "start_ns", "end_ns", "bytes", "physical", "pid", "wal_role", "wal_file", "logical_start", "control")
    writes = [{key: c[key] for key in columns} for c in observed if c["wal_role"] != "NON_WAL"]
    return {"summary": summary, "samples": samples, "wal_writes": writes,
            "calibration": calibration_rows(samples), "regimes": regimes, "intervals": intervals}


def analyze_capture(directory):
    plan = json.loads((directory / "plan.json").read_text())
    expected_policy = dict(EXTENT_POLICY)
    expected_policy["counter_interval_ms"] = plan["policy"]["counter_interval_ms"]
    if (plan["protocol"] != EXTENT_PROTOCOL or expected_policy != plan["policy"]
            or expected_policy["counter_interval_ms"] not in (100, 200)):
        raise ValueError("Unsupported extent protocol or policy")
    formats = json.loads((directory / "formats.json").read_text())
    if not set(EXTRA_EVENTS) <= formats.keys():
        raise ValueError("Missing ext4 ownership event definitions")
    results, all_intervals, all_calibration = [], [], []
    strata_by_format = defaultdict(set)
    for workload in plan["workloads"]:
        for position, fmt in enumerate(plan["order"]):
            name = f"{workload['name']}-{position}-{fmt}"
            record = json.loads((directory / (name + "-capture.json")).read_text())
            benchmark = json.loads((directory / (name + "-benchmark.json")).read_text())
            timeline = json.loads((directory / (name + "-timeline.json")).read_text())
            expected = {"num": workload["num"], "threads": workload["writers"], "batch_size": workload["batch"],
                        "wal_format_version": int(fmt[1:]), "wal_io_mode": "parallel", "value_size": 1024}
            if (record["returncode"] or any(benchmark["params"].get(k) != v for k, v in expected.items())
                    or timeline["interval_ms"] != plan["policy"]["counter_interval_ms"]):
                raise ValueError("Capture differs from the predeclared plan")
            result = analyze_run(directory / (name + ".perf"), formats, timeline, benchmark,
                                 plan["device"], plan["policy"])
            (directory / (name + "-extent-analysis.json")).write_text(json.dumps(result, indent=2) + "\n")
            identity = {"name": name, "format": fmt, "workload": workload["name"]}
            results.append({**identity, **result["summary"]})
            all_intervals.extend({**identity, **row} for row in result["intervals"])
            all_calibration.extend({**identity, **row} for row in result["calibration"])
            for row in result["calibration"]:
                strata_by_format[fmt].add(row["stratum"])
            print(json.dumps({"name": name, "trace_quality_ok": result["summary"]["trace_quality_ok"],
                              "window_write_roles": result["summary"]["window_write_roles"],
                              "DATA_samples": result["summary"]["eligible_wal_DATA_samples"]}), flush=True)
    summary = {"protocol": EXTENT_PROTOCOL, "runs": results, "common_strata": sorted(
        strata_by_format["v7"] & strata_by_format["v8"]), "RFC_performance_qualified": False,
        "calibration_pending": True, "scope": "File-attributed per-shape latency distributions; no inherited namespace thresholds and no same-cohort performance ratio."}
    (directory / "extent-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    trace.write_csv(directory / "interval-metrics.csv", all_intervals, list(all_intervals[0]))
    trace.write_csv(directory / "wal-calibration.csv", all_calibration,
                    ["name", "format", "workload", "stratum", "samples", "p10_us", "p50_us", "p90_us", "p99_us", "calibrated", "reason"])
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--inventory", action="store_true")
    parser.add_argument("--capture-dir", type=Path)
    args = parser.parse_args()
    if args.capture_dir:
        if args.snapshot or args.inventory:
            parser.error("Choose snapshot mode or offline capture analysis")
        analyze_capture(args.capture_dir)
        return
    if not args.snapshot:
        parser.error("--snapshot DATABASE is required")
    print(json.dumps(snapshot(args.snapshot, args.inventory), separators=(",", ":")))


if __name__ == "__main__":
    main()
