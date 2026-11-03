"""Offline NVMe command matching, conservative regimes, and metric joins."""

import argparse
from collections import Counter, deque
import csv
import json
from pathlib import Path
import re
import statistics
import struct


POLICY = {
    "window_samples": 16, "confirm_windows": 5,
    "fast_below_us": 100, "slow_above_us": 400,
    "max_sample_gap_ns": 250_000_000, "guard_ns": 20_000_000,
    "signal_write_bytes": 4096, "logical_block_bytes": 512,
    "min_interval_ns": 250_000_000, "min_percentile_samples": 100,
}


def layout(text):
    fields = {}
    for declaration, offset, size, signed in re.findall(
        r"field:([^;]+);\s*offset:(\d+);\s*size:(\d+);\s*signed:(\d+);", text
    ):
        name = declaration.split()[-1].split("[")[0]
        fields[name] = (int(offset), int(size), bool(int(signed)), "char" in declaration or "[" in declaration)
    return int(re.search(r"^ID: (\d+)$", text, re.M)[1]), fields


def field(raw, fields, name):
    offset, size, signed, array = fields[name]
    if offset + size > len(raw):
        raise ValueError("Truncated tracepoint payload")
    value = raw[offset:offset + size]
    return value if array else int.from_bytes(value, "little", signed=signed)


def decode_perf(path, formats):
    data = Path(path).read_bytes()
    if data[:8] != b"PERFILE2" or len(data) < 104:
        raise ValueError("Expected native little-endian perf.data")
    header = struct.unpack_from("<13Q", data)
    stride, attrs_offset, attrs_size, data_offset, data_size = header[2:7]
    if stride < 48 or attrs_size % stride or data_offset + data_size > len(data):
        raise ValueError("Invalid perf sections")
    descriptions = {layout(text)[0]: (name, layout(text)[1]) for name, text in formats.items()}
    attributes = {}
    for at in range(attrs_offset, attrs_offset + attrs_size, stride):
        kind, _, config, _, sample_type = struct.unpack_from("<IIQQQ", data, at)
        flags = struct.unpack_from("<Q", data, at + 40)[0]
        if stride < 112 or not flags & (1 << 25) or struct.unpack_from("<i", data, at + 92)[0] != 1:
            raise ValueError("Trace must explicitly use CLOCK_MONOTONIC")
        ids_offset, ids_size = struct.unpack_from("<QQ", data, at + stride - 16)
        for position in range(ids_offset, ids_offset + ids_size, 8):
            attributes[struct.unpack_from("<Q", data, position)[0]] = (kind, config, sample_type)
    events, record_counts, lost = [], Counter(), 0
    at = data_offset
    while at < data_offset + data_size:
        kind, _, size = struct.unpack_from("<IHH", data, at)
        if size < 8 or at + size > data_offset + data_size:
            raise ValueError("Invalid perf record")
        record_counts[kind] += 1
        if kind == 2:
            lost += struct.unpack_from("<Q", data, at + 16)[0]
        elif kind == 13:
            lost += struct.unpack_from("<Q", data, at + 8)[0]
        elif kind == 9:
            pos = at + 8
            identity = struct.unpack_from("<Q", data, pos)[0]
            attr_kind, config, sample_type = attributes[identity]
            allowed = (1 << 16) | 1 | 2 | 4 | 8 | 64 | 128 | 256 | 512 | 1024
            if not sample_type & (1 << 16) or sample_type & ~allowed:
                raise ValueError("Unsupported perf sample layout")
            pos += 8
            if sample_type & 1:
                pos += 8
            pid = tid = timestamp = cpu = None
            if sample_type & 2:
                pid, tid = struct.unpack_from("<II", data, pos)
                pos += 8
            if sample_type & 4:
                timestamp = struct.unpack_from("<Q", data, pos)[0]
                pos += 8
            for bit in (8, 64, 512):
                if sample_type & bit:
                    pos += 8
            if sample_type & 128:
                cpu = struct.unpack_from("<I", data, pos)[0]
                pos += 8
            if sample_type & 256:
                pos += 8
            if sample_type & 1024:
                length = struct.unpack_from("<I", data, pos)[0]
                raw = data[pos + 4:pos + 4 + length]
                if pos + 4 + length > at + size:
                    raise ValueError("Truncated raw sample")
                if attr_kind == 2 and config in descriptions:
                    name, fields = descriptions[config]
                    if field(raw, fields, "common_type") != config or timestamp is None:
                        raise ValueError("Tracepoint identity or clock missing")
                    event = {"event": name, "time_ns": timestamp, "pid": pid, "tid": tid, "cpu": cpu}
                    for key in fields:
                        if key.startswith("common_") or key == "cmd":
                            continue
                        value = field(raw, fields, key)
                        event[key] = value.hex() if key == "cdw10" else (
                            value.split(b"\0", 1)[0].decode(errors="replace") if isinstance(value, bytes) else value)
                    if name == "nvme/nvme_setup_cmd" and event["qid"] > 0 and event["opcode"] in (1, 2):
                        command = bytes.fromhex(event["cdw10"])
                        event.update({"lba": int.from_bytes(command[:8], "little"),
                                      "blocks": int.from_bytes(command[8:10], "little") + 1,
                                      "control": int.from_bytes(command[10:12], "little")})
                    events.append(event)
        at += size
    return sorted(events, key=lambda e: e["time_ns"]), {"lost": lost, "record_counts": dict(record_counts),
                                                        "event_counts": dict(Counter(e["event"] for e in events)),
                                                        "clock": "CLOCK_MONOTONIC"}


def match_commands(events, disk="nvme0n1"):
    active, commands, anomalies = {}, [], Counter()
    seen_times = set()
    for event in events:
        if not event["event"].startswith("nvme/") or event["disk"] != disk or event["qid"] <= 0:
            continue
        key = (event["ctrl_id"], event["qid"], event["cid"])
        identity = (*key, event["time_ns"])
        if identity in seen_times:
            anomalies["same_key_same_timestamp"] += 1
        seen_times.add(identity)
        if event["event"] == "nvme/nvme_setup_cmd":
            if key in active:
                anomalies["duplicate_live_key"] += 1
            active[key] = event
        else:
            setup = active.pop(key, None)
            if setup is None:
                anomalies["completion_without_setup"] += 1
                continue
            if event["time_ns"] <= setup["time_ns"]:
                anomalies["nonpositive_duration"] += 1
                continue
            if event["status"] or event["retries"]:
                anomalies["error_or_retry"] += 1
            commands.append({"key": list(key), "start_ns": setup["time_ns"], "end_ns": event["time_ns"],
                             "opcode": setup["opcode"], "nsid": setup["nsid"], "pid": setup["pid"],
                             "tid": setup["tid"], "status": event["status"], "retries": event["retries"],
                             "bytes": setup.get("blocks", 0) * POLICY["logical_block_bytes"],
                             "control": setup.get("control", 0), "overlap": False})
    anomalies["setup_without_completion"] = len(active)
    return sorted(commands, key=lambda c: c["start_ns"]), dict(anomalies)


def post_flush_samples(commands, process_id, policy=POLICY, nsid=1):
    selected = [c.copy() for c in commands if c["nsid"] in (nsid, 0xFFFFFFFF)]
    events = sorted([(c["start_ns"], 1, i) for i, c in enumerate(selected)] +
                    [(c["end_ns"], 0, i) for i, c in enumerate(selected)])
    active, candidates, pending = set(), [], None
    for timestamp, kind, index in events:
        command = selected[index]
        if kind == 0:
            active.discard(index)
            if command["opcode"] == 0 and not command["status"] and not command["retries"]:
                pending = {"flush_start_ns": command["start_ns"], "flush_end_ns": timestamp,
                           "flush_latency_us": (timestamp - command["start_ns"]) / 1000,
                           "inflight_at_flush": len(active)}
        else:
            if active:
                command["overlap"] = True
                for other in active:
                    selected[other]["overlap"] = True
            if command["opcode"] == 1 and pending is not None:
                candidates.append({**pending, "command_index": index, "inflight_at_write": len(active)})
                pending = None  # one first write per completed flush, never a later substitute
            active.add(index)
    samples = []
    for candidate in candidates:
        command = selected[candidate.pop("command_index")]
        reasons = []
        for bad, reason in [
            (command["bytes"] != policy["signal_write_bytes"], "uncalibrated_write_size"),
            (command["pid"] != process_id, "other_process"),
            (bool(command["control"]), "nonordinary_write_control"),
            (bool(command["status"] or command["retries"]), "error_or_retry"),
            (bool(candidate["inflight_at_flush"] or candidate["inflight_at_write"] or command["overlap"]), "overlapping_namespace_io"),
        ]:
            if bad:
                reasons.append(reason)
        samples.append({**candidate, "write_start_ns": command["start_ns"], "time_ns": command["end_ns"],
                        "post_flush_write_us": (command["end_ns"] - command["start_ns"]) / 1000,
                        "write_bytes": command["bytes"], "issuer_pid": command["pid"], "issuer_tid": command["tid"],
                        "eligible": not reasons, "reasons": reasons})
    return sorted(samples, key=lambda s: s["time_ns"])


def detect_regimes(samples, start_ns, end_ns, policy=POLICY, quality_ok=True):
    """Only confirm forward; remove uncertain history around a changed median."""
    stable, current, window = [], None, deque(maxlen=policy["window_samples"])
    pending, count, previous = None, 0, None
    for sample in samples if quality_ok else []:
        if not start_ns <= sample["flush_start_ns"] <= sample["write_start_ns"] < sample["time_ns"] <= end_ns:
            continue
        gap = previous is not None and sample["time_ns"] - previous > policy["max_sample_gap_ns"]
        previous = sample["time_ns"]
        if gap or not sample["eligible"]:
            current, pending, count = None, None, 0
            window.clear()
        if not sample["eligible"]:
            continue
        window.append(sample)
        if len(window) < policy["window_samples"]:
            continue
        median = statistics.median(s["post_flush_write_us"] for s in window)
        state = "fast" if median < policy["fast_below_us"] else (
            "slow" if median > policy["slow_above_us"] else "unknown")
        if current is not None and state == current["state"]:
            current["end_ns"] = sample["time_ns"]
            continue
        if current is not None:
            current["end_ns"] = min(current["end_ns"], window[0]["write_start_ns"])
            current = None
        if state == "unknown":
            pending, count = None, 0
            continue
        count = count + 1 if pending == state else 1
        pending = state
        if count >= policy["confirm_windows"]:
            current = {"start_ns": sample["time_ns"], "end_ns": sample["time_ns"], "state": state}
            stable.append(current)
            pending, count = None, 0
    output, cursor = [], start_ns
    for segment in stable:
        begin, end = segment["start_ns"] + policy["guard_ns"], segment["end_ns"] - policy["guard_ns"]
        if begin >= end:
            continue
        if begin < cursor:
            raise ValueError("Overlapping detected regimes")
        if cursor < begin:
            output.append({"start_ns": cursor, "end_ns": begin, "state": "unknown"})
        valid = [s["post_flush_write_us"] for s in samples if s["eligible"]
                 and begin <= s["write_start_ns"] < s["time_ns"] <= end]
        output.append({"start_ns": begin, "end_ns": end, "state": segment["state"],
                       "median_post_flush_us": statistics.median(valid) if valid else None, "samples": len(valid)})
        cursor = end
    if cursor < end_ns:
        output.append({"start_ns": cursor, "end_ns": end_ns, "state": "unknown"})
    return output


def containing_regime(regimes, start, end):
    found = [r for r in regimes if r["state"] != "unknown" and r["start_ns"] <= start < end <= r["end_ns"]]
    return found[0] if len(found) == 1 else None


def metric_intervals(timeline, regimes, policy=POLICY):
    if timeline["clock"] != "CLOCK_MONOTONIC" or timeline["schema"] != "kv-engine.wal-benchmark-timeline.v1":
        raise ValueError("Timeline clock/schema mismatch")
    points, requests = timeline["counter_points"], timeline["requests"]
    if len(points) < 2 or not all(
        p["sample_begin_ns"] <= p["sample_end_ns"] < q["sample_begin_ns"]
        for p, q in zip(points, points[1:])
    ):
        raise ValueError("Unordered counter points")
    if any(not points[0]["sample_end_ns"] <= r["start_ns"] < r["end_ns"] < points[-1]["sample_end_ns"]
           or r["puts"] <= 0 for r in requests):
        raise ValueError("Request outside capture or invalid duration")
    output = []
    for first, last in zip(points, points[1:]):
        start, end = first["sample_end_ns"], last["sample_end_ns"]
        spans = [r for r in requests if start <= r["end_ns"] < end]
        latencies = sorted((r["end_ns"] - r["start_ns"]) / 1e6 for r in spans)
        duration = (end - start) / 1e9
        region = containing_regime(regimes, first["sample_begin_ns"], end)
        reason = ""
        if region is None:
            reason = "unknown_or_crossing_regime"
        elif any(r["start_ns"] < region["start_ns"] or r["end_ns"] > region["end_ns"] for r in spans):
            reason = "request_crosses_regime"  # reject the whole interval, not just slow requests
        elif end - start < policy["min_interval_ns"]:
            reason = "short_interval"
        metrics = {key: last[key] - first[key] for key in (
            "user_cpu_ms", "system_cpu_ms", "sync_count", "reported_commit_groups",
            "worker_notification_requests", "worker_eventfd_writes")}
        if any(value < 0 for value in metrics.values()):
            raise ValueError("Nonmonotonic CPU or WAL counters")
        puts = sum(r["puts"] for r in spans)
        output.append({"start_ns": start, "end_ns": end, "duration_s": duration,
                       "state": region["state"] if not reason else "unknown", "rejection_reason": reason,
                       "puts": puts, "puts_s": puts / duration, "latency_samples": len(latencies),
                       "p50_ms": statistics.median(latencies) if latencies else None,
                       "p99_ms": latencies[int((len(latencies) - 1) * .99)] if len(latencies) >= policy["min_percentile_samples"] else None,
                       "process_cpu_s": (metrics.pop("user_cpu_ms") + metrics.pop("system_cpu_ms")) / 1000,
                       **metrics})
    if sum(row["puts"] for row in output) != sum(r["puts"] for r in requests):
        raise ValueError("Interval operation conservation failed")
    return output


def analyze(perf_path, formats, timeline, benchmark, policy=POLICY):
    events, metadata = decode_perf(perf_path, formats)
    commands, anomalies = match_commands(events)
    required = {"nvme/nvme_setup_cmd", "nvme/nvme_complete_rq", "block/block_rq_issue", "block/block_rq_complete"}
    missing = sorted(required - metadata["event_counts"].keys())
    quality = metadata["lost"] == 0 and not any(anomalies.values()) and not missing
    points = timeline["counter_points"]
    start, end = points[0]["sample_begin_ns"], points[-1]["sample_end_ns"]
    samples = post_flush_samples(commands, timeline["process_id"], policy)
    regimes = detect_regimes(samples, start, end, policy, quality)
    intervals = metric_intervals(timeline, regimes, policy)
    if sum(r["puts"] for r in timeline["requests"]) != benchmark["result"]["ops"]:
        raise ValueError("Request log does not cover the benchmark's operations")
    summary = {"trace_quality_ok": quality, "metadata": metadata, "matching_anomalies": anomalies,
               "missing_events": missing, "matched_commands": len(commands), "post_flush_samples": len(samples),
               "eligible_post_flush_samples": sum(s["eligible"] for s in samples),
               "ineligible_reasons": dict(Counter(reason for s in samples for reason in s["reasons"])),
               "intervals": len(intervals), "eligible_intervals": sum(r["state"] != "unknown" for r in intervals),
               "policy": policy, "RFC_performance_qualified": False,
               "scope": "Observed isolated 4KiB post-flush namespace-write latency, not independent proof of firmware state. Block events retained as context; partial completions are not naively paired."}
    return {"summary": summary, "samples": samples, "regimes": regimes, "intervals": intervals}


def write_csv(path, rows, columns):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def analyze_capture(directory):
    """Keep every planned run, including captures with no classifiable interval."""
    plan = json.loads((directory / "plan.json").read_text())
    if plan["protocol"] != "kv-engine.passive-wal-trace.v1" or plan["policy"] != POLICY:
        raise ValueError("Unsupported capture protocol/policy; do not reinterpret old cohorts")
    formats = json.loads((directory / "formats.json").read_text())
    summaries, intervals, regimes = [], [], []
    for workload in plan["workloads"]:
        for position, fmt in enumerate(plan["order"]):
            name = f"{workload['name']}-{position}-{fmt}"
            capture = json.loads((directory / (name + "-capture.json")).read_text())
            if capture["returncode"] or capture["format"] != fmt or capture["workload"] != workload:
                raise ValueError("Failed or mismatched capture")
            benchmark = json.loads((directory / (name + "-benchmark.json")).read_text())
            expected = {"num": workload["num"], "batch_size": workload["batch"],
                        "threads": workload["writers"], "wal_format_version": int(fmt[1:]),
                        "value_size": 1024, "wal_io_mode": "parallel"}
            if any(benchmark["params"].get(key) != value for key, value in expected.items()):
                raise ValueError("Benchmark parameters differ from the predeclared plan")
            result = analyze(directory / (name + ".perf"), formats,
                             json.loads((directory / (name + "-timeline.json")).read_text()), benchmark)
            (directory / (name + "-analysis.json")).write_text(json.dumps(result, indent=2) + "\n")
            identity = {"name": name, "format": fmt, "workload": workload["name"]}
            summaries.append({**identity, **result["summary"]})
            intervals.extend({**identity, **row} for row in result["intervals"])
            regimes.extend({**identity, **row} for row in result["regimes"])
    summary = {"runs": summaries, "RFC_performance_qualified": False,
               "scope": "No cross-format ratio: this command exports time intervals, not a winner selection."}
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_csv(directory / "storage-regimes.csv", sorted(regimes, key=lambda row: row["start_ns"]),
              ["name", "format", "workload", "start_ns", "end_ns", "state", "median_post_flush_us", "samples"])
    write_csv(directory / "interval-metrics.csv", sorted(intervals, key=lambda row: row["start_ns"]), list(intervals[0]))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-dir", type=Path)
    parser.add_argument("--perf-data", type=Path)
    parser.add_argument("--formats-json", type=Path)
    parser.add_argument("--timeline-json", type=Path)
    parser.add_argument("--benchmark-json", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.capture_dir:
        if any((args.perf_data, args.formats_json, args.timeline_json, args.benchmark_json, args.output_dir)):
            parser.error("Choose a capture directory or explicit files")
        summary = analyze_capture(args.capture_dir)
        print(json.dumps({"captures": len(summary["runs"]), "trace_quality_ok": all(
            r["trace_quality_ok"] for r in summary["runs"]), "eligible_intervals": sum(
            r["eligible_intervals"] for r in summary["runs"]), "RFC_performance_qualified": False}, indent=2))
        return
    if not all((args.perf_data, args.formats_json, args.timeline_json, args.benchmark_json, args.output_dir)):
        parser.error("Explicit mode requires perf data, formats, timeline, benchmark and output directory")
    result = analyze(args.perf_data, json.loads(args.formats_json.read_text()),
                     json.loads(args.timeline_json.read_text()), json.loads(args.benchmark_json.read_text()))
    args.output_dir.mkdir(exist_ok=True)
    (args.output_dir / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    write_csv(args.output_dir / "storage-regimes.csv", result["regimes"],
              ["start_ns", "end_ns", "state", "median_post_flush_us", "samples"])
    write_csv(args.output_dir / "interval-metrics.csv", result["intervals"], list(result["intervals"][0]))
    print(json.dumps(result["summary"], indent=2))


if __name__ == "__main__":
    main()
