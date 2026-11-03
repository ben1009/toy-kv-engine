from collections import Counter
from pathlib import Path
import hashlib
import json
import re


p = Path(__file__).resolve().parent / "storage-trace"
probe = json.loads((p / "storage-probe.json").read_text())
pid = probe["pid"]
line_re = re.compile(r"^\s*(.*?)\s+(\d+)\s+\[(\d+)\]\s+(\d+)\.(\d+):\s+nvme:(\w+):\s+(.*)")
active, commands, device_commands = {}, [], []
unmatched = Counter()
raw_format_fallback_lines = 0
last_opcode = None
for line in (p / "perf.txt").read_text().splitlines():
    match = line_re.match(line)
    if not match:
        unmatched["unparsed_lines"] += 1
        continue
    comm, tid, cpu, seconds, fraction, event, payload = match.groups()
    fields = dict(re.findall(r"([a-z0-9_]+)=(\S*)", payload))
    raw_format_fallback_lines += "[FAILED TO PARSE]" in payload
    if fields.get("disk") != "nvme0n1":
        unmatched["other_disks"] += 1
        continue
    key = tuple(int(fields[name]) for name in ["ctrl_id", "qid", "cid"])
    timestamp = int(seconds) * 1_000_000_000 + int(fraction.ljust(9, "0"))
    if event == "nvme_setup_cmd":
        if key in active:
            unmatched["duplicate_live_keys"] += 1
        active[key] = {"setup_ns": timestamp, "pid": int(tid), "cpu": int(cpu),
                       "opcode": int(fields["opcode"]), "key": key,
                       "follows_device_flush": last_opcode == 0}
    elif event == "nvme_complete_rq":
        setup = active.pop(key, None)
        if setup is None:
            unmatched["completion_without_setup"] += 1
            continue
        assert timestamp >= setup["setup_ns"]
        record = {**setup, "completion_ns": timestamp,
                  "duration_ns": timestamp - setup["setup_ns"], "status": int(fields["status"])}
        device_commands.append(record)
        last_opcode = setup["opcode"]
        if setup["pid"] != pid:
            unmatched["other_pid_pairs"] += 1
            continue
        commands.append(record)


def stats(values):
    if not values:
        return None
    values = sorted(values)
    return {"count": len(values), "mean_us": sum(values) / len(values) / 1000,
            "p50_us": values[len(values) // 2] / 1000,
            "p99_us": values[int((len(values) - 1) * .99)] / 1000,
            "max_us": values[-1] / 1000,
            "over_1ms": sum(v > 1_000_000 for v in values),
            "over_5ms": sum(v > 5_000_000 for v in values)}


groups = {name: [x for x in commands if x["opcode"] == opcode]
          for name, opcode in [("write", 1), ("read", 2)]}
groups["device_flush"] = [x for x in device_commands if x["opcode"] == 0]
groups["write_following_device_flush"] = [x for x in groups["write"] if x["follows_device_flush"]]
groups["other_write"] = [x for x in groups["write"] if not x["follows_device_flush"]]
summary = {"probe_pid": pid, "scope": "NVMe setup-to-driver-completion latency. Writes/reads filtered by submitting probe PID and disk; flushes cover the whole disk because kernel worker threads submit them. Following-flush means the last observed device completion at write setup was a flush, not a proof of internal controller ordering. Includes driver/transport/interrupt latency, not direct firmware/NAND timing.",
           "stats": {name: stats([x["duration_ns"] for x in values]) for name, values in groups.items()},
           "matched_probe_commands": len(commands), "matched_device_commands": len(device_commands),
           "other_or_unmatched": dict(unmatched),
           "pending_at_end": len(active), "nonzero_status_commands": sum(x["status"] != 0 for x in device_commands),
           "raw_field_format_fallback_lines": raw_format_fallback_lines,
           "decode_note": "perf reports FAILED TO PARSE for the opcode-specific pretty formatter, but emits raw disk/ctrl_id/qid/cid/opcode/status fields used for pairing. No opcode-specific cdw payload decoding is used.",
           "trace_sha256": hashlib.sha256((p / "perf.data").read_bytes()).hexdigest(),
           "slowest_probe_commands": sorted(commands, key=lambda x: x["duration_ns"], reverse=True)[:30]}
assert not unmatched["duplicate_live_keys"]
assert not unmatched["unparsed_lines"]
assert not active
assert summary["nonzero_status_commands"] == 0
(p / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({key: summary[key] for key in ["stats", "matched_probe_commands", "other_or_unmatched", "pending_at_end", "nonzero_status_commands", "raw_field_format_fallback_lines"]}, indent=2))
