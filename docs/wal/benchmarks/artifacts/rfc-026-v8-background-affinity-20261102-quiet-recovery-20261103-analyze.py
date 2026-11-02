"""Analyze one fixed same-inode durability probe before/after a quiet interval."""

from pathlib import Path
import json


p = Path(__file__).resolve().parent
prefix = Path(__file__).name.removesuffix("-analyze.py")
r = json.loads((p / f"{prefix}-result.json").read_text())
assert r["complete"] and r["quiet_elapsed_s"] >= 1800
assert r["operations_per_probe"] == 1024 and r["block_bytes"] == 4096
assert r["fdatasync_every_writes"] == 4 and r["allowed_cpus"] == [8]
assert not r["initial_jobs"] and not r["final_jobs"] and not r["interfering_jobs"]


def stats(values):
    values = sorted(values)
    return {"count": len(values), "mean_us": sum(values) / len(values) / 1000,
            "p50_us": values[len(values) // 2] / 1000,
            "p99_us": values[int((len(values) - 1) * .99)] / 1000,
            "max_us": values[-1] / 1000,
            "over_1ms": sum(v > 1_000_000 for v in values),
            "over_5ms": sum(v > 5_000_000 for v in values)}


probes = []
for q in r["probes"]:
    assert len(q["write_ns"]) == 1024 and len(q["sync_ns"]) == 256
    assert q["final_block_contents_validated"]
    assert stats(q["write_ns"]) == q["write"]
    assert stats(q["sync_ns"]) == q["sync"]
    slow = [i for i, n in enumerate(q["write_ns"]) if n > 5_000_000]
    probes.append({
        "name": q["name"], "ops_s": q["ops_s"], "elapsed_s": q["elapsed_s"],
        "write": q["write"], "sync": q["sync"],
        "first_write_after_fdatasync": stats(q["write_ns"][::4]),
        "other_writes": stats([n for i, n in enumerate(q["write_ns"]) if i % 4]),
        "slow_write_indices": slow,
        "all_over_5ms_writes_immediately_follow_probe_fdatasync": all(i % 4 == 0 for i in slow),
    })
assert [q["name"] for q in probes] == ["before-quiet", "after-quiet"]

previous, previous_elapsed = r["quiet_diskstats_before"], 0
stretch = longest = 0
no_io_periods = 0
for sample in r["quiet_samples"]:
    current, elapsed = sample["diskstats"], sample["elapsed_s"]
    assert elapsed > previous_elapsed
    if all(current[k] == previous[k] for k in ["reads", "writes", "discards", "flushes"]) and not previous["inflight"] and not current["inflight"]:
        no_io_periods += 1
        stretch += elapsed - previous_elapsed
        longest = max(longest, stretch)
    else:
        stretch = 0
    previous, previous_elapsed = current, elapsed

d = r["quiet_diskstats_delta"]
summary = {
    "complete": True, "quiet_elapsed_s": r["quiet_elapsed_s"],
    "settings_changed": False, "production_code_changed": False,
    "normal_performance_qualification": False,
    "operations_per_probe": 1024, "writes_per_probe_mib": 4, "syncs_per_probe": 256,
    "quiet_samples": len(r["quiet_samples"]),
    "whole_device_quiet_interval": {"read_mib": d["sectors_read"] / 2048,
                                   "write_mib": d["sectors_written"] / 2048,
                                   "discard_mib": d["sectors_discarded"] / 2048,
                                   "flushes": d["flushes"], "io_ms": d["io_ms"]},
    "interfering_jobs_observed": r["interfering_jobs"],
    "sample_intervals_without_completed_io": no_io_periods,
    "longest_observed_stretch_without_completed_host_io_s": longest,
    "idle_limitation": "Benchmarks/builds/maintenance absent at observed checks. OS still writes/flushes. Five-second host counters do not measure internal NAND/GC activity or establish thirty minutes of uninterrupted device idleness.",
    "probes": probes,
    "ops_s_change_pct": (probes[1]["ops_s"] / probes[0]["ops_s"] - 1) * 100,
    "interpretation": "No observed return to the earlier fast state after thirty minutes without benchmark writes. Post-fdatasync write delay and eight >5ms writes persist. This does not identify garbage collection or prove that longer/fully idle conditions cannot help.",
}
(p / f"{prefix}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({"quiet_elapsed_s": r["quiet_elapsed_s"], "ops_s_change_pct": summary["ops_s_change_pct"],
                  "background_write_mib": d["sectors_written"] / 2048, "background_flushes": d["flushes"],
                  "over_5ms_writes": [q["write"]["over_5ms"] for q in probes]}, indent=2))
