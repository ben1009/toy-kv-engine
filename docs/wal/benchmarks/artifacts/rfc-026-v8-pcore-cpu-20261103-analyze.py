"""Recompute the diagnostic summary from the archived measurements and stacks."""

from collections import Counter
from pathlib import Path
import gzip
import json
import re


p = Path(__file__).resolve().parent
prefix = Path(__file__).name.removesuffix("-analyze.py")
records = [json.loads(line) for line in (p / f"{prefix}-measurements.jsonl").read_text().splitlines()]
native = json.loads((p / f"{prefix}-native-evidence.json").read_text())
header_re = re.compile(r"^\S.*?\s+(\d+)/(\d+)\s+\[(\d+)\]\s+(\d+\.\d+): cpu-clock:u:")
map_re = re.compile(r"\[(0x[0-9a-f]+)\([^)]*\) @ (0x[0-9a-f]+|0) <([0-9a-f]+)>\]: r-xp")


def phases(raw):
    result = {}
    for index, name in enumerate(raw["phase_names"]):
        buckets = [thread["buckets"][index] for thread in raw["threads"]]
        measured = [b for b in buckets if b["samples"]]
        result[name] = {
            "calls": sum(b["calls"] for b in buckets),
            "samples": sum(b["samples"] for b in buckets),
            "calls_without_any_samples_in_thread": sum(b["calls"] for b in buckets if not b["samples"]),
            "estimated_cpu_s": sum(b["cpu_ns"] * b["calls"] / b["samples"] for b in measured) / 1e9,
            "estimated_wall_s": sum(b["wall_ns"] * b["calls"] / b["samples"] for b in measured) / 1e9,
        }
    return result


def samples(record):
    canonical_maps = [map_re.search(line) for line in record["libc_mmaps"]]
    last = [m for m in canonical_maps if m][-1]
    assert last[3] == native["libc_build_id"]
    base = int(last[1], 16) - int(last[2], 16)
    counts, categories, offsets, callers = Counter(), Counter(), Counter(), Counter()
    cpus, tids = Counter(), set()
    no_stack, one_frame = 0, 0
    lock_sites = {0xA5FDC, 0xA5FFC, 0xA4809, 0xA481C}
    mapped_lock_samples = 0
    with gzip.open(p / record["sample_archive"], "rt") as f:
        blocks = f.read().split("\n\n")
    for block in blocks:
        lines = block.splitlines()
        if not lines:
            continue
        match = header_re.match(lines[0])
        assert match, lines[0]
        cpus[int(match[3])] += 1
        tids.add(int(match[2]))
        stack = [line.strip().split(" ", 1) for line in lines[1:] if line.startswith("\t")]
        if not stack:
            no_stack += 1
            continue
        if len(stack) == 1:
            one_frame += 1
        ip, symbol_dso = stack[0]
        symbol, dso = symbol_dso.rsplit(" (", 1)
        dso = dso.removesuffix(")")
        counts[symbol_dso] += 1
        if "libmimalloc.so" in dso:
            category = "mimalloc"
        elif "libc.so" in dso:
            category = "libc"
            offset = int(ip, 16) - base
            offsets[hex(offset)] += 1
            if offset in lock_sites:
                mapped_lock_samples += 1
            caller = next((frame[1] for frame in stack[1:] if "[unknown]" not in frame[1]
                           and "libc.so" not in frame[1]), "[caller unresolved]")
            callers[caller] += 1
        elif "blake3" in symbol:
            category = "blake3"
        elif "sha256" in symbol:
            category = "sha256"
        elif "crc32" in symbol:
            category = "crc32"
        elif "publish_commit_ts" in symbol:
            category = "mvcc_publish"
        elif "search_position" in symbol and "SkipList" in symbol:
            category = "skiplist_search"
        elif "insert_internal" in symbol and "SkipList" in symbol:
            category = "skiplist_insert"
        elif "__vdso_clock_gettime" in symbol:
            category = "clock_gettime"
        else:
            category = "other"
        categories[category] += 1
    total = sum(cpus.values())
    assert total and not no_stack
    return {
        "samples": total, "P_samples": sum(v for k, v in cpus.items() if k < 16),
        "E_samples": sum(v for k, v in cpus.items() if k >= 16),
        "threads": len(tids), "samples_without_stack": no_stack,
        "one_frame_stacks": one_frame,
        "categories": {k: {"samples": v, "pct": 100 * v / total} for k, v in categories.items()},
        "top_leaf_symbols": counts.most_common(20),
        "libc_elf_base": hex(base), "libc_build_id": last[3],
        "top_libc_offsets": offsets.most_common(20),
        "top_libc_non_libc_callers": callers.most_common(12),
        "exact_verified_libc_lock_site_samples": mapped_lock_samples,
        "exact_verified_libc_lock_site_pct": mapped_lock_samples * 100 / total,
        "lock_site_note": "Lower bound from four exact sampled lock/xchg branch sites, not all libc or allocator CPU; private function names are stripped.",
    }


results = []
for record in records:
    raw, status = record["raw"], record["status"]
    q = raw["result"]
    assert status["returncode"] == 0 and q["ops"] == 750000
    assert record["samples_lost"] == 0
    assert q["wal_pipeline"]["commit_buffers"] == 11728
    meansync = q["wal_pipeline"]["fdatasync_ns"] / q["wal_pipeline"]["sync_count"] / 1e6
    metrics = {
        "cohort": record["cohort"], "name": record["name"], "format": status["format"],
        "allocator": status.get("allocator", "system"), "puts_s": q["ops_per_sec"],
        "p99_ms": raw["latency"]["p99_ms"], "mean_sync_ms": meansync,
        "syncs": q["wal_pipeline"]["sync_count"],
        "worker_groups": q["wal_pipeline"]["worker_eventfd_notifications"],
        "process_cpu_s": (q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000,
        "command_tree_max_individual_rss_kib": status.get("command_tree_max_individual_rss_kib"),
        "storage_flag": raw["latency"]["p99_ms"] > 10 or meansync > 1,
        "performance_ranking_eligible": False,
        "profiler_lost_samples": record["samples_lost"],
        "phase_estimates": phases(record["phases"]), "cpu_samples": samples(record),
    }
    if status["format"] == "v8":
        digest = q["wal_v8_digest"]
        assert digest["payload_hash_bytes"] == 787500000
        assert digest["post_encode_hash_reread_bytes"] == 0
        metrics["digest_metrics"] = dict(digest)
        metrics["digest_estimated_cpu_s"] = {
            k: digest[k] * 11728 / digest["cpu_timing_sampled_tickets"] / 1e9 for k in
            ["payload_hash_cpu_ns", "header_batch_hash_cpu_ns", "ordered_link_hash_cpu_ns"]}
    assert metrics["cpu_samples"]["E_samples"] == 0
    results.append(metrics)

allocator = {r["name"]: r for r in results if r["cohort"] == "allocator"}
pairs = []
for baseline, candidate in [("00-system", "01-mimalloc"), ("03-system", "02-mimalloc")]:
    b, c = allocator[baseline], allocator[candidate]
    pairs.append({
        "baseline": baseline, "candidate": candidate,
        "cpu_change_pct": 100 * (c["process_cpu_s"] / b["process_cpu_s"] - 1),
        "peak_rss_change_pct": 100 * (c["command_tree_max_individual_rss_kib"] / b["command_tree_max_individual_rss_kib"] - 1),
        "diagnostic_only": True, "storage_flags_on_both": b["storage_flag"] and c["storage_flag"],
    })

summary = {
    "purpose": "Existing-SSD CPU attribution and reversible allocator screen, not performance qualification.",
    "runtime_changed": False, "allocator_retained": False,
    "measurement_count": len(results), "storage_flagged": sum(r["storage_flag"] for r in results),
    "phase_method": "Per-thread calls/samples scaling of sampled thread CPU clocks. Inclusive parents and children must not be added. Missing samples are reported, not imputed.",
    "sample_method": "Timed userspace cpu-clock samples at 499 Hz. Leaf shares locate sampled userspace work; they are not percentages of process user+system CPU. Partial callchains do not make every caller identifiable.",
    "memory_method": "wait4 command-tree ru_maxrss: maximum individual-process RSS, not sum, delta or timed-only peak; includes benchmark/profiler lifetimes.",
    "allocator_pairs": pairs, "measurements": results,
}
(p / f"{prefix}-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({"captures": len(results), "flagged": summary["storage_flagged"],
                  "allocator_pairs": pairs, "allocator_retained": False}, indent=2))
