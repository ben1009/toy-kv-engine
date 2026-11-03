from collections import defaultdict
from pathlib import Path
import ast
import hashlib
import json
import re
import shutil
import statistics
import subprocess

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from run import validate_roles, modes


p = Path(__file__).resolve().parent
repo = p.parent.parent.parent
plan = json.loads((p / "plan.json").read_text())
rows = []
for index, item in enumerate(plan["schedule"]):
    name = f"{index:02}-v8-{item['mode']}"
    status_path = p / (name + "-status.json")
    if not status_path.exists():
        continue
    status = json.loads(status_path.read_text())
    assert status["returncode"] == 0, status
    kernel_mask = "0-31" if item["mode"] == "writers" else "0-15"
    registrations = status["iowq_affinity_registrations"]
    assert registrations and all(f"registered=1 cpus={kernel_mask} " in line for line in registrations)
    raw = p / (name + ".json")
    audit = p / (name + "-affinity.json")
    record = json.loads(raw.read_text())
    roles = json.loads(audit.read_text())
    validate_roles(roles, item["mode"])
    q, latency = record["result"], record["latency"]
    w, digest = q["wal_pipeline"], q["wal_v8_digest"]
    assert q["ops"] == 750000
    assert digest["payload_hash_bytes"] == 787500000
    assert digest["post_encode_hash_reread_bytes"] == 0
    scale = 11728 / digest["cpu_timing_sampled_tickets"] / 1e9
    mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
    rows.append({
        **item, "name": name, "record": record, "audit": roles, "status": status,
        "raw_record_path": str(raw.relative_to(repo)),
        "raw_record_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
        "audit_path": str(audit.relative_to(repo)),
        "audit_sha256": hashlib.sha256(audit.read_bytes()).hexdigest(),
        "puts_s": q["ops_per_sec"], "p99_ms": latency["p99_ms"],
        "cpu_s": (q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000,
        "context": roles["context"], "syncs": w["sync_count"],
        "mean_sync_ms": mean, "stall_flag": latency["p99_ms"] > 10 or mean > 1,
        "payload_hash_cpu_estimated_s": digest["payload_hash_cpu_ns"] * scale,
        "ordered_link_cpu_estimated_s": digest["ordered_link_hash_cpu_ns"] * scale,
        "role_masks_validated": True,
    })

groups = defaultdict(list)
for row in rows:
    groups[row["mode"]].append(row)
summaries = []
fields = ["name", "puts_s", "p99_ms", "cpu_s", "context", "syncs", "mean_sync_ms",
          "payload_hash_cpu_estimated_s", "ordered_link_cpu_estimated_s"]
for mode in modes:
    values = groups[mode]
    eligible = [x for x in values if not x["stall_flag"]]
    best = max(eligible, key=lambda x: x["puts_s"]) if eligible else None
    summaries.append({"mode": mode, "total": len(values), "eligible": len(eligible),
                      "best_eligible": {key: best[key] for key in fields} if best else None,
                      "median_eligible": {key: statistics.median(x[key] for x in eligible)
                                          for key in ["puts_s", "p99_ms", "cpu_s", "payload_hash_cpu_estimated_s"]} if eligible else None})
summary = {"samples": len(rows), "flagged": sum(r["stall_flag"] for r in rows),
           "groups": summaries, "runtime_changed": False,
           "binary_sha256": plan["binary_sha256"],
           "scope": "One new executable for six cumulative placement modes; all masks audited and io-wq registration explicitly verified. Historical scores are context only. No production affinity change."}
(p / "measurements.jsonl").write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows))
(p / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
for row in rows:
    print(row["name"], "flagged" if row["stall_flag"] else "eligible", round(row["puts_s"]),
          round(row["p99_ms"], 3), round(row["cpu_s"], 3), "hash", round(row["payload_hash_cpu_estimated_s"], 3))
for row in summaries:
    print(json.dumps(row))

if len(rows) == len(plan["schedule"]) and "RERUN DONE" in (p / "run.log").read_text().splitlines():
    source_hashes = json.loads((p.parent / "source-hashes.json").read_text())
    for name, expected in source_hashes.items():
        assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == expected
    subprocess.run(["git", "diff", "--quiet", "HEAD", "--", "kv-engine", "Cargo.toml", "Cargo.lock"], cwd=repo, check=True)
    assert hashlib.sha256(Path(plan["binary"]).read_bytes()).hexdigest() == plan["binary_sha256"]
    for script in ["rerun.py", "analyze.py"]:
        ast.parse((p / script).read_text(), filename=script)
    (p / "checks.json").write_text(json.dumps({
        "normal_captures_completed": len(rows), "all_returncodes_zero": True,
        "all_16_writer_masks_and_all_background_roles_validated_every_capture": True,
        "kernel_registration_success_and_actual_masks_verified": True,
        "all_750000_operation_counts_and_V8_hash_byte_counts_validated": True,
        "post_encode_hash_reread_bytes": 0, "runtime_diff_empty": True,
        "benchmark_hook_retained": False, "same_binary_as_original_cohort": True,
        "binary_sha256": plan["binary_sha256"], "source_hashes": source_hashes,
    }, indent=2) + "\n")
    art = repo / "docs/wal/benchmarks/artifacts"
    for suffix in ["plan.json", "rerun.py", "analyze.py", "measurements.jsonl", "summary.json", "checks.json", "run.log"]:
        shutil.copyfile(p / suffix, art / (p.parent.name + "-" + p.name + "-" + suffix))
    print("Archived rerun as a separate cohort. Runtime unchanged.")
