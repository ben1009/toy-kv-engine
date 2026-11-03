from collections import defaultdict
from pathlib import Path
import ast
import hashlib
import json
import re
import shutil
import statistics
import subprocess

from run import validate_roles


p = Path(__file__).resolve().parent
repo = p.parent.parent
plan = json.loads((p / "plan.json").read_text())
rows = []
for index, item in enumerate(plan["schedule"]):
    name = f"{index:02}-v8-{item['mode']}"
    status_path = p / (name + "-status.json")
    if not status_path.exists():
        continue
    status = json.loads(status_path.read_text())
    assert status["returncode"] == 0, status
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
for mode in ["default", "writers", "whole"]:
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
           "scope": "One new benchmark-only executable for the three placement modes; historical binary scores are context only. No production affinity change or worker pool."}
(p / "measurements.jsonl").write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows))
(p / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
for row in rows:
    print(row["name"], "flagged" if row["stall_flag"] else "eligible", round(row["puts_s"]),
          round(row["p99_ms"], 3), round(row["cpu_s"], 3), "hash", round(row["payload_hash_cpu_estimated_s"], 3))
for row in summaries:
    print(json.dumps(row))

if len(rows) == 18 and "DONE" in (p / "run.log").read_text().splitlines():
    for row in rows:
        assert (repo / row["raw_record_path"]).read_bytes() == (p / (row["name"] + ".json")).read_bytes()
    smoke_rows = []
    for path in sorted(p.glob("smoke-*-status.json")):
        status = json.loads(path.read_text())
        name = status["name"]
        assert status["returncode"] == 0
        record = json.loads((p / (name + ".json")).read_text())
        audit = json.loads((p / (name + "-affinity.json")).read_text())
        validate_roles(audit, status["mode"], fmt=status["format"])
        smoke_rows.append({"status": status, "record": record, "audit": audit})
    assert len(smoke_rows) == 6
    (p / "smoke-measurements.json").write_text(json.dumps(smoke_rows, separators=(",", ":")) + "\n")
    source_hashes = json.loads((p / "source-hashes.json").read_text())
    for name, expected in source_hashes.items():
        assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == expected
    subprocess.run(["git", "diff", "--quiet", "HEAD", "--", "kv-engine", "Cargo.toml", "Cargo.lock"], cwd=repo, check=True)
    for script in ["run.py", "analyze.py"]:
        ast.parse((p / script).read_text(), filename=script)
    (p / "checks.json").write_text(json.dumps({
        "normal_captures_completed": 18, "smoke_captures_completed": 6,
        "all_returncodes_zero": True, "all_16_writer_masks_validated_every_capture": True,
        "WAL_worker_completion_reaper_and_maintenance_masks_validated_before_and_after": True,
        "all_750000_operation_counts_and_V8_hash_byte_counts_validated": True,
        "post_encode_hash_reread_bytes": 0, "runtime_diff_empty": True,
        "benchmark_source_restored": True, "benchmark_hook_retained": False,
        "no_new_worker_pool": True, "no_production_affinity_change": True,
        "binary_sha256": plan["binary_sha256"],
        "source_hashes": source_hashes,
    }, indent=2) + "\n")
    art = repo / "docs/wal/benchmarks/artifacts"
    for suffix in ["plan.json", "run.py", "analyze.py", "measurements.jsonl", "summary.json",
                   "smoke-measurements.json", "smoke-checks.json", "source-hashes.json", "instrumented-hashes.json",
                   "restoration.json", "writer-affinity.patch", "checks.json", "build.log", "build-confirm.log"]:
        shutil.copyfile(p / suffix, art / (p.name + "-" + suffix))
    print("Archived 18 scored captures, all six smoke audits and the temporary benchmark patch. Runtime unchanged.")
