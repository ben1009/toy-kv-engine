from pathlib import Path
import ast
import hashlib
import json
import re
import shutil
import subprocess


p = Path(__file__).resolve().parent
repo = p.parent.parent
assert "DONE" in (p / "run.log").read_text().splitlines()
assert "DONE" in (p / "confirmation.log").read_text().splitlines()
assert "DONE" in (p / "v7-rerun.log").read_text().splitlines()
rows = [json.loads(line) for line in (p / "measurements.jsonl").read_text().splitlines()]
assert len(rows) == 40
assert sum(row["mode"] == "normal" for row in rows) == 32
assert sum(row["mode"] == "diagnostic" for row in rows) == 8
for row in rows:
    assert row["status"]["returncode"] == 0
    raw = repo / row["source_record"]
    assert hashlib.sha256(raw.read_bytes()).hexdigest() == row["source_record_sha256"]
    assert json.loads(raw.read_text()) == row["record"]
    q = row["record"]["result"]
    assert q["ops"] == 750000
    if row["format"] == "v8":
        assert q["wal_v8_digest"]["payload_hash_bytes"] == 787500000
        assert q["wal_v8_digest"]["post_encode_hash_reread_bytes"] == 0

original_hashes = json.loads((repo / "target/rfc-026-v8-producer-attribution-20261102/source-hashes.json").read_text())
for path, expected in original_hashes.items():
    assert hashlib.sha256((repo / path).read_bytes()).hexdigest() == expected
subprocess.run(["git", "diff", "--quiet", "HEAD", "--", "kv-engine", "Cargo.toml", "Cargo.lock"], cwd=repo, check=True)
assert not subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "--", "kv-engine"], cwd=repo).strip()
for script in ["run.py", "confirm.py", "v7-rerun.py", "analyze.py", "archive.py", "write-report.py"]:
    ast.parse((p / script).read_text(), filename=script)
profiles = json.loads((p / "perf-export.json").read_text())
assert len(profiles) == 16 and all(r["returncode"] == 0 for r in profiles)
cpus = json.loads((p / "cpu-sample-summary.json").read_text())
assert len(cpus["samples"]) == 8
for name, data in cpus["samples"].items():
    assert data["all"]["P_samples"] + data["all"]["E_samples"] > 0
    if "pcore" in name:
        assert data["all"]["E_samples"] == 0

checks = {
    "runs_completed": 40, "normal_runs": 32, "diagnostic_runs": 8,
    "all_returncodes_zero": True, "all_raw_record_hashes_match": True,
    "all_operations_per_run": 750000,
    "v8_payload_hash_bytes_per_run": 787500000, "v8_post_encode_hash_reread_bytes_per_run": 0,
    "runtime_unchanged_from": "b8da402a276d7969f4c40ca99787a10a63d93f33",
    "original_source_sha256": original_hashes,
    "runtime_git_diff_empty": True, "new_rust_files": [],
    "profile_exports_succeeded": 16, "pcore_recordings_have_no_E_core_user_samples": True,
    "python_syntax_validated": True,
    "new_runtime_candidate": False, "tests_or_builds_during_measurements": False,
    "production_defaults_changed": False, "committed": False, "pushed": False, "merged": False,
}
(p / "checks.json").write_text(json.dumps(checks, indent=2) + "\n")
art = repo / "docs/wal/benchmarks/artifacts"
for suffix in ["plan.json", "confirmation-plan.json", "v7-rerun-plan.json", "run.py", "confirm.py", "v7-rerun.py", "analyze.py", "archive.py", "write-report.py",
               "measurements.jsonl", "summary.json", "cpu-sample-summary.json", "core-masks.json",
               "topology.txt", "filesystem.txt", "perf-export.json", "checks.json"]:
    shutil.copyfile(p / suffix, art / (p.name + "-" + suffix))
for source in sorted(p.glob("*-symbols.txt")):
    normalized = "\n".join(re.sub(r"\s+", " ", line.strip()) for line in source.read_text().splitlines()) + "\n"
    (art / (p.name + "-" + source.name)).write_text(normalized)
print("Archived 40 complete measurements and all diagnostics. Runtime unchanged; no production candidate.")
