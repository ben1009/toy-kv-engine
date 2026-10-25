#!/usr/bin/env python3
"""Record diagnostic CPU samples; these runs are excluded from throughput scoring."""
import csv
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path("/home/liu/proj/toy-kv-engine")
ARTIFACTS = ROOT / "docs/wal/benchmarks/artifacts"
STEM = "rfc-025-v7-packer-encoding-cpu-profile-20261025"
SCRATCH = Path("/dev/shm/toy-kv-v7-packer-profile-20261025")
WORK = ROOT / "target/wal-opt/20261025-v7-handoff"
BINARY = ROOT / "target/release/write-perf-v7-packer-encoding"
if SCRATCH.exists() or (ARTIFACTS / (STEM + ".jsonl")).exists():
    raise SystemExit("refusing to overwrite existing profile data")
SCRATCH.mkdir()
try:
    with (ARTIFACTS / (STEM + "-runs.tsv")).open("w") as output:
        writer = csv.writer(output, delimiter="\t", lineterminator="\n")
        writer.writerow(["format", "binary_sha256", "run_id", "raw_sha256", "command"])
        for version in (7, 4):
            db = SCRATCH / ("v" + str(version))
            data = WORK / ("packer-v" + str(version) + "-perf.data")
            command = ["perf", "record", "-e", "cpu-clock:u", "-F", "997", "--call-graph", "dwarf,8192", "-o", str(data), "--", str(BINARY), "--suite", "legacy", "--preset", "smoke", "--wal", "--bench", "wal_concurrent", "--num", "100000", "--threads", "8", "--value-size", "1024", "--target-sst-size", "1073741824", "--latency-sample-every", "100", "--output", "json", "--wal-io-mode", "parallel", "--path", str(db)]
            if version == 7:
                command += ["--pitr", "--pitr-format", "v7"]
            raw = subprocess.check_output(command).strip()
            record = json.loads(raw)
            with (ARTIFACTS / (STEM + ".jsonl")).open("ab") as stream:
                stream.write(raw + b"\n")
            writer.writerow([version, hashlib.sha256(BINARY.read_bytes()).hexdigest(), record["run_id"], hashlib.sha256(raw).hexdigest(), subprocess.list2cmdline(command)])
            with (ARTIFACTS / (STEM + "-v" + str(version) + ".txt")).open("w") as stream:
                subprocess.run(["perf", "report", "--stdio", "--no-children", "--call-graph", "none", "--percent-limit", "0.3", "-i", str(data)], stdout=stream, check=True)
            data.unlink()
            shutil.rmtree(db)
            repository = Path(str(db) + ".pitr-repository")
            if repository.exists():
                shutil.rmtree(repository)
finally:
    shutil.rmtree(SCRATCH)
