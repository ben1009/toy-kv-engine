#!/usr/bin/env python3
"""Diagnostic CPU samples for the retained coalescing build; exclude from scoring."""
import csv
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path('/home/liu/proj/toy-kv-engine')
ART = ROOT / 'docs/wal/benchmarks/artifacts'
WORK = ROOT / 'target/wal-opt/20261025-v7-remaining-cost'
STEM = 'rfc-025-v7-coalescing-cpu-profile-20261025'
SCRATCH = Path('/dev/shm/toy-kv-v7-coalescing-profile-20261025')
BINARY = ROOT / 'target/release/write-perf-v7-preparation-coalesce'
EXPECTED = 'b7418dda91701807a76809a094eaa0538540efcac9a1cb0f683881f9ad02ae34'
assert hashlib.sha256(BINARY.read_bytes()).hexdigest() == EXPECTED
if SCRATCH.exists() or (ART / (STEM + '.jsonl')).exists():
    raise SystemExit('refusing to overwrite existing profiles')
SCRATCH.mkdir()
try:
    with (ART / (STEM + '-runs.tsv')).open('w') as stream:
        writer = csv.writer(stream, delimiter='\t', lineterminator='\n')
        writer.writerow(['cell', 'source_commit', 'binary_sha256', 'run_id', 'raw_sha256', 'command'])
        for cell, version, batch, operations in [('v7-batch64', 7, True, 250000), ('v7-point', 7, False, 100000), ('v4-point', 4, False, 100000)]:
            db = SCRATCH / cell
            data = WORK / (cell + '.data')
            command = ['perf', 'record', '-e', 'cpu-clock:u', '-F', '997', '--call-graph', 'dwarf,8192', '-o', str(data), '--', str(BINARY), '--suite', 'legacy', '--preset', 'smoke', '--wal', '--bench', 'wal_batch_concurrent' if batch else 'wal_concurrent', '--num', str(operations), '--threads', '8', '--value-size', '1024', '--target-sst-size', '1073741824', '--latency-sample-every', '100', '--output', 'json', '--wal-io-mode', 'parallel', '--path', str(db)]
            if batch:
                command += ['--wal-batch-size', '64']
            if version == 7:
                command += ['--pitr', '--pitr-format', 'v7']
            print('profiling', cell, flush=True)
            raw = subprocess.check_output(command).strip()
            record = json.loads(raw)
            with (ART / (STEM + '.jsonl')).open('ab') as output:
                output.write(raw + b'\n')
            writer.writerow([cell, '34a8b334', EXPECTED, record['run_id'], hashlib.sha256(raw).hexdigest(), subprocess.list2cmdline(command)])
            stream.flush()
            for mode, args in [('flat', ['--no-children', '--call-graph', 'none']), ('calls', ['--children', '--call-graph', 'graph,0.8,caller'])]:
                result = subprocess.check_output(['perf', 'report', '--stdio', '--percent-limit', '0.3', '-i', str(data), *args], text=True)
                (WORK / (cell + '-' + mode + '.txt')).write_text(result)
                (ART / (STEM + '-' + cell + '-' + mode + '.txt')).write_text('\n'.join(line.rstrip() for line in result.splitlines()) + '\n')
            shutil.rmtree(db)
            repository = Path(str(db) + '.pitr-repository')
            if repository.exists():
                shutil.rmtree(repository)
finally:
    shutil.rmtree(SCRATCH)
