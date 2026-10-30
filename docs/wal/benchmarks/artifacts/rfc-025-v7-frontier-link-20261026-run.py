#!/usr/bin/env python3
"""Audit a bounded native FRONTIER/sync scheduling diagnostic."""
import csv
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-frontier-link-20261026'
WORK = ROOT / 'target/wal-opt/20261026-v7-frontier-link'
BINARY = WORK / 'frontier-probe'
POLICIES = ['syscalls', 'uring_default', 'uring_coop_single', 'uring_defer_single']
CELLS = ['point8', 'batch64x8']


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    identity = json.loads((ART / (STEM + '-build.json')).read_text())
    assert sha(BINARY) == identity['binary_sha256']
    assert sha(ART / (STEM + '-probe.c')) == identity['source_sha256']
    spec = importlib.util.spec_from_file_location(
        'audit', ART / 'rfc-025-v7-extent-io-20261026-run.py')
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)
    audit.BLOCKED.add(BINARY.name)
    assert not audit.host_sample()['competing']
    raw_path, audit_path, manifest_path = [
        ART / (STEM + suffix) for suffix in ('.jsonl', '-host.jsonl', '-runs.tsv')]
    scratch = WORK / 'scratch'
    assert not any(p.exists() for p in (raw_path, audit_path, manifest_path, scratch))
    scratch.mkdir()
    time.sleep(30)
    try:
        with raw_path.open('wb') as raw, audit_path.open('wb') as audits, manifest_path.open('w') as manifest:
            writer = csv.writer(manifest, delimiter='\t', lineterminator='\n')
            writer.writerow(['run_id', 'cell', 'phase', 'round', 'policy', 'binary_sha256',
                             'raw_sha256', 'audit_sha256', 'command'])
            for cell in CELLS:
                schedule = [('warmup', 0, policy) for policy in POLICIES]
                for round_index in range(1, 6):
                    shift = (round_index - 1) % len(POLICIES)
                    order = POLICIES[shift:] + POLICIES[:shift]
                    if round_index % 2 == 0:
                        order = list(reversed(order))
                    schedule.extend(('scored', round_index, policy) for policy in order)
                for phase, round_index, policy in schedule:
                    key = f'{cell}-{phase}-{round_index}-{policy}'
                    path = scratch / (key + '.data')
                    command = [str(BINARY), policy, cell, str(path)]
                    stdout, stderr, observation, clean = audit.run_audited(command)
                    (WORK / (key + '.stderr')).write_bytes(stderr)
                    if observation['returncode']:
                        (WORK / (key + '-failed.stdout')).write_bytes(stdout)
                        raise RuntimeError((key, observation['returncode'], stderr.decode()))
                    measured = json.loads(stdout)
                    assert measured['policy'] == policy and measured['cell'] == cell
                    assert measured['cycles'] == 500 and measured['readback_verified']
                    assert not path.exists()
                    run_id = str(uuid.uuid4())
                    record = dict(run_id=run_id, cell=cell, phase=phase, round=round_index,
                                  policy=policy, benchmark=measured)
                    observation.update(run_id=run_id, cell=cell, phase=phase, round=round_index,
                                       policy=policy, clean_sampled=clean,
                                       binary_sha256=identity['binary_sha256'])
                    data = json.dumps(record, sort_keys=True, separators=(',', ':')).encode()
                    host = json.dumps(observation, sort_keys=True, separators=(',', ':')).encode()
                    raw.write(data + b'\n'); raw.flush()
                    audits.write(host + b'\n'); audits.flush()
                    writer.writerow([run_id, cell, phase, round_index, policy, identity['binary_sha256'],
                                     hashlib.sha256(data).hexdigest(), hashlib.sha256(host).hexdigest(),
                                     subprocess.list2cmdline(command)])
                    manifest.flush()
                    assert clean, 'Competing activity detected; diagnostic preserved and stopped'
                    print(key, 'clean_sampled', flush=True)
                    time.sleep(10)
    finally:
        shutil.rmtree(scratch)
    assert sha(BINARY) == identity['binary_sha256']


if __name__ == '__main__':
    main()
