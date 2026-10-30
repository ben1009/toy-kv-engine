#!/usr/bin/env python3
"""Fixed four-arm comparison isolating coordinator-owned v7 prefix hashing."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path('/home/liu/proj/toy-kv-engine')
ART = ROOT / 'docs/wal/benchmarks/artifacts'
WORK = ROOT / 'target/wal-opt/20261026-v7-deferred-prefix-hash'
IDENTITY = json.loads((ART / 'rfc-025-v7-deferred-prefix-hash-20261026-binaries.json').read_text())
CELLS = {
    'tmpfs-batch64': ('tmpfs', 750000, 64, 3),
    'tmpfs-point8': ('tmpfs', 100000, None, 3),
    'ext4-batch64': ('ext4', 750000, 64, 5),
    'tmpfs-point1': ('tmpfs', 100000, None, 3),
    'ext4-point8': ('ext4', 100000, None, 5),
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    cell = sys.argv[1]
    filesystem, operations, batch_size, pairs = CELLS[cell]
    writers = 1 if cell.endswith('point1') else 8
    stem = 'rfc-025-v7-deferred-prefix-hash-' + cell + '-20261026'
    scratch = (Path('/dev/shm') if filesystem == 'tmpfs' else WORK) / (stem + '-scratch')
    raw_path = ART / (stem + '.jsonl')
    manifest = ART / (stem + '-runs.tsv')
    assert not any(p.exists() for p in (scratch, raw_path, manifest)), 'refusing to overwrite captures'
    for data in IDENTITY.values():
        assert sha(ROOT / data['path']) == data['sha256']
    scratch.mkdir()
    ids = set()
    try:
        with raw_path.open('wb') as raw_stream, manifest.open('w') as stream:
            writer = csv.writer(stream, delimiter='\t', lineterminator='\n')
            writer.writerow(['source_record','phase','filesystem','workload','writers','operations','pair','arm','binary_sha256','run_id','raw_sha256','command','nvme_temperature_millic'])
            schedule = [(0, arm, 'warmup') for arm in ('old_v4', 'old_v7', 'new_v4', 'new_v7')]
            for pair in range(1, pairs + 1):
                arms = ('old_v4','new_v4','old_v7','new_v7') if pair % 2 else ('new_v7','old_v7','new_v4','old_v4')
                schedule += [(pair, arm, 'scored') for arm in arms]
            if filesystem == 'ext4':
                time.sleep(30)
            for pair, arm, phase in schedule:
                identity = IDENTITY['retained' if arm.startswith('old') else 'candidate']
                version = 4 if arm.endswith('v4') else 7
                name = f'{phase}-p{pair}-{arm}'
                db = scratch / (name + '-db')
                command = [str(ROOT / identity['path']), '--suite','legacy','--preset','smoke','--wal',
                    '--bench','wal_batch_concurrent' if batch_size else 'wal_concurrent',
                    '--num',str(operations),'--threads',str(writers),'--value-size','1024',
                    '--target-sst-size','2147483648','--latency-sample-every','10' if batch_size else '100',
                    '--output','json','--wal-io-mode','parallel','--path',str(db)]
                if batch_size:
                    command += ['--wal-batch-size',str(batch_size)]
                if version == 7:
                    command += ['--pitr','--pitr-format','v7']
                raw = subprocess.check_output(command).strip()
                record = json.loads(raw)
                assert record['params']['wal_format_version'] == version
                assert record['params']['wal_io_mode'] == 'parallel'
                assert record['params']['threads'] == writers
                assert record['params']['batch_size'] == batch_size
                assert record['params']['value_size'] == 1024
                assert record['engine_options']['target_sst_size'] == 2147483648
                assert record['result']['ops'] == operations
                assert record['latency']['samples'] == (1176 if batch_size else 1000)
                assert record['run_id'] not in ids
                ids.add(record['run_id'])
                temperature = 'unavailable'
                for hwmon in Path('/sys/class/hwmon').glob('hwmon*'):
                    try:
                        if (hwmon / 'name').read_text().strip() == 'nvme':
                            temperature = (hwmon / 'temp1_input').read_text().strip()
                            break
                    except (FileNotFoundError, PermissionError):
                        pass
                raw_stream.write(raw + b'\n')
                raw_stream.flush()
                writer.writerow([name+'.json',phase,filesystem,'batch64' if batch_size else 'point'+str(writers),writers,operations,pair,arm,
                    identity['sha256'],record['run_id'],hashlib.sha256(raw).hexdigest(),subprocess.list2cmdline(command),temperature])
                stream.flush()
                for disposable in (db, Path(str(db)+'.pitr-repository')):
                    if disposable.exists():
                        shutil.rmtree(disposable)
                print(cell, phase, pair, arm, flush=True)
                time.sleep(10 if filesystem == 'ext4' else 3)
        for data in IDENTITY.values():
            assert sha(ROOT / data['path']) == data['sha256']
    finally:
        shutil.rmtree(scratch)


if __name__ == '__main__':
    main()
