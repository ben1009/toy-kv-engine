#!/usr/bin/env python3
"""Highest matching scored rates across the four extent follow-up cohorts."""
import csv
import hashlib
import json
from pathlib import Path

ART = Path(__file__).resolve().parent
PLAN = json.loads((ART / 'rfc-025-v7-serial-startup-20261027-plan.json').read_text())
IDENTITY = json.loads((ART / 'rfc-025-v7-serial-startup-20261027-binaries.json').read_text())
COHORTS = [('async-extent', '20261026'), ('serial-extent', '20261027'),
           ('demand-extent', '20261027'), ('serial-startup', '20261027')]
output = {cell: dict(options=cfg, peaks={}) for cell, cfg in PLAN['matrix'].items()}
for variant, date in COHORTS:
    for manifest in sorted(ART.glob('rfc-025-v7-' + variant + '-initial-ext4*-'+date+'-runs.tsv')):
        raw_path = manifest.with_name(manifest.name.replace('-runs.tsv', '.jsonl'))
        raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest())
               for line in raw_path.read_bytes().splitlines() for r in [json.loads(line)]}
        for row in csv.DictReader(manifest.open(), delimiter='\t'):
            if row['phase'] != 'scored' or row['filesystem'] != 'ext4':
                continue
            record, digest = raw[row['run_id']]
            assert digest == row['raw_sha256']
            if record['params']['wal_io_mode'] != 'parallel':
                continue
            version = record['params']['wal_format_version']
            label = ('v4_control' if version == 4 else
                     'kept_async_v7' if row['binary_sha256'] == IDENTITY['retained']['sha256'] else
                     'hybrid_v7' if row['binary_sha256'] == IDENTITY['candidate']['sha256'] else None)
            if label is None:
                continue
            for cell, cfg in PLAN['matrix'].items():
                if (record['params']['threads'] != cfg['writers']
                        or record['params']['batch_size'] != cfg['batch']
                        or record['result']['ops'] != cfg['operations']
                        or record['latency']['sample_every'] != cfg['sample_every']
                        or record['latency']['samples'] != cfg['samples']
                        or record['params']['value_size'] != 1024
                        or record['engine_options']['target_sst_size'] != 2147483648):
                    continue
                item = dict(puts_per_sec=record['result']['ops_per_sec'], run_id=row['run_id'],
                            raw_sha256=digest, binary_sha256=row['binary_sha256'],
                            cohort=variant, arm=row['arm'], pair=int(row['pair']),
                            manifest=manifest.name, raw=raw_path.name)
                old = output[cell]['peaks'].get(label)
                if old is None or item['puts_per_sec'] > old['puts_per_sec']:
                    output[cell]['peaks'][label] = item
for cell, data in output.items():
    assert set(data['peaks']) == {'v4_control', 'kept_async_v7', 'hybrid_v7'}, cell
output['scope'] = 'Highest scored records across the four named System/native-I/O extent cohorts with exactly matching workload options; warmups excluded. Descriptive peaks only; no pooling of candidate binaries or override of fixed paired guards.'
output['script_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
print(json.dumps(output, indent=2))
