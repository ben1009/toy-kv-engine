#!/usr/bin/env python3
"""Descriptive scored peaks from matching captures of this exact executable."""
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-current-v4-parallel-20261027'
PRIOR = 'rfc-025-v7-eligible-coalescing-20261027'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    matrix=json.loads((ART/(STEM+'-matrix.json')).read_text())
    identity=json.loads((ART/(STEM+'-binaries.json')).read_text())
    assert identity['v4']==identity['v7'] and sha(ROOT/identity['v4']['path'])==identity['v4']['sha256']
    output={}
    for cell,cfg in matrix['cells'].items():
        observations={'v4':[],'v7':[]};sources={}
        for family in [STEM,PRIOR]:
            rawpath=ART/(family+'-'+cell+'.jsonl')
            manifest=ART/(family+'-'+cell+'-runs.tsv')
            lines=rawpath.read_bytes().splitlines()
            raw={r['run_id']:(r,hashlib.sha256(line).hexdigest())for line in lines for r in [json.loads(line)]}
            sources[family]={str(p.relative_to(ROOT)):sha(p)for p in [rawpath,manifest]}
            for row in csv.DictReader(manifest.open(),delimiter='\t'):
                if row['phase']!='scored' or row['binary_sha256']!=identity['v4']['sha256']:
                    continue
                r,digest=raw[row['run_id']]
                assert row['raw_sha256']==digest
                if family==PRIOR:
                    assert row['arm']=='new_v7'
                arm='v7'if r['params']['wal_format_version']==7 else'v4'
                p,e=r['params'],r['engine_options']
                assert r['workload']==cfg['bench'] and r['result']['ops']==cfg['operations']
                assert p['wal_io_mode']=='parallel' and p['threads']==cfg['writers']
                assert p['batch_size']==cfg['batch'] and p['value_size']==1024
                assert e['target_sst_size']==2147483648 and '--compaction none'in row['command']
                assert p['wal_format_version']==(7 if arm=='v7' else 4)
                c=r['result']['wal_pipeline']
                marker=c['frontier_write_ns']/max(c['frontier_writes'],1)/1e6
                sync=c['fdatasync_ns']/max(c['sync_count'],1)/1e6
                observations[arm].append(dict(cohort=family,run_id=r['run_id'],raw_sha256=digest,
                                              binary_sha256=row['binary_sha256'],rate=r['result']['ops_per_sec'],
                                              p99_ms=r['latency']['p99_ms'],frontier_mean_ms=marker,
                                              fdatasync_mean_ms=sync,
                                              flagged=sync>=1.5 or (arm=='v7'and marker>=1)))
        result={}
        for arm,rows in observations.items():
            valid=[r for r in rows if not r['flagged']]
            result[arm]=dict(scored=len(rows),unflagged=len(valid),flagged_run_ids=[r['run_id']for r in rows if r['flagged']],
                             peak=max(valid,key=lambda r:r['rate'])if valid else None)
        v4,v7=result['v4']['peak'],result['v7']['peak']
        output[cell]=dict(**result,throughput_ratio=v7['rate']/v4['rate']if v4 and v7 else None,
                          peak_run_p99_ratio=v7['p99_ms']/v4['p99_ms']if v4 and v7 else None,
                          sources=sources)
    print(json.dumps(dict(cells=output,binary=identity['v4'],
                          method='Post-capture descriptive view requested after the maintainer reiterated ignoring obvious stalls. Exclude individually flagged scored observations using the original combined mean-I/O mask, then select each format maximum over these two matching cohorts. Warmups excluded; p99 belongs to each peak-throughput run. Same exact executable and workload settings; unequal sample counts and separate cohorts mean this is not a paired or sustained-performance estimate. Unflagged does not mean stall-free. No raw or predeclared paired analysis is changed.',
                          analyzer_sha256=sha(Path(__file__))),indent=2))


if __name__=='__main__':
    main()
