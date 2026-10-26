#!/usr/bin/env python3
"""Verify SSD vectored-DATA comparisons and fixed conditional guards."""
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
IDENTITY = json.loads((ROOT / 'rfc-025-v7-vectored-data-20261026-binaries.json').read_text())
CELLS = {
    'ext4-batch64': (750000,64,5,1176),
    'ext4-point8': (100000,None,5,1000),
    'ext4-point1': (20000,None,3,200),
    'ext4-point2': (20000,None,3,200),
    'ext4-point4': (50000,None,3,500),
}
ARMS = ('old_v4','new_v4','old_v7','new_v7')
PIPELINE = ('commit_groups','commit_buffers','commit_bytes','sync_count','fdatasync_ns','frontier_writes','frontier_bytes','frontier_write_ns','sync_coalesce_wait_calls','sync_coalesce_wait_ns','preallocation_ns','wal_sync_wait_ns','memtable_insert_ns')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cpu(r):
    return r['result']['process_cpu_user_ms'] + r['result']['process_cpu_system_ms']


def throughput(r):
    return r['result']['ops_per_sec']


def load(cell, phase):
    operations,batch,pair_count,samples = CELLS[cell]
    stem='rfc-025-v7-vectored-data-'+phase+'-'+cell+'-20261026'
    lines=(ROOT/(stem+'.jsonl')).read_bytes().splitlines()
    records={}
    for line in lines:
        r=json.loads(line)
        assert r['run_id'] not in records
        records[r['run_id']]=(r,hashlib.sha256(line).hexdigest())
    rows=list(csv.DictReader((ROOT/(stem+'-runs.tsv')).open(),delimiter='\t'))
    assert len(rows)==len(records)==len(lines)==4*(pair_count+1)
    assert {row['run_id'] for row in rows}==set(records)
    schedule={}
    for row in rows:
        key=(row['phase'],int(row['pair']),row['arm'])
        assert key not in schedule
        r,digest=records[row['run_id']]
        assert digest==row['raw_sha256']
        assert row['arm'] in ARMS
        assert row['binary_sha256']==IDENTITY['retained' if row['arm'].startswith('old') else 'candidate']['sha256']
        assert r['workload']==('wal_batch_concurrent' if batch else 'wal_concurrent')
        assert r['params']['threads']=={'ext4-point1':1,'ext4-point2':2,'ext4-point4':4}.get(cell,8)
        assert r['params']['value_size']==1024
        assert r['params']['batch_size']==batch
        assert r['params']['wal_io_mode']=='parallel'
        assert r['params']['wal_format_version']==(4 if row['arm'].endswith('v4') else 7)
        assert r['engine_options']['target_sst_size']==2147483648
        assert r['result']['ops']==operations
        assert r['latency']['samples']==samples
        schedule[key]=r
    assert set(schedule)=={('warmup',0,arm) for arm in ARMS}|{('scored',pair,arm) for pair in range(1,pair_count+1) for arm in ARMS}
    pairs=[{arm:schedule['scored',pair,arm] for arm in ARMS} for pair in range(1,pair_count+1)]
    return pairs, list(records), {
        suffix:sha(ROOT/(stem+suffix)) for suffix in ('.jsonl','-runs.tsv','-run.sh')
    }


def compare(cell, phase='initial'):
    pairs,ids,artifacts=load(cell,phase)
    changes=[]
    for index,p in enumerate(pairs,1):
        changes.append({
            'pair':index,
            'v7_throughput_change':throughput(p['new_v7'])/throughput(p['old_v7']),
            'v4_throughput_change':throughput(p['new_v4'])/throughput(p['old_v4']),
            'normalized_v7_v4_change':(throughput(p['new_v7'])/throughput(p['new_v4']))/(throughput(p['old_v7'])/throughput(p['old_v4'])),
            'v7_cpu_change':cpu(p['new_v7'])/cpu(p['old_v7']),
            'v7_p50_change':p['new_v7']['latency']['p50_ms']/p['old_v7']['latency']['p50_ms'],
            'v7_p99_change':p['new_v7']['latency']['p99_ms']/p['old_v7']['latency']['p99_ms'],
            'v7_write_request_change':p['new_v7']['result']['wal_pipeline']['commit_buffers']/p['old_v7']['result']['wal_pipeline']['commit_buffers'],
            'v7_sync_count_change':p['new_v7']['result']['wal_pipeline']['sync_count']/p['old_v7']['result']['wal_pipeline']['sync_count'],
            'v7_data_bytes_change':p['new_v7']['result']['wal_pipeline']['commit_bytes']/p['old_v7']['result']['wal_pipeline']['commit_bytes'],
        })
    stem='rfc-025-v7-vectored-data-'+phase+'-'+cell+'-20261026'
    rows=list(csv.DictReader((ROOT/(stem+'-runs.tsv')).open(),delimiter='\t'))
    audits={}
    for line in (ROOT/(stem+'-host.jsonl')).read_bytes().splitlines():
        item=json.loads(line)
        assert item['run_id'] not in audits
        audits[item['run_id']]=(item,hashlib.sha256(line).hexdigest())
    assert set(audits)=={r['run_id'] for r in rows}
    expected=[('warmup',0,arm) for arm in ('old_v4','old_v7','new_v4','new_v7')]
    for pair in range(1,CELLS[cell][2]+1):
        order=ARMS if pair%2 else tuple(reversed(ARMS))
        expected += [('scored',pair,arm) for arm in order]
    assert [(r['phase'],int(r['pair']),r['arm']) for r in rows]==expected
    samples=0
    for row in rows:
        item,digest=audits[row['run_id']]
        assert digest==row['audit_sha256']
        assert item['clean_sampled'] and row['isolation_status']=='clean_sampled'
        assert item['returncode']==0 and not item['sampling_errors'] and item['during']
        assert item['arm']==row['arm'] and item['pair']==int(row['pair']) and item['phase']==row['phase']
        assert item['binary_sha256']==row['binary_sha256']
        for sample in [item['before'],*item['during'],item['after']]:
            assert not sample['competing']
            samples+=1
    return {
        'host_samples':samples,'host_audit_sha256':sha(ROOT/(stem+'-host.jsonl')),
        'artifacts_sha256':artifacts,'run_ids':ids,'pairs':changes,
        'median_changes':{key:statistics.median(p[key] for p in changes) for key in changes[0] if key!='pair'},
        'per_arm':{
            arm:{
                'median_puts_per_sec':statistics.median(throughput(p[arm]) for p in pairs),
                'peak_puts_per_sec':max(throughput(p[arm]) for p in pairs),
                'median_wall_ms':statistics.median(p[arm]['result']['measure_elapsed_ms'] for p in pairs),
                'median_cpu_ms':statistics.median(cpu(p[arm]) for p in pairs),
                'median_p50_ms':statistics.median(p[arm]['latency']['p50_ms'] for p in pairs),
                'median_p99_ms':statistics.median(p[arm]['latency']['p99_ms'] for p in pairs),
                'pipeline_medians':{metric:statistics.median(p[arm]['result']['wal_pipeline'][metric] for p in pairs) for metric in PIPELINE},
            } for arm in ARMS
        },
        'paired_v7_v4':{
            label:statistics.median(throughput(p[prefix+'_v7'])/throughput(p[prefix+'_v4']) for p in pairs)
            for label,prefix in (('retained','old'),('candidate','new'))
        },
    }


def compare_pooled(cell):
    assert cell in ('ext4-point8','ext4-batch64')
    phases={phase:compare(cell,phase) for phase in ('initial','confirmation')}
    pairs=[]
    records=[]
    arms_data={arm:[] for arm in ARMS}
    ids=[]
    for phase,part in phases.items():
        pairs.extend(dict(capture_phase=phase,**pair) for pair in part['pairs'])
        ids.extend(part['run_ids'])
        stem='rfc-025-v7-vectored-data-'+phase+'-'+cell+'-20261026'
        raw={r['run_id']:r for r in map(json.loads,(ROOT/(stem+'.jsonl')).read_text().splitlines())}
        rows=list(csv.DictReader((ROOT/(stem+'-runs.tsv')).open(),delimiter='\t'))
        for i in range(1,6):
            record={row['arm']:raw[row['run_id']] for row in rows if row['phase']=='scored' and int(row['pair'])==i}
            assert set(record)==set(ARMS)
            records.append(record)
            for arm in ARMS:arms_data[arm].append(record[arm])
    assert len(pairs)==len(records)==10 and len(ids)==len(set(ids))==48
    return {
        'host_samples':sum(part['host_samples'] for part in phases.values()),
        'captures':{phase:{key:part[key] for key in ('host_samples','host_audit_sha256','artifacts_sha256')} for phase,part in phases.items()},
        'run_ids':ids,'pairs':pairs,
        'median_changes':{key:statistics.median(pair[key] for pair in pairs) for key in phases['initial']['median_changes']},
        'per_arm':{arm:{
            'median_puts_per_sec':statistics.median(throughput(r) for r in group),
            'peak_puts_per_sec':max(throughput(r) for r in group),
            'median_wall_ms':statistics.median(r['result']['measure_elapsed_ms'] for r in group),
            'median_cpu_ms':statistics.median(cpu(r) for r in group),
            'median_p50_ms':statistics.median(r['latency']['p50_ms'] for r in group),
            'median_p99_ms':statistics.median(r['latency']['p99_ms'] for r in group),
            'pipeline_medians':{metric:statistics.median(r['result']['wal_pipeline'][metric] for r in group) for metric in PIPELINE},
        } for arm,group in arms_data.items()},
        'paired_v7_v4':{label:statistics.median(throughput(p[prefix+'_v7'])/throughput(p[prefix+'_v4']) for p in records) for label,prefix in (('retained','old'),('candidate','new'))},
    }


if __name__=='__main__':
    cells=[arg for arg in sys.argv[1:] if not arg.startswith('--')] or ['ext4-batch64','ext4-point8']
    phase='confirmation' if '--confirmation' in sys.argv else 'initial'
    output={cell:(compare_pooled(cell) if '--pooled' in sys.argv else compare(cell,phase)) for cell in cells}
    ids=[i for cell in cells for i in output[cell]['run_ids']]
    assert len(ids)==len(set(ids))
    if {'ext4-batch64','ext4-point8'} <= set(cells):
        guards={}
        for cell in ('ext4-batch64','ext4-point8'):
            x=output[cell]['median_changes']
            arms=output[cell]['per_arm']
            guards[cell+'_paired_raw_floor']=x['v7_throughput_change']>=0.95
            guards[cell+'_paired_normalized_floor']=x['normalized_v7_v4_change']>=0.95
            guards[cell+'_arm_median_floor']=arms['new_v7']['median_puts_per_sec']/arms['old_v7']['median_puts_per_sec']>=0.95
            guards[cell+'_p99_ceiling']=x['v7_p99_change']<=1.10
            guards[cell+'_sync_count_ceiling']=x['v7_sync_count_change']<=1.05
            guards[cell+'_identical_data_bytes']=all(pair['v7_data_bytes_change']==1.0 for pair in output[cell]['pairs'])
        guards['ssd_material_gain']=any(
            output[cell]['median_changes']['v7_throughput_change']>=1.02
            and output[cell]['median_changes']['normalized_v7_v4_change']>=1.0
            for cell in ('ext4-batch64','ext4-point8')
        )
        guards['fewer_data_requests']=any(output[cell]['median_changes']['v7_write_request_change']<=0.95 for cell in ('ext4-batch64','ext4-point8'))
        guards['ssd_phase_pass']=all(guards.values())
        output['ssd_guards']=guards
    low_cells={'ext4-point1','ext4-point2','ext4-point4'}
    if low_cells <= set(cells):
        guards={}
        for cell in sorted(low_cells):
            x=output[cell]['median_changes']
            arms=output[cell]['per_arm']
            guards[cell+'_paired_raw_floor']=x['v7_throughput_change']>=0.95
            guards[cell+'_paired_normalized_floor']=x['normalized_v7_v4_change']>=0.95
            guards[cell+'_arm_median_floor']=arms['new_v7']['median_puts_per_sec']/arms['old_v7']['median_puts_per_sec']>=0.95
            guards[cell+'_p99_ceiling']=x['v7_p99_change']<=1.10
            guards[cell+'_identical_data_bytes']=all(pair['v7_data_bytes_change']==1.0 for pair in output[cell]['pairs'])
        guards['low_concurrency_phase_pass']=all(guards.values())
        output['low_concurrency_guards']=guards
    output['experiment_status']={
        'adopted':False,
        'reason':('Initial SSD phase passed; complete low-concurrency SSD guards and fixed SSD confirmation before retention.' if output['ssd_guards']['ssd_phase_pass'] else 'Reject: initial SSD phase failed; cancel conditional low-concurrency and confirmation captures and restore retained source and executable.') if 'ssd_guards' in output else 'Partial matrix; no retention decision.',
    }
    output['analysis_sha256']=sha(Path(__file__))
    output['driver_sha256']=sha(ROOT/'rfc-025-v7-vectored-data-20261026-run.py')
    output['identity_sha256']=sha(ROOT/'rfc-025-v7-vectored-data-20261026-binaries.json')
    print(json.dumps(output,indent=2))
