#!/usr/bin/env python3
"""Verify System/mimalloc raw metrics, host audit and both-format guards."""
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
IDENTITY = json.loads((ROOT / 'rfc-025-v7-mimalloc-20261026-binaries.json').read_text())
CELLS = {
    'tmpfs-batch64': (750000,64,3,1176),
    'tmpfs-point8': (100000,None,3,1000),
    'ext4-batch64': (750000,64,5,1176),
    'tmpfs-point1': (100000,None,3,1000),
    'ext4-point8': (100000,None,5,1000),
}
ARMS = ('old_v4','new_v4','old_v7','new_v7')
PIPELINE = ('commit_groups','commit_buffers','commit_bytes','sync_count','fdatasync_ns','frontier_writes','frontier_bytes','frontier_write_ns','sync_coalesce_wait_calls','sync_coalesce_wait_ns','preallocation_ns','wal_sync_wait_ns','memtable_insert_ns')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cpu(r):
    return r['result']['process_cpu_user_ms'] + r['result']['process_cpu_system_ms']


def throughput(r):
    return r['result']['ops_per_sec']


def load(cell):
    operations,batch,pair_count,samples = CELLS[cell]
    stem='rfc-025-v7-mimalloc-'+('confirmation' if '--confirmation' in sys.argv else 'initial')+'-'+cell+'-20261026'
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
        assert r['params']['threads']==(1 if cell.endswith('point1') else 8)
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


def compare(cell):
    pairs,ids,artifacts=load(cell)
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
        })
    stem='rfc-025-v7-mimalloc-'+('confirmation' if '--confirmation' in sys.argv else 'initial')+'-'+cell+'-20261026'
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


def allocator_details(cell):
    pairs, _, _ = load(cell)
    stem='rfc-025-v7-mimalloc-'+('confirmation' if '--confirmation' in sys.argv else 'initial')+'-'+cell+'-20261026'
    audits={r['run_id']:r for r in map(json.loads,(ROOT/(stem+'-host.jsonl')).read_text().splitlines())}
    details={}
    for version in ('v4','v7'):
        ratios=[]
        for index,pair in enumerate(pairs,1):
            old,new=pair['old_'+version],pair['new_'+version]
            old_rss=audits[old['run_id']]['child_rusage']['max_rss_kib']
            new_rss=audits[new['run_id']]['child_rusage']['max_rss_kib']
            assert old_rss>0 and new_rss>0
            ratios.append(dict(pair=index,throughput=throughput(new)/throughput(old),cpu=cpu(new)/cpu(old),p99=new['latency']['p99_ms']/old['latency']['p99_ms'],whole_child_peak_rss=new_rss/old_rss))
        changes={key:statistics.median(row[key] for row in ratios) for key in ratios[0] if key!='pair'}
        old_rates=[throughput(pair['old_'+version]) for pair in pairs]
        new_rates=[throughput(pair['new_'+version]) for pair in pairs]
        changes['arm_median_throughput']=statistics.median(new_rates)/statistics.median(old_rates)
        details[version]=dict(pairs=ratios,median_changes=changes,system_median=statistics.median(old_rates),mimalloc_median=statistics.median(new_rates),system_peak=max(old_rates),mimalloc_peak=max(new_rates))
    return details


if __name__=='__main__':
    cells=[arg for arg in sys.argv[1:] if not arg.startswith('--')] or list(CELLS)
    output={cell:compare(cell) for cell in cells}
    for cell in cells:output[cell]['allocator_comparison']=allocator_details(cell)
    guards={}
    for cell in cells:
        for version in ('v4','v7'):
            x=output[cell]['allocator_comparison'][version]['median_changes']
            guards[cell+'_'+version+'_raw_floor']=x['throughput']>=0.95
            guards[cell+'_'+version+'_arm_median_floor']=x['arm_median_throughput']>=0.95
            guards[cell+'_'+version+'_cpu_ceiling']=x['cpu']<=1.05
            if cell.startswith('ext4') or cell.endswith('point1'):
                guards[cell+'_'+version+'_p99_ceiling']=x['p99']<=1.10
    guards['material_gain']=any((x:=output[cell]['allocator_comparison'][version]['median_changes'])['throughput']>=1.02 and x['cpu']<=1.0 for cell in cells for version in ('v4','v7'))
    output['guards']=guards
    output['initial_phase_complete']=set(cells)==set(CELLS)
    output['initial_phase_pass']=output['initial_phase_complete'] and all(guards.values())
    output['rss_investigation_required']=[cell+'_'+version for cell in cells for version in ('v4','v7') if output[cell]['allocator_comparison'][version]['median_changes']['whole_child_peak_rss']>1.20]
    ids=[run_id for cell in cells for run_id in output[cell]['run_ids']]
    assert len(ids)==len(set(ids))
    output['unique_run_count']=len(ids)
    output['analysis_sha256']=sha(Path(__file__))
    output['driver_sha256']=sha(ROOT/'rfc-025-v7-mimalloc-20261026-run.py')
    output['plan_sha256']=sha(ROOT/'rfc-025-v7-mimalloc-20261026-plan.json')
    output['adopted']=False
    print(json.dumps(output,indent=2))
