#!/usr/bin/env python3
"""Verify complete four-arm full durability-cycle feedback comparisons, controls, and hashes."""
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
IDENTITY = json.loads((ROOT / 'rfc-025-v7-durability-cycle-cost-20261026-binaries.json').read_text())
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
    stem='rfc-025-v7-durability-cycle-cost-'+cell+'-20261026'
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
    return {
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
                'pipeline_medians':{metric:statistics.median(p[arm]['result']['wal_pipeline'][metric] for p in pairs) for metric in PIPELINE + (('v7_durability_cycle_ns','v7_durability_cycle_promotions') if arm == 'new_v7' else ())},
            } for arm in ARMS
        },
        'candidate_cycle_diagnostics': {
            'runs': [
                {
                    'run_id':p['new_v7']['run_id'],
                    'sync_count':(c:=p['new_v7']['result']['wal_pipeline'])['sync_count'],
                    'promoted_cycles':c['v7_durability_cycle_promotions'],
                    'promoted_fraction':c['v7_durability_cycle_promotions']/c['sync_count'],
                    'mean_cycle_us':c['v7_durability_cycle_ns']/c['sync_count']/1000,
                    'mean_fdatasync_us':c['fdatasync_ns']/c['sync_count']/1000,
                    'mean_frontier_write_us':c['frontier_write_ns']/c['sync_count']/1000,
                    'other_cycle_ms':(c['v7_durability_cycle_ns']-c['fdatasync_ns']-c['frontier_write_ns'])/1e6,
                } for p in pairs
            ],
        },
        'paired_v7_v4':{
            label:statistics.median(throughput(p[prefix+'_v7'])/throughput(p[prefix+'_v4']) for p in pairs)
            for label,prefix in (('retained','old'),('candidate','new'))
        },
    }


if __name__=='__main__':
    cells=sys.argv[1:] or ['tmpfs-batch64','tmpfs-point8']
    output={cell:compare(cell) for cell in cells}
    ids=[i for cell in cells for i in output[cell]['run_ids']]
    assert len(ids)==len(set(ids))
    if set(cells)==set(CELLS):
        guards={}
        for cell in CELLS:
            x=output[cell]['median_changes']
            arms=output[cell]['per_arm']
            guards[cell+'_paired_raw_floor']=x['v7_throughput_change']>=0.95
            guards[cell+'_paired_normalized_floor']=x['normalized_v7_v4_change']>=0.95
            guards[cell+'_arm_median_floor']=arms['new_v7']['median_puts_per_sec']/arms['old_v7']['median_puts_per_sec']>=0.95
            if cell.startswith('tmpfs'):
                guards[cell+'_cpu_ceiling']=x['v7_cpu_change']<=1.05
            if cell in ('ext4-batch64','ext4-point8','tmpfs-point1'):
                guards[cell+'_p99_ceiling']=x['v7_p99_change']<=1.10
        guards['ssd_material_gain']=any(
            output[cell]['median_changes']['v7_throughput_change']>=1.02
            and output[cell]['median_changes']['normalized_v7_v4_change']>=1.0
            for cell in ('ext4-batch64','ext4-point8')
        )
        guards['initial_pass']=all(guards.values())
        output['initial_guards']=guards
    output['experiment_status']={
        'adopted':False,
        'reason':('Initial guards passed; fixed SSD confirmation is required.' if output['initial_guards']['initial_pass'] else 'Rejected: fixed initial guards failed; restore the retained source and executable.') if 'initial_guards' in output else 'Partial matrix; no retention decision.',
        'failed_initial_guards':[key for key,value in output.get('initial_guards',{}).items() if not value and key!='initial_pass'],
    }
    output['analysis_sha256']=sha(Path(__file__))
    output['driver_sha256']=sha(ROOT/'rfc-025-v7-durability-cycle-cost-20261026-run.py')
    output['identity_sha256']=sha(ROOT/'rfc-025-v7-durability-cycle-cost-20261026-binaries.json')
    print(json.dumps(output,indent=2))
