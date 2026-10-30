#!/usr/bin/env python3
"""Pool the fixed SSD confirmation with the original five batch pairs."""
import csv,hashlib,json,statistics
from pathlib import Path
ART=Path(__file__).resolve().parent
PREFIX='rfc-025-v7-large-group-hint-cache'
IDENTITY=json.loads((ART/(PREFIX+'-20261026-binaries.json')).read_text())
ARMS=('old_v4','new_v4','old_v7','new_v7')
PIPELINE=('commit_groups','commit_buffers','commit_bytes','sync_count','fdatasync_ns','frontier_writes','frontier_bytes','frontier_write_ns','sync_coalesce_wait_calls','sync_coalesce_wait_ns','preallocation_ns')

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def rate(r):return r['result']['ops_per_sec']
def cpu(r):return r['result']['process_cpu_user_ms']+r['result']['process_cpu_system_ms']
pairs=[];inputs={};ids=[]
for cohort in ('initial','confirmation'):
 stem=PREFIX+('-confirmation' if cohort=='confirmation' else '')+'-ext4-batch64-20261026'
 raw=ART/(stem+'.jsonl');mf=ART/(stem+'-runs.tsv')
 inputs[raw.name]=sha(raw);inputs[mf.name]=sha(mf)
 records={}
 for line in raw.read_bytes().splitlines():
  r=json.loads(line);assert r['run_id'] not in records
  records[r['run_id']]=(r,hashlib.sha256(line).hexdigest())
 rows=list(csv.DictReader(mf.open(),delimiter='\t'));assert len(rows)==len(records)==24
 schedule={}
 for row in rows:
  r,digest=records[row['run_id']];assert digest==row['raw_sha256']
  arm=row['arm'];key=(row['phase'],int(row['pair']),arm);assert key not in schedule and arm in ARMS
  assert row['binary_sha256']==IDENTITY['retained' if arm.startswith('old') else 'candidate']['sha256']
  assert r['params']['wal_format_version']==(4 if arm.endswith('v4') else 7)
  assert r['params']['wal_io_mode']=='parallel' and r['params']['threads']==8
  assert r['params']['value_size']==1024 and r['params']['batch_size']==64
  assert r['engine_options']['target_sst_size']==2147483648
  assert r['result']['ops']==750000 and r['latency']['samples']==1176
  schedule[key]=r;ids.append(r['run_id'])
 assert set(schedule)=={('warmup',0,a) for a in ARMS}|{('scored',p,a) for p in range(1,6) for a in ARMS}
 for p in range(1,6):pairs.append((cohort,p,{a:schedule['scored',p,a] for a in ARMS}))
assert len(ids)==len(set(ids))==48
changes=[]
for cohort,p,x in pairs:
 changes.append({'cohort':cohort,'pair':p,'v7_throughput_change':rate(x['new_v7'])/rate(x['old_v7']),
  'normalized_v7_v4_change':(rate(x['new_v7'])/rate(x['new_v4']))/(rate(x['old_v7'])/rate(x['old_v4'])),
  'v7_cpu_change':cpu(x['new_v7'])/cpu(x['old_v7']),
  'v7_p50_change':x['new_v7']['latency']['p50_ms']/x['old_v7']['latency']['p50_ms'],
  'v7_p99_change':x['new_v7']['latency']['p99_ms']/x['old_v7']['latency']['p99_ms']})
med={k:statistics.median(x[k] for x in changes) for k in changes[0] if k not in ('cohort','pair')}
per_arm={a:{'median_puts_per_sec':statistics.median(rate(x[a]) for _,_,x in pairs),
 'peak_puts_per_sec':max(rate(x[a]) for _,_,x in pairs),
 'median_cpu_ms':statistics.median(cpu(x[a]) for _,_,x in pairs),
 'median_p50_ms':statistics.median(x[a]['latency']['p50_ms'] for _,_,x in pairs),
 'median_p99_ms':statistics.median(x[a]['latency']['p99_ms'] for _,_,x in pairs),
 'pipeline_medians':{k:statistics.median(x[a]['result']['wal_pipeline'][k] for _,_,x in pairs) for k in PIPELINE}} for a in ARMS}
median_rate_ratio=per_arm['new_v7']['median_puts_per_sec']/per_arm['old_v7']['median_puts_per_sec']
original=json.loads((ART/(PREFIX+'-20261026-summary.json')).read_text())
guard={'original_guards':original['retention_guard']['keep'],'pooled_raw_loss_at_most_five_percent':med['v7_throughput_change']>=.95,
 'pooled_normalized_loss_at_most_five_percent':med['normalized_v7_v4_change']>=.95,'per_arm_median_loss_at_most_five_percent':median_rate_ratio>=.95}
guard['keep']=all(guard.values())
output={'pairs':changes,'median_changes':med,'per_arm':per_arm,'per_arm_median_rate_ratio':median_rate_ratio,
 'retention_extension':guard,'run_ids':ids,'input_sha256':inputs,
 'original_summary_sha256':sha(ART/(PREFIX+'-20261026-summary.json')),
 'confirmation_driver_sha256':sha(ART/(PREFIX+'-confirmation-20261026-run.py')),
 'analysis_sha256':sha(Path(__file__)),
 'limits':'Exactly one fixed confirmation, pooled with all original pairs and stalls. Paired medians and per-arm medians are separate statistics. This does not qualify the complete RFC gate.'}
print(json.dumps(output,indent=2))
