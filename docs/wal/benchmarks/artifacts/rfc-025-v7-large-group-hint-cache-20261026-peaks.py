#!/usr/bin/env python3
"""Keep the strongest scored references for the same longer ext4 cells."""
import csv,hashlib,json
from pathlib import Path
ART=Path(__file__).resolve().parent
LABELS=('prefix-hash-overlap','hash-scheduling','hash-pipeline','large-group-hint-cache','large-group-hint-cache-confirmation')
CELLS={'ext4-batch64':(750000,64,1176),'ext4-point8':(100000,None,1000)}
output={}
for cell,(operations,batch,samples) in CELLS.items():
 best={};inputs={}
 for label in LABELS:
  if label.endswith('confirmation') and cell!='ext4-batch64':continue
  stem='rfc-025-v7-'+label+'-'+cell+'-20261026'
  raw=ART/(stem+'.jsonl');manifest=ART/(stem+'-runs.tsv')
  inputs[raw.name]=hashlib.sha256(raw.read_bytes()).hexdigest()
  inputs[manifest.name]=hashlib.sha256(manifest.read_bytes()).hexdigest()
  records={}
  for line in raw.read_bytes().splitlines():
   r=json.loads(line);records[r['run_id']]=(r,hashlib.sha256(line).hexdigest())
  for row in csv.DictReader(manifest.open(),delimiter='\t'):
   r,sha=records[row['run_id']];assert sha==row['raw_sha256']
   assert r['params']['threads']==8 and r['params']['value_size']==1024
   assert r['params']['batch_size']==batch and r['result']['ops']==operations
   assert r['params']['wal_io_mode']=='parallel'
   assert r['engine_options']['target_sst_size']==2147483648
   assert r['latency']['samples']==samples
   if row['phase']!='scored':continue
   category='v4_control' if row['arm'].endswith('v4') else ('retained_v7' if row['arm']=='old_v7' else ('candidate_v7' if label in ('large-group-hint-cache','large-group-hint-cache-confirmation') else None))
   if category is None:continue
   rate=r['result']['ops_per_sec']
   if category not in best or rate>best[category]['puts_per_sec']:
    best[category]={'puts_per_sec':rate,'run_id':r['run_id'],'arm':row['arm'],'binary_sha256':row['binary_sha256'],'manifest':manifest.name,'raw':raw.name,'raw_record_sha256':sha}
 output[cell]={'peak_scored_references':best,'input_sha256':inputs}
output['limits']='Peaks are distinct from paired medians. Only the same longer operation counts, 2-GiB SST target, and sampling settings are included. Earlier 50K/100K-put, 1-GiB-target records remain separate references.'
output['analysis_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
print(json.dumps(output,indent=2))
