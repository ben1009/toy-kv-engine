#!/usr/bin/env python3
"""Two excluded readiness traces; every source variant has the same instrumentation."""
import csv
import hashlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parents[4]
ART=Path(__file__).resolve().parent
STEM='rfc-025-v7-coordinator-tail-20261027'
WORK=ROOT/'target/wal-opt/20261027-v7-coordinator-tail'
spec=importlib.util.spec_from_file_location('batch_capture',ART/'rfc-025-v7-clean-tail-20261027-run.py')
capture=importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)
capture.WORK=WORK
capture.STEM=STEM
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
identity=json.loads((ART/(STEM+'-binaries.json')).read_text())
assert not (ART/(STEM+'-captures.json')).exists()
items=[]
time.sleep(30)
with (ART/(STEM+'-runs.jsonl')).open('xb') as output:
    for i,arm in enumerate(['baseline','retirement_hint'],1):
        name=f'case{i}-{arm}'
        os.environ['KV_WAL_STAGE_TRACE']=str(WORK/(name+'-stage'))
        meta,raw=capture.run_case(arm,name,750000,identity)
        paths=sorted(WORK.glob(name+'-stage.*.csv'))
        data=[r for p in paths for r in csv.DictReader(p.open()) if '.data.'in p.name]
        frontiers=[r for p in paths for r in csv.DictReader(p.open()) if '.frontier.'in p.name and r['event']=='3']
        assert len([r for r in data if r['event']=='1'])==11728
        assert len([r for r in data if r['event']=='2'])==11728
        assert len(frontiers)==json.loads(raw)['result']['wal_pipeline']['frontier_writes']
        meta['stage_files']={str(p.relative_to(ROOT)):sha(p)for p in paths}
        meta['stage_event_counts']={'data_staging':11728,'data_reap':11728,'frontier_capture':len(frontiers)}
        items.append(meta)
        output.write(raw+b'\n')
        output.flush()
        (ART/(STEM+'-captures.json')).write_text(json.dumps(items,indent=2)+'\n')
        time.sleep(15)
