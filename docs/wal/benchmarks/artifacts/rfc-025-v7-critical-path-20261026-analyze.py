#!/usr/bin/env python3
"""Extract sampled thread CPU evidence and reproduce the critical-path summary."""
import collections
import csv
import gzip
import hashlib
import io
import json
import re
import sys
from pathlib import Path

ART=Path(__file__).resolve().parent
ROOT=ART.parents[3]
WORK=ROOT/'target/wal-opt/20261026-v7-scheduling-diagnosis'
STEM='rfc-025-v7-critical-path-20261026'
CELLS=('v4-batch64','v7-batch64','v7-point8')
HEADER=re.compile(r'\s*(.*?)\s+(\d+)/(\d+)\s+(\d+)\s*$')


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def extract():
    for cell in CELLS:
        source=WORK/(cell+'-samples.txt')
        output=io.StringIO()
        writer=csv.writer(output,delimiter='\t',lineterminator='\n')
        writer.writerow(['comm','pid','tid','period_ns','leaf_symbol'])
        for block in source.read_text().split('\n\n'):
            lines=block.splitlines()
            if not lines:continue
            m=HEADER.fullmatch(lines[0])
            if not m:continue
            leaf='[unavailable]'
            for line in lines[1:]:
                symbol=re.match(r'\s*[0-9a-fA-F]+\s+(.+)',line)
                if symbol:
                    leaf=symbol[1]
                    break
            writer.writerow([m[1],m[2],m[3],m[4],leaf])
        path=ART/(STEM+'-'+cell+'-samples.tsv.gz')
        path.write_bytes(gzip.compress(output.getvalue().encode(),mtime=0))
    metadata={cell:{'perf_data_sha256':sha(WORK/(cell+'.data')),'script_fields':'comm,pid,tid,period,ip,sym'} for cell in CELLS}
    (ART/(STEM+'-capture.json')).write_text(json.dumps(metadata,indent=2)+'\n')


def summarize():
    raw_lines=(ART/(STEM+'.jsonl')).read_bytes().splitlines()
    rows=list(csv.DictReader((ART/(STEM+'-runs.tsv')).open(),delimiter='\t'))
    assert len(rows)==len(raw_lines)==3
    output={}
    for row,line in zip(rows,raw_lines):
        record=json.loads(line)
        assert hashlib.sha256(line).hexdigest()==row['raw_sha256']
        assert record['run_id']==row['run_id']
        assert row['binary_sha256']=='b2de5e1c73fdf8abccca325dd0ebd4f65bc3dac96dd2b3533df6575204be248d'
        cell=row['cell']
        path=ART/(STEM+'-'+cell+'-samples.tsv.gz')
        samples=list(csv.DictReader(io.StringIO(gzip.decompress(path.read_bytes()).decode()),delimiter='\t'))
        periods=collections.defaultdict(int)
        sha_period=0
        for sample in samples:
            period=int(sample['period_ns'])
            periods[sample['comm']]+=period
            if sample['comm']=='wal-ordered-pac' and sample['leaf_symbol'].startswith('sha2::sha256::x86::digest_blocks'):
                sha_period+=period
        output[cell]={
            'run_id':record['run_id'],
            'measure_elapsed_ms':record['result']['measure_elapsed_ms'],
            'puts_per_sec':record['result']['ops_per_sec'],
            'timed_process_cpu_ms':record['result']['process_cpu_user_ms']+record['result']['process_cpu_system_ms'],
            'samples':len(samples),
            'sampled_user_cpu_ms_by_comm':{comm:n/1e6 for comm,n in sorted(periods.items())},
            'sampled_packer_sha_self_ms':sha_period/1e6,
            'packer_sha_fraction_of_sampled_packer_user_cpu':sha_period/periods['wal-ordered-pac'],
            'samples_sha256':sha(path),
        }
    output['sampling_limits']='Unscored whole-process cpu-clock:u samples; user CPU period estimates include setup/close. Packer SHA is identified by thread and leaf symbol; it is not total process CPU or a percentage of end-to-end wall time.'
    output['artifacts_sha256']={suffix:sha(ART/(STEM+suffix)) for suffix in ('.jsonl','-runs.tsv','-profile.py','-capture.json')}
    output['analysis_sha256']=sha(Path(__file__))
    print(json.dumps(output,indent=2))


if __name__=='__main__':
    if sys.argv[1:]==['--extract']:extract()
    else:summarize()
