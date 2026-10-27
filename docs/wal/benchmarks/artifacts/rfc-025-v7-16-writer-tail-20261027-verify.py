#!/usr/bin/env python3
"""Reconstruct excluded diagnostic builds and verify preserved evidence/restoration."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-16-writer-tail-20261027'
BUF = 'rfc-025-v7-frontier-buffer-reuse-20261027'
SCALE = 'rfc-025-v7-writer-scaling-20261027'
TAIL = 'rfc-025-v7-clean-tail-20261027'
READY = 'rfc-025-v7-ready-tail-20261027'
COORD = 'rfc-025-v7-coordinator-tail-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-coordinator-tail'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(stem, suffix):
    return json.loads((ART/(stem+suffix)).read_text())


def main():
    # Verify the candidate comparison separately; it binds the prior 384 calls.
    buf_bytes = subprocess.check_output([sys.executable, str(ART/(BUF+'-verify.py'))], cwd=ROOT)
    assert buf_bytes == (ART/(BUF+'-verification.json')).read_bytes()
    buf = json.loads(buf_bytes)
    assert buf['unique_new_invocations']==32 and buf['global_unique_invocations']==416
    assert not buf['source_retained'] and not buf['assessment_passed']
    buf_summary = read(BUF,'-summary.json')
    scored_ids = {run for c in buf_summary.values() if isinstance(c,dict) and 'run_ids' in c for run in c['run_ids']}
    assert len(scored_ids)==32
    plan = read(BUF,'-plan.json')
    manifest = list(csv.DictReader((ART/(BUF+'-source-sha256.tsv')).open(), delimiter='\t'))
    original = {r['path']:subprocess.check_output(['git','show',plan['source_base']+':'+r['path']],cwd=ROOT)
                for r in manifest}
    assert len(original)==128
    scratch = WORK/'verification-source'
    assert not scratch.exists()
    builds_verified = []
    binaries = set()
    for family in [TAIL,READY,COORD]:
        identity = read(family,'-binaries.json')
        assert set(identity)=={'baseline','retirement_hint'}
        for arm,item in identity.items():
            assert sha(ROOT/item['path'])==item['sha256']
            binaries.add(item['sha256'])
            source_manifest=list(csv.DictReader((ART/(family+'-'+arm+'-source-sha256.tsv')).open(),delimiter='\t'))
            assert {r['path']for r in source_manifest}==set(original)
            try:
                scratch.mkdir()
                for path,data in original.items():
                    p=scratch/path;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
                patches=[]
                if arm=='retirement_hint':
                    patches.append(ART/'rfc-025-v7-batch-retirement-hint-20261027-candidate.patch')
                patches.append(ART/(TAIL+'-harness.patch'))
                if family!=TAIL:
                    patches.append(ART/(family+'-'+arm+'-stage.patch'))
                for patch in patches:
                    subprocess.run(['patch','--fuzz=0','--no-backup-if-mismatch','-p1','-i',str(patch)],
                                   cwd=scratch,check=True,capture_output=True)
                assert all(sha(scratch/r['path'])==r['sha256']for r in source_manifest), (family,arm)
                builds_verified.append(family+'/'+arm)
            finally:
                if scratch.exists():
                    shutil.rmtree(scratch)
        builds=read(family,'-builds.json')
        assert len(builds)==2
        for build in builds:
            assert build['exit_code']==0 and sha(ROOT/build['log'])==build['log_sha256']
    assert len(binaries)==6
    scale_env = read(SCALE,'-environment.json')
    library = ROOT/'target/wal-opt/20261027-v7-writer-scaling/io-trace.so'
    assert sha(library)==scale_env['trace_library_sha256']
    for suffix,key in [('-io-trace.c','trace_source_sha256'),('-run.py','driver_sha256'),('-plan.json','plan_sha256')]:
        assert sha(ART/(SCALE+suffix))==scale_env[key]
    diagnostics=set()
    samples=0
    for family,count in [(SCALE,8),(TAIL,4),(READY,2),(COORD,2)]:
        captures=read(family,'-captures.json')
        raw_lines=(ART/(family+'-runs.jsonl')).read_bytes().splitlines()
        assert len(captures)==len(raw_lines)==count
        for meta,line in zip(captures,raw_lines):
            assert meta['excluded_from_scoring'] and meta['clean_sampled'] and not meta['sampling_errors']
            assert meta['returncode']==0 and not meta['before']['competing'] and not meta['after']['competing']
            assert hashlib.sha256(line).hexdigest()==meta['raw_sha256']
            raw=json.loads(line)
            assert raw['run_id']==meta['run_id'] and raw['run_id'] not in diagnostics|scored_ids
            diagnostics.add(raw['run_id'])
            assert raw['result']['ops']==750000 and raw['params']['batch_size']==64
            assert raw['params']['wal_format_version']==7 and raw['params']['wal_io_mode']=='parallel'
            assert raw['params']['threads']==(meta['writers'] if family==SCALE else 16)
            assert sha(ROOT/meta['binary']['path'])==meta['binary']['sha256']
            assert meta.get('preload_library_sha256',meta.get('preload_sha256'))==sha(library)
            for path,digest in {**meta['files'],**meta.get('stage_files',{})}.items():
                assert sha(ROOT/path)==digest,path
            db=Path(meta['command'][meta['command'].index('--path')+1])
            assert not db.exists() and not Path(str(db)+'.pitr-repository').exists()
            if family!=SCALE:
                assert meta['mapped_tickets']==11728 and meta['trace_samples']==1184
                samples+=1184
        data=subprocess.check_output([sys.executable,str(ART/(family+'-analyze.py'))],cwd=ROOT)
        assert data==(ART/(family+'-summary.json')).read_bytes(),family
    probe=read(TAIL,'-probe.json')
    line=(ART/(TAIL+'-probe.jsonl')).read_bytes().strip()
    assert hashlib.sha256(line).hexdigest()==probe['raw_sha256']
    assert probe['excluded_from_scoring'] and probe['mapped_tickets']==80 and probe['trace_samples']==16
    assert probe['run_id'] not in diagnostics|scored_ids
    diagnostics.add(probe['run_id']);samples+=16
    for path,digest in probe['files'].items():
        assert sha(ROOT/path)==digest,path
    assert len(diagnostics)==17 and diagnostics.isdisjoint(scored_ids)
    # Explicitly rule out collisions with every preceding scored/warmup/profile
    # invocation, rather than just adding the previously verified count.
    old=set()
    families=['rfc-025-v7-ssd-sync-preparation-20261027','rfc-025-v7-ssd-two-chunk-lookahead-20261027',
              'rfc-025-v7-ssd-batch-window-20261027','rfc-025-v7-ssd-batch-window-followup-20261027',
              'rfc-025-v7-batch-membership-20261027','rfc-025-v7-batch-wait-budget-20261027',
              'rfc-025-v7-batch-retirement-hint-20261027']
    for family in families:
        for cell in read(family,'-summary.json').values():
            if isinstance(cell,dict) and 'run_ids'in cell:
                assert old.isdisjoint(cell['run_ids']);old.update(cell['run_ids'])
    solo=read('rfc-025-v7-ssd-batch-solo-control-20261027','-summary.json')['cell']['run_ids']
    assert old.isdisjoint(solo);old.update(solo)
    for family in [families[0],families[4]]:
        for line in (ART/(family+'-profiles.jsonl')).read_text().splitlines():
            run=json.loads(line)['run_id'];assert run not in old;old.add(run)
    assert len(old)==384 and old.isdisjoint(diagnostics|scored_ids)
    for attempt in read(READY,'-attempts.json'):
        assert sha(ROOT/attempt['log'])==attempt['log_sha256']
    for attempt in read(COORD,'-attempts.json'):
        for item in attempt['files']:
            assert sha(ROOT/item['path'])==item['sha256']
    assert all((ROOT/path).read_bytes()==data for path,data in original.items())
    restoration=read(BUF,'-restoration.json')
    assert restoration['source_mtimes_refreshed'] and restoration['tests']==1971
    assert sha(ROOT/'target/release/write-perf')==plan['baseline']['sha256']==restoration['canonical_sha256']
    print(json.dumps(dict(source_base=plan['source_base'],reconstructed_builds=builds_verified,inputs_per_build=128,
                          verified_diagnostic_binaries=6,trace_library_sha256=sha(library),
                          all_summaries_regenerate=True,all_capture_hashes_match=True,
                          excluded_diagnostic_invocations=len(diagnostics),mapped_api_samples=samples,
                          candidate_comparison_invocations=32,unique_new_invocations=49,
                          global_unique_invocations=len(old|scored_ids|diagnostics),
                          retained_source_restored=True,canonical_sha256=restoration['canonical_sha256'],
                          source_retained=False,tests=1971,cpu_is_diagnostic=True,
                          buffer_verification_sha256=sha(ART/(BUF+'-verification.json')),
                          verifier_sha256=sha(Path(__file__))),indent=2))


if __name__=='__main__':
    main()
