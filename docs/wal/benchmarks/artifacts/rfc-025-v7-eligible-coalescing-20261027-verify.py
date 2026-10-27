#!/usr/bin/env python3
"""Verify exact retained inputs and the fixed eligibility/coalescing comparison."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-eligible-coalescing-20261027'
WORK = ROOT/'target/wal-opt/20261027-v7-eligible-coalescing'
CHANGED = 'kv-engine/src/wal/parallel/runtime.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART/(STEM+suffix)).read_text())


def main():
    plan, env, identity = read('-plan.json'), read('-environment.json'), read('-binaries.json')
    manifest = list(csv.DictReader((ART/(STEM+'-source-sha256.tsv')).open(),delimiter='\t'))
    assert len(manifest)==len({r['path']for r in manifest})==128
    scratch=WORK/'verification-source'
    assert not scratch.exists()
    original={}
    try:
        scratch.mkdir()
        for row in manifest:
            data=subprocess.check_output(['git','show',plan['source_base']+':'+row['path']],cwd=ROOT)
            original[row['path']]=hashlib.sha256(data).hexdigest()
            p=scratch/row['path'];p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
        subprocess.run(['patch','--fuzz=0','-p1','-i',str(ART/(STEM+'-candidate.patch'))],
                       cwd=scratch,check=True,capture_output=True)
        assert all(sha(scratch/r['path'])==r['sha256']for r in manifest)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    assert [r['path']for r in manifest if r['sha256']!=original[r['path']]]==[CHANGED]
    assert identity['baseline']==plan['baseline']
    assert all(sha(ROOT/item['path'])==item['sha256']for item in identity.values())
    assert env['source_base']==plan['source_base'] and not env['allocator_environment_overrides']
    assert ' ext4 'in env['filesystem']
    for key,suffix in [('source_manifest_sha256','-source-sha256.tsv'),('patch_sha256','-candidate.patch'),
                       ('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),('driver_sha256','-run.py')]:
        assert env[key]==sha(ART/(STEM+suffix))
    prior_path=ART/plan['prior_verification']
    assert sha(prior_path)==env['prior_verification_sha256']
    prior=json.loads(prior_path.read_text())
    assert prior['global_unique_invocations']==plan['prior_global_invocations']==453
    assert prior['retained_source_restored'] and prior['canonical_sha256']==identity['baseline']['sha256']
    data=subprocess.check_output([sys.executable,str(ART/(STEM+'-analyze.py'))],cwd=ROOT)
    assert data==(ART/(STEM+'-summary.json')).read_bytes()
    summary=json.loads(data);status=summary['status'];matrix=read('-matrix.json')
    assert len(matrix['cells'])==4 and status['captured_cells']==list(matrix['cells'])
    assert status['all_planned_cells_complete'] and status['captured_cells_pass'] and not status['cancelled_cells']
    assert summary['ext4-batch16']['stall_filter']['matched_pairs']==[1,4,5]
    assert all(summary[cell]['stall_filter']['matched_pairs']==[1,2,3]for cell in matrix['followup'])
    ids={run for cell in status['captured_cells']for run in summary[cell]['run_ids']}
    assert len(ids)==status['records']==36 and status['scored_records']==28 and status['warmup_records']==8
    prior_ids=set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            record=json.loads(line)
            if isinstance(record,dict)and'run_id'in record:
                prior_ids.add(record['run_id'])
    assert ids.isdisjoint(prior_ids)
    assert all(not (WORK/(cell+'-scratch')).exists()for cell in matrix['cells'])
    decision=read('-decision.json')
    assert decision['assessment_passed'] and decision['source_retained'] and decision['cpu_is_diagnostic']
    assert decision['captured_cells']==status['captured_cells'] and not decision['cancelled_cells']
    assert read('-peaks.json')['cells']=={cell+'/'+part:c['per_arm']for cell in status['captured_cells']for part,c in summary[cell]['comparisons'].items()}
    checks=read('-checks.json');phases={c['phase']:c for c in checks['checks']}
    assert checks['outside_sandbox'] and checks['tests']==1972 and checks['focused_tests']==6
    assert {'focused','format','check','release','direct spelling','final spelling'}<=set(phases)
    for check in checks['checks']:
        assert check['exit_code']==0 and sha(ROOT/check['log'])==check['log_sha256']
    assert '6 tests run: 6 passed'in (ROOT/phases['focused']['log']).read_text()
    log=(ROOT/phases['check']['log']).read_text()
    assert '1972 tests run: 1972 passed, 0 skipped'in log and 'error:'not in log and 'FLAKY'not in log
    assert all(sha(ROOT/r['path'])==r['sha256']for r in manifest)
    commit=decision['source_commit']
    committed=subprocess.check_output(['git','show',commit+':'+CHANGED],cwd=ROOT)
    assert hashlib.sha256(committed).hexdigest()==sha(ROOT/CHANGED)
    assert subprocess.check_output(['git','rev-parse',commit+'^'],cwd=ROOT,text=True).strip()==plan['source_base']
    assert subprocess.check_output(['git','diff-tree','--no-commit-id','--name-only','-r',commit],cwd=ROOT,text=True).splitlines()==[CHANGED]
    assert sha(ROOT/'target/release/write-perf')==identity['candidate']['sha256']
    print(json.dumps(dict(source_base=plan['source_base'],source_commit=commit,reconstructed_inputs=128,
                          changed_input=CHANGED,verified_binaries=2,summary_regenerates=True,
                          records=36,scored_records=28,warmup_records=8,excluded_diagnostic_invocations=0,
                          global_unique_invocations=453+len(ids),all_four_stages_pass=True,
                          filtered_pairs={cell:len(summary[cell]['stall_filter']['matched_pairs'])for cell in matrix['cells']},
                          tests=1972,focused_tests=6,source_retained=True,cpu_is_diagnostic=True,
                          canonical_sha256=identity['candidate']['sha256'],
                          checks_sha256=sha(ART/(STEM+'-checks.json')),decision_sha256=sha(ART/(STEM+'-decision.json')),
                          verifier_sha256=sha(Path(__file__))),indent=2))


if __name__=='__main__':
    main()
