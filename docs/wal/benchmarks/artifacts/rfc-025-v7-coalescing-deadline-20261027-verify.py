#!/usr/bin/env python3
"""Verify the one-candidate retry-window comparison and exact baseline restoration."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-coalescing-deadline-20261027'
WORK = ROOT/'target/wal-opt/20261027-v7-coalescing-deadline'
CHANGED = 'kv-engine/src/wal/parallel/runtime.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART/(STEM+suffix)).read_text())


def main():
    plan, env, identity = read('-plan.json'), read('-environment.json'), read('-binaries.json')
    manifest = list(csv.DictReader((ART/(STEM+'-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest)==len({r['path']for r in manifest})==128
    scratch = WORK/'verification-source'
    assert not scratch.exists()
    original = {}
    try:
        scratch.mkdir()
        for row in manifest:
            data = subprocess.check_output(['git','show',plan['source_base']+':'+row['path']],cwd=ROOT)
            original[row['path']] = hashlib.sha256(data).hexdigest()
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
    assert ' ext4 ' in env['filesystem']
    for key,suffix in [('source_manifest_sha256','-source-sha256.tsv'),('patch_sha256','-candidate.patch'),
                       ('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),('driver_sha256','-run.py')]:
        assert env[key]==sha(ART/(STEM+suffix))
    prior_path=ART/plan['prior_verification']
    assert sha(prior_path)==env['prior_verification_sha256']
    prior=json.loads(prior_path.read_text())
    assert prior['global_unique_invocations']==plan['prior_global_invocations']==433
    assert prior['retained_source_restored'] and prior['canonical_sha256']==identity['baseline']['sha256']
    data=subprocess.check_output([sys.executable,str(ART/(STEM+'-analyze.py'))],cwd=ROOT)
    assert data==(ART/(STEM+'-summary.json')).read_bytes()
    summary=json.loads(data);status=summary['status'];matrix=read('-matrix.json')
    assert len(matrix['cells'])==4
    assert status['captured_cells']==['ext4-batch16','ext4-batch8']
    assert status['cancelled_cells']==['ext4-point8','ext4-batch1']
    assert summary['ext4-batch16']['v7_passed'] and not summary['ext4-batch8']['v7_passed']
    assert summary['ext4-batch16']['stall_filter']['matched_pairs']==[3,4,5]
    assert summary['ext4-batch8']['stall_filter']['matched_pairs']==[1,2,3]
    ids={run for cell in status['captured_cells']for run in summary[cell]['run_ids']}
    assert len(ids)==status['records']==20 and status['scored_records']==16 and status['warmup_records']==4
    # The earlier verification is a saved proof of its final live state. Bind
    # it above, and check current IDs against prior saved raw records directly.
    prior_ids=set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            record=json.loads(line)
            if isinstance(record,dict) and 'run_id'in record:
                prior_ids.add(record['run_id'])
    assert ids.isdisjoint(prior_ids)
    for cell in matrix['cells']:
        assert not (WORK/(cell+'-scratch')).exists()
        if cell in status['cancelled_cells']:
            assert not (ART/(STEM+'-'+cell+'.jsonl')).exists()
    decision=read('-decision.json')
    expected=status['all_planned_cells_complete'] and status['captured_cells_pass']
    assert decision['assessment_passed']==expected and not decision['source_retained'] and decision['cpu_is_diagnostic']
    assert decision['captured_cells']==status['captured_cells'] and decision['cancelled_cells']==status['cancelled_cells']
    assert read('-peaks.json')['cells']=={cell+'/'+part:c['per_arm']for cell in status['captured_cells']for part,c in summary[cell]['comparisons'].items()}
    checks=read('-checks.json');phases={c['phase']:c for c in checks['checks']}
    assert checks['outside_sandbox'] and checks['tests']==1972 and checks['focused_tests']==6 and checks['restored_tests']==1971
    assert {'focused','format','check','release','direct spelling','restored check','final spelling'}<=set(phases)
    for check in checks['checks']:
        assert check['exit_code']==0 and sha(ROOT/check['log'])==check['log_sha256']
    assert '6 tests run: 6 passed'in (ROOT/phases['focused']['log']).read_text()
    for phase,count in [('check',1972),('restored check',1971)]:
        log=(ROOT/phases[phase]['log']).read_text()
        assert f'{count} tests run: {count} passed'in log and '0 skipped'in log and 'error:'not in log
    assert 'Compiling kv-engine'in (ROOT/phases['restored check']['log']).read_text()
    assert '(1 flaky)'in (ROOT/phases['restored check']['log']).read_text() and checks['restored_flaky_tests']==1
    assert all(sha(ROOT/path)==digest for path,digest in original.items())
    restored=read('-restoration.json')
    assert restored['source_hashes'][CHANGED]==original[CHANGED] and restored['source_mtimes_refreshed']
    assert restored['tests']==1971 and restored['check_exit_code']==0
    assert sha(ROOT/'target/release/write-perf')==restored['canonical_sha256']==identity['baseline']['sha256']
    print(json.dumps(dict(source_base=plan['source_base'],reconstructed_inputs=128,changed_input=CHANGED,
                          verified_binaries=2,summary_regenerates=True,records=20,scored_records=16,warmup_records=4,
                          excluded_diagnostic_invocations=0,global_unique_invocations=433+len(ids),
                          stall_filter_fixed_before_capture=True,filtered_pairs={'batch16':3,'batch8':3},
                          tests=1972,focused_tests=6,restored_tests=1971,restored_flaky_tests=1,source_retained=False,
                          retained_source_restored=True,cpu_is_diagnostic=True,
                          canonical_sha256=restored['canonical_sha256'],
                          checks_sha256=sha(ART/(STEM+'-checks.json')),decision_sha256=sha(ART/(STEM+'-decision.json')),
                          verifier_sha256=sha(Path(__file__))),indent=2))


if __name__=='__main__':
    main()
