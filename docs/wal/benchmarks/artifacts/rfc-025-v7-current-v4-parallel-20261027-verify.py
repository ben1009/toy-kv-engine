#!/usr/bin/env python3
"""Verify the unchanged executable, fresh format captures and labelled peak view."""
import ast
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-current-v4-parallel-20261027'
WORK = ROOT/'target/wal-opt/20261027-v7-current-v4-parallel'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART/(STEM+suffix)).read_text())


def main():
    plan,env,identity=read('-plan.json'),read('-environment.json'),read('-binaries.json')
    assert identity['v4']==identity['v7']==plan['binary']
    assert sha(ROOT/identity['v4']['path'])==sha(ROOT/'target/release/write-perf')==identity['v4']['sha256']
    assert env['source_base']==plan['source_base'] and env['source_commit']==plan['source_commit']
    assert ' ext4 'in env['filesystem'] and not env['allocator_environment_overrides']
    for key,suffix in [('source_manifest_sha256','-source-sha256.tsv'),('binaries_sha256','-binaries.json'),
                       ('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),('driver_sha256','-run.py')]:
        assert env[key]==sha(ART/(STEM+suffix))
    manifest=list(csv.DictReader((ART/(STEM+'-source-sha256.tsv')).open(),delimiter='\t'))
    assert len(manifest)==len({r['path']for r in manifest})==128
    for row in manifest:
        assert sha(ROOT/row['path'])==row['sha256']
        original=subprocess.check_output(['git','show',plan['source_base']+':'+row['path']],cwd=ROOT)
        assert hashlib.sha256(original).hexdigest()==row['sha256']
    prior_path=ART/plan['prior_verification']
    assert sha(prior_path)==env['prior_verification_sha256']
    prior=json.loads(prior_path.read_text())
    assert prior['global_unique_invocations']==plan['prior_global_invocations']==489
    assert prior['source_retained'] and prior['source_commit']==plan['source_commit']
    assert prior['canonical_sha256']==identity['v4']['sha256']
    assert prior['tests']==1972 and prior['focused_tests']==6
    for suffix in ['-run.py','-child.py','-run-all.py','-analyze.py','-best.py','-verify.py']:
        ast.parse((ART/(STEM+suffix)).read_text())
    summary_bytes=subprocess.check_output([sys.executable,str(ART/(STEM+'-analyze.py'))],cwd=ROOT)
    assert summary_bytes==(ART/(STEM+'-summary.json')).read_bytes()
    summary=json.loads(summary_bytes);status=summary['status'];matrix=read('-matrix.json')
    assert status['all_planned_cells_complete'] and status['captured_cells']==list(matrix['cells'])
    assert not status['source_changed'] and status['comparison_only'] and not status['missing_cells']
    ids={run for cell in matrix['cells']for run in summary[cell]['run_ids']}
    assert len(ids)==status['records']==36 and status['scored_records']==28 and status['warmup_records']==8
    expected={'ext4-batch16':[1,2],'ext4-batch8':[],'ext4-point8':[1,2,3],'ext4-batch1':[3]}
    assert {c:summary[c]['stall_views']['combined']['matched_pairs']for c in matrix['cells']}==expected
    assert all(not (WORK/(cell+'-scratch')).exists()for cell in matrix['cells'])
    previous_ids=set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            record=json.loads(line)
            if isinstance(record,dict)and'run_id'in record:
                previous_ids.add(record['run_id'])
    assert ids.isdisjoint(previous_ids)
    best_bytes=subprocess.check_output([sys.executable,str(ART/(STEM+'-best.py'))],cwd=ROOT)
    assert best_bytes==(ART/(STEM+'-best.json')).read_bytes()
    best=json.loads(best_bytes)
    assert best['binary']==identity['v4']
    for cell,data in best['cells'].items():
        for arm in ['v4','v7']:
            peak=data[arm]['peak']
            assert peak and not peak['flagged'] and peak['binary_sha256']==identity['v4']['sha256']
            assert peak['cohort']in [STEM,'rfc-025-v7-eligible-coalescing-20261027']
        for sources in data['sources'].values():
            for path,digest in sources.items():
                assert sha(ROOT/path)==digest
    interpretation=read('-interpretation.json')
    assert interpretation['all_raw_runs_preserved'] and not interpretation['source_changed']
    assert not interpretation['parity_qualified'] and interpretation['retained_source_commit']==plan['source_commit']
    checks=read('-checks.json')
    assert checks['outside_sandbox'] and checks['reused_tests']==1972 and checks['reused_focused_tests']==6
    for check in checks['checks']:
        assert check['exit_code']==0 and sha(ROOT/check['log'])==check['log_sha256']
    print(json.dumps(dict(source_base=plan['source_base'],source_commit=plan['source_commit'],verified_unchanged_inputs=128,
                          identical_executable_for_both_formats=True,canonical_sha256=identity['v4']['sha256'],
                          summary_regenerates=True,best_view_regenerates=True,all_four_cells_complete=True,
                          records=36,scored_records=28,warmup_records=8,new_unique_invocations=36,
                          global_unique_invocations=489+len(ids),reused_tests=1972,reused_focused_tests=6,
                          source_changed=False,parity_qualified=False,
                          prior_verification_sha256=env['prior_verification_sha256'],
                          checks_sha256=sha(ART/(STEM+'-checks.json')),interpretation_sha256=sha(ART/(STEM+'-interpretation.json')),
                          verifier_sha256=sha(Path(__file__))),indent=2))


if __name__=='__main__':
    main()
