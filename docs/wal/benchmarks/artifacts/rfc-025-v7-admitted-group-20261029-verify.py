#!/usr/bin/env python3
"""Verify admitted-window source, captures, restoration and matching-binary scores."""
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[4]
ART=Path(__file__).resolve().parent
WORK=ROOT/'target/wal-opt/20261029-v7-admitted-group'
STEM='rfc-025-v7-admitted-group-20261029'
OLD='rfc-025-v7-admission-grouping-20261029'
COOL='rfc-025-v7-admitted-group-cool-20261029'
PRODUCTION={'kv-engine/src/chaos/failpoint.rs','kv-engine/src/mem_table.rs','kv-engine/src/mvcc.rs','kv-engine/src/wal.rs','kv-engine/src/wal/parallel/runtime.rs'}
sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
def read(suffix):return json.loads((ART/(STEM+suffix)).read_text())
def rows(suffix):return list(csv.DictReader((ART/(STEM+suffix)).open(),delimiter='\t'))
def regenerate(script,output):
 data=subprocess.check_output([sys.executable,str(ART/(STEM+script))],cwd=ROOT)
 assert data==(ART/(STEM+output)).read_bytes()
 return json.loads(data)

def main():
 plan,env,outcome,pre=read('-plan.json'),read('-environment.json'),read('-outcome.json'),read('-preflight.json')
 parent=ART/(OLD+'-verification.json')
 assert pre['parent_verified'] and parent.read_bytes()==(WORK/'parent-verification.json').read_bytes()
 previous=json.loads(parent.read_text())
 assert pre['head']==plan['source_base']=='59becf2791cfcefb91dee80f20af6157215c392c'
 before,after=rows('-baseline-sha256.tsv'),rows('-source-sha256.tsv')
 assert len(before)==len(after)==128
 base={r['path']:r['sha256'] for r in before}
 assert {r['path'] for r in after if r['sha256']!=base[r['path']]}==PRODUCTION
 prior=list(csv.DictReader((ART/(OLD+'-source-sha256.tsv')).open(),delimiter='\t'))
 prior_map={r['path']:r['sha256'] for r in prior}
 assert {r['path'] for r in after if r['sha256']!=prior_map[r['path']]}=={'kv-engine/src/wal/parallel/runtime.rs'}
 scratch=WORK/'verify-source';assert not scratch.exists()
 try:
  scratch.mkdir()
  for row in before:
   data=subprocess.check_output(['git','show',plan['source_base']+':'+row['path']],cwd=ROOT)
   assert hashlib.sha256(data).hexdigest()==row['sha256']
   target=scratch/row['path'];target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
  subprocess.run(['patch','--fuzz=0','-p1','-i',str(ART/(STEM+'-candidate.patch'))],cwd=scratch,check=True,capture_output=True)
  assert all(sha(scratch/r['path'])==r['sha256'] for r in after)
 finally:
  if scratch.exists():shutil.rmtree(scratch)
 live=after if outcome['source_retained'] else before
 assert all(sha(ROOT/r['path'])==r['sha256'] for r in live)
 identity=read('-binaries.json');assert identity['retained-v7']==plan['baseline']
 assert all(sha(ROOT/x['path'])==x['sha256'] for x in identity.values())
 expected=identity['candidate-v7' if outcome['source_retained'] else 'retained-v7']['sha256']
 assert sha(ROOT/'target/release/write-perf')==outcome['canonical_sha256']==expected
 assert ' ext4 ' in env['filesystem']
 for key,suffix in [('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),('driver_sha256','-run.py'),('child_sha256','-child.py'),('analyzer_sha256','-analyze.py'),('orchestrator_sha256','-run-all.py'),('source_manifest_sha256','-source-sha256.tsv'),('patch_sha256','-candidate.patch'),('checks_sha256','-checks.json')]:
  assert env[key]==sha(ART/(STEM+suffix))
 summary=regenerate('-analyze.py','-summary.json');best=regenerate('-best.py','-best.json')
 assert summary['screens_complete'] and not summary['screen_passed'] and not summary['guards_complete']
 assert summary['missing_cells']==plan['guard_cells'] and summary['invocations']==36 and summary['scored_records']==27
 cool_plan=json.loads((ART/(COOL+'-plan.json')).read_text())
 cool_env=json.loads((ART/(COOL+'-environment.json')).read_text())
 assert cool_plan['trigger']['primary_summary_sha256']==sha(ART/(STEM+'-summary.json'))
 assert all(x['p99_ms']>=8 and x['frontier_mean_ms']>=.12 for x in cool_plan['trigger']['candidate_batch8_samples'])
 assert json.loads((ART/(COOL+'-binaries.json')).read_text())==identity
 assert cool_env['parent_source_manifest_sha256']==sha(ART/(STEM+'-source-sha256.tsv'))
 assert cool_env['parent_checks_sha256']==sha(ART/(STEM+'-checks.json'))
 for key,suffix in [('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),('driver_sha256','-run.py'),('child_sha256','-child.py'),('analyzer_sha256','-analyze.py'),('orchestrator_sha256','-run-all.py')]:
  assert cool_env[key]==sha(ART/(COOL+suffix))
 cool_data=subprocess.check_output([sys.executable,str(ART/(COOL+'-analyze.py'))],cwd=ROOT)
 assert cool_data==(ART/(COOL+'-summary.json')).read_bytes();cool=json.loads(cool_data)
 cool_best=subprocess.check_output([sys.executable,str(ART/(COOL+'-best.py'))],cwd=ROOT)
 assert cool_best==(ART/(COOL+'-best.json')).read_bytes()
 assert cool['screens_complete']
 if cool['screen_passed']:
  assert cool['guards_complete'] and not cool['missing_cells'] and cool['invocations']==36 and cool['scored_records']==27
 else:
  assert not cool['guards_complete'] and cool['missing_cells']==cool_plan['guard_cells'] and cool['invocations']==12 and cool['scored_records']==9
 assert outcome['source_retained']==(summary['cells']['ext4-batch1']['passed'] and summary['cells']['ext4-batch16']['passed'] and cool['retention_passed'])
 first_ids={rid for cell in summary['cells'].values() for rid in cell['run_ids']};cool_ids={rid for cell in cool['cells'].values() for rid in cell['run_ids']}
 assert len(first_ids)==36 and len(cool_ids)==cool['invocations'] and first_ids.isdisjoint(cool_ids)
 ids=first_ids|cool_ids
 previous_ids=set()
 for path in ART.glob('rfc-025-v7-*.jsonl'):
  if path.name.startswith(STEM) or path.name.startswith(COOL):continue
  for line in path.read_text().splitlines():
   record=json.loads(line)
   if isinstance(record,dict) and 'run_id' in record:previous_ids.add(record['run_id'])
 assert ids.isdisjoint(previous_ids) and len(previous_ids)>=previous['full_archive_unique_invocations']
 checks=read('-checks.json');assert checks['outside_sandbox'] and checks['full_tests']==1986 and checks['focused_tests']==66
 for item in checks['logs'].values():assert sha(ROOT/item['path'])==item['sha256']
 assert (ROOT/checks['logs']['race-stress.log']['path']).read_text().count('6 tests run: 6 passed')==10
 full=(ROOT/checks['logs']['full-check.log']['path']).read_text()
 assert re.search(r'1986 tests run: 1986 passed(?: \([^)]*\))?, 0 skipped',full) and 'FLAKY' not in full and 'error:' not in full
 if not outcome['source_retained']:
  assert outcome['rust_mtimes_refreshed'] and outcome['atomic_binary_replacement']
  for check in outcome['restored_checks']:assert check['returncode']==0 and sha(ROOT/check['log_path'])==check['log_sha256']
  restored=next((ROOT/c['log_path']).read_text() for c in outcome['restored_checks'] if c['step']=='restored-check')
  assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped',restored) and 'FLAKY' not in restored and 'error:' not in restored
 assert all(not (WORK/(cell+'-scratch')).exists() for cell in read('-matrix.json')['cells'])
 assert all(not ((ROOT/'target/wal-opt/20261029-v7-admitted-group-cool')/(cell+'-scratch')).exists() for cell in json.loads((ART/(COOL+'-matrix.json')).read_text())['cells'])
 print(json.dumps(dict(source_base=plan['source_base'],reconstructed_inputs=128,live_inputs_verified=128,source_retained=outcome['source_retained'],new_completed_invocations=len(ids),scored_records=summary['scored_records']+cool['scored_records'],global_unique_invocations=previous['global_unique_invocations']+len(ids),full_archive_unique_invocations=previous['full_archive_unique_invocations']+len(ids),inherited_interrupted_without_json=1,screens_passed=cool['screen_passed'],guards_complete=cool['guards_complete'],best_scores_verified=True,summaries_regenerate=True,candidate_tests=1986,focused_tests=66,restored_tests=1973 if not outcome['source_retained'] else None,canonical_sha256=expected,source_patch_sha256=env['patch_sha256'],source_manifest_sha256=env['source_manifest_sha256'],checks_sha256=env['checks_sha256'],summary_sha256=sha(ART/(STEM+'-summary.json')),best_sha256=sha(ART/(STEM+'-best.json')),cool_summary_sha256=sha(ART/(COOL+'-summary.json')),cool_best_sha256=sha(ART/(COOL+'-best.json')),outcome_sha256=sha(ART/(STEM+'-outcome.json')),verifier_sha256=sha(Path(__file__))),indent=2))
if __name__=='__main__':main()
