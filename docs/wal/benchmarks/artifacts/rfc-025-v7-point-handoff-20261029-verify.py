#!/usr/bin/env python3
"""Verify the historical diagnostic bundle independently of later source changes."""
import csv,hashlib,json,re,shutil,subprocess,sys
from pathlib import Path
R=Path(__file__).resolve().parents[4];A=Path(__file__).resolve().parent;N='rfc-025-v7-point-handoff-20261029';W=R/'target/wal-opt/20261029-v7-point-handoff';sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def read(x):return json.loads((A/(N+x)).read_text())
p,e,b=read('-plan.json'),read('-environment.json'),read('-binaries.json');base=list(csv.DictReader((A/(N+'-baseline-sha256.tsv')).open(),delimiter='\t'));candidate=list(csv.DictReader((A/(N+'-source-sha256.tsv')).open(),delimiter='\t'));before={r['path']:r['sha256']for r in base};assert len(base)==len(candidate)==128
changed={r['path']for r in candidate if r['sha256']!=before[r['path']]};assert changed=={'kv-engine/src/wal.rs','kv-engine/src/wal/parallel/runtime.rs','kv-engine/src/wal/parallel/worker.rs','kv-engine/src/bin/write-perf.rs'}
scratch=W/'verify-source';assert not scratch.exists()
try:
 scratch.mkdir()
 for row in base:
  data=subprocess.check_output(['git','show',p['source_base']+':'+row['path']],cwd=R);assert hashlib.sha256(data).hexdigest()==row['sha256'];q=scratch/row['path'];q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes(data)
 subprocess.run(['patch','--fuzz=0','-p1','-i',str(A/(N+'-instrumentation.patch'))],cwd=scratch,check=True,capture_output=True)
 assert all(sha(scratch/r['path'])==r['sha256']for r in candidate)
finally:
 if scratch.exists():shutil.rmtree(scratch)
for k,suffix in [('driver_sha256','-run.py'),('plan_sha256','-plan.json'),('patch_sha256','-instrumentation.patch'),('source_manifest_sha256','-source-sha256.tsv')]:assert e[k]==sha(A/(N+suffix))
assert sha(A/p['prior_verification'])==e['prior_verification_sha256'];prior=json.loads((A/p['prior_verification']).read_text());assert prior['global_unique_invocations']==p['prior_global_invocations']==803
old=p['prior_verification'].removesuffix('-verification.json')
for k,suffix in [('checks_sha256','-checks.json'),('decision_sha256','-decision.json'),('verifier_sha256','-verify.py')]:assert prior[k]==sha(A/(old+suffix))
assert b['baseline']==p['baseline'] and all(sha(R/i['path'])==i['sha256']for i in b.values())
checks=read('-checks.json')
for c in checks['checks']:assert c['exit_code']==0 and sha(R/c['log'])==c['log_sha256']
full=(W/'check.log').read_text();assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped',full)and'FLAKY'not in full
captures=read('-captures.json');raw=[json.loads(l)for l in(A/(N+'-runs.jsonl')).read_text().splitlines()];assert len(captures)==len(raw)==2
for c,r,case in zip(captures,raw,p['schedule']):
 assert c['run_id']==r['run_id']and c['operations']==r['result']['ops']==case['operations']and c['writers']==r['params']['threads']==case['writers']
 assert c['excluded_from_scoring']and c['clean_sampled']and c['returncode']==0 and not c['sampling_errors']
 assert c['binary']==b['diagnostic']
 for name,h in c['files'].items():assert sha(R/name)==h
 samples=[json.loads(l)for l in(R/next(x for x in c['files']if x.endswith('-samples.jsonl'))).read_text().splitlines()]
 assert not any(s['competing']for s in[c['before'],*[s['host_audit']for s in samples if'host_audit'in s],c['after']])
 assert json.loads((R/next(x for x in c['files']if x.endswith('-events.meta.json'))).read_text())==dict(events=c['events'],dropped=0)
output=subprocess.check_output([sys.executable,str(A/(N+'-analyze.py'))],cwd=R);assert output==(A/(N+'-summary.json')).read_bytes();summary=json.loads(output)
restored=read('-restoration.json');assert restored['restored_source_hashes']==before and restored['canonical_sha256']==b['baseline']['sha256'];assert restored['summary_sha256']==sha(A/(N+'-summary.json'))
ids={r['run_id']for r in raw};assert len(ids)==2;previous=set()
for f in A.glob('rfc-025-v7-*.jsonl'):
 if f.name.startswith(N):continue
 for line in f.read_text().splitlines():
  row=json.loads(line)
  if isinstance(row,dict)and'run_id'in row:previous.add(row['run_id'])
assert ids.isdisjoint(previous)
print(json.dumps(dict(source_base=p['source_base'],reconstructed_inputs=128,summary_regenerates=True,new_completed_invocations=2,global_unique_invocations=805,full_archive_unique_invocations=prior['full_archive_unique_invocations']+2,inherited_interrupted_without_json=1,retained_source_restored=True,restoration_scope='Historical diagnostic-end restoration; the successor optimization verifier owns final live-state checks.',canonical_sha256=b['baseline']['sha256'],checks_sha256=sha(A/(N+'-checks.json')),restoration_sha256=sha(A/(N+'-restoration.json')),summary_sha256=sha(A/(N+'-summary.json')),verifier_sha256=sha(Path(__file__))),indent=2))
