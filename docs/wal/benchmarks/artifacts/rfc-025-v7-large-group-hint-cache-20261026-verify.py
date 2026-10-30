from pathlib import Path
import ast,csv,hashlib,json,subprocess,tempfile
ART=Path(__file__).resolve().parent
ROOT=ART.parents[3]
WORK=ROOT/'target/wal-opt/20261026-v7-large-group-hint-cache'
WORK.mkdir(parents=True,exist_ok=True)
BASE='20b011b1e1c52deccce88fce4e04330a27956ee2'
RUNTIME='kv-engine/src/wal/parallel/runtime.rs'

def sha(data):return hashlib.sha256(data).hexdigest()

base_cache={}
for label,patch_suffix in [('fast-sync-wait','diagnostic'),('deferred-prefix-hash','rejected'),('prefix-hash-overlap','rejected'),('hash-scheduling','rejected'),('hash-pipeline','rejected'),('large-group-coalescing','rejected'),('large-group-hint-cache','rejected')]:
 stem='rfc-025-v7-'+label+'-20261026'
 manifest=ART/(stem+'-source-sha256.tsv')
 rows=list(csv.DictReader(manifest.open(),delimiter='\t'))
 assert len(rows)==128 and len({r['path'] for r in rows})==128
 for r in rows:
  if r['path'] not in base_cache:
   base_cache[r['path']]=subprocess.check_output(['git','show',BASE+':'+r['path']],cwd=ROOT)
 with tempfile.TemporaryDirectory(prefix='source-verification-',dir=WORK) as directory:
  destination=Path(directory)/RUNTIME;destination.parent.mkdir(parents=True)
  destination.write_bytes(base_cache[RUNTIME])
  patch=ART/(stem+'-'+patch_suffix+'.patch')
  subprocess.run(['patch','--batch','--forward','-p1','-d',directory,'-i',str(patch)],check=True,capture_output=True)
  for r in rows:
   data=destination.read_bytes() if r['path']==RUNTIME else base_cache[r['path']]
   assert sha(data)==r['sha256'],(label,r['path'])
 identities=json.loads((ART/(stem+'-binaries.json')).read_text())
 for identity in identities.values():
  assert sha((ROOT/identity['path']).read_bytes())==identity['sha256']
 if label=='large-group-hint-cache':
  for r in rows:assert (ROOT/r['path']).read_bytes()==base_cache[r['path']],r['path']
  assert sha((ROOT/'target/release/write-perf').read_bytes())==identities['retained']['sha256']
 print(label,'128 source hashes and executable identities verified; manifest',sha(manifest.read_bytes()))

summaries=[
 ('rfc-025-v7-fast-sync-wait-20261026',[]),
 ('rfc-025-v7-deferred-prefix-hash-20261026',['tmpfs-batch64','tmpfs-point8','tmpfs-point1']),
 ('rfc-025-v7-prefix-hash-overlap-20261026',['tmpfs-batch64','tmpfs-point8','tmpfs-point1','ext4-batch64','ext4-point8']),
 ('rfc-025-v7-hash-scheduling-20261026',['tmpfs-batch64','tmpfs-point8','tmpfs-point1','ext4-batch64','ext4-point8']),
 ('rfc-025-v7-hash-pipeline-20261026',['tmpfs-batch64','tmpfs-point8','tmpfs-point1','ext4-batch64','ext4-point8']),
 ('rfc-025-v7-large-group-coalescing-20261026',['tmpfs-batch64','tmpfs-point8']),
 ('rfc-025-v7-large-group-hint-cache-20261026',['tmpfs-batch64','tmpfs-point8','tmpfs-point1','ext4-batch64','ext4-point8']),
 ('rfc-025-v7-critical-path-20261026',[]),
]
for stem,cells in summaries:
 script=ART/(stem+'-analyze.py');ast.parse(script.read_text())
 raw=subprocess.check_output(['python3',str(script),*cells])
 assert raw==(ART/(stem+'-summary.json')).read_bytes(),stem
 print(stem,'summary regenerated exactly')
ids=[]
for pattern in ('rfc-025-v7-fast-sync-wait-*-20261026.jsonl','rfc-025-v7-deferred-prefix-hash-*-20261026.jsonl','rfc-025-v7-prefix-hash-overlap-*-20261026.jsonl','rfc-025-v7-hash-scheduling-*-20261026.jsonl','rfc-025-v7-hash-pipeline-*-20261026.jsonl','rfc-025-v7-large-group-coalescing-*-20261026.jsonl','rfc-025-v7-large-group-hint-cache-*-20261026.jsonl','rfc-025-v7-critical-path-20261026.jsonl'):
 for p in ART.glob(pattern):
  for line in p.read_bytes().splitlines():ids.append(json.loads(line)['run_id'])
assert len(ids)==523 and len(set(ids))==523,(len(ids),len(set(ids)))
print('All 523 new run IDs are distinct.')
for pattern in ('rfc-025-v7-fast-sync-wait-*-run.sh','rfc-025-v7-deferred-prefix-hash-*-run.sh','rfc-025-v7-prefix-hash-overlap-*-run.sh','rfc-025-v7-hash-scheduling-*-run.sh','rfc-025-v7-hash-pipeline-*-run.sh','rfc-025-v7-large-group-coalescing-*-run.sh','rfc-025-v7-large-group-hint-cache-*-run.sh'):
 for p in ART.glob(pattern):subprocess.run(['bash','-n',str(p)],check=True)
print('All captured driver wrappers pass shell syntax checks.')

for suffix in ('pooled','peaks'):
 script=ART/('rfc-025-v7-large-group-hint-cache-20261026-'+suffix+'.py')
 output=ART/('rfc-025-v7-large-group-hint-cache-20261026-'+suffix+'.json')
 assert subprocess.check_output(['python3',str(script)])==output.read_bytes()
 print(suffix,'summary regenerated exactly')
