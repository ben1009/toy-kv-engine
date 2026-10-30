from pathlib import Path
import gzip,hashlib,subprocess,sys
root=Path('/home/liu/proj/toy-kv-engine')
source=root/'kv-engine/src/pitr/mod.rs'
patch=gzip.decompress((root/'docs/wal/benchmarks/artifacts/rfc-025-v7-append-entries-20261030-candidate.patch.gz').read_bytes())
paths=['kv-engine/src/pitr/mod.rs','kv-engine/src/wal/v7/codec.rs']
def diff():
 return subprocess.check_output(['git','diff','--no-ext-diff','--binary','HEAD','--',*paths],cwd=root)
assert diff()==patch
original=source.read_bytes()
injected=b'\n#[cfg(test)]\n#[path = "../../../target/wal-opt/20261030-v7-append-entries/microbench.rs"]\nmod v7_append_cpu_screen;\n'
assert b'mod v7_append_cpu_screen;' not in original
try:
 source.write_bytes(original+injected)
 cmd=['cargo','test','--offline','--locked','--release','--workspace','--all-features','--lib','logical_append_cpu_screen','--','--ignored','--nocapture','--test-threads=1']
 out=root/'target/wal-opt/20261030-v7-append-entries/microbench.log'
 with out.open('w') as f:
  completed=subprocess.run(cmd,cwd=root,stdout=f,stderr=subprocess.STDOUT)
finally:
 source.write_bytes(original)
assert diff()==patch, 'candidate source differed after removing the diagnostic test'
print('manual CPU test exit:',completed.returncode,'patch SHA-256:',hashlib.sha256(patch).hexdigest(),flush=True)
sys.exit(completed.returncode)
