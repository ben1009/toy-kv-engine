from pathlib import Path
import subprocess,sys
root=Path('/home/liu/proj/toy-kv-engine');p=root/'kv-engine/src/wal/v7/codec.rs';orig=p.read_bytes();inc=b'\n#[cfg(test)]\n#[path = "/home/liu/proj/toy-kv-engine/target/wal-opt/20261030-v7-direct-frame/src/main.rs"]\nmod direct_frame_cpu_prototype;\n'
try:
 p.write_bytes(orig+inc)
 result=subprocess.run(['cargo','nextest','run','--offline','--locked','--workspace','--all-features','--lib','-E','test(matches_real_v7_codec_and_rolling_sha)','--retries','0'],cwd=root)
finally:p.write_bytes(orig)
sys.exit(result.returncode)
