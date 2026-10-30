#!/usr/bin/env python3
import subprocess,sys
from pathlib import Path
art=Path(__file__).resolve().parent
stem='rfc-025-v7-progress-sync-followup-20261030'
for cell in ['ext4-batch8','ext4-batch16']:
 print('START',cell,flush=True);subprocess.run([sys.executable,str(art/(stem+'-run.py')),cell],check=True);print('DONE',cell,flush=True)
summary=subprocess.check_output([sys.executable,str(art/(stem+'-analyze.py'))]);(art/(stem+'-summary.json')).write_bytes(summary)
print('SUMMARY captured',flush=True)
