#!/usr/bin/env python3
import subprocess,sys
from pathlib import Path
art=Path(__file__).resolve().parent
stem='rfc-025-v7-group-classification-retry-20261030'
print('START ext4-batch16',flush=True)
subprocess.run([sys.executable,str(art/(stem+'-run.py')),'ext4-batch16'],check=True)
summary=subprocess.check_output([sys.executable,str(art/(stem+'-analyze.py'))]);(art/(stem+'-summary.json')).write_bytes(summary)
print('DONE ext4-batch16; summary captured',flush=True)
