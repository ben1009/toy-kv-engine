#!/usr/bin/env python3
import json,subprocess,sys
from pathlib import Path
art=Path(__file__).resolve().parent
stem='rfc-025-v7-group-classification-20261030'
plan=json.loads((art/(stem+'-plan.json')).read_text())
for cell in plan['screen_cells']:
 print('START',cell,flush=True); subprocess.run([sys.executable,str(art/(stem+'-run.py')),cell],check=True);print('DONE',cell,flush=True)
screen=json.loads(subprocess.check_output([sys.executable,str(art/(stem+'-analyze.py')),*plan['screen_cells']]))
if screen['screen_passed']:
 for cell in plan['guard_cells']:
  print('START',cell,flush=True);subprocess.run([sys.executable,str(art/(stem+'-run.py')),cell],check=True);print('DONE',cell,flush=True)
else:print('SKIP point guards: batch screen did not qualify',flush=True)
summary=subprocess.check_output([sys.executable,str(art/(stem+'-analyze.py'))]);(art/(stem+'-summary.json')).write_bytes(summary)
print('SUMMARY captured',flush=True)
