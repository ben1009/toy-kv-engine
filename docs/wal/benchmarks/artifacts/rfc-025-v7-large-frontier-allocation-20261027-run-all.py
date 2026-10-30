#!/usr/bin/env python3
"""Run fixed SSD stages, preserving failures and cancelling dependent stages."""
import json
import subprocess
import sys
from pathlib import Path
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-large-frontier-allocation-20261027'
matrix = json.loads((ART/(STEM+'-matrix.json')).read_text())
completed = []
for cell in matrix['cells']:
    print('START CELL',cell,flush=True)
    subprocess.run([sys.executable,'-u',str(ART/(STEM+'-run.py')),cell],check=True)
    completed.append(cell)
    data = subprocess.check_output([sys.executable,str(ART/(STEM+'-analyze.py')),*completed])
    (ART/(STEM+'-summary.json')).write_bytes(data)
    summary = json.loads(data)
    for part,value in summary[cell]['comparisons'].items():
        print('FINISH',cell,part,value['median'],value['guards'],flush=True)
    print('FILTERED',cell,summary[cell]['stall_filter']['matched_pairs'],summary[cell]['stall_filter']['median'],summary[cell]['stall_filter']['guards'],flush=True)
    if not summary[cell]['v7_passed']:
        print('STOP: completed stage lacks filtered throughput/p99 benefit or sufficient matched pairs; dependent stages cancelled',flush=True)
        break
print('COMPLETED CELLS',completed,flush=True)
