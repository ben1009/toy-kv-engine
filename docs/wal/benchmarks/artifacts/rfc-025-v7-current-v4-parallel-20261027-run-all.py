#!/usr/bin/env python3
"""Run all fixed current-v4/v7 format comparisons without outcome-based stopping."""
import json
import subprocess
import sys
from pathlib import Path
ART=Path(__file__).resolve().parent
STEM='rfc-025-v7-current-v4-parallel-20261027'
matrix=json.loads((ART/(STEM+'-matrix.json')).read_text())
completed=[]
for cell in matrix['cells']:
    print('START CELL',cell,flush=True)
    subprocess.run([sys.executable,'-u',str(ART/(STEM+'-run.py')),cell],check=True)
    completed.append(cell)
    result=subprocess.check_output([sys.executable,str(ART/(STEM+'-analyze.py')),*completed])
    (ART/(STEM+'-summary.json')).write_bytes(result)
    summary=json.loads(result)
    print('RAW',cell,next(iter(summary[cell]['comparisons'].values()))['median'],flush=True)
    for mode in ['combined','sync_only']:
        view=summary[cell]['stall_views'][mode]
        print(mode,cell,view['matched_pairs'],view['median'],flush=True)
print('COMPLETED CELLS',completed,flush=True)
