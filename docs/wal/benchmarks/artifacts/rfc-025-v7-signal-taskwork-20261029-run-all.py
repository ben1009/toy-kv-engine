#!/usr/bin/env python3
"""Run frozen screen, guards and independent confirmation in dependency order."""
import json,subprocess,sys
from pathlib import Path
ART=Path(__file__).resolve().parent
STEM='rfc-025-v7-signal-taskwork-20261029'
matrix=json.loads((ART/(STEM+'-matrix.json')).read_text());completed=[];summary={}
def run_cells(cells):
    global summary
    for cell in cells:
        print('START',cell,flush=True)
        subprocess.run([sys.executable,'-u',str(ART/(STEM+'-run.py')),cell],check=True)
        completed.append(cell)
        data=subprocess.check_output([sys.executable,str(ART/(STEM+'-analyze.py')),*completed])
        (ART/(STEM+'-summary.json')).write_bytes(data);summary=json.loads(data)
        print('FINISH',cell,summary[cell]['stall_filter'],flush=True)
run_cells(matrix['screen'])
if all(summary[c]['v7_passed'] for c in matrix['screen']):
    run_cells(matrix['followup'])
    if all(summary[c]['v7_passed'] for c in matrix['followup']):
        run_cells(matrix['confirmation'])
print('CAPTURE COMPLETE',completed,'cancelled',summary['status']['cancelled_cells'],flush=True)
