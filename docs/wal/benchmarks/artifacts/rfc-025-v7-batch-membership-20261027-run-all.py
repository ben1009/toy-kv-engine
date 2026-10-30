#!/usr/bin/env python3
"""Run the fixed batch-membership SSD cells without performance stop rules."""
import json
import subprocess
import sys
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-batch-membership-20261027'
matrix = json.loads((ART / (STEM + '-matrix.json')).read_text())
completed = []
for cell in matrix['cells']:
    print('START CELL', cell, flush=True)
    subprocess.run([sys.executable, '-u', str(ART / (STEM + '-run.py')), cell], check=True)
    completed.append(cell)
    output = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py')), *completed])
    (ART / (STEM + '-summary.json')).write_bytes(output)
    summary = json.loads(output)
    print('FINISH CELL', cell, flush=True)
    for part, value in summary[cell]['comparisons'].items():
        print(part, json.dumps(value['median'], sort_keys=True), 'guards', value['guards'], flush=True)
print('ALL FIVE CELLS COMPLETE', flush=True)
