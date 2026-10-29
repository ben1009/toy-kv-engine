#!/usr/bin/env python3
"""Complete all batch screens, then execute the frozen guards only on passing screens."""
import json
import subprocess
import sys
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-admitted-group-cool-20261029'
plan = json.loads((ART / (STEM + '-plan.json')).read_text())


def capture(cells):
    for cell in cells:
        print('START', cell, flush=True)
        subprocess.run([sys.executable, '-u', str(ART / (STEM + '-run.py')), cell], check=True)
        print('FINISH', cell, flush=True)


def summarize():
    data = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))])
    (ART / (STEM + '-summary.json')).write_bytes(data)
    result = json.loads(data)
    for cell, item in result['cells'].items():
        print(cell, item['ratios'], item['checks'], flush=True)
    return result


capture(plan['screen_cells'])
result = summarize()
if result['screen_passed']:
    capture(plan['guard_cells'])
    result = summarize()
print('CAPTURE COMPLETE', result['invocations'], 'retention', result['retention_passed'], flush=True)
