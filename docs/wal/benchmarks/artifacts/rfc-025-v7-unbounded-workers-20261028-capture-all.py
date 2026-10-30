#!/usr/bin/env python3
"""Require a successful mechanism probe before launching the fixed matrix."""
import subprocess
import sys
from pathlib import Path
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-unbounded-workers-20261028'
for suffix in ['-probe-run.py', '-run-all.py']:
    subprocess.run([sys.executable, '-u', str(ART / (STEM + suffix))], check=True)
