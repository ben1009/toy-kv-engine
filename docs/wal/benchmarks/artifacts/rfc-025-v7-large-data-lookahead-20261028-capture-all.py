#!/usr/bin/env python3
"""Complete the excluded diagnostic before the fixed SSD performance matrix."""
import subprocess
import sys
from pathlib import Path
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-large-data-lookahead-20261028'
subprocess.run([sys.executable, '-u', str(ART / (STEM + '-probe-run.py'))], check=True)
diagnosis = subprocess.check_output([sys.executable, str(ART / (STEM + '-diagnose.py'))])
(ART / (STEM + '-diagnosis.json')).write_bytes(diagnosis)
subprocess.run([sys.executable, '-u', str(ART / (STEM + '-run-all.py'))], check=True)
