import json,subprocess,sys
from pathlib import Path
A=Path(__file__).resolve().parent;N='rfc-025-v7-batch-attribution-20261029'
for cell in json.loads((A/(N+'-matrix.json')).read_text())['cells']:
 print('START',cell,flush=True)
 subprocess.run([sys.executable,'-u',str(A/(N+'-run.py')),cell],check=True)
 print('FINISH',cell,flush=True)
print('SCORED CAPTURE COMPLETE',flush=True)
