#!/usr/bin/env python3
import subprocess,sys
from pathlib import Path
art=Path(__file__).resolve().parent
stem="rfc-025-v7-dynamic-point-20261030"
for cell in ["ext4-point1","ext4-point8","ext4-point16"]:
 print("START",cell,flush=True)
 subprocess.run([sys.executable,"-u",str(art/(stem+"-run.py")),cell],check=True)
 print("FINISH",cell,flush=True)
result=subprocess.check_output([sys.executable,str(art/(stem+"-analyze.py"))])
(art/(stem+"-summary.json")).write_bytes(result)
print("CAPTURE COMPLETE",flush=True)
