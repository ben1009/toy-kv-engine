#!/usr/bin/env python3
"""Exec one benchmark child and retain its Linux wait4 lifetime resource usage."""
import json
import os
import sys
import time
from pathlib import Path

output=Path(sys.argv[1])
assert sys.argv[2]=='--' and not output.exists()
command=sys.argv[3:]
assert command
start=time.monotonic()
pid=os.fork()
if pid==0:
    os.execv(command[0],command)
_,status,usage=os.wait4(pid,0)
result=dict(pid=pid,max_rss_kib=usage.ru_maxrss,minor_page_faults=usage.ru_minflt,
            major_page_faults=usage.ru_majflt,voluntary_context_switches=usage.ru_nvcsw,
            involuntary_context_switches=usage.ru_nivcsw,whole_process_user_seconds=usage.ru_utime,
            whole_process_system_seconds=usage.ru_stime,whole_process_elapsed_seconds=time.monotonic()-start)
output.write_text(json.dumps(result,sort_keys=True)+'\n')
code=os.waitstatus_to_exitcode(status)
sys.exit(code if code>=0 else 128-code)
