from pathlib import Path
import os
import runpy
import sys

assert os.geteuid() == 0
os.setgroups([])
os.setgid(1000)
os.setuid(1000)
os.sched_setaffinity(0, {8})
sys.argv = [str(Path(__file__).with_name("probe.py")), *sys.argv[1:]]
runpy.run_path(sys.argv[0], run_name="__main__")
