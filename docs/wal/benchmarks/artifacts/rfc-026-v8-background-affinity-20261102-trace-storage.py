from pathlib import Path
import json
import os
import shutil
import subprocess


p = Path(__file__).resolve().parent / "storage-trace"
assert os.geteuid() == 0
trace_fd = os.memfd_create("toy-kv-nvme-trace", 0)
try:
    command = ["perf", "record", "-a", "-m", "256",
               "-e", "nvme:nvme_setup_cmd", "-e", "nvme:nvme_complete_rq",
               "-o", f"/proc/self/fd/{trace_fd}", "--", "taskset", "-c", "0-15",
               "python3", str(p / "probe.py")]
    result = subprocess.run(command, pass_fds=(trace_fd,), capture_output=True)
    # The perf stream and diagnostic stdout/stderr stay in RAM until the
    # workload finishes, avoiding trace-writer traffic on the measured SSD.
    (p / "probe.log").write_bytes(result.stdout)
    (p / "perf.stderr").write_bytes(result.stderr)
    os.lseek(trace_fd, 0, os.SEEK_SET)
    with os.fdopen(os.dup(trace_fd), "rb") as source, (p / "perf.data").open("wb") as target:
        shutil.copyfileobj(source, target)
    record = {"command": command, "returncode": result.returncode,
              "trace_in_memory_during_workload": True,
              "trace_bytes": (p / "perf.data").stat().st_size,
              "global_device_settings_changed": False}
    (p / "capture.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record))
    if result.returncode:
        print(result.stderr.decode()[-2400:])
    raise SystemExit(result.returncode)
finally:
    os.close(trace_fd)
