"""Repeat the file-only comparison at restored APST; read-only admin commands."""

from collections import Counter
from pathlib import Path
import datetime
import hashlib
import json
import os
import re
import signal
import subprocess
import time

from decode import decode
from nvme_readonly import read_admin


p = Path(__file__).resolve().parent
assert os.geteuid() == 0 and not (p / "confirm-result.json").exists()
fd = os.open("/dev/nvme0", os.O_RDONLY)
value, table = read_admin(fd, 0x0A, 256, cdw10=0x0C)
vwc, _ = read_admin(fd, 0x0A, cdw10=0x06)
assert value == 1 and vwc == 1
trace_fd = os.memfd_create("nvme-fua-restored-confirm", 0)
command = ["/usr/bin/perf", "record", "-a", "-m", "64", "-R", "-T", "--sample-cpu",
           "--clockid", "mono", "-e", "nvme:nvme_setup_cmd", "-o", f"/proc/self/fd/{trace_fd}",
           "--", "/usr/bin/python3", str(p / "as_user.py")]
time.sleep(3)
child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         pass_fds=(trace_fd,), start_new_session=True)
try:
    out, err = child.communicate(timeout=60)
except BaseException:
    os.killpg(child.pid, signal.SIGTERM)
    try:
        child.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.communicate()
    raise
assert child.returncode == 0, err.decode()
q = json.loads(out)
setup_id = int(re.search(r"^ID: (\d+)$", (p / "nvme_setup_cmd-format.txt").read_text(), re.M)[1])
events, meta = decode(f"/proc/self/fd/{trace_fd}", setup_id)
assert meta["lost"] == 0
for phase in q["results"]:
    writes = [e for e in events if e["disk"] == "nvme0n1" and e["qid"] > 0
              and e["opcode"] == 1 and e["pid"] == phase["pid"]
              and phase["start_ns"] <= e["time_ns"] <= phase["end_ns"]]
    assert len(writes) == phase["operations"]
    expected = 0x4000 if phase["mode"] == "dsync1" else 0
    assert all(e["control"] == expected and e["nlb_zero_based"] == 7 for e in writes)
    phase["verified_nvme_write_controls"] = {hex(k): v for k, v in Counter(e["control"] for e in writes).items()}
after, after_table = read_admin(fd, 0x0A, 256, cdw10=0x0C)
after_vwc, _ = read_admin(fd, 0x0A, cdw10=0x06)
assert after == value and after_table == table and after_vwc == vwc
os.close(fd)
report = {"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
          "apst_value": value, "apst_table_sha256": hashlib.sha256(table).hexdigest(),
          "vwc_value": vwc, "apst_and_vwc_unchanged": True,
          "device_settings_changed": False, "raw_device_writes": False,
          "record": q, "trace_metadata": meta, "perf_stderr": err.decode(),
          "command": command, "nvme_setups": events}
os.lseek(trace_fd, 0, os.SEEK_SET)
with (p / "confirm.perf").open("wb") as target:
    while block := os.read(trace_fd, 1024 * 1024):
        target.write(block)
os.close(trace_fd)
(p / "confirm-result.json").write_text(json.dumps(report, indent=2) + "\n")
for path in [p / "confirm.perf", p / "confirm-result.json"]:
    os.chown(path, 1000, 1000)
print(json.dumps({"apst_unchanged": True, "trace_verified": True,
    "phases": [{k: v for k, v in a.items() if k in ["mode", "ops_s", "write", "durable_request", "first_after_sync"]}
               for a in q["results"]]}))
