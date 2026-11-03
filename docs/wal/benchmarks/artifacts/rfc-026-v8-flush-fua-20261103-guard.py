"""Bounded APST comparison; only FID 0x0c may be changed, then restored."""

from collections import Counter
from pathlib import Path
import ctypes
import datetime
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time

from decode import decode
from nvme_readonly import Command, IOCTL_ADMIN, libc, read_admin


p = Path(__file__).resolve().parent
smoke = sys.argv[1:] == ["--smoke"]
assert not sys.argv[1:] or smoke
assert os.geteuid() == 0
stem = "smoke" if smoke else "full"
assert not (p / f"{stem}-result.json").exists()
setup_id = int(re.search(r"^ID: (\d+)$", (p / "nvme_setup_cmd-format.txt").read_text(), re.M)[1])
fd = os.open("/dev/nvme0", os.O_RDWR)
_, identity = read_admin(fd, 0x06, 4096, cdw10=1)
assert identity[24:64].decode().strip() == "SOLIDIGM SSDPFKNU010TZ"
assert identity[64:72].decode().strip() == "004C" and identity[265] & 1
original, table = read_admin(fd, 0x0A, 256, cdw10=0x0C)
vwc_before, _ = read_admin(fd, 0x0A, cdw10=0x06)
report = {"firmware": "004C", "smoke": smoke,
          "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
          "original_apst_value": original, "original_apst_table_hex": table.hex(),
          "original_vwc_value": vwc_before,
          "module_default_ps_max_latency_us": Path("/sys/module/nvme_core/parameters/default_ps_max_latency_us").read_text().strip(),
          "module_parameter_changed": False, "persistent_save_bit_used": False,
          "raw_device_writes": False, "captures": [], "restored_exactly": False,
          "scope": "File-only synchronous O_DIRECT writes. RWF_DSYNC is called FUA only after every targeted NVMe write is traced with control bit 0x4000. flush1 and dsync1 give each write a durability boundary; flush4 is a separate workload."}
needs_restore = False
streams = []


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")
    os.chown(path, 1000, 1000)


def set_apst(value):
    assert value in [original, original & ~1]
    payload = ctypes.create_string_buffer(table, len(table))
    command = Command(opcode=0x09, nsid=0, cdw10=0x0C, cdw11=value,
                      addr=ctypes.addressof(payload), data_len=len(table), timeout_ms=5000)
    assert command.cdw10 & (1 << 31) == 0
    rc = libc.ioctl(fd, IOCTL_ADMIN, ctypes.byref(command))
    if rc < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    if rc:
        raise RuntimeError(f"Set APST returned status {rc:#x}")
    observed, observed_table = read_admin(fd, 0x0A, 256, cdw10=0x0C)
    assert observed == value and observed_table == table
    report.setdefault("feature_readbacks", []).append({"value": observed,
        "table_sha256": hashlib.sha256(observed_table).hexdigest(),
        "utc": datetime.datetime.now(datetime.timezone.utc).isoformat()})


def interrupted(signum, _frame):
    raise InterruptedError(f"signal {signum}; restoring original APST")


signal.signal(signal.SIGINT, interrupted)
signal.signal(signal.SIGTERM, interrupted)


def capture(name, probe_arg=None):
    trace_fd = os.memfd_create("nvme-flush-fua", 0)
    streams.append((name, trace_fd))
    command = ["/usr/bin/perf", "record", "-a", "-m", "64", "-R", "-T", "--sample-cpu",
               "--clockid", "mono", "-e", "nvme:nvme_setup_cmd", "-o", f"/proc/self/fd/{trace_fd}",
               "--", "/usr/bin/python3", str(p / "as_user.py")]
    if probe_arg:
        command.append(probe_arg)
    # Capture buffers, stdout and stderr stay in RAM until all phases/restoration finish.
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             pass_fds=(trace_fd,), start_new_session=True)
    try:
        out, err = child.communicate(timeout=60)
    except BaseException:
        os.killpg(child.pid, signal.SIGTERM)
        try:
            out, err = child.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            out, err = child.communicate()
        report.setdefault("failed_capture", []).append({"name": name, "stderr": err.decode(errors="replace")})
        raise
    assert child.returncode == 0, err.decode()
    q = json.loads(out)
    events, meta = decode(f"/proc/self/fd/{trace_fd}", setup_id)
    assert meta["lost"] == 0
    for phase in q["results"]:
        matching = [e for e in events if e["disk"] == "nvme0n1" and e["qid"] > 0
                    and e["opcode"] == 1 and e["pid"] == phase["pid"]
                    and phase["start_ns"] <= e["time_ns"] <= phase["end_ns"]]
        assert len(matching) == phase["operations"], (phase["mode"], len(matching), phase["operations"])
        assert all(e["nlb_zero_based"] == 7 for e in matching)
        expected = 0x4000 if phase["mode"] == "dsync1" else 0
        assert all(e["control"] == expected for e in matching), Counter(e["control"] for e in matching)
        phase["verified_nvme_write_controls"] = {hex(k): v for k, v in Counter(e["control"] for e in matching).items()}
    report["captures"].append({"name": name, "command": command, "returncode": child.returncode,
        "record": q, "trace_metadata": meta, "perf_stderr": err.decode(),
        "nvme_setups": events, "trace_in_ram_during_all_probes": True})
    print(json.dumps({"capture": name, "trace_verified": True,
        "phases": [{"mode": a["mode"], "ops_s": a["ops_s"], "write": a["write"],
                    "first_after_sync": a.get("first_after_sync")} for a in q["results"]]}), flush=True)


try:
    time.sleep(3)
    if smoke:
        capture("smoke", "smoke")
    else:
        assert (p / "smoke-result.json").exists()
        assert json.loads((p / "smoke-result.json").read_text())["restored_exactly"]
        capture("original")
        needs_restore = True  # covers an uncertain Set Features completion as well
        set_apst(original & ~1)
        time.sleep(3)
        capture("apst-disabled")
        set_apst(original)
        needs_restore = False
        time.sleep(3)
        capture("restored", "restored")
finally:
    try:
        if needs_restore:
            set_apst(original)
        value_after, table_after = read_admin(fd, 0x0A, 256, cdw10=0x0C)
        vwc_after, _ = read_admin(fd, 0x0A, cdw10=0x06)
        assert value_after == original and table_after == table and vwc_after == vwc_before
        report["restored_exactly"] = True
        report["final_apst_value"] = value_after
        report["final_apst_table_sha256"] = hashlib.sha256(table_after).hexdigest()
        report["final_vwc_value"] = vwc_after
    finally:
        os.close(fd)
        report["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for name, stream in streams:
            os.lseek(stream, 0, os.SEEK_SET)
            path = p / f"{stem}-{name}.perf"
            with path.open("wb") as target:
                while block := os.read(stream, 1024 * 1024):
                    target.write(block)
            os.chown(path, 1000, 1000)
            os.close(stream)
        save_json(p / f"{stem}-result.json", report)
        print(json.dumps({"restored_exactly": report["restored_exactly"], "result": str(p / f"{stem}-result.json")}), flush=True)
