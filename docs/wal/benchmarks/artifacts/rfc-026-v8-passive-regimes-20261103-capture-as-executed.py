"""Capture passive NVMe/block traces and application timelines in RAM.

Run as root only for perf tracepoint access. The benchmark drops to the invoking
user before exec. This runner never opens a raw device or changes device policy.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from wal_trace_regimes import POLICY


EVENTS = ("nvme/nvme_setup_cmd", "nvme/nvme_complete_rq",
          "block/block_rq_issue", "block/block_rq_complete")
ORDER = ("v7", "v8", "v8", "v7")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cpus(value):
    output = set()
    for part in value.split(","):
        ends = [int(n) for n in part.split("-")]
        if len(ends) == 1:
            output.add(ends[0])
        elif len(ends) == 2 and ends[0] <= ends[1]:
            output.update(range(ends[0], ends[1] + 1))
        else:
            raise ValueError("Invalid CPU range")
    if not output or min(output) < 0:
        raise ValueError("Empty or negative CPU mask")
    return sorted(output)


def child():
    # This mode is called by perf after recording starts. No privileged workload.
    uid, gid = int(sys.argv[2]), int(sys.argv[3])
    if uid <= 0 or gid <= 0:
        raise ValueError("Benchmark must run as an ordinary user")
    os.setgroups([])
    os.setgid(gid)
    os.setuid(uid)
    os.sched_setaffinity(0, cpus(sys.argv[4]))
    os.execv(sys.argv[5], sys.argv[5:])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpus", default="0-15")
    parser.add_argument("--uid", type=int, default=int(os.getenv("SUDO_UID", "0")))
    parser.add_argument("--gid", type=int, default=int(os.getenv("SUDO_GID", "0")))
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if os.geteuid() != 0 or args.uid <= 0 or args.gid <= 0:
        parser.error("Use sudo; only perf needs privilege, the benchmark must be non-root")
    binary, out = args.binary.resolve(strict=True), args.output_dir.resolve()
    cpu_list = cpus(args.cpus)
    if not set(cpu_list) <= os.sched_getaffinity(0):
        parser.error("Requested CPUs are not available")
    formats = {name: (Path("/sys/kernel/tracing/events") / name / "format").read_text()
               for name in EVENTS}
    perf_version = subprocess.check_output(["/usr/bin/perf", "version"], text=True).strip()
    workloads = [{"name": "point", "num": 1024 if args.smoke else 20000, "writers": 1, "batch": 1}]
    if not args.smoke:
        workloads.append({"name": "batch64", "num": 750000, "writers": 16, "batch": 64})
    order = ORDER[:2] if args.smoke else ORDER
    plan = {"protocol": "kv-engine.passive-wal-trace.v1", "binary": str(binary),
            "binary_sha256": sha256(binary), "runner_sha256": sha256(Path(__file__)),
            "analyzer_sha256": sha256(Path(__file__).with_name("wal_trace_regimes.py")),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "cpu_mask": cpu_list, "events": EVENTS, "clock": "CLOCK_MONOTONIC",
            "kernel": os.uname().release, "perf": perf_version,
            "policy": POLICY, "order": order, "workloads": workloads, "uid": args.uid,
            "capture_in_ram_for_entire_cohort": True, "raw_device_writes": False,
            "device_settings_changed": False, "RFC_performance_qualified": False,
            "scope": "Instrumented diagnostic; no endpoint probes. The calibrated 4KiB isolated-write signal does not classify arbitrary bulk or overlapping writes."}
    out.mkdir(parents=True, exist_ok=False)
    os.chown(out, args.uid, args.gid)

    def save(name, data):
        path = out / name
        path.write_bytes(data)
        os.chown(path, args.uid, args.gid)

    def save_json(name, data):
        save(name, (json.dumps(data, indent=2) + "\n").encode())

    # Persist the protocol before measurements. All subsequent observer data stays
    # in memfds / pipes until every workload in this cohort has stopped.
    save_json("plan.json", plan)
    save_json("formats.json", formats)
    pending = []
    active = None

    def interrupted(signum, _frame):
        raise InterruptedError(f"signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        for spec in workloads:
            for position, fmt in enumerate(order):
                if sha256(binary) != plan["binary_sha256"] or (
                    Path("/proc/sys/kernel/random/boot_id").read_text().strip() != plan["boot_id"]
                ):
                    raise RuntimeError("Binary or boot changed")
                name = f"{spec['name']}-{position}-{fmt}"
                trace_fd = os.memfd_create("passive-nvme-trace", 0)
                timeline_fd = os.memfd_create("wal-application-timeline", 0)
                os.fchown(timeline_fd, args.uid, args.gid)
                record = {"name": name, "format": fmt, "workload": spec,
                          "started_unix_ns": time.time_ns()}
                pending.append((name, trace_fd, timeline_fd, record))
                env = {k: v for k, v in os.environ.items()
                       if not k.startswith(("TOYKV_", "KV_PERF_")) and k != "LD_PRELOAD"}
                env["TOYKV_WAL_TIMELINE"] = f"/proc/self/fd/{timeline_fd}"
                env["TOYKV_WAL_TIMELINE_INTERVAL_MS"] = "1000"
                command = ["/usr/bin/perf", "record", "-a", "-m", "64", "-c", "1",
                           "-R", "-T", "--sample-cpu", "--clockid", "mono"]
                for event in EVENTS:
                    command += ["-e", event.replace("/", ":")]
                command += ["-o", f"/proc/self/fd/{trace_fd}", "--", "/usr/bin/python3",
                            str(Path(__file__).resolve()), "--as-user", str(args.uid), str(args.gid),
                            args.cpus, str(binary), "--bench", "wal_batch_concurrent",
                            "--wal-batch-size", str(spec["batch"]), "--pitr", "--pitr-format", fmt,
                            "--wal-io-mode", "parallel", "--num", str(spec["num"]),
                            "--threads", str(spec["writers"]), "--value-size", "1024",
                            "--target-sst-size", "2147483648", "--compaction", "none", "--output", "json",
                            "--latency-sample-every", "10", "--path", str(out / (name + "-db"))]
                record["command"] = command
                active = subprocess.Popen(command, env=env, pass_fds=(trace_fd, timeline_fd),
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
                stdout, stderr = active.communicate(timeout=180)
                record.update({"returncode": active.returncode, "finished_unix_ns": time.time_ns(),
                               "stdout": stdout.decode(errors="replace"), "stderr": stderr.decode(errors="replace")})
                active = None
                if record["returncode"]:
                    raise RuntimeError(f"Capture {name} failed: {record['stderr'][-2000:]}")
                benchmark = json.loads(record["stdout"])
                if benchmark["result"]["ops"] != spec["num"] or (
                    benchmark["params"]["wal_format_version"] != int(fmt[1:])
                ):
                    raise RuntimeError("Workload or format mismatch")
                os.lseek(timeline_fd, 0, os.SEEK_SET)
                timeline = json.loads(os.read(timeline_fd, os.fstat(timeline_fd).st_size))
                if sum(r["puts"] for r in timeline["requests"]) != spec["num"]:
                    raise RuntimeError("Application timeline operation loss")
                print(json.dumps({"capture": name, "ops": spec["num"], "returncode": 0,
                                  "trace_bytes": os.fstat(trace_fd).st_size}), flush=True)
    finally:
        if active is not None and active.poll() is None:
            os.killpg(active.pid, signal.SIGTERM)
            try:
                stdout, stderr = active.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(active.pid, signal.SIGKILL)
                stdout, stderr = active.communicate()
            pending[-1][3].update({"returncode": active.returncode,
                                   "stdout": stdout.decode(errors="replace"), "stderr": stderr.decode(errors="replace")})
        for name, trace_fd, timeline_fd, record in pending:
            save_json(name + "-capture.json", record)
            for suffix, fd in ((".perf", trace_fd), ("-timeline.json", timeline_fd)):
                os.lseek(fd, 0, os.SEEK_SET)
                save(name + suffix, os.read(fd, os.fstat(fd).st_size))
                os.close(fd)
            if record.get("returncode") == 0:
                save(name + "-benchmark.json", record["stdout"].encode())


if __name__ == "__main__":
    if sys.argv[1:2] == ["--as-user"]:
        child()
    else:
        main()
