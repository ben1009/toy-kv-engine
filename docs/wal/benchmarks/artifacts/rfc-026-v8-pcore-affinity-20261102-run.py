from pathlib import Path
import hashlib
import json
import os
import signal
import subprocess
import time


p = Path(__file__).resolve().parent
repo = p.parent.parent
normal = repo / "target/rfc-026-v8-notification-split-20261102/write-perf-cursor"
diagnostic = repo / "target/rfc-026-v8-producer-attribution-20261102/write-perf-producer-attribution"
expected = {
    "normal": "937a3843a044e107e15db756674be151875e388ff3a0a392cbd4e97463bbd2aa",
    "diagnostic": "fbb90ea8a6f20bf476760e79ae60e04cda3edae04138d45ae5a9e6346da299f1",
}
for mode, binary in [("normal", normal), ("diagnostic", diagnostic)]:
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == expected[mode]
assert sorted(os.sched_getaffinity(0)) == list(range(32))
assert Path("/sys/devices/cpu_core/cpus").read_text().strip() == "0-15"
assert Path("/sys/devices/cpu_atom/cpus").read_text().strip() == "16-31"

# Balanced positions and reversed format/affinity comparisons. No new binary,
# scheduler policy, or run selection is chosen after observing measurements.
orders = [
    [("v7", "hybrid"), ("v8", "hybrid"), ("v8", "pcore"), ("v7", "pcore")],
    [("v8", "hybrid"), ("v7", "pcore"), ("v7", "hybrid"), ("v8", "pcore")],
    [("v7", "pcore"), ("v8", "pcore"), ("v8", "hybrid"), ("v7", "hybrid")],
    [("v8", "pcore"), ("v7", "hybrid"), ("v7", "pcore"), ("v8", "hybrid")],
]
schedule = []
for mode, rounds in [("normal", orders), ("diagnostic", [orders[0], orders[2]])]:
    for round_index, order in enumerate(rounds):
        for position, (fmt, affinity) in enumerate(order):
            schedule.append({"mode": mode, "round": round_index, "position": position,
                             "format": fmt, "affinity": affinity})
plan = {
    "source_revision": "efa90f118742296cd95eb7a1593e0c636df46acd",
    "runtime_revision": "b8da402a276d7969f4c40ca99787a10a63d93f33",
    "binary_paths": {"normal": str(normal), "diagnostic": str(diagnostic)},
    "binary_sha256": expected,
    "schedule": schedule,
    "affinity": {"hybrid": "inherited native mask 0-31; no taskset",
                 "pcore": "taskset -c 0-15 applied to benchmark and inherited by its userspace threads"},
    "workload": {"puts": 750000, "writers": 16, "batch": 64, "value_bytes": 1024,
                 "wal_io_mode": "parallel", "compaction": "none", "target_sst_size": 2147483648,
                 "latency_sample_every": 10},
    "normal": "Same retained executable for all four cells. Timed process CPU, normal WAL/digest counters, timed getrusage context deltas. No perf or attribution scopes.",
    "diagnostic": "Same preserved diagnostic executable for all four cells. Whole-batch phase CPU/wall scopes sampled 1/64/thread, existing digest sample metrics, perf cpu-clock:u 499Hz, DWARF4096, --sample-cpu; perf enabled only during timed interval and recorded to memfd.",
    "cooldown_before_each_round_seconds": 45,
    "between_runs_seconds": 3,
    "stall_flag": "p99_ms > 10 OR mean fdatasync_ms > 1; all records retained, flagged scores excluded from best/eligible comparisons",
    "selection": "Highest eligible normal throughput per cell with same-run p99/CPU/contexts; also report eligible medians and complete raw cohort. Diagnostics never enter best-score rankings.",
    "limits": ["Whole-process P affinity changes available physical-core count, SMT and contention for all roles; it cannot isolate hash placement alone.",
               "CPU sample shares are time-weighted sample shares, not batch/byte proportions; do not use them as job-placement probabilities.",
               "Sampled phase scopes are inclusive; child scopes cannot be added to encoding parent."],
    "production_runtime_modified": False,
}
assert not (p / "plan.json").exists()
(p / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")

for index, item in enumerate(schedule):
    mode, fmt, affinity = item["mode"], item["format"], item["affinity"]
    name = f"{index:02}-{mode}-{fmt}-{affinity}"
    if item["position"] == 0:
        print("COOLDOWN 45s", mode, "round", item["round"], flush=True)
        time.sleep(45)
    env = dict(os.environ)
    for key in ["TOYKV_WAL_ATTRIBUTION", "TOYKV_PREPARATION_MICRO", "TOYKV_WAL_CONTEXT", "KV_PERF_CONTROL_DIR"]:
        env.pop(key, None)
    binary = normal if mode == "normal" else diagnostic
    command = [str(binary), "--bench", "wal_batch_concurrent", "--wal-batch-size", "64",
               "--pitr", "--pitr-format", fmt, "--wal-io-mode", "parallel",
               "--num", "750000", "--threads", "16", "--value-size", "1024",
               "--target-sst-size", "2147483648", "--compaction", "none",
               "--output", "json", "--latency-sample-every", "10",
               "--path", str(p / (name + "-db"))]
    if affinity == "pcore":
        command = ["taskset", "-c", "0-15"] + command
    fd = None
    if mode == "diagnostic":
        ctl = p / (name + "-control")
        ctl.mkdir()
        for pipe in ["control", "ack"]:
            os.mkfifo(ctl / pipe)
        env["KV_PERF_CONTROL_DIR"] = str(ctl)
        env["TOYKV_WAL_ATTRIBUTION"] = str(p / (name + "-phases.json"))
        fd = os.memfd_create("pcore-affinity-perf", 0)
        command = ["perf", "record", "-q", "-e", "cpu-clock:u", "-F", "499",
                   "--call-graph", "dwarf,4096", "--sample-cpu", "--delay=-1", "--control",
                   f"fifo:{ctl}/control,{ctl}/ack", "-o", f"/proc/self/fd/{fd}", "--"] + command
    else:
        env["TOYKV_WAL_CONTEXT"] = str(p / (name + "-context.json"))
    print("START", name, flush=True)
    started = time.time()
    with (p / (name + ".json")).open("w") as out, (p / (name + ".stderr")).open("w") as err:
        process = subprocess.Popen(command, stdout=out, stderr=err, env=env,
                                   pass_fds=[] if fd is None else [fd], start_new_session=True)
        try:
            code = process.wait(timeout=180)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            code = 124
    if fd is not None:
        os.lseek(fd, 0, 0)
        with (p / (name + ".perf")).open("wb") as out:
            while block := os.read(fd, 1024 * 1024):
                out.write(block)
        os.close(fd)
    (p / (name + "-status.json")).write_text(json.dumps({
        **item, "name": name, "command": command, "returncode": code,
        "started_at_unix": started, "finished_at_unix": time.time(),
    }, indent=2) + "\n")
    if code:
        print("FAILED", name, code, (p / (name + ".stderr")).read_text()[-1200:], flush=True)
        raise SystemExit(code)
    data = json.loads((p / (name + ".json")).read_text())
    result, latency = data["result"], data["latency"]
    wal = result["wal_pipeline"]
    mean_sync = wal["fdatasync_ns"] / wal["sync_count"] / 1e6
    print(json.dumps({"name": name, "puts_s": round(result["ops_per_sec"]),
                      "p99_ms": round(latency["p99_ms"], 3),
                      "cpu_s": round((result["process_cpu_user_ms"] + result["process_cpu_system_ms"]) / 1000, 3),
                      "syncs": wal["sync_count"], "groups": wal["commit_groups"],
                      "mean_sync_ms": round(mean_sync, 3),
                      "stall_flag": latency["p99_ms"] > 10 or mean_sync > 1}), flush=True)
    time.sleep(3)
print("DONE", flush=True)
