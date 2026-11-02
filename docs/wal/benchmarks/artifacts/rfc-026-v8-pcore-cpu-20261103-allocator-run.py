from pathlib import Path
import hashlib
import json
import os
import signal
import subprocess
import sys
import time


p = Path(__file__).resolve().parent
repo = p.parent.parent.parent
binary = repo / "target/rfc-026-v8-producer-attribution-20261102/write-perf-producer-attribution"
expected = "fbb90ea8a6f20bf476760e79ae60e04cda3edae04138d45ae5a9e6346da299f1"
assert hashlib.sha256(binary.read_bytes()).hexdigest() == expected
assert sorted(os.sched_getaffinity(0)) == list(range(32))
smoke = "--smoke" in sys.argv
schedule = ["system", "mimalloc"] if smoke else ["system", "mimalloc", "mimalloc", "system"]
num = 4096 if smoke else 750000
plan = {"purpose": "Reversible V8 allocator CPU diagnostic: baseline glibc versus installed mimalloc C overrides, including aligned buffers. perf collector does not inherit the override. Not a production allocator adoption or qualified SSD gain.",
        "binary": str(binary), "binary_sha256": expected,
        "runtime_revision": "b8da402a276d7969f4c40ca99787a10a63d93f33",
        "memory_metric": "wait4 ru_maxrss for the perf command tree: maximum individual process RSS, not the sum or a timed-only peak. Includes the benchmark and profiler.",
        "allocator_activation": json.loads((p / "activation.json").read_text()),
        "override": "LD_PRELOAD=/usr/lib/libmimalloc.so.2 on the benchmark child only; no package install or host allocator setting changes",
        "schedule": schedule, "puts_per_capture": num, "writers": 16, "batch": 64,
        "value_bytes": 1024, "wal_io_mode": "parallel", "target_sst_size": 2147483648,
        "compaction": "none", "latency_sample_every": 10,
        "placement": "taskset 0-15 on benchmark; inherited by application, WAL and engine threads. Verify zero E-core userspace samples. No global scheduler or device changes.",
        "perf_mmap_pages": 64,
        "profiling": "Preserved phase scopes sample 1/64 calls per thread with CPU and wall clocks; inclusive scopes are not summed with children. perf cpu-clock:u 499Hz, DWARF4096, sample-cpu, enabled only during timed writes via existing control FIFOs. perf stream remains in memfd until workload completion.",
        "storage_flag": "p99_ms >10 OR mean fdatasync_ms >1; preserve every diagnostic and label flags. Diagnostics do not enter best-score tables even when eligible.",
        "comparison": "Inspect CPU hotspots repeated across reversed format order, not just relative percentages. Stalls change concurrency and CPU work, so phase differences do not by themselves prove a source-level regression.",
        "initial_idle_seconds": 0 if smoke else 15, "between_captures_seconds": 3,
        "runtime_changed": False, "device_settings_changed": False}
plan_path = p / ("smoke-plan.json" if smoke else "plan.json")
assert not plan_path.exists()
plan_path.write_text(json.dumps(plan, indent=2) + "\n")
if not smoke:
    assert (p / "smoke-checks.json").exists()
    print("COOLDOWN 15s", flush=True)
    time.sleep(15)
for index, allocator in enumerate(schedule):
    fmt = "v8"
    name = ("smoke-" if smoke else "") + f"{index:02}-{allocator}"
    env = dict(os.environ)
    for key in list(env):
        if key == "LD_PRELOAD" or key.startswith("MIMALLOC_"):
            env.pop(key, None)
    for key in ["TOYKV_PREPARATION_MICRO", "TOYKV_WAL_CONTEXT", "TOYKV_AFFINITY_MODE", "TOYKV_AFFINITY_REPORT", "TOYKV_IOWQ_PCORE", "TOYKV_WRITER_PCORE"]:
        env.pop(key, None)
    ctl = p / (name + "-control")
    ctl.mkdir()
    for pipe in ["control", "ack"]:
        os.mkfifo(ctl / pipe)
    env["KV_PERF_CONTROL_DIR"] = str(ctl)
    env["TOYKV_WAL_ATTRIBUTION"] = str(p / (name + "-phases.json"))
    fd = os.memfd_create("v8-pcore-cpu", 0)
    command = ["perf", "record", "-m", "64", "-e", "cpu-clock:u", "-F", "499",
               "--call-graph", "dwarf,4096", "--sample-cpu", "--delay=-1", "--control",
               f"fifo:{ctl}/control,{ctl}/ack", "-o", f"/proc/self/fd/{fd}", "--",
               "taskset", "-c", "0-15", "env",
               *(["LD_PRELOAD=/usr/lib/libmimalloc.so.2", "MIMALLOC_VERBOSE=0", "MIMALLOC_SHOW_STATS=0"] if allocator == "mimalloc" else []), str(binary), "--bench", "wal_batch_concurrent",
               "--wal-batch-size", "64", "--pitr", "--pitr-format", fmt,
               "--wal-io-mode", "parallel", "--num", str(num), "--threads", "16",
               "--value-size", "1024", "--target-sst-size", "2147483648", "--compaction", "none",
               "--output", "json", "--latency-sample-every", "10", "--path", str(p / (name + "-db"))]
    print("START", name, flush=True)
    started = time.time()
    with (p / (name + ".json")).open("w") as out, (p / (name + ".stderr")).open("w") as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, env=env, pass_fds=(fd,), start_new_session=True)
        peak_rss_kib = None
        try:
            deadline = time.monotonic() + 180
            while True:
                waited, wait_status, usage = os.wait4(child.pid, os.WNOHANG)
                if waited:
                    code = os.waitstatus_to_exitcode(wait_status)
                    child.returncode = code
                    peak_rss_kib = usage.ru_maxrss
                    break
                if time.monotonic() >= deadline:
                    raise subprocess.TimeoutExpired(command, 180)
                time.sleep(0.02)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            code = 124
    os.lseek(fd, 0, 0)
    with (p / (name + ".perf")).open("wb") as out:
        while block := os.read(fd, 1024 * 1024):
            out.write(block)
    os.close(fd)
    status = {"name": name, "format": fmt, "allocator": allocator, "command": command, "returncode": code,
              "started_at_unix": started, "finished_at_unix": time.time(),
              "command_tree_max_individual_rss_kib": peak_rss_kib}
    (p / (name + "-status.json")).write_text(json.dumps(status, indent=2) + "\n")
    assert code == 0, status
    raw = json.loads((p / (name + ".json")).read_text())
    phases = json.loads((p / (name + "-phases.json")).read_text())
    assert phases["threads"] and phases["phase_names"]
    q = raw["result"]
    assert q["ops"] == num
    if fmt == "v8":
        assert q["wal_v8_digest"]["payload_hash_bytes"] == num * 1050
        assert q["wal_v8_digest"]["post_encode_hash_reread_bytes"] == 0
    w = q["wal_pipeline"]
    mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
    print(json.dumps({"name": name, "puts_s": q["ops_per_sec"], "p99_ms": raw["latency"]["p99_ms"],
                      "cpu_s": (q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000,
                      "syncs": w["sync_count"], "mean_sync_ms": mean,
                      "storage_flag": raw["latency"]["p99_ms"] > 10 or mean > 1,
                      "diagnostic_only": True}), flush=True)
    time.sleep(3)
if smoke:
    (p / "smoke-checks.json").write_text(json.dumps({"captures": 2, "all_commands_successful": True,
                                                       "timed_perf_and_phase_outputs_present": True,
                                                       "operation_and_digest_assertions_pass": True,
                                                       "normal_performance_scores": False}, indent=2) + "\n")
print("DONE", flush=True)
