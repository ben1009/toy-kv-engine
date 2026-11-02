from pathlib import Path
import hashlib
import json
import os
import subprocess

p = Path(__file__).resolve().parent
binary = p / "write-perf-producer-attribution"
schedule = [8, 31, 31, 8]
plan = {
    "schedule_cpu_ids": schedule,
    "reason": "Initial probe used CPU31. Topology exposes SMT cores at 5.1-5.3GHz max and single-thread cores at 3.9GHz max; compare one representative of each in reversed order before drawing a hash-backend conclusion.",
    "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
    "changes": "CPU affinity for isolated probe only; identical codec, BLAKE3, SHA, data, chunk sizes and iteration counts; no SSD or production runtime affinity changes.",
    "method": "One warmup plus seven reversed-stage rounds per invocation, 2000 iterations per stage, two invocations per CPU.",
}
assert not (p / "micro-cores-plan.json").exists()
(p / "micro-cores-plan.json").write_text(json.dumps(plan, indent=2) + "\n")
topology = subprocess.run(["lscpu", "-J"], capture_output=True, text=True, check=True)
(p / "cpu-topology.json").write_text(topology.stdout)
for index, cpu in enumerate(schedule):
    name = f"micro-core-{index:02}-cpu{cpu}"
    env = dict(os.environ, TOYKV_PREPARATION_MICRO=str(p / (name + ".json")))
    env.pop("TOYKV_WAL_ATTRIBUTION", None)
    cmd = ["taskset", "-c", str(cpu), str(binary), "--bench", "wal_batch_concurrent", "--output", "json", "--path", str(p / "unused-micro-db")]
    with (p / (name + ".stdout")).open("w") as out, (p / (name + ".stderr")).open("w") as err:
        result = subprocess.run(cmd, stdout=out, stderr=err, env=env, timeout=180)
    assert result.returncode == 0, name
    print("DONE", name, flush=True)
