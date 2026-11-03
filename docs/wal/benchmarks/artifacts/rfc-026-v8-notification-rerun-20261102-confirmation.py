"""Run or resume the frozen notification confirmation cohort.

A resumed cohort can span a different storage regime; inspect the recorded
start times and interruption archive before making any paired comparison.
"""

from pathlib import Path
import fcntl
import hashlib
import json
import os
import subprocess
import time


def ensure_frozen_plan(plan_path, plan):
    if plan_path.exists():
        if json.loads(plan_path.read_text()) != plan:
            raise ValueError("existing confirmation plan differs from the requested plan")
        return False
    with plan_path.open("x") as output:
        output.write(json.dumps(plan, indent=2) + "\n")
    return True


def sample_paths(directory, name):
    return [
        directory / f"{name}.json",
        directory / f"{name}.stderr",
        directory / f"{name}-context.json",
        directory / f"{name}-status.json",
    ]


def sample_completed(directory, name, command):
    result_path, _, context_path, status_path = sample_paths(directory, name)
    if not status_path.exists():
        return False
    try:
        status = json.loads(status_path.read_text())
    except (OSError, ValueError):
        return False
    if status.get("returncode") == 0 and (
        status.get("name") != name or status.get("command") != command
    ):
        raise ValueError(f"completed sample {name} does not match its frozen command")
    if not result_path.exists():
        return False
    try:
        result = json.loads(result_path.read_text())
        if context_path.exists():
            json.loads(context_path.read_text())
        return (
            status["name"] == name
            and status["command"] == command
            and status["returncode"] == 0
            and result["result"]["wal_pipeline"]["sync_count"] > 0
            and result["result"]["ops"] > 0
            and isinstance(result["latency"]["p99_ms"], (int, float))
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


def archive_incomplete_sample(directory, name):
    paths = sample_paths(directory, name)
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    root = directory / "confirmation-interrupted" / name
    root.mkdir(parents=True, exist_ok=True)
    index = 0
    while True:
        destination = root / f"attempt-{index:03}"
        try:
            destination.mkdir()
            break
        except FileExistsError:
            index += 1
    for source in existing:
        source.rename(destination / source.name)
    return destination


def run_cohort(directory):
    if "DONE" not in (directory / "run.log").read_text().splitlines():
        raise ValueError("the original rerun cohort has not completed")
    original_plan = json.loads((directory / "plan.json").read_text())
    bins = {arm: Path(path) for arm, path in original_plan["binary_paths"].items()}
    hashes = {arm: hashlib.sha256(binary.read_bytes()).hexdigest() for arm, binary in bins.items()}
    if hashes != original_plan["binary_sha256"]:
        raise ValueError("benchmark executable hashes differ from the original plan")
    cases = [("batch", 8), ("point", 8), ("batch", 16)]
    schedule = []
    for round_index in range(2):
        for kind, writers in cases if round_index == 0 else list(reversed(cases)):
            arms = ["baseline", "kept"] if round_index == 0 else ["kept", "baseline"]
            for arm in arms:
                schedule.append([kind, writers, arm])
    plan = {
        "schedule": schedule,
        "binary_sha256": hashes,
        "binary_paths": original_plan["binary_paths"],
        "purpose": "One bounded follow-up for missing eligible concurrent arms in the first rerun; no code, parameters or filter changes. Reverse both case and arm order. Preserve first cohort separately. Keep implementation regardless of result.",
        "initial_cooldown_seconds": 45,
        "between_runs_seconds": 3,
        "selection": original_plan["selection"],
        "stall_filter": original_plan["stall_filter"],
        "operations": original_plan["operations"],
    }
    created = ensure_frozen_plan(directory / "confirmation-plan.json", plan)
    if not created:
        with (directory / "confirmation-resume-log.jsonl").open("a") as log:
            log.write(json.dumps({"event": "resume", "at_unix": time.time()}) + "\n")
    print("COOLDOWN 45 seconds", flush=True)
    time.sleep(45)
    failures = []
    for sample_index, (kind, writers, arm) in enumerate(schedule):
        name = f"confirmation-{sample_index:02}-{kind}-{arm}-{writers}"
        result_path, stderr_path, context_path, status_path = sample_paths(directory, name)
        env = dict(os.environ, TOYKV_WAL_CONTEXT=str(context_path))
        cmd = [
            str(bins[arm]), "--bench", "wal_batch_concurrent" if kind == "batch" else "wal_concurrent",
            "--pitr", "--pitr-format", "v8", "--wal-io-mode", "parallel",
            "--num", str(750000 if kind == "batch" else 20000),
            "--threads", str(writers), "--value-size", "1024", "--target-sst-size", "2147483648",
            "--compaction", "none", "--output", "json", "--latency-sample-every", "10",
            "--path", str(directory / (name + "-db")),
        ]
        if kind == "batch":
            cmd += ["--wal-batch-size", "64"]
        if sample_completed(directory, name, cmd):
            print("SKIP completed", name, flush=True)
            continue
        archived = archive_incomplete_sample(directory, name)
        if archived:
            with (directory / "confirmation-resume-log.jsonl").open("a") as log:
                log.write(json.dumps({"event": "retry", "name": name, "archived": str(archived), "at_unix": time.time()}) + "\n")
        print("START", name, flush=True)
        started_at = time.time()
        with result_path.open("x") as out, stderr_path.open("x") as err:
            try:
                returncode = subprocess.run(cmd, stdout=out, stderr=err, env=env, timeout=180).returncode
            except subprocess.TimeoutExpired:
                returncode = 124
        status_path.write_text(json.dumps({
            "name": name, "command": cmd, "returncode": returncode,
            "started_at_unix": started_at, "finished_at_unix": time.time(),
        }, indent=2) + "\n")
        if returncode:
            print("FAILED", name, returncode, flush=True)
            failures.append(name)
            time.sleep(3)
            continue
        d = json.loads(result_path.read_text())
        q, latency = d["result"], d["latency"]
        w = q["wal_pipeline"]
        mean = w["fdatasync_ns"] / w["sync_count"] / 1e6
        context = json.loads(context_path.read_text()) if context_path.exists() else {}
        print(json.dumps({
            "name": name, "puts_s": round(q["ops_per_sec"]), "p99_ms": round(latency["p99_ms"], 3),
            "cpu_s": round((q["process_cpu_user_ms"] + q["process_cpu_system_ms"]) / 1000, 3),
            "voluntary": context.get("voluntary"), "eventfd_writes": w["worker_eventfd_writes"],
            "syncs": w["sync_count"], "mean_sync_ms": round(mean, 3),
            "stall_flag": latency["p99_ms"] > 10 or mean > 1,
        }), flush=True)
        time.sleep(3)
    if failures:
        raise RuntimeError(f"confirmation samples failed and can be retried: {failures}")
    print("DONE", flush=True)


def main():
    directory = Path(__file__).resolve().parent
    # One process owns the cohort while preserving incomplete attempts.
    with (directory / "confirmation.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run_cohort(directory)


if __name__ == "__main__":
    main()
