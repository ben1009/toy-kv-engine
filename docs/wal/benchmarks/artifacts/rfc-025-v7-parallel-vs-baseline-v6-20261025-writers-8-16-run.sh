#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/liu/proj/toy-kv-engine
out_root="$repo_root/target/rfc025-v7-parallel-vs-baseline-v6-20261025"
record_root="$out_root/records"
baseline="$repo_root/target/rfc025-v4-pre-v7/target/release/write-perf"
candidate="$repo_root/target/release/write-perf"

mkdir -p "$record_root"

run_one() {
    local storage="$1"
    local workload="$2"
    local pair="$3"
    local arm="$4"
    local phase="$5"
    local executable
    local mode
    local threads
    local db_root
    local record="$record_root/${phase}-${storage}-${workload}-p${pair}-${arm}.json"
    local db_path

    if [[ "$arm" == baseline-v6-leader ]]; then
        executable="$baseline"
        mode=leader
    elif [[ "$arm" == candidate-v7-parallel ]]; then
        executable="$candidate"
        mode=parallel
    else
        echo "unknown arm: $arm" >&2
        return 2
    fi

    case "$workload" in
        write8) threads=8 ;;
        write16) threads=16 ;;
        *) echo "unknown workload: $workload" >&2; return 2 ;;
    esac

    if [[ "$storage" == tmpfs ]]; then
        db_root=/dev/shm/rfc025-v7-parallel-vs-baseline-v6-20261025
    else
        db_root="$out_root/ext4-db"
    fi
    db_path="$db_root/${phase}-${workload}-p${pair}-${arm}"

    case "$db_path" in
        "$db_root"/*) ;;
        *) echo "refusing unexpected database path: $db_path" >&2; return 70 ;;
    esac
    if [[ -e "$record" || -e "$db_path" || -e "$db_path.pitr-repository" ]]; then
        echo "refusing to overwrite existing benchmark data for $db_path" >&2
        return 73
    fi

    if [[ "$arm" == baseline-v6-leader ]]; then
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --pitr --num 50000 --threads "$threads" --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode "$mode" --path "$db_path" > "$record"
    else
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --pitr --pitr-format v7 --num 50000 --threads "$threads" --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode "$mode" --path "$db_path" > "$record"
    fi

    python3 - "$record" "$arm" <<'PY'
import json
import sys

record_path, arm = sys.argv[1:]
with open(record_path, encoding="utf-8") as stream:
    record = json.load(stream)
params = record["params"]
expected_mode = "leader" if arm == "baseline-v6-leader" else "parallel"
assert params["wal_io_mode"] == expected_mode, (arm, params["wal_io_mode"])
if arm == "candidate-v7-parallel":
    assert params["wal_format_version"] == 7, params["wal_format_version"]
assert record["result"]["ops"] == 50000
PY

    rm -rf -- "$db_path" "$db_path.pitr-repository"
    printf '%s %s %s %s %s\n' "$phase" "$storage" "$workload" "$pair" "$arm" >&2
    sleep 3
}

for storage in tmpfs ext4; do
    for workload in write8 write16; do
        run_one "$storage" "$workload" 0 baseline-v6-leader warmup
        run_one "$storage" "$workload" 0 candidate-v7-parallel warmup
        for pair in 1 2 3; do
            if (( pair == 2 )); then
                run_one "$storage" "$workload" "$pair" candidate-v7-parallel scored
                run_one "$storage" "$workload" "$pair" baseline-v6-leader scored
            else
                run_one "$storage" "$workload" "$pair" baseline-v6-leader scored
                run_one "$storage" "$workload" "$pair" candidate-v7-parallel scored
            fi
        done
    done
done

rmdir /dev/shm/rfc025-v7-parallel-vs-baseline-v6-20261025 2>/dev/null || true
rmdir "$out_root/ext4-db" 2>/dev/null || true
