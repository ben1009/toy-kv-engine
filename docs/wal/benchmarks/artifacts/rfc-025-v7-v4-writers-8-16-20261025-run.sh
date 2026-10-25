#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/liu/proj/toy-kv-engine
out_root="$repo_root/target/rfc025-v7-vs-v4-first-leg-20261024"
record_root="$out_root/records"
executable="$repo_root/target/release/write-perf"

mkdir -p "$record_root"

run_one() {
    local storage="$1"
    local workload="$2"
    local pair="$3"
    local arm="$4"
    local phase="$5"
    local threads
    local db_root
    local record="$record_root/${phase}-${storage}-${workload}-p${pair}-${arm}.json"
    local db_path

    case "$workload" in
        write8) threads=8 ;;
        write16) threads=16 ;;
        *) echo "unknown workload: $workload" >&2; return 2 ;;
    esac

    if [[ "$storage" == tmpfs ]]; then
        db_root=/dev/shm/rfc025-v7-vs-v4-first-leg-20261024
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

    if [[ "$arm" == v4 ]]; then
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --num 50000 --threads "$threads" --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode parallel --path "$db_path" > "$record"
    elif [[ "$arm" == v7 ]]; then
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --pitr --pitr-format v7 --num 50000 --threads "$threads" --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode parallel --path "$db_path" > "$record"
    else
        echo "unknown arm: $arm" >&2
        return 2
    fi

    python3 - "$record" "$arm" <<'PY'
import json
import sys

record_path, arm = sys.argv[1:]
with open(record_path, encoding="utf-8") as stream:
    record = json.load(stream)
params = record["params"]
expected_format = 4 if arm == "v4" else 7
assert params["wal_format_version"] == expected_format, (arm, params["wal_format_version"])
assert params["wal_io_mode"] == "parallel", (arm, params["wal_io_mode"])
assert record["result"]["ops"] == 50000
PY

    rm -rf -- "$db_path" "$db_path.pitr-repository"
    printf '%s %s %s %s %s\n' "$phase" "$storage" "$workload" "$pair" "$arm" >&2
    sleep 3
}

for storage in tmpfs ext4; do
    for workload in write8 write16; do
        run_one "$storage" "$workload" 0 v4 warmup
        run_one "$storage" "$workload" 0 v7 warmup
        for pair in 1 2 3; do
            if (( pair == 2 )); then
                run_one "$storage" "$workload" "$pair" v7 scored
                run_one "$storage" "$workload" "$pair" v4 scored
            else
                run_one "$storage" "$workload" "$pair" v4 scored
                run_one "$storage" "$workload" "$pair" v7 scored
            fi
        done
    done
done

rmdir /dev/shm/rfc025-v7-vs-v4-first-leg-20261024 2>/dev/null || true
rmdir "$out_root/ext4-db" 2>/dev/null || true
