#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/liu/proj/toy-kv-engine
out_root="$repo_root/target/diag-v7-coalesce-fast-8writers-20261025"
record_root="$out_root/records"
executable="$repo_root/target/release/write-perf"

if [[ -e "$out_root" || -e /dev/shm/diag-v7-coalesce-fast-8writers-20261025 ]]; then
    echo "refusing to overwrite existing benchmark data" >&2
    exit 73
fi

mkdir -p "$record_root"

run_one() {
    local storage="$1"
    local pair="$2"
    local arm="$3"
    local phase="$4"
    local record="$record_root/${phase}-${storage}-write8-p${pair}-${arm}.json"
    local db_root
    local db_path

    if [[ "$storage" == tmpfs ]]; then
        db_root=/dev/shm/diag-v7-coalesce-fast-8writers-20261025
    else
        db_root="$out_root/ext4-db"
    fi
    db_path="$db_root/${phase}-write8-p${pair}-${arm}"
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
            --num 50000 --threads 8 --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode parallel --path "$db_path" > "$record"
    elif [[ "$arm" == v7 ]]; then
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --pitr --pitr-format v7 --num 50000 --threads 8 --value-size 1024 \
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
    printf '%s %s write8 %s %s\n' "$phase" "$storage" "$pair" "$arm" >&2
    sleep 3
}

for storage in tmpfs ext4; do
    run_one "$storage" 0 v4 warmup
    run_one "$storage" 0 v7 warmup
    for pair in 1 2 3; do
        if (( pair == 2 )); then
            run_one "$storage" "$pair" v7 scored
            run_one "$storage" "$pair" v4 scored
        else
            run_one "$storage" "$pair" v4 scored
            run_one "$storage" "$pair" v7 scored
        fi
    done
done

rmdir /dev/shm/diag-v7-coalesce-fast-8writers-20261025 2>/dev/null || true
rmdir "$out_root/ext4-db" 2>/dev/null || true
