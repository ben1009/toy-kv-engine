#!/usr/bin/env bash
set -euo pipefail
repo_root=/home/liu/proj/toy-kv-engine
out_root="$repo_root/target/rfc025-v7-vs-v4-first-leg-20261024"
executable="$repo_root/target/release/write-perf"

run_one() {
    local storage="$1"
    local workload="$2"
    local pair="$3"
    local arm="$4"
    local phase="$5"
    local db_root
    local record="$out_root/records/${phase}-${storage}-${workload}-p${pair}-${arm}.json"
    local db_path
    if [[ "$storage" == tmpfs ]]; then
        db_root=/dev/shm/rfc025-v7-vs-v4-first-leg-20261024
    else
        db_root="$out_root/ext4-db"
    fi
    db_path="$db_root/${phase}-${workload}-p${pair}-${arm}"
    if [[ "$workload" == write1 ]]; then
        if [[ "$arm" == v4 ]]; then
            "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
                --num 5000 --threads 1 --value-size 1024 --target-sst-size 1073741824 \
                --latency-sample-every 100 --output json --wal-io-mode parallel --path "$db_path" > "$record"
        else
            "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
                --pitr --pitr-format v7 --num 5000 --threads 1 --value-size 1024 \
                --target-sst-size 1073741824 --latency-sample-every 100 --output json \
                --wal-io-mode parallel --path "$db_path" > "$record"
        fi
    elif [[ "$workload" == write4 ]]; then
        if [[ "$arm" == v4 ]]; then
            "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
                --num 50000 --threads 4 --value-size 1024 --target-sst-size 1073741824 \
                --latency-sample-every 100 --output json --wal-io-mode parallel --path "$db_path" > "$record"
        else
            "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
                --pitr --pitr-format v7 --num 50000 --threads 4 --value-size 1024 \
                --target-sst-size 1073741824 --latency-sample-every 100 --output json \
                --wal-io-mode parallel --path "$db_path" > "$record"
        fi
    else
        echo "unknown workload: $workload" >&2
        return 2
    fi
    printf '%s %s %s %s %s\n' "$phase" "$storage" "$workload" "$pair" "$arm" >&2
    sleep 3
}

for storage in tmpfs ext4; do
    for workload in write1 write4; do
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
