#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/liu/proj/toy-kv-engine
artifact_root="$repo_root/docs/wal/benchmarks/artifacts"
artifact_name=rfc-025-v7-mvcc-preparation-v4-parallel-tmpfs-1-2-4-8writers-20261025
scratch_root="$repo_root/target/diag-v7-mvcc-preparation-writers-1-2-4-8-20261025"
db_root=/dev/shm/diag-v7-mvcc-preparation-writers-1-2-4-8-20261025
executable="$repo_root/target/release/write-perf-v7-mvcc-preparation"
raw="$artifact_root/$artifact_name.jsonl"
manifest="$artifact_root/$artifact_name-runs.tsv"

if [[ -e "$scratch_root" || -e "$db_root" || -e "$raw" || -e "$manifest" ]]; then
    echo "refusing to overwrite existing benchmark data" >&2
    exit 73
fi
if [[ ! -x "$executable" ]]; then
    echo "missing release write-perf executable: $executable" >&2
    exit 69
fi

mkdir -p "$scratch_root/records" "$db_root"
trap 'rm -rf -- "$scratch_root" "$db_root"' EXIT
printf 'source_record\tphase\tfilesystem\tworkload\twriters\tpair\tarm\tbinary_sha256\trun_id\traw_sha256\tcommand\n' > "$manifest"

run_one() {
    local writers="$1"
    local pair="$2"
    local arm="$3"
    local phase="$4"
    local name="$phase-tmpfs-write$writers-p$pair-$arm"
    local record="$scratch_root/records/$name.json"
    local db_path="$db_root/$name"
    local command

    if [[ "$arm" == v4 ]]; then
        command="$executable --suite legacy --preset smoke --wal --bench wal_concurrent --num 100000 --threads $writers --value-size 1024 --target-sst-size 1073741824 --latency-sample-every 100 --output json --wal-io-mode parallel --path $db_path"
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --num 100000 --threads "$writers" --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode parallel --path "$db_path" > "$record"
    elif [[ "$arm" == v7 ]]; then
        command="$executable --suite legacy --preset smoke --wal --bench wal_concurrent --pitr --pitr-format v7 --num 100000 --threads $writers --value-size 1024 --target-sst-size 1073741824 --latency-sample-every 100 --output json --wal-io-mode parallel --path $db_path"
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --pitr --pitr-format v7 --num 100000 --threads "$writers" --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode parallel --path "$db_path" > "$record"
    else
        echo "unknown benchmark arm: $arm" >&2
        return 2
    fi

    python3 - "$record" "$arm" "$writers" "$executable" "$raw" "$manifest" "$name" "$phase" "$pair" "$command" <<'PY'
import hashlib
import json
import subprocess
import sys

record_path, arm, writers, executable, raw_path, manifest_path, name, phase, pair, command = sys.argv[1:]
with open(record_path, "rb") as stream:
    raw = stream.read().strip()
record = json.loads(raw)
expected_format = 4 if arm == "v4" else 7
assert record["params"]["wal_format_version"] == expected_format
assert record["params"]["wal_io_mode"] == "parallel"
assert record["params"]["threads"] == int(writers)
assert record["params"]["value_size"] == 1024
assert record["result"]["ops"] == 100_000
binary_sha256 = subprocess.check_output(["sha256sum", executable], text=True).split()[0]
with open(raw_path, "ab") as stream:
    stream.write(raw + b"\n")
with open(manifest_path, "a", encoding="utf-8") as stream:
    stream.write(
        f"{name}.json\t{phase}\ttmpfs\twrite\t{writers}\t{pair}\t{arm}\t"
        f"{binary_sha256}\t{record['run_id']}\t{hashlib.sha256(raw).hexdigest()}\t{command}\n"
    )
PY

    rm -rf -- "$db_path" "$db_path.pitr-repository"
    printf '%s tmpfs write%s pair %s %s\n' "$phase" "$writers" "$pair" "$arm" >&2
    sleep 3
}

for writers in 1 2 4 8; do
    run_one "$writers" 0 v4 warmup
    run_one "$writers" 0 v7 warmup
    for pair in 1 2 3; do
        if (( pair % 2 == 0 )); then
            run_one "$writers" "$pair" v7 scored
            run_one "$writers" "$pair" v4 scored
        else
            run_one "$writers" "$pair" v4 scored
            run_one "$writers" "$pair" v7 scored
        fi
    done
done
