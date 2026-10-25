#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/liu/proj/toy-kv-engine
artifact_root="$repo_root/docs/wal/benchmarks/artifacts"
artifact_name=rfc-025-v7-v4-hash-hotpath-8writers-20261025
scratch_root="$repo_root/target/diag-v7-hash-hotpath-8writers-20261025"
db_root=/dev/shm/diag-v7-hash-hotpath-8writers-20261025
executable="$repo_root/target/release/write-perf"
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
printf 'source_record\tphase\tfilesystem\tworkload\tpair\tarm\trun_id\traw_sha256\tcommand\n' > "$manifest"

run_one() {
    local pair="$1"
    local arm="$2"
    local phase="$3"
    local name="$phase-tmpfs-write8-p$pair-$arm"
    local record="$scratch_root/records/$name.json"
    local db_path="$db_root/$name"
    local command

    if [[ "$arm" == v4 ]]; then
        command="$executable --suite legacy --preset smoke --wal --bench wal_concurrent --num 50000 --threads 8 --value-size 1024 --target-sst-size 1073741824 --latency-sample-every 100 --output json --wal-io-mode parallel --path $db_path"
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --num 50000 --threads 8 --value-size 1024 --target-sst-size 1073741824 \
            --latency-sample-every 100 --output json --wal-io-mode parallel \
            --path "$db_path" > "$record"
    elif [[ "$arm" == v7 ]]; then
        command="$executable --suite legacy --preset smoke --wal --bench wal_concurrent --pitr --pitr-format v7 --num 50000 --threads 8 --value-size 1024 --target-sst-size 1073741824 --latency-sample-every 100 --output json --wal-io-mode parallel --path $db_path"
        "$executable" --suite legacy --preset smoke --wal --bench wal_concurrent \
            --pitr --pitr-format v7 --num 50000 --threads 8 --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode parallel --path "$db_path" > "$record"
    else
        echo "unknown benchmark arm: $arm" >&2
        return 2
    fi

    python3 - "$record" "$arm" "$raw" "$manifest" "$name" "$phase" "$pair" "$command" <<'PY'
import hashlib
import json
import sys

record_path, arm, raw_path, manifest_path, name, phase, pair, command = sys.argv[1:]
with open(record_path, "rb") as stream:
    raw = stream.read().strip()
record = json.loads(raw)
expected_format = 4 if arm == "v4" else 7
assert record["params"]["wal_format_version"] == expected_format
assert record["params"]["wal_io_mode"] == "parallel"
assert record["params"]["threads"] == 8
assert record["result"]["ops"] == 50_000
with open(raw_path, "ab") as stream:
    stream.write(raw + b"\n")
with open(manifest_path, "a", encoding="utf-8") as stream:
    stream.write(
        f"{name}.json\t{phase}\ttmpfs\twrite8\t{pair}\t{arm}\t"
        f"{record['run_id']}\t{hashlib.sha256(raw).hexdigest()}\t{command}\n"
    )
PY

    rm -rf -- "$db_path" "$db_path.pitr-repository"
    printf '%s tmpfs write8 pair %s %s\n' "$phase" "$pair" "$arm" >&2
    sleep 3
}

run_one 0 v4 warmup
run_one 0 v7 warmup
for pair in 1 2 3; do
    if (( pair == 2 )); then
        run_one "$pair" v7 scored
        run_one "$pair" v4 scored
    else
        run_one "$pair" v4 scored
        run_one "$pair" v7 scored
    fi
done
