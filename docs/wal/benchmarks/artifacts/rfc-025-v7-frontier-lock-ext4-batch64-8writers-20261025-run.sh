#!/usr/bin/env bash
set -euo pipefail

repo_root=/home/liu/proj/toy-kv-engine
artifact_root="$repo_root/docs/wal/benchmarks/artifacts"
artifact_name=rfc-025-v7-frontier-lock-ext4-batch64-8writers-20261025
scratch_root="/home/liu/proj/toy-kv-engine/target/wal-opt/diag-v7-frontier-lock-ext4-batch64-8writers-20261025"
old_executable="$repo_root/target/release/write-perf-v7-cached-extents"
new_executable="$repo_root/target/release/write-perf-v7-frontier-lock"
raw="$artifact_root/$artifact_name.jsonl"
manifest="$artifact_root/$artifact_name-runs.tsv"

if [[ -e "$scratch_root" || -e "$raw" || -e "$manifest" ]]; then
    echo "refusing to overwrite existing benchmark data" >&2
    exit 73
fi
for executable in "$old_executable" "$new_executable"; do
    if [[ ! -x "$executable" ]]; then
        echo "missing release write-perf executable: $executable" >&2
        exit 69
    fi
done

mkdir -p "$scratch_root/records"
trap 'rm -rf -- "$scratch_root"' EXIT
printf 'source_record\tphase\tfilesystem\tworkload\twriters\toperations\tpair\tarm\tbinary_sha256\trun_id\traw_sha256\tcommand\tnvme_temperature_millic\n' > "$manifest"

run_one() {
    local pair="$1" arm="$2" phase="$3"
    local executable format name record db_path command
    case "$arm" in
        old_v4) executable="$old_executable"; format=v4 ;;
        new_v4) executable="$new_executable"; format=v4 ;;
        old_v7) executable="$old_executable"; format=v7 ;;
        new_v7) executable="$new_executable"; format=v7 ;;
        *) echo "unknown benchmark arm: $arm" >&2; return 2 ;;
    esac
    name="$phase-ext4-batch64-write8-p$pair-$arm"
    record="$scratch_root/records/$name.json"
    db_path="$scratch_root/$name-db"
    if [[ "$format" == v4 ]]; then
        command="$executable --suite legacy --preset smoke --wal --bench wal_batch_concurrent --wal-batch-size 64 --num 100000 --threads 8 --value-size 1024 --target-sst-size 1073741824 --latency-sample-every 100 --output json --wal-io-mode parallel --path $db_path"
        "$executable" --suite legacy --preset smoke --wal --bench wal_batch_concurrent --wal-batch-size 64 \
            --num 100000 --threads 8 --value-size 1024 --target-sst-size 1073741824 \
            --latency-sample-every 100 --output json --wal-io-mode parallel \
            --path "$db_path" > "$record"
    else
        command="$executable --suite legacy --preset smoke --wal --bench wal_batch_concurrent --wal-batch-size 64 --pitr --pitr-format v7 --num 100000 --threads 8 --value-size 1024 --target-sst-size 1073741824 --latency-sample-every 100 --output json --wal-io-mode parallel --path $db_path"
        "$executable" --suite legacy --preset smoke --wal --bench wal_batch_concurrent --wal-batch-size 64 \
            --pitr --pitr-format v7 --num 100000 --threads 8 --value-size 1024 \
            --target-sst-size 1073741824 --latency-sample-every 100 --output json \
            --wal-io-mode parallel --path "$db_path" > "$record"
    fi

    python3 - "$record" "$arm" "$executable" "$raw" "$manifest" "$name" "$phase" "$pair" "$command" <<'PY'
import hashlib
import json
import subprocess
import sys
from pathlib import Path

record_path, arm, executable, raw_path, manifest_path, name, phase, pair, command = sys.argv[1:]
with open(record_path, "rb") as stream:
    raw = stream.read().strip()
record = json.loads(raw)
expected_format = 4 if arm.endswith("v4") else 7
assert record["params"]["wal_format_version"] == expected_format
assert record["params"]["wal_io_mode"] == "parallel"
assert record["params"]["threads"] == 8
assert record["params"]["value_size"] == 1024
assert record["result"]["ops"] == 100_000
temperature = "unavailable"
for hwmon in Path("/sys/class/hwmon").glob("hwmon*"):
    try:
        if (hwmon / "name").read_text().strip() == "nvme":
            temperature = (hwmon / "temp1_input").read_text().strip()
            break
    except (FileNotFoundError, PermissionError):
        pass
binary_sha256 = subprocess.check_output(["sha256sum", executable], text=True).split()[0]
with open(raw_path, "ab") as stream:
    stream.write(raw + b"\n")
with open(manifest_path, "a", encoding="utf-8") as stream:
    stream.write(
        f"{name}.json\t{phase}\text4\tbatch64-write8\t8\t100000\t{pair}\t{arm}\t"
        f"{binary_sha256}\t{record['run_id']}\t{hashlib.sha256(raw).hexdigest()}\t{command}\t{temperature}\n"
    )
PY

    rm -rf -- "$db_path" "$db_path.pitr-repository"
    printf '%s ext4 batch64-write8 pair %s %s\n' "$phase" "$pair" "$arm" >&2
    sleep 10
}

sleep 30

for arm in old_v4 old_v7 new_v4 new_v7; do
    run_one 0 "$arm" warmup
done
run_one 1 old_v4 scored
run_one 1 new_v4 scored
run_one 1 old_v7 scored
run_one 1 new_v7 scored
run_one 2 new_v7 scored
run_one 2 old_v7 scored
run_one 2 new_v4 scored
run_one 2 old_v4 scored
run_one 3 old_v4 scored
run_one 3 new_v4 scored
run_one 3 old_v7 scored
run_one 3 new_v7 scored
run_one 4 new_v7 scored
run_one 4 old_v7 scored
run_one 4 new_v4 scored
run_one 4 old_v4 scored
run_one 5 old_v4 scored
run_one 5 new_v4 scored
run_one 5 old_v7 scored
run_one 5 new_v7 scored
