#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 9 ]]; then
    echo "usage: $0 <commit> <num> <project> <zone> <instance> <safe-deadline-epoch> <machine-type> <writers-csv> <suite>" >&2
    exit 64
fi

COMMIT="$1"
NUM="$2"
PROJECT="$3"
ZONE="$4"
INSTANCE="$5"
SAFE_DEADLINE_EPOCH="$6"
MACHINE_TYPE="$7"
WRITERS_CSV="$8"
SUITE="$9"

case "$MACHINE_TYPE" in
    c4-standard-8-lssd) SSD_COUNT=1; SSD_TOTAL_GIB=375 ;;
    c4-standard-16-lssd) SSD_COUNT=2; SSD_TOTAL_GIB=750 ;;
    *) echo "unsupported machine type: $MACHINE_TYPE" >&2; exit 64 ;;
esac
IFS=',' read -r -a WRITERS <<<"$WRITERS_CSV"
if (( ${#WRITERS[@]} == 0 )); then
    echo "at least one writer count is required" >&2
    exit 64
fi
declare -A SEEN_WRITERS=()
for writer in "${WRITERS[@]}"; do
    if [[ ! "$writer" =~ ^(1|2|4|8|16)$ ]]; then
        echo "unsupported writer count: $writer" >&2
        exit 64
    fi
    if [[ -n "${SEEN_WRITERS[$writer]:-}" ]]; then
        echo "duplicate writer count: $writer" >&2
        exit 64
    fi
    SEEN_WRITERS[$writer]=1
done
if [[ "$SUITE" != "batch64" && "$SUITE" != "rfc026-full" \
    && "$SUITE" != "v8-small-ticket-profile" ]]; then
    echo "unsupported benchmark suite: $SUITE" >&2
    exit 64
fi

if [[ ! "$COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "invalid full commit SHA: $COMMIT" >&2
    exit 64
fi
if [[ ! "$NUM" =~ ^[0-9]+$ ]] || (( NUM < 64 )); then
    echo "num must be >=64" >&2
    exit 64
fi
if [[ ! "$SAFE_DEADLINE_EPOCH" =~ ^[0-9]+$ ]] || (( SAFE_DEADLINE_EPOCH < 1 )); then
    echo "safe VM deadline must be a positive Unix epoch" >&2
    exit 64
fi

BOOT_LOG=/var/log/gcp-free-trial-wal-bench.log
exec > >(tee -a "$BOOT_LOG") 2>&1
echo "[$(date --iso-8601=seconds)] starting GCP WAL benchmark setup"

apt-get update
INSTALL_PACKAGES=(
    build-essential ca-certificates clang cmake curl git libclang-dev libssl-dev mdadm pkg-config
    python3 sudo time util-linux xz-utils
)
if [[ "$SUITE" == "v8-small-ticket-profile" ]]; then
    INSTALL_PACKAGES+=(linux-tools-common linux-tools-gcp)
fi
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${INSTALL_PACKAGES[@]}"

id bench >/dev/null 2>&1 || useradd --create-home --shell /bin/bash bench
usermod -aG sudo bench
printf 'bench ALL=(ALL) NOPASSWD:ALL\n' >/etc/sudoers.d/90-wal-bench
chmod 0440 /etc/sudoers.d/90-wal-bench

mapfile -t local_ssd_links < <(find /dev/disk/by-id -maxdepth 1 -type l -name 'google-local-nvme-ssd-[0-9]*' -print | sort)
if (( ${#local_ssd_links[@]} != SSD_COUNT )); then
    echo "expected $SSD_COUNT Google NVMe Local SSD symlinks, found ${#local_ssd_links[@]}" >&2
    printf '%s\n' "${local_ssd_links[@]}" >&2
    exit 1
fi
local_ssds=()
SSD_BYTES=0
MIN_BYTES=$((375 * 1024 * 1024 * 1024))
for ((index = 0; index < SSD_COUNT; index++)); do
    expected_link="google-local-nvme-ssd-$index"
    if [[ "$(basename "${local_ssd_links[$index]}")" != "$expected_link" ]]; then
        echo "refusing unexpected Local SSD device identifier: ${local_ssd_links[$index]}" >&2
        exit 1
    fi
    device="$(readlink -f "${local_ssd_links[$index]}")"
    if [[ ! -b "$device" ]]; then
        echo "Local SSD link does not resolve to a block device: $device" >&2
        exit 1
    fi
    device_bytes="$(blockdev --getsize64 "$device")"
    if (( device_bytes < MIN_BYTES )); then
        echo "Local SSD is smaller than 375 GiB: $device ($device_bytes bytes)" >&2
        exit 1
    fi
    mounts="$(lsblk -nr -o MOUNTPOINT "$device")"
    if [[ -n "$(tr -d '[:space:]' <<<"$mounts")" ]]; then
        echo "refusing to format a mounted Local SSD: $device" >&2
        exit 1
    fi
    signatures="$(wipefs --no-act --noheadings --output TYPE "$device")"
    if [[ -n "$signatures" ]]; then
        echo "refusing to format a Local SSD with existing filesystem signatures: $device: $signatures" >&2
        exit 1
    fi
    local_ssds+=("$device")
    SSD_BYTES=$((SSD_BYTES + device_bytes))
done
if (( SSD_COUNT == 1 )); then
    WAL_DEVICE="${local_ssds[0]}"
else
    WAL_DEVICE=/dev/md/walbench
    if [[ -e "$WAL_DEVICE" ]]; then
        echo "refusing to reuse existing RAID device: $WAL_DEVICE" >&2
        exit 1
    fi
    mdadm --create "$WAL_DEVICE" --run --assume-clean --level=0 --raid-devices="$SSD_COUNT" "${local_ssds[@]}"
    udevadm settle
fi

RESULTS=/mnt/gcp-wal-bench/results
WORK=/mnt/gcp-wal-bench/work
mkdir -p /mnt/gcp-wal-bench "$RESULTS" "$WORK"
mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "$WAL_DEVICE"
mount -o noatime "$WAL_DEVICE" /mnt/gcp-wal-bench
mkdir -p "$RESULTS" "$WORK"
chown -R bench:bench /mnt/gcp-wal-bench
exec > >(tee -a "$RESULTS/setup.log") 2>&1

cat >"$RESULTS/host.txt" <<EOF
project=$PROJECT
zone=$ZONE
instance=$INSTANCE
machine_type=$MACHINE_TYPE
local_ssd_count=$SSD_COUNT
local_ssd_devices=${local_ssds[*]}
benchmark_filesystem_device=$WAL_DEVICE
local_ssd_bytes=$SSD_BYTES
local_ssd_layout=$([[ "$SSD_COUNT" == 1 ]] && echo single-device || echo raid0)
commit=$COMMIT
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
kernel=$(uname -srvm)
EOF
cp /etc/os-release "$RESULTS/os-release.txt"
uname -a >"$RESULTS/uname.txt"
lscpu >"$RESULTS/lscpu.txt"
lsblk -o NAME,TYPE,SIZE,FSTYPE,MOUNTPOINT,MODEL,SERIAL >"$RESULTS/lsblk.txt"
findmnt /mnt/gcp-wal-bench >"$RESULTS/findmnt.txt"
df -hT /mnt/gcp-wal-bench >"$RESULTS/df.txt"

chown -R bench:bench /mnt/gcp-wal-bench
chown bench:bench "$BOOT_LOG"
set +e
runuser -u bench -- env \
    BENCH_COMMIT="$COMMIT" \
    BENCH_PROJECT="$PROJECT" \
    BENCH_ZONE="$ZONE" \
    BENCH_INSTANCE="$INSTANCE" \
    BENCH_NUM="$NUM" \
    BENCH_REPO_URL="https://github.com/ben1009/toy-kv-engine.git" \
    BENCH_RESULTS="$RESULTS" \
    BENCH_WORK="$WORK" \
    BENCH_SAFE_DEADLINE_EPOCH="$SAFE_DEADLINE_EPOCH" \
    BENCH_MACHINE_TYPE="$MACHINE_TYPE" \
    BENCH_SSD_COUNT="$SSD_COUNT" \
    BENCH_SSD_TOTAL_GIB="$SSD_TOTAL_GIB" \
    BENCH_DEVICE="$WAL_DEVICE" \
    BENCH_WRITERS="$WRITERS_CSV" \
    BENCH_SUITE="$SUITE" \
    BENCH_BOOT_LOG="$BOOT_LOG" \
    bash -s <<'BENCH_USER_SCRIPT'
set -Eeuo pipefail

source "$HOME/.cargo/env" 2>/dev/null || true
if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
source "$HOME/.cargo/env"
IFS=',' read -r -a WRITER_COUNTS <<<"$BENCH_WRITERS"
if (( ${#WRITER_COUNTS[@]} == 0 )); then
    echo "no writer counts were passed to the benchmark shell" >&2
    exit 64
fi

REPO="$BENCH_WORK/toy-kv-engine"
mkdir -p "$REPO"
git -C "$REPO" init --quiet
git -C "$REPO" remote add origin "$BENCH_REPO_URL"
git -C "$REPO" fetch --depth=1 origin "$BENCH_COMMIT"
git -C "$REPO" checkout --quiet --detach FETCH_HEAD
ACTUAL_COMMIT="$(git -C "$REPO" rev-parse HEAD)"
if [[ "$ACTUAL_COMMIT" != "$BENCH_COMMIT" ]]; then
    echo "checked out $ACTUAL_COMMIT instead of requested $BENCH_COMMIT" >&2
    exit 1
fi
if [[ ! -r /tmp/gcp_wal_matrix.py ]]; then
    echo "benchmark matrix module was not uploaded" >&2
    exit 1
fi
cp /tmp/gcp_wal_matrix.py "$REPO/scripts/gcp_wal_matrix.py"

TOOLCHAIN="$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$REPO/rust-toolchain")"
if [[ -z "$TOOLCHAIN" ]]; then
    echo "could not read pinned Rust toolchain from rust-toolchain" >&2
    exit 1
fi
rustup toolchain install "$TOOLCHAIN" --profile minimal

cd "$REPO"
export PYTHONPATH="$REPO/scripts${PYTHONPATH:+:$PYTHONPATH}"
BENCH_FEATURES=bench
if [[ "$BENCH_SUITE" == "v8-small-ticket-profile" ]]; then
    BENCH_FEATURES=bench,hotpath-profile
fi
if [[ "$BENCH_SUITE" == "v8-small-ticket-profile" ]]; then
    if ! command -v perf >/dev/null 2>&1; then
        echo "perf is not installed; profiling suite requires perf" >&2
        exit 1
    fi
    perf version >"$BENCH_RESULTS/perf-version.txt" 2>&1
fi
cargo "+$TOOLCHAIN" build --locked --release --package kv-engine --features "$BENCH_FEATURES" --bin write-perf
BINARY="$REPO/target/release/write-perf"
BINARY_SHA256="$(sha256sum "$BINARY" | cut -d' ' -f1)"
printf '%s\n' "$ACTUAL_COMMIT" >"$BENCH_RESULTS/commit.txt"
printf '%s\n' "$TOOLCHAIN" >"$BENCH_RESULTS/rust-toolchain.txt"
printf '%s\n' "$BINARY_SHA256" >"$BENCH_RESULTS/write-perf.sha256"
cp "$REPO/rust-toolchain" "$BENCH_RESULTS/rust-toolchain.toml"

if [[ "$BENCH_SUITE" == "v8-small-ticket-profile" ]]; then
    if [[ ! -r /tmp/gcp-wal-profile-runner.py ]]; then
        echo "profile runner was not uploaded" >&2
        exit 1
    fi
    cp /tmp/gcp-wal-profile-runner.py "$REPO/scripts/gcp_wal_profile_runner.py"
    echo "WAL_BENCHMARK_STARTED $(date --iso-8601=seconds)" | tee -a "$BENCH_BOOT_LOG"
    PYTHONPATH="$REPO/scripts${PYTHONPATH:+:$PYTHONPATH}" \
        python3 "$REPO/scripts/gcp_wal_profile_runner.py" \
        --binary "$BINARY" \
        --results-dir "$BENCH_RESULTS" \
        --db-root "$BENCH_WORK/databases" \
        --writers "$BENCH_WRITERS" \
        --safe-deadline-epoch "$BENCH_SAFE_DEADLINE_EPOCH"
elif [[ "$BENCH_SUITE" == "rfc026-full" ]]; then
    python3 - "$BENCH_RESULTS/benchmark-plan.json" "$ACTUAL_COMMIT" "$BINARY_SHA256" \
        "$BENCH_WRITERS" "$BENCH_MACHINE_TYPE" "$BENCH_SSD_COUNT" "$BENCH_SSD_TOTAL_GIB" <<'PY'
import json
from pathlib import Path
import sys
from gcp_wal_matrix import build_benchmark_plan

path, commit, binary_hash, writers_csv, machine_type, ssd_count, ssd_gib = sys.argv[1:]
writers = [int(value) for value in writers_csv.split(",")]
plan = build_benchmark_plan(
    full_matrix=True,
    writers=writers,
    machine_type=machine_type,
    machine_spec={"vcpus": int(machine_type.split("-")[2]), "ssd_count": int(ssd_count), "ssd_gib": int(ssd_gib)},
    commit=commit,
    binary_sha256=binary_hash,
)
Path(path).write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
PY

printf 'writer\tworkload\trepetition\tarm\texit_code\tops\tputs_per_second\tp99_ms\tcpu_seconds\tsync_count\tduration_seconds\n' \
    >"$BENCH_RESULTS/summary.tsv"
DB_ROOT="$BENCH_WORK/databases"
mkdir -p "$DB_ROOT"
RUN_TIMEOUT_SECONDS=900
FAILED=0
RUN_INDEX=0
RUN_PLAN="$BENCH_RESULTS/rfc026-schedule.tsv"
python3 - "$RUN_PLAN" "$BENCH_WRITERS" <<'PY'
import csv
import sys
from gcp_wal_matrix import build_rfc026_schedule

path, writers_csv = sys.argv[1:]
schedule = build_rfc026_schedule(int(value) for value in writers_csv.split(","))
with open(path, "w", encoding="utf-8", newline="") as stream:
    writer = csv.DictWriter(stream, fieldnames=schedule[0].keys(), delimiter="\t")
    writer.writeheader()
    writer.writerows({**item, "batch_size": item["batch_size"] or "none"} for item in schedule)
PY

while IFS=$'\t' read -r PLAN_INDEX WRITER_COUNT WORKLOAD REPETITION ARM BENCH_NAME BATCH_ENTRIES EXPECTED_BATCH_SIZE RUN_NUM SAMPLE_EVERY; do
    [[ "$PLAN_INDEX" == "run_index" ]] && continue
    NOW="$(date +%s)"
    if (( NOW + RUN_TIMEOUT_SECONDS + 30 + 300 >= BENCH_SAFE_DEADLINE_EPOCH )); then
        echo "stopping before safe VM deadline; partial RFC 026 matrix retained" >&2
        FAILED=1
        break
    fi
    RUN_INDEX=$PLAN_INDEX
    NAME="w${WRITER_COUNT}-${WORKLOAD}-r${REPETITION}-${ARM}"
    RUN_DIR="$BENCH_RESULTS/$NAME"
    mkdir -p "$RUN_DIR"
    WAL_MODE=parallel
    case "$ARM" in
        v4_parallel) EXPECTED_VERSION=4 ;;
        v7_parallel) EXPECTED_VERSION=7 ;;
        v8_parallel) EXPECTED_VERSION=8 ;;
        v8_leader) EXPECTED_VERSION=8; WAL_MODE=leader ;;
        *) echo "unexpected arm: $ARM" >&2; FAILED=1; break ;;
    esac
    COMMAND=("$BINARY" --bench "$BENCH_NAME" --wal --wal-io-mode "$WAL_MODE"
        --num "$RUN_NUM" --threads "$WRITER_COUNT" --value-size 1024 --target-sst-size 2147483648
        --compaction none --output json --latency-sample-every "$SAMPLE_EVERY" --path "$DB_ROOT/$NAME")
    if [[ "$WORKLOAD" != point ]]; then
        COMMAND+=(--wal-batch-size "$BATCH_ENTRIES")
    fi
    if [[ "$ARM" == v7_parallel ]]; then
        COMMAND+=(--pitr --pitr-format v7)
    elif [[ "$ARM" == v8_parallel || "$ARM" == v8_leader ]]; then
        COMMAND+=(--pitr --pitr-format v8)
    fi
    RUN_ENV=("TOYKV_WAL_TIMELINE=$RUN_DIR/timeline.json" "TOYKV_WAL_TIMELINE_INTERVAL_MS=100")
    printf '%q ' "${COMMAND[@]}" >"$RUN_DIR/command.sh"
    printf '\n' >>"$RUN_DIR/command.sh"
    date -u +%Y-%m-%dT%H:%M:%SZ >"$RUN_DIR/start_utc.txt"
    RUN_START="$(date +%s.%N)"
    if (( RUN_INDEX == 1 )); then
        echo "WAL_BENCHMARK_STARTED $(date --iso-8601=seconds)" | tee -a "$BENCH_BOOT_LOG"
    fi
    set +e
    timeout --signal=INT --kill-after=30s "$RUN_TIMEOUT_SECONDS" env "${RUN_ENV[@]}" "${COMMAND[@]}" \
        >"$RUN_DIR/result.json" 2>"$RUN_DIR/stderr.log"
    EXIT_CODE=$?
    set -e
    RUN_END="$(date +%s.%N)"
    date -u +%Y-%m-%dT%H:%M:%SZ >"$RUN_DIR/end_utc.txt"
    DURATION="$(python3 - "$RUN_START" "$RUN_END" <<'PY'
from decimal import Decimal
import sys
print(Decimal(sys.argv[2]) - Decimal(sys.argv[1]))
PY
)"
    set +e
    python3 - "$RUN_DIR" "$WRITER_COUNT" "$WORKLOAD" "$REPETITION" "$ARM" "$EXIT_CODE" \
        "$DURATION" "$RUN_NUM" "$EXPECTED_VERSION" "$WAL_MODE" "$EXPECTED_BATCH_SIZE" <<'PY'
import csv
import json
import math
from pathlib import Path
import sys

run_dir = Path(sys.argv[1])
writers = int(sys.argv[2])
workload, repetition, arm = sys.argv[3], int(sys.argv[4]), sys.argv[5]
exit_code, duration = int(sys.argv[6]), float(sys.argv[7])
requested_ops, expected_version = int(sys.argv[8]), int(sys.argv[9])
expected_mode, expected_batch_size = sys.argv[10], sys.argv[11]
expected_batch_size = int(expected_batch_size) if expected_batch_size != "none" else None
record = None
if exit_code == 0:
    try:
        record = json.loads((run_dir / "result.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        exit_code = 65
        (run_dir / "validation-error.txt").write_text(str(error) + "\n", encoding="utf-8")

metrics = {"ops": "", "puts_per_second": "", "p99_ms": "", "cpu_seconds": "", "sync_count": ""}
if record is not None:
    result = record.get("result", {})
    params = record.get("params", {})
    pipeline = result.get("wal_pipeline") or {}
    try:
        cpu_ms = float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])
        metrics = {
            "ops": result["ops"],
            "puts_per_second": float(result["ops_per_sec"]),
            "p99_ms": float(record["latency"]["p99_ms"]),
            "cpu_seconds": cpu_ms / 1000,
            "sync_count": int(pipeline["sync_count"]) if pipeline.get("sync_count") is not None else "",
        }
        if any(not math.isfinite(float(metrics[key])) or float(metrics[key]) < 0
               for key in ("puts_per_second", "p99_ms", "cpu_seconds")):
            raise ValueError("benchmark metrics contain non-finite or negative values")
    except (KeyError, TypeError, ValueError) as error:
        exit_code = 65
        (run_dir / "validation-error.txt").write_text(f"required metric missing/invalid: {error}\n", encoding="utf-8")
        metrics = {"ops": "", "puts_per_second": "", "p99_ms": "", "cpu_seconds": "", "sync_count": ""}
    expected_params = {
        "num": requested_ops,
        "threads": writers,
        "value_size": 1024,
        "batch_size": expected_batch_size,
        "wal_format_version": expected_version,
        "wal_io_mode": expected_mode,
    }
    actual_params = {key: params.get(key) for key in expected_params}
    if workload == "batch64":
        expected_batch_size = min(64, max(1, (requested_ops + writers - 1) // writers))
        expected_params["batch_size"] = expected_batch_size
        actual_params["batch_size"] = params.get("batch_size")
    if result.get("ops") != requested_ops or actual_params != expected_params:
        exit_code = 65
        (run_dir / "validation-error.txt").write_text(
            f"expected ops={requested_ops}, params={expected_params}; got ops={result.get('ops')}, "
            f"params={actual_params}\n", encoding="utf-8")

row = {"writer": writers, "workload": workload, "repetition": repetition, "arm": arm,
       "exit_code": exit_code, **metrics, "duration_seconds": duration}
with (run_dir.parent / "summary.tsv").open("a", encoding="utf-8", newline="") as stream:
    csv.DictWriter(stream, fieldnames=row.keys(), delimiter="\t").writerow(row)
(run_dir / "run-metadata.json").write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
if exit_code:
    raise SystemExit(exit_code)
PY
    VALIDATION_STATUS=$?
    set -e
    if (( VALIDATION_STATUS != 0 )); then EXIT_CODE=$VALIDATION_STATUS; fi
    echo "run=$NAME exit=$EXIT_CODE duration=${DURATION}s"
    if (( EXIT_CODE != 0 )); then FAILED=1; break; fi
done <"$RUN_PLAN"

EXPECTED_RUNS=$((${#WRITER_COUNTS[@]} * 4 * 4 * 4))
if (( RUN_INDEX != EXPECTED_RUNS )); then
    echo "incomplete RFC 026 matrix: completed $RUN_INDEX of $EXPECTED_RUNS planned runs" >&2
    FAILED=1
fi

python3 - "$BENCH_RESULTS" "$FAILED" "$RUN_INDEX" "$BENCH_WRITERS" <<'PY'
import csv
import json
from pathlib import Path
from statistics import median
import sys
from gcp_wal_matrix import RFC026_ARMS, RFC026_WORKLOADS, build_rfc026_schedule, validate_rfc026_rows

root, failed, runs = Path(sys.argv[1]), bool(int(sys.argv[2])), int(sys.argv[3])
writers = [int(value) for value in sys.argv[4].split(",")]
rows = list(csv.DictReader((root / "summary.tsv").open(encoding="utf-8"), delimiter="\t"))
workloads = tuple(workload["name"] for workload in RFC026_WORKLOADS)
arms = RFC026_ARMS
validation = validate_rfc026_rows(rows, writers)
planned = len(build_rfc026_schedule(writers))
comparison = []
by_cell = {}
for writer in writers:
    for workload in workloads:
        cell = {}
        for arm in arms:
            selected = [row for row in rows if row["writer"] == str(writer)
                        and row["workload"] == workload and row["arm"] == arm and row["exit_code"] == "0"]
            stats = {"runs": len(selected)}
            for field in ("puts_per_second", "p99_ms", "cpu_seconds", "sync_count"):
                values = []
                for row in selected:
                    try: values.append(float(row[field]))
                    except (KeyError, TypeError, ValueError): pass
                stats[field] = median(values) if values else None
            cell[arm] = stats
        base = cell["v4_parallel"]
        for arm in arms:
            stats = cell[arm]
            comparison.append({
                "writer": writer, "workload": workload, "arm": arm, **stats,
                "throughput_change_pct_vs_v4": (
                    (stats["puts_per_second"] / base["puts_per_second"] - 1) * 100
                    if base["puts_per_second"] not in (None, 0) and stats["puts_per_second"] is not None else None
                ),
                "p99_change_pct_vs_v4": (
                    (stats["p99_ms"] / base["p99_ms"] - 1) * 100
                    if base["p99_ms"] not in (None, 0) and stats["p99_ms"] is not None else None
                ),
            })
        by_cell[f"w{writer}-{workload}"] = cell

with (root / "comparison.tsv").open("w", encoding="utf-8", newline="") as stream:
    columns = ["writer", "workload", "arm", "runs", "puts_per_second", "p99_ms", "cpu_seconds", "sync_count",
               "throughput_change_pct_vs_v4", "p99_change_pct_vs_v4"]
    writer = csv.DictWriter(stream, fieldnames=columns, delimiter="\t")
    writer.writeheader()
    writer.writerows(comparison)

summary = {
    "complete": not failed and runs == planned and validation["complete"],
    "runs_completed": len(rows), "runs_planned": planned, "writers": writers,
    "workloads": list(workloads), "arms": list(arms), "results": rows,
    "matrix_validation": validation,
    "comparison_by_cell": by_cell,
    "qualification_note": "Same-VM comparison only; no cross-machine or storage-state qualification is implied.",
}
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
(root / "remote-exit.json").write_text(json.dumps({"failed": failed, "runs_started": runs}, indent=2) + "\n")
PY
    date -u +%Y-%m-%dT%H:%M:%SZ >"$BENCH_RESULTS/finished_utc.txt"
    echo "RFC 026 full matrix finished; runs_started=$RUN_INDEX failed=$FAILED"
    if (( FAILED )); then exit 1; fi
else
python3 - "$BENCH_RESULTS/benchmark-plan.json" "$BENCH_NUM" "$ACTUAL_COMMIT" "$BINARY_SHA256" \
    "$BENCH_WRITERS" "$BENCH_MACHINE_TYPE" "$BENCH_SSD_COUNT" "$BENCH_SSD_TOTAL_GIB" <<'PY'
import json
from pathlib import Path
import sys
from gcp_wal_matrix import build_benchmark_plan

path, number, commit, binary_hash, writers_csv, machine_type, ssd_count, ssd_gib = sys.argv[1:]
plan = build_benchmark_plan(
    full_matrix=False,
    writers=(int(value) for value in writers_csv.split(",")),
    machine_type=machine_type,
    machine_spec={"vcpus": int(machine_type.split("-")[2]), "ssd_count": int(ssd_count), "ssd_gib": int(ssd_gib)},
    num=int(number),
    commit=commit,
    binary_sha256=binary_hash,
)
Path(path).write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
PY

printf 'writer\tblock\tposition\tformat\texit_code\tops\tputs_per_second\tp99_ms\tcpu_seconds\tsync_count\tduration_seconds\n' \
    >"$BENCH_RESULTS/summary.tsv"

DB_ROOT="$BENCH_WORK/databases"
mkdir -p "$DB_ROOT"
RUN_TIMEOUT_SECONDS=900
FAILED=0
RUN_INDEX=0

for WRITERS in "${WRITER_COUNTS[@]}"; do
    for BLOCK in 1 2; do
        if (( BLOCK == 1 )); then
            ORDER=(v4 v7 v8 v8 v7 v4)
        else
            ORDER=(v8 v7 v4 v4 v7 v8)
        fi
        for POSITION in 1 2 3 4 5 6; do
            NOW="$(date +%s)"
            if (( NOW + RUN_TIMEOUT_SECONDS + 30 + 300 >= BENCH_SAFE_DEADLINE_EPOCH )); then
                echo "stopping before safe VM deadline; partial matrix retained" >&2
                FAILED=1
                break 3
            fi
            FORMAT="${ORDER[$((POSITION - 1))]}"
            RUN_INDEX=$((RUN_INDEX + 1))
            NAME="w${WRITERS}-b${BLOCK}-p${POSITION}-${FORMAT}"
            RUN_DIR="$BENCH_RESULTS/$NAME"
            mkdir -p "$RUN_DIR"
            COMMAND=("$BINARY" --bench wal_batch_concurrent --wal-batch-size 64 --wal --wal-io-mode parallel
                --num "$BENCH_NUM" --threads "$WRITERS" --value-size 1024 --target-sst-size 2147483648
                --compaction none --output json --latency-sample-every 10 --path "$DB_ROOT/$NAME")
            RUN_ENV=("TOYKV_WAL_TIMELINE=$RUN_DIR/timeline.json" "TOYKV_WAL_TIMELINE_INTERVAL_MS=100")
            if [[ "$FORMAT" != v4 ]]; then
                COMMAND+=(--pitr --pitr-format "$FORMAT")
            fi
            printf '%q ' "${COMMAND[@]}" >"$RUN_DIR/command.sh"
            printf '\n' >>"$RUN_DIR/command.sh"
            date -u +%Y-%m-%dT%H:%M:%SZ >"$RUN_DIR/start_utc.txt"
            RUN_START="$(date +%s.%N)"
            if (( RUN_INDEX == 1 )); then
                # Keep a marker on the boot-disk log if result transfer fails.
                echo "WAL_BENCHMARK_STARTED $(date --iso-8601=seconds)" | tee -a "$BENCH_BOOT_LOG"
            fi
            set +e
            timeout --signal=INT --kill-after=30s "$RUN_TIMEOUT_SECONDS" env "${RUN_ENV[@]}" "${COMMAND[@]}" \
                >"$RUN_DIR/result.json" 2>"$RUN_DIR/stderr.log"
            EXIT_CODE=$?
            set -e
            RUN_END="$(date +%s.%N)"
            date -u +%Y-%m-%dT%H:%M:%SZ >"$RUN_DIR/end_utc.txt"
            DURATION="$(python3 - "$RUN_START" "$RUN_END" <<'PY'
from decimal import Decimal
import sys
print(Decimal(sys.argv[2]) - Decimal(sys.argv[1]))
PY
)"
            set +e
            python3 - "$RUN_DIR" "$WRITERS" "$BLOCK" "$POSITION" "$FORMAT" "$EXIT_CODE" "$DURATION" "$BENCH_NUM" <<'PY'
from __future__ import annotations
import csv
import json
import math
from pathlib import Path
import sys

run_dir = Path(sys.argv[1])
writers, block, position = map(int, sys.argv[2:5])
fmt, exit_code = sys.argv[5], int(sys.argv[6])
duration, requested_ops = float(sys.argv[7]), int(sys.argv[8])
summary_path = run_dir.parent / "summary.tsv"
record = None
if exit_code == 0:
    try:
        record = json.loads((run_dir / "result.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        exit_code = 65
        (run_dir / "validation-error.txt").write_text(str(error) + "\n", encoding="utf-8")

metrics = {"ops": "", "puts_per_second": "", "p99_ms": "", "cpu_seconds": "", "sync_count": ""}
if record is not None:
    result = record.get("result", {})
    params = record.get("params", {})
    pipeline = result.get("wal_pipeline") or {}
    try:
        cpu_ms = float(result["process_cpu_user_ms"]) + float(result["process_cpu_system_ms"])
        metrics = {
            "ops": result["ops"],
            "puts_per_second": float(result["ops_per_sec"]),
            "p99_ms": float(record["latency"]["p99_ms"]),
            "cpu_seconds": cpu_ms / 1000,
            "sync_count": int(pipeline["sync_count"]) if pipeline.get("sync_count") is not None else "",
        }
        numeric = (metrics["puts_per_second"], metrics["p99_ms"], metrics["cpu_seconds"])
        if any(not math.isfinite(value) or value < 0 for value in numeric):
            raise ValueError("benchmark metrics contain non-finite or negative values")
    except (KeyError, TypeError, ValueError) as error:
        exit_code = 65
        (run_dir / "validation-error.txt").write_text(
            f"required benchmark metric is missing or invalid: {error}\n", encoding="utf-8")
        metrics = {"ops": "", "puts_per_second": "", "p99_ms": "", "cpu_seconds": "", "sync_count": ""}
    expected = int(fmt[1:]) if fmt != "v4" else 4
    effective_batch_size = min(64, max(1, (requested_ops + writers - 1) // writers))
    expected_params = {
        "num": requested_ops,
        "threads": writers,
        "value_size": 1024,
        "batch_size": effective_batch_size,
        "wal_format_version": expected,
        "wal_io_mode": "parallel",
    }
    actual_params = {key: params.get(key) for key in expected_params}
    if result.get("ops") != requested_ops or actual_params != expected_params:
        exit_code = 65
        (run_dir / "validation-error.txt").write_text(
            f"expected ops={requested_ops}, params={expected_params}; got ops={result.get('ops')}, "
            f"params={actual_params}\n", encoding="utf-8")

row = {"writer": writers, "block": block, "position": position, "format": fmt,
       "exit_code": exit_code, **metrics, "duration_seconds": duration}
with summary_path.open("a", encoding="utf-8", newline="") as stream:
    csv.DictWriter(stream, fieldnames=row.keys(), delimiter="\t").writerow(row)
(run_dir / "run-metadata.json").write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
if exit_code:
    raise SystemExit(exit_code)
PY
            VALIDATION_STATUS=$?
            set -e
            if (( VALIDATION_STATUS != 0 )); then
                EXIT_CODE=$VALIDATION_STATUS
            fi
            echo "run=$NAME exit=$EXIT_CODE duration=${DURATION}s"
            if (( EXIT_CODE != 0 )); then
                FAILED=1
                break 3
            fi
        done
    done
done

python3 - "$BENCH_RESULTS" "$FAILED" "$RUN_INDEX" "$BENCH_WRITERS" "$BENCH_NUM" <<'PY'
import csv
import json
from pathlib import Path
from statistics import median
import sys
from gcp_wal_matrix import LEGACY_ARMS, build_batch64_schedule, validate_batch64_rows

root, failed, runs = Path(sys.argv[1]), bool(int(sys.argv[2])), int(sys.argv[3])
writers = [int(value) for value in sys.argv[4].split(",")]
requested_ops = int(sys.argv[5])
rows = list(csv.DictReader((root / "summary.tsv").open(encoding="utf-8"), delimiter="\t"))
validation = validate_batch64_rows(rows, writers, requested_ops)
planned = len(build_batch64_schedule(writers))
metric_fields = {
    "puts_per_second": "median_puts_per_second",
    "p99_ms": "median_p99_ms",
    "cpu_seconds": "median_cpu_seconds",
    "sync_count": "median_sync_count",
}

def summarize(run_rows):
    summary = {"runs": len(run_rows)}
    for field, output in metric_fields.items():
        values = []
        for row in run_rows:
            try:
                values.append(float(row[field]))
            except (KeyError, TypeError, ValueError):
                continue
        summary[output] = median(values) if values else None
    return summary

by_writer = {}
comparison_rows = []
for writer in writers:
    per_format = {}
    for fmt in LEGACY_ARMS:
        selected = [row for row in rows if row["writer"] == str(writer)
                    and row["format"] == fmt and row["exit_code"] == "0"]
        stats = summarize(selected)
        per_format[fmt] = stats
    baseline = per_format["v4"]
    comparisons = {}
    for fmt in ("v7", "v8"):
        current = per_format[fmt]
        comparisons[fmt] = {}
        for field, output in (("median_puts_per_second", "throughput_change_pct_vs_v4"),
                              ("median_p99_ms", "p99_change_pct_vs_v4"),
                              ("median_cpu_seconds", "cpu_change_pct_vs_v4")):
            base_value, current_value = baseline[field], current[field]
            comparisons[fmt][output] = (
                (current_value / base_value - 1) * 100
                if base_value not in (None, 0) and current_value is not None else None
            )
    for fmt in LEGACY_ARMS:
        row = {"writer": writer, "format": fmt, **per_format[fmt]}
        if fmt == "v4":
            row.update({
                "throughput_change_pct_vs_v4": 0.0 if baseline["median_puts_per_second"] is not None else None,
                "p99_change_pct_vs_v4": 0.0 if baseline["median_p99_ms"] is not None else None,
                "cpu_change_pct_vs_v4": 0.0 if baseline["median_cpu_seconds"] is not None else None,
            })
        else:
            row.update(comparisons[fmt])
        comparison_rows.append(row)
    by_writer[str(writer)] = {"formats": per_format, "relative_to_v4": comparisons}

with (root / "comparison.tsv").open("w", encoding="utf-8", newline="") as stream:
    columns = ["writer", "format", "runs", *metric_fields.values(),
               "throughput_change_pct_vs_v4", "p99_change_pct_vs_v4", "cpu_change_pct_vs_v4"]
    output_writer = csv.DictWriter(stream, fieldnames=columns, delimiter="\t")
    output_writer.writeheader()
    output_writer.writerows(comparison_rows)

summary = {
    "complete": not failed and runs == planned and validation["complete"],
    "runs_completed": len(rows),
    "runs_planned": planned,
    "writers": writers,
    "formats": list(LEGACY_ARMS),
    "matrix_validation": validation,
    "results": rows,
    "comparison_by_writer": by_writer,
    "comparison_method": "median of successful repetitions per writer and format; percent changes are versus V4",
    "qualification_note": "Raw same-VM captures only. No cross-run storage-state matching or performance qualification is implied.",
}
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
(root / "remote-exit.json").write_text(json.dumps({"failed": failed, "runs_started": runs}, indent=2) + "\n")
PY

date -u +%Y-%m-%dT%H:%M:%SZ >"$BENCH_RESULTS/finished_utc.txt"
echo "WAL benchmark matrix finished; runs_started=$RUN_INDEX failed=$FAILED"
if (( FAILED )); then
    exit 1
fi
fi
BENCH_USER_SCRIPT
RUN_STATUS=$?
set -e
date -u +%Y-%m-%dT%H:%M:%SZ >"$RESULTS/remote-finished-utc.txt"
echo "remote_runner_exit=$RUN_STATUS" >"$RESULTS/remote-status.txt"
chown -R bench:bench /mnt/gcp-wal-bench
sync
exit "$RUN_STATUS"
