#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 6 ]]; then
    echo "usage: $0 <commit> <num> <project> <zone> <instance> <runtime-budget-seconds>" >&2
    exit 64
fi

COMMIT="$1"
NUM="$2"
PROJECT="$3"
ZONE="$4"
INSTANCE="$5"
RUNTIME_BUDGET_SECONDS="$6"

if [[ ! "$COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "invalid full commit SHA: $COMMIT" >&2
    exit 64
fi
if [[ ! "$NUM" =~ ^[0-9]+$ ]] || (( NUM < 64 )); then
    echo "num must be >=64" >&2
    exit 64
fi
if [[ ! "$RUNTIME_BUDGET_SECONDS" =~ ^[0-9]+$ ]] || (( RUNTIME_BUDGET_SECONDS < 1800 )); then
    echo "remaining VM runtime budget is too short" >&2
    exit 64
fi

BOOT_LOG=/var/log/gcp-free-trial-wal-bench.log
REMOTE_STARTED_EPOCH="$(date +%s)"
exec > >(tee -a "$BOOT_LOG") 2>&1
echo "[$(date --iso-8601=seconds)] starting GCP WAL benchmark setup"

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential ca-certificates clang cmake curl git libclang-dev libssl-dev pkg-config \
    python3 sudo time util-linux xz-utils

id bench >/dev/null 2>&1 || useradd --create-home --shell /bin/bash bench
usermod -aG sudo bench
printf 'bench ALL=(ALL) NOPASSWD:ALL\n' >/etc/sudoers.d/90-wal-bench
chmod 0440 /etc/sudoers.d/90-wal-bench

mapfile -t local_ssd_links < <(find /dev/disk/by-id -maxdepth 1 -type l -name 'google-local-nvme-ssd-[0-9]*' -print | sort)
if (( ${#local_ssd_links[@]} != 1 )); then
    echo "expected exactly one Google NVMe Local SSD symlink, found ${#local_ssd_links[@]}" >&2
    printf '%s\n' "${local_ssd_links[@]}" >&2
    exit 1
fi
if [[ "$(basename "${local_ssd_links[0]}")" != google-local-nvme-ssd-0 ]]; then
    echo "refusing unexpected Local SSD device identifier: ${local_ssd_links[0]}" >&2
    exit 1
fi
LOCAL_SSD="$(readlink -f "${local_ssd_links[0]}")"
if [[ ! -b "$LOCAL_SSD" ]]; then
    echo "Local SSD link does not resolve to a block device: $LOCAL_SSD" >&2
    exit 1
fi
MIN_BYTES=$((375 * 1024 * 1024 * 1024))
SSD_BYTES="$(blockdev --getsize64 "$LOCAL_SSD")"
if (( SSD_BYTES < MIN_BYTES )); then
    echo "Local SSD is smaller than 375 GiB: $SSD_BYTES bytes" >&2
    exit 1
fi
MOUNTPOINTS="$(lsblk -nr -o MOUNTPOINT "$LOCAL_SSD")"
if [[ -n "$(tr -d '[:space:]' <<<"$MOUNTPOINTS")" ]]; then
    echo "refusing to format a mounted Local SSD: $LOCAL_SSD" >&2
    exit 1
fi
SIGNATURES="$(wipefs --no-act --noheadings --output TYPE "$LOCAL_SSD")"
if [[ -n "$SIGNATURES" ]]; then
    echo "refusing to format a Local SSD with existing filesystem signatures: $LOCAL_SSD" >&2
    exit 1
fi

RESULTS=/mnt/gcp-wal-bench/results
WORK=/mnt/gcp-wal-bench/work
mkdir -p /mnt/gcp-wal-bench "$RESULTS" "$WORK"
mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "$LOCAL_SSD"
mount -o noatime "$LOCAL_SSD" /mnt/gcp-wal-bench
mkdir -p "$RESULTS" "$WORK"
chown -R bench:bench /mnt/gcp-wal-bench
exec > >(tee -a "$RESULTS/setup.log") 2>&1

cat >"$RESULTS/host.txt" <<EOF
project=$PROJECT
zone=$ZONE
instance=$INSTANCE
machine_type=c4-standard-8-lssd
local_ssd_device=$LOCAL_SSD
local_ssd_bytes=$SSD_BYTES
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
    BENCH_NUM="$NUM" \
    BENCH_REPO_URL="https://github.com/ben1009/toy-kv-engine.git" \
    BENCH_RESULTS="$RESULTS" \
    BENCH_WORK="$WORK" \
    BENCH_RUNTIME_BUDGET_SECONDS="$RUNTIME_BUDGET_SECONDS" \
    BENCH_SCRIPT_STARTED_EPOCH="$REMOTE_STARTED_EPOCH" \
    BENCH_BOOT_LOG="$BOOT_LOG" \
    bash -s <<'BENCH_USER_SCRIPT'
set -Eeuo pipefail

source "$HOME/.cargo/env" 2>/dev/null || true
if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
source "$HOME/.cargo/env"

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

TOOLCHAIN="$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$REPO/rust-toolchain")"
if [[ -z "$TOOLCHAIN" ]]; then
    echo "could not read pinned Rust toolchain from rust-toolchain" >&2
    exit 1
fi
rustup toolchain install "$TOOLCHAIN" --profile minimal

cd "$REPO"
cargo "+$TOOLCHAIN" build --locked --release --package kv-engine --features bench --bin write-perf
BINARY="$REPO/target/release/write-perf"
BINARY_SHA256="$(sha256sum "$BINARY" | cut -d' ' -f1)"
printf '%s\n' "$ACTUAL_COMMIT" >"$BENCH_RESULTS/commit.txt"
printf '%s\n' "$TOOLCHAIN" >"$BENCH_RESULTS/rust-toolchain.txt"
printf '%s\n' "$BINARY_SHA256" >"$BENCH_RESULTS/write-perf.sha256"
cp "$REPO/rust-toolchain" "$BENCH_RESULTS/rust-toolchain.toml"

python3 - "$BENCH_RESULTS/benchmark-plan.json" "$BENCH_NUM" "$ACTUAL_COMMIT" "$BINARY_SHA256" <<'PY'
import json
from pathlib import Path
import sys

path, number, commit, binary_hash = sys.argv[1:]
formats = ["v4", "v7", "v8"]
orders = [["v4", "v7", "v8", "v8", "v7", "v4"], ["v8", "v7", "v4", "v4", "v7", "v8"]]
plan = {
    "commit": commit,
    "binary_sha256": binary_hash,
    "formats": formats,
    "writers": [1, 4, 8],
    "writes_per_run": int(number),
    "batch_entries": 64,
    "value_bytes": 1024,
    "order_blocks": orders,
    "runs_planned": len(formats) * len([1, 4, 8]) * 4,
    "runtime_protocol": "one build; balanced V4/V7/V8 order; no storage-state probing",
}
Path(path).write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
PY

printf 'writer\tblock\tposition\tformat\texit_code\tops\tputs_per_second\tp99_ms\tcpu_seconds\tsync_count\tduration_seconds\n' \
    >"$BENCH_RESULTS/summary.tsv"

DB_ROOT="$BENCH_WORK/databases"
mkdir -p "$DB_ROOT"
RUN_TIMEOUT_SECONDS=900
if (( BENCH_RUNTIME_BUDGET_SECONDS < RUN_TIMEOUT_SECONDS )); then
    RUN_TIMEOUT_SECONDS=$((BENCH_RUNTIME_BUDGET_SECONDS / 4))
fi
FAILED=0
RUN_INDEX=0
START_EPOCH="$BENCH_SCRIPT_STARTED_EPOCH"

for WRITERS in 1 4 8; do
    for BLOCK in 1 2; do
        if (( BLOCK == 1 )); then
            ORDER=(v4 v7 v8 v8 v7 v4)
        else
            ORDER=(v8 v7 v4 v4 v7 v8)
        fi
        for POSITION in 1 2 3 4 5 6; do
            NOW="$(date +%s)"
            ELAPSED=$((NOW - START_EPOCH))
            if (( ELAPSED + RUN_TIMEOUT_SECONDS + 30 + 300 >= BENCH_RUNTIME_BUDGET_SECONDS )); then
                echo "stopping before VM auto-delete deadline; partial matrix retained" >&2
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
    if result.get("ops") != requested_ops or params.get("wal_format_version") != expected:
        exit_code = 65
        (run_dir / "validation-error.txt").write_text(
            f"expected ops={requested_ops}, format=v{expected}; got ops={result.get('ops')}, "
            f"format={params.get('wal_format_version')}\n", encoding="utf-8")

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

python3 - "$BENCH_RESULTS" "$FAILED" "$RUN_INDEX" <<'PY'
import csv
import json
from pathlib import Path
from statistics import median
import sys

root, failed, runs = Path(sys.argv[1]), bool(int(sys.argv[2])), int(sys.argv[3])
rows = list(csv.DictReader((root / "summary.tsv").open(encoding="utf-8"), delimiter="\t"))
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
for writer in (1, 4, 8):
    per_format = {}
    for fmt in ("v4", "v7", "v8"):
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
    for fmt in ("v4", "v7", "v8"):
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
    "complete": not failed and len(rows) == 36 and all(row["exit_code"] == "0" for row in rows),
    "runs_completed": len(rows),
    "runs_planned": 36,
    "writers": [1, 4, 8],
    "formats": ["v4", "v7", "v8"],
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
BENCH_USER_SCRIPT
RUN_STATUS=$?
set -e
date -u +%Y-%m-%dT%H:%M:%SZ >"$RESULTS/remote-finished-utc.txt"
echo "remote_runner_exit=$RUN_STATUS" >"$RESULTS/remote-status.txt"
chown -R bench:bench /mnt/gcp-wal-bench
sync
exit "$RUN_STATUS"
