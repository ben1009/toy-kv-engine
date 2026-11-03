# WAL extent attribution and calibration observations

This extends the [passive trace method](passive-wal-trace-method.md) with file
identity, allocation-time evidence, frame roles and finer application counters.
It adds no production WAL behavior. The protocol is
`kv-engine.passive-wal-extents.v1`; its first cohort is calibration data, with
no inherited fast/slow thresholds and no performance ratio.

## Capture and replay

```bash
cargo build --release --package kv-engine --features bench --bin write-perf
sudo python3 -B docs/wal/benchmarks/capture_wal_trace.py \
  --binary target/release/write-perf \
  --output-dir target/wal-extent-calibration \
  --wal-extents --counter-interval-ms 100 --cpus 0-15
python3 -B docs/wal/benchmarks/wal_extent_trace.py \
  --capture-dir target/wal-extent-calibration
```

The runner retains the declared V7 → V8 → V8 → V7 schedule for point writes
and 16-writer batch64. The executable, boot, mask, parameters, device layout,
script hashes and policy are fixed before measurement. `--smoke` captures two
1,024-put point runs. The extent protocol supports 100 or 200 ms counters;
the older protocol keeps its one-second setting and original analyzer.

The benchmark's existing timeline hook can invoke a read-only Python helper
through `TOYKV_WAL_EXTENT_HELPER`. It obtains the first snapshot before the
first application counter, and the final snapshot and frame inventory after
the last counter, before engine close. Helper stdout is a pipe; timeline and
perf output remain in RAM until all cohort measurements finish. No FIEMAP,
frame scan, detection write or extra flush runs inside the application window.
Reads after each run can affect subsequent device/cache state, so the capture
also remains an instrumented diagnostic rather than an overhead-free control.

## Ownership and role evidence

FIEMAP requests use flags zero. In particular, `FIEMAP_FLAG_SYNC` is absent:
[the kernel documents that flag as synchronizing the file before mapping](https://docs.kernel.org/filesystems/fiemap.html).
The helper checks inode/device/size/mtime stability around each query and inventory.
Only ordinary aligned extents, including allocated UNWRITTEN extents, are used.
Unknown, delayed, encoded, shared and otherwise unsupported mappings are excluded.
All file reads go through the filesystem; no raw device is opened.

On this host the home filesystem is partition 3 of `nvme0n1`. Its partition
starts at byte 54,761,881,600. FIEMAP physical offsets are translated by that
partition offset before matching NVMe LBAs; block trace sectors use 512-byte
units. The implementation supports directly mounted NVMe namespaces with
512-byte LBAs and 4 KiB aligned partitions/filesystem blocks. Stacked devices
and unsupported layouts fail explicitly.

An ending FIEMAP alone cannot establish ownership at the time of an earlier
write. The protocol adds:

```text
ext4:ext4_ext_map_blocks_exit
ext4:ext4_free_blocks
```

The [kernel mapping event](https://raw.githubusercontent.com/torvalds/linux/v6.12/include/trace/events/ext4.h)
contains device, inode, logical/physical filesystem blocks and mapping result.
Each WAL block needs either an unchanged initial FIEMAP mapping, or a matching
inode/block mapping event **before command submission**. Post-only mappings
are not backdated. Conflicting mappings, overlapping writes to one block,
aliases and file-data frees prevent positive attribution. Extent-tree metadata
frees are recorded separately and do not invalidate unchanged file-data blocks.
An initial WAL inode disappearing before inventory is an error.

File membership does not determine record kind. The final inventory checks
V7/V8 frame magic, header version, kind, offset, header digest binding, local
CRC and padding. It records DATA, FRONTIER, HEADER and untyped spans. Earlier
initialization/overwritten writes are not counted as DATA just because a later
DATA frame occupies the same location. Partial-sector writes invalidate prior
whole-frame attribution. Mixed roles/files, unknown allocation times, kernel
issuers and partial ranges remain unresolved. The block-issue counter records
location matches as context; block completions are not naively paired by sector.

Every capture also checks inventory DATA bytes against all request batch sizes,
including each writer's final partial batch. The fixed protocol's 11-byte keys
and 1,024-byte values encode 1,050 bytes per entry, with a 48-byte logical header
and 4,008-byte fragment payload. This catches a missing WAL image or inventory
coverage gap before reporting an attribution percentage.

## Calibration dimensions

For the most recently completed namespace FLUSH, retain the first subsequent
positively attributed WAL DATA submission. Non-WAL writes and known FRONTIER
or initialization traffic remain context; a possible but unresolved DATA write
consumes the observation rather than allowing a later convenient substitute.
This is a namespace sequence, not a claim that the FLUSH belongs to a particular
file or that its completion is the application's fdatasync return.

Samples retain exact size, timing, file identity, file offset, initial and peak
queue depth, intervening reads, FUA/ordinary writes and first namespace-write
role/control/size/latency. Calibration rows are separated by:

- Write size: up to 4 KiB, 16 KiB, 64 KiB, 128 KiB, or larger.
- Queue depth at DATA submission and maximum overlap during the command.
- Queue depth at FLUSH completion.
- Intervening FUA, ordinary, mixed or absent writes, and presence of reads.
- FLUSH-to-DATA submission gap: up to 100 µs, up to 500 µs, or longer.

Common classes are reported separately for point and batch64, never across
workloads. Per-class medians and quantiles describe observations only; p99 is
omitted below 100 samples. No class automatically inherits the earlier isolated
ordinary 4 KiB thresholds.

## Output and limits

`wal-calibration.csv` describes per-run, per-class distributions.
`interval-metrics.csv` records actual counter intervals, request throughput and
p50/p99, CPU, syncs, format-dependent reported groups and worker counters. It
also joins WAL DATA median, first namespace-write median, FLUSH-to-DATA setup
gap and observed class count. Each I/O observation is assigned by its DATA
completion time; its earlier FLUSH and first-write timestamps remain in the raw
sample rather than being clipped to the counter window. Request logs stay exact; percentiles are never
averaged. All counter/request conservation checks from the earlier method apply.

The cohort intentionally exports unknown state labels and
`calibration_pending=true`. This makes the missing validation explicit instead
of applying convenient thresholds to new write paths. Extent attribution solves
file/role ambiguity; it does not prove that latency of DATA alone identifies the
same device regime when an intervening journal FUA carries the post-FLUSH cost.
A later classifier must freeze and validate its per-path/size/overlap calibration
on fresh data before producing state-matched V7/V8 comparisons. Historical
captures keep their original decisions.
