# RFC 024: buffered WAL diagnostic

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

## Result

Removing `O_DIRECT` from the existing parallel WAL did not establish a stable
performance environment. Buffered writes were slower in observed medians, and
the eight-writer arm included a severe throughput collapse. No production
change is proposed on this evidence.

## Method

The physical P41 Plus remained on firmware 004C, direct NVMe (Ahci, VMD
bypassed), ext4, and Linux 6.12.71-1-lts. Both arms used the identical retained
candidate, SHA-256
`6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da`.

A temporary LD_PRELOAD diagnostic library intercepted `open`, `open64`,
`openat`, and `openat64`. Both arms used the library; only the buffered arm
removed `O_DIRECT`, and only for paths ending in `.wal`. The wrapper forwards
opens using `SYS_openat`; consequently this is an instrumented diagnostic,
not a production implementation or an adoption benchmark. It exports no
`fdatasync` replacement. WAL encoding, aligned padding, batching, io_uring,
and sync coordination were unchanged. This tests buffering within the current
pipeline, not an independently optimized buffered-WAL design.

Two 50,000-operation smoke runs inspected `/proc/PID/fdinfo` and confirmed
direct WAL descriptors in the direct arm and none in the buffered arm. These
runs were excluded from scoring. Every scored run checked the wrapper's counts:
all targeted handles changed in the buffered arm, none in the direct arm.

For each of 4, 8, and 16 writers, eight 200,000-operation runs followed fixed
ABBA/BAAB order: direct, buffered, buffered, direct, buffered, direct, direct,
buffered. Values were 1 KiB; latency sampling occurred every ten operations.
Four writers used 1 MiB SST rotation; eight/sixteen used a 1 GiB target. Five
seconds separated runs. The executable, wrapper, and captured outputs were
staged in RAM; all database files were on the physical SSD. All samples were
retained. The fixed stability limits were throughput max/min <= 1.05 and p99
max/min <= 1.10 across all four runs of each arm/case.

## Observations

| Writers | Direct median puts/s | Buffered median puts/s | Direct median p99 ms | Buffered median p99 ms | Buffered stable? |
| --- | ---: | ---: | ---: | ---: | --- |
| 4 | 9,498 | 8,421 | 1.285 | 1.742 | Yes, this session |
| 8 | 17,355 | 11,961 | 1.188 | 1.839 | No |
| 16 | 23,633 | 18,652 | 1.768 | 2.357 | No |

The buffered eight-writer arm ranged from 5,206 to 12,037 puts/s and
1.813–3.069 ms p99. The direct run immediately after its slow sample reached
17,334 puts/s. Buffered sixteen-writer throughput spread was 5.16% and p99
spread 32.1%. Direct four-writer p99 spread narrowly failed at 10.26%; direct
sixteen-writer p99 spread was 14.68%. Only the direct eight-writer and buffered
four-writer arms passed both limits in this session.

These medians are descriptive, not qualified estimates of a performance effect:
several stability controls failed. The absence of a four-writer collapse in
this interval does not establish that buffering prevents it. Results reject
simply removing `O_DIRECT` as a sufficient fix; they do not isolate SSD firmware
as the unique cause, and do not rule out a differently designed buffered path.
Commit timing still includes the unchanged WAL durability wait; this is not a
write-to-page-cache-only throughput measurement. Crash recovery of a production
buffered mode has not been qualified by this diagnostic.

## Artifacts and cleanup

`target/rfc024-buffered-wal-20261004/` contains the wrapper source/library,
runner, protocol, raw outputs, descriptor checks, summaries, completion record,
and SHA-256 manifest. All 26 raw JSON outputs match the recorded results.
Disposable database directories and RAM staging were removed. No Rust source,
production binary, persistent environment variable, or host setting changed.
