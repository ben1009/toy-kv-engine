# RFC 025: schedule v7 hashing around sync latency, 2026-10-26

**Result: rejected; the fixed SSD batch guard fails.**

This candidate overlaps ordered prefix hashing with other WAL work without
fixing the hash stage for every storage device. It follows the
[critical-path diagnosis](rfc-025-v7-critical-path-20261026.md) and the
[rejected fixed-stage experiments](rfc-025-v7-prefix-hash-overlap-20261026.md).
The original coalescing policy, on-disk bytes, and sync-before-ACK boundary
remain intact.

## Scheduling and ownership

The runtime initially favors hashing on the ordered packer. After a sync of
at least 100 microseconds, it keeps that preference so packer work can overlap
the preceding storage barrier. After a cheaper sync, it defers logical groups
larger than 32 KiB to the sync coordinator, allowing the packer to frame and
submit later DATA while the coordinator hashes. These use the existing
sync-latency threshold, with no new runtime configuration or thread.

The hint is a relaxed atomic boolean. It changes where work is attempted;
every value is safe because a mutex protects the single ordered hash cursor.
The packer never waits on that mutex. A busy state or deferred predecessor
queues the logical inputs in ticket order before submitting their DATA group.
The coordinator hashes deferred inputs only through its contiguous written
target before appending FRONTIER and performing the single WAL sync.

The packer may hash ahead of the coordinator's captured target. Exact endpoint
snapshots preserve each covered logical prefix. When such a snapshot already
exists, the coordinator reuses it without taking the hash-state mutex; this
avoids interfering with the next inline packer group. Neither a snapshot nor
hash completion publishes durability. FRONTIER write and successful sync still
precede every durable watermark and ACK.

A separate logical-input pin shares the original batch reservation. DATA CQE
retirement and aligned-buffer reuse cannot release the logical memory charge
before hashing finishes. Close discards queued logical inputs before retiring
the remaining owned reservations. The immutable logical bytes, frame CRCs,
digest domain, ticket/timestamp checks, poison behavior, and recovery rules
remain unchanged. V4 has no hashing policy or additional per-sync hint store.

Tests block hashing while later DATA is framed/submitted/reaped, keep logical
charges after DATA completion, exercise a large inline group followed by a
fast-mode deferred group and a slow-mode small successor, preserve earlier
snapshots across out-of-order completion and marker recapture, and recover the
exact batch sequence. Hash-failure tests retain the acknowledged recovery
prefix. Logical pin tests retain charges across close, handle drop, and durable
cleanup until the retained bytes are dropped.

The full `cargo make check` passes all 1,966 tests with zero skips, retries, or
slow tests: nextest `955235ef-6a3d-4ec4-aa7e-7a324cd031eb`. Formatting, dependency
sorting, all-target/all-feature Clippy, unused dependencies, and spelling pass.
The release build passes. Logs remain under
`target/wal-opt/20261026-v7-hash-scheduling`.

## Fixed comparison

The source base is `20b011b1`. The candidate executable is
`target/release/write-perf-v7-hash-scheduling`, SHA-256
`8e909820c5b7680913fdeade04c92124990904f654a9e55bcf93793b0af0c1c0`.
Its [128-input manifest](artifacts/rfc-025-v7-hash-scheduling-20261026-source-sha256.tsv)
has SHA-256 `8e6bed552b8d5908179b8146d53573a526742236bb2fd1cbd0fa6f0b9d86e54d`.
The [source patch](artifacts/rfc-025-v7-hash-scheduling-20261026-rejected.patch)
reconstructs the measured candidate. The retained control is the exact
immutable-batch executable, SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
Both release builds use:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

Each CPU cell has four warmups and three alternating four-arm pairs, using
retained/candidate executables with v4 and v7. Passing the CPU screen permits
a three-pair solo guard, then five-pair ext4 batch64 and point comparisons.
Batch64 uses 750,000 puts and latency sampling every ten batches, yielding
1,176 observations. Point cells use 100,000 puts and sampling every 100
operations, yielding 1,000 observations. Eight writers use 1-KiB values and a
2-GiB SST target; the solo guard changes only the writer count.

Tmpfs has three-second gaps; ext4 has an initial 30-second cooldown and
ten-second gaps. The host is the same Intel Core i9-13900T and Solidigm
SSDPFKNU010TZ, with ext4 on `/dev/nvme0n1p3`. All databases are fresh and removed
after capture. Setup/close stay outside the measured interval, and pipeline
counters reset after setup and are captured before drain/close. No tests,
builds, or profiling run alongside scored comparisons. Stalls and unfavorable
controls stay included, with no peak-selected repeats.

The fixed screen requires raw and v4-normalized tmpfs batch throughput to
reach baseline, batch CPU growth at most 5%, and point raw loss at most 5%.
Retention also requires the solo and both ext4 raw throughput guards to stay
within 5%. CPU and sampled latency outcomes are inspected separately. Ratios
are medians of paired ratios. Normalization compares each v7/v4 rate to its
same-executable control; CPU is timed process user plus system time. These
screens do not qualify the RFC Stage 8 workload or operational gate.

The [driver](artifacts/rfc-025-v7-hash-scheduling-20261026-run.py) freezes
[binary identities](artifacts/rfc-025-v7-hash-scheduling-20261026-binaries.json),
refuses to overwrite captures, and cleans its scratch databases. The
[analyzer](artifacts/rfc-025-v7-hash-scheduling-20261026-analyze.py) verifies raw
hashes, complete schedules, settings, sample counts, and executable identities
recorded in each manifest.

## CPU screen


| Cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs batch64 | 1.167x | 1.121x | 1.024x | 0.757x | 1.065x |
| tmpfs point, 8 writers | 0.988x | 1.036x | 1.002x | 0.987x | 1.042x |
| tmpfs point, 1 writer | 1.019x | 1.002x | 0.998x | 0.980x | 0.847x |

The CPU and solo guards pass. Batch sampled p50 falls while p99 rises 6.5%;
point8 sampled p99 rises 4.2%. Throughput gains do not establish uniform latency
improvement. SSD outcomes are evaluated separately.

## SSD result: rejected


| Cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| ext4 batch64 | 0.944x | 0.965x | 1.047x | 1.023x | 1.057x |
| ext4 point, 8 writers | 1.008x | 1.002x | 0.986x | 0.991x | 1.060x |

The point guard passes, but batch raw throughput changes 0.944x, below the
fixed 0.95 threshold, so this candidate is rejected. The five batch ratios are
0.944x, 1.029x, 0.503x, 1.089x, 0.938x. All remain included. The batch peak is 395,973 puts/s,
below this capture's retained peak of 419,587. Paired ratios and per-arm
medians are different summaries and are not interchangeable.

The source also differs from the original inline path: it frames every batch
in a group before hashing their logical inputs. The original frames, hashes,
and drops each logical allocation before advancing to the next batch. A
separate follow-up restores that per-batch order and requires a sustained run
of cheap syncs before moving large hashes to the coordinator. This is a source
difference and a testable follow-up; these measurements do not prove it
accounts for the whole loss.

All 76 scored records and 20 warmups remain. The [complete summary](artifacts/rfc-025-v7-hash-scheduling-20261026-summary.json)
has retention guard `false`. The rejected source patch and its 128 hashes
reconstruct from `20b011b1`; all 96 run IDs are distinct.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs-batch64 | [script](artifacts/rfc-025-v7-hash-scheduling-tmpfs-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-scheduling-tmpfs-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-scheduling-tmpfs-batch64-20261026.jsonl) |
| tmpfs-point8 | [script](artifacts/rfc-025-v7-hash-scheduling-tmpfs-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-scheduling-tmpfs-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-scheduling-tmpfs-point8-20261026.jsonl) |
| tmpfs-point1 | [script](artifacts/rfc-025-v7-hash-scheduling-tmpfs-point1-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-scheduling-tmpfs-point1-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-scheduling-tmpfs-point1-20261026.jsonl) |
| ext4-batch64 | [script](artifacts/rfc-025-v7-hash-scheduling-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-scheduling-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-scheduling-ext4-batch64-20261026.jsonl) |
| ext4-point8 | [script](artifacts/rfc-025-v7-hash-scheduling-ext4-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-scheduling-ext4-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-scheduling-ext4-point8-20261026.jsonl) |

The eleven earlier retained optimizations remain. V4 parity and the complete
RFC Stage 8 gate remain unmet.

Source patches use zero-context diffs; apply them to the stated base with
`git apply --unidiff-zero`. The verifier also checks exact reconstructed bytes.

[WAL documentation](../README.md).
