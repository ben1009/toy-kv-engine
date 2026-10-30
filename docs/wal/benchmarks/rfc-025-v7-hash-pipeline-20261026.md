# RFC 025: interleaved and deferred v7 prefix hashing, 2026-10-26

**Result: rejected; the CPU guard fails and the solo tail worsens.**

This candidate preserves the original inline path's per-batch framing,
hashing, and logical-buffer release. After eight consecutive syncs below the
existing 100-microsecond threshold, large groups can hash on the coordinator
while the packer prepares later DATA. A slower sync resets that preference.
It follows the [critical-path profile](rfc-025-v7-critical-path-20261026.md),
[rejected fixed-stage policies](rfc-025-v7-prefix-hash-overlap-20261026.md), and
[rejected immediate latency hint](rfc-025-v7-hash-scheduling-20261026.md).

## Work placement and correctness

Initially and after a slow sync, the packer tries to frame and hash each batch
inline before starting the next batch. Logical bytes are released immediately
after that hash. This matches the retained path's order, avoiding the prior
prototype's whole-group framing pass before hashing and preserving overlap
with a preceding storage barrier. A brief cheap-sync burst cannot immediately
switch a storage-bound workload's hash stage.

After sustained cheap syncs, groups above 32 KiB of logical data defer their
hashing. The packer frames DATA and queues encoder-owned logical bytes in ticket
order before submitting the group. The coordinator hashes only its contiguous
written target before appending FRONTIER and performing the existing single
WAL sync. Neither admission nor the packer mutex is held during deferred hashing.

A relaxed atomic hint selects work placement; a mutex protects the single
ordered hash cursor. The packer tries that mutex without waiting. A busy state
or deferred predecessor queues the group instead. Mode changes cannot skip a
ticket. Exact endpoint snapshots let the coordinator reuse a precomputed prefix
without contending with a later inline group. Hash completion and snapshots
never publish durability: FRONTIER write and successful sync still precede ACK.
There is no new thread, format, public configuration, or change to v4's hash path.

Logical inputs hold their own pin on the original resource reservation. A DATA
CQE can retire and its aligned allocation can be reused while logical bytes
remain charged. Close drains I/O and the coordinator, discards queued inputs,
and retires remaining owned operations. Encoded bytes drop before their pin.
The existing CRCs, SHA-256 domain, timestamps, poison rules, and recovery
validation remain intact.

Tests hold the hash-state mutex while later DATA frames are submitted and
reaped, verify durability stays at genesis and logical bytes remain charged,
and then recover the exact batches after hashing resumes. Mixed-mode tests
cover a large inline group, a deferred large predecessor, a small successor
after a preference change, earlier snapshots, out-of-order completion, marker
recapture, and recovery. Hash failure preserves the acknowledged recovery
prefix; its test deterministically forces the deferred path. A retained
logical input keeps its charge across close, handle drop, and durable cleanup.

`cargo make check` passes all 1,966 tests with zero skips, retries, or slow tests:
nextest `f548451f-c67a-4021-8e47-8e578e945591`. Formatting, dependency sorting,
all-target/all-feature Clippy, unused dependencies, and spelling pass. The
release build passes. Logs remain under
`target/wal-opt/20261026-v7-hash-pipeline`.

## Fixed measurement

The source base is `20b011b1`. The executable is
`target/release/write-perf-v7-hash-pipeline`, SHA-256
`644ca097378dd11f5409e99475d7747aaed2b7c05dba01cc5cb838ef6edda739`.
Its [128-input manifest](artifacts/rfc-025-v7-hash-pipeline-20261026-source-sha256.tsv)
has SHA-256 `8ddf5ce60aec759b8594d07df2479d63488a61d7e5b31364b1d3a2b29fb7a9ca`.
The [patch](artifacts/rfc-025-v7-hash-pipeline-20261026-rejected.patch)
reconstructs the candidate. The retained executable is
`target/release/write-perf-v7-validated-logical-batch`, SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
Both use:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

The protocol matches the preceding candidates. Each CPU cell has four warmups
and three alternating four-arm pairs: retained/candidate binaries with v4 and
v7. Passing the CPU screen normally permits a three-pair solo guard, then five-pair ext4
batch64 and point comparisons. Batch64 writes 750,000 puts, sampling every ten
batches for 1,176 latency observations. Point cells write 100,000 puts, sampling
every 100 operations for 1,000 observations. Eight writers use 1-KiB values and
a 2-GiB SST target; the solo guard changes only writer count.

Tmpfs has three-second gaps. Ext4 has a 30-second initial cooldown and ten-second
gaps, on the same Intel Core i9-13900T and Solidigm SSDPFKNU010TZ, using
`/dev/nvme0n1p3`. Each record uses a fresh database removed after capture.
Setup/close stay outside the timed interval. Pipeline counters reset after setup
and are captured before drain/close. No tests, builds, or profiling overlap
scored runs. Every stall and unfavorable control remains; there are no
peak-selected repeats.

The fixed screen requires raw and v4-normalized tmpfs batch throughput to reach
baseline, batch CPU growth at most 5%, and point raw loss at most 5%. Retention
also requires the solo and both ext4 raw throughput guards to stay within 5%.
CPU and latency outcomes are reported separately. Ratios are medians of paired
ratios; normalization compares each v7/v4 rate to its same-executable control.
CPU is timed process user plus system time. Passing these screens does not
qualify RFC Stage 8 or establish uniform nonregression.

The [driver](artifacts/rfc-025-v7-hash-pipeline-20261026-run.py) checks the frozen
[binary identities](artifacts/rfc-025-v7-hash-pipeline-20261026-binaries.json),
refuses overwrites, and cleans scratch databases. The
[analyzer](artifacts/rfc-025-v7-hash-pipeline-20261026-analyze.py) verifies raw
hashes, complete schedules, settings, sample counts, and recorded identities.

## CPU result and extended diagnostic


| Cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs batch64 | 1.123x | 1.108x | 1.100x | 0.814x | 1.063x |
| tmpfs point, 8 writers | 1.037x | 1.016x | 0.949x | 0.981x | 1.102x |

Batch throughput improves 12.3%, but its CPU rises 10.0%, above the planned
5% guard. That CPU gate remains failed. The fixed solo/SSD schedule is extended
as an explicit throughput/CPU tradeoff diagnostic for review, with no additional
repeats or peak selection. It cannot override or relabel the failed CPU guard.

The median number of batch groups rises from 6,500 to 10,645 while DATA buffer
count stays fixed by the logical workload. Sync count changes from 3,084 to
3,026, and recorded coalescing wait from 430.6 to 229.5 ms. More group handoffs
are a plausible CPU cost of faster packer progress; overlapping counters do not
isolate their causal contribution. The complete records preserve this cost.

## Extended diagnostic: rejected


| Cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs point, 1 writer | 0.953x | 0.899x | 1.027x | 1.021x | 1.865x |
| ext4 batch64 | 1.036x | 0.917x | 0.900x | 0.990x | 1.037x |
| ext4 point, 8 writers | 0.998x | 0.985x | 1.006x | 1.009x | 1.191x |

The SSD raw guards pass, but the original CPU guard remains failed. Solo
throughput falls 4.7% and sampled p99 rises 86.5%; SSD point p99 rises 19.1%.
The candidate is rejected. Its raw SSD batch peak reaches 443,166 puts/s,
above this capture's retained 421,837 and v4-control 400,363. Peaks remain
separate from paired ratios and do not override the failed guard or tail cost.

All 76 scored records and 20 warmups remain. The [complete summary](artifacts/rfc-025-v7-hash-pipeline-20261026-summary.json)
has retention guard `false`. Source reconstruction from `20b011b1` reproduces
all 128 hashes, and all 96 run IDs in this candidate are distinct.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs-batch64 | [script](artifacts/rfc-025-v7-hash-pipeline-tmpfs-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-pipeline-tmpfs-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-pipeline-tmpfs-batch64-20261026.jsonl) |
| tmpfs-point8 | [script](artifacts/rfc-025-v7-hash-pipeline-tmpfs-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-pipeline-tmpfs-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-pipeline-tmpfs-point8-20261026.jsonl) |
| tmpfs-point1 | [script](artifacts/rfc-025-v7-hash-pipeline-tmpfs-point1-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-pipeline-tmpfs-point1-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-pipeline-tmpfs-point1-20261026.jsonl) |
| ext4-batch64 | [script](artifacts/rfc-025-v7-hash-pipeline-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-pipeline-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-pipeline-ext4-batch64-20261026.jsonl) |
| ext4-point8 | [script](artifacts/rfc-025-v7-hash-pipeline-ext4-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-hash-pipeline-ext4-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-hash-pipeline-ext4-point8-20261026.jsonl) |

No hash-offload code is retained. The follow-up returns to the original packer
and precomputes group-size metadata to limit the earlier zero-wait policy to
large-batch waves. Point and mixed waves keep their existing short wait, while
all workloads retain slow-sync coalescing. The eleven earlier optimizations
remain; v4 parity and the complete RFC gate remain unmet.

Source patches use zero-context diffs; apply them to the stated base with
`git apply --unidiff-zero`. The verifier also checks exact reconstructed bytes.

[WAL documentation](../README.md).
