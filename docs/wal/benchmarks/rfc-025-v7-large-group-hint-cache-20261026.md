# RFC 025: precomputed group sizes for v7 coalescing, 2026-10-26

**Result: rejected after the fixed SSD confirmation; the retained source and executable are restored.**

This candidate keeps hashing in the retained ordered packer. It precomputes
whether every logical batch in a packed group is at least 64 KiB, then avoids
the short coalescing wait for known large written waves after a cheap sync.
A cached negative hint keeps point-only waves out of the new metadata lookup.

The change follows the [critical-path diagnosis](rfc-025-v7-critical-path-20261026.md),
[fixed hash-stage experiments](rfc-025-v7-prefix-hash-overlap-20261026.md),
[immediate hash-stage hint](rfc-025-v7-hash-scheduling-20261026.md),
[interleaved hash-offload experiment](rfc-025-v7-hash-pipeline-20261026.md), and
[uncached group-size lookup](rfc-025-v7-large-group-coalescing-20261026.md).
The hash-offload code is discarded; only the small group-size scheduling change
is proposed here.

## Change and invariants

The packer derives a nonpersisted `large_group` bit while materializing the
existing logical inputs. Every member must reach 64 KiB; mixed and smaller
groups clear the bit. The existing endpoint snapshot carries it alongside the
logical prefix metadata. A relaxed atomic records changes in the last packed
group's bit. Point-only traffic keeps that atomic false, so the coordinator
avoids additional admission/durability locks for this classification.

A true cached hint is not sufficient to skip waiting. The coordinator checks
all endpoint snapshots in `(durable_frontier, written_frontier]`, requires
known large membership throughout, and requires a snapshot at the written
endpoint. Unknown, mixed, small, or empty waves keep the existing policy. The
cache can conservatively suppress the optimization; it cannot replace the
membership check or prove durability.

After a sync shorter than 100 microseconds, a known large wave selects a
zero-duration coalescing wait. The existing bounded result drain still runs
before the next target is captured. Point/mixed waves keep the 100-microsecond
wait. Syncs at least 100 microseconds retain 400-microsecond coalescing for all
workloads. The first-cycle behavior is unchanged.

DATA framing, ordered SHA-256, buffer ownership, resource charges, physical
allocation, FRONTIER construction, poison behavior, and recovery validation
use the retained implementation. ACK still follows a successful FRONTIER write
and WAL sync. More frequent frontiers remain covered by each batch's existing
worst-case marker credit. The wire format and digest domains are unchanged;
physical grouping and marker counts can differ with scheduling. No new worker,
public configuration, or read-path behavior is introduced. V4's waiting policy
is unchanged.

New tests cover complete unsynced waves, mixed group membership, durable
cutoffs, missing endpoint metadata, and classification reset between groups.
The materializer test uses real encoded logical inputs, exercises a large
batch alongside a small batch, and verifies transient buffer cleanup. The
full `cargo make check` passes 1,965 tests with zero skips, retries, or slow
tests: nextest `cf24ba78-b356-4425-9f7f-f9fc7cebd319`. Formatting, dependency
sorting, all-target/all-feature Clippy, unused dependencies, and spelling pass.
Release build passes. Logs remain under
`target/wal-opt/20261026-v7-large-group-hint-cache`.

## Fixed comparison

The source base is `20b011b1`. The executable is
`target/release/write-perf-v7-large-group-hint-cache`, SHA-256
`f9d920a1980bb6a334bedbafed1d661f313ebef3ae39660a1f1d7ddb7e093fe1`.
Its [128-input source manifest](artifacts/rfc-025-v7-large-group-hint-cache-20261026-source-sha256.tsv)
has SHA-256 `465481f669077c6642d7b82324f5598d157cfc1b5de6843650be417b9374385c`.
The [patch](artifacts/rfc-025-v7-large-group-hint-cache-20261026-rejected.patch)
reconstructs the measured source. The retained executable is
`target/release/write-perf-v7-validated-logical-batch`, SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
Both builds use:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

The protocol matches the preceding candidates: four warmups and three
alternating four-arm pairs in each CPU cell, using retained/candidate v4 and
v7. A passing CPU screen permits a three-pair solo guard and five-pair ext4
batch64 and point comparisons. Batch64 writes 750,000 puts and samples every
ten batches, for 1,176 observations. Point cells write 100,000 puts and sample
every 100 operations, for 1,000 observations. Eight writers use 1-KiB values
and a 2-GiB SST target; the solo guard changes only writer count.

Tmpfs has three-second gaps. Ext4 uses the same Intel Core i9-13900T and
Solidigm SSDPFKNU010TZ, on `/dev/nvme0n1p3`, with a 30-second initial cooldown
and ten-second gaps. Databases are fresh and removed after capture. Setup/close
are outside the measured interval. Pipeline counters reset after setup and are
captured before drain/close. Checks, builds, and profiling never overlap scored
comparisons. All controls and stalled records remain included, with no
peak-selected repeats.

The fixed screen requires raw and v4-normalized tmpfs batch throughput to reach
baseline, batch CPU growth at most 5%, and point raw loss at most 5%. Retention
also requires the solo and both SSD raw throughput guards to stay within 5%.
CPU and sampled latency costs are reported separately. Ratios are medians of
paired ratios; normalization compares each v7/v4 rate with its same-executable
control. CPU is timed process user plus system time. These screens do not
qualify the complete RFC Stage 8 workload or operational gate.

The [driver](artifacts/rfc-025-v7-large-group-hint-cache-20261026-run.py) freezes
[binary identities](artifacts/rfc-025-v7-large-group-hint-cache-20261026-binaries.json),
refuses overwrite, and removes scratch databases. The
[analyzer](artifacts/rfc-025-v7-large-group-hint-cache-20261026-analyze.py)
verifies hashes, settings, latency sample counts, and complete schedules.

## CPU screen

| Cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs batch64 | 1.101x | 1.122x | 1.006x | 0.906x | 1.133x |
| tmpfs point, 8 writers | 1.064x | 1.051x | 0.963x | 1.021x | 0.780x |

Both CPU guards pass. Batch sampled p99 rises 13.3% despite higher throughput
and lower sampled p50; the change does not improve every latency metric.

## Original solo and SSD screen


| Cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs point, 1 writer | 0.976x | 1.127x | 1.020x | 1.003x | 1.357x |
| ext4 batch64 | 0.968x | 0.922x | 1.029x | 0.994x | 1.003x |
| ext4 point, 8 writers | 0.984x | 1.003x | 1.033x | 1.004x | 0.987x |

The original raw throughput/CPU guards pass. However, the batch SSD rates are
bimodal: the retained/candidate per-arm medians are 414,449/260,372 puts/s,
while the median paired ratio is 0.968x. The paired normalized ratio falls
7.8%. These different summaries expose an unresolved concern, rather than
establishing typical SSD improvement. Solo sampled p99 also rises 35.7%.

## Fixed confirmation and final rejection

Before collecting more results, the follow-up fixes exactly one additional
five-pair SSD batch comparison with the same binaries, settings, four warmups,
and cooldowns. All original and confirmation pairs are pooled, including every
stall. Retention now additionally requires pooled raw and normalized ratios
and the ratio of per-arm median rates to each reach 0.95. This stricter check
addresses the observed bimodality; it never loosens the original guards.

| Ten-pair SSD batch statistic | Candidate / retained |
| --- | ---: |
| Paired throughput | 1.001x |
| v4-normalized throughput | 0.990x |
| Paired process CPU | 0.995x |
| Sampled p50 | 0.983x |
| Sampled p99 | 0.970x |
| Ratio of per-arm median rates | 0.946x |

The pooled paired throughput is nearly flat (+0.1%), with normalized throughput
-1.0% and CPU -0.5%. The retained/candidate per-arm median rates remain
402,882/381,063 puts/s, a ratio of 0.94584x. This fails the predeclared 0.95
confirmation floor, so the candidate is rejected. No further repeats follow.

The original [summary](artifacts/rfc-025-v7-large-group-hint-cache-20261026-summary.json) retains its initial
screen result. The [pooled analyzer](artifacts/rfc-025-v7-large-group-hint-cache-20261026-pooled.py) reproduces
the [confirmation result](artifacts/rfc-025-v7-large-group-hint-cache-20261026-pooled.json), whose retention
extension is `false`. Different medians are not interchangeable; peaks do not
override this failure. All 96 scored records and 24 warmups for this candidate,
including the confirmation, remain available.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs-batch64 | [script](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-batch64-20261026.jsonl) |
| tmpfs-point8 | [script](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-point8-20261026.jsonl) |
| tmpfs-point1 | [script](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-point1-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-point1-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-large-group-hint-cache-tmpfs-point1-20261026.jsonl) |
| ext4-batch64 | [script](artifacts/rfc-025-v7-large-group-hint-cache-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-large-group-hint-cache-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-large-group-hint-cache-ext4-batch64-20261026.jsonl) |
| ext4-point8 | [script](artifacts/rfc-025-v7-large-group-hint-cache-ext4-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-large-group-hint-cache-ext4-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-large-group-hint-cache-ext4-point8-20261026.jsonl) |
| ext4 batch confirmation | [script](artifacts/rfc-025-v7-large-group-hint-cache-confirmation-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-large-group-hint-cache-confirmation-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-large-group-hint-cache-confirmation-ext4-batch64-20261026.jsonl) |

## Strongest comparable scored references

| Longer ext4 cell | v4 control | Retained v7 | Candidate v7 |
| --- | ---: | ---: | ---: |
| Batch64 puts/s | 412,503 | 480,373 | 453,655 |
| Point puts/s | 18,379 | 16,574 | 16,411 |

These peaks use the same longer operation counts, 2-GiB SST target, and sampling
settings across this investigation. The retained v7 peak uses the unchanged
`614f0cb1` executable. The [peak analyzer](artifacts/rfc-025-v7-large-group-hint-cache-20261026-peaks.py) and
[reference identities](artifacts/rfc-025-v7-large-group-hint-cache-20261026-peaks.json) retain every winning
run ID, binary hash, raw hash, and input identity. They exclude rejected v7
binaries from the retained reference and exclude warmups/profiles from scoring.

Earlier short-cell references remain 20,420 point / 482,410 batch64 puts/s for
v4 and 15,759 / 391,992 for retained v7. Those use 50K/100K puts, a 1-GiB SST
target, and coarser batch sampling. The longer-cell peaks do not erase those
historical measurements or establish parity for every writer count/workload.

## Restoration and verification

All Rust changes from this investigation are reverted byte-for-byte to
`20b011b1`, with the runtime source modification time refreshed before checking.
The default `target/release/write-perf` is restored to retained SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`. A fresh
`cargo make check` passes all 1,963 retained-source tests with zero skips,
retries, or slow tests: nextest `800ffc4e-5167-480a-83a9-a30838c97a64`. The
restoration log is `target/wal-opt/20261026-v7-large-group-hint-cache/restored-check.log`.

The [verification script](artifacts/rfc-025-v7-large-group-hint-cache-20261026-verify.py) reconstructs seven
128-input source variants from `20b011b1`, verifies saved executable hashes,
checks the restored source/default executable, regenerates all summaries,
and checks complete schedules and distinct IDs. Across the investigation,
all 408 scored records, 112 warmups, and three unscored profiles are preserved;
all 523 run IDs are distinct. Scratch databases are removed, and work stays in
the local repository apart from disposable tmpfs benchmark databases.

No hash offload, new ownership, or coalescing-policy change remains. The eleven
earlier optimizations are preserved. V4 parity and the full RFC Stage 8 gate
remain unmet. A future coalescing experiment should distinguish a cheap
`fdatasync` from a cheap full v7 durability cycle: the current timer excludes
FRONTIER write and preparation cost. That is a remaining hypothesis, not an
implemented or qualified improvement.

Source patches use zero-context diffs; apply them to the stated base with
`git apply --unidiff-zero`. The verifier also checks exact reconstructed bytes.

[WAL documentation](../README.md).
