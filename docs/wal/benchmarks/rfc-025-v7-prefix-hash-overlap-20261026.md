# RFC 025: fixed coordinator hashing experiments, 2026-10-26

**Result: both fixed hashing policies are rejected.**

This experiment follows the [critical-path profile](rfc-025-v7-critical-path-20261026.md).
It moves large ordered logical hashes from the packer to the sync coordinator,
allowing later DATA framing and I/O to proceed during hashing. Small groups
retain inline hashing when their ordered hash state is available. The original
100/400-microsecond coalescing policy remains.

## Change and correctness

The packer frames each DATA group, including the existing CRCs and ticket
checks. Logical inputs above 32 KiB per group enter an ordered FIFO before
the group is submitted to the worker. After capturing a contiguous written
target, the coordinator hashes that FIFO through the target, appends FRONTIER,
and performs the existing single `fdatasync` before publishing durability.
Neither admission nor the packer mutex is held while the coordinator hashes.
No new worker thread, external queue, format, or configuration is introduced.

A group at or below 32 KiB tries to acquire the hash-state mutex without
waiting. It hashes inline only when the hash cursor reaches its first ticket.
A busy state or deferred predecessor sends the group through the FIFO instead.
This preserves one serial hash order across small and large inputs. Exact
endpoint snapshots remain available if the packer hashes ahead of the
coordinator's captured durability target. Large inputs can overlap later DATA
preparation; small point writes avoid a mandatory coordinator handoff.

Logical bytes keep a separate pin on the existing batch operation. Reaping a
DATA CQE and recycling its aligned allocation cannot release the logical
buffer's resource charge before hashing finishes. Cancellation, poison, close,
and durable image cleanup retain their original ownership rules. The immutable
logical bytes, digest domain, timestamps, FRONTIER layout, recovery validation,
and sync-before-ACK gate remain unchanged.

The overlap test blocks hashing and verifies later DATA can be framed,
submitted, and reaped while the durable frontier stays at genesis and logical
memory stays charged. The mixed-group test covers inline snapshots ahead of a
captured target, a small group behind a deferred large predecessor,
out-of-order DATA completion, marker recapture, exact recovery, and zero
transient charges after close. Separate tests verify a coordinator hash error
poisons admission while retaining acknowledged recovery data, and a detached
logical input retains its charge across close, handle drop, and durable cleanup.

`cargo make check` passes all 1,966 tests with zero skips, retries, or slow tests:
nextest `37a31596-cc8a-4e08-b1e9-42f1a52e3417`. Formatting, dependency sorting,
all-target/all-feature Clippy, unused-dependency checks, and spelling pass.
The release build passes. Logs remain under
`target/wal-opt/20261026-v7-prefix-hash-overlap`.

## First variant: defer every group, rejected

Deferring every group improves the tmpfs batch64 screen 20.5% raw and 18.0%
v4-normalized, with 1.0% more process CPU and a 0.660x sampled p99. However,
eight-writer point throughput falls 9.9% raw and 10.4% normalized, with 4.6%
more CPU and 1.139x sampled p99. The point guard rejects that variant.

The solo cell had already been launched before the failed CPU gate was acted
on; its complete records are preserved. It changes raw throughput +3.4% and
normalized throughput -0.1%. The qualification parent is stopped during the
next driver's initial cooldown. No SSD benchmark record is captured; only its
empty capture files and empty scratch directory are removed. This additional
solo result does not override the failed point guard.

All 36 scored records and 12 warmups remain. The first variant passes the full
check with 1,965 tests, including one unrelated slow compaction test (nextest
`3fb53c9c-35f7-4596-b180-bfc7d3f94865`). Its executable is
`target/release/write-perf-v7-deferred-prefix-hash`, SHA-256
`56d6bd0e888cc1e821e7204d6b9fcc29b706581c116d136c95f1a949779052bd`.
The [source patch](artifacts/rfc-025-v7-deferred-prefix-hash-20261026-rejected.patch)
applies to `20b011b1`. The
[128-input manifest](artifacts/rfc-025-v7-deferred-prefix-hash-20261026-source-sha256.tsv)
has SHA-256 `46f0c9f679049ae968f72a8f89ce7e4359ea16d7210b2abf0a669db0044a550a`.
The [binary identities](artifacts/rfc-025-v7-deferred-prefix-hash-20261026-binaries.json),
[driver](artifacts/rfc-025-v7-deferred-prefix-hash-20261026-run.py),
[analyzer](artifacts/rfc-025-v7-deferred-prefix-hash-20261026-analyze.py), and
[summary](artifacts/rfc-025-v7-deferred-prefix-hash-20261026-summary.json)
keep the rejected variant separate from the revised candidate.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs batch64 | [script](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-batch64-20261026.jsonl) |
| tmpfs point, 8 writers | [script](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-point8-20261026.jsonl) |
| tmpfs point, 1 writer | [script](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-point1-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-point1-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-deferred-prefix-hash-tmpfs-point1-20261026.jsonl) |

## Revised candidate and fixed protocol

The revised source base is `20b011b1`. The executable is
`target/release/write-perf-v7-prefix-hash-overlap`, SHA-256
`e7e7eeece8e954bd56ede6260b40ae14cb15b5b5622c7a9cde08830ac53cade5`.
Its [128-input source manifest](artifacts/rfc-025-v7-prefix-hash-overlap-20261026-source-sha256.tsv)
has SHA-256 `8aa7224222d3505047eee43c0c249659a439ba41e0015642bd35557528dc23b2`.
The [patch](artifacts/rfc-025-v7-prefix-hash-overlap-20261026-rejected.patch)
reconstructs the candidate from that base. Both variants are compared against
the exact retained immutable-batch executable, SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
Release builds use:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

CPU cells use four warmups and three alternating four-arm pairs. A passing
screen permits a three-pair solo guard, followed by five-pair ext4 batch64 and
point comparisons. Every pair contains retained/candidate v4 and v7 controls.
Batch64 writes 750,000 puts and samples every ten batches, for 1,176 latency
samples. Point cells write 100,000 puts and sample every 100 operations, for
1,000 samples. Eight writers use 1-KiB values and a 2-GiB SST target; the solo
guard changes only the writer count.

Tmpfs has three-second gaps. Ext4 uses the local repository on
`/dev/nvme0n1p3`, with a 30-second initial cooldown and ten-second gaps. The host
is the Intel Core i9-13900T and Solidigm SSDPFKNU010TZ. Every record uses a fresh
database. Setup and close stay outside the timed window; profile counters reset
after setup and are captured before drain/close. Checks, builds, and profiling
never overlap scored runs. All moving controls and stalls remain included.

The fixed screen requires raw and v4-normalized batch throughput to reach
baseline, batch CPU growth at most 5%, and point raw loss at most 5%. Retention
also requires the solo and both SSD raw throughput guards to stay within 5%.
CPU and sampled p50/p99 outcomes are reported alongside throughput; passing
these screens does not establish universal nonregression or RFC Stage 8.
Ratios are medians of paired ratios, with each v7/v4 ratio normalized to its
same-executable control. CPU is timed process user plus system time.

## CPU screen


| Cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs batch64 | 1.152x | 1.153x | 1.041x | 0.802x | 1.071x |
| tmpfs point, 8 writers | 0.994x | 0.969x | 1.053x | 1.011x | 1.016x |
| tmpfs point, 1 writer | 1.009x | 0.978x | 0.990x | 0.993x | 1.020x |

The revised CPU screen passes its fixed throughput/CPU guards. Point8 uses
5.3% more CPU despite nearly flat raw throughput; its v4-normalized rate falls
3.1%. The solo normalized rate falls 2.2%. These results do not support a
uniform CPU or latency improvement claim. SSD qualification is evaluated
separately.

## SSD result: revised fixed policy rejected


| Cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| ext4 batch64 | 0.685x | 0.679x | 1.686x | 1.599x | 1.130x |
| ext4 point, 8 writers | 0.655x | 0.639x | 1.152x | 1.062x | 1.111x |

The fixed policy fails both SSD guards. Batch raw throughput falls 31.5% and
CPU rises 68.6%; point throughput falls 34.5%. One batch pair favors the
candidate by more than 3x because its retained control stalls; that pair stays
in the data and does not override the unfavorable median. All 76 scored
records and 20 warmups from the revised fixed policy remain available.

The source permits packer hashing to overlap a preceding slow sync. Moving it
onto the sync coordinator can lose that overlap; the new shared hash-state
handoff can also contend with inline point groups. The measurements do not
isolate every cause of the CPU increase or establish a core-placement cause.
The fixed policy is rejected. A separate candidate uses observed sync latency
as a scheduling hint and reuses completed endpoint snapshots without taking
the hash-state mutex.

The [analyzer](artifacts/rfc-025-v7-prefix-hash-overlap-20261026-analyze.py)
reproduces the complete [summary](artifacts/rfc-025-v7-prefix-hash-overlap-20261026-summary.json),
whose retention guard is `false`. The [driver](artifacts/rfc-025-v7-prefix-hash-overlap-20261026-run.py)
and [binary identities](artifacts/rfc-025-v7-prefix-hash-overlap-20261026-binaries.json)
keep both executables fixed across all cells.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs-batch64 | [script](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-batch64-20261026.jsonl) |
| tmpfs-point8 | [script](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-point8-20261026.jsonl) |
| tmpfs-point1 | [script](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-point1-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-point1-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-prefix-hash-overlap-tmpfs-point1-20261026.jsonl) |
| ext4-batch64 | [script](artifacts/rfc-025-v7-prefix-hash-overlap-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-prefix-hash-overlap-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-prefix-hash-overlap-ext4-batch64-20261026.jsonl) |
| ext4-point8 | [script](artifacts/rfc-025-v7-prefix-hash-overlap-ext4-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-prefix-hash-overlap-ext4-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-prefix-hash-overlap-ext4-point8-20261026.jsonl) |

Both fixed-policy patches reconstruct their 128-input manifests from
`20b011b1`. All 144 fixed-policy run IDs are distinct. These comparisons do not
qualify the RFC Stage 8 workload gate. The eleven earlier retained optimizations
are unaffected by rejection.

Source patches use zero-context diffs; apply them to the stated base with
`git apply --unidiff-zero`. The verifier also checks exact reconstructed bytes.

[WAL documentation](../README.md).
