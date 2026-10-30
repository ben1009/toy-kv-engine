# RFC 025: ordered v7 encoding outside admission, 2026-10-25

**Result: retain ordered packer encoding for its concurrent-batch gain. V4
parity remains unmet.** The highest ext4 batch64 score rises from the previous
retained v7 high of 194,069 to 341,049 puts/s, a 75.7% improvement. This is
still 29.3% below the highest comparable v4 score of 482,410. Single puts
remain approximately 25.2% below their highest comparable v4 score.

This follows the rejected
[resource-retirement experiments](rfc-025-v7-resource-retirement-20261025.md).
Those accounting changes remain reverted. This pass changes where existing
DATA framing and logical SHA-256 work run in the v7 Parallel pipeline.

## Change and ordering

Previously, v7 admission framed DATA, verified the logical batch, updated its
streaming logical hash, and copied a hash snapshot for each admitted ticket.
Synchronous admission runs inside the MVCC write sequencer. Large batches
therefore held both shared locks while encoding their payloads.

Admission now assigns the same ticket, timestamp, and physical allocation,
then queues the owned logical bytes and charged DATA buffer. The existing
ordered packer frames DATA and advances its logical hash in ticket order,
while holding neither WAL admission nor the MVCC sequencer. It publishes one
digest snapshot for the packed group's endpoint before submitting that group
to the I/O worker. Out-of-order CQEs therefore still select the digest for
their exact contiguous completed prefix.

The packer reuses bounded group scratch storage. Existing reservations cover
both logical bytes and DATA buffers until materialization consumes the input;
moving ownership into the queue does not bypass admission limits. A framing
failure after coordinate assignment poisons the entire unsubmitted group.
Persistent image charges remain owned until close/recovery, and previously
acknowledged tickets remain recoverable. Expected capacity and metadata
rejections still precede coordinate assignment.

The existing full frame encoder, canonical byte order, CRC checks, SHA
preimages, one-sync protocol, recovery rules, and format defaults are retained.
The ordinary v4 packing path continues to queue already encoded buffers.

## Executables and protocol

The working source is `f08f4cd7f0bac424d9732837401a89a76ec5b5b6` plus the
local optimization changes. This pass changes only
`kv-engine/src/wal/parallel/runtime.rs` among the Rust files. The
[source manifest](artifacts/rfc-025-v7-packer-encoding-20261025-source-sha256.tsv)
records SHA-256 hashes for the candidate's Rust sources, Cargo files, lockfile,
and toolchain pin. Its SHA-256 is
`d3f91476235483f63da9eb31193fd15aec750ae64fd10b6f1407616d5146d6d4`.

Both executables use
`cargo build --release --package kv-engine --bin write-perf --features bench`:

| Build | Binary SHA-256 |
| --- | --- |
| Retained extent-initialization baseline | `06efc8d7dc473ddfd770832c9075c6cfffdc9ab05df13460481d4f2302f2625b` |
| Ordered packer encoding | `9a9a108ae23a3928ad88e42664ecc79c85a8e2d3f8ed5f81ef6aeb3b034e469e` |

Each experiment includes both executables' v4 controls and v7/PITR paths, one
unscored warmup per arm, and five alternating four-arm scored pairs. Every
run uses a fresh database, 1-KiB values, a 1-GiB SST target, Parallel I/O,
latency sampling every 100 operations, and three seconds idle afterward.
Single-put runs issue 50,000 puts; batch64 runs issue 100,000 puts. Eight
writers test concurrent throughput; one writer checks low-concurrency cost.

The host is an Intel Core i9-13900T with 32 logical CPUs, Linux
6.12.71-1-lts, and the pinned nightly Rust toolchain. Tmpfs uses `/dev/shm`;
ext4 uses the repository's filesystem on the Solidigm SSDPFKNU010TZ. Source
and executables stay in the local repository. Tmpfs holds temporary benchmark
databases only. Tests/builds and benchmarks run sequentially, outside the
sandbox for io_uring access. All scored runs, including SSD stalls, remain
in the analysis. PITR setup and close are outside the measured write interval.

## Paired measurements

Ratios compare candidate v7 to baseline v7 within each pair. The normalized
ratio additionally divides by the candidate-v4/baseline-v4 throughput ratio
from that pair. A median of paired ratios can differ substantially from the
ratio of the two arm medians, especially when SSD stalls hit different arms.
Process CPU is the measured user plus system CPU time.

| Workload | Baseline / candidate v7 median puts/s | Median paired throughput | Median v4-normalized ratio | Median CPU ratio |
| --- | ---: | ---: | ---: | ---: |
| tmpfs, 8 writers, single puts | 107,897 / 109,473 | 0.989x | 1.034x | 0.933x |
| ext4, 8 writers, single puts | 14,294 / 8,401 | 1.003x | 1.048x | 0.826x |
| ext4, 8 writers, batch64 | 67,881 / 318,341 | 1.904x | 1.810x | 0.553x |
| tmpfs, 8 writers, batch64 | 187,472 / 649,130 | 3.517x | 3.220x | 0.390x |
| tmpfs, 1 writer, single puts | 41,322 / 40,020 | 0.976x | 0.891x | 1.003x |
| tmpfs, 1 writer, identical-binary repeat | 40,458 / 41,528 | 1.010x | 1.060x | 0.977x |

| Workload | Median paired sampled p50 ratio | Median paired sampled p99 ratio |
| --- | ---: | ---: |
| tmpfs, 8 writers, single puts | 0.892x | 1.011x |
| ext4, 8 writers, single puts | 0.995x | 1.036x |
| ext4, 8 writers, batch64 | 0.620x | 0.825x |
| tmpfs, 8 writers, batch64 | 0.632x | 0.616x |
| tmpfs, 1 writer, single puts | 1.017x | 1.006x |
| tmpfs, 1 writer, identical-binary repeat | 1.003x | 1.024x |

The first single-writer screen's 2.4% lower paired raw throughput prompted a
complete repeat with the identical binaries. The repeat is 1.0% faster raw
and 6.0% faster normalized. Across all ten pairs, raw throughput is 1.6%
lower, the normalized ratio is 0.7% higher, CPU changes by 0.998x, and sampled
p50/p99 change by 1.010x / 1.015x. These mixed small differences do not
establish low-concurrency equivalence or a no-regression result. All ten
pairs, including the first screen's unfavorable normalized result, are kept.

Tmpfs confirms a large batch64 software gain without physical SSD stalls:
median paired throughput improves 3.52 times and process CPU falls 61.0%.
The fastest v7 tmpfs batch64 result is 734,359 puts/s, compared with the
candidate executable's v4 high of 1,154,851. Even here, batch64 parity is not
established.

Ext4 single puts do not establish a meaningful improvement over the previous
v7 peak. Both binaries stall: the third/fourth pairs have v7 around
4.4–4.7k puts/s, and the last pair has baseline/candidate v7 at 14,524 /
8,401. The median paired ratio is near one despite the much lower candidate
arm median. The mixed point-write measurements do not support a claim of
no regression.

Ext4 batch64's last two pairs show baseline/candidate v7 at 174,681 / 332,629
and 177,164 / 341,049 puts/s. The earliest pairs stall in both v4 and v7;
none are excluded. Tmpfs replication and the two stable ext4 pairs support
retaining the concurrent-batch change.

## Highest comparable ext4 scores

As requested, compare the highest scored runs under matching workload
settings to reduce the effect of SSD stalls. The historical references are
the [extent-initialization point and batch64 runs](rfc-025-v7-parallel-vs-v4-parallel-writer-sweep-20261025.md#shared-v7-extent-initialization-on-ext4)
and the baseline arm of the
[resource-retirement batch64 repeat](rfc-025-v7-resource-retirement-20261025.md).
Warmups and rejected candidate binaries are excluded from retained-v7 peaks.
These maxima come from different runs and do not replace paired comparisons
or establish statistical equivalence.

| Workload, 8 writers | Previous retained v7 high | Candidate v7 high | Highest comparable v4 | Remaining v7 gap |
| --- | ---: | ---: | ---: | ---: |
| Single puts, 50,000 operations | 15,256 | 15,278 | 20,420 | 25.2% |
| Batch64, 100,000 operations | 194,069 | 341,049 | 482,410 | 29.3% |

The current experiment's highest v4 batch64 result is 368,686 puts/s, making
candidate v7 92.5% of that score. Using that number alone would conceal the
remaining gap to the historical v4 high. The candidate improves the highest
retained v7 batch64 score by 75.7%, while its point-write peak improves only
0.14%.

At the fastest ext4 v7 batch64 runs, baseline/candidate commit-group counts
are 1,557 / 1,134 and FRONTIER/sync counts are 706 / 364. Both write the same
108,822,528 DATA bytes. Process CPU falls from 1,181 to 588 ms. These counters
are consistent with less admission serialization and better grouping; they
are observations from separate best runs, not an isolated causal breakdown.

## Correctness and remaining work

The new deterministic tests hold the packer while admitting single-frame and
multi-frame batches, confirm admission leaves DATA unframed and its hash
unchanged, then verify the exact recovered logical prefix. A second test
damages a queued logical batch after an earlier ACK: the entire affected
group fails without submission, later admission rejects, close releases its
transient resources, and recovery preserves exactly the earlier ACK. It does
not arm a process-global failpoint.

The focused run passes 208 tests, covering Parallel runtime, v7 codec,
recovery, native writes, rotation, and async large batches. Its nextest run ID
is `342c0eca-7472-40d2-ba4f-4a100b1fa6b9`. The complete `cargo make check`
passes all 1,958 tests and formatting, dependency ordering, all-target
all-feature Clippy with warnings denied, unused-dependency checks, and spelling.
Its nextest run ID is `5fa3080a-619c-4495-8925-67d3b984af6e`.

The retained change targets concurrent-batch encoding. Point writes,
low-concurrency behavior, and the remaining v4 gap still need attention. This
screen does not complete RFC 025's broader workload, device, sanitizer, or
recovery-performance qualification, and does not pass the Stage 8 gate.

## Reproduction and artifacts

The [analyzer](artifacts/rfc-025-v7-packer-encoding-20261025-analyze.py) checks
raw-record hashes against each manifest, validates executable hashes and
workload parameters, and reproduces paired ratios and historical maxima.
The [summary JSON](artifacts/rfc-025-v7-packer-encoding-20261025-summary.json)
includes sampled latencies, CPU ratios, exact winning run IDs, and pipeline
counters. It uses the preceding
[shared analysis helper](artifacts/rfc-025-v7-mvcc-preparation-20261025-analyze.py).
The six datasets contain 120 scored records and 24 unscored warmups.

| Experiment | Exact driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs, 8 writers, single puts | [script](artifacts/rfc-025-v7-packer-encoding-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-packer-encoding-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-packer-encoding-tmpfs-8writers-20261025.jsonl) |
| ext4, 8 writers, single puts | [script](artifacts/rfc-025-v7-packer-encoding-ext4-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-packer-encoding-ext4-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-packer-encoding-ext4-8writers-20261025.jsonl) |
| ext4, 8 writers, batch64 | [script](artifacts/rfc-025-v7-packer-encoding-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-packer-encoding-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-packer-encoding-ext4-batch64-8writers-20261025.jsonl) |
| tmpfs, 8 writers, batch64 | [script](artifacts/rfc-025-v7-packer-encoding-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-packer-encoding-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-packer-encoding-tmpfs-batch64-8writers-20261025.jsonl) |
| tmpfs, 1 writer, single puts | [script](artifacts/rfc-025-v7-packer-encoding-tmpfs-1writer-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-packer-encoding-tmpfs-1writer-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-packer-encoding-tmpfs-1writer-20261025.jsonl) |
| tmpfs, 1 writer, identical-binary repeat | [script](artifacts/rfc-025-v7-packer-encoding-tmpfs-1writer-20261025-rerun1-run.sh) | [TSV](artifacts/rfc-025-v7-packer-encoding-tmpfs-1writer-20261025-rerun1-runs.tsv) | [JSONL](artifacts/rfc-025-v7-packer-encoding-tmpfs-1writer-20261025-rerun1.jsonl) |

[WAL documentation](../README.md).
