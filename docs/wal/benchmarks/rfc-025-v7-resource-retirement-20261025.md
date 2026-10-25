# RFC 025: v7 resource-retirement experiments, 2026-10-25

**Result: both retirement candidates are rejected.** The replicated CPU-side
gain comes with lower batch64 throughput, and neither candidate establishes
an SSD gain. Source is restored to the preceding extent-initialization build.
Its highest retained ext4 score remains 15,256 puts/s versus v4's 20,420, a
25.3% gap. V4 parity remains unmet.

This pass follows the
[extent-initialization measurements](rfc-025-v7-parallel-vs-v4-parallel-writer-sweep-20261025.md#shared-v7-extent-initialization-on-ext4).
It tests combining unused FRONTIER-credit and DATA-buffer retirement after
a successful durability advance. No wire bytes, durability barriers, recovery
rules, or format defaults change.

## Diagnosis

In the highest earlier scored ext4 runs, v4 and the retained v7 build spent
1.588 / 1.602 seconds in `fdatasync`. V7 also spent 114 ms writing FRONTIERs,
115 ms preparing extents, and 230 ms in coalescing receive waits. These are
separate best runs rather than a causal decomposition of the throughput gap.

A fresh user-CPU profile sampled the same v7 build and its v4 path with:

```text
perf record -e cpu-clock:u -F 997 --call-graph dwarf,8192
```

V7 captured 3,650 samples, including 1.23% in unused FRONTIER-credit release
and 5.78% in producer mutex slow paths. Shared channel receives and MVCC
publication also account for substantial samples. Both profiles include setup
and close; v4 additionally drains its memtable after the measured interval.
The percentages identify candidate work and do not measure isolated effects.
The [profile manifest](artifacts/rfc-025-v7-small-extent-init-cpu-profile-20261025-runs.tsv),
[JSONL](artifacts/rfc-025-v7-small-extent-init-cpu-profile-20261025.jsonl),
[v4 report](artifacts/rfc-025-v7-small-extent-init-cpu-profile-20261025-v4.txt),
and [v7 report](artifacts/rfc-025-v7-small-extent-init-cpu-profile-20261025-v7.txt)
retain the exact commands, executable hash, and sampled symbols. These runs
are diagnostic and are excluded from scored comparisons.

## Candidate

Previously, each nonterminal ticket covered by a FRONTIER released its unused
marker credit under the image-owner and resource-ledger locks, then acquired
those locks again to retire transient buffers. Credit retirement occurred
while holding WAL admission. The candidate combines the two checked charge
updates and completes resource retirement after releasing WAL admission.

Only the final covered ticket retains the consumed marker credit. DATA frames,
source-spool usage, and seal-index entries remain owned by their installed
image until their existing release events. A remaining DATA-buffer pin takes
the conservative path: release only unused marker credit and defer buffer
retirement until the final pin drops. Pressure waiters are notified after the
operation count retires. A failed post-sync accounting update still poisons
the runtime before publication.

The baseline binary is
`06efc8d7dc473ddfd770832c9075c6cfffdc9ab05df13460481d4f2302f2625b`;
the candidate is
`34c3447f08d7ef8b05bdc037b1134318b5bf185d0141582c6b4191308cfda576`.
Both use the release build with feature `bench`. V4 controls use each binary's
ordinary Parallel path; v7 enables PITR and explicitly selects Parallel.

## Protocol and results

Every comparison uses eight writers, 1-KiB values, a 1-GiB SST target, one
warmup per arm, five alternating four-arm scored pairs, fresh databases, and
a three-second idle between runs. Single-put comparisons issue 50,000 puts;
batch64 comparisons issue 100,000 puts. All scored runs remain included.
The host and filesystem match the preceding writer-sweep report: Intel
Core i9-13900T, Linux 6.12.71-1-lts, ext4 on the Solidigm SSDPFKNU010TZ, and
tmpfs for CPU comparisons. PITR setup, rotation, and archive maintenance stay
outside the measured write interval.

| Comparison | Baseline / candidate v7 median puts/s | Median paired raw change | Median paired v4-normalized change | Median paired CPU change |
| --- | ---: | ---: | ---: | ---: |
| tmpfs puts | 105,813 / 111,758 | 1.024x | 1.053x | 0.981x |
| tmpfs puts, identical-binary repeat | 105,294 / 110,428 | 1.040x | 1.062x | 0.937x |
| ext4 puts | 6,203 / 4,622 | 0.994x | 1.006x | 0.985x |
| ext4 batch64 | 175,842 / 169,956 | 0.924x | 0.955x | 1.000x |
| ext4 batch64, identical-binary repeat | 60,860 / 58,645 | 0.975x | 0.993x | 0.984x |

Across the ten tmpfs pairs, the median normalized improvement is 5.8% and
process CPU falls by 4.1%. The two five-pair normalized medians are 5.3% and
6.2%, supporting a modest CPU-side benefit. Sampled p99 changes by `1.056x`
and `1.286x` respectively; a tail-latency improvement is not established.

Ext4 puts do not establish an improvement. Both binaries stall: pair 1 has
old/new v4 at 10,373 / 5,073 puts/s and old/new v7 at 4,527 / 4,501. Pair 5
has old/new v7 at 4,677 / 4,622. The stable third/fourth pairs are approximately
14.4–14.9k v7 puts/s. All five pairs are retained; the medians and paired-ratio
medians therefore answer different questions.

### Highest scored ext4 results

As requested, compare the highest throughput under matching settings to
reduce the influence of stalled runs. Warmups are excluded. Single puts use
eight writers, 50,000 operations and 1-KiB values in every row. The reference
v4 and previous v7 peaks are pinned to the preceding experiment's records.

| Format / build | Highest puts/s | Relative to highest v4 |
| --- | ---: | ---: |
| v4 Parallel, highest comparable prior score | 20,420 | 100.0% |
| Previous retained v7 | 15,256 | 74.7% |
| Resource-retirement candidate v7 | 14,868 | 72.8% |

The candidate's SSD peak is 2.5% below the previous retained v7 peak and 27.2%
below the highest v4 score. The CPU improvement does not establish SSD parity
or removal of device stalls.

Batch64's highest candidate score is 195,006 puts/s versus 187,393 for its
baseline in the same comparison (+4.1%). However, median paired raw throughput
falls by 7.6% and normalized throughput by 4.5%. The higher peak alone does
not establish absence of regression. The repeat has a lower candidate peak (184,368 versus 194,069 puts/s).
Across both batch64 datasets, the highest candidate result is 195,006 versus
194,069 for the baseline (+0.5%), while the median paired raw throughput
changes by `0.949x` and the normalized ratio by `0.961x`. Both five-pair
normalized medians are below one. SSD stalls remain in the records, but these
results do not justify retaining the candidate for this performance goal.
The per-ticket and whole-group code changes are both reverted.

## Whole-group variant: rejected

A second candidate retired all tickets covered by a FRONTIER under one pair
of owner/ledger locks. Its binary was
`0eb717d809a8baef5397398b086d5f5243c83b1167f3261378682d9d750005f9`.
Five tmpfs pairs gave a median raw change of `1.048x`, a normalized change of
`1.012x`, and CPU change of `0.985x`. The normalized gain was smaller than the
simpler transaction's replicated result. The extra shared-lock and deferred
operation-drop handling is not retained; no SSD performance claim is made
for this variant.

Its [driver](artifacts/rfc-025-v7-frontier-retirement-tmpfs-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-frontier-retirement-tmpfs-8writers-20261025-runs.tsv),
[JSONL](artifacts/rfc-025-v7-frontier-retirement-tmpfs-8writers-20261025.jsonl),
[analysis](artifacts/rfc-025-v7-frontier-retirement-20261025-analyze.py),
and [summary](artifacts/rfc-025-v7-frontier-retirement-20261025-summary.json)
retain that comparison. The variant passed 111 focused tests after correcting
the new three-batch fixture's quota capacities; source snapshots remain in
`target/wal-opt/20261025-v7-coalesced-retirement` for local inspection.

## Reproduction and correctness

The simpler transaction passed 108 focused tests outside the sandbox,
including exact resource charges with and without retained marker credit,
multiple DATA pins surviving image cleanup, rejection of unadmitted operations,
registered quota-waiter wakeups, Leader compatibility, cancellation, rotation,
and Parallel I/O failure recovery. Nextest run:
`122f0e5d-9bac-4976-a7be-e31c4731aa00`. Formatting and all-target/all-feature
Clippy with warnings denied passed for that exact candidate. After the
comparisons, both modified Rust files are restored byte-for-byte from their
pre-experiment snapshots, preserving all preceding optimization work.

Each dataset includes a driver, manifest, and raw measurements:

- tmpfs: [driver](artifacts/rfc-025-v7-coalesced-retirement-tmpfs-8writers-20261025-run.sh), [manifest](artifacts/rfc-025-v7-coalesced-retirement-tmpfs-8writers-20261025-runs.tsv), [JSONL](artifacts/rfc-025-v7-coalesced-retirement-tmpfs-8writers-20261025.jsonl).
- tmpfs repeat: [driver](artifacts/rfc-025-v7-coalesced-retirement-tmpfs-8writers-20261025-rerun1-run.sh), [manifest](artifacts/rfc-025-v7-coalesced-retirement-tmpfs-8writers-20261025-rerun1-runs.tsv), [JSONL](artifacts/rfc-025-v7-coalesced-retirement-tmpfs-8writers-20261025-rerun1.jsonl).
- ext4 puts: [driver](artifacts/rfc-025-v7-coalesced-retirement-ext4-8writers-20261025-run.sh), [manifest](artifacts/rfc-025-v7-coalesced-retirement-ext4-8writers-20261025-runs.tsv), [JSONL](artifacts/rfc-025-v7-coalesced-retirement-ext4-8writers-20261025.jsonl).
- ext4 batch64: [driver](artifacts/rfc-025-v7-coalesced-retirement-ext4-batch64-8writers-20261025-run.sh), [manifest](artifacts/rfc-025-v7-coalesced-retirement-ext4-batch64-8writers-20261025-runs.tsv), [JSONL](artifacts/rfc-025-v7-coalesced-retirement-ext4-batch64-8writers-20261025.jsonl).
- ext4 batch64 repeat: [driver](artifacts/rfc-025-v7-coalesced-retirement-ext4-batch64-8writers-20261025-rerun1-run.sh), [manifest](artifacts/rfc-025-v7-coalesced-retirement-ext4-batch64-8writers-20261025-rerun1-runs.tsv), [JSONL](artifacts/rfc-025-v7-coalesced-retirement-ext4-batch64-8writers-20261025-rerun1.jsonl).

The [analysis script](artifacts/rfc-025-v7-coalesced-retirement-20261025-analyze.py)
validates executable hashes, per-record hashes and workload settings and
reproduces the [summary](artifacts/rfc-025-v7-coalesced-retirement-20261025-summary.json),
including pinned prior peaks and paired medians. Benchmark scratch databases
are removed after each run. This evidence remains exploratory and does not
pass the RFC 025 performance gate.

The restored source passes the complete repository check outside the sandbox:

```bash
cargo make check
```

All 1,956 workspace library, binary and integration tests pass, along with
formatting, dependency ordering, all-target/all-feature Clippy, unused-dependency
checks and spelling. The local check log is
`target/wal-opt/20261025-v7-coalesced-retirement/check.log`. Both restored Rust
files match their pre-experiment snapshots byte-for-byte, and the default
release `write-perf` executable is restored from the retained baseline binary.
