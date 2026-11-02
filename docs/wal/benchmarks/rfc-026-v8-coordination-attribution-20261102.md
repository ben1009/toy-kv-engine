# RFC 026 V8 versus V7: timed coordination attribution (2026-11-02)

## Result

V8's extra CPU is real, but it is not concentrated in ticket-container
operations. The strongest signals are producer wakeup traffic, memtable
publication and MVCC publication. The bounded ticket queue is already a
preallocated `VecDeque`; replacing that queue with another ring would not
remove those costs.

A smaller experiment mirrored poison, durable-ticket and change-generation
status in atomics, leaving authoritative transitions and wait predicates under
the existing mutex. It passed 82 V8 tests but failed the eight- and
sixteen-writer throughput screen. It is **not retained**. Temporary attribution
hooks have also been removed. The runtime remains the pre-experiment version;
PR #532 remains open and is not merged.

## Method and limits

All runs use the local repository on ext4, Solidigm P41 Plus, Parallel WAL,
750,000 puts, batches of 64, sixteen writers unless specified, 1 KiB values,
no compaction, a 2 GiB target SST and latency sampling every ten batches.
Release builds enable `bench`. No digest, wire, FRONTIER, sync, DATA grouping
or coalescing rule changed in the attribution builds.

The [plans](artifacts/rfc-026-v8-coordination-attribution-20261102-plans.json)
record executable hashes and schedules. The initial profile reverses V7/V8
order; a complete follow-up also captures V8 worker/reaper thread statistics
by dropping the closed engine before exporting them. A separate reversed
control cohort disables scope/allocation hooks and CPU sampling. The scope
and control cohorts enable detailed WAL diagnostics with `--profile`. A
normal-counter follow-up disables those diagnostics, but both samples stall
and cannot establish a timing comparison.

`perf record` observes only the timed write interval, using a control FIFO.
Its output stays in a Linux `memfd` until measurement ends, then is saved in
the repository. Setup, PITR enablement, close and maintenance are outside the
interval. Scope timings sample one invocation per 64, per phase and thread;
CPU uses the thread CPU clock. A temporary allocator wrapper counts Rust
allocations/frees; native `DirectBuf` allocations are counted separately.
`getrusage` measures timed process context-switch deltas.

Scope CPU/wall estimates are **inclusive**, extrapolated separately per thread
and phase. Do not sum nested rows. Clock, TLS and allocator instrumentation
add overhead; allocator instrumentation alone occupies roughly 4–6% of the
sampled user CPU. Tiny lock scopes include timing overhead. These are
diagnostics, not performance qualification or precise lock-hold measurements.
Thread resource deltas start at the first observed scope; process deltas cover
the full timed interval.

The unchanged exploratory stall flag is p99 above 10 ms or mean fdatasync
above 1 ms. Best-score tables select the highest eligible throughput and
retain p99 from the same sample. Eligibility does not prove an absence of
isolated stalls. All 20 measurement records, including flagged runs, are in
the [measurement archive](artifacts/rfc-026-v8-coordination-attribution-20261102-measurements.jsonl).
Only bulky per-sync observations are omitted there; their counts, original
local paths and full-file SHA256 values are recorded. Complete originals,
executables and perf recordings remain under
`target/rfc-026-v8-v7-coordination-20261102/`.

## Complete timed profile

Both samples below pass the aggregate stall filter. Profiling overhead makes
these rates diagnostic; they are not new production performance scores.

| Metric | V7 | V8 |
|---|---:|---:|
| Puts/s | 483,074 | 403,411 |
| p99 ms | 5.143 | 4.474 |
| Process user CPU s | 4.934 | 8.489 |
| Process system CPU s | 1.268 | 2.252 |
| Total process CPU s | 6.202 | 10.742 |
| Voluntary context switches | 69,887 | 151,539 |
| Involuntary context switches | 710 | 724 |
| Worker DATA groups | 5,922 | 6,235 |
| Syncs | 1,516 | 1,563 |
| Syncs / DATA group | 0.256 | 0.251 |
| Eventfd notification requests | 5,922 | 6,235 |
| Actual eventfd writes | 1,800 | 4,262 |
| fdatasync total ms | 531.9 | 722.7 |
| FRONTIER write total ms | 124.9 | 121.9 |

V8 uses 73% more total CPU in this pair and makes 2.17 times as many voluntary
context switches. The DATA group and sync counts are close; neither a large
increase in sync count nor FRONTIER write time explains the CPU difference.
Eventfd write coalescing differs despite similar notification-request counts.
These observations identify costs; they do not prove which scheduling change
would improve throughput or tail latency.

**Counter interpretation matters:** V7 `commit_groups` counts DATA worker
groups; V8 suppresses that worker counter and records one commit group per
FRONTIER/sync cycle. Dividing V8 `sync_count` by `commit_groups` therefore gives
one by construction. The table uses instrumented successful worker submission
counts, corroborated by completion publication, for DATA groups. The existing
counter semantics have not been changed by this report.

The [full per-thread data](artifacts/rfc-026-v8-coordination-attribution-20261102-thread-phases.json)
and [scaled phase estimates](artifacts/rfc-026-v8-coordination-attribution-20261102-phases.json)
support the following approximate attribution:

| Phase | V7 CPU s | V8 CPU s | V8 calls |
|---|---:|---:|---:|
| Memtable publication | 1.837 | 3.160 | 11,728 |
| MVCC publication, including spin/park | 0.190 | 1.309 | 11,728 |
| Canonical preflight | 0.133 | 0.155 | 11,728 |
| V8 encoding, entry CRC and inline BLAKE3 | not separately measured | 2.089 | 11,728 |
| Frame materialization/readiness checks | 0.767 | 0.404 | 11,728 |
| V7 ordered logical SHA | 0.838 | — | — |
| DirectBuf allocation | 0.096 | 0.141 | 13,291 |
| Coordinator mutex acquisition | not instrumented | 0.607 | 441,380 |
| FRONTIER-owner mutex acquisition | not applicable | 0.318 | 45,410 |
| Coordinator change wait | not applicable | 0.245 | 33,621 |
| Submission mutex acquisition | not instrumented | 0.028 | 24,442 |
| DATA publish, including packing | not separately measured | 0.146 | 11,728 |
| Worker CQE processing | 0.040 | 0.067 | 23,753 |
| Completion-channel receive | not instrumented | 0.119 | 6,235 |
| Completion publication | not separately measured | 0.056 | 6,235 |
| Resource and guard retirement | not separately measured | 0.035 | 1,563 each |

V7 frame scopes include producer framing and packer checks of already-framed
batches. Its canonical encode was not isolated separately from the producer
scope, so that row must not be treated as zero V7 encoding cost.

The ordered V8 link remains small: the built-in digest sample extrapolates to
about 13 ms, versus about 0.93 s for payload BLAKE3. Actual payload-hash bytes
are 787,500,000 and post-encode hash reread bytes are zero. Sampling can change
with core placement; these extrapolations are approximate. The RFC digest
formula is not the next optimization target.

User-CPU samples independently put memtable skip-list insert/search and MVCC
publication among the largest costs. See the
[V8 symbols](artifacts/rfc-026-v8-coordination-attribution-20261102-complete-00-v8-16-symbols.txt)
and [V7 symbols](artifacts/rfc-026-v8-coordination-attribution-20261102-complete-01-v7-16-symbols.txt).
The higher cost of shared publication code is consistent with different
concurrent execution and cache/allocator contention; the profile does not
establish that causal chain by itself.

Allocation counts are almost equal: V8 makes 10,037,611 Rust allocations and
V7 10,012,722, a difference of 0.25%. Requested Rust bytes are 3.891 versus
3.824 GB. Native aligned allocation/free counts are 13,291 versus 13,244.
These totals include allocations retained by the memtable. They do not support
an explanation based on a large increase in allocation count.

On this successful path, ownership rules imply 11,728 inserts and removals
each for the pending-DATA, producer-guard and resource-reservation maps,
plus one FRONTIER-offset insert/removal per sync. Those are derived operation
counts, not independently timed BTree operations. Variable `get`/`contains`
calls and cache misses were not counted separately. Map-containing phase
timings and sampled symbols do not establish a dominant tree-lookup cost.

## Control cohort without attribution hooks

The same executable supports both formats. This cohort keeps detailed WAL
diagnostics enabled, removes the temporary scope/allocator hooks, and uses
timed `perf stat` without stack sampling. All four runs are eligible.

| Format | Best puts/s | Same-run p99 ms | Same-run total CPU s | Other run puts/s |
|---|---:|---:|---:|---:|
| V7 | 541,791 | 4.419 | 5.141 | 480,981 |
| V8 | 539,131 | 4.181 | 8.434 | 404,116 |

Using the requested best-score rule, V8 is 0.5% behind V7 in this small cohort
and has lower p99, but uses 64% more CPU. The wide within-format variation
prevents a parity or adoption claim. These diagnostic-enabled control peaks
must not be substituted for the earlier normal-counter V4 qualification data.
`perf stat` software context-switch/migration events were unavailable under
the effective user-only event restriction and returned zero; no conclusions
use those zeros. Context-switch conclusions use `getrusage` above.

## Atomic status prototype: not retained

The [prototype](artifacts/rfc-026-v8-coordination-attribution-20261102-status-mirrors.patch)
uses acquire reads of poison, durable-ticket and generation mirrors. Release
stores occur at their existing locked transitions. Admission, authoritative
readiness checks, poison decisions and condition-variable predicates retain
the state mutex. It neither changes the queue nor adds a coalescing policy.

The screen reverses execution order and uses normal runtime counters without
scope hooks or detailed sync diagnostics. Each arm has two samples at each
writer count. At eight writers one sample per arm is stall-flagged; both
sixteen-writer samples per arm are eligible.

| Writers | Baseline puts/s | Baseline p99 ms | Candidate puts/s | Candidate p99 ms | Throughput | p99 |
|---|---:|---:|---:|---:|---:|---:|
| 8 | 320,101 | 3.591 | 312,944 | 3.352 | -2.2% | -6.7% |
| 16 | 440,931 | 4.123 | 420,705 | 4.645 | -4.6% | +12.7% |

Eighty-two V8 unit, concurrency, recovery and crash-boundary tests passed
outside the sandbox before this screen. Passing correctness checks does not
justify retaining a candidate that fails the throughput/p99 gate. The
prototype has been reverted and no further parameter sweep was run.

## Reproduction and next decision

The [temporary Linux-only attribution patch](artifacts/rfc-026-v8-coordination-attribution-20261102-attribution.patch)
applies to `eef5de99`, before the status prototype. Build it with
`cargo build --release --package kv-engine --features bench --bin write-perf`.
Set `TOYKV_WAL_ATTRIBUTION` to a JSON output path to enable its scopes. The
[initial recording script](artifacts/rfc-026-v8-coordination-attribution-20261102-run.py),
[complete-thread follow-up](artifacts/rfc-026-v8-coordination-attribution-20261102-run-complete.py),
[normal-counter follow-up](artifacts/rfc-026-v8-coordination-attribution-20261102-run-normal-attribution.py),
[control script](artifacts/rfc-026-v8-coordination-attribution-20261102-run-controls.py)
and [prototype screen](artifacts/rfc-026-v8-coordination-attribution-20261102-run-mirrors.py)
preserve command lines. Their executable/artifact paths refer to the original
local target directories and must be adjusted when reproducing elsewhere.

A full ring rewrite is not supported yet. A more targeted next investigation
would separate durability-progress notifications from preparation/readiness
notifications, then determine whether reducing producer wakeups also reduces
MVCC spinning and concurrent memtable contention. That is a new experiment,
not a retained change or a measured win in this report. Earlier failed copy
and publication screens are documented
[separately](rfc-026-v8-copy-publication-20261102.md).
