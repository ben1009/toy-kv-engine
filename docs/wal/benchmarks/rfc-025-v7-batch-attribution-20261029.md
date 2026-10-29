# RFC 025: where the v4/v7 batch gap is spent

**The missing work includes CPU-heavy DATA framing and rolling SHA-256 on the
ordered v7 packer, before DATA submission.** The existing WAL durability-wait
counter includes that CPU work; it must not be interpreted as SSD wait alone.
FRONTIER write time explains only part of the solo batch gap. No runtime change
is retained in this diagnostic slice.

Fresh scored comparisons use the same retained executable for v4 and v7. The
best solo batch64 scores are 85,618 versus 71,166 puts/s, a 16.9% v7 gap.
Including previous scored v7 runs of this exact executable gives a v7 best of
72,647 puts/s, narrowing that gap to 15.2%. Every p99 below belongs to the same
invocation as its displayed throughput; medians do not rank the results.

| Best-score view | Writers | V4 puts/s | V7 puts/s | V7 change | V4 p99 ms | V7 p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fresh cohort | 1 | 85,618 | 71,166 | -16.9% | 1.195 | 1.264 |
| Matching-binary history plus fresh | 1 | 85,618 | 72,647 | -15.2% | 1.195 | 1.373 |
| Fresh cohort | 8 | 388,372 | 405,202 | +4.3% | 2.511 | 7.836 |
| Matching-binary history plus fresh | 8 | 388,372 | 478,798 | +23.3% | 2.511 | 2.653 |

The eight-writer results do not establish a uniform v7 throughput penalty or
parity: throughput and tail behavior differ, storage conditions vary, and
historical sample counts are unequal. The frozen best-score input index contains
66 scored records across these two workloads. All original scores, warmups,
individual stall flags and raw maxima remain available in the
[best-score analysis](artifacts/rfc-025-v7-batch-attribution-20261029-best.json).

The scored solo winners make the distinction between bytes, sync and CPU clear:

| Metric for 750,000 puts | V4 | V7 |
| --- | ---: | ---: |
| Elapsed time | 8.760 s | 10.539 s |
| Process CPU | 4.448 s | 6.450 s |
| DATA bytes | 816,001,024 | 816,001,024 |
| Sync count | 11,719 | 11,719 |
| Measured fdatasync time | 3.744 s | 3.594 s |
| FRONTIER write time | 0 | 0.305 s |
| Caller WAL durability wait | 5.747 s | 7.774 s |
| Memtable insertion time | 2.074 s | 1.158 s |

V7 has about 2.00 seconds more process CPU and 1.78 seconds more elapsed time,
despite slightly less measured sync time. The marker writes alone take about
0.305 seconds. These counters identify a substantial CPU contribution, without
assigning the remaining wall-time difference to a particular syscall.

Six separate, excluded diagnostic captures add CLOCK_MONOTONIC and
CLOCK_THREAD_CPUTIME_ID events. All **70,317 batch API calls** map to a wait
ticket, packer group, DATA submission/reap and covering sync/publication cycle.
Each call's stages are nonnegative and sum exactly to its API duration. Native
FRONTIER and sync intervals independently fit inside the Rust event intervals.
No event is dropped. The two standard solo traces report:

| Measured component | V4 CPU | V7 CPU | Scope |
| --- | ---: | ---: | --- |
| Caller before WAL wait | 0.739 s | 1.443 s | Includes preparation/admission; components below can be nested |
| Ordered packer work | 0.024 s | 2.051 s | Group preparation through ready-to-submit |
| DATA framing/checksumming | — | 0.850 s | Subset of v7 packer work |
| Logical-prefix SHA-256 update | — | 1.140 s | Subset of v7 packer work |
| Canonical entry preparation | — | 0.718 s | Subset of caller preparation |
| Building owned PITR entries | — | 0.195 s | Subset of caller preparation |

The standard solo v7 packer consumes approximately 2.062 seconds of wall time
and 2.051 seconds of thread CPU: that stage is almost entirely CPU work in this
capture. Framing and rolling SHA-256 account for about 97% of its measured CPU.
V4 already prepares its own encoded batch before admission; the caller and
packer rows must be considered together. Nested rows must not be added again.

The current ordering in [the packer](../../../kv-engine/src/wal/parallel/runtime.rs)
is:

```text
v7 canonical entries / owned logical bytes
  -> construct DATA frames and frame CRCs
  -> update the ordered logical SHA-256 prefix
  -> publish the prefix snapshot
  -> submit DATA
  -> DATA completion
  -> FRONTIER
  -> one fdatasync
  -> publish / ACK
```

This places framing and hashing ahead of the same batch's I/O, so that work is
visible in single-writer latency. It also explains why a large WAL wait counter
does not by itself establish a disk bottleneck. The measurements support reducing
or hiding that serial CPU work as a next target, rather than assuming the only
format difference is one extra small write.

Source inspection confirms concrete work: the MVCC PITR path builds owned entry
keys/values; canonical preparation validates entries and builds a sorted point-operation index; DATA
materialization initializes each frame, copies payload and computes its frame
CRC; the packer then hashes the complete logical stream. The encoder-owned path
already avoids rechecking its entry CRC, so removing that check again is not a
new opportunity. The host advertises SHA-NI on all 32 logical CPUs, but these
traces do not record the selected hash backend or CPU frequency. Missing hardware
acceleration is not established.

The extent-initialization hypothesis was corrected by the measurements. A v4
startup flag initially suggested that large v4 writes always use zero-filled
extents. In fact, `take_admitted_group` computes the initialization boolean for
both formats, and v4 passes it to its initializer. Both large-batch paths mostly
skip zero filling. Native preparation writes during the standard solo captures
are 126,976 bytes for v4 and zero for v7. The
[correction record](artifacts/rfc-025-v7-batch-attribution-20261029-extent-hypothesis-correction.json)
preserves the initial inference and the evidence that rejects it.

The two forced-initialization v7 diagnostics use the existing initialization
path without changing framing, async dispatch, FRONTIER or sync rules. Solo
preparation writes rise to 864,018,432 bytes, taking about 4.184 seconds of native
write time. At eight writers, preparation writes rise to 824,172,544 bytes and
DATA completion intervals fall, but sync and FRONTIER latencies also improve
substantially between captures. Those mixed results do not establish an isolated
extent-conversion benefit or justify a retained policy. This intervention does
not reproduce ordinary v4 large-batch behavior.

Diagnostic throughput is excluded from all best-score rankings. In particular,
the standard diagnostic v4 solo sync mean is about 1.60 ms and v7 about 0.54 ms;
their raw throughput cannot quantify a format gap. Trace locking, CPU-clock
syscalls and task sampling also perturb execution. Thread-CPU timings locate
busy work, but their magnitude can depend on CPU placement/frequency and cache
state. They do not assign an exact fraction of the scored 15–17% gap. Concurrent
call stages, nested CPU components and native I/O durations overlap.

The scored stage completes 16 fixed invocations: two warmups and three
alternating pairs at each of one/eight writers. The unmodified executable is
`target/release/write-perf-v7-large-data-async-dispatch`, SHA-256
`f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8`, for both formats.
All use explicit Parallel, batch64, 750,000 puts, 1 KiB values, a 2 GiB SST target,
no compaction, sampling every ten batches and fresh local ext4 databases.
The v4 harness flushes its memtable after the timed work, whereas PITR v7 skips
that flush. Process CPU and traced API paths exclude close/flush, but later storage
conditioning can still differ.
Cooldown is 60 seconds per cell, with 15-second gaps. Best-score selection excludes
individual invocations with mean FRONTIER write >=1 ms or mean fdatasync >=1.5 ms;
raw maxima are preserved. Matching historical scores use the same binary and
configuration, never another optimization's executable.

After scoring, one temporary instrumented build collects v4/v7 at one/eight
writers plus v7 initialization interventions at one/eight writers. All six run
in their frozen order; no replacement captures or new production candidate is
introduced. Before/1 Hz/after host audits find no recognized competing process,
and 100 ms task sampling is used only for diagnostics. Sampling cannot prove
exclusive device access. No build, test, edit or other profile overlaps capture.

Instrumentation validation passes 79 focused tests without retries. The full
check passes 1,973 tests after one automatic timeout retry of
`tests::simple_leveled_compaction::test_integration`; that test passes alone with
retries disabled. Two validation-script gates initially stop on the recorded
retry and then on nextest's singular `1 test run` summary; their drivers/logs are
preserved, and the already-passing isolated test is not repeated. A capture-driver
syntax typo is caught before capture and corrected. See the
[validation notes](artifacts/rfc-025-v7-batch-attribution-20261029-validation-notes.json).
The diagnostic release build succeeds. All five edited Rust files, all 128
baseline inputs and the canonical executable are then restored exactly. A fresh
restored `cargo make check` passes all **1,973 tests without retries or skips**.

Reproduce the score, attribution and final verification:

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-batch-attribution-20261029-best.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-batch-attribution-20261029-trace-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-batch-attribution-20261029-verify.py
```

The verifier reconstructs the five-file instrumentation from `cf655cdd`, checks
all captured files and source/binary hashes, reproduces the score and trace
summaries, verifies native intervals and every API partition, and confirms full
restoration. The 22 new completed invocations bring the lineage to **947** and
the broader archive to **5,043**, plus one inherited interrupted invocation
without JSON. Scratch databases are removed. Detailed traces, path mappings and
logs remain under local ignored `target`; raw benchmark results and summaries
are versioned. No optimization, push or PR is retained in this slice.
