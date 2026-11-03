# RFC 026: V7/V8 P-core affinity comparison, 2026-11-02

Status: diagnostic experiment; production runtime and affinity defaults unchanged.
The retained notification separation, single durability driver and published
cursor remain in place. The source still matches `b8da402a`; `efa90f11` added
the preceding attribution report.

Best eligible V8 batch64 throughput rises from **514,879** to
**650,192 puts/s** with whole-process P-core affinity
(+26.3%), while same-run p99 and process CPU improve.
The V8 affinity result is promising, but the four-way V7/V8 comparison remains inconclusive: at least one V7 control cell has no eligible run. This experiment does not establish V8/V7 parity or a format-specific affinity advantage.

## Normal-counter comparison

Each row selects the highest eligible throughput for that format/mask using the
same retained normal-counter executable. p99, CPU and supporting counters come
from that same run. Diagnostic scores never enter this ranking.

| Format | Affinity | Eligible / runs | Best puts/s | Same-run p99 (ms) | Same-run CPU (s) | Selected run |
|---|---|---:|---:|---:|---:|---|
| V7 | default 0–31 | 0/10 | — | — | — | no eligible score |
| V7 | P cores 0–15 | 1/10 | 636,928 | 3.800 | 4.153 | `v7-rerun-02-normal-v7-pcore` |
| V8 | default 0–31 | 2/6 | 514,879 | 4.829 | 8.165 | `01-normal-v8-hybrid` |
| V8 | P cores 0–15 | 2/6 | 650,192 | 3.335 | 5.854 | `02-normal-v8-pcore` |

With P-core affinity, the best V8 score is +2.1% versus V7 throughput, -12.2% in p99, and +41.0% in process CPU. The remaining CPU gap is material even when throughput is close. These best scores come from different cohorts; they are not a matched pair.

V8 eligible throughput medians are
493,459 puts/s (default) and
644,307 puts/s (P cores).
There are only two eligible observations per V8 cell. Both clean P-core runs
are in the initial cohort; the longer-cooldown confirmation has no clean P-core
V8 run. This limits repeatability claims.

The best V8 default/P-core scores are adjacent initial runs `01` and `02`.
Their supporting counters are:

| Metric | Default | P cores | Change |
|---|---:|---:|---:|
| Throughput (puts/s) | 514,879 | 650,192 | +26.3% |
| p99 (ms) | 4.829 | 3.335 | -30.9% |
| Process CPU (s) | 8.165 | 5.854 | -28.3% |
| Estimated payload BLAKE3 CPU (s) | 0.815 | 0.552 | -32.2% |
| Voluntary context switches | 97,586 | 92,398 | -5.3% |
| Involuntary context switches | 1,533 | 585 | -61.8% |
| Syncs | 1,309 | 931 | -28.9% |
| Mean fdatasync (ms) | 0.433 | 0.416 | -3.9% |

The second clean P-core V8 run reaches 638,421 puts/s, p99 3.156ms and CPU
6.203s. The second clean default V8 run reaches 472,040 puts/s, p99 4.830ms and
CPU 9.543s. Every flagged record remains in the raw table and artifact.

## What affinity changes

The i9-13900T kernel exposes P-core logical CPUs `0-15` and E-core CPUs
`16-31`. Default runs inherit `0-31`. P-core runs start the benchmark with
`taskset -c 0-15`; its userspace threads inherit that mask. Kernel I/O work is
not separately pinned. CPU-aware perf samples confirm zero E-core userspace
samples in all four P-core diagnostic recordings.

Whole-process affinity also reduces the available physical cores from 24 to
8 with SMT and changes placement for encoding, publication, grouping and I/O
coordination together. This is not an isolated producer-hash intervention.
The unchanged coalescing policy produces fewer syncs in the best P-core V8 run;
affinity therefore changes pipeline timing as well as hash execution cost.

In the selected V8 pair, process CPU falls by 2.310s while sampled payload
BLAKE3 CPU falls by about 0.263s, roughly 11.4% of that difference. The remaining
saving cannot be attributed to BLAKE3 alone. A bounded preparation stage might
recover only part of the whole-process result and would introduce its own
coordination costs. No such production stage is introduced here.

The previous report's 53.1% E-core BLAKE3 figure is a time-weighted CPU sample
share, not a fraction of batches or bytes. It cannot be used directly as a
job-placement probability to weight P/E microbench times or predict a 38%
hash saving. The live CPU measurements above provide the observed comparison.

## Separate phase diagnostics

Eight separate captures use the preserved attribution executable, whole-batch
phase clocks sampled once per 64 calls per thread, and timed-only perf recording.
Only 1/8 passes the storage filter;
all P-core diagnostics are flagged. These figures are diagnostic observations,
not qualified performance comparisons or proof of a publication speedup.

All times below are estimated thread CPU seconds except process CPU, which is
the measured timed process CPU. Encoding contains allocation and entry encoding;
for V8 it also contains payload hashing. **Do not add hash to V8 encoding.** V7
ordered SHA is outside its encoding/frame scopes. Do not add nested child scopes
or extrapolate stalled publication costs to clean runs.

| Capture | Storage filter | Encode parent | Frames + CRC | Payload hash (V7 SHA / V8 BLAKE3) | Memtable publication | MVCC publication | Process CPU |
|---|---|---:|---:|---:|---:|---:|---:|
| `16-diagnostic-v7-hybrid` | flagged | 0.659 | 0.694 | 0.921 | 1.726 | 0.532 | 6.902 |
| `17-diagnostic-v8-hybrid` | eligible | 2.252 | 0.440 | 0.998 | 2.996 | 0.382 | 9.471 |
| `18-diagnostic-v8-pcore` | flagged | 1.654 | 0.332 | 0.653 | 2.437 | 0.535 | 7.062 |
| `19-diagnostic-v7-pcore` | flagged | 0.747 | 0.914 | 1.391 | 2.774 | 0.326 | 9.444 |
| `20-diagnostic-v7-pcore` | flagged | 0.768 | 0.937 | 1.343 | 2.766 | 0.302 | 9.267 |
| `21-diagnostic-v8-pcore` | flagged | 2.071 | 0.404 | 0.882 | 3.035 | 1.018 | 10.347 |
| `22-diagnostic-v8-hybrid` | flagged | 2.356 | 0.510 | 0.989 | 2.702 | 1.098 | 11.355 |
| `23-diagnostic-v7-hybrid` | flagged | 0.926 | 0.918 | 1.153 | 2.177 | 0.728 | 9.906 |

All V8 runs report exactly 787,500,000 payload hash bytes and zero
`post_encode_hash_reread_bytes`. Hash CPU estimates scale the existing 184
sampled ticket timings to the exact 11,728 logical batches. No digest formula,
CRC implementation, allocator, DATA grouping, sync policy or wire bytes changed.

The CPU-aware leaf samples show the placement intervention directly:

| Capture | Hash leaf category | P samples | E samples | E share |
|---|---|---:|---:|---:|
| `16-diagnostic-v7-hybrid` | ordered-packer SHA256 leaf | 419 | 38 | 8.3% |
| `17-diagnostic-v8-hybrid` | BLAKE3 bulk leaf | 135 | 220 | 62.0% |
| `18-diagnostic-v8-pcore` | BLAKE3 bulk leaf | 254 | 0 | 0.0% |
| `19-diagnostic-v7-pcore` | ordered-packer SHA256 leaf | 675 | 0 | 0.0% |
| `20-diagnostic-v7-pcore` | ordered-packer SHA256 leaf | 651 | 0 | 0.0% |
| `21-diagnostic-v8-pcore` | BLAKE3 bulk leaf | 297 | 0 | 0.0% |
| `22-diagnostic-v8-hybrid` | BLAKE3 bulk leaf | 164 | 192 | 53.9% |
| `23-diagnostic-v7-hybrid` | ordered-packer SHA256 leaf | 453 | 122 | 21.2% |

V7's SHA subset is restricted to thread IDs that actually execute the measured
ordered logical-prefix SHA scope; it does not combine the physical WAL digest
thread. Leaf shares describe sampled CPU time, not operations or elapsed time.

## Method and storage qualification

All captures run natively outside the sandbox on the same ext4 filesystem
(`/dev/nvme0n1p3`). Workload: 750,000 puts, 16 writers, batch64, 1KiB values,
Parallel WAL, no compaction, 2GiB SST target, latency sampling every 10 batches.
PITR maintenance remains outside the timed interval.

The initial plan fixes four balanced normal rounds, one capture per cell per
round, then two reversed diagnostic rounds. Each round starts after 45s idle;
captures are separated by 3s. Its 16 normal runs yielded no eligible V7 control.
A separate fixed confirmation adds two reversed normal rounds with 120s idle
per round and 10s gaps, retaining separate cohort labels.

The maintainer then explicitly requested a V7 rerun. A new fixed plan adds
eight V7-only normal captures, four per mask, with reversed orders, 120s idle
per round and 10s gaps. Rerun outcome: **hybrid: 0/4 eligible; pcore: 1/4 eligible**.
Earlier captures and V8 scores remain unchanged.

The predeclared storage flag is `p99 > 10ms OR mean fdatasync > 1ms`. It is
identical for both formats and affinities. This produces
27/32 flagged normal captures and
7/8 flagged diagnostics,
34/40 overall. No flagged score is
promoted because it is fast, and no record is silently replaced. Missing
controls do not establish that the missing format is slower.

No build, test or other benchmark from this task runs concurrently with a
capture. Normal captures use the existing context-switch probe and normal
benchmark counters, without `--profile`, new phase scopes or perf. Diagnostic
captures use `cpu-clock:u` at 499Hz, DWARF4096 and `--sample-cpu`, enabled only
inside the timed interval through the existing perf control FIFO. Perf writes
to a memory file until each timed interval has ended. Offline stack export
finishes before the confirmation cooldown begins.

## All normal captures

| Capture | Puts/s | p99 (ms) | Process CPU (s) | Mean sync (ms) | Storage filter |
|---|---:|---:|---:|---:|---|
| `00-normal-v7-hybrid` | 97,737 | 31.600 | 9.503 | 1.182 | flagged |
| `01-normal-v8-hybrid` | 514,879 | 4.829 | 8.165 | 0.433 | eligible |
| `02-normal-v8-pcore` | 650,192 | 3.335 | 5.854 | 0.416 | eligible |
| `03-normal-v7-pcore` | 119,716 | 27.627 | 8.014 | 1.858 | flagged |
| `04-normal-v8-hybrid` | 80,740 | 29.126 | 11.361 | 2.055 | flagged |
| `05-normal-v7-pcore` | 86,366 | 28.475 | 9.206 | 2.796 | flagged |
| `06-normal-v7-hybrid` | 100,961 | 39.333 | 9.114 | 1.311 | flagged |
| `07-normal-v8-pcore` | 638,421 | 3.156 | 6.203 | 0.414 | eligible |
| `08-normal-v7-pcore` | 94,953 | 30.376 | 9.082 | 2.617 | flagged |
| `09-normal-v8-pcore` | 83,623 | 30.217 | 10.796 | 2.579 | flagged |
| `10-normal-v8-hybrid` | 79,345 | 29.602 | 11.414 | 2.045 | flagged |
| `11-normal-v7-hybrid` | 135,525 | 28.484 | 8.064 | 0.868 | flagged |
| `12-normal-v8-pcore` | 290,356 | 27.547 | 7.112 | 1.090 | flagged |
| `13-normal-v7-hybrid` | 86,564 | 29.315 | 10.102 | 1.406 | flagged |
| `14-normal-v7-pcore` | 90,677 | 27.553 | 9.324 | 2.634 | flagged |
| `15-normal-v8-hybrid` | 77,677 | 29.305 | 11.403 | 2.136 | flagged |
| `confirm-00-normal-v7-hybrid` | 101,913 | 31.955 | 9.862 | 1.249 | flagged |
| `confirm-01-normal-v7-pcore` | 112,157 | 28.511 | 9.104 | 1.864 | flagged |
| `confirm-02-normal-v8-pcore` | 102,147 | 29.276 | 10.167 | 2.674 | flagged |
| `confirm-03-normal-v8-hybrid` | 105,061 | 30.903 | 10.890 | 2.282 | flagged |
| `confirm-04-normal-v8-hybrid` | 472,040 | 4.830 | 9.543 | 0.478 | eligible |
| `confirm-05-normal-v8-pcore` | 131,505 | 31.918 | 9.541 | 2.166 | flagged |
| `confirm-06-normal-v7-pcore` | 108,504 | 28.972 | 8.683 | 1.911 | flagged |
| `confirm-07-normal-v7-hybrid` | 105,683 | 29.255 | 10.056 | 1.146 | flagged |
| `v7-rerun-00-normal-v7-hybrid` | 99,701 | 30.179 | 10.110 | 1.178 | flagged |
| `v7-rerun-01-normal-v7-pcore` | 117,429 | 30.801 | 8.628 | 1.745 | flagged |
| `v7-rerun-02-normal-v7-pcore` | 636,928 | 3.800 | 4.153 | 0.357 | eligible |
| `v7-rerun-03-normal-v7-hybrid` | 116,354 | 29.345 | 9.520 | 1.064 | flagged |
| `v7-rerun-04-normal-v7-pcore` | 103,493 | 31.842 | 9.330 | 1.892 | flagged |
| `v7-rerun-05-normal-v7-hybrid` | 104,391 | 31.993 | 10.050 | 1.298 | flagged |
| `v7-rerun-06-normal-v7-hybrid` | 75,775 | 164.456 | 10.451 | 1.349 | flagged |
| `v7-rerun-07-normal-v7-pcore` | 103,768 | 72.770 | 8.265 | 2.119 | flagged |

## Reproduction and checks

The normal executable SHA256 is
`937a3843a044e107e15db756674be151875e388ff3a0a392cbd4e97463bbd2aa`.
The separate diagnostic executable SHA256 is
`fbb90ea8a6f20bf476760e79ae60e04cda3edae04138d45ae5a9e6346da299f1`.
Both are preserved from the preceding experiments; no recompilation was needed.
The preceding [producer attribution report](rfc-026-v8-producer-attribution-20261102.md)
contains the diagnostic source patch and instrumentation limitations.

The [initial plan](artifacts/rfc-026-v8-pcore-affinity-20261102-plan.json),
[confirmation plan](artifacts/rfc-026-v8-pcore-affinity-20261102-confirmation-plan.json),
[user-requested V7 rerun plan](artifacts/rfc-026-v8-pcore-affinity-20261102-v7-rerun-plan.json),
[summary](artifacts/rfc-026-v8-pcore-affinity-20261102-summary.json),
[complete measurements including raw JSON and phase records](artifacts/rfc-026-v8-pcore-affinity-20261102-measurements.jsonl),
[CPU sample summary](artifacts/rfc-026-v8-pcore-affinity-20261102-cpu-sample-summary.json),
and [checks](artifacts/rfc-026-v8-pcore-affinity-20261102-checks.json) preserve the full result.
The [initial runner](artifacts/rfc-026-v8-pcore-affinity-20261102-run.py),
[confirmation runner](artifacts/rfc-026-v8-pcore-affinity-20261102-confirm.py),
[V7 rerun runner](artifacts/rfc-026-v8-pcore-affinity-20261102-v7-rerun.py),
[analysis](artifacts/rfc-026-v8-pcore-affinity-20261102-analyze.py), [report generator](artifacts/rfc-026-v8-pcore-affinity-20261102-write-report.py)
and [archive script](artifacts/rfc-026-v8-pcore-affinity-20261102-archive.py)
use paths under `target/rfc-026-v8-pcore-affinity-20261102/`, which need adjustment on another machine.
Original perf recordings, stacks, normal JSON, phase files and logs remain there.

All 40 commands exit successfully. Every raw-record checksum, operation count,
V8 hash-byte count and zero-reread assertion passes. All 16 perf exports succeed.
The original source checksums and empty runtime Git diff confirm no Rust change;
only this report, its artifacts and the documentation index are added. No
production affinity default or performance qualification gate changes.
