# RFC 026: foreground writer affinity versus whole-process affinity, 2026-11-02

Status: diagnostic only. The temporary benchmark hook is removed; production
runtime, affinity defaults, digest, wire format and coalescing policy are unchanged.

**Pinning application writers to P-cores does not recover the whole-process
affinity gain in this cohort.** Best eligible writer-only throughput is 475,293
puts/s, essentially equal to default's 475,250. Whole-process affinity reaches
655,686 puts/s. Writer-only still uses 9.147s CPU versus whole-process's 5.853s.
Two complete clean rounds in opposite order support this distinction.

## Best eligible scores

All rows use one new executable, 750,000 puts, 16 writers, batch64, 1KiB values,
V8 Parallel, no compaction and the same ext4 SSD. Each row selects the highest
eligible throughput for its mode; p99, CPU and hash counters come from that run.

| Mode | Writer CPUs | Main/background CPUs | Eligible / runs | Best puts/s | p99 (ms) | Process CPU (s) | Estimated payload BLAKE3 CPU (s) |
|---|---|---|---:|---:|---:|---:|---:|
| Default | 0–31 | 0–31 | 2/6 | 475,250 | 3.935 | 9.082 | 0.971 |
| Writers only | 0–15 | 0–31 | 3/6 | 475,293 | 3.461 | 9.147 | 0.911 |
| Whole process | 0–15 | 0–15 | 3/6 | 655,686 | 3.357 | 5.853 | 0.549 |

Selected records are `05-v8-default`, `01-v8-writers` and `02-v8-whole`.
Writer-only versus default is +0.009% throughput, -12.1% p99 and +0.7% CPU.
Versus whole-process, writer-only is 27.5% lower throughput and 56.3% higher CPU.

Eligible throughput medians are 468,154 / 474,634 / 654,516 puts/s respectively;
CPU medians are 9.377 / 9.147 / 5.853s. Medians are supporting evidence; the
headline comparison follows the maintainer's best-score selection rule.

| Same-run counter | Default best | Writers-only best | Whole-process best |
|---|---:|---:|---:|
| Voluntary context switches | 98,825 | 98,904 | 90,928 |
| Involuntary context switches | 1,376 | 355 | 743 |
| Syncs / embedded FRONTIERs | 1,405 | 1,360 | 928 |
| Mean fdatasync (ms) | 0.452 | 0.444 | 0.413 |

Writer-only already reduces involuntary context switches below whole-process,
yet retains much higher CPU and sync counts. That counter alone cannot explain
the whole-process gain. In V8, `commit_groups` describes durability groups,
not the number of worker DATA submission groups.

## Clean reversed rounds

| Round / execution order | Default puts/s / p99 | Writers-only puts/s / p99 | Whole-process puts/s / p99 |
|---|---:|---:|---:|
| 0: default → writers → whole | 461,058 / 3.667ms | 475,293 / 3.461ms | 655,686 / 3.357ms |
| 1: whole → writers → default | 475,250 / 3.935ms | 474,634 / 4.074ms | 654,516 / 3.234ms |

Every capture in these two rounds passes the predeclared storage filter.
Round 2 also supplies clean writer-only and whole-process captures, but its
default capture is flagged. The remaining rounds are storage-limited; their
records remain below and in the complete artifact.

## What the intervention does and does not isolate

The benchmark changes each application's own writer-thread affinity once at
thread entry, before its first write. It performs no per-batch affinity calls.
Writers execute canonical encoding, CRC, BLAKE3, publication and any submission,
prefix-folding or durability-driver work that V8 executes inline on its caller.
Writer-only is therefore broader than a hash-only experiment.

Each of the 16 writer masks is checked, and all other threads are audited before
and after the timed interval. For writer-only, `wal-io-worker`,
`wal-v8-completi`, the engine background runtime, Tokio runtime workers and visible
kernel `iou-wrk-*` tasks retain `0-31`. Whole-process runs inherit `0-15` for
all these roles. All 18 full captures and all six smoke captures pass these
role assertions. Delayed writer task cleanup, if present, is identified by
recorded writer TIDs and excluded from the background classification.

An allowed mask does not establish where an unrestricted worker actually ran.
The experiment does not identify which background role causes the difference.
Whole-process affinity also changes physical-core availability, SMT contention,
cache sharing, power/frequency behavior and completion/coalescing timing together.

Even payload BLAKE3 CPU is substantially different between writer-only and
whole-process despite the writer masks being identical: the selected estimates
are 0.911s and 0.549s. This is inconsistent with explaining the entire gap as
application hashing simply executing on E-cores. Frequency, contention, cache
and pipeline interactions remain possible explanations; they are not separately
measured here.

The next useful role split includes the WAL I/O worker, completion reaper and
kernel io_uring workers. Pinning only the userspace WAL thread would leave the
kernel worker mask as a separate variable. This result does not justify adding
a BLAKE3 worker pool or choosing a production affinity default.

## Method and validation

The fixed plan contains all six permutations of the three modes, one triplet
per round, six captures per mode. Each round starts after 45s idle, with 3s
between captures. The identical flag is `p99 > 10ms OR mean fdatasync > 1ms`.
Ten of 18 full captures are flagged; none contributes to a score. Eight eligible
full captures remain. Six additional 4,096-put V7/V8 smoke captures validate all
mode/format combinations and are never used as performance scores.

The source modification is confined to `write-perf`: optional affinity setup,
thread-mask audits, returned placement metadata and getrusage context deltas.
The original thread-launch timing remains, including setup at writer entry in
every mode. `/proc` snapshots and audit JSON writes occur outside timed CPU/wall
metrics. There are no new per-batch clocks, perf recording, allocation wrappers
or `--profile`. Existing digest CPU timing still samples once per 64 tickets.

All V8 full captures hash 787,500,000 entry bytes, report zero
`post_encode_hash_reread_bytes`, and complete 750,000 puts. CPU estimates scale
the existing sampled hash timings to the exact 11,728 logical batches. Build,
formatting and smoke validation finish before the scored cohort; no builds,
tests or other benchmark from this task run alongside a scored capture.

The executable SHA256 is
`b422f940a95fef7fa81da96d85e3787e8909e8226fe6d5e62a720fd0d8e1e81d`.
It is compiled once and reused across all modes. The temporary source is
preserved as a patch, then restored exactly before the full cohort starts.
The source baseline is `da7d4d67`, whose runtime remains `b8da402a`.
The new executable is a comparison control for this cohort; earlier executable
scores in the [whole-process affinity report](rfc-026-v8-pcore-affinity-20261102.md)
are context only, not pooled into this ranking or a new V7 comparison.

## Complete full-size cohort

| Capture | Puts/s | p99 (ms) | CPU (s) | Estimated payload hash CPU (s) | Syncs | Storage filter |
|---|---:|---:|---:|---:|---:|---|
| `00-v8-default` | 461,058 | 3.667 | 9.671 | 1.042 | 1349 | eligible |
| `01-v8-writers` | 475,293 | 3.461 | 9.147 | 0.911 | 1360 | eligible |
| `02-v8-whole` | 655,686 | 3.357 | 5.853 | 0.549 | 928 | eligible |
| `03-v8-whole` | 654,516 | 3.234 | 5.689 | 0.553 | 992 | eligible |
| `04-v8-writers` | 474,634 | 4.074 | 8.604 | 0.858 | 1337 | eligible |
| `05-v8-default` | 475,250 | 3.935 | 9.082 | 0.971 | 1405 | eligible |
| `06-v8-writers` | 445,353 | 3.949 | 9.712 | 0.993 | 1382 | eligible |
| `07-v8-whole` | 638,532 | 3.227 | 6.261 | 0.607 | 1010 | eligible |
| `08-v8-default` | 91,756 | 30.615 | 10.957 | 0.996 | 1661 | flagged |
| `09-v8-default` | 80,096 | 33.035 | 11.494 | 1.028 | 1749 | flagged |
| `10-v8-whole` | 83,554 | 30.146 | 10.808 | 0.958 | 1592 | flagged |
| `11-v8-writers` | 127,927 | 32.265 | 9.885 | 0.851 | 1520 | flagged |
| `12-v8-whole` | 289,897 | 19.781 | 7.016 | 0.655 | 1112 | flagged |
| `13-v8-default` | 80,264 | 33.136 | 11.479 | 1.022 | 1730 | flagged |
| `14-v8-writers` | 79,489 | 30.409 | 12.107 | 0.952 | 1757 | flagged |
| `15-v8-writers` | 77,631 | 29.425 | 11.529 | 0.910 | 1752 | flagged |
| `16-v8-default` | 227,807 | 29.447 | 9.805 | 1.019 | 1434 | flagged |
| `17-v8-whole` | 143,887 | 27.267 | 8.258 | 0.750 | 1298 | flagged |

## Artifacts

- [Fixed plan](artifacts/rfc-026-v8-writer-affinity-20261102-plan.json)
- [Summary and eligible medians](artifacts/rfc-026-v8-writer-affinity-20261102-summary.json)
- [All raw measurements, statuses and per-thread audits](artifacts/rfc-026-v8-writer-affinity-20261102-measurements.jsonl)
- [All six smoke measurements and audits](artifacts/rfc-026-v8-writer-affinity-20261102-smoke-measurements.json)
- [Temporary benchmark-only patch](artifacts/rfc-026-v8-writer-affinity-20261102-writer-affinity.patch)
- [Runner and role-mask assertions](artifacts/rfc-026-v8-writer-affinity-20261102-run.py)
- [Analysis and archive assertions](artifacts/rfc-026-v8-writer-affinity-20261102-analyze.py)
- [Restoration record](artifacts/rfc-026-v8-writer-affinity-20261102-restoration.json)
- [Checks](artifacts/rfc-026-v8-writer-affinity-20261102-checks.json)

Scripts refer to `target/rfc-026-v8-writer-affinity-20261102/` and require path
adjustment elsewhere. The preserved executable, full benchmark source snapshots,
original JSON, audit files and logs remain in that local directory. All 24
commands succeed, all role/hash/operation checks pass, and the runtime Git diff
is empty. Formatting, spell-checking and patch-application checks pass. Narrow
spell-check patterns preserve literal Linux thread names without changing raw
audit records. No benchmark affinity hook or runtime optimization is retained.
