# RFC 026 passive NVMe regimes, 2026-11-03

The passive capture and timestamp join work. All eight full captures have zero
lost events, no command-matching anomalies, and all four required tracepoints.
However, this cohort supplies **zero classified metrics intervals** and therefore
no valid V7/V8 performance ratio. The calibrated first-write signal is obscured
by filesystem traffic in V8 and by uncalibrated bulk/overlapping writes in batch64.
This is measurement progress, not a performance qualification.

The [method](passive-wal-trace-method.md),
[capture runner](capture_wal_trace.py), and
[analyzer](wal_trace_regimes.py) are reusable. Only the `write-perf` benchmark gets
an opt-in timeline hook. No engine/WAL runtime, digest, wire format, grouping,
fsync policy or affinity policy changed. `trace-cmd` and `bpftrace` were absent;
installed `perf` provided the needed events, so no packages were installed.

## Environment and protocol

Same local ext4 filesystem on the Solidigm P41 Plus 1 TB,
`SOLIDIGM SSDPFKNU010TZ`, firmware `004C`; i9-13900T, whole-process CPUs 0–15.
Kernel 6.12.71-1-lts, perf 6.19. Two smoke captures verified the memfd handoff,
non-root benchmark execution, event formats, monotonic clock and operation counts.

The full schedule declared V7 → V8 → V8 → V7 for point writes and then for
16-writer batch64. The point case uses the batch API with batch size 1. Every
capture used one executable, 1 KiB values, Parallel PITR, no compaction and a
2 GiB SST target. All trace/timeline/stdout/stderr data stayed in RAM until the
entire eight-run cohort stopped. No endpoint probe or device-setting change ran.
The binary SHA256 is
`da000b99a0d3b63039976a1b6468dad26abce297a7def2a035a04a55cff62c44`.

The classifier policy was saved before capture: isolated ordinary 4 KiB writes,
16-sample median, five confirmations, fast <100 µs, slow >400 µs, 250 ms maximum
gap, 20 ms guards, and at least 250 ms per classified metrics interval. The
application sampler interval was one second. Classification was not loosened
after seeing the results. The archived analyzer snapshot matches the hash in
that plan; the current analyzer adds a cohort export command and LF CSV output with identical gates.

## Observations inside the application windows

| Run | Matched NVMe commands, whole capture | Eligible first-write observations in application window | Confirmed slow time | Classified metrics intervals |
|---|---:|---:|---:|---:|
| Point V7, first | 61,964 | 19,672 | 13.342 s | 0 / 26 |
| Point V8, first | 100,489 | 0 | 0 | 0 / 38 |
| Point V8, reverse | 100,754 | 0 | 0 | 0 / 17 |
| Point V7, reverse | 61,900 | 19,677 | 13.997 s | 0 / 26 |
| Batch64 V7, first | 18,956 | 0 | 0 | 0 / 8 |
| Batch64 V8, first | 17,968 | 0 | 0 | 0 / 9 |
| Batch64 V8, reverse | 17,989 | 0 | 0 | 0 / 8 |
| Batch64 V7, reverse | 18,239 | 0 | 0 | 0 / 2 |

V7 point's eligible first-write medians were 582.250 and 584.448 µs. It produced
156 and 157 short slow segments, interrupted by unsuitable observations.
No full counter interval fit within a single guarded segment. The exporter
retains those segments in `storage-regimes.csv`; it does not turn their combined
duration into a full-run slow score.

V8 point's first post-FLUSH namespace writes were almost entirely from other
processes and used FUA. Block events also show concurrent `jbd2` writes and
metadata-marked writes, consistent with journal ordering; exact file identity
remains an inference. This prevents treating the first namespace write as an ordinary WAL DATA write. It
also prevents substituting a later, faster application write. The trace does
not by itself establish why the two runtimes generate different journal traffic;
that needs a separate file/extent or filesystem attribution experiment.

Batch64 had larger writes and overlapping namespace I/O, with many first writes
also coming from the journal. None met the existing calibrated signal inside
the application windows. The raw results include a large whole-run throughput
change between the first and reverse V7 capture, but no state label supports a
causal performance comparison. All raw runs remain preserved without a ratio.

## Verification and artifacts

The benchmark binary's 158 tests passed with all features. Clippy passed with
warnings denied; the binary also checked with default features disabled. The
29 trace/matching/regime/join/CSV tests and 40 existing paired-runner tests passed.
Tests cover ID reuse/collisions, raw layout and clock, lost/truncated samples,
partial overlap, wrong issuer/size/control, medians and hysteresis, unobserved
gaps, crossing requests/counter reads, operation conservation, and percentile
sample counts. Formatting, spelling and artifact hashes passed. All eight complete analysis
outputs replayed identically from the compressed raw traces and timelines.

Artifacts use prefix `rfc-026-v8-passive-regimes-20261103-`:

- [Predeclared plan](artifacts/rfc-026-v8-passive-regimes-20261103-plan.json),
  [kernel event formats](artifacts/rfc-026-v8-passive-regimes-20261103-formats.json),
  [summary](artifacts/rfc-026-v8-passive-regimes-20261103-summary.json).
- [Storage regimes](artifacts/rfc-026-v8-passive-regimes-20261103-storage-regimes.csv),
  [interval metrics](artifacts/rfc-026-v8-passive-regimes-20261103-interval-metrics.csv),
  [hash manifest](artifacts/rfc-026-v8-passive-regimes-20261103-hashes.json).
- All eight `.perf.gz`, `-timeline.json.gz`, `-analysis.json.gz`, `-benchmark.json`
  and `-capture.json` files, plus the capture runner and analyzer snapshots.
  Decompress each run's perf/timeline before using the analyzer. Perf bytes are
  retained rather than only rendered text; every rejected observation is retained.

The result is useful passive evidence, but the general bulk-I/O state detector
is not solved. Further qualification requires a predeclared calibration that
handles actual bulk/parallel requests and journal ordering, or another independent
state signal. No performance claim is made from the current unknown intervals.
