# RFC 026 V8 admission-visible frontier-join screen (2026-11-01)

This historical screen predates the 2026-11-03 maintainer decision to make V8
the default for new PITR WALs. Statements below about V7 being the default
describe the state and recommendation at the time of this experiment. The
adoption decision left performance qualification open; see the
[V8 adoption note](../rfc-026-v8-default-20261103.md).

## Result

The admission-visible 10µs join is not retained. It waits when a ticket after
the current frontier-ready prefix has been admitted, including while that
ticket is still digesting, framing, or writing. This exposes a stronger join
signal than the previously screened frame-ready predicate, but the candidate
and its adjacent zero-wait control both ran during severe ext4 sync stalls.
The screen cannot score throughput or p99 for this policy.

A separate same-checkout 8-writer V8/V7 pair ran without the same stall
signature. V8 Parallel measured 330,674 ops/s, p99 2.698ms and 3,188 syncs;
V7 Parallel measured 498,401 ops/s, p99 1.507ms and 1,515 syncs. This is one
directional pair, not qualification evidence, but it confirms that V8's sync
grouping remains a significant performance question. The recommendation at the
time of this screen was to keep V7 as the default and V8 creation disabled.
The later adoption decision selected V8 for new PITR WALs despite the
unqualified performance matrix.

## Method

The release `write-perf` binary with `--features bench` ran on
`/dev/nvme0n1p3` (ext4) with 8 writers, 750,000 operations, batch size 64,
1 KiB values, no compaction, and a 2 GiB target SST size. Latency was sampled
every 10 operations. The V8/V7 pair used the same checkout and differed only
in `--pitr-format`. The admission-join candidate changed only the V8 Parallel
frontier wait predicate and used the same 10µs maximum as the earlier
frame-ready join screen.

Command shape:

```bash
write-perf --bench wal_batch_concurrent --wal-batch-size 64 \
  --pitr --pitr-format v8 --wal-io-mode parallel --num 750000 \
  --threads 8 --value-size 1024 --target-sst-size 2147483648 \
  --compaction none --output json --latency-sample-every 10
```

## Same-checkout V8/V7 pair

| Format | Run ID | Throughput (ops/s) | p50 (ms) | p99 (ms) | Max (ms) | Syncs | `fdatasync` (s) | Measurement (s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| V8 Parallel | `1793506830252` | 330,674 | 1.312 | 2.698 | 44.365 | 3,188 | 1.820 | 2.268 |
| V7 Parallel | `1793506838341` | 498,401 | 0.946 | 1.507 | 2.085 | 1,515 | 0.481 | 1.505 |

V8 was 33.7% lower in throughput and 79.0% higher in p99 in this pair. The
V8 sample had a 44.4ms maximum outlier, so this comparison is directional;
repeatable ext4 cohorts are still required. The sync-count difference suggests
that V8 frontier group formation needs further investigation. It does not by
itself prove that sync count is the only cause of the throughput gap.

## Admission-join candidate and controls

| Policy | Writers | Run ID | Throughput (ops/s) | p99 (ms) | Max (ms) | Syncs | Solo groups | `fdatasync` (s) | Measurement (s) | Classification |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 10µs admission join | 8 | `1793507052457` | 33,539 | 35.315 | 53.417 | 3,013 | 924 | 21.666 | 22.362 | Stall-dominated |
| Zero wait | 8 | `1793507149223` | 24,406 | 59.320 | 120.122 | 2,986 | 1,481 | 29.914 | 30.730 | Stall-dominated |

Both runs spent nearly all of their measurement interval in aggregate
`fdatasync` time and had tens-of-milliseconds p99 latency. Their throughput,
latency, and sync-time differences are not scored. The candidate had 1,868
join waits totaling 23.365ms and 27 more syncs than the later stalled
control; its lower solo-group count did not produce an interpretable result
under this device condition. The candidate run occurred before the control,
so the two runs do not establish a paired policy effect.

## Verification and decision

The candidate passed the focused V8 suite (135 tests) and Clippy. An
independent review found no correctness or progress blocker; it noted that the
wait remains bounded even when an admitted ticket has not started I/O. No
clean paired performance result was obtained, so the candidate code was not
retained. The recommendation at the time was to isolate the sync-grouping gap
with repeatable ext4 runs while preserving the then-current V7 default.
