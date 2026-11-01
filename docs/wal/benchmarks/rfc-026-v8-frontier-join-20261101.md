# RFC 026 V8 in-flight successor frontier-join screen (2026-11-01)

## Result

The 40µs join reduced durability syncs and raised throughput in two usable
candidate runs, but p99 latency was consistently slightly worse than the one
usable zero-wait control. A 20µs follow-up did not produce a clean confirming
run. Do not retain the join policy; V8 Parallel returns to the zero-wait
frontier baseline.

The retention rule for this screen is to keep a candidate only when both
throughput and p99 improve over the zero-wait control.

The experiment changed only the coordinator policy: when the contiguous
frontier-ready prefix had an immediate successor whose digest and frames were
ready and whose positional DATA write had started but not completed, the
coordinator could wait for up to 40µs before syncing the current prefix. The
wire format and durability protocol were unchanged.

## Method

The release `write-perf` binary with `--features bench` ran on
`/dev/nvme0n1p3` (ext4) with 16 writers, 750,000 operations, batch size 64,
1 KiB values, PITR V8 Parallel, no compaction, and a 2 GiB target SST size.
Latency was sampled every 10 operations. The zero-wait control used the same
code with the join duration set to zero; the candidate used a 40µs limit. A
20µs candidate was screened separately.

The existing `sync_coalesce_wait` counters do not include this experimental
join, so the screen has no direct total for time spent in the new wait.

Command shape:

```bash
write-perf --bench wal_batch_concurrent --wal-batch-size 64 \
  --pitr --pitr-format v8 --wal-io-mode parallel --num 750000 \
  --threads 16 --value-size 1024 --target-sst-size 2147483648 \
  --compaction none --output json --latency-sample-every 10
```

## Usable 40µs screen

| Policy | Run ID | Throughput (ops/s) | p50 (ms) | p99 (ms) | Max (ms) | Syncs | `fdatasync` (s) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Zero wait | `1793502513599` | 297,160 | 3.242 | 5.662 | 51.743 | 2,429 | 1.732 |
| 40µs | `1793502555475` | 311,553 | 3.207 | 5.901 | 36.115 | 2,259 | 1.637 |
| 40µs | `1793502578120` | 322,421 | 2.963 | 5.838 | 66.583 | 2,276 | 1.570 |

The two candidate runs had median throughput 316,987 ops/s (+6.7%), median
sync count 2,268 (-6.6%), and median `fdatasync` time 1.604s (-7.4%) relative
to the single usable zero-wait run. Candidate median p99 was 5.870ms, 3.7%
higher than the control. It fails the stated retention rule: throughput
improved, but p99 did not.

## Excluded runs

| Policy | Run ID | Throughput (ops/s) | p99 (ms) | `fdatasync` (s) | Reason |
|---|---:|---:|---:|---:|---|
| Zero wait | `1793502611408` | 49,236 | 50.246 | 14.206 | Stall-dominated: 14.2s of sync time in a 15.2s measurement |
| Zero wait | `1793502668227` | 27,588 | 76.368 | 26.112 | Stall-dominated: 26.1s of sync time in a 27.2s measurement |
| 20µs | `1793502762864` | 69,627 | 112.066 | 9.973 | Stall-dominated: 10.0s of sync time in a 10.8s measurement |

There was only one usable zero-wait control, so the 40µs results are
directional. However, both usable 40µs candidate runs had higher p99 than that
control. The 20µs exploratory run `1793502785189` measured 292,229 ops/s,
31.608ms p99, 1.690s in `fdatasync`, and an 83.205ms maximum. It is reported as
an unfavorable exploratory result, not a scored qualification sample: it has
no paired 20µs control, and the preceding 20µs run was stall-dominated. The
in-flight successor signal may reduce sync count, but these data do not
justify adding a latency wait to the durability path.

## Verification

The candidate implementation passed the focused frontier-join tests and all
70 V8 unit tests, including completion wakeup, bounded timeout, poison, close,
and close-time sync of the already-ready prefix. The candidate code was not
retained after the performance screen.
