# RFC 026 V8 10µs frontier-join screen (2026-11-01)

## Result

Do not retain the conditional 10µs frontier join. Its only clean paired
8-writer run had nearly flat throughput (+0.7%), slightly worse p99 latency
(+2.3%), and only 1.1% fewer syncs. The candidate wait fired frequently, but
the measured result does not meet the retention rule that throughput and p99
must both improve. V8 Parallel returns to the zero-wait frontier baseline.

The candidate waited only when the immediate successor had published its
digest and frames but had not yet published DATA completion. It allowed at
most 10µs per frontier cycle. DATA grouping, digest computation, frontier
format, and sync protocol were unchanged.

## Method

The release `write-perf` binary with `--features bench` ran on
`/dev/nvme0n1p3` (ext4) with 8 writers, 750,000 operations, batch size 64,
1 KiB values, PITR V8 Parallel, no compaction, and a 2 GiB target SST size.
Latency was sampled every 10 operations. The control binary was built from
`9eb1a792` and the candidate binary from the uncommitted frontier-join change.

Command shape:

```bash
write-perf --bench wal_batch_concurrent --wal-batch-size 64 \
  --pitr --pitr-format v8 --wal-io-mode parallel --num 750000 \
  --threads 8 --value-size 1024 --target-sst-size 2147483648 \
  --compaction none --output json --latency-sample-every 10
```

## Usable paired run

| Policy | Run ID | Throughput (ops/s) | p50 (ms) | p99 (ms) | Max (ms) | Syncs | Solo groups | Join waits | Join wait (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Zero wait | `1793506552651` | 338,882 | 1.320 | 2.675 | 52.651 | 3,183 | 1,675 | 0 | 0 |
| 10µs join | `1793506563036` | 341,090 | 1.324 | 2.737 | 4.498 | 3,148 | 1,082 | 2,361 | 17.290 |

The candidate was +0.65% in throughput, +2.31% in p99, and -1.10% in syncs.
Solo groups fell by 35.4%, but that change did not translate to a consistent
latency or throughput win in this pair. The baseline's maximum latency was a
single large outlier; p99 remained the scored tail measure.

## Excluded controls

| Policy | Run ID | Throughput (ops/s) | p99 (ms) | `fdatasync` (s) | Measurement (s) | Reason |
|---|---:|---:|---:|---:|---:|---|
| Zero wait, 16 writers | `1793506575379` | 38,587 | 51.981 | 18.827 | 19.437 | Storage-stall dominated |
| Zero wait, 8 writers | `1793506606102` | 23,331 | 48.651 | 31.290 | 32.145 | Storage-stall dominated |

The two controls were excluded because `fdatasync` consumed nearly the entire
measurement interval and tail latency reached tens of milliseconds. No
candidate run was scored during this stall period. These runs do not establish
the candidate's behavior at 16 writers.

## Verification

The candidate passed the focused V8 suite: 135 tests passed. An independent
review found no correctness or progress issue. The code was not retained
because the clean performance pair did not satisfy the keep rule.
