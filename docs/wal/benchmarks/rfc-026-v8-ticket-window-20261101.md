# RFC 026 V8 Parallel ticket-window screen (2026-11-01)

## Result

The V8 Parallel admission window was increased from 8 to 32 tickets. Explicit
V8 Leader groups remain capped at 8 tickets. The larger window is bounded by
the coordinator's existing in-flight byte limit, which is one maximum WAL
segment.

On the 16-writer batch-64 ext4 screen, the 32-ticket candidate formed fewer
durability groups and showed higher throughput and lower p99 latency than the
8-ticket control. Keep this as a provisional V8 Parallel optimization. This
small screen is not RFC 026 performance qualification and does not change the
v7 default or enable V8 for normal database creation.

## Method

The runs used the Linux `write-perf` binary built with `--features bench` on
`/dev/nvme0n1p3` (ext4), with 16 writers, 750,000 entries, batch size 64, 1 KiB
values, PITR v8 Parallel, no compaction, and a 2 GiB target SST size. Latency
was sampled every 10 workload operations. The control binary used the committed
8-ticket limit; the candidate changed only the V8 Parallel ticket-window
limit to 32.

Command shape:

```bash
write-perf --bench wal_batch_concurrent --wal-batch-size 64 \
  --pitr --pitr-format v8 --wal-io-mode parallel --num 750000 \
  --threads 16 --value-size 1024 --target-sst-size 2147483648 \
  --compaction none --output json --latency-sample-every 10
```

## Scored runs

| Limit | Run ID | Throughput (entries/s) | p50 (ms) | p95 (ms) | p99 (ms) | Syncs | Tickets/sync | fdatasync time (s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | `1793500949896` | 267,130 | 3.518 | 5.991 | 6.918 | 3,397 | 3.45 | 2.201 |
| 32 | `1793500938114` | 312,908 | 3.168 | 4.372 | 5.526 | 2,419 | 4.85 | 1.653 |
| 32 | `1793500987390` | 291,041 | 3.541 | 4.604 | 5.608 | 2,319 | 5.06 | 1.686 |

Across the two candidate runs, median throughput was 301,975 entries/s
(13.0% above the single scored control); median p99 was 5.567 ms (19.5% lower).
The candidate's median sync count was 2,369, 30.3% below the control, while
the number of committed ticket buffers was the same (11,728) in every scored
run. Candidate run maxima were 7.7 ms and 40.7 ms; the latter is an isolated
tail outlier despite its p99 remaining near 5.6 ms.

## Caveats

There is only one scored 8-ticket control, so the throughput and latency
comparison is directional. A preceding 8-ticket run was dominated by an SSD
stall (`fdatasync` took 30.5 s of a 31.8 s run; p99 was 116.8 ms) and is
excluded. A candidate rerun exceeded the benchmark's 5 s PITR admission-barrier
retry budget and produced no metrics. Short 100,000-entry screens were also
discarded because both configurations experienced sustained `fdatasync`
stalls and had too few latency samples.

The repeatable mechanism signal is that the 32-ticket window increased average
ticket coverage per durability sync from 3.45 to about 4.95. Qualification
still needs repeated paired ext4 runs and the full RFC 026 workload matrix.

## Follow-up screens

An 8-writer pair was not comparable because the 8-ticket control was stalled:

| Limit | Throughput (entries/s) | p99 (ms) | Syncs | fdatasync time (s) |
|---:|---:|---:|---:|---:|
| 8 | 46,685 | 47.671 | 2,973 | 14.647 |
| 32 | 263,122 | 3.329 | 3,072 | 1.899 |

The control spent most of its 16.1 s run in `fdatasync`; the candidate finished
in 2.85 s. The candidate also had slightly more syncs, so this pair says nothing
reliable about the 32-ticket cap at 8 writers.

A 64-ticket cap was screened at 16 writers. One attempt failed before metrics
because the PITR admission barrier did not reopen within 5 s. The successful
retry (`1793501267713`) measured 331,831 entries/s, p99 5.370 ms, and 2,316
syncs for 11,728 tickets. This is close to the 32-ticket candidate's 2,319–
2,419 syncs and is only one successful sample, so the 64-ticket cap was not
retained. The committed cap remains 32.
