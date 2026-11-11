# RFC 026 V8 DATA submission-order probe — 2026-11-11

## Question

Does requiring the DATA submitter to wait for the next ticket before submitting
later ready tickets limit V8 Point throughput? This probe compares the normal
ticket-prefix submitter with a benchmark-only path that submits each ready
ticket at its admission-assigned physical offset. Digest folding, the
contiguous ready prefix, FRONTIER construction, `fdatasync`, and durable
publication remain ticket ordered.

The benchmark-only control is `TOYKV_BENCH_V8_DATA_SUBMIT_ORDER=ordered|ready`
and is available only in `bench` builds. The worker accepts gaps in the ready
mode, while still rejecting overlapping live ticket ranges or file ranges.

## Setup

The binary was built with the repository-pinned nightly from source commit
`23a9470f`, which adds the benchmark-only probe and recovery tests. Its SHA-256 was
`02a71f146fd0c1e7b66c5e6ee5d1f27bd5f24042047656ba90eee860cfacb7df`.

The temporary host was `c4-standard-16-lssd` in `us-east1-b`: 16 logical CPUs
across 8 cores, two NVMe Local SSDs in RAID0, and ext4. Each run used V8
Parallel, Direct FRONTIER, fixed 400 μs coalescing, 16 writers, 1 KiB values,
and 100,000 measured Point puts. Each process first performed a 32,768-put
concurrent warmup to initialize io-wq; the benchmark counters were reset before
the measured workload. Both arms used the same all-CPU io-wq mask; user-space
helper and writer threads were not pinned.
There was no `--profile` or `--wal-sync-diagnostics`; the benchmark still
collected its normal aggregate counters and sampled latency once per 100 puts.

Twenty runs were arranged as five counterbalanced four-run blocks, alternating
ABBA and BAAB order. Every run used a fresh database. Validation confirmed
100,000 measured operations, the requested arm, stable WAL metadata during the
run, DATA and FRONTIER `O_DIRECT=true`, and buffered Sync FD. The post-warmup
io-wq thread count was 29–32; this is a thread count, not a count of workers
executing concurrently.

## Results

| Metric | Ordered ticket prefix | Any ready ticket | Change |
| --- | ---: | ---: | ---: |
| Throughput mean | 20.55k puts/s | 20.44k puts/s | -0.5% |
| Throughput median | 20.74k puts/s | 20.59k puts/s | -0.7% |
| p50 latency median | 0.748 ms | 0.754 ms | +0.8% |
| p99 latency median | 1.234 ms | 1.369 ms | +10.9% |
| Sync count mean | 7,558 | 7,606 | +0.6% |
| Tickets per Sync mean | 13.23 | 13.15 | -0.6% |
| Process CPU mean | 11.333 s | 11.374 s | +0.4% |
| Mean `fdatasync` time | 108.7 μs | 110.2 μs | +1.4% |
| FRONTIER write + Sync total mean | 1.219 s | 1.240 s | +1.7% |

The paired block throughput changes (ready versus ordered) were `+2.96%`,
`-0.89%`, `-3.50%`, `+0.65%`, and `-1.74%`. The median paired change was
`-0.89%`, with a range from `-3.50%` to `+2.96%`; three of five blocks favored
ordered submission. Both arms reached a maximum of 16 in-flight worker groups
and 16 outstanding write SQEs.

This run does not demonstrate a throughput improvement from the tested
any-ready path. Sync count, CPU use, and cumulative FRONTIER-write-plus-Sync
time were also effectively unchanged. The p99 medians moved in the wrong
direction, but one-in-100 latency sampling and the run-to-run variation do not
support a stable tail-latency conclusion.

## Limits and follow-up

This is a probe of one implementation, not a clean rejection of the
head-of-line-blocking hypothesis. The ready arm submits one worker group per
ticket, whereas the ordered arm can pack contiguous tickets into one group;
the experiment therefore changes worker command/group granularity as well as
which ready tickets are eligible for submission. It also records the selected
mode but not how many tickets actually leapfrogged an unready head. A flat
result could mean there was little reordering to exploit, or that any benefit
was offset by the per-ticket submission overhead.

Do not enable this path by default. The next useful probe should retain
independent ticket completion while batching adjacent ready tickets where
possible, and record actual leapfrog counts plus per-ticket preparation,
submission, CQE, and ready-prefix wait intervals. Keep digest folding and
durable publication ordered. There is no evidence here to justify a TPC rewrite
or a production change.

The targeted benchmark-feature tests passed for relaxed worker ordering,
non-overlapping ranges, contiguous durable-frontier publication, and active
recovery discarding a completed successor behind a missing head. The GCP VM,
subnet, firewall, network, and lifetime guard were destroyed after the run.
Only this summary is retained in the repository; raw run files were not
uploaded.
