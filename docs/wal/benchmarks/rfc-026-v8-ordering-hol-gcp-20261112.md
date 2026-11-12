# RFC 026 V8 ordering head-of-line diagnostics on GCP, 2026-11-12

## Result

This diagnostic run points to DATA-completion-prefix waiting as the ordering
signal most associated with the throughput change. Pinning io-wq to logical CPU
8 reduced its aggregate ticket-wait time by 62% and increased median throughput
by 28.7% in this workload. DATA-submission and digest-folding HOL measurements
were small and barely changed.

This does not show that single-CPU placement improves V8 scalability. Affinity
changes scheduling and completion timing together, and the HOL counters do not
identify which earlier ticket or kernel event caused each gap. The data supports
targeted investigation of completion-prefix formation; it does not justify
changing the production execution model or relaxing durable-prefix ordering.

## Setup

- GCP C4, 16 vCPUs, two NVMe Local SSDs in RAID0, ext4.
- 16 writers; each run performed 100,000 point puts with 1 KiB values.
- PITR V8 Parallel, Direct FRONTIER, fixed 400 μs coalescing.
- Five counterbalanced adjacent pairs: io-wq allowed on all logical CPUs versus
  io-wq pinned to logical CPU 8. WAL helper and writer affinity remained
  unpinned. The actual io-wq masks were verified after each run.
- `--profile` and `--wal-sync-diagnostics` were off. The opt-in
  `--wal-hol-diagnostics` counters were on. Every run reported 100,000 CQEs.
- Source base: `2eb897949e9691eb132827874fe3699b19962e02`; the benchmark-only
  diagnostics were applied as a local patch for this run.

## Results

Values below are medians across five runs per arm unless otherwise specified.

| Metric | All logical CPUs | CPU 8 | CPU 8 vs all |
| --- | ---: | ---: | ---: |
| Throughput | 21.03k puts/s | 27.02k puts/s | +28.7% median paired |
| Sampled p50 | 0.751 ms | 0.575 ms | -23.4% |
| Sampled p99 | 1.105 ms | 0.946 ms | -14.4% |
| Process CPU | 11.621 s | 9.111 s | -21.6% |
| Sync count | 7,342 | 6,448 | -12.2% |
| Mean `fdatasync` | 105.93 μs | 105.74 μs | essentially unchanged |
| Eventfd writes | 39,296 | 35,722 | -9.1% |

All five paired throughput comparisons favored CPU 8: +32.46%, +28.51%,
+28.71%, +28.78%, and +28.43%.

| Ordering measurement | All logical CPUs | CPU 8 |
| --- | ---: | ---: |
| DATA-submission HOL episodes | 3,748 | 3,569 |
| DATA-submission HOL wall time | 25.42 ms | 25.08 ms |
| Submission successor ticket-time | 32.32 ms | 33.49 ms |
| Digest-order HOL tickets | 2,943 | 2,876 |
| Digest-order ticket-time | 17.19 ms | 18.06 ms |
| DATA-completion-prefix HOL tickets | 27,407 | 23,131 |
| DATA-completion-prefix ticket-time | 4,606.88 ms | 1,734.32 ms |

Ticket-time is the sum of individual tickets' waiting durations. These waits
overlap and must not be added as wall-clock benchmark time. Submission HOL wall
time is measured separately. DATA-completion-prefix wait starts when a DATA
completed ticket is observed behind an earlier ticket whose DATA is incomplete,
and ends when that ticket joins the contiguous DATA-complete prefix. It does not
include subsequent digest or durability waits.

## Interpretation

The strongest separation is in DATA-completion-prefix waiting: aggregate
ticket-time falls by 62.4%, while digest and submission measurements remain
close. Mean `fdatasync` latency is unchanged, and Sync count falls by 12.2%.
This is consistent with io-wq placement changing DATA completion timing and
therefore the groups presented to the durability driver. It is not proof that
CQE reordering is the cause: this instrumentation does not correlate a blocked
ticket to a particular SQE/CQE, device queue event, or predecessor latency.

The next useful diagnostic is to correlate per-ticket DATA submit and CQE
timestamps with the first incomplete prefix ticket and the resulting durability
group boundary. Keep io-wq placement as a diagnostic control, and compare at
multiple masks and writer counts before considering any policy change.

## Limits and validation

This is one 16-writer Point workload with diagnostic instrumentation enabled;
the latency percentiles are sampled, and the throughput numbers are diagnostic
results rather than formal qualification scores. Pinning one logical CPU is not
a proposed production optimization and does not test multi-core scalability.

The captured run JSON had a stale `v8_hol_diagnostics_enabled: false` field:
benchmark warmup reset the profile copy of that flag after the runtime counters
had been enabled. All ten runs had nonzero submission, digest, and completion
HOL counters, confirming the instrumentation ran. The exporter now records the
CLI configuration directly; this metadata correction does not change the
measurement path, so the GCP matrix was not rerun.

Local checks after the exporter correction:

- `cargo fmt --all -- --check`
- `cargo check --package kv-engine --no-default-features --lib`
- `cargo check --package kv-engine --features bench --bin write-perf`
- `cargo test --package kv-engine --features bench --bin write-perf wal_hol_diagnostics_require_bench_build_and_v8_parallel`

The temporary GCP VM and network resources were destroyed and verified absent.
No raw archive of this HOL run was added to the repository.
