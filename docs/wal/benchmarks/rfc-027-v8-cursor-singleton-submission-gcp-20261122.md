# RFC 027 V8 Cursor singleton submission GCP qualification (2026-11-22)

## Decision

The allocation-free singleton DATA submission candidate does not pass the
Point throughput gate. Keep it unqualified and do not combine it with Direct
Completion or change any production default. Point8 gained only 1.11% and won
three of five pairs; Point16 regressed 0.43% and won one pair. Point1 and
Batch64 stayed within their throughput guardrails, and sampled Point p99 did
not show a material paired regression.

The Direct Completion experiment remains a separate candidate and did not pass
its Point16 promotion gate. Since Singleton also failed its throughput gate,
this run does not justify a combined Direct Completion + Singleton experiment.

## Results

One `c4-standard-16-lssd` host in `us-central1-b` ran a counterbalanced
five-repetition comparison of Cursor control and Cursor singleton submission.
Each run performed 100,000 operations with 1 KiB values. Point ran at 1, 8,
and 16 writers; Batch64 at 16 writers was a throughput guard. Both arms used
the same release binary, Cursor progress mode 2, completion relay, Direct
FRONTIER, O_DIRECT DATA and FRONTIER, dynamic coalescing, ordered DATA
submission, and 1 MiB Point-only sliding lookahead. Only
`TOYKV_BENCH_V8_CURSOR_SINGLETON_SUBMISSION` changed from `0` to `1`.
Direct Completion and detailed diagnostics were disabled. Point latency was
sampled every 10 completed puts.

Throughput, CPU/Put, and the latency percentiles are medians across five runs
per arm. Paired changes are the median of the five within-repetition
percentage changes, not the ratio of arm medians.

| Workload | Writers | Cursor control kputs/s | Singleton kputs/s | Paired throughput change | Candidate wins | CPU/Put control → singleton (μs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 7.499 | 7.589 | +0.34% | 3/5 | 64.34 → 64.00 |
| Point | 8 | 30.903 | 31.245 | +1.11% | 3/5 | 54.71 → 54.99 |
| Point | 16 | 43.058 | 42.730 | −0.43% | 1/5 | 77.90 → 77.60 |
| Batch64 | 16 | 630.789 | 629.896 | −0.18% | 2/5 | 4.95 → 5.02 |

| Workload | Writers | Puts/sync control → singleton | p50 ms control → singleton | p95 ms control → singleton | p99 ms control → singleton | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 1.000 → 1.000 | 0.126 → 0.125 | 0.149 → 0.146 | 0.395 → 0.397 | +0.41% |
| Point | 8 | 7.715 → 7.722 | 0.238 → 0.237 | 0.400 → 0.401 | 0.712 → 0.719 | +0.02% |
| Point | 16 | 10.103 → 10.121 | 0.329 → 0.332 | 0.659 → 0.662 | 0.936 → 0.941 | −0.01% |
| Batch64 | 16 | 512.821 → 515.464 | — | — | — | — |

Percentiles are medians of per-run sampled percentiles, not percentiles
computed from pooled raw samples. Batch64 was throughput-only. The candidate
passed the Point1/Batch64 paired-throughput guard (no worse than 2%) and the
Point8/Point16 p99 guard (no more than 5% regression). It failed the Point
throughput gate: both Point8 and Point16 must gain at least 5% and win at
least four of five pairs. Point8 gained 1.11%; Point16 regressed 0.43% and won
one pair. `candidate_qualified` is therefore false.

The singleton representation reduced temporary collection work for a
single-ticket group, but this experiment found no meaningful throughput
benefit. It does not establish that the candidate is incorrect; it establishes
that the measured performance is insufficient to justify promotion.

## Run verification and provenance

- Planned, started, attempted, and completed: 40/40 runs.
- `summary.json`: `complete: true`; matrix validation observed all 40 planned
  runs with no missing, duplicate, invalid, or unexpected cells.
  `remote-exit.json`: `failed: false`, 40 runs started and attempted.
- The strict singleton-submission validator confirmed all planned run IDs have
  successful `result.json` files and matching `run-metadata.json` files.
- Runtime source commit: `d45934360d1db51796bc4149754616efb81da9f2`.
- Release `write-perf` SHA-256:
  `e8864383208d2984ba999aa70e8019691421bdc5e6df320b54c8daa6f1a995b3`.
- Host: 16-vCPU C4, two Local SSD devices in RAID0, ext4, Linux
  `7.0.0-1017-gcp`, pinned nightly `nightly-2026-09-23`, zone
  `us-central1-b`. The benchmark filesystem was `/dev/md/walbench`.
- The downloaded archive matched the remote checksum:
  `0c376916a37c6f300a857c0b91c2558502efbd5e9ea64e9b219183e4fdd6b95d`.
  The verified archive contained 300 files. The archive and per-run metadata
  remain locally at
  `bench-results/gcp-v8-cursor-singleton-submission-d4593436-us-central1-b/`.
- Cleanup verification confirms the benchmark VM, temporary SSH firewall rule,
  isolated VPC, subnet, egress route, and Terraform resources are absent; the
  Terraform state is empty and retained outside the repository.

This is a same-host, same-binary benchmark-only comparison. It does not
qualify mixed Point/Batch concurrency or production behavior, and it does not
change the Cursor or V8 production defaults.

