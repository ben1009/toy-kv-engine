# RFC 026 V8 coalescing budget A/B on GCP — 2026-11-11

## Result

The adaptive 100/400 μs rule is changing group size in response to FRONTIER
I/O latency. Across the dynamic-control runs, Buffered selected 400 μs for all
but two eligible decisions; Direct selected 100 μs for 30% of eligible
decisions. Fixing the eligible-branch budget at 400 μs raised Direct throughput
by 5.5% versus its dynamic control and reduced its median sync count by 18.3%.

A 100 μs budget consistently made smaller groups and more syncs. Relative to
100 μs, 400 μs increased median throughput by 12.2% with Buffered FRONTIER and
8.4% with Direct FRONTIER, while reducing median sync counts by about 43% in
both modes. Buffered dynamic runs already behaved like fixed 400 μs runs.

This supports the scheduling-feedback hypothesis for the Direct path. It does
not qualify a production policy: this was four repetitions per arm with
per-sync diagnostics enabled, and the fixed-budget override is bench-only.
No WAL durability ordering or default runtime behavior changed.

## Setup and method

- Source commit: `6d13c519eb08a76556f44610e7a30335bbb4491a`.
- GCP `c4-standard-16-lssd`, 16 writers, Intel Xeon 6985P, two NVMe Local SSDs
  in RAID0, ext4 with `noatime`.
- Each run performed 100,000 concurrent Point puts with 1 KiB values, V8 PITR
  Parallel WAL, and compaction disabled.
- `--profile` was off. `--wal-sync-diagnostics` was on to retain per-sync
  measurements; operation latency was sampled every 100 completed operations.
- The bench-only `TOYKV_WAL_V8_COALESCE_BUDGET_US` override accepted `100` or
  `400`; when unset, the existing latency-based rule selected the budget.
  The override applies only when the coalescing branch is eligible: a nonempty
  ready prefix shorter than the admitted ticket count, with a prior sync
  latency available. Fixed values required sync diagnostics. Dynamic selection
  retained the existing `latency >= 100 μs ? 400 μs : 100 μs` rule.
- Four mixed-order blocks ran six arms each. `B` means Buffered FRONTIER,
  `D` means Direct FRONTIER, and `dyn` means the dynamic rule:

  | Block | Run order |
  | --- | --- |
  | 1 | B400, D400, Bdyn, D100, B100, Ddyn |
  | 2 | B100, D100, Ddyn, D400, B400, Bdyn |
  | 3 | D400, B400, Bdyn, B100, D100, Ddyn |
  | 4 | D100, B100, Ddyn, B400, D400, Bdyn |

All 24 runs completed 100,000 operations. The 229,355 sync observations
succeeded, sync counts matched observation counts, and every run had stable
WAL I/O metadata. DATA was `O_DIRECT=true` in every arm; FRONTIER matched the
configured mode; the sync descriptor remained buffered. All three descriptors
referred to the same device and inode.

## Throughput and group size

Medians are across four runs per arm. The throughput range shows the four
individual run values. Tickets per advance are pooled across that arm's sync
observations.

| FRONTIER | Budget | Throughput median (range) | Syncs per run, median | Tickets per advance p50 / p95 | Eligible decisions selecting 100 / 400 μs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Buffered | 100 μs | 18.12k/s (17.87–18.26k) | 12,987 | 7 / 16 | 37,410 / 0 |
| Buffered | 400 μs | 20.34k/s (20.00–20.40k) | 7,413 | 16 / 16 | 0 / 25,179 |
| Buffered | Dynamic | 20.38k/s (20.12–20.46k) | 7,398.5 | 16 / 16 | 2 / 25,147 |
| Direct | 100 μs | 19.49k/s (19.32–19.68k) | 13,028.5 | 7 / 15 | 40,031 / 0 |
| Direct | 400 μs | 21.12k/s (20.97–21.31k) | 7,404 | 16 / 16 | 0 / 25,223 |
| Direct | Dynamic | 20.01k/s (19.87–20.12k) | 9,067.5 | 15 / 16 | 9,169 / 21,411 |

Every observed coalescing call entered the condition-variable wait in nearly all
eligible cases. Among calls with `waited=true`, the actual call duration and
net ready-prefix growth were:

| FRONTIER | Budget | Coalescing call p50 / p95 | Ready-prefix tickets gained p50 / p95 |
| --- | ---: | ---: | ---: |
| Buffered | 100 μs | 124.9 / 167.6 μs | 1 / 8 |
| Buffered | 400 μs | 402.1 / 444.5 μs | 15 / 15 |
| Buffered | Dynamic | 401.8 / 444.4 μs | 15 / 15 |
| Direct | 100 μs | 125.0 / 167.7 μs | 2 / 9 |
| Direct | 400 μs | 401.8 / 444.5 μs | 15 / 15 |
| Direct | Dynamic | 293.2 / 436.9 μs | 13 / 15 |

The wait duration includes condition-variable overhead. `waited=true` means
the call entered the condition-variable wait; it does not mean the entire
budget elapsed. Ready-prefix growth is the net change during the call and may
include tickets admitted while waiting; before/after admitted counts were
recorded to make that visible.

## Durability I/O timings

These are pooled per-advance percentiles. The final column is the median, by
run, of the sum of all per-advance FRONTIER-write and `fdatasync` timings.

| FRONTIER | Budget | FRONTIER write p50 / p95 | `fdatasync` p50 / p95 | Write + sync p50 / p95 | Cumulative write + sync per run |
| --- | ---: | ---: | ---: | ---: | ---: |
| Buffered | 100 μs | 34.9 / 247.7 μs | 168.5 / 194.3 μs | 201.3 / 420.1 μs | 3.114 s |
| Buffered | 400 μs | 4.3 / 31.3 μs | 167.6 / 190.6 μs | 173.1 / 208.3 μs | 1.306 s |
| Buffered | Dynamic | 4.3 / 31.2 μs | 167.8 / 189.8 μs | 173.4 / 207.0 μs | 1.300 s |
| Direct | 100 μs | 47.0 / 254.0 μs | 105.5 / 128.0 μs | 159.8 / 362.8 μs | 2.649 s |
| Direct | 400 μs | 39.3 / 63.2 μs | 103.7 / 122.8 μs | 144.4 / 177.4 μs | 1.102 s |
| Direct | Dynamic | 40.6 / 257.6 μs | 104.6 / 124.6 μs | 147.9 / 364.5 μs | 1.552 s |

The per-sync sample confirms the scheduling effect: for Direct, fixed 400 μs
eliminated the dynamic arm's 9,169 short-budget selections, increased the
median group from 15 to 16 tickets, and reduced median syncs from 9,068 to
7,404. Buffered dynamic behavior already selected 400 μs for 25,147 of 25,149
eligible decisions, so fixed 400 μs produced effectively the same throughput
and sync count.

Direct fixed 400 μs also had lower combined write-plus-sync p50 and p95 than
Buffered fixed 400 μs in this run set. With only four runs per arm and
per-sync diagnostics enabled, this is an exploratory result, not a qualified
performance claim. The sampled operation p99 varied between repetitions and
is not used to claim a tail-latency improvement.

## Follow-up

The next useful check is a lower-overhead confirmation of the Direct dynamic
versus fixed-400 result, with the same balanced order and no per-sync record
capture. If it holds, use a policy that avoids switching to 100 μs for Direct
syncs near the threshold; do not infer that 400 μs should become a universal
default from these data. Keep the experiment control bench-only until a
separate runtime proposal has recovery and durability qualification.

The VM, boot disk, firewall, subnet, and network were destroyed after artifact
collection. Independent GCP inventory showed zero remaining resources. The
compressed raw JSON is retained in local `/tmp/gcp-v8-fixed-budget-20261111`
with SHA-256
`0c688ebd6fd52f30e6ab9061f7a05234ee428982e103c18d77be83637271575f`; raw data
is not committed to this PR.
