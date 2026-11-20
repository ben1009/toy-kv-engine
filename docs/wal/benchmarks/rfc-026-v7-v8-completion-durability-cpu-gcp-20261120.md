# RFC 026 V7/V8 completion and durability CPU diagnosis on GCP — 2026-11-20

This diagnostic separates completion receive, ticket publication, prefix
advancement, and durability preparation CPU for V7 and V8. It found similar
completion-receive CPU, a small DATA-complete prefix-publication cost, and
larger measured V8 ticket-publication and durability-preparation scopes. The
scopes are not fully equivalent and overlap, so these results identify where
to investigate next; they do not establish a causal explanation for the V8
Point throughput gap.

## Setup

- One `c4-standard-16-lssd` GCP VM in `us-central1-b`, with two Local SSDs
  striped as ext4 RAID0.
- 100,000 Point Puts per run, 1 KiB values, 8 and 16 writers, three paired
  repetitions at each writer count. V7 Parallel was compared with V8 Parallel
  using 1 MiB Point Lookahead, Direct FRONTIER, and dynamic coalescing.
- One release `write-perf` binary was used for all runs. The benchmark enabled
  Thread CPU sampling once per 16 calls in each phase and latency sampling once
  per 100 Puts. These are diagnostic results, not uninstrumented performance
  scores.
- The complete retry produced 12 successful runs. `summary.json` reported
  `complete: true` and 12/12 runs; `remote-exit.json` reported
  `failed: false` and 12 runs started. The local workflow verifier matched all
  planned run IDs to successful `result.json` and `run-metadata.json` files.

The initial attempt was excluded: a temporary runner incorrectly required a
V8-only I/O stability field in V7 results and could not summarize an invalid
run. The runner was corrected, and the complete plan was rerun on the same VM.
The partial first-attempt archive was retained separately.

## Diagnostic run context

Values are medians across the three runs in each arm. The paired throughput
change is the median of the three per-pair changes.

| Writers | V7 throughput | V8 throughput | Paired V8 change | V7 CPU/Put | V8 CPU/Put | V7 → V8 Puts/Sync |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 35.57k/s | 32.00k/s | −9.70% (0/3 V8 wins) | 40.25 μs | 52.25 μs | 7.69 → 7.70 |
| 16 | 48.94k/s | 42.09k/s | −14.20% (0/3 V8 wins) | 59.52 μs | 78.59 μs | 12.16 → 10.01 |

The runs show the expected V7/V8 diagnostic gap, but the sampling changes the
workload and only three pairs were run. Do not use these throughput values as
an uninstrumented benchmark baseline. Latencies were sampled every 100 Puts;
the p95/p99 values in the raw results are correspondingly coarse.

## Phase measurements

CPU is estimated Thread CPU per Put: sampled Thread CPU is extrapolated by the
phase's total call count and sample count. Call rates count every invocation,
not just sampled invocations. Paired deltas are the median of the three
within-repetition V8-minus-V7 differences. Values are rounded.

| Measured scope (CPU μs/Put; calls/Put) | 8 writers V7 | 8 writers V8 | Paired Δ | 16 writers V7 | 16 writers V8 | Paired Δ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Completion receive | 3.50 (0.709) | 2.91 (0.977) | −0.56 | 3.13 (0.616) | 3.31 (0.945) | +0.09 |
| Ticket publication | 0.78 (0.579) | 4.35 (1.953) | +3.58 | 0.92 (0.528) | 4.90 (1.890) | +3.96 |
| DATA prefix publication / advance | 0.21 (0.579) | 0.43 (0.977) | +0.22 | 0.23 (0.528) | 0.48 (0.945) | +0.24 |
| Digest prefix fold | 2.13 (0.579) | no calls | — | 2.37 (0.528) | no calls | — |
| V8 frontier-ready prefix scan | — | 0.43 (0.371) | — | — | 0.43 (0.204) | — |
| Durability preparation | 2.93 (0.130) | 8.18 (0.130) | +5.30 | 2.46 (0.085) | 7.87 (0.100) | +5.40 |

The receiver phase did not show a material V8 CPU excess. At 16 writers, the
V8 DATA-complete group-publication subphase was about 0.24 μs/Put above the V7
prefix-advance timer. The largest measured differences were in ticket
publication and durability preparation. V8 also recorded a separate
frontier-ready prefix scan at about 0.43 μs/Put.

The V8 digest-prefix-fold hook recorded no calls. This is not a measurement of
zero total V8 digest CPU: V8 computes its entry digests during encoding, outside
this completion-path hook. V7's digest-prefix-fold timer therefore has no
direct V8 counterpart in this table.

## Scope and interpretation limits

- Phase timers overlap. V7 ticket publication wraps `process_group_result`,
  which includes the separately timed data-prefix and digest-prefix work. The
  V8 ticket-publication counter covers both `reap_v8_group_result` and the
  coordinator's data-group completion publication; the DATA-complete
  publication timer is nested within that work. Do not add these rows.
- The durability timers are not equivalent boundaries: V7 measures
  `prepare_written_prefix_sync`, while V8 measures `drive_frontier_through`.
  Their paired difference is a candidate for finer diagnosis, not a comparable
  unit-cost estimate.
- Completion receive call counts are also not one-for-one operations. V7's
  receiver polls and drains completion channels; V8's reaper blocks on its
  channel. Thread CPU excludes time asleep in the receive call, but the timer
  does not measure runnable-queue delay.
- The phase sampler extrapolates from every 16th call. The reported
  CPU/Put estimates can vary if selected calls differ from the unsampled calls.
  This run did not include an attribution-off control.
- V8 had more syncs at 16 writers (median 9,991 vs V7's 8,221), while the
  counts were nearly equal at 8 writers (12,987 vs 13,000). This is consistent
  with earlier group-size observations but does not prove that sync grouping
  causes the CPU or throughput difference.

## Next diagnostic

Keep the WAL algorithm unchanged. First split V8 ticket publication into its
Reaper and coordinator call sites, and split the V8 durability timer into
exclusive work scopes while retaining the existing outer timer. Keep call
counts alongside sampled CPU. If a phase remains a candidate, use a separate
uninstrumented A/B before proposing a runtime change.

## Reproduction and retained results

- Source commit: `6e2d637e35199f6abe456b02d235d3c381ad69c4`.
- Temporary benchmark-only Rust patch SHA-256:
  `0a55b4badad6f703fe401428c96daef98e953c200efb2968a34c55478324ddb8`.
- Temporary runner SHA-256:
  `e0e33b01a756cda95272e3e4c510b9a3eb0fa126143f2bb7ff2b26268fb6ffdb`.
- Binary SHA-256:
  `a2ed44f3b288932e1d71228cd95fdca2200166cefcb07b57d482f4c4867edc57`.
- Verified result archive retained locally at
  `/tmp/gcp-wal-completion-cpu/results/completion-durability-20261120/verified-collection/collection-attempt-01/results.tar.gz`.
  SHA-256:
  `57e98784a6317d6b0432157b5adce755b292cecc78dc024e105183f75a36ce49`.
- Archive checksum metadata and the extracted run results are retained beside
  the archive under `verified-collection/`.
- The VM, SSH firewall, isolated VPC, subnet, and route were verified absent
  after collection. Terraform state and cleanup logs remain under
  `/tmp/gcp-wal-completion-cpu/`.

No WAL runtime code was changed by this diagnostic or report.
