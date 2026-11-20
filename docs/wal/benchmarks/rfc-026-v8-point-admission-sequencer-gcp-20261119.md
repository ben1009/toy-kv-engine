# V8 Point Admission and Sequencer Breakdown

Date: 2026-11-19

## Scope and method

This is a diagnostic follow-up to the [V7/V8 Point Producer critical-path
report](rfc-026-v7-v8-point-producer-critical-path-gcp-20261119.md). It splits
the V8 admission interval under `LsmMvccInner::write_lock` into resource
reservation, DATA buffer allocation, ticket publication, and nested lock
operations. It does not change WAL runtime behavior.

The source base was `6949ff79146b21bb7266f166d26be6f9b18da08b`. A temporary
bench-only patch was applied remotely; its SHA-256 is
`970448f225b25db5af0455d8fb8226e6f3ba58159cc53ab8ff2a1aff3c569bca`. The
release benchmark binary SHA-256 was
`95e179f6917ac8e0da9648f8dc7314f9077988bd890ef3a612880e80b95bf2a2`.
Neither the patch nor the runner changes are committed.

The host was `c4-standard-16-lssd`, with 16 vCPUs, two 375 GB NVMe Local SSDs
in RAID0, ext4, and kernel `7.0.0-1017-gcp`. Rust was
`nightly-2026-09-23`. Each cell ran 100,000 1 KiB Point puts. V7 Parallel was
compared with V8 Parallel using 1 MiB Point-only Lookahead, Direct FRONTIER,
and dynamic coalescing. CPU attribution sampled every 64 puts. There was one
instrumented run per writer/version cell, eight runs total; this is not a
throughput score matrix.

## Diagnostic run context

The following throughput and process CPU values came from the instrumented
runs. They are contextual only: the sampler can perturb the measured path, and
there was one run per cell.

| Writers | V7 puts/s | V8 puts/s | V7 process CPU/Put | V8 process CPU/Put |
|---:|---:|---:|---:|---:|
| 1 | 6,397 | 7,431 | 68.64 μs | 61.55 μs |
| 4 | 22,358 | 19,937 | 39.03 μs | 45.89 μs |
| 8 | 35,186 | 31,256 | 41.02 μs | 55.18 μs |
| 16 | 49,122 | 41,649 | 60.49 μs | 81.41 μs |

At 8 and 16 writers, V8 also covered fewer puts per sync in this diagnostic
run: 7.65 vs 7.72 at 8 writers, and 9.78 vs 12.11 at 16 writers. These
single-run values are not evidence that sync grouping caused the observed
throughput difference.

## Sequencer wait and hold

Each percentile is across 1,562 completed sampled Point operations in one run.
Wait is measured around acquisition of the shared `write_lock`. The V8 hold
timer covers the serialized timestamp setup and admission region. The
temporary patch did not capture a comparable V7 hold interval.

| Writers | WAL | Lock wait wall p50 / p95 | Lock wait thread CPU p50 / p95 | Lock hold wall p50 / p95 | Lock hold thread CPU p50 / p95 |
|---:|---|---:|---:|---:|---:|
| 8 | V7 | 0.329 / 12.832 μs | 0.329 / 5.113 μs | — | — |
| 8 | V8 | 12.692 / 79.002 μs | 2.966 / 11.286 μs | 8.733 / 25.176 μs | 8.692 / 24.734 μs |
| 16 | V7 | 0.363 / 54.942 μs | 0.368 / 9.086 μs | — | — |
| 16 | V8 | 44.525 / 164.414 μs | 3.364 / 13.238 μs | 9.645 / 37.393 μs | 9.614 / 26.948 μs |

V8's sampled wall wait rises sharply with writer count, while its median hold
time changes much less. Thread CPU wait remains far below wall wait, so most of
the measured wait is blocked or scheduling time rather than busy spinning.
V7 also has a long wait tail at 16 writers; the distinction is clearest in the
median and at 8 writers. This supports the sequencer as a concurrency-sensitive
cost, but does not quantify how much throughput an optimization would recover.

## V8 admission subphases

These values are per completed sampled V8 operation. Admission and its
subphases are nested; do not add their independent percentiles.

| Writers | Admission wall p50 / p95 | Admission CPU p50 / p95 | Resource reserve CPU p50 / p95 | `DirectBufAllocation` CPU p50 / p95 | Ticket publish CPU p50 / p95 |
|---:|---:|---:|---:|---:|---:|
| 8 | 7.196 / 22.970 μs | 7.189 / 22.415 μs | 0.859 / 2.946 μs | 0.472 / 4.384 μs | 0.740 / 2.883 μs |
| 16 | 7.972 / 35.523 μs | 7.963 / 24.477 μs | 0.903 / 2.922 μs | 0.478 / 4.142 μs | 0.892 / 4.247 μs |

Nested mutex waits were short at the median and p95:

| Writers | Frontier resource map wait wall p50 / p95 | Reserved-end lock wait wall p50 / p95 | WAL cursor update CPU p50 / p95 |
|---:|---:|---:|---:|
| 8 | 0.288 / 0.844 μs | 0.282 / 0.376 μs | 0.368 / 1.107 μs |
| 16 | 0.322 / 1.360 μs | 0.291 / 0.494 μs | 0.390 / 2.290 μs |

The measured nested lock waits do not account for the full admission interval.
Neither buffer allocation, resource reservation, nor ticket publication
dominates its median by itself. Frame Finish remains a separate path outside
`write_lock`; its sampled thread CPU p50 / p95 was 8.585 / 13.992 μs at 8
writers and 9.913 / 20.807 μs at 16 writers.

## Interpretation

The strongest signal is a long, mostly off-CPU queue before V8 enters the
sequencer, combined with roughly 8–10 μs of active CPU while the lock is held
at the median. The additional admission subphases show that the measured
resource-map and reserved-end mutex waits are small; they do not identify one
inner operation as the dominant cause. This narrows the next investigation to
the serialized admission region and its handoff behavior, but does not prove
that `write_lock` is the sole Point throughput bottleneck.

No optimization is justified by this single diagnostic pass. If testing a
change, isolate one reduction in work under `write_lock`, keep ticket ordering
and resource ownership unchanged, then use uninstrumented paired throughput
runs. Keep Frame Finish as a separate candidate.

## Validation, artifacts, and cleanup

The runner reports `complete: true`, 8/8 planned runs completed, and
`remote-exit.json` reports `failed: false` with 8 runs started. All eight
planned writer/version cells have a successful `result.json` and matching
`run-metadata.json`. Each cell contains 1,562 completed producer samples with
zero dropped producer samples; thread CPU timing was supported and no thread
samples were dropped. The exact source-patch checksum matches the value saved
with the results.

The transferred archive checksum was verified against the retained collection
metadata. The raw archive and its SHA-256 sidecar are kept locally at
`/tmp/gcp-v8-admission-6949ff79/results-c/collection-attempt-01/`; raw host
artifacts are not checked into the repository. The summarized analysis is at
`/tmp/gcp-v8-admission-6949ff79/admission-analysis.json`.

After verification, the VM, boot disk, temporary SSH firewall, isolated VPC,
subnet, route, and static SSH firewall were confirmed absent. Terraform state
is empty. Cleanup verification is retained at
`/tmp/gcp-v8-admission-6949ff79/cleanup-verification.json`. Billing, APIs, and
quota settings were not changed.
