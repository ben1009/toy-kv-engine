# V7 / V8 Point CPU and scheduler diagnosis

Date: 2026-11-13 UTC

Source commit: `fb51a88a97be087361285a35bb5a6fad58109d30`

Suite: diagnostic-only, 16 writers, 100,000 Point puts, 1 KiB values

Environment: GCP C4 16-vCPU / 8-core VM, Linux `7.0.0-1017-gcp`, ext4 on `/dev/md127`, `rw,noatime,stripe=256`

WAL baseline: V7 Parallel, V8 Sliding Fallocate + Buffered/Dynamic, V8 Sliding Fallocate + Direct/400 μs.

## Result integrity

The corrected run completed all 8 planned tasks. `summary.json` reports `complete: true` and 8/8 runs; `remote-exit.json` reports `failed: false` and 8 started runs. Each run ID has a successful `result.json` and matching `run-metadata.json`. The archive and checksum sidecar are retained locally; verified SHA-256:

`6ab72949536b2e15cecd618aa1444784fe7d019cd6c28f27d845cd0b460d272f`

Runner patch bundle SHA-256: `caa93e31ffebe606500aa2fba8ec8d84739c0e01e80ba3a6f9754ff1eca29503`.

An earlier collection attempt is also retained but excluded from comparison: its runner misclassified arm suffixes, and its I/O validation compared mutable extent-preparation counters. Those issues were corrected in an ignored, local runner patch; the verified rerun above used the corrected bundle. No archive was uploaded to GitHub.

## Perf stat

One capture per arm; these are diagnostic runs, not unprofiled throughput scores. `perf stat` could not access hardware `cycles` or `instructions` on this VM, so those columns are unavailable.

| Arm | Diagnostic puts/s | Task clock | Task clock / put | Context switches | / put | CPU migrations | / put | Syncs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| V7 Parallel | 44.05k | 5.361 s | 53.61 μs | 333,780 | 3.338 | 37,551 | 0.376 | 8,169 |
| V8 Buffered/Dynamic | 31.15k | 6.686 s | 66.86 μs | 382,935 | 3.829 | 34,420 | 0.344 | 6,340 |
| V8 Direct/400 | 33.84k | 6.533 s | 65.33 μs | 380,215 | 3.802 | 34,319 | 0.343 | 6,334 |

Task-clock per operation is about 25% higher for Buffered V8 and 22% higher for Direct/400 than V7. Context switches per operation are about 15% higher. Migrations are about 8% lower in V8, so migration count alone does not explain its remaining Point gap. Each row is a single profiled capture; use these only as directional evidence.

The measured handles matched the intended configurations: V8 DATA was `O_DIRECT` in both arms; Buffered FRONTIER was buffered; Direct/400 FRONTIER was `O_DIRECT`; the sync descriptor was buffered in both. Sliding Fallocate was active. Handle diagnostics refer to the actual WAL inode for each run.

## On-CPU profile

`perf record` used the software `task-clock:ppp` event because hardware sampling was unavailable. It captured 352 V7, 487 Buffered V8, and 464 Direct/400 samples, with zero lost samples.

`publish_commit_ts` was the largest named self-time hotspot in each capture: 19.32% V7, 22.59% Buffered V8, 20.26% Direct/400. Mutex slow-path samples were around 3–4% in each report. Futex wait/wake frames appear in the call graphs, but these samples are on-CPU samples and do not measure time asleep. No clear named hotspot in `acquire_durability_driver`, `publish_durable_frontier`, or `fdatasync_once` dominated the captured CPU samples.

This points to MVCC publication as a CPU hotspot worth separating from WAL work. The percentages are single-run samples and do not establish that MVCC publication causes the throughput gap.

## Scheduler trace

`perf sched` was run for V7 and Direct/400 only. For the `write-perf` task, average runnable-to-running delay was 8 μs (max 159 μs) for V7 and 9 μs (max 977 μs) for V8 Direct/400. This is runnable queue delay; it does not include sleeping time in condition variables or mutex waits, and cannot identify which wait primitive blocked a thread. The system-wide context-switch totals in the trace were 344,772 and 447,119 respectively; they include host tasks, unlike the process-scoped `perf stat` counts.

## Interpretation and limits

- The results support additional V8 CPU work per put and more context-switch activity, but they do not show unusually large runnable-queue delay on average.
- Lower V8 migration counts make CPU migration an unlikely sole explanation.
- `publish_commit_ts` is a visible CPU hotspot in all variants, with a larger sample share in Buffered V8. This needs a matched low-overhead follow-up before attributing causality.
- The run does not separate elected-driver CPU from its I/O wait, follower wake/resume, coordinator/frontier lock time, or durable-publish-to-`put()` ACK latency. It also has no V8 Buffered `perf sched` arm.
- The requested sampled phase instrumentation was not run in this VM window. In particular, the proposed `CLOCK_THREAD_CPUTIME_ID` per-advance measurements and publish-to-WAL-return / MVCC-return intervals remain open. No WAL runtime source was changed in this run.

## Next diagnostic

Add opt-in bench-only sampling (for example one advance in 32 or 64) for driver election, frontier lock, coalescing, write+sync, publish, follower resume, WAL return, and `put()` return. Capture wall and thread CPU separately. Run the phase capture at 1, 4, 8, and 16 writers; keep perf, BPF, and detailed sync diagnostics in separate runs. Do not use diagnostic throughput as an unprofiled score.

## Cloud resources

No cleanup or Terraform destroy was run. The benchmark VM, boot disk, temporary SSH ingress rule, isolated VPC/subnet/route, and Terraform state were retained per the benchmark instructions. The VM was configured with a 60-minute maximum lifetime and may reach its scheduled auto-delete time; network resources and state remain retained. No cleanup verification is claimed.
