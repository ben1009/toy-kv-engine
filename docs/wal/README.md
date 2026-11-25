# WAL documentation

Ordinary V4 WALs and new PITR V8 WALs default to Parallel I/O by maintainer
decision. Start with the [V8 default adoption note](rfc-026-v8-default-20261103.md)
for selection, migration, capacity, and compatibility behavior. The
[V4 adoption note](rfc-024-parallel-wal-default-20261005.md) covers ordinary WALs.
The performance qualification matrices remain unqualified; default adoption
is not a performance parity claim.

[RFC 025](../../rfcs/025-parallel-wal-embedded-frontiers.md) defines V7's
embedded DATA/FRONTIER frames and single-sync durability protocol.
[RFC 026](../../rfcs/026-composable-wal-prefix-digest.md) retains that protocol
and adds V8's BLAKE3 entry/batch digests and fixed-size ordered prefix chain.
Eligible legacy/V7 databases migrate on resume; capacity deferral preserves
older writers. Explicit V6/V7 controls never downgrade existing V8 segments.
Active recovery validates logical hard anchors before candidate fallback and
fresh-inode normalization; sealed/archive validation stays strict.

## Current format selection

| WAL format | Default I/O | Selection and compatibility |
| --- | --- | --- |
| Legacy unframed | Buffered | Preserve the legacy codec and recovery behavior. |
| Older MVCC V2/V3 | Leader | Recover existing files without changing their format. |
| Ordinary V4 | Parallel | Remains the non-PITR writer format. |
| PITR V5/V6 | Leader | Retain existing writers when migration is deferred; V6 is an explicit compatibility preference. |
| PITR V7 | Parallel | Supported for existing data and the explicit V7 preference. |
| PITR V8 | Parallel | Default for newly enabled PITR and eligible resume migrations. |

`LsmStorageOptions::pitr_writer_format` defaults to `PitrWriterFormat::V8`.
`WalIoMode::Leader` remains an explicit comparison path for V4/V7/V8.
Capacity deferral keeps the existing writer writable; selecting V6 or V7 never
downgrades V8. V7/V8 require WAL-enabled reopen and currently reject PITR disable
operations before mutation. See the adoption note for lifecycle details.

V8 Parallel normally uses `O_DIRECT`. If the filesystem rejects direct I/O,
its worker uses a buffered handle with the same DATA completion → FRONTIER
write → WAL sync → durable publication ordering. This fallback does not extend
to the V4 direct-I/O path.

## Retained V8 runtime

Admission publishes tickets before payload preparation. Producers encode
canonical entries with entry CRC and BLAKE3, then materialize DATA frames and
frame CRCs. The submitter forms groups from admitted tickets, while readiness
controls submission. The ordered coordinator folds a 113-byte digest link per
ticket, and bounded durability coalescing combines completed prefixes into
FRONTIER/sync cycles.

Preparation, durability progress and durable publication use separate
notifications. One driver forms FRONTIERs. The WAL's published cursor path
lets a durable follower return without taking the physical FRONTIER mutex.
The direct writer append path takes that mutex to construct commit metadata.
The benchmark-only [RFC 028 dedicated durability driver](../../rfcs/028-v8-dedicated-durability-driver.md)
tests moving driver execution to an independently owned thread; caller-driven
durability remains the default while D3a is being qualified.
See the [notification rerun](benchmarks/rfc-026-v8-notification-rerun-20261102.md)
for the evidence and the [implementation plan](rfc-026-implementation-plan.md)
for the current runtime.

Dated filenames identify individual measurements and follow-ups. Their defaults,
candidate decisions and validation counts describe the measured revision;
the adoption notes above define current selection. The
[benchmark overview](benchmarks/rfc-024-parallel-wal-benchmark.md) connects
the retained and rejected experiments.

## RFC 027 qualification result

The [V8 Point Progress Ring](../../rfcs/027-v8-point-progress-ring.md) proposed
moving producer readiness to a bounded ring and having the elected driver apply
queued completions in batches. Ring did not pass its Point8/Point16 throughput
promotion gates. Cursor remains opt-in and is not the default: the latest
qualification did not pass the throughput gate, and non-bench production-path
qualification is still required. See the [Ring qualification](benchmarks/rfc-027-v8-point-progress-ring-gcp-20261121.md),
[archived three-arm Cursor qualification](benchmarks/rfc-027-v8-point-progress-cursor-gcp-20261122.md),
[Cursor direct-completion comparison](benchmarks/rfc-027-v8-cursor-direct-completion-gcp-20261122.md),
[Cursor singleton-submission comparison](benchmarks/rfc-027-v8-cursor-singleton-submission-gcp-20261122.md),
and the [implementation plan](rfc-027-v8-point-progress-ring-implementation-plan.md).
The [RFC 028 Point16 critical-path diagnostic](benchmarks/rfc-028-v8-point16-critical-path-gcp-20261122.md)
compares Cursor Relay and Direct Completion phase timings at 8 and 16 writers;
it is diagnostic only and does not establish a throughput winner or a runtime
bottleneck.
The [Cursor coalescing follow-up](benchmarks/rfc-028-v8-cursor-coalescing-gcp-20261123.md)
shows progress-token waiting dominates the sampled coalescing wall time, while
completion drain and coordinator-lock wait remain small. It also records a
stale availability flag despite populated phase fields; these single-run
diagnostics do not establish a performance winner.
The [Point Resource Preclaim final A/B](benchmarks/rfc-028-v8-point-resource-preclaim-final-gcp-20261124.md)
compares Stage A with the optimized Stage A+B3 across five paired Point8,
Point16, and Batch64 runs. B3 missed both Point throughput gates; keep Stage A
and stop the preclaim optimization.
The [Resource Retirement Ledger lock A/B](benchmarks/rfc-028-v8-resource-retirement-lock-gcp-20261124.md)
compares the pre- and post-lock-merge implementations across V8 Point1/8/16,
V7 Point16, and Batch64. Point16's paired throughput gain was 0.36% (3/5 wins),
with unchanged Puts/Sync; the experiment did not establish a stable performance
benefit. Its report documents the custom-plan validation and artifact recovery.
The [V8 Point Resource Retirement C2 A/B](benchmarks/rfc-028-v8-point-resource-retirement-c2-gcp-20261124.md)
compares per-ticket retirement, C1 detach, and C2 batch retirement across Point8,
Point16, and Batch64. C2 improved paired Point throughput by 2.07% and 3.27%,
below the 5% retention gate; it is not performance-qualified for default use.
The [Stage A plus C2 follow-up](benchmarks/rfc-028-v8-point-resource-retirement-stage-a-c2-gcp-20261124.md)
repeats the three-way comparison with Point buffer preallocation enabled. The
combined C2 gain was 4.70% at Point8 and 1.25% at Point16, still below the 5%
gate; Batch64 remained effectively unchanged.
The [coalescing-budget A/B](benchmarks/rfc-028-v8-cursor-coalescing-budget-gcp-20261123.md)
found fixed 400 μs consistently ahead of Dynamic at 8 and 16 writers, but its
paired throughput gains stayed below the 5% qualification threshold. Dynamic
remains the default.
The [ready-target A/B](benchmarks/rfc-028-v8-cursor-coalescing-ready-target-gcp-20261125.md)
missed the overall gate: Target8 gained 7.57% at Point16, but lost 3.44% at
Point8; Target12 stayed below the Point16 threshold. Dynamic remains the default.
The [D1 ACK-check A/B](benchmarks/rfc-028-v8-point-ack-d1-gcp-20261124.md)
found no stable throughput gain from consolidating the Coordinator ACK checks;
Point16 was 1.86% slower in paired median.

## Implementation and adoption

- [V8 PITR WAL default adoption](rfc-026-v8-default-20261103.md)
- [Parallel WAL default adoption](rfc-024-parallel-wal-default-20261005.md)
- [Parallel WAL Implementation Plan](rfc-024-parallel-wal-implementation-plan.md)
- [RFC 025 Implementation Plan: Parallel WAL with Embedded Frontiers and One Sync](rfc-025-parallel-wal-embedded-frontiers-implementation-plan.md)
- [RFC 026 Implementation Plan: Composable WAL Prefix Digests](rfc-026-implementation-plan.md)
- [Native async point-write integration](rfc-024-native-async-integration-20261005.md)
- [Retire frozen parallel WAL runtimes](rfc-024-frozen-wal-retirement.md)

## Format RFCs

- [RFC 025: Parallel WAL with Embedded Frontiers and One Sync](../../rfcs/025-parallel-wal-embedded-frontiers.md)
- [RFC 026: Composable WAL Prefix Digests](../../rfcs/026-composable-wal-prefix-digest.md)

## Benchmarks and qualification

- [V8 Point ticket lookup A/B on GCP](benchmarks/rfc-026-v8-ticket-index-gcp-20261120.md)
- [V7/V8 Point Frame Finish to DATA submission on GCP](benchmarks/rfc-026-v8-frame-finish-gcp-20261120.md)
- [V7/V8 Point worker notifications on GCP](benchmarks/rfc-026-v7-v8-worker-notifications-gcp-20261120.md)
- [V8 Point 3 μs DATA Group Join on GCP](benchmarks/rfc-026-v8-point-short-group-join-gcp-20261120.md)
- [V7/V8 cumulative thread CPU on GCP](benchmarks/rfc-026-v7-v8-thread-role-cpu-gcp-20261120.md)
- [V7/V8 cumulative writer-stage CPU on GCP](benchmarks/rfc-026-v7-v8-writer-stage-cpu-gcp-20261120.md)
- [V7/V8 completion and durability CPU diagnosis on GCP](benchmarks/rfc-026-v7-v8-completion-durability-cpu-gcp-20261120.md)
- [GCP Free Trial WAL benchmark runner](benchmarks/gcp-free-trial-wal-bench.md)
- [RFC 026 GCP V8 point sync diagnostic, 2026-11-10](benchmarks/rfc-026-v8-point-sync-gcp-20261110.md)
- [RFC 026 V8 FRONTIER I/O A/B on GCP, 2026-11-11](benchmarks/rfc-026-v8-frontier-io-ab-gcp-20261111.md)
- [RFC 026 V7/V8 CPU affinity on GCP, 2026-11-11](benchmarks/rfc-026-v7-v8-cpu-affinity-gcp-20261111.md)
- [RFC 026 V7/V8 io-wq affinity sweep on GCP, 2026-11-11](benchmarks/rfc-026-v7-v8-iowq-affinity-sweep-gcp-20261111.md)
- [RFC 026 V8 ordering HOL diagnostics on GCP, 2026-11-12](benchmarks/rfc-026-v8-ordering-hol-gcp-20261112.md)
- [RFC 026 V8 sliding extent initialization on GCP, 2026-11-12](benchmarks/rfc-026-v8-sliding-initialize-gcp-20261112.md)
- [RFC 026 V8 sliding fallocate full matrix on GCP, 2026-11-12](benchmarks/rfc-026-v8-sliding-fallocate-matrix-gcp-20261112.md)
- [RFC 026 V8 CQE-to-reaper timeline on GCP, 2026-11-12](benchmarks/rfc-026-v8-cqe-reaper-gcp-20261112.md)
- [RFC 026 V8 ext4 extent preparation on GCP, 2026-11-12](benchmarks/rfc-026-v8-ext4-extent-preparation-gcp-20261112.md)
- [RFC 026 V8 bounded extent preparation on GCP, 2026-11-12](benchmarks/rfc-026-v8-bounded-extent-gcp-20261112.md)
- [RFC 026 V7/V8 FRONTIER and coalescing comparison on GCP, 2026-11-13](benchmarks/rfc-026-v7-v8-frontier-gcp-20261113.md)
- [RFC 026 V7/V8 Point CPU and scheduler diagnosis on GCP, 2026-11-13](benchmarks/rfc-026-v7-v8-point-perf-sched-gcp-20261113.md)
- [RFC 026 V7/V8 Point durability phase comparison on GCP, 2026-11-15](benchmarks/rfc-026-v7-v8-durability-phases-gcp-20261115.md)
- [RFC 026 V7/V8 durability critical-path test on GCP, 2026-11-15](benchmarks/rfc-026-v7-v8-durability-critical-path-gcp-20261115.md)
- [RFC 026 V7/V8 sync-descriptor and fdatasync diagnostic on GCP, 2026-11-16](benchmarks/rfc-026-v7-v8-sync-fd-gcp-20261116.md)
- [RFC 026 V8 unwritten-extent and JBD2 diagnostic on GCP, 2026-11-16](benchmarks/rfc-026-v8-unwritten-extent-jbd2-gcp-20261116.md)
- [RFC 026 V8 Point-only Lookahead comparison on GCP, 2026-11-17](benchmarks/rfc-026-v8-point-only-lookahead-gcp-20261117.md)
- [RFC 026 V8 Point FRONTIER coalescing comparison on GCP, 2026-11-17](benchmarks/rfc-026-v8-frontier-coalescing-gcp-20261117.md)
- [RFC 026 V8 Point Lookahead Buffered vs Direct FRONTIER on GCP, 2026-11-17](benchmarks/rfc-026-v8-lookahead-frontier-mode-gcp-20261117.md)
- [RFC 026 V7/V8 Point FRONTIER qualification rerun on GCP, 2026-11-17](benchmarks/rfc-026-v7-v8-point-frontier-qualification-gcp-20261117.md)
- [RFC 026 V7/V8 cross-host Point FRONTIER qualification on GCP, 2026-11-17](benchmarks/rfc-026-v7-v8-cross-host-point-frontier-gcp-20261117.md)
- [RFC 026 V7/V8 Point durability attribution on GCP, 2026-11-17](benchmarks/rfc-026-v7-v8-point-durability-attribution-gcp-20261117.md)
- [RFC 026 V7/V8 Point thread-sample reanalysis on GCP, 2026-11-17](benchmarks/rfc-026-v7-v8-point-thread-sample-reanalysis-gcp-20261117.md)
- [RFC 026 V8 pre-publish retirement lock-scope A/B on GCP, 2026-11-17](benchmarks/rfc-026-v8-retirement-lock-scope-gcp-20261117.md)
- [RFC 026 V8 Point retirement credit batching A/B on GCP, 2026-11-17](benchmarks/rfc-026-v8-point-retirement-credit-batching-gcp-20261117.md)
- [RFC 026 V8 Lookahead extent-policy scan diagnostic on GCP, 2026-11-18](benchmarks/rfc-026-v8-lookahead-policy-scan-gcp-20261118.md)
- [RFC 026 V8 extent-policy Vec A/B on GCP, 2026-11-18](benchmarks/rfc-026-v8-extent-policy-vec-ab-gcp-20261118.md)
- [RFC 026 V7/V8 Point Producer critical-path diagnosis on GCP, 2026-11-19](benchmarks/rfc-026-v7-v8-point-producer-critical-path-gcp-20261119.md)
- [RFC 026 V7/V8 Point DATA submission and HOL timeline rerun on GCP, 2026-11-19](benchmarks/rfc-026-v7-v8-point-data-completion-hol-rerun-gcp-20261119.md)
- [RFC 026 V7/V8 Point same-ticket DATA HOL diagnosis on GCP, 2026-11-20](benchmarks/rfc-026-v7-v8-point-hol-ticket-gcp-20261120.md)
- [RFC 026 V8 Point DATA group join experiment on GCP, 2026-11-17](benchmarks/rfc-026-v8-point-data-group-gcp-20261117.md)
- [RFC 026 V8 batched durable-guard check GCP A/B, 2026-11-15](benchmarks/rfc-026-v8-durable-guard-check-gcp-20261115.md)
- [RFC 026 full WAL matrix rerun on GCP, 2026-11-15](benchmarks/rfc-026-v8-durable-guard-full-matrix-gcp-20261115.md)
- [RFC 026 V8 default configuration factorial on GCP, 2026-11-15](benchmarks/rfc-026-v8-default-config-factorial-gcp-20261115.md)
- [RFC 026 V8 sliding fallocate window comparison on GCP, 2026-11-15](benchmarks/rfc-026-v8-extent-window-gcp-20261115.md)
- [RFC 026 V8 Sliding Lookahead full-matrix comparison on GCP, 2026-11-16](benchmarks/rfc-026-v8-sliding-lookahead-full-matrix-gcp-20261116.md)
- [RFC 026 V8 durability phase sampling on GCP, 2026-11-13](benchmarks/rfc-026-v8-durability-sampling-gcp-20261113.md)
- [RFC 026 V7/V8 Point MVCC publication and sampler overhead on GCP, 2026-11-14](benchmarks/rfc-026-v7-v8-mvcc-publication-gcp-20261114.md)
- [RFC 026 V7/V8 Point MVCC publication path diagnostics on GCP, 2026-11-14](benchmarks/rfc-026-v7-v8-mvcc-publication-paths-gcp-20261114.md)
- [RFC 026 V7/V8 Point commit-order trace on GCP, 2026-11-14](benchmarks/rfc-026-v7-v8-commit-order-trace-gcp-20261114.md)
- [RFC 026 V8 coalescing budget sweep on GCP, 2026-11-14](benchmarks/rfc-026-v8-coalescing-sweep-gcp-20261114.md)
- [RFC 026 V8 0 vs 400 μs coalescing confirmation with V7 control on GCP, 2026-11-15](benchmarks/rfc-026-v7-v8-coalescing-confirmation-gcp-20261115.md)
- [RFC 026 V7/V8 Point critical-path comparison on GCP, 2026-11-15](benchmarks/rfc-026-v7-v8-point-critical-path-gcp-20261115.md)
- [RFC 026 V8 DATA submission-order probe on GCP, 2026-11-11](benchmarks/rfc-026-v8-data-submit-order-gcp-20261111.md)
- [RFC 026 V8 coalescing budget A/B on GCP, 2026-11-11](benchmarks/rfc-026-v8-coalescing-budget-gcp-20261111.md)
- [RFC 026 V8 low-overhead coalescing confirmation on GCP, 2026-11-11](benchmarks/rfc-026-v8-low-overhead-budget-gcp-20261111.md)
- [RFC 026 V7 vs V8 Direct/400 μs on GCP, 2026-11-11](benchmarks/rfc-026-v7-v8-direct400-gcp-20261111.md)
- [Held-out WAL latency calibration method](benchmarks/wal-state-calibration-method.md)
- [RFC 026 held-out calibration: no qualified V7/V8 ratio, 2026-11-03](benchmarks/rfc-026-v8-calibrated-states-20261103.md)
- [WAL extent attribution and calibration observations](benchmarks/wal-extent-attribution-method.md)
- [RFC 026 extent-aware traces: DATA coverage and intervening FUA, 2026-11-03](benchmarks/rfc-026-v8-wal-extents-20261103.md)
- [Passive WAL trace regimes and monotonic metric joins](benchmarks/passive-wal-trace-method.md)
- [RFC 026 passive traces: clean capture, insufficient classification coverage, 2026-11-03](benchmarks/rfc-026-v8-passive-regimes-20261103.md)
- [Paired V7/V8 diagnostic method and storage-state classification](benchmarks/paired-v7-v8-method.md)
- [RFC 026 independent pairs and bounded stable-state admission, 2026-11-03](benchmarks/rfc-026-v8-independent-pairs-20261103.md)
- [RFC 026 paired V7/V8 probes: both blocks transition, 2026-11-03](benchmarks/rfc-026-v8-state-paired-20261103.md)
- [RFC 026 P-core CPU profiles and allocator screen (not retained), 2026-11-03](benchmarks/rfc-026-v8-pcore-cpu-20261103.md)
- [RFC 026 V8 background-role and kernel-worker affinity (inconclusive), 2026-11-02](benchmarks/rfc-026-v8-background-affinity-20261102.md)
- [RFC 026 writer-only versus whole-process P-core affinity, 2026-11-02](benchmarks/rfc-026-v8-writer-affinity-20261102.md)
- [RFC 026 V7/V8 P-core affinity comparison and V7 rerun, 2026-11-02](benchmarks/rfc-026-v8-pcore-affinity-20261102.md)
- [RFC 026 producer preparation attribution and hybrid-core hash costs, 2026-11-02](benchmarks/rfc-026-v8-producer-attribution-20261102.md)
- [RFC 026 V8 notification retention and unchanged-binary rerun, 2026-11-02](benchmarks/rfc-026-v8-notification-rerun-20261102.md)
- [RFC 026 V8 notification separation (combined candidate retained by maintainer), 2026-11-02](benchmarks/rfc-026-v8-notification-split-20261102.md)
- [RFC 026 V8/V7 timed CPU attribution and rejected atomic-status prototype, 2026-11-02](benchmarks/rfc-026-v8-coordination-attribution-20261102.md)
- [RFC 026 V8 copy and publication screens (not retained), 2026-11-02](benchmarks/rfc-026-v8-copy-publication-20261102.md)
- [RFC 025 point-write handoff diagnosis, 2026-10-29](benchmarks/rfc-025-v7-point-handoff-20261029.md)
- [RFC 025 ordinary v7 completion notification (inconclusive; batch16 has no matched pairs), 2026-10-29](benchmarks/rfc-025-v7-signal-taskwork-20261029.md)
- [RFC 025 ordinary v7 notification retry (inconclusive; two matched batch16 pairs), 2026-10-29](benchmarks/rfc-025-v7-signal-taskwork-retry-20261029.md)
- [RFC 025 full ordinary-notification rerun, assessed using best scores, 2026-10-29](benchmarks/rfc-025-v7-signal-taskwork-rerun-20261029.md)
- [RFC 025 v4/v7 batch attribution: serialized framing/hash CPU, 2026-10-29](benchmarks/rfc-025-v7-batch-attribution-20261029.md)
- [RFC 025 contiguous entry CRC (not retained; batch8 throughput falls), 2026-10-29](benchmarks/rfc-025-v7-entry-crc-20261029.md)
- [RFC 025 post-submit SHA overlap (not retained alone; concurrent batch checks fail), 2026-10-29](benchmarks/rfc-025-v7-post-submit-hash-20261029.md)
- [RFC 025 producer framing plus SHA overlap (not retained; point guards fail), 2026-10-29](benchmarks/rfc-025-v7-producer-framing-20261029.md)
- [RFC 025 physical DATA CRC from source components (not retained; SSD qualification incomplete), 2026-10-29](benchmarks/rfc-025-v7-frame-source-crc-20261029.md)
- [RFC 025 admission-driven producer framing (not retained; batch grouping remains fragmented), 2026-10-29](benchmarks/rfc-025-v7-admission-grouping-20261029.md)
- [RFC 025 admitted-ticket grouping (not retained; eight-writer SSD qualification inconclusive), 2026-10-29](benchmarks/rfc-025-v7-admitted-group-20261029.md)
- [RFC 025 admitted-ticket SSD rerun (inconclusive; all writer counts under-sampled), 2026-10-29](benchmarks/rfc-025-v7-ssd-rerun-20261029.md)
- [RFC 025 dynamic admitted-window grouping (experimental; performance unqualified), 2026-10-30](benchmarks/rfc-025-v7-dynamic-window-20261030.md)
- [RFC 025 completion priority for small writes (not retained; independent p99 benefit fails to repeat), 2026-10-28](benchmarks/rfc-025-v7-small-completion-priority-20261028.md)
- [RFC 025 ready-completion-first scheduling (not retained; batch p99 checks fail), 2026-10-28](benchmarks/rfc-025-v7-ready-completion-first-20261028.md)
- [RFC 025 unused-suffix preparation after large DATA (not retained; extra writes fail SSD batch checks), 2026-10-28](benchmarks/rfc-025-v7-large-data-lookahead-20261028.md)
- [RFC 025 sync priority over optional extent initialization (not retained; fixed SSD checks fail), 2026-10-28](benchmarks/rfc-025-v7-sync-priority-extents-20261028.md)
- [RFC 025 unbounded kernel worker limit (not retained; eight-writer and solo guards fail), 2026-10-28](benchmarks/rfc-025-v7-unbounded-workers-20261028.md)
- [RFC 025 kernel worker pool limits (rejected; all four SSD workloads measured), 2026-10-28](benchmarks/rfc-025-v7-async-pool-limits-20261028.md)
- [RFC 025 async dispatch for large v7 DATA (retained provisionally; confirmation pending), 2026-10-27 UTC](benchmarks/rfc-025-v7-large-data-async-dispatch-20261027.md)
- [RFC 025 large-wave FRONTIER allocation (not retained; filtered screen misses throughput/p99), 2026-10-27](benchmarks/rfc-025-v7-large-frontier-allocation-20261027.md)
- [Current v4/v7 Parallel SSD comparison (stall-limited pairs and same-executable peaks), 2026-10-27](benchmarks/rfc-025-v7-current-v4-parallel-20261027.md)
- [RFC 025 coalesce eligible v7 prefixes (retained for review; all four guards pass), 2026-10-27](benchmarks/rfc-025-v7-eligible-coalescing-20261027.md)
- [RFC 025 one coalescing deadline across retries (not retained; eight-writer throughput loss), 2026-10-27](benchmarks/rfc-025-v7-coalescing-deadline-20261027.md)
- [RFC 025 sixteen-writer tail: separated stalls and repeated coalescing, 2026-10-27](benchmarks/rfc-025-v7-16-writer-tail-diagnosis-20261027.md)
- [RFC 025 coordinator FRONTIER-buffer reuse (not retained; storage variation), 2026-10-27](benchmarks/rfc-025-v7-frontier-buffer-reuse-20261027.md)
- [RFC 025 batch geometry at worker retirement (rejected; existing optimization kept), 2026-10-27](benchmarks/rfc-025-v7-batch-retirement-hint-20261027.md)
- [RFC 025 compact batch geometry and wait-cost budget (rejected), 2026-10-27](benchmarks/rfc-025-v7-batch-wait-budget-20261027.md)
- [RFC 025 packed-batch membership diagnosis and initial prototype (rejected), 2026-10-27](benchmarks/rfc-025-v7-batch-membership-20261027.md)
- [RFC 025 batch-window remaining SSD tests (provisionally retained; solo unresolved), 2026-10-27](benchmarks/rfc-025-v7-ssd-batch-window-followup-20261027.md)
- [RFC 025 SSD extent lookahead and initial batch-coalescing screen, 2026-10-27](benchmarks/rfc-025-v7-ssd-batch-and-extent-20261027.md)
- [RFC 025 BBTree memtable trial (CPU screen failed; SSD tests cancelled), 2026-10-27](benchmarks/rfc-025-bbtree-memtable-20261027.md)
- [RFC 025 allocator retry after SSD improvements (not adopted), 2026-10-27](benchmarks/rfc-025-v7-mimalloc-ssd-followup-20261027.md)
- [RFC 025 serial extent scheduling fix and 16-writer SSD results (local review; full gate failed), 2026-10-27](benchmarks/rfc-025-v7-serial-preparation-20261027.md)
- [RFC 025 asynchronous v7 extent preparation (maintainer-retained; serial p99 follow-up), 2026-10-26–27](benchmarks/rfc-025-v7-async-extent-20261026.md)
- [RFC 025 vectored v7 DATA submissions (rejected), 2026-10-26](benchmarks/rfc-025-v7-vectored-data-20261026.md)
- [RFC 025 linked FRONTIER write/sync and task-work diagnostic (rejected), 2026-10-26](benchmarks/rfc-025-v7-frontier-link-20261026.md)
- [RFC 025 larger v7 extent-initialization writes (rejected), 2026-10-26](benchmarks/rfc-025-v7-extent-io-20261026.md)
- [RFC 025 System versus mimalloc allocator trials (rejected), 2026-10-26](benchmarks/rfc-025-v7-mimalloc-20261026.md)
- [RFC 025 v7 DATA packer refill policies (rejected), 2026-10-26](benchmarks/rfc-025-v7-packer-refill-20261026.md)
- [RFC 025 buffered v7 I/O experiments and audited SSD rerun (rejected), 2026-10-26](benchmarks/rfc-025-v7-buffered-io-ssd-20261026.md)
- [RFC 025 requested SSD rerun with full-cycle timing enabled, 2026-10-26](benchmarks/rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026.md)
- [RFC 025 full v7 durability-cycle feedback (rejected), 2026-10-26](benchmarks/rfc-025-v7-durability-cycle-cost-20261026.md)
- [RFC 025 precomputed v7 group hints and pooled SSD confirmation (rejected), 2026-10-26](benchmarks/rfc-025-v7-large-group-hint-cache-20261026.md)
- [RFC 025 fixed v7 hash-stage experiments (rejected), 2026-10-26](benchmarks/rfc-025-v7-prefix-hash-overlap-20261026.md)

- [RFC 025 v7 critical-path diagnosis and rejected waiting policy, 2026-10-26](benchmarks/rfc-025-v7-critical-path-20261026.md)

- [RFC 025 borrowed v7 producer preparation (rejected), 2026-10-26](benchmarks/rfc-025-v7-borrowed-preparation-20261026.md)

- [RFC 025 user-requested ext4 batch64 rerun, 2026-10-26](benchmarks/rfc-025-v7-maintenance-batch-rerun-20261026.md)

- [RFC 025 v7 maintenance-state clone experiment (rejected), 2026-10-25–26](benchmarks/rfc-025-v7-maintenance-snapshot-20261025.md)

- [RFC 025 immutable v7 logical batches and repeated comparisons, 2026-10-25](benchmarks/rfc-025-v7-validated-logical-batch-20261025.md)

- [RFC 025 v7 preparation and bounded batching experiments, 2026-10-25](benchmarks/rfc-025-v7-preparation-and-batching-20261025.md)

- [RFC 025 v7 FRONTIER encoding outside admission, 2026-10-25](benchmarks/rfc-025-v7-frontier-lock-scope-20261025.md)
- [RFC 025 v7 ticket-boundary experiments (rejected), 2026-10-25](benchmarks/rfc-025-v7-ticket-boundary-experiments-20261025.md)
- [RFC 025 v7 cached extent readiness and cooled ext4 comparisons, 2026-10-25](benchmarks/rfc-025-v7-extent-readiness-cache-20261025.md)
- [RFC 025 v7 ordered encoding outside admission, 2026-10-25](benchmarks/rfc-025-v7-packer-encoding-20261025.md)
- [RFC 025 v7 resource-retirement experiments (rejected), 2026-10-25](benchmarks/rfc-025-v7-resource-retirement-20261025.md)
- [RFC 025 v7 shared extent initialization and highest ext4 scores, 2026-10-25](benchmarks/rfc-025-v7-parallel-vs-v4-parallel-writer-sweep-20261025.md#shared-v7-extent-initialization-on-ext4)
- [RFC 025 v7 preparation before the MVCC sequencer, 2026-10-25](benchmarks/rfc-025-v7-parallel-vs-v4-parallel-writer-sweep-20261025.md#mvcc-preparation-before-the-sequencer)
- [RFC 025 v7 physical digest hot-path optimization, 2026-10-25](benchmarks/rfc-025-v7-parallel-vs-v4-parallel-writer-sweep-20261025.md#physical-digest-and-prefix-finalization-follow-up)
- [RFC 025 v7 Parallel versus v4 Parallel writer sweep, 2026-10-25](benchmarks/rfc-025-v7-parallel-vs-v4-parallel-writer-sweep-20261025.md)
- [RFC 025 v7 Parallel versus baseline v6 Leader exploratory screen, 2026-10-25](benchmarks/rfc-025-v7-parallel-vs-baseline-v6-20261025.md)
- [RFC 025 v7 Leader versus v6 Leader exploratory screen, 2026-10-25](benchmarks/rfc-025-v7-leader-vs-v6-leader-20261025.md)
- [RFC 025 v7 Parallel versus v7 Leader exploratory screen, 2026-10-25](benchmarks/rfc-025-v7-parallel-vs-leader-20261025.md)
- [RFC 025 v7 Parallel versus v4 Parallel exploratory screen, 2026-10-24](benchmarks/rfc-025-v7-parallel-vs-v4-20261024.md)
- [RFC 025 v4 Parallel first-leg exploratory screen, 2026-10-24](benchmarks/rfc-025-v4-parallel-first-leg-20261024.md)
- [Parallel WAL benchmark outcomes](benchmarks/rfc-024-parallel-wal-benchmark.md)
- [Batch64 parallel WAL versus Leader rerun, 2026-10-06](benchmarks/rfc-024-native-batch64-leader-rerun-20261006.md)
- [Native async parallel WAL versus leader, 2026-10-05](benchmarks/rfc-024-native-async-vs-leader-20261005.md)
- [WAL qualification matrix, 2026-10-01](benchmarks/rfc-024-wal-qualification-20261001.md)
- [Controlled ext4 comparison, 2026-10-04](benchmarks/rfc-024-controlled-comparison-20261004.md)
- [Low-concurrency parallel comparison, 2026-10-03](benchmarks/rfc-024-parallel-comparison-20261003.md)
- [WAL Baseline](benchmarks/rfc-024-wal-baseline.md)

## Environment investigations

- [P41 Plus 004C: file-based APST and verified FUA controls, 2026-11-03](environment/rfc-026-p41-plus-apst-fua-20261103.md)
- [Buffered WAL diagnostic](environment/rfc-024-buffered-wal-diagnostic-20261004.md)
- [Physical SSD environment mitigation attempts, 2026-10-04](environment/rfc-024-environment-mitigations-20261004.md)
- [Investigation of unstable ext4 measurements](environment/rfc-024-wal-stall-diagnosis.md)

## Optimization experiments

These reports include retained changes, rejected candidates and diagnostic
experiments. Each report records its own outcome and measurement limits.

- [Batch allocation follow-up, 2026-10-04](experiments/rfc-024-batch-allocation-followup-20261004.md)
- [Batch preparation follow-up, 2026-10-04](experiments/rfc-024-batch-preparation-followup-20261004.md)
- [Batch serialization follow-up, 2026-10-04](experiments/rfc-024-batch-serialization-followup-20261004.md)
- [Ext4 batch tail — attribution and the gate design](experiments/rfc-024-batch-tail-attribution-20261002.md)
- [Batch64 publication and packing follow-up, 2026-10-04](experiments/rfc-024-batch64-publication-and-packing-20261004.md)
- [Background-I/O gate — measured, rejected](experiments/rfc-024-bg-io-gate-20261002.md)
- [Context switches and worker retirement, 2026-10-04](experiments/rfc-024-context-switch-and-worker-retirement-20261004.md)
- [RFC 026 Point borrowed-input experiment — no stable gain, 2026-11-18](experiments/rfc-026-point-borrowed-input-gcp-20261118.md)
- [RFC 026 V8 Point single-frame direct encoding GCP qualification — below throughput gate, 2026-11-18](experiments/rfc-026-point-direct-frame-v1-20261118.md)
- [RFC 027 V8 Point Progress Ring GCP qualification — promotion gates failed, 2026-11-21](benchmarks/rfc-027-v8-point-progress-ring-gcp-20261121.md)
- [RFC 027 V8 Point Progress Cursor GCP qualification — promotion gates failed, 2026-11-22](benchmarks/rfc-027-v8-point-progress-cursor-gcp-20261122.md)
- [RFC 027 V8 Cursor direct-completion GCP qualification — Point16 gate failed, 2026-11-22](benchmarks/rfc-027-v8-cursor-direct-completion-gcp-20261122.md)
- [RFC 027 V8 Cursor singleton-submission GCP qualification — throughput gate failed, 2026-11-22](benchmarks/rfc-027-v8-cursor-singleton-submission-gcp-20261122.md)
- [RFC 028 V8 Point critical-path GCP diagnostic — no single bottleneck established, 2026-11-22](benchmarks/rfc-028-v8-point16-critical-path-gcp-20261122.md)
- [RFC 028 V8 Point D1 ACK-check GCP A/B — no stable throughput gain, 2026-11-24](benchmarks/rfc-028-v8-point-ack-d1-gcp-20261124.md)
- [RFC 028 V8 Dedicated Durability Driver D3a GCP A/B — no broad throughput win, 2026-11-24](benchmarks/rfc-028-v8-dedicated-durability-driver-gcp-20261124.md)
- [RFC 028 V8 Independent ACK Waiter D3b GCP A/B — no throughput win, 2026-11-25](benchmarks/rfc-028-v8-independent-ack-gcp-20261125.md)
- [RFC 028 V8 Cursor Direct Completion Wake Token/Select GCP A/B — no throughput win, 2026-11-25](benchmarks/rfc-028-v8-cursor-direct-completion-wake-gcp-20261125.md)
- [RFC 028 V8 cursor publication D2a/D2b historical GCP A/B — baseline was not a full pre-D2 restore, 2026-11-24](benchmarks/rfc-028-v8-cursor-publication-d2-gcp-20261124.md)
- [RFC 028 V8 cursor publication D2a requalification — below throughput gate, 2026-11-24](benchmarks/rfc-028-v8-cursor-publication-d2a-gcp-requalification-20261124.md)
- [RFC 028 V8 Cursor coalescing GCP diagnostic — progress-token wait dominates sampled coalescing wall time, 2026-11-23](benchmarks/rfc-028-v8-cursor-coalescing-gcp-20261123.md)
- [RFC 028 V8 Cursor coalescing-budget A/B — fixed 400 μs wins consistently but misses the throughput gate, 2026-11-23](benchmarks/rfc-028-v8-cursor-coalescing-budget-gcp-20261123.md)
- [RFC 028 V8 Cursor coalescing ready-target A/B — Point16 Target8 gain, overall gate failed, 2026-11-25](benchmarks/rfc-028-v8-cursor-coalescing-ready-target-gcp-20261125.md)
- [RFC 028 V8 Point buffer preallocation GCP A/B — Point8 +9.59%, Point16 +3.88%, 2026-11-23](benchmarks/rfc-028-v8-point-buffer-preallocation-gcp-20261123.md)
- [RFC 028 V7/V8 Point buffer preallocation same-host qualification — Point8 +9.78%, Point16 +4.15%, 2026-11-23](benchmarks/rfc-028-v7-v8-point-preallocation-same-host-gcp-20261123.md)
- [RFC 028 V8 Point resource preclaim Stage B A/B — throughput gate failed, 2026-11-23](benchmarks/rfc-028-v8-point-resource-preclaim-stage-b-gcp-20261123.md)
- [RFC 026 V7/V8 Point admission and sync diagnostic — sequencer wait is real but not demonstrably saturated, 2026-11-19](experiments/rfc-026-v8-point-admission-sync-diagnostic-20261119.md)
- [Deferred task work with native completion waiting](experiments/rfc-024-deferred-ring-wait-20261003.md)
- [Extent preparation for large batches](experiments/rfc-024-large-batch-preallocation-20261004.md)
- [Native forced-async kernel dispatch, 2026-10-06](experiments/rfc-024-native-async-kernel-dispatch-20261006.md)
- [Cooperative WAL durability and publication experiment](experiments/rfc-024-native-async-waits-20261005.md)
- [Native async batch ownership follow-up, 2026-10-05](experiments/rfc-024-native-batch-ownership-followup-20261005.md)
- [Native async batch pooling follow-up](experiments/rfc-024-native-batch-pooling-20261006.md)
- [Native batch pooling root-cause investigation](experiments/rfc-024-native-batch-pooling-cause-20261006.md)
- [Native commit waits and completion drain, 2026-10-06](experiments/rfc-024-native-completion-drain-20261006.md)
- [Native WAL coalescing cutoff refresh, 2026-10-06](experiments/rfc-024-native-cutoff-refresh-20261006.md)
- [Native durability wakeups, 2026-10-06](experiments/rfc-024-native-durable-wake-20261006.md)
- [Native skip-list metadata padding screen](experiments/rfc-024-native-metadata-padding-20261006.md)
- [Native submission and key preparation, 2026-10-06](experiments/rfc-024-native-submission-and-key-preparation-20261006.md)
- [Native sync-batching limit, 2026-10-06](experiments/rfc-024-native-sync-batching-20261006.md)
- [Publication cache experiments, 2026-10-01](experiments/rfc-024-publication-cache-experiments-20261001.md)
- [Progress-aware publication waiting](experiments/rfc-024-publication-progress-20261003.md)
- [Cooperative publication wait](experiments/rfc-024-publication-yield-20261002.md)
- [Ring command and completion waiting](experiments/rfc-024-ring-command-wait-20261003.md)
- [Sync cutoff regression investigation, 2026-10-03](experiments/rfc-024-sync-cutoff-investigation-20261003.md)
- [Synchronous `pwritev` group submission — rejected](experiments/rfc-024-sync-submit-qualification-20261002.md)
- [Tokio client experiment, 2026-10-04](experiments/rfc-024-tokio-client-experiment-20261004.md)
- [1-writer batch64 tail and pipeline-hop measurements](experiments/rfc-024-w1-batch64-p99-hops-20261002.md)
- [Release write ordering before producer packing](experiments/rfc-024-wal-admission-handoff-20261002.md)
- [WAL optimization follow-up, 2026-10-01](experiments/rfc-024-wal-optimization-followup-20261001.md)
- [Release-mode worker invariant bookkeeping](experiments/rfc-024-worker-invariants-20261003.md)

[All documentation](../README.md).
