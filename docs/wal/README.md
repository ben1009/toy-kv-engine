# WAL documentation

Start with [RFC 024](../../rfcs/024-dedicated-wal-pipeline.md) and the
[default adoption note](rfc-024-parallel-wal-default-20261005.md). Ordinary
v4 WALs default to ticket-ordered parallel I/O by maintainer decision. PITR
v5/v6 and older MVCC WALs retain leader I/O; legacy unframed WALs retain
buffered I/O. The full RFC performance gate remains unqualified; the reports
retain measured revisions and failed controls.

[RFC 025](../../rfcs/025-parallel-wal-embedded-frontiers.md) proposes Parallel as
the default for future WAL formats, with narrowly documented exceptions. PITR
v7 embeds DATA/FRONTIER frames in the WAL and uses one sync per durability
advance. Active recovery validates candidate prefixes and permits fallback
above durable manifest boundaries; sealing/archive validation remains strict.
The proposal has not changed the current format defaults above.

Dated filenames identify individual measurements and follow-ups. The
[benchmark overview](benchmarks/rfc-024-parallel-wal-benchmark.md) connects
the retained and rejected experiments.

## Implementation and adoption

- [Parallel WAL default adoption](rfc-024-parallel-wal-default-20261005.md)
- [Parallel WAL Implementation Plan](rfc-024-parallel-wal-implementation-plan.md)
- [Native async point-write integration](rfc-024-native-async-integration-20261005.md)
- [Retire frozen parallel WAL runtimes](rfc-024-frozen-wal-retirement.md)

## Proposed extensions

- [RFC 025: Parallel WAL with Embedded Frontiers and One Sync](../../rfcs/025-parallel-wal-embedded-frontiers.md)

## Benchmarks and qualification

- [Parallel WAL benchmark outcomes](benchmarks/rfc-024-parallel-wal-benchmark.md)
- [Batch64 parallel WAL versus Leader rerun, 2026-10-06](benchmarks/rfc-024-native-batch64-leader-rerun-20261006.md)
- [Native async parallel WAL versus leader, 2026-10-05](benchmarks/rfc-024-native-async-vs-leader-20261005.md)
- [WAL qualification matrix, 2026-10-01](benchmarks/rfc-024-wal-qualification-20261001.md)
- [Controlled ext4 comparison, 2026-10-04](benchmarks/rfc-024-controlled-comparison-20261004.md)
- [Low-concurrency parallel comparison, 2026-10-03](benchmarks/rfc-024-parallel-comparison-20261003.md)
- [WAL Baseline](benchmarks/rfc-024-wal-baseline.md)

## Environment investigations

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
