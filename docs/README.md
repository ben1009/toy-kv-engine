# Documentation

Reports and implementation notes are grouped by topic. Feature designs live
in the [RFC directory](../rfcs/).

| Folder | Contents |
| --- | --- |
| [WAL](wal/README.md) | Parallel WAL implementation, adoption, benchmarks, experiments and environment investigations. |
| [Benchmarks](benchmarks/) | Cross-engine comparisons, vLog, range deletion, io_uring and profiling reports. |
| [Backup](backup/) | Incremental backup implementation and benchmarks. |
| [PITR](pitr/) | Point-in-time recovery implementation and performance. |
| [Async](async/) | Async lifecycle measurement and scan findings. |
| [GCP test environment](../infra/gcp/test-environment/README.md) | Terraform setup for a reusable GCP test host with Local SSD and quota preflight. |
| [Plans and specs](superpowers/) | Focused implementation plans and design specifications. |
| [Assets](assets/) | Documentation images. |

Start with the [WAL documentation index](wal/README.md) for the current
parallel WAL policy and its measured results. Ordinary WALs use V4 Parallel;
new PITR WALs use V8 Parallel, with V6/V7 available as explicit compatibility
preferences. The V8 default is a maintainer adoption decision while performance
qualification remains open. Dated reports preserve their measured revisions
and do not define the current writer selection.

## WAL implementation

- [V8 PITR WAL default adoption](wal/rfc-026-v8-default-20261103.md)
- [RFC 024: Parallel WAL Implementation Plan](wal/rfc-024-parallel-wal-implementation-plan.md)
- [RFC 025 Implementation Plan: Parallel WAL with Embedded Frontiers and One Sync](wal/rfc-025-parallel-wal-embedded-frontiers-implementation-plan.md)
- [RFC 026 Implementation Plan: Composable WAL Prefix Digests](wal/rfc-026-implementation-plan.md)
- [V8 Point Progress Ring proposal](wal/rfc-026-v8-point-fast-lane.md)

## Benchmark reports

- [Crud-bench: ToyKV vs Fjall (matched config)](benchmarks/bench-report-crud-bench-fjall.md)
- [Crud-bench: ToyKV vs RocksDB](benchmarks/bench-report-crud-bench-rocksdb.md)
- [DeleteRange Performance Report](benchmarks/bench-report-deleterange.md)
- [VLog Performance Benchmark Report](benchmarks/bench-report-vlog.md)
- [Io_uring Benchmark Results](benchmarks/io-uring-bench.md)
- [Performance Profiling Report](benchmarks/perf-profile.md)
- [GCP Free Trial WAL benchmark runner](wal/benchmarks/gcp-free-trial-wal-bench.md)
- [GCP V8 point sync diagnostic, 2026-11-10](wal/benchmarks/rfc-026-v8-point-sync-gcp-20261110.md)
- [GCP V8 FRONTIER I/O A/B, 2026-11-11](wal/benchmarks/rfc-026-v8-frontier-io-ab-gcp-20261111.md)
- [GCP V7/V8 CPU affinity, 2026-11-11](wal/benchmarks/rfc-026-v7-v8-cpu-affinity-gcp-20261111.md)
- [GCP V8 ordering HOL diagnostics, 2026-11-12](wal/benchmarks/rfc-026-v8-ordering-hol-gcp-20261112.md)
- [GCP V7/V8 Point CPU and scheduler diagnosis, 2026-11-13](wal/benchmarks/rfc-026-v7-v8-point-perf-sched-gcp-20261113.md)
- [GCP V7/V8 Point durability phase comparison, 2026-11-15](wal/benchmarks/rfc-026-v7-v8-durability-phases-gcp-20261115.md)
- [GCP V7/V8 durability critical-path test, 2026-11-15](wal/benchmarks/rfc-026-v7-v8-durability-critical-path-gcp-20261115.md)
- [GCP V7/V8 sync-descriptor and fdatasync diagnostic, 2026-11-16](wal/benchmarks/rfc-026-v7-v8-sync-fd-gcp-20261116.md)
- [GCP V8 unwritten-extent and JBD2 diagnostic, 2026-11-16](wal/benchmarks/rfc-026-v8-unwritten-extent-jbd2-gcp-20261116.md)
- [GCP V7/V8 cross-host Point FRONTIER qualification, 2026-11-17](wal/benchmarks/rfc-026-v7-v8-cross-host-point-frontier-gcp-20261117.md)
- [GCP V7/V8 Point durability attribution, 2026-11-17](wal/benchmarks/rfc-026-v7-v8-point-durability-attribution-gcp-20261117.md)
- [GCP V7/V8 Point thread-sample reanalysis, 2026-11-17](wal/benchmarks/rfc-026-v7-v8-point-thread-sample-reanalysis-gcp-20261117.md)
- [GCP V8 pre-publish retirement lock-scope A/B, 2026-11-17](wal/benchmarks/rfc-026-v8-retirement-lock-scope-gcp-20261117.md)
- [GCP V8 Point retirement credit batching A/B, 2026-11-17](wal/benchmarks/rfc-026-v8-point-retirement-credit-batching-gcp-20261117.md)
- [GCP V7/V8 hot-mutex diagnostics, 2026-11-18](wal/benchmarks/rfc-026-v7-v8-hot-mutex-gcp-20261118.md)
- [GCP V7/V8 Point CPU attribution and sampler control, 2026-11-18](wal/benchmarks/rfc-026-v7-v8-point-cpu-attribution-gcp-20261118.md)
- [GCP V7/V8 Point Producer critical-path diagnosis, 2026-11-19](wal/benchmarks/rfc-026-v7-v8-point-producer-critical-path-gcp-20261119.md)
- [GCP V8 Point Admission and sequencer breakdown, 2026-11-19](wal/benchmarks/rfc-026-v8-point-admission-sequencer-gcp-20261119.md)
- [GCP V7/V8 sync decision snapshots, 2026-11-19](wal/benchmarks/rfc-026-v7-v8-sync-decision-snapshots-gcp-20261119.md)
- [GCP V7/V8 DATA completion timeline, 2026-11-19](wal/benchmarks/rfc-026-v7-v8-data-completion-timeline-gcp-20261119.md)
- [GCP V7/V8 DATA completion handoff retest, 2026-11-19](wal/benchmarks/rfc-026-v7-v8-completion-handoff-retest-gcp-20261119.md)
- [GCP V7/V8 Point DATA submission and HOL timeline rerun, 2026-11-19](wal/benchmarks/rfc-026-v7-v8-point-data-completion-hol-rerun-gcp-20261119.md)
- [GCP V7/V8 Point same-ticket DATA HOL diagnosis, 2026-11-20](wal/benchmarks/rfc-026-v7-v8-point-hol-ticket-gcp-20261120.md)
- [GCP V7/V8 Point CPU attribution retest, 2026-11-20](wal/benchmarks/rfc-026-v7-v8-point-cpu-attribution-gcp-20261120.md)
- [GCP V7/V8 cumulative writer-stage CPU, 2026-11-20](wal/benchmarks/rfc-026-v7-v8-writer-stage-cpu-gcp-20261120.md)
- [GCP V8 Point 3 μs DATA Group Join, 2026-11-20](wal/benchmarks/rfc-026-v8-point-short-group-join-gcp-20261120.md)
- [GCP V8 Point ticket lookup A/B, 2026-11-20](wal/benchmarks/rfc-026-v8-ticket-index-gcp-20261120.md)
- [GCP V8 Lookahead extent-policy scan diagnostic, 2026-11-18](wal/benchmarks/rfc-026-v8-lookahead-policy-scan-gcp-20261118.md)
- [GCP V8 extent-policy Vec A/B, 2026-11-18](wal/benchmarks/rfc-026-v8-extent-policy-vec-ab-gcp-20261118.md)
- [GCP V8 durability phase sampling, 2026-11-13](wal/benchmarks/rfc-026-v8-durability-sampling-gcp-20261113.md)
- [GCP V7/V8 Point MVCC publication and sampler overhead, 2026-11-14](wal/benchmarks/rfc-026-v7-v8-mvcc-publication-gcp-20261114.md)
- [GCP V7/V8 Point MVCC publication path diagnostics, 2026-11-14](wal/benchmarks/rfc-026-v7-v8-mvcc-publication-paths-gcp-20261114.md)
- [GCP V8 sliding extent initialization, 2026-11-12](wal/benchmarks/rfc-026-v8-sliding-initialize-gcp-20261112.md)
- [GCP V8 sliding fallocate full matrix, 2026-11-12](wal/benchmarks/rfc-026-v8-sliding-fallocate-matrix-gcp-20261112.md)
- [GCP V8 sliding fallocate window comparison, 2026-11-15](wal/benchmarks/rfc-026-v8-extent-window-gcp-20261115.md)

## Backup

- [RFC 022 Backup Benchmarks](backup/backup-benchmarks.md)
- [RFC 022 Incremental Backup Implementation Plan](backup/rfc-022-incremental-backup-plan.md)

## Point-in-time recovery

- [PITR Performance Baseline](pitr/pitr-performance.md)
- [RFC 023: Point-in-Time Recovery Implementation Plan](pitr/rfc-023-pitr-implementation-plan.md)

## Async APIs and scans

- [Async Phase 3 Measurement Plan](async/async-phase3-measurement.md)
- [Async Scan Findings](async/async-scan-findings.md)
- [Parallel Scan Findings](async/parallel-scan-findings.md)

## Focused plans and specifications

- [Reduce key cloning: design](superpowers/specs/2026-06-05-reduce-key-cloning-design.md)
- [Reduce key cloning: implementation plan](superpowers/plans/2026-06-05-reduce-key-cloning-plan.md)
