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
| [Plans and specs](superpowers/) | Focused implementation plans and design specifications. |
| [Assets](assets/) | Documentation images. |

Start with the [WAL documentation index](wal/README.md) for the current
parallel WAL policy and its measured results.

## Benchmark reports

- [Crud-bench: ToyKV vs Fjall (matched config)](benchmarks/bench-report-crud-bench-fjall.md)
- [Crud-bench: ToyKV vs RocksDB](benchmarks/bench-report-crud-bench-rocksdb.md)
- [DeleteRange Performance Report](benchmarks/bench-report-deleterange.md)
- [VLog Performance Benchmark Report](benchmarks/bench-report-vlog.md)
- [Io_uring Benchmark Results](benchmarks/io-uring-bench.md)
- [Performance Profiling Report](benchmarks/perf-profile.md)

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
