# V8 Point ticket lookup A/B on GCP

**Decision: reject the O(1) lookup candidate and revert the runtime change.** It did not meet the 2% minimum stop threshold: Point16's paired median was +0.21% with 3/5 wins, and Point8 was −0.81% with 1/5 wins. The Point16 p99 samples were noisy and did not show a consistent improvement.

## Method

One `c4-standard-16-lssd` VM in `us-central1-a`, with two 375 GB NVMe Local SSDs in ext4 RAID0 (`rw,noatime,stripe=256`). The VM used the repository-pinned `nightly-2026-09-23` toolchain. Baseline and candidate release binaries were built on that same host with `--features bench`, from source snapshots whose only difference was `kv-engine/src/wal/v8/parallel.rs`.

The candidate replaced ticket searches in `mark_digest_ready`, `mark_frames_ready`, `mark_data_complete`, and `mark_data_group_complete` with deque-relative indexing from `durable_ticket_end`. It also changed the completion-group loops to use direct indexed access rather than iterator `skip(index)`. A correctness test exercised indexing after durable-prefix removal. The candidate passed the 128-test V8 WAL module suite, including recovery and failpoint cases, before the performance run.

The clean score suite contained five counterbalanced pairs for each workload: Point8, Point16, and Batch64 at 16 writers. Each run issued 100,000 logical puts with 1 KiB values. Both arms used V8 Parallel, Direct FRONTIER, 1 MiB Point-only Lookahead, dynamic coalescing, and disabled Point direct-frame encoding. Batch64 followed the existing Fallocate-only classification. Latency was sampled every 100 operations. `perf`, CPU attribution, sync diagnostics, and completion timelines were disabled; the bench-only extent-policy scan counter was explicitly set to `0` and validated as disabled in every result.

## Results

Throughput is the median of five runs per arm. Paired change is the median of the five within-pair `(candidate / baseline) - 1` values.

| Workload | Baseline (k puts/s) | O(1) candidate (k puts/s) | Paired change | Candidate wins | CPU/put, baseline → candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Point8 | 32.220 | 32.083 | −0.81% | 1/5 | 51.34 → 51.55 μs |
| Point16 | 41.581 | 41.945 | +0.21% | 3/5 | 77.09 → 76.89 μs |
| Batch64, 16 writers | 425.177 | 421.787 | −0.31% | 2/5 | 4.89 → 4.88 μs |

| Point latency paired change | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Point8 | +0.61% | −2.06% | −13.16% |
| Point16 | +2.07% | −3.05% | +0.83% |

Negative latency changes favor the candidate. Point16 p50 was worse in all five pairs. Its p99 was lower in only two pairs and varied widely, including 4.004 ms for the candidate versus 0.973 ms for baseline in one pair. Batch64 produced only 16 sampled latency values per run, so its p99 is not useful for comparison.

The change did not materially alter durability grouping. Median Point8 syncs were 13,001 → 13,031, with puts/sync 7.69 → 7.67. Point16 syncs were 9,907 → 9,948, with puts/sync 10.09 → 10.05. Batch64 throughput had one −7.94% pair, but its paired median was −0.31%; these five pairs do not establish a systematic Batch64 regression.

The Point results miss the required ≥3% stable gain and fall below the 2% rejection boundary. The runtime candidate was therefore reverted; no O(1) ticket lookup change remains in the branch. The measurements do not justify extending this micro-optimization.

## Reproducibility and cleanup

- Baseline source commit: `e74469ed`.
- Candidate patch SHA-256: `3f681a045ab4a9af72de23cfbeaab775a573f4ed91e89279c9b411b5421fc523`.
- Baseline binary SHA-256: `d84d5dc5b3decf1815f89b3620e9c49a02f1f7b360047b84695e2d57f87c2e4a`.
- Candidate binary SHA-256: `7bdb99cddc6385d99cbfbb7c3d7475cf02cac7cd5ea25282e8d97f9d621eb8ad`.
- All 30 clean planned runs completed successfully. The archive includes the plan, each `result.json` and matching `run-metadata.json`, binaries, source snapshots, build logs, and I/O metadata. Its local path is `/tmp/gcp-v8-ticket-index/ticket-index-ab-clean-20261121.tar.gz`; SHA-256: `7944e38c7f58685a0dd79c74e6830ebd69fce7843ff8d4f0ac56a4059963b558`.
- An earlier 30-run attempt had the bench-only extent-policy counter enabled by default and was excluded from the decision. It is retained separately at `/tmp/gcp-v8-ticket-index/ticket-index-ab-20261121.tar.gz` (SHA-256 `e7e2ad8ea779a9cab5d4015b2046f564103e00d621e400f278d6a06aeeed6ac5`). The clean suite explicitly disabled this counter.
- Terraform destroyed all five temporary resources. Project-scoped inventory checks confirmed the VM, boot disk, firewall, subnet, VPC, and route were absent. Billing settings were unchanged.
