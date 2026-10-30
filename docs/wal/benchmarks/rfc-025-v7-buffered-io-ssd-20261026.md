# RFC 025: buffered v7 I/O experiments and audited SSD rerun, 2026-10-26

**Result: neither buffering variant is retained. Both fail qualification in the fresh process-audited SSD rerun. The eleven earlier optimizations and direct-I/O source remain.**

The maintainer reported that another Rust project had been running during the first two matrices, then requested a rerun after it finished. Those original measurements are labelled potentially contaminated and are excluded from qualification, pooling, and the fresh peak comparison. All raw records remain. The rerun uses the exact same saved executables, with no rebuild or test job during measurement.

## Variants and correctness

Both variants follow the [durability-cycle investigation](rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026.md). The retained control is `target/release/write-perf-v7-validated-logical-batch`, SHA-256 `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.

| Variant | Change | Candidate SHA-256 |
| --- | --- | --- |
| Buffered FRONTIER | Open a non-append buffered FRONTIER/sync handle; DATA and extent preparation remain direct | `25660d36f5d1f1332693af104ee5ef390e3c383cdbf256a967d29d0e7943f7d7` |
| Buffered Parallel | Buffer DATA, FRONTIER, and extent preparation through the existing ticket/packer/io_uring/completion pipeline | `e06550ac79c26d0f1c758c1ea07aad71e3296964a32d39a2f4b0c19edabcf5cb` |

Both preserve byte framing, hashes, allocation bounds, fixed cutoffs, inode/header validation, error poisoning, and recovery rules. A successful write CQE does not publish durability. The coordinator still waits for the captured DATA prefix, completes its FRONTIER write, and successfully calls one inode `fdatasync` before publication. Buffered Parallel retains the existing extent-initialization policy; it is not a separately tuned general buffered backend.

A new controlled test pauses before the first sync, completes later multi-frame DATA writes beside the unsynced FRONTIER, verifies the original marker bytes and unchanged public durable watermark, then releases sync and checks recovery of every acknowledged batch. The large batch uses two legal 35,000-byte values. Existing short-marker and failed-sync poison tests also pass.

Linux documents that [fdatasync flushes file data and required metadata](https://man7.org/linux/man-pages/man2/fsync.2.html), and cautions that [mixing direct and buffered I/O can reduce throughput](https://man7.org/linux/man-pages/man2/open.2.html). Those contracts motivated the tests; they do not by themselves identify the cause of a measured regression.

## Fresh rerun protocol

The [audited rerun plan](artifacts/rfc-025-v7-buffered-isolated-20261026-plan.json) was saved before capture. The [shared driver](artifacts/rfc-025-v7-buffered-isolated-20261026-run.py) checks host activity before launch, samples recognized Rust build/test/linker and other benchmark processes once per second while the child runs, and checks again after exit. Process arguments are not collected. Sampling is held in memory until the command finishes. Unexpected activity marks the run and stops the matrix after preserving it. No competing process is killed.

All **967 recorded host samples** are clear. Sampling detects sustained recognized tasks; it cannot prove absence of every shorter task, unrelated workload, or storage stall. Slow measurements remain in the results.

Each variant has two ext4 cells: eight writers, 1-KiB values, a 2-GiB SST target, 100,000 point puts or 750,000 batch64 puts. Latency is sampled every 100 point operations or ten batches. Each cell has four unscored warmups and five four-arm pairs: retained v4, candidate-build v4, retained v7, and candidate v7. Odd rounds use old v4/new v4/old v7/new v7; even rounds reverse the order. SSD cooldowns are 30 seconds before each cell and ten seconds after every run. Scratch databases stay in the local repository target directory and are removed after each run.

The isolated rerun preserves **80 scored records and 16 warmups**, with 96 unique run IDs. The earlier potentially contaminated matrices contain another 80 scored records and 16 warmups. The two sets are not pooled.

## Fresh throughput, CPU, and tail latency

Rates are per-arm medians in puts/s. Changes are medians of within-pair ratios and can differ from ratios of the displayed medians. V4-normalized change is `(candidate v7 / candidate v4) / (retained v7 / retained v4)` within each pair.

| Variant / workload | Retained median | Candidate median | Paired raw change | V4-normalized change | CPU change | p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Buffered FRONTIER, direct DATA, Point | 16,052 | 6,872 | -2.2% | -3.2% | +3.1% | -2.4% |
| Buffered FRONTIER, direct DATA, Batch64 | 386,443 | 377,925 | -2.8% | -4.9% | +4.5% | +9.1% |
| Buffered DATA and FRONTIER, Point | 15,785 | 13,754 | -12.4% | -13.3% | +18.0% | +23.4% |
| Buffered DATA and FRONTIER, Batch64 | 386,442 | 333,967 | -13.6% | -12.2% | +14.4% | +27.0% |

Buffered FRONTIER fails the point per-arm median floor and achieves no required material SSD gain. Its point arm medians differ sharply even though the paired median changes only -2.2%, because several slow runs occur in different pairs. Both summaries are retained. Buffered Parallel fails throughput and p99 guards in both workloads; its point v4 control ratio is +0.9%, while v7 loses 12.4% raw throughput.

Neither variant satisfies the predeclared SSD phase. The dependent RAM cells and extra SSD confirmation are cancelled. The restored direct-I/O implementation remains the result of this slice; v4 parity and the complete RFC 025 Stage 8 gate remain unmet.

## Fresh scored peaks

Only the audited rerun contributes to this table. Values are puts/s; all cells use the matching longer counts, SST target, cadence, and eight writers.

| Workload | V4 control | Retained direct v7 | Buffered FRONTIER | Buffered DATA + FRONTIER |
| --- | ---: | ---: | ---: | ---: |
| Point | 18,300 | 16,450 | 16,097 | 13,969 |
| Batch64 | 406,529 | 432,643 | 406,355 | 352,580 |

The [fresh peak identities](artifacts/rfc-025-v7-buffered-isolated-20261026-peaks.json) include run IDs, binary hashes, raw-record hashes, manifests, and host audits. Peaks supplement the paired comparisons; they do not replace the failed gates.

The earlier unmonitored matrices reached 627,763 batch puts/s for a v4 control and 20,788 point puts/s for retained v7. Those records remain in their raw files but are not promoted to audited references after the concurrency report. Older historical references of 480,373 retained v7 batch puts/s and 18,379 v4 point puts/s also remain historical references, rather than inputs to this fresh qualification. Earlier 50K/100K-put, 1-GiB-SST short cells stay separate.

## Earlier potentially contaminated measurements

These numbers describe the first captures, not the final qualification. The concurrency report does not identify exactly which commands overlapped the other project, so the whole initial pair of matrices is excluded conservatively.

| Variant / workload | Original paired raw change | Fresh paired raw change |
| --- | ---: | ---: |
| Buffered FRONTIER, direct DATA, Point | -0.8% | -2.2% |
| Buffered FRONTIER, direct DATA, Batch64 | -14.3% | -2.8% |
| Buffered DATA and FRONTIER, Point | -13.3% | -12.4% |
| Buffered DATA and FRONTIER, Batch64 | -19.0% | -13.6% |

The rerun changes the magnitude of some losses, especially FRONTIER-only batch writes. It does not establish how much of that difference came from the other project versus changing storage conditions. No old/new pooling is used to seek a passing result.

## Counter interpretation

Buffering can reduce the synchronous FRONTIER-write timer while increasing the subsequent sync timer. In the all-buffered variant, fdatasync can also perform physical DATA writeback that a direct-I/O CQE previously completed before the sync stage. These timing boundaries differ, so a lower marker timer or a sum of selected counters is not a complete wall-time explanation. Data-write overlap, page-cache work, metadata, grouping, and storage stalls are not independently isolated by these counters. The primary decision uses throughput, CPU, p99, and all recorded runs.

## Sources and captures

Both source snapshots are relative to `b9684dc5`. The patch files use zero-context hunks (`git apply --unidiff-zero` or GNU `patch -p1`).

| Variant | Source patch | 128-input manifest | Binary identities | Initial plan |
| --- | --- | --- | --- |
| Buffered FRONTIER, direct DATA | [patch](artifacts/rfc-025-v7-buffered-frontier-20261026-candidate.patch) | [TSV](artifacts/rfc-025-v7-buffered-frontier-20261026-source-sha256.tsv) | [JSON](artifacts/rfc-025-v7-buffered-frontier-20261026-binaries.json) | [plan](artifacts/rfc-025-v7-buffered-frontier-20261026-plan.json) |
| Buffered DATA and FRONTIER | [patch](artifacts/rfc-025-v7-buffered-parallel-20261026-candidate.patch) | [TSV](artifacts/rfc-025-v7-buffered-parallel-20261026-source-sha256.tsv) | [JSON](artifacts/rfc-025-v7-buffered-parallel-20261026-binaries.json) | [plan](artifacts/rfc-025-v7-buffered-parallel-20261026-plan.json) |

| Fresh cell | Script | Manifest | Raw records | Host audit |
| --- | --- | --- | --- | --- |
| Buffered FRONTIER, direct DATA, Point | [script](artifacts/rfc-025-v7-buffered-frontier-isolated-ext4-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-buffered-frontier-isolated-ext4-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-buffered-frontier-isolated-ext4-point8-20261026.jsonl) | [JSONL](artifacts/rfc-025-v7-buffered-frontier-isolated-ext4-point8-20261026-host.jsonl) |
| Buffered FRONTIER, direct DATA, Batch64 | [script](artifacts/rfc-025-v7-buffered-frontier-isolated-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-buffered-frontier-isolated-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-buffered-frontier-isolated-ext4-batch64-20261026.jsonl) | [JSONL](artifacts/rfc-025-v7-buffered-frontier-isolated-ext4-batch64-20261026-host.jsonl) |
| Buffered DATA and FRONTIER, Point | [script](artifacts/rfc-025-v7-buffered-parallel-isolated-ext4-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-buffered-parallel-isolated-ext4-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-buffered-parallel-isolated-ext4-point8-20261026.jsonl) | [JSONL](artifacts/rfc-025-v7-buffered-parallel-isolated-ext4-point8-20261026-host.jsonl) |
| Buffered DATA and FRONTIER, Batch64 | [script](artifacts/rfc-025-v7-buffered-parallel-isolated-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-buffered-parallel-isolated-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-buffered-parallel-isolated-ext4-batch64-20261026.jsonl) | [JSONL](artifacts/rfc-025-v7-buffered-parallel-isolated-ext4-batch64-20261026-host.jsonl) |

The original raw files and manifests retain the same names without `-isolated-`; their assessment files now carry the concurrency qualification label. Original scripts, plans, summaries, source snapshots, and saved binaries remain available.

The [audited analyzer](artifacts/rfc-025-v7-buffered-isolated-20261026-analyze.py) reuses the original arithmetic, verifies every raw/audit SHA, all expected arms and order, options, counters, unique IDs, and empty competing-process samples. The [FRONTIER-only summary](artifacts/rfc-025-v7-buffered-frontier-isolated-20261026-summary.json) and [all-buffered summary](artifacts/rfc-025-v7-buffered-parallel-isolated-20261026-summary.json) preserve every pair.

## Restoration and validation

All Rust changes from both prototypes are restored byte-for-byte. The canonical `target/release/write-perf` is restored to retained SHA-256 `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`. Source modification times were refreshed before the restored full check. The rerun uses separately named frozen candidate binaries; it makes no new Rust changes or rebuilds. All benchmark databases and source-reconstruction scratch directories are removed.

Candidate FRONTIER-only full check: 1964 tests run: 1964 passed (1 slow), 0 skipped; nextest `5e5b1868-15b6-4fcc-83fc-8ba40a1e36d4`. All-buffered full check: 1964 tests run: 1964 passed, 0 skipped; nextest `032a8505-ebb4-4c2e-807f-bdcb4022b9b5`. Restored-source full check: 1963 tests run: 1963 passed (1 slow), 0 skipped; nextest `5babb16f-fe65-4eb2-bcdd-11ebfb4cb952`. The slow tests are compaction integration tests; no retry was needed in these final checks. Formatting, Clippy, dependency sorting, unused-dependency, and spelling tasks pass.

The [verification script](artifacts/rfc-025-v7-buffered-isolated-20261026-verify.py) reconstructs both sources and regenerates the audited summaries. The [verification record](artifacts/rfc-025-v7-buffered-isolated-20261026-verification.json) retains validation IDs and counts. Final evidence validation checks exact source reconstruction, binary hashes, audit/summary regeneration, links, artifact syntax, and direct `typos`. No check/build/profile overlaps the audited scored runs.

[WAL documentation](../README.md).
