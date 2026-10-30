# RFC 025: linked FRONTIER-sized write and sync diagnostic, 2026-10-26

**Result: do not advance this linked-commit design to an engine candidate. None of the three ring configurations reduces marker-plus-sync latency by the fixed 3% target in both native workloads. Engine I/O remains unchanged.**

This is a separate mechanism diagnostic after the [larger extent-write experiment](rfc-025-v7-extent-io-20261026.md). It tests whether linking a marker-sized write to whole-file DATASYNC can remove enough userspace handoff cost to justify implementing and qualifying that backend. It also compares the task-work configurations suggested by the maintainer. These are synthetic I/O measurements, not engine throughput, WAL-codec validation or crash-recovery qualification.

## Current engine and flags

The [dedicated WAL worker](../../../kv-engine/src/wal/parallel/worker.rs) already creates its ring with `COOP_TASKRUN | SINGLE_ISSUER`, falling back to a flagless ring if setup returns EINVAL. The same worker thread creates, submits to and reaps that ring. It waits for the ring or command eventfd with `poll()`. The v7 coordinator separately performs its FRONTIER write with synchronous `pwrite`, then a synchronous whole-file `fdatasync`.

`DEFER_TASKRUN` requires a single issuer that drives completion work through `io_uring_enter(GETEVENTS)`. It cannot simply replace the cooperative flag while retaining the existing external-poll completion loop. These flags schedule completion work; they do not relax durability or remove the device flush. See [the Linux setup documentation](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html).

An [older v4 native-wait experiment](../experiments/rfc-024-deferred-ring-wait-20261003.md) correctly integrated deferred task work and interruptible command waiting. It was rejected: eight-writer point throughput improved 2.7%, but batch64 lost 6.9%, and other guards failed. That used an older engine and Linux 6.18.9-arch1-2. It is historical evidence, not a current-v7 result.

## Native protocol

One issuing thread uses synthetic opaque bytes, with 500 cycles per process. A point-sized group is 32 KiB of DATA-shaped bytes followed by a 4-KiB marker; a batch-sized group is 544 KiB followed by the same marker. These labels model the byte counts of eight small or large tickets. There are no concurrent engine writers, MVCC, framing, CRCs, prefix hashes or recovery in this probe.

Every cycle first completes an O_DIRECT DATA write. The syscall policy then writes the marker with O_DIRECT `pwrite` and calls `fdatasync`. The other policies submit a linked `WRITE -> FSYNC(DATASYNC)` pair, using a registered descriptor for the same inode and `GETEVENTS` completion waits. The DATA write precedes both paths. Both CQEs must have the expected tag and full successful result before a linked cycle completes. No range-limited sync or speculative acknowledgment is used.

The point-sized file is preallocated and zero-initialized before timing; the batch-sized file is preallocated with unwritten extents. That distinction follows the retained small/large initialization policies. Allocation, initialization, setup, readback verification and cleanup are outside the cycle timer but inside process auditing. Every DATA and marker byte is read back and compared before the process succeeds. File images are about 18.4 and 280.6 MB; each is deleted after verification.

The [fixed plan](artifacts/rfc-025-v7-frontier-link-20261026-plan.json) specifies one warmup per policy and workload, then five rotated/reversed four-policy rounds, with a 30-second initial cooldown and ten seconds between processes. All 40 scored native observations, eight warmups and 151 clear host-process samples remain. Sampling is before, once per second during, and after each command. No recognized competing build, test or benchmark is observed; sampling cannot exclude every brief task or device stall. No build, profile or test overlaps scoring.

Advancing a ring mode requires at least 3% paired median marker-plus-sync latency improvement in both workloads, with no round losing more than 5% cycle throughput or adding more than 10% p99. This is only a screen for spending effort on a Rust backend. A passing native probe would still require a real engine implementation, failure/ownership tests and the engine performance matrix. All three modes fail even the median latency target.

## Results

The following are medians of same-round ring/syscall ratios. Positive marker-plus-sync changes mean slower commits. Cycle throughput includes the synchronous DATA write and is cycles/s, not puts/s.

| Linked configuration | Point marker + sync | Batch marker + sync | Point cycle throughput | Batch cycle throughput |
| --- | ---: | ---: | ---: | ---: |
| Default ring | +13.8% | +1.9% | -2.8% | +0.2% |
| COOP_TASKRUN + SINGLE_ISSUER | +11.1% | +2.9% | -6.7% | -0.3% |
| SINGLE_ISSUER + DEFER_TASKRUN | +11.5% | +2.6% | -4.3% | -0.7% |

Per-arm median marker-plus-sync times are 284 / 323 / 316 / 317 microseconds for point-sized groups and 387 / 394 / 390 / 396 microseconds for batch-sized groups, in syscall / default / cooperative-single / deferred-single order. These per-arm summaries differ from medians of paired ratios. Point cycle p99 spans per-arm medians of about 9.0–9.4 ms; those slow samples stay included.

Within the linked modes, cooperative-single versus default reduces paired marker-plus-sync latency 2.4% for point and 0.8% for batch. Deferred-single versus cooperative-single changes it +1.5% and -0.1%. Those small comparisons do not establish an end-to-end v7 gain. In particular, switching task-work flags does not recover the added cost of this linked coordinator design relative to its direct-syscall baseline. The probe does not isolate the precise kernel scheduling cause of the overhead.

## Evidence and validation

The host is the same Linux 6.12.71-1-lts/ext4 Solidigm system as the extent experiment. GCC 15.2.1 builds the diagnostic with `-O3 -g -Wall -Wextra -Werror`, against installed liburing 2.14. The exact-flag executable SHA-256 is `9d73b22c83aa5eb49b2013b2933f083c27f49a6c110e6421c8207a89d95cfdd9`.

- [Probe source](artifacts/rfc-025-v7-frontier-link-20261026-probe.c), [build identity](artifacts/rfc-025-v7-frontier-link-20261026-build.json), [audited driver](artifacts/rfc-025-v7-frontier-link-20261026-run.py) and [analyzer](artifacts/rfc-025-v7-frontier-link-20261026-analyze.py).
- [All native records](artifacts/rfc-025-v7-frontier-link-20261026.jsonl), [run manifest](artifacts/rfc-025-v7-frontier-link-20261026-runs.tsv), [host samples](artifacts/rfc-025-v7-frontier-link-20261026-host.jsonl) and [verified summary](artifacts/rfc-025-v7-frontier-link-20261026-summary.json).
- Two setup preflights fail before any scored linked observation. The first detects liburing's implicit `NO_SQARRAY`; the second detects descriptor/feature fields the installed mmap helper leaves to its caller. The final probe uses explicit setup flags, mapping, identity fields and SQ-index initialization. [First failure](artifacts/rfc-025-v7-frontier-link-20261026-preflight-failure.json) and [second failure](artifacts/rfc-025-v7-frontier-link-20261026-preflight2-failure.json), their source/build snapshots and their two syscall warmups remain separately under the `-preflight` and `-preflight2` prefixes. They are excluded from the fixed comparison. No scored result is discarded or overwritten. The [verifier](artifacts/rfc-025-v7-frontier-link-20261026-verify.py) and [verification result](artifacts/rfc-025-v7-frontier-link-20261026-verification.json) check all saved executables, both preflights, exact summary regeneration, distinct record IDs and unchanged engine source.

The larger-write prototype and the restored engine each pass `cargo make check`: 1,963 tests, zero skips/retries. Direct spelling checks pass. The engine source, System allocator and retained release executable are restored, with the existing one-sync protocol and eleven optimizations intact. No linked backend or deferred completion loop is installed. V4 parity and the complete Stage 8 gate remain unmet.

[WAL documentation](../README.md).
