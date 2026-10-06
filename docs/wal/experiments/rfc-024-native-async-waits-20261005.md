# RFC 024: cooperative WAL durability and publication experiment

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Experiment started October 4, 2026; corrected-teardown confirmation completed October 5 (Asia/Chongqing).

The native wait prototype is promising, but its confirmation did not pass the frozen repeatability screen. At confirmation time it was isolated: the retained engine, dependency manifests, toolchain, async API guards, and normal `write-perf` binary were unchanged. The later engine integration is tracked in the [integration report](../rfc-024-native-async-integration-20261005.md); all measurements below describe the archived prototype.

## Corrected-teardown confirmation

NVMe-backed ext4 (`/dev/nvme0n1p3`), ordinary v4 parallel WAL, 16 clients, 64 puts per batch, 1 KiB values, 262,144 puts per observation. Eight Tokio runtime workers execute the native arm. Every arm flushes and closes after the measured window. The reference executable uses the retained production engine.

Each comparison below is the median of three fixed block geometric-mean ratios. Every block contains two observations per arm. These are observed estimates, not qualified production gains.

| Native eight-worker arm compared with | Throughput | Batch p99 | CPU per put | Process switches per batch | Repeat controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Matched synchronous control | +11.3% | -0.2% | -37.3% | +70.7% | 1/3 |
| Retained parallel WAL reference | +14.3% | -1.1% | -37.5% | +103.5% | 0/3 |

All three native/control throughput ratios were positive: +6.8%, +11.8%, +11.3%. The native/reference ratios were +11.1%, +14.3%, +16.6%.

The frozen control requires both repeats of both arms to stay within 5% throughput and 10% p99. Confirmation blocks 0 and 1 failed native/control repeatability: native throughput spread was 10.7% and 11.2%, with p99 spread 17.9% and 16.6%. Block 0 also failed for the control. Block 2 passed. The retained reference exceeded the throughput control in every block (one by only 0.005 percentage points). No confirmation retry rule triggered; no results were discarded or replaced.

Descriptive medians of the six primary observations per arm are provided separately. Dividing these medians does not reproduce the paired estimator above.

| Arm | Puts/s | Batch p99 | CPU µs/put | Switches/batch | Peak in-flight groups |
| --- | ---: | ---: | ---: | ---: | ---: |
| Retained parallel reference | 513,558 | 3.285 ms | 10.425 | 5.064 | 12–14 |
| Matched synchronous control | 538,327 | 3.244 ms | 10.410 | 6.051 | 11–15 |
| Native waits, eight workers | 591,582 | 3.228 ms | 6.485 | 10.317 | 15–15 |

Native peak outstanding write SQEs were 15 in every confirmation observation. The configured limits remain 32 in-flight groups and 256 ring entries. Sixteen synchronous logical clients cannot keep 32 separate commits outstanding. These are software counters, not measured device queue depth.

## What was implemented

The fork adds awaitable WAL durability and MVCC publication to the existing io_uring backend. Durability waits recheck the contiguous durable frontier and poison boundary; publication registers ready timestamps and then awaits the visibility prefix. Native publication uses neither the synchronous publication spin loop nor a per-commit blocking-pool job. Notification registration precedes predicate checks, following the [Tokio Notify protocol](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html).

An opt-in dedicated packer handles extent preparation and worker submission. The matched synchronous control uses this same packer and the same batch preparation and memtable publication helpers. Its commit waits remain synchronous. This primary comparison isolates cooperative waits and task scheduling over a common data path. Comparison with the retained reference additionally includes the packer and wrapper/preparation differences, so the entire reference gain cannot be attributed solely to async waits.

After admission, an owned Tokio task holds prepared entries, an engine lifecycle guard, and a concurrency permit until durability and publication finish. Caller cancellation leaves that task running, and close waits for it. Destruction/cancellation of the owned task poisons an admitted MVCC reservation rather than retiring a potentially durable write. The existing WAL worker retains its buffer, ring, and file ownership rules. `fdatasync` continues on its dedicated coordinator; this experiment does not replace that syscall or filesystem behavior.

The wrapper keeps its engine private and uses one active memtable. It accepts ordinary v4 point batches only: at most 64 records, 512-byte keys, 1 KiB values, 16 active commits, and a conservative 512 MiB lifetime input budget. This bounds direct-buffer residency below the 64 MiB WAL budget. Rotation, general memtable freeze integration, PITR v5/v6, serializable transactions, and the general async APIs are outside this prototype. Final flush requires exclusive wrapper ownership, proving that writers can no longer publish into the memtable being frozen.

## Initial screen and harness correction

The initial four-arm study retained 12 warmups, 24 primary observations, and two separately labelled same-arm retries. Eight-worker native/control results were +7.8% throughput, -35.1% CPU/put, and -0.8% p99, with 2/3 repeat controls. The four-worker secondary arm showed +16.2% throughput and -64.3% CPU/put, but 0/3 controls; it was not selected as the confirmed configuration.

Before confirmation, inspection found that the retained reference flushed its database after measurement while the new probe only closed it. The probe was corrected to flush and close with exclusive ownership after all writers finish. Writer-window operations were unchanged, but the executable changed. This confirmation is a corrected protocol, not an exact same-binary rerun. The initial binary, protocol, raw data, and patch remain archived; the two datasets are not pooled.

Confirmation retained nine warmups and 18 primary observations in three predefined mirrored/rotated blocks. Both stages used fresh databases, RAM-staged executables, captured output, five-second idle intervals, and no concurrent build, tracing, test, or benchmark work. An obvious regression triggers an additional one-second pause and one separately labelled retry under frozen rules.

## Validation and decision

The isolated fork passed `cargo make check`, including default and all-feature Clippy, formatting, dependency checks, typos, and 1,429 native tests with zero skips. The standalone probe passed strict Clippy and three native tests covering all scheduling arms, remainder batches, clients without work, normal reads/recovery, and exclusive-ownership final flush. Eight focused engine tests cover single-executor progress, contiguous publication, poison wakeups, caller/owned-task cancellation, close draining, validation without timestamp holes, sync failure without ghost publication, and WAL recovery.

The repeated direction of the gain and large CPU reduction justify preserving the prototype. Confirmation still failed the repeatability screen. No RFC performance adoption gate or general async implementation is complete. In particular, these comparisons use the retained parallel implementation rather than the leader baseline; the full workload matrix, leader comparison, confidence intervals, and rotation/PITR contracts remain necessary before production adoption.

Process switch counts increased despite lower CPU use. Counts alone therefore do not explain commit cost. The lower memtable timing totals and removal of publication spinning are leads for a later profile/ablation, not proof of which component caused the gain.

## Reproduction artifacts

- Initial protocol: `target/rfc024-native-async-ca4cfcca-20261004/protocol.json`, analysis: `target/rfc024-native-async-ca4cfcca-20261004/analysis.json`, raw observations: `target/rfc024-native-async-ca4cfcca-20261004/records.json`, and initial patch: `target/rfc024-native-async-ca4cfcca-20261004/candidate.patch`.
- Corrected confirmation protocol: `target/rfc024-native-async-ca4cfcca-20261004/confirmation/protocol.json`, analysis: `target/rfc024-native-async-ca4cfcca-20261004/confirmation/analysis.json`, raw observations: `target/rfc024-native-async-ca4cfcca-20261004/confirmation/records.json`, and prototype patch: `target/rfc024-native-async-ca4cfcca-20261004/confirmation/candidate.patch`.
- Prototype source: `target/rfc024-native-async-ca4cfcca-20261004/fork/kv-engine/src/lsm_storage/native_async_probe.rs`, probe harness: `target/rfc024-native-async-ca4cfcca-20261004/probe/src/main.rs`, compiled source hashes: `target/rfc024-native-async-ca4cfcca-20261004/confirmation/source-hashes.json`, and production integrity: `target/rfc024-native-async-ca4cfcca-20261004/final-integrity.json`.

Artifacts are local and ignored by Git. Initial and corrected binaries are preserved separately; the normal production benchmark binary retains SHA-256 `72891a06445b1e08a5ec187b4cbed8c41cbf0377c80b0e2aa5fc4748c40b0eb3`.
