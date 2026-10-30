# RFC 025: 16-writer batch tail and storage stalls, 2026-10-27

The earlier **-1.0% throughput / +23.1% p99** retirement-hint result does not establish that sixteen writers inherently regress. After separating obvious stalls as requested, the final diagnostic observes p99 **4.574 ms baseline versus 4.605 ms prototype**. A strict I/O/DATA-queue mask gives **4.552 versus 4.603 ms**, with 1,152 and 1,135 samples retained. The earlier 23.1% p99 difference does not reproduce in this diagnostic. Its original cause cannot be proved retroactively because that cohort did not save individual operation timings.

There is a concrete source mechanism worth addressing: retaining a previous FRONTIER can make a newly completed ticket prefix physically ineligible. The coordinator can then start another coalescing window on its next attempt. In the final trace, the prototype performs **51 such eligibility retries versus 20**, and **1,380 successful syncs versus 1,175**. This is a measured explanation for avoidable waiting and extra sync work, not proof of an invariant 16-writer penalty or an isolated attribution of the original p99 gap.

The saved retirement-hint prototype and the more recent FRONTIER-buffer-reuse candidate are different source variants. Their measurements remain separate throughout this report. Neither becomes retained because of these diagnostic captures. The existing f7 build remains in place.

## The exact earlier three pairs

All commands write 750,000 entries, batch64, sixteen writers, 1-KiB values, 2-GiB SST target, explicit v7 Parallel and no compaction on fresh local ext4 databases. Each invocation samples 1,184 API calls. The prototype derives large-batch geometry at worker retirement and also requires a previous sync of at least 400 us before choosing an 800-us coalescing window. The retained policy can choose 800 us for a known large single-ticket wave after a 100-us sync. These two changes have not been isolated from each other here.

| Pair | Baseline puts/s | Prototype puts/s | Baseline p99 ms | Prototype p99 ms | Throughput / p99 change |
|---|---:|---:|---:|---:|---:|
| 1 | 508,250 | 503,229 | 4.934 | 6.076 | -1.0% / +23.1% |
| 2 | 288,686 | 406,519 | 22.684 | 17.338 | +40.8% / -23.6% |
| 3 | 203,943 | 36,525 | 36.444 | 59.278 | -82.1% / +62.7% |

Pair3 is an obvious severe-stall observation: prototype FRONTIER time rises from **0.899 to 9.642 seconds**, fdatasync from **1.358 to 6.080 seconds**, while sync count changes from **1,332 to 1,335**. It is excluded from the requested filtered interpretation, and its original bytes remain saved. Across pairs1–2, the median of their paired ratios is throughput **+19.9%**, p99 **-0.2%**. Pair2 itself has substantially longer I/O tails than pair1, so that filtered number is not evidence of a repeatable 20% source improvement.

Pair1 is the fastest matching pair and remains throughput near parity with a **1.142-ms p99 increase**. Mean fdatasync is essentially flat, **359 versus 361 us**; aggregate FRONTIER time increases only **71.8 to 97.3 ms**; coalescing time falls **625.3 to 532.4 ms**. No saved per-request trace can establish whether its extra p99 is an I/O outlier, queueing, scheduling or source policy. The median of the original three ratios happens to select pair1 for both metrics; the published headline therefore cannot be blamed solely on pair3.

Sources: [raw runs](artifacts/rfc-025-v7-batch-retirement-hint-20261027-ext4-batch16.jsonl), [run identities](artifacts/rfc-025-v7-batch-retirement-hint-20261027-ext4-batch16-runs.tsv), [original report](rfc-025-v7-batch-retirement-hint-20261027.md). The original qualification record is preserved unchanged; this report adds the requested filtered diagnosis.

## Eight versus sixteen writers

Eight excluded captures trace the retained executable and the separate buffer-reuse candidate, in a fixed forward/reverse schedule. Synchronous WAL calls are timestamped by a native interposer; a 100-ms observer samples thread waits, host work and device counters. The preload buffers events in memory and writes them at process exit. No SSD SMART/temperature polling occurs during the captures. DATA io_uring requests are not visible in this first stage.

The same buffer-reuse executable has this first-round comparison:

| Writers | Puts/s | API p99 ms | Mean FRONTIER write ms | Synchronous initialization MiB | Mean speculative tail KiB |
|---|---:|---:|---:|---:|---:|
| 8 | 367,538 | 2.987 | 0.072 | 19.25 | 22.9 |
| 16 | 565,226 | 4.539 | 0.057 | 69.88 | 151.7 |

Throughput scales **1.54x**. Higher API latency with more writers does not, by itself, imply lower throughput or a source regression. In the reverse round, this executable reaches 458,415 puts/s at eight writers but 419,080 at sixteen; that sixteen-writer run has p99 18.329 ms and more I/O tails. Thus the claim that sixteen writers always lose is not supported.

Sixteen writers do create more physical work ahead of the contiguous written prefix. The fast sixteen-writer trace has speculative bytes before 53% of markers, versus 14% at eight writers, and the coordinator performs 102 versus 36 fallocate calls. In the reverse round the initialization totals are 91.25 versus 7.625 MiB. These are synchronous non-frame initialization writes inside the active FRONTIER window, separate from roughly 816 MB of DATA; they are not total device write amplification.

Storage stalls are independently visible. The slow retained sixteen-writer invocation reaches just 26,799 puts/s: mean marker write **10.426 ms**, fdatasync **5.913 ms**, fallocate **9.954 ms**. The coordinator is repeatedly sampled in `D` state at `__iomap_dio_rw` and `jbd2_log_wait_commit`. Host-wide mean write latency over the sampled window is about **8.0 ms**, versus **0.12 ms** for the fast candidate-sixteen run. These observations establish kernel I/O/journal delay. They do not identify SSD firmware garbage collection, thermal throttling or power management as its cause. Extra initialization alone cannot explain the multi-fold slowdowns: the slow retained run initializes fewer bytes than the fast candidate run.

Sources: [fixed plan](artifacts/rfc-025-v7-writer-scaling-20261027-plan.json), [raw runs](artifacts/rfc-025-v7-writer-scaling-20261027-runs.jsonl), [summary](artifacts/rfc-025-v7-writer-scaling-20261027-summary.json), [analyzer](artifacts/rfc-025-v7-writer-scaling-20261027-analyze.py), [interposer](artifacts/rfc-025-v7-writer-scaling-20261027-io-trace.c), [capture hashes](artifacts/rfc-025-v7-writer-scaling-20261027-captures.json), [environment](artifacts/rfc-025-v7-writer-scaling-20261027-environment.json).

## Following an API call to its durable ticket

The next stages use the **exact retirement-hint source**, not buffer reuse. Each baseline/prototype pair gets identical temporary instrumentation and its own source manifest and executable identity. The harness records sampled batch start/end times, emits them after measured work and maps their first keys to WAL tickets. Every full capture maps all **11,728 tickets** and all **1,184 sampled calls**. Native FRONTIER counts must match engine counters.

The first four captures timestamp API calls and synchronous I/O. A 5,000-entry bootstrap probe validates the key/ticket mapping. The initial 1-ms synchronous-call mask leaves pooled baseline/prototype p99 **8.284/6.229 ms**, but the 2-ms mask flips it to **8.382/11.913 ms**. That sensitivity shows why an unflagged sample cannot yet be called stall-free: async DATA and completion scheduling were missing.

Two readiness captures then add DATA SQE-staging and CQE-reap times, group membership, and FRONTIER target-capture time. The stronger mask removes samples overlapping any traced synchronous WAL call or DATA staging-to-reap interval of at least 1/2/5 ms. Its 1-ms view leaves **zero comparable baseline samples** and **619 prototype samples**, prototype p99 **4.571 ms**. No baseline/prototype clean ratio is possible in that stage. The remaining tail is concentrated before DATA staging and while waiting for the later tickets included in its eventual captured prefix.

SQE staging to CQE reap includes submission, kernel I/O and scheduling/reaping delay. The maximum group CQE time is a lower bound before group-result publication, not its exact coordinator receive time. The added coordinator stage below addresses that remaining gap. No mask deletes an original run or sample file, and filtered sample distributions do not produce filtered throughput estimates.

Sources: [batch-timing plan](artifacts/rfc-025-v7-clean-tail-20261027-plan.json), [common harness patch](artifacts/rfc-025-v7-clean-tail-20261027-harness.patch), [batch-timing summary](artifacts/rfc-025-v7-clean-tail-20261027-summary.json), [readiness plan](artifacts/rfc-025-v7-ready-tail-20261027-plan.json), [readiness summary](artifacts/rfc-025-v7-ready-tail-20261027-summary.json). Their matching `-captures.json`, `-binaries.json`, `-builds.json`, `-runs.jsonl`, `-analyze.py` and per-arm source manifests preserve the exact inputs. A diagnostic-only formatting failure before the readiness build is recorded in [attempts](artifacts/rfc-025-v7-ready-tail-20261027-attempts.json).

## Final coordinator checkpoints

The last two fixed captures add receive/drain/coalescing/sync-return timestamps. Both complete with comparable short-I/O conditions. They are diagnostics with tracing overhead, not independent qualification or a replacement for the original scored cohort.

| Measure | Retained baseline | Retirement-hint prototype |
|---|---:|---:|
| Throughput, puts/s | 564,746 | 524,463 |
| Raw API p99, ms | 4.574 | 4.605 |
| 1-ms mask samples / p99 ms | 1,152 / 4.552 | 1,135 / 4.603 |
| 2-ms mask samples / p99 ms | 1,183 / 4.569 | 1,184 / 4.605 |
| Successful FRONTIER/sync cycles | 1,175 | 1,380 |
| Coalescing calls, 400-us / 800-us budget | 709 / 491 | 1,243 / 224 |
| No-progress attempts with new written DATA | 20 | 51 |
| No-progress attempts without new contiguous DATA | 6 | 37 |
| Initial result processing/drain p99, us | 16.484 | 16.295 |
| Result processing inside coalescing p99, us | 2.892 | 2.139 |

The traced prototype is **7.1% lower throughput**, with **0.7% higher raw p99** and **1.1% higher masked p99**. Its sync count is **17.4% higher**; total fdatasync rises from 420.7 to 478.9 ms and packer preparation from 35.4 to 86.2 ms. These counters can overlap across threads and are not an additive elapsed-time decomposition. The two policy changes select more 400-us windows in this trace. Their effects cannot be separated without another controlled experiment.

The remaining top 1% samples in the 1-ms view have median time before DATA staging **3.059/2.889 ms** for baseline/prototype, DATA staging-to-reap **0.112/0.140 ms**, waiting for later captured DATA **0.988/0.704 ms**, and fdatasync **0.484/0.450 ms**. These independent stage medians do not sum to a particular p99 sample. Before-staging time includes producer preparation/admission, ordered packing, readiness and submission; this instrumentation does not isolate all of those components.

The physical eligibility check is correct and necessary. A previous marker at the allocation tail can follow DATA that it did not certify. A successor must retain that marker, so it must also include every intervening DATA batch in its ticket prefix. `eligible_durable_end_for` rejects a prefix whose first excluded DATA still lies before the retained marker end. More speculative DATA at sixteen writers makes this situation possible more often.

A concrete filtered prototype sample, ticket **171**, takes **4.925 ms**:

```text
previous durable ticket_end = 170
written ticket_end = 174
first excluded DATA end = 12,300,288
previous FRONTIER end = 12,374,016

coalesce 400 us (actual 469 us)
  -> target 174 is ineligible: retaining the marker retains excluded DATA
  -> no FRONTIER append and no fdatasync
wait for another group completion
coalesce another 400 us (actual 467 us)
  -> target 179 is eligible
  -> FRONTIER + one fdatasync -> ACK
```

The two windows consume **0.937 ms**. Previous fdatasync return to target capture spans **1.641 ms**: post-sync bookkeeping/notification 0.103 ms, those two windows 0.937 ms, initial/capture drains about 0.002 ms, and handoff/other gaps 0.600 ms. The sample also spends 2.314 ms before its DATA is staged. These intervals describe different overlapping portions of the API timeline; they must not be added as independent costs. No traced synchronous call or DATA queue interval of at least 1 ms overlaps this sample.

The final traces therefore locate repeated coalescing after physical ineligibility and additional sync cycles as concrete optimization targets. They do not show multi-millisecond result-map processing as the dominant clean tail. A future candidate can avoid restarting a full optional coalescing budget after an ineligible attempt and reduce speculative preparation, while keeping physical-prefix eligibility, the FRONTIER chain, poison handling and sync-before-ACK unchanged. This slice does not implement that policy or relax the protocol.

Sources: [final plan](artifacts/rfc-025-v7-coordinator-tail-20261027-plan.json), [raw runs](artifacts/rfc-025-v7-coordinator-tail-20261027-runs.jsonl), [summary and exact sample](artifacts/rfc-025-v7-coordinator-tail-20261027-summary.json), [analyzer](artifacts/rfc-025-v7-coordinator-tail-20261027-analyze.py), [capture hashes](artifacts/rfc-025-v7-coordinator-tail-20261027-captures.json), [executable identities](artifacts/rfc-025-v7-coordinator-tail-20261027-binaries.json), [builds](artifacts/rfc-025-v7-coordinator-tail-20261027-builds.json), [trace-build correction](artifacts/rfc-025-v7-coordinator-tail-20261027-attempts.json).

## Final state and verification

All temporary engine/harness changes are restored to source base `3aca2bdb`. Canonical `target/release/write-perf` is retained f7, SHA-256 `f7fd96e5ca0ecaaaa23f59f135e7ce9430521f986834e5c95f2d32070e378d54`. The [buffer-reuse report](rfc-025-v7-frontier-buffer-reuse-20261027.md) records candidate correctness and restored-source checks: 1,971 tests, with one unrelated compaction retry in the restored check. No new retained optimization, allocator/runtime change, PR or push is included.

These stages add **17 excluded invocations**: eight scaling traces, one mapping probe, four batch-timing traces, two readiness traces and two coordinator traces. The separate buffer candidate has 32 invocations. Together with the previously verified 384, there are **433 unique invocations**; reused CPU profiles are counted only once.

The [combined verifier](artifacts/rfc-025-v7-16-writer-tail-20261027-verify.py) reconstructs each diagnostic source and common harness, checks binary/capture hashes, regenerates summaries, checks unique run IDs and verifies final restoration. [Verification result](artifacts/rfc-025-v7-16-writer-tail-20261027-verification.json). Large syscall, stage and per-operation traces stay in local ignored `target/wal-opt` with versioned hashes; aggregate raw benchmark output and summaries are versioned. This is an investigation, with no full RFC adoption or v4-parity qualification.

[WAL documentation](../README.md).
