# RFC 025: hash large groups after DATA submission, 2026-10-29

**Standalone candidate rejected.** The solo best improves, but eight-writer best throughput falls 11.7%. Sixteen writers provide only one eligible candidate run, which also trails retained v7. Both Rust files and the canonical executable are restored; all earlier retained optimizations remain. Further producer-framing work will be measured separately.

## Change and correctness

The [CPU attribution](rfc-025-v7-batch-attribution-20261029.md) identifies about 1.14 s of ordered logical SHA256 work per 750,000 batch puts before DATA submission. This candidate frames large-only groups, submits their DATA, hashes on the same ordered packer, then publishes the exact endpoint snapshot. Small and mixed groups retain inline framing/hash publication. The existing 64-KiB per-buffer threshold classifies groups; no new worker, shared hash queue, allocator, I/O policy, coalescing interval, or digest definition is introduced.

A pending endpoint under the admission mutex lets the coordinator accept either event order: DATA complete before prefix ready, or prefix ready before DATA complete. FRONTIER construction waits for the matching digest and contiguous completed DATA. Waiting releases admission; a missing snapshot without the exact pending endpoint remains an error. Packer failure and panic clear the pending state and wake the coordinator. One FRONTIER write and one successful WAL fdatasync still precede publication and ACK.

Logical inputs remain on the packer's bounded reusable scratch vector. The admission reservation owns their charge until prefix publication and durability; close joins the packer before retiring remaining reservations. Existing DATA buffer pins cover later CQEs. Controlled tests pause hashing after actual DATA completion, verify that no new FRONTIER or ACK appears and logical memory stays charged after DATA retirement, then exercise concurrent close with large and small successors and exact recovery. Error and panic tests wake waiters and retain only the acknowledged recovery prefix. Tests that arm gates/failpoints use the required `failpoint_` prefix.

## Fresh best scores

Each row uses the highest-throughput eligible scored invocation for each exact executable. Its p99 and CPU come from that same invocation. Warmups are excluded; all raw scores and aggregate stall flags remain available.

| Writers, batch64 | Retained v7 puts/s | Candidate puts/s | Throughput | Retained / candidate p99 ms | CPU | Eligible retained / candidate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 67,596 | 73,671 | +9.0% | 1.321 / 1.289 | +3.2% | 3 / 3 |
| 8 | 467,448 | 412,971 | -11.7% | 2.622 / 2.377 | +22.0% | 2 / 3 |
| 16 | 581,512 | 485,276 | -16.5% | 4.371 / 9.639 | +11.0% | 3 / 1 |

The solo winning runs have nearly equal mean fdatasync latency (about 0.309 and 0.310 ms), consistent with useful overlap rather than a faster storage barrier. The candidate uses 3.2% more process CPU in those two runs. This is a throughput/latency observation, not a measured fraction of hashing hidden by I/O.

At eight writers, best CPU rises 22.0%. The candidate frames a whole group before hashing its inputs; the retained packer interleaves framing and hashing per batch. Cache locality, group formation, and the added digest-readiness handoff are possible contributors, not isolated causes. The trace does not prove that all high-concurrency p99 comes from packer queueing. Producer framing can be tested next without changing the wire protocol.

The predefined mask independently flags runs with mean FRONTIER writes at least 1 ms or mean fdatasync at least 1.5 ms. Two sixteen-writer candidate scores are flagged. The single remaining candidate result is insufficient for the required two eligible scores, so its displayed ratio cannot qualify that cell. The eight-writer throughput check already fails. No point-write guards or score-selected repeats are run.

## Strongest matching-binary reference

These historical-plus-fresh high scores use **puts/s / p99 ms from the throughput-winning invocation**. Cohort sizes are unequal; this view preserves the stronger retained reference without claiming a controlled source-effect estimate.

| Writers, batch64 | V4 Parallel best | Retained v7 best | Candidate best |
| --- | ---: | ---: | ---: |
| 1 | 85,618 / 1.195 | 72,647 / 1.373 | 73,671 / 1.289 |
| 8 | 388,372 / 2.511 | 478,798 / 2.653 | 412,971 / 2.377 |
| 16 | 595,941 / 2.967 | 606,948 / 4.065 | 485,276 / 9.639 |

The candidate's solo high is about 1.4% above the retained historical high. This does not establish v4 parity or qualify the RFC performance gate.

## Reproduction and validation

The [frozen matrix](artifacts/rfc-025-v7-post-submit-hash-20261029-matrix.json) has three mandatory batch screens and three conditional point guards. All batch screens complete regardless of performance. Each cell uses one warmup per arm followed by three alternating triplets of retained v4, retained v7, and candidate v7. The pass completes **36 invocations: 27 scored and 9 warmups**, with no replacement runs. Each invocation writes 750,000 puts in batch64, 1-KiB values, a 2-GiB SST target, and samples latency every ten batch calls. Fresh databases stay under the local repository and are removed. Each cell has a 60-second initial cooldown and 15-second gaps.

The host is the same Core i9-13900T and Solidigm SSD on local ext4. One-second audits detect no recognized competing builds, tests, profiles, or benchmarks. Production source, builds, tests, and profiles remain frozen during capture. Aggregate means can miss stalls; an eligible score is not evidence of stall-free storage.

The candidate passes **1,975 full tests and 55 focused WAL runtime tests**, with no retries, selected I/O-path skips, or full-suite skips, plus formatting, strict Clippy, dependency/spelling checks, and release build outside the sandbox. The initial compile catches a test-only `String` error conversion; its log is preserved before the corrected checks. The first restored check also flags truncated Git hunk labels in the evidence patch; [patch notes](artifacts/rfc-025-v7-post-submit-hash-20261029-patch-notes.json) preserve the original bytes and the label-only correction before final checks. After rejection, both Rust files are restored with fresh modification times and the canonical binary is replaced through a fresh inode. A fresh restored full check passes **1,973 tests**, followed by direct spelling verification.

The retained executable is `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8`; the rejected executable is `27079b2afcd608d455a9bb91ccdad04f1567337b54896f4a486f0c51ccdd54e0`. The [zero-context patch](artifacts/rfc-025-v7-post-submit-hash-20261029-candidate.patch) reconstructs the [128-input candidate manifest](artifacts/rfc-025-v7-post-submit-hash-20261029-source-sha256.tsv) from `1dce85d8`. The [checks](artifacts/rfc-025-v7-post-submit-hash-20261029-checks.json), [disposition](artifacts/rfc-025-v7-post-submit-hash-20261029-outcome.json), and [verification receipt](artifacts/rfc-025-v7-post-submit-hash-20261029-verification.json) preserve validation and restoration evidence.

The [driver](artifacts/rfc-025-v7-post-submit-hash-20261029-run.py), [analyzer](artifacts/rfc-025-v7-post-submit-hash-20261029-analyze.py), [fresh summary](artifacts/rfc-025-v7-post-submit-hash-20261029-summary.json), [historical comparison](artifacts/rfc-025-v7-post-submit-hash-20261029-best.json), and [verifier](artifacts/rfc-025-v7-post-submit-hash-20261029-verify.py) reproduce the results. Raw captures: [one writer](artifacts/rfc-025-v7-post-submit-hash-20261029-ext4-batch1.jsonl), [eight writers](artifacts/rfc-025-v7-post-submit-hash-20261029-ext4-batch8.jsonl), [sixteen writers](artifacts/rfc-025-v7-post-submit-hash-20261029-ext4-batch16.jsonl). The receipt adds 36 unique completed invocations to the inherited 5,067, for 5,103; the inherited interrupted invocation remains separate.
