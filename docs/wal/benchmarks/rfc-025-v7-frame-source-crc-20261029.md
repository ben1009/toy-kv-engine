# RFC 025: physical DATA CRC from source components, 2026-10-29

**No qualified optimization retained.** The one- and eight-writer screens miss the throughput and p99 bounds; sixteen writers provide only one eligible score per build and remain inconclusive. Storage latency differs substantially between several winning runs, so these rates do not isolate the CRC implementation's effect. The codec and retained canonical executable are restored, with the exact candidate binary and patch preserved.

## Change and correctness

This P2 candidate runs independently on retained v7, without the rejected P0/P1 pipeline changes. It updates physical DATA CRC from the constructed common header, fragment header, original logical payload, and constant zero padding. It does not reread the completed output payload for CRC. Whole-frame initialization stays unchanged, as do the wire CRC coverage, canonical entry CRC, logical SHA256, format, allocation accounting, scheduling, workers, and one-sync protocol. FRONTIER encoding and the decoder retain their original CRC implementation.

Copying and `crc.update(payload)` remain separate operations and still read the payload twice. This is source-based CRC construction, not a fused single-load copy/CRC primitive. It also adds CRC update boundaries; a free CPU gain cannot be assumed. It is distinct from the earlier [canonical entry CRC experiment](rfc-025-v7-entry-crc-20261029.md).

Existing byte-exact DATA golden vectors, fixed record digests, fragment boundary tests, dirty-buffer coverage, and the decoder's unchanged complete-frame CRC validate the output independently. Both encoder-owned and checked materialization paths are exercised. The candidate passes **1,973 full tests and 69 focused codec/runtime tests**, without retries or full-suite skips, plus formatting, strict Clippy, dependency/spelling checks, and release build outside the sandbox.

## Fresh best scores

Each row selects the highest-throughput eligible scored invocation for each exact executable, and uses p99 and process CPU from that same invocation. Sixteen-writer numbers are context only: one eligible score per arm is below the required two.

| Batch64 writers | Retained puts/s | Candidate puts/s | Retained / candidate p99 ms | Process CPU seconds | Eligible retained / candidate | Check |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 67,547 | 34,729 | 1.359 / 10.480 | 6.963 / 7.083 | 3 / 3 | Fail |
| 8 | 350,478 | 302,364 | 3.149 / 8.279 | 4.417 / 4.437 | 3 / 3 | Fail |
| 16 | 175,289 | 582,341 | 30.331 / 4.475 | 6.177 / 3.597 | 1 / 1 | Insufficient |

The solo winners have mean fdatasync times of 0.309 / 0.893 ms and nearly equal process CPU (candidate +1.7%). The eight-writer winners have mean fdatasync of 0.307 / 0.380 ms and mean FRONTIER writes of 0.067 / 0.145 ms; CPU differs by only +0.5%. These storage differences prevent interpreting the throughput loss as CRC CPU cost.

The predeclared mask independently flags mean FRONTIER writes at least 1 ms or mean fdatasync at least 1.5 ms. Two scored runs per build are flagged at sixteen writers. Several low-rate solo/eight-writer scores still pass that aggregate mask, demonstrating that eligibility does not establish stall-free I/O. The lone eligible sixteen-writer candidate is faster than its fresh control, but that is insufficient to qualify the cell. No extra repeats are selected, and conditional point guards do not run.

## Strongest matching-binary reference

Historical-plus-fresh retained highs use the exact retained executable and the same workload. Their cohorts are unequal; they provide context, not a controlled estimate of this source change. Every p99 belongs to its throughput-winning invocation.

| Batch64 writers | V4 Parallel best puts/s / p99 ms | Retained v7 best | Candidate best |
| --- | ---: | ---: | ---: |
| 1 | 85,618 / 1.195 | 72,647 / 1.373 | 34,729 / 10.480 |
| 8 | 388,372 / 2.511 | 478,798 / 2.653 | 302,364 / 8.279 |
| 16 | 595,941 / 2.967 | 606,948 / 4.065 | 582,341 / 4.475 |

The candidate does not establish v4 parity or qualify the RFC performance gate. This experiment also does not establish that physical frame CRC readback is unimportant; the streaming API tested here does not provide true fused copy/CRC.

## Reproduction and restoration

The [plan](artifacts/rfc-025-v7-frame-source-crc-20261029-plan.json) and [matrix](artifacts/rfc-025-v7-frame-source-crc-20261029-matrix.json) specify all three mandatory batch screens: one warmup per arm, then three alternating retained/candidate pairs. All **24 invocations complete: 18 scored and 6 warmups**. Each writes 750,000 puts in batch64 with 1-KiB values, a 2-GiB SST target, and latency sampling every ten calls. Captures use local ext4, fresh databases under the repository, a 60-second initial cooldown per cell, and 15-second gaps; scratch databases are removed. Host audits find no recognized competing builds, tests, profiles, or benchmarks, and production source stays frozen during capture.

The [checks](artifacts/rfc-025-v7-frame-source-crc-20261029-checks.json) bind the candidate validation. After nonqualification, the one changed codec file is restored with a fresh modification time and the retained canonical executable is replaced through a fresh inode. The restored full check passes **1,973 tests with one recorded retry** after the simple-leveled compaction integration test times out. That test then passes in isolation, and a fresh complete suite passes **1,973 tests with retries disabled**, followed by direct spelling verification. An initial zero-retry attempt mistakenly included Criterion benchmark targets without their longer benchmark profile and timed out; the attempt is preserved in the [disposition](artifacts/rfc-025-v7-frame-source-crc-20261029-outcome.json), followed by the correct library and test scope.

The retained executable is `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8`; the candidate is `ea86ec239062a7c17bec6c262dd1ff4b0628d01cdef4965eaa7a5a20d4ef297d`. The [patch](artifacts/rfc-025-v7-frame-source-crc-20261029-candidate.patch) reconstructs the [128-input manifest](artifacts/rfc-025-v7-frame-source-crc-20261029-source-sha256.tsv) from `474ce01e`.

The [driver](artifacts/rfc-025-v7-frame-source-crc-20261029-run.py), [analyzer](artifacts/rfc-025-v7-frame-source-crc-20261029-analyze.py), [fresh summary](artifacts/rfc-025-v7-frame-source-crc-20261029-summary.json), [historical comparison](artifacts/rfc-025-v7-frame-source-crc-20261029-best.json), and [verifier](artifacts/rfc-025-v7-frame-source-crc-20261029-verify.py) reproduce the comparison. Raw captures: [one writer](artifacts/rfc-025-v7-frame-source-crc-20261029-ext4-batch1.jsonl), [eight writers](artifacts/rfc-025-v7-frame-source-crc-20261029-ext4-batch8.jsonl), [sixteen writers](artifacts/rfc-025-v7-frame-source-crc-20261029-ext4-batch16.jsonl). The [receipt](artifacts/rfc-025-v7-frame-source-crc-20261029-verification.json) adds 24 unique completed invocations to the inherited 5,199, for **5,223**, plus the inherited interrupted invocation without JSON.
