# RFC 025: asynchronous v7 extent initialization, 2026-10-26–27

**Result: retained at the maintainer's explicit request, despite the failed single-writer p99 guard. The [serial follow-up](rfc-025-v7-serial-preparation-20261027.md) addresses serial preparation and includes 16-writer measurements. The original failed measurements remain unchanged.**

## Measured results and retention decision

The initial eight-writer SSD phase passes: paired batch64 throughput gains 6.0% raw and 4.9% normalized, with 9.2% lower p99. Point throughput gains 0.7% raw; measured preparation wait falls 73.7% and p99 falls 36.7%. The subsequent low-concurrency phase fails only the one-writer p99 limit. Its median paired p99 ratio is 1.231 against a frozen ceiling of 1.10. Absolute per-arm median p99 increases from 0.759 to 0.934 ms, about 175 microseconds. Throughput is effectively unchanged.

Rates below are per-arm medians; changes are medians of same-round ratios. Normalization is `(candidate v7 / candidate v4) / (original v7 / original v4)`.

| SSD workload | Original / async median puts/s | Paired raw | V4-normalized | CPU | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Point, 1 writer | 2,238 / 2,235 | -0.1% | -0.1% | +1.7% | +23.1% |
| Point, 2 writers | 4,321 / 4,272 | -1.1% | -1.6% | -1.6% | -37.2% |
| Point, 4 writers | 8,425 / 8,427 | +0.0% | +0.3% | -2.2% | -23.7% |
| Point, 8 writers | 15,542 / 15,765 | +0.7% | -0.4% | +1.2% | -36.7% |
| Batch64, 8 writers | 360,001 / 386,199 | +6.0% | +4.9% | -10.0% | -9.2% |

All **76 scored records, 20 warmups, 96 unique IDs and 1,006 clear host samples** remain. All scheduled low-concurrency cells complete. The planned confirmation is cancelled after the failed guard. The original protocol would reject this source. The maintainer then explicitly instructs **“Keep and Optimize for 1 writer”**. That instruction overrides automatic rejection: the source is retained in `0ffebd516b865b01371ed65014ac0347ae2fb420`, and the serial scheduling fix is tracked separately. This is a maintainer retention decision, not a passing performance qualification or a correctness failure. See the [original decision](artifacts/rfc-025-v7-async-extent-20261026-decision.json) and [explicit retention record](artifacts/rfc-025-v7-async-extent-20261026-maintainer-retention.json).

Point preparation wait falls from a per-arm median 228 to 62 ms per 100,000 puts. Accumulated `fdatasync` time remains about 3.2 seconds. Batch sync counts decrease by a median paired 6.3% without changing their policy. These observations explain why readiness savings alone are insufficient to claim point throughput parity. Background zero writes competing with serial syncs are a hypothesis for the light-load tail regression; this experiment does not isolate their complete kernel-level cause.

V4 controls contain moving stalls, especially in the point8 and point1 cells. Every run remains included. Sampled process isolation does not remove storage variability or prove that every difference is caused solely by the candidate. There is no complete Stage 8 performance qualification.

The highest matching initial eight-writer scored rates, excluding warmups, are:

| SSD workload | V4 control | Original v7 | Async v7 |
| --- | ---: | ---: | ---: |
| Point | 17,185 | 15,832 | 15,863 |
| Batch64 | 384,134 | 370,084 | 418,117 |

[Peak identities](artifacts/rfc-025-v7-async-extent-20261026-peaks.json) preserve all five cells' exact hashes and run IDs. The stronger historical retained v7 batch peak of 480,373 puts/s remains a separate cohort in the [earlier SSD rerun](rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026.md); it is not pooled here. Point parity remains unmet. This cohort measures 1/2/4/8 writers; the maintainer subsequently requests a separate 16-writer follow-up.

## Candidate and durability

The retained ext-family v7 path prepares an entire newly allocated 1-MiB extent synchronously. This candidate allocates the same bounded extent, initializes only the required 128-KiB-aligned prefix on demand, and wakes one background worker to prepare the unused suffix. It does not allocate another lookahead extent or require an additional spool reservation. RAM and other filesystem selectors retain their existing behavior.

A shared preparation mutex and monotonic cursor protect DATA, FRONTIER, demand initialization and large-DATA allocation-only skips. Readiness is published only after successful initialization; both DATA and FRONTIER check it before writing. Foreground demand can prepare its own next chunk if the worker is delayed. The background target is coalesced through a bounded wake channel, and each background operation initializes at most 128 KiB of already allocated space. The sleeping worker holds a weak runtime reference.

An unused-suffix initialization error is cached under the cursor lock. It does not invalidate an already acknowledged ready prefix; later demand crossing that prefix consumes the sticky error and poisons the affected operation. Runtime close joins the worker. Sealing explicitly joins it before truncation, preventing a later background write from re-extending a frozen inode. That barrier stops only preparation, preserving existing failure/resume behavior for the DATA runtime.

The DATA codec, logical and physical hashes, ticket ordering, FRONTIER encoding, direct-I/O selectors, and one whole-WAL `fdatasync` followed by publication remain unchanged. The candidate changes preparation scheduling; the grouping policy and coalescing deadline stay fixed, although realized group and sync counts can change with timing.

## Fixed qualification

The plan is frozen before source changes and scoring. All cells use ext4 on the local SSD, 1-KiB values and a 2-GiB SST target. There are no RAM performance captures. Each cell has four excluded warmups, alternating four-arm rounds, a 30-second initial cooldown and ten seconds between runs. Arms are retained/candidate v4 controls and retained/candidate v7. Recognized competing builds, tests and benchmarks are sampled before, once per second during, and after every command. All stalls remain in the results.

The initial phase has five rounds each of eight-writer point writes (100,000 puts, sampling every 100 operations) and batch64 (750,000 puts, sampling every ten batches). Both require paired raw, normalized and arm-median throughput at least 0.95, paired p99 at most 1.10, identical DATA bytes, and a sync-count ratio at most 1.05. At least one cell must gain 2% raw with normalized throughput at least unchanged. Point preparation wait must decrease at least 20%. CPU includes the background thread during the measurement window and is reported separately; the packer preparation timer measures readiness wait, not total background I/O.

Passing the initial phase triggers three rounds each of one- and two-writer point writes (20,000 puts) and four-writer point writes (50,000 puts), all sampling every 100 operations. These require paired raw, normalized and arm-median throughput at least 0.95, p99 at most 1.10 and identical DATA bytes. Passing those checks triggers exactly five more rounds per initial cell with the unchanged source and executable. Initial and confirmation records are pooled as ten raw pairs per cell before applying the initial guards again. No extra attempt is selected by score.

## Correctness and provenance

The source base is `e2989d6d13b2d4b02d00212788e3988443206d75`. The retained executable is `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`; the prototype is `ef112fd0dea7f292a0be9ab47eae6276242dd6b290efccdf4335f03e41b17d0a`. Both saved release executables use feature `bench`.

The prototype passes `cargo make check`: 1,968 tests, zero skips or retries. Eight focused tests also execute outside the sandbox. Five new tests pause the background worker to verify ready-prefix acknowledgement, foreground progress, close without another write, large-DATA skip safety, cached failure propagation, exact acknowledged recovery, and preparation quiescence before either truncation or resumed writes. Existing tests cover initialized garbage, preserved DATA/FRONTIER bytes and physical caps. Direct spelling checks and the release build pass.

Two earlier fixture corrections remain in the check record: failpoint configuration returns a string error, and a single value must fit the existing u16 length. The final large-DATA fixtures use four 32-KiB values. No production value limit changes.

## Reproduction and preserved evidence

- [Frozen plan](artifacts/rfc-025-v7-async-extent-20261026-plan.json), [source patch](artifacts/rfc-025-v7-async-extent-20261026-candidate.patch), [128 input hashes](artifacts/rfc-025-v7-async-extent-20261026-source-sha256.tsv), [executable identities](artifacts/rfc-025-v7-async-extent-20261026-binaries.json), and [environment](artifacts/rfc-025-v7-async-extent-20261026-environment.json).
- [Driver](artifacts/rfc-025-v7-async-extent-20261026-run.py), [analyzer](artifacts/rfc-025-v7-async-extent-20261026-analyze.py), [initial summary](artifacts/rfc-025-v7-async-extent-20261026-summary.json), and [low-concurrency summary](artifacts/rfc-025-v7-async-extent-20261026-low-summary.json).
- Raw records: [point1](artifacts/rfc-025-v7-async-extent-initial-ext4-point1-20261026.jsonl), [point2](artifacts/rfc-025-v7-async-extent-initial-ext4-point2-20261026.jsonl), [point4](artifacts/rfc-025-v7-async-extent-initial-ext4-point4-20261026.jsonl), [point8](artifacts/rfc-025-v7-async-extent-initial-ext4-point8-20261026.jsonl), and [batch64](artifacts/rfc-025-v7-async-extent-initial-ext4-batch64-20261026.jsonl). Each stem has its exact `-runs.tsv`, `-host.jsonl` and `-run.sh` files.
- [Checks](artifacts/rfc-025-v7-async-extent-20261026-checks.json), [historical restoration before retention](artifacts/rfc-025-v7-async-extent-20261026-restoration.json), [verifier](artifacts/rfc-025-v7-async-extent-20261026-verify.py), and [verification result](artifacts/rfc-025-v7-async-extent-20261026-verification.json).

A temporary restoration before the maintainer's instruction also passes all 1,963 original tests. The explicit retention then reinstates the exact tested asynchronous source and executable. Both source states and all build logs remain identifiable; no failed guard is erased. All disposable benchmark databases are removed. The follow-up keeps source changes and measured evidence separate, with no PR or push.

[WAL documentation](../README.md).
