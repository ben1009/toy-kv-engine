# RFC 025: vectored v7 DATA submissions, 2026-10-26

**Result: reject. Gathering DATA buffers reduces submissions, but throughput gains stay below 1% and point p99 rises 31.5%. The eleven retained optimizations remain.**

This SSD-only experiment follows the [native write/sync diagnostic](rfc-025-v7-frontier-link-20261026.md). It targets DATA submission overhead without changing the codec, grouping policy, FRONTIER write, whole-WAL `fdatasync`, or acknowledgement order. No RAM performance result is used for qualification.

## Candidate and fixed comparison

For active v7 WALs on ext-family filesystems, the I/O worker gathers up to eight physically contiguous DATA buffers into an explicit-offset `WRITEV`. A FRONTIER gap ends the vector; a singleton retains the existing `WRITE`. Each original aligned buffer and its reservation remain owned until the completion is reconciled. Submission ambiguity retains the complete vector and its iovec metadata. V4 and other filesystems keep their original submission path. Fewer submission entries do not imply the same reduction in physical NVMe commands.

The [plan](artifacts/rfc-025-v7-vectored-data-20261026-plan.json) precedes source changes and scoring. Each cell has four excluded warmups and exactly five alternating four-arm rounds: retained/candidate v4 controls and retained/candidate v7. Both use eight writers, 1-KiB values, and a 2-GiB SST target. Point writes use 100,000 puts with every-100-operation latency sampling; batch64 uses 750,000 puts and every-ten-batch sampling. Runs have a 30-second initial cooldown and ten seconds between commands.

Both cells require paired raw, v4-normalized and arm-median throughput ratios of at least 0.95, paired p99 at most 1.10, identical DATA bytes, and a sync-count ratio at most 1.05. At least one cell must gain 2% raw with normalized throughput at least unchanged, and DATA submissions must decrease. Passing results would trigger fixed 1/2/4-writer SSD guards and five additional rounds per initial cell. The initial phase fails, so those conditional captures are cancelled.

All **40 scored records, eight warmups, 48 unique IDs, and 485 clear host samples** remain. The audit checks recognized competing builds, tests, and benchmarks before, once per second during, and after each command. Sampling cannot exclude every brief or unrelated task. Every storage stall remains included.

## Results

Rates are per-arm medians. Changes are medians of same-round ratios; normalized change is `(candidate v7 / candidate v4) / (retained v7 / retained v4)`.

| SSD workload | Retained / candidate median puts/s | Paired raw | V4-normalized | CPU | p99 | DATA submissions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point, 8 writers | 15,719 / 15,687 | +0.8% | -1.2% | -0.6% | +31.5% | -27.0% |
| Batch64, 8 writers | 374,253 / 359,536 | +0.7% | +8.5% | +5.8% | -1.1% | -30.2% |

The batch normalized improvement coincides with weaker candidate-binary v4 controls: their median paired throughput ratio is 0.936. Neither raw gain reaches the fixed 2% requirement. Point latency also fails its bound. Arm medians and paired medians differ because the storage variation moves between rounds; the results do not isolate every difference as a code effect.

Point DATA submissions fall from a median 100,000 to 73,018. Median accumulated `fdatasync` time remains about 3.2 seconds; preparation wait rises from 221 to 261 ms. Batch submissions fall from 11,720 to 8,179, while CPU rises. Reducing these requests alone does not deliver a useful SSD improvement.

The highest matching scored rates exclude warmups:

| SSD workload | V4 control | Retained v7 | Candidate v7 |
| --- | ---: | ---: | ---: |
| Point | 17,393 | 15,863 | 15,859 |
| Batch64 | 401,119 | 378,965 | 386,515 |

The [peak identities](artifacts/rfc-025-v7-vectored-data-20261026-peaks.json) retain run IDs, binary hashes and raw-record hashes. These peaks do not override the complete fixed comparison or establish v4 parity.

## Checks and reproduction

Source base: `e2989d6d13b2d4b02d00212788e3988443206d75`. Retained executable: `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`. Prototype: `dc0e2b3977e23fb6c6f9b23d0d70ced4728a06daa207ed6c0ca563c77a9ab2cf`. Both saved release executables use feature `bench`.

- [Exact source patch](artifacts/rfc-025-v7-vectored-data-20261026-candidate.patch), [128 build-input hashes](artifacts/rfc-025-v7-vectored-data-20261026-source-sha256.tsv), [binary identities](artifacts/rfc-025-v7-vectored-data-20261026-binaries.json), and [environment](artifacts/rfc-025-v7-vectored-data-20261026-environment.json).
- [Driver](artifacts/rfc-025-v7-vectored-data-20261026-run.py), [analyzer](artifacts/rfc-025-v7-vectored-data-20261026-analyze.py), and [verified summary](artifacts/rfc-025-v7-vectored-data-20261026-summary.json).
- [Point raw records](artifacts/rfc-025-v7-vectored-data-initial-ext4-point8-20261026.jsonl) and [batch raw records](artifacts/rfc-025-v7-vectored-data-initial-ext4-batch64-20261026.jsonl); each stem also has `-runs.tsv`, `-host.jsonl`, and `-run.sh` files.
- [Checks](artifacts/rfc-025-v7-vectored-data-20261026-checks.json), [restoration record](artifacts/rfc-025-v7-vectored-data-20261026-restoration.json), [verifier](artifacts/rfc-025-v7-vectored-data-20261026-verify.py), and [verification result](artifacts/rfc-025-v7-vectored-data-20261026-verification.json).

The prototype passes all **1,969 tests** in `cargo make check`, with zero skips or retries. Six focused tests execute outside the sandbox, including actual direct vectored I/O across a FRONTIER gap, short completions, metadata lifetime during ambiguous submission, retained ownership of later requests, exact v7 recovery, and reservation retirement. Direct spelling checks and the release build pass.

Both modified Rust files and the canonical executable are restored before the separate asynchronous-extent experiment. The [historical restoration record](artifacts/rfc-025-v7-vectored-data-20261026-restoration.json) binds their original hashes. No vectored submission change is retained, and all disposable benchmark databases are removed. Point parity and the complete Stage 8 gate remain unmet.

[WAL documentation](../README.md).
