# Dynamic admitted-window grouping experiment

The draft candidate restores the previously tested producer-framing and post-submit SHA candidate, then changes its grouping window. It is **not performance-qualified**. The changes are separated into reviewable commits; the [source patch](artifacts/rfc-025-v7-dynamic-window-20261030-source.patch.gz) records the complete measured candidate; decompressing it yields the captured patch whose SHA-256 is recorded in the environment metadata.

The packer observes admitted tickets independently of readiness, refreshes the window whenever admission or readiness wakes it, and submits a contiguous ready prefix. It uses one 100 µs maximum deadline for unfinished successors. A ready solo ticket gets up to 10 µs to join another producer only if one is already preparing; no concurrent producer means no deliberate wait. A full eight-ticket window, close, or poison also ends the join. The preparation counter is a bounded scheduling hint, not a durability condition. FRONTIER encoding, SHA definition, sync and wire format are unchanged relative to the restored candidate.

## Verification

The final `cargo make check` passed **1,992 tests**, with `NEXTEST_RETRIES=0`, two test threads and local scratch. One preceding full run timed out in an unrelated simple-leveled compaction test; that test passed individually and the next two full checks passed. The six new concurrency tests passed ten consecutive runs on the final source. They cover a later admission extending an open group, a concurrent preparer joining a solo group, an uncontended solo producer, a full window, poison, and close. The source and frozen binaries match the [capture verification](artifacts/rfc-025-v7-dynamic-window-20261030-verification.json).

Both CI failures on the prior PR head passed on rerun; those checks do not cover this candidate. The original sanitizer failure was `clean_disable_archives_final_boundary_and_drains_reclaimable_sources`, which encountered an Active v7 WAL and failed its expected disable; the earlier dependency-review failure was a GitHub checkout connection error. A passing rerun does not establish the cause of the sanitizer failure.

## Batch64 screen

The fixed-window column is the preserved producer-framing candidate, not the main-branch v7 baseline. Batch workloads used 200,000 puts, 1 KiB values and one warmup plus three scored runs per arm. Cells ran with 5 seconds initial and 2 seconds between runs on the same known-unstable ext4 SSD. Entries show the highest eligible throughput with its own p99; parentheses are eligible runs out of three. The screen flags mean FRONTIER write >= 0.12 ms or mean WAL `fdatasync` >= 0.6 ms. Mean filters can miss DATA stalls, so unflagged does not mean clean.

| Writers | v4 Parallel puts/s / p99 ms | Fixed window puts/s / p99 ms | Dynamic window puts/s / p99 ms |
| ---: | ---: | ---: | ---: |
| 1 | 79,057 / 1.172 (2/3) | 76,493 / 1.224 (3/3) | 80,844 / 1.202 (2/3) |
| 8 | — (0/3) | — (0/3) | — (0/3) |
| 16 | 587,828 / 2.955 (2/3) | 549,671 / 6.043 (3/3) | 513,975 / 5.314 (2/3) |

At 8 writers, all samples were flagged. Fixed-window runs formed 2,835–2,963 groups and 2,593–2,815 solo groups; dynamic-window runs formed 2,391–2,406 groups and 1,684–1,721 solo groups. These counts demonstrate behavior under these runs, but the storage outliers make a throughput conclusion unsafe.

At 16 writers, the fixed-window best eligible run formed 2,135 groups / 1,328 solo, and the dynamic best formed 1,497 / 205. The dynamic sample was 6.5% slower but had 12.1% lower p99 than fixed. This short, noisy screen cannot establish v4 parity or a source-induced regression.

## Point-write guard

The guard used 20,000 puts per run with the same fixed-window and dynamic binaries, the same warmup/scored schedule, and the same filter. The best eligible sample and its own p99 are shown.

| Writers | Fixed window puts/s / p99 ms | Dynamic window puts/s / p99 ms |
| ---: | ---: | ---: |
| 1 | 2,288 / 0.755 (3/3) | 1,972 / 2.428 (3/3) |
| 8 | 13,952 / 2.188 (3/3) | 15,857 / 1.165 (3/3) |
| 16 | 23,774 / 1.509 (1/3) | — (0/3) |

The one-writer data contain a 393 puts/s / 16.223 ms dynamic run and a 1,785 puts/s / 2.565 ms fixed run even though their sync and FRONTIER means were below the filter limits. The one-writer discrepancy is therefore not a reliable attribution to the code; the no-contender test verifies only that the new group policy does not deliberately wait. The 16-writer point guard is under-sampled. Point performance is not qualified.

## Raw scored runs

### Batch64

| Cell | Arm | Round | Puts/s | p99 ms | Groups | Solo | FRONTIER mean ms | Sync mean ms | Flagged |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| ext4-batch1 | retained-v7 | 1 | 76,493 | 1.224 | 3125 | 3125 | 0.024 | 0.306 | no |
| ext4-batch1 | candidate-v7 | 1 | 12,502 | 16.836 | 3125 | 3125 | 1.082 | 1.973 | yes |
| ext4-batch1 | retained-v4 | 1 | 11,674 | 17.324 | 3125 | 3125 | — | 3.361 | yes |
| ext4-batch1 | candidate-v7 | 2 | 80,844 | 1.202 | 3125 | 3125 | 0.025 | 0.305 | no |
| ext4-batch1 | retained-v4 | 2 | 77,292 | 1.223 | 3125 | 3125 | — | 0.332 | no |
| ext4-batch1 | retained-v7 | 2 | 70,143 | 1.257 | 3125 | 3125 | 0.031 | 0.313 | no |
| ext4-batch1 | retained-v4 | 3 | 79,057 | 1.172 | 3125 | 3125 | — | 0.325 | no |
| ext4-batch1 | retained-v7 | 3 | 69,357 | 1.274 | 3125 | 3125 | 0.042 | 0.314 | no |
| ext4-batch1 | candidate-v7 | 3 | 75,660 | 1.251 | 3125 | 3125 | 0.029 | 0.309 | no |
| ext4-batch8 | retained-v4 | 1 | 84,788 | 25.941 | 3085 | 3042 | — | 2.528 | yes |
| ext4-batch8 | retained-v7 | 1 | 84,614 | 17.720 | 2949 | 2796 | 1.124 | 1.355 | yes |
| ext4-batch8 | candidate-v7 | 1 | 85,635 | 16.901 | 2404 | 1721 | 0.951 | 1.436 | yes |
| ext4-batch8 | retained-v7 | 2 | 80,207 | 24.691 | 2963 | 2815 | 1.492 | 1.277 | yes |
| ext4-batch8 | candidate-v7 | 2 | 85,638 | 25.642 | 2406 | 1703 | 1.036 | 1.198 | yes |
| ext4-batch8 | retained-v4 | 2 | 81,499 | 25.522 | 3056 | 2986 | — | 2.745 | yes |
| ext4-batch8 | candidate-v7 | 3 | 77,416 | 22.904 | 2391 | 1684 | 1.272 | 1.248 | yes |
| ext4-batch8 | retained-v4 | 3 | 84,294 | 23.462 | 3092 | 3056 | — | 2.618 | yes |
| ext4-batch8 | retained-v7 | 3 | 84,823 | 18.243 | 2835 | 2593 | 1.235 | 1.149 | yes |
| ext4-batch16 | candidate-v7 | 1 | 35,060 | 51.213 | 2352 | 1734 | 5.486 | 7.551 | yes |
| ext4-batch16 | retained-v4 | 1 | 36,994 | 77.967 | 2905 | 2679 | — | 12.657 | yes |
| ext4-batch16 | retained-v7 | 1 | 536,879 | 5.638 | 2067 | 1162 | 0.059 | 0.354 | no |
| ext4-batch16 | retained-v4 | 2 | 587,828 | 2.955 | 3134 | 3132 | — | 0.377 | no |
| ext4-batch16 | retained-v7 | 2 | 483,081 | 5.276 | 2140 | 1335 | 0.063 | 0.362 | no |
| ext4-batch16 | candidate-v7 | 2 | 467,257 | 4.694 | 1482 | 254 | 0.067 | 0.343 | no |
| ext4-batch16 | retained-v7 | 3 | 549,671 | 6.043 | 2135 | 1328 | 0.056 | 0.339 | no |
| ext4-batch16 | candidate-v7 | 3 | 513,975 | 5.314 | 1497 | 205 | 0.067 | 0.344 | no |
| ext4-batch16 | retained-v4 | 3 | 561,933 | 3.005 | 3128 | 3123 | — | 0.387 | no |

### Point

| Cell | Arm | Round | Puts/s | p99 ms | Groups | Solo | FRONTIER mean ms | Sync mean ms | Flagged |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| ext4-point1 | candidate-v7 | 1 | 393 | 16.223 | 20000 | 20000 | 0.027 | 0.309 | no |
| ext4-point1 | retained-v7 | 1 | 2,288 | 0.755 | 20000 | 20000 | 0.020 | 0.245 | no |
| ext4-point1 | retained-v7 | 2 | 2,271 | 0.749 | 20000 | 20000 | 0.020 | 0.247 | no |
| ext4-point1 | candidate-v7 | 2 | 1,967 | 2.429 | 20000 | 20000 | 0.020 | 0.245 | no |
| ext4-point1 | candidate-v7 | 3 | 1,972 | 2.428 | 20000 | 20000 | 0.019 | 0.248 | no |
| ext4-point1 | retained-v7 | 3 | 1,785 | 2.565 | 20000 | 20000 | 0.018 | 0.244 | no |
| ext4-point8 | retained-v7 | 1 | 8,213 | 4.053 | 15120 | 11092 | 0.057 | 0.310 | no |
| ext4-point8 | candidate-v7 | 1 | 15,478 | 1.170 | 10472 | 3553 | 0.019 | 0.253 | no |
| ext4-point8 | candidate-v7 | 2 | 15,857 | 1.165 | 11094 | 4395 | 0.018 | 0.252 | no |
| ext4-point8 | retained-v7 | 2 | 13,952 | 2.188 | 15853 | 12223 | 0.028 | 0.275 | no |
| ext4-point8 | retained-v7 | 3 | 9,023 | 4.015 | 14910 | 10818 | 0.057 | 0.299 | no |
| ext4-point8 | candidate-v7 | 3 | 15,612 | 1.217 | 11093 | 4291 | 0.021 | 0.254 | no |
| ext4-point16 | candidate-v7 | 1 | 3,012 | 28.885 | 13106 | 7705 | 1.313 | 0.973 | yes |
| ext4-point16 | retained-v7 | 1 | 2,772 | 30.387 | 15724 | 12740 | 1.473 | 1.123 | yes |
| ext4-point16 | retained-v7 | 2 | 3,007 | 28.073 | 15869 | 12963 | 1.316 | 0.947 | yes |
| ext4-point16 | candidate-v7 | 2 | 2,892 | 26.769 | 13189 | 7817 | 1.391 | 1.031 | yes |
| ext4-point16 | candidate-v7 | 3 | 12,600 | 19.458 | 11253 | 4599 | 0.304 | 0.375 | yes |
| ext4-point16 | retained-v7 | 3 | 23,774 | 1.509 | 15383 | 11792 | 0.023 | 0.276 | no |

The complete batch capture has [summary](artifacts/rfc-025-v7-dynamic-window-20261030-summary.json), [matrix](artifacts/rfc-025-v7-dynamic-window-20261030-matrix.json), and raw data for [1 writer](artifacts/rfc-025-v7-dynamic-window-20261030-ext4-batch1.jsonl), [8 writers](artifacts/rfc-025-v7-dynamic-window-20261030-ext4-batch8.jsonl), and [16 writers](artifacts/rfc-025-v7-dynamic-window-20261030-ext4-batch16.jsonl). The point capture has [summary](artifacts/rfc-025-v7-dynamic-point-20261030-summary.json), [matrix](artifacts/rfc-025-v7-dynamic-point-20261030-matrix.json), and raw data for [1 writer](artifacts/rfc-025-v7-dynamic-point-20261030-ext4-point1.jsonl), [8 writers](artifacts/rfc-025-v7-dynamic-point-20261030-ext4-point8.jsonl), and [16 writers](artifacts/rfc-025-v7-dynamic-point-20261030-ext4-point16.jsonl). The archived drivers use the repository-local `target/wal-opt/20261030-v7-dynamic-group` directory; matching manifests, host samples, plans, binary identities, and capture logs are stored beside the raw data.
