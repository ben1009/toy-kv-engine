# RFC 026 V7/V8 Point Commit-Order Trace on GCP

**Date:** 2026-11-14 UTC

**Source commit:** `e3b4de1225cea0ef928e271b0b3012cc68c09471`

**Source patch SHA-256:** `ffa3def5cf6cb1948711e5315f7a666d9b884c38ac5eaea62e829c8ef9951b3a`
**Benchmark binary SHA-256:** `149eae34d27403ff2cd7c0cf77a3cac6376c4c076ae9ef9aa46aad9645b91dd5`

## Finding

The V8 commit-order gap is already visible by **WAL return**. Commit timestamp reservation and WAL ticket admission had no observed inversions in either version. The measured inversion rate at WAL return was 0.249% for V7 and 0.450% for V8. MemTable completion and MVCC entry did not materially increase those rates, so this trace does not point to MVCC as the origin of the extra reordering.

This identifies where the difference is first observable, not its cause. In particular, multiple tickets covered by one durable-frontier publication share a timestamp; those ties are excluded from the frontier inversion denominator. The trace therefore cannot establish the order of individual writers inside one durability group or distinguish publication handoff from writer wakeup and scheduling.

## GCP comparison

The runner completed two diagnostic traces and five counterbalanced score pairs: 12 successful runs total. Each trace retained 1,024 complete commit records. Score runs did not enable commit-order tracing; latency was sampled every 100 operations.

| 16-writer Point metric | V7 Parallel | V8 Direct/400 + Sliding Fallocate |
| --- | ---: | ---: |
| Median throughput | 48.01k puts/s | 33.44k puts/s |
| Median paired throughput change | — | −30.69% |
| Median process CPU | 6.040 s | 7.259 s |
| Median sampled p50 | 0.303 ms | 0.476 ms |
| Median sampled p95 | 0.561 ms | 0.634 ms |
| Median sampled p99 | 0.843 ms | 1.382 ms |
| Paired throughput wins | — | 0/5 |

V8 used Direct FRONTIER, a fixed 400 μs coalescing budget, and a 4 MiB sliding `fallocate` window. The 16-vCPU C4 host had 8 physical cores / 16 threads, two Local SSDs in RAID0, ext4 mounted with `rw,noatime,stripe=256`, and kernel `7.0.0-1017-gcp`.

## Commit-order inversions

Each trace contains 1,024 records, yielding 523,776 possible commit-timestamp pairs. An inversion means the recorded stage order disagrees with commit timestamp order. The frontier stage excludes pairs with equal monotonic timestamps.

| Recorded stage | V7 inversions | V8 inversions |
| --- | ---: | ---: |
| Timestamp reserved | 0 / 523,776 (0%) | 0 / 523,776 (0%) |
| WAL ticket admitted | 0 / 523,776 (0%) | 0 / 523,776 (0%) |
| Durable frontier published | 0; 6,249 tied pairs excluded | 0; 7,464 tied pairs excluded |
| WAL returned | 1,303 / 523,776 (0.249%) | 2,359 / 523,776 (0.450%) |
| MemTable publish complete | 1,351 / 523,776 (0.258%) | 2,355 / 523,776 (0.450%) |
| MVCC publication entry | 1,346 / 523,776 (0.257%) | 2,364 / 523,776 (0.451%) |
| MVCC publication complete | 737 / 523,776 (0.141%) | 1,760 / 523,776 (0.336%) |

The V8 rate at WAL return is about 1.8× V7's. The rate stays nearly unchanged through MemTable completion and MVCC entry. It falls by MVCC publication completion in both traces, though V8 remains higher.

The median durable-publication-to-WAL-return interval was 60.0 μs for V7 and 38.4 μs for V8. V8 therefore showed more return-order inversions despite a shorter median interval. This is consistent with a difference in return ordering or scheduling; it does not prove that wakeup scheduling caused the inversions.

The inversion percentages are descriptive for one trace per version. Pairwise comparisons share records and are not independent statistical samples. Equal-time frontier pairs also prevent a strict within-group ordering claim.

## Interpretation and next measurement

The trace narrows the next question to the boundary between durable publication and individual WAL returns. A follow-up should correlate each frontier group with its writers' return events and capture wakeup-to-running delay for those same writers. The current data do not justify changing the MVCC sequencer or WAL protocol.

## Result verification and retention

The local runner verifier reported all 12 scheduled runs complete, with matching successful result and metadata files. `remote-exit.json` reported `failed: false` and `runs_started: 12`. The transferred archive SHA-256 matched the retained checksum metadata. The raw archive remains local and was not added to GitHub.

The VM and its TCP/22 firewall rule remain retained until the configured 60-minute auto-delete deadline, **2026-11-14 17:06:23 UTC**. The rule was verified to match the approved SSH `/32`; the isolated VPC and subnet were left untouched.
