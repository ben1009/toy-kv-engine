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

### WAL-return inversions by durability group

I grouped records by `frontier_ticket_end`, which is assigned to every ticket covered by the same durable-frontier publication. For each pair ordered by `commit_ts`, I classified the WAL-return inversion as intra-group when both records had the same frontier end, and inter-group otherwise. All 1,024 records in each trace had a frontier group; the intra-group pair counts exactly match the equal-timestamp frontier pairs excluded above.

| Version | Intra-group inversions / pairs | Intra-group rate | Inter-group inversions / pairs | Inter-group rate |
| --- | ---: | ---: | ---: | ---: |
| V7 | 1,303 / 6,249 | 20.85% | 0 / 517,527 | 0% |
| V8 | 2,355 / 7,464 | 31.55% | 4 / 516,312 | 0.00077% |

This accounts for the overall inversion totals: all 1,303 V7 inversions and 2,355 of 2,359 V8 inversions occurred within a durability group. Of the 1,056 additional V8 inversions in this trace, 1,052 were intra-group and four were inter-group. The captured records represented 89 V7 groups and 69 V8 groups; 33 and 59 groups, respectively, contained all 16 traced writers.

The observed difference is therefore concentrated in the order in which writers covered by one durable publication return. It does not point to cross-group Driver Handoff as the source of most inversions. This remains descriptive evidence from one 1,024-record trace per version: pair comparisons are dependent, and captured groups at the trace boundaries may be partial. The result does not identify whether intra-group ordering comes from notification behavior, scheduling, or another part of the writer return path.

### Comparison at the same complete group size

The offline analyzer now stratifies by the number of ticketed Point writes in a group and excludes any group whose full contiguous ticket range was not captured. This avoids attributing the difference solely to the larger groups in V8.

| Version | Fully observed groups | Complete 16-ticket groups | Records | WAL-return inversions / comparable pairs | Pair-weighted rate | Median per-group rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| V7 | 88 | 33 | 528 | 695 / 3,960 | 17.55% | 14.17% |
| V8 | 68 | 59 | 944 | 2,209 / 7,080 | 31.20% | 28.33% |

The final observed group was incomplete in each trace and excluded from this size-stratified table: V7 captured 3 of 16 tickets and V8 captured 14 of 16. At the same complete 16-ticket group size, V8's pair-weighted inversion rate remains higher by 13.65 percentage points (about 1.78×). Thus the larger share of full groups does not by itself explain the observed intra-group difference. This is still one trace per version, and pair comparisons within a group share records; treat the rates as descriptive, not independent statistical evidence.

The reproducible calculation is in `scripts/gcp_commit_order_trace_runner.py`; offline tests cover intra/inter-group classification, complete-size stratification, a truncated tail group, and equal-time ties.

## Interpretation and next measurement

The trace narrows the next question to writer wakeup and resume ordering within each durability group. A follow-up should correlate each frontier group with its writers' return events and capture wakeup-to-running delay for those same writers. The current data do not justify changing the MVCC sequencer or WAL protocol.

## Result verification and retention

The local runner verifier reported all 12 scheduled runs complete, with matching successful result and metadata files. `remote-exit.json` reported `failed: false` and `runs_started: 12`. The transferred archive SHA-256 matched the retained checksum metadata. The raw archive remains local and was not added to GitHub.

The VM and its TCP/22 firewall rule remain retained until the configured 60-minute auto-delete deadline, **2026-11-14 17:06:23 UTC**. The rule was verified to match the approved SSH `/32`; the isolated VPC and subnet were left untouched.
