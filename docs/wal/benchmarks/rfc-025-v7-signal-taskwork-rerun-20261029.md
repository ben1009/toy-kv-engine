# RFC 025: full SSD rerun, assessed using best scores

The maintainer requested another rerun and fresh data for the other workloads.
All six cells finish: **56 invocations, 12 warmups and 44 scored runs**, without
replacement captures. The maintainer then reminded us to use best scores. The
assessment uses each exact binary's **highest-throughput scored invocation per
workload**, with p99 taken from that same invocation. Paired medians remain
available as diagnostic evidence and do not determine this decision.

The ordinary-notification candidate is **not retained**. Historical best scores
improve both throughput and p99 for point1 and point16, but point8 throughput is
1.4% lower. Batch1, batch8 and batch16 throughput is 7.5%, 13.8% and 10.9% lower.
The point8 improvement requirement and batch throughput guards are not met using
the requested best-score view. This conclusion compares recorded peaks; it does
not establish the cause of the slower individual runs or qualify v4 parity.

The table below uses the historical best scored runs of the exact two binaries,
including this rerun. Every p99 belongs to its displayed throughput score:

| Workload | Retained puts/s | Candidate puts/s | Retained p99 ms | Candidate p99 ms | Throughput change | p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point, 1 writers | 2,012 | 2,343 | 2.422 | 0.735 | +16.5% | -69.6% |
| Point, 8 writers | 16,749 | 16,515 | 1.156 | 1.119 | -1.4% | -3.1% |
| Point, 16 writers | 20,479 | 25,211 | 3.521 | 1.770 | +23.1% | -49.7% |
| Batch64, 1 writers | 72,647 | 67,176 | 1.373 | 1.331 | -7.5% | -3.0% |
| Batch64, 8 writers | 478,798 | 412,847 | 2.653 | 2.612 | -13.8% | -1.6% |
| Batch64, 16 writers | 606,948 | 540,956 | 4.065 | 4.260 | -10.9% | +4.8% |

The independent rerun's best scores are:

| Workload | Retained puts/s | Candidate puts/s | Retained p99 ms | Candidate p99 ms | Throughput change | p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point, 1 writers | 2,012 | 2,343 | 2.422 | 0.735 | +16.5% | -69.6% |
| Point, 8 writers | 16,749 | 16,515 | 1.156 | 1.119 | -1.4% | -3.1% |
| Point, 16 writers | 9,791 | 25,211 | 10.861 | 1.770 | +157.5% | -83.7% |
| Batch64, 1 writers | 69,533 | 34,439 | 1.367 | 10.818 | -50.5% | +691.5% |
| Batch64, 8 writers | 394,091 | 412,847 | 2.940 | 2.612 | +4.8% | -11.2% |
| Batch64, 16 writers | 588,223 | 540,920 | 4.195 | 4.420 | -8.0% | +5.4% |

Both views independently select each arm's maximum throughput. The unchanged
obvious-stall cutoff excludes an individual invocation if mean FRONTIER write
is at least 1 ms or mean fdatasync is at least 1.5 ms. Warmups and diagnostic
captures are excluded. In this dataset, **all selected best scores are also the
unfiltered raw maxima**, so the cutoff does not remove a winning score. Throughput
and latency are never selected from different invocations. Run IDs, raw hashes,
source manifests and associated I/O means are in the
[best-score summary](artifacts/rfc-025-v7-signal-taskwork-rerun-20261029-best-summary.json).

The historical index freezes 41 saved capture sources and contains 194 unique
scored records matching these binary hashes and workload configurations. It uses
existing records without adding them again to the invocation lineage. The two
arms have different numbers of historical observations. These are peak results,
not estimates of an average effect or proof that every storage delay is removed.
The aggregate cutoff also misses short delays. Every raw capture is preserved.

The [initial plan](artifacts/rfc-025-v7-signal-taskwork-rerun-20261029-plan.json)
was frozen before capture. The question about other data expanded the schedule
before capture to all six workloads regardless of interim performance. Batch16
and point8 have two warmups plus five alternating pairs each; point1, point16,
batch8 and batch1 have two warmups plus three pairs each. Every cell gets a
60-second cooldown, with 15-second gaps between invocations. No later result
changes the capture schedule.

The initial assessment still used paired guards. The
[maintainer assessment record](artifacts/rfc-025-v7-signal-taskwork-rerun-20261029-maintainer-assessment.json)
records the explicit reminder during capture and restores the requested best-score
ranking. Point8 must improve both peak throughput and associated p99. Other cells
keep throughput at least 0.95 and p99 at most 1.10 relative to the best retained
score, with identical DATA bytes. Historical best scores determine retention;
the fresh cohort's best scores are shown separately. This correction changes
assessment only, with no new or replacement benchmark invocations.

All runs use fresh local ext4 databases, explicit v7 Parallel, 1 KiB values,
a 2 GiB SST target and no compaction. Batch64 runs use 750,000 puts and sampling
every ten batches. Point8/16 use 100,000 puts with sampling every 100 puts;
point1 uses 20,000 puts with sampling every ten puts. Kernel is `6.12.71-1-lts`.
Before/1 Hz/after audits find no recognized competing process; the audit cannot
prove exclusive device access. No build, test, profile, source edit or SSD health
query overlaps the captures.

The saved binaries remain byte-identical to the original trial:

| Arm | Saved binary | SHA-256 |
| --- | --- | --- |
| Retained | `target/release/write-perf-v7-large-data-async-dispatch` | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Ordinary notification | `target/release/write-perf-v7-signal-taskwork` | `79592a3cbf0c4f9e5411690cd5e369e62b9abdc6991f88b03b0b0d9b42071889` |

The candidate requests ordinary completion notification with `SINGLE_ISSUER`
for active ext-filesystem v7 workers. Other workers keep
`COOP_TASKRUN | SINGLE_ISSUER`. It is the same saved candidate as the previous two
cohorts, with no new policy, codec, ownership or durability change.

This rerun changes no Rust source or canonical executable and performs no new
build. It reuses the immediately preceding outside-sandbox correctness checks:
1,975 candidate tests, 49 focused tests without retries, and 1,973 retained tests
without retries or skips. Their logs, source manifests and original release-build
provenance are reverified. Final verification checks all 128 live baseline inputs
and the retained canonical executable. Documentation spelling and staged diff
checks pass. The verifier reconstructs the candidate from `d3366ebb` plus the
saved patch, validates the fixed captures and audits, regenerates both analysis
views and checks the historical winners against the frozen input index.

Reproduce the requested best-score analysis and verification:

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-signal-taskwork-rerun-20261029-best-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-signal-taskwork-rerun-20261029-verify.py
```

The earlier cohorts and receipts remain unchanged. This cohort adds 56 unique
completed invocations to the 869-invocation lineage, reaching **925**, with 5,021
unique invocations in the broader archive and one inherited interrupted invocation
without JSON. Scratch databases are removed. Raw results are versioned; logs
and saved executables stay under the repository's ignored `target` directory.
No push or PR is made.
