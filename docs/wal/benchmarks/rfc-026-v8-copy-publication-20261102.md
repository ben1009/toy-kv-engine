# RFC 026 V8 copy and publication screens (2026-11-02)

## Result

None of these candidates is retained. A borrowed-input encoder has a useful
isolated CPU result, but the sixteen-writer SSD throughput check fails.
Reducing publication spinning, moving batch-64 buffers into the memtable,
combining those copy removals and retaining one FRONTIER owner also fail
throughput/p99 checks. The runtime has been restored.

These screens preceded a separate timed V8/V7 CPU investigation. They are
recorded to distinguish tested negative results from proposed optimizations. No RFC digest, wire, recovery or sync contract changed.

## Method

Release `write-perf` with `bench`, ext4 on the Solidigm P41 Plus, 1 KiB
values, Parallel WAL, no compaction, 2 GiB target SST, latency sampling every
ten operations. Batch tests use 750,000 puts in batches of 64; point guards
use 5,000 puts at one writer and 20,000 at eight or sixteen writers.
Databases, executables, complete originals and logs remain under
`target/rfc-026-v8-wakeup-profile-20261102/` in the local repository.

[Plans and hashes](artifacts/rfc-026-v8-copy-publication-20261102-plans.json)
preserve the schedules written before execution. Cohorts reverse order;
cooled follow-ups and subsequent guards are reported separately. Compare the
highest eligible throughput and its same-run p99 within each cohort. The
unchanged stall flag is p99 above 10 ms or mean fdatasync above 1 ms.
No unfavorable result was reclassified. Small samples and within-cohort
variation prevent confidence or adoption claims.

All 80 screen/guard measurements, including flagged samples, are in the
[archive](artifacts/rfc-026-v8-copy-publication-20261102-measurements.jsonl).
The [summary](artifacts/rfc-026-v8-copy-publication-20261102-summary.json)
includes eligible/total counts, medians and best-sample identities. Missing
eligible arms remain missing; they are not compared with a peak from another
cohort. Complete source records remain local, with original-path/hash
metadata in the archive.

## Single FRONTIER owner: rejected

The [candidate](artifacts/rfc-026-v8-copy-publication-20261102-single-owner.patch)
holds the existing FRONTIER mutex across readiness waits, so followers wait
on the owner rather than repeatedly contending after coordinator changes.

| Writers | Baseline puts/s | Baseline p99 ms | Candidate puts/s | Candidate p99 ms |
|---|---:|---:|---:|---:|
| 8 | 354,420 | 2.827 | 347,438 | 3.108 |
| 16 | 514,336 | 4.398 | 418,540 | 5.184 |

All eight samples are eligible. Both throughput and p99 fail. Holding one
owner longer is not sufficient to demonstrate a cheaper durability path.

## Borrowed point preparation: isolated CPU gain, SSD rejection

The [prototype](artifacts/rfc-026-v8-copy-publication-20261102-borrowed.patch)
borrows the existing point-batch `Bytes` rather than allocating key/value
`Vec`s inside another owned `WalBatch`. It reserves and charges a bounded
canonical index, preserves last-wins duplicate handling and emits the same
logical bytes, CRC, entry/batch digest and DATA frames. Raw puts and already
prefixed puts remain distinct; deletes do not reuse value bytes.

The [isolated probe](artifacts/rfc-026-v8-copy-publication-20261102-borrowed-cpu-probe.rs)
performs 3,000 prepare/encode/frame cycles per arm, six rounds in alternating
order, batch 64. Its [output](artifacts/rfc-026-v8-copy-publication-20261102-borrowed-cpu.log)
has median 35.625 µs for the owned path and 27.197 µs borrowed, about 23.7%
less elapsed time per batch in this single-thread CPU-bound probe.
That is not an end-to-end SSD result.

Early SSD cohorts are heavily stall-limited. A cooled eight-writer pair looks
positive, but the later complete guard cohort is decisive:

| Writers | Baseline puts/s | Baseline p99 ms | Borrowed puts/s | Borrowed p99 ms | Throughput |
|---|---:|---:|---:|---:|---:|
| 8 | 334,579 | 3.228 | 346,245 | 3.030 | +3.5% |
| 16 | 486,216 | 4.123 | 421,208 | 4.124 | -13.4% |

Both arms have two eligible samples at these writer counts. The guard
cohort's one-writer baseline has no eligible batch sample, so it cannot
establish a solo result. Point guards do not rescue the failed batch-16
throughput result. After fixture correction, 149 V8-related tests and
all-target/all-feature Clippy passed for the prototype; it was still reverted.

## Publication and combined copy experiments: rejected

The [publication prototype](artifacts/rfc-026-v8-copy-publication-20261102-publication.patch)
lowers the hints selecting the existing short MVCC publication spin from
32/16 to 8/4. It does not change publication ordering or its locked predicate.
The combined arm adds that hint change to borrowed encoding.

| Writers | Arm | Best puts/s | Same-run p99 ms |
|---|---|---:|---:|
| 8 | Baseline | 327,763 | 2.373 |
| 8 | Reduced-spin hints | 337,978 | 2.636 |
| 8 | Reduced-spin plus borrowed | 325,965 | 3.092 |
| 16 | Baseline | 450,330 | 4.775 |
| 16 | Reduced-spin hints | 420,241 | 4.714 |
| 16 | Reduced-spin plus borrowed | 423,831 | 4.478 |

The standalone change loses eight-writer p99 and sixteen-writer throughput.
The combined arm also loses throughput. Spin limits and thresholds are
restored to their previous values.

The [owned-publication prototype](artifacts/rfc-026-v8-copy-publication-20261102-owned-publish.patch)
lowers the memtable's existing owned-batch cutoff from 512 entries to 64.
It moves already owned buffers into publication instead of copying them
again. The final
[combined-copy prototype](artifacts/rfc-026-v8-copy-publication-20261102-both-copies.patch)
adds borrowed WAL encoding while leaving the original spin policy intact.

| Cohort | Writers | Baseline puts/s | Baseline p99 ms | Candidate puts/s | Candidate p99 ms |
|---|---:|---:|---:|---:|---:|
| Owned publication | 8 | 315,748 | 3.240 | 334,934 | 3.898 |
| Owned publication | 16 | 437,064 | 4.220 | 426,198 | 4.587 |
| Both copy removals | 8 | 336,211 | 3.092 | 324,920 | 3.213 |
| Both copy removals | 16 | 456,151 | 4.392 | 424,248 | 4.176 |

Owned publication loses eight-writer p99 and sixteen-writer throughput/p99.
Both copy removals lose throughput. The memtable cutoff is restored to 512.
The borrowed patch is preserved with the original spin hints; combining it
with the separate publication patch reproduces the combined-spin variant.

A separate one-ticket DATA-group limit compiled but was never benchmarked;
it was restored when the investigation switched to attribution. There is no
performance result or retained change for that variant.

The negative results do not prove that copies or spinning are free. They
show that these particular changes do not pass the SSD retention gate.
The next decision should use timed attribution rather than treating the
isolated encoding gain or a lower CPU total as an end-to-end win.
