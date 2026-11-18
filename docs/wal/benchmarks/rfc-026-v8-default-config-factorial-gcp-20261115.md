# RFC 026 V8 Default Configuration Factorial on GCP — 2026-11-15

Source commit: `0f25de1374c8fce60e689c6d827e56fff35959e5`

Binary SHA-256: `a312a6ff8920178a9429c2422885c8211fa551861236ca9d29a66ad354709c23`

## Test design

This comparison isolates three V8 benchmark settings on the retained
`c4-standard-16-lssd` host: FRONTIER mode, coalescing budget, and sliding extent
preparation. It ran 16 writers, 1 KiB values, no compaction, and fresh database
paths per run. Point used 100,000 puts; Batch8 used 300,000 puts. Each workload
ran all eight V8 combinations four times, plus four V7 Parallel controls, for
72 runs total.

The V8 factors were buffered/direct FRONTIER, dynamic/400 μs coalescing, and no
extent preparation/sliding `fallocate` with a 4 MiB window. The 400 μs override
only sets the eligible coalescing branch's budget; it does not add an
unconditional wait. V7 ignored the V8-only settings.

The source and benchmark binary were built from the exact commit above on the
same retained host used for the preceding RFC 026 matrix. The experiment did
not enable `perf`, detailed profiling, or the periodic WAL timeline. It did
retain the workload's latency sampling and aggregate WAL counters. These are
same-host exploratory scores, not uninstrumented qualification results; the
host had already run the preceding matrix, and its Local SSD state was not
reset. Configuration order was deterministically shuffled within each
workload/repetition.

The host was a 16-vCPU C4 with two Local SSDs in RAID0, ext4
(`rw,noatime,stripe=256`), on Linux `7.0.0-1017-gcp`.

## Results

Medians across four runs per cell. Throughput is k puts/s; CPU is process CPU
microseconds per put; syncs and puts/sync are per run; p50 is sampled put
latency in milliseconds.

| Workload | FRONTIER | Coalescing | Extent prep | k puts/s | CPU μs/put | Syncs | Puts/sync | p50 ms |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| Point | V7 Parallel | — | — | 47.20 | 61.30 | 8,335.5 | 12.00 | 0.307 |
| Point | Buffered | Dynamic | None | 19.91 | 115.20 | 7,485.5 | 13.36 | 0.784 |
| Point | Buffered | 400 μs | None | 19.62 | 117.72 | 7,565 | 13.22 | 0.789 |
| Point | Direct | Dynamic | None | 19.85 | 116.63 | 8,915.5 | 11.22 | 0.775 |
| Point | Direct | 400 μs | None | 20.70 | 115.25 | 7,490.5 | 13.35 | 0.748 |
| Point | Buffered | Dynamic | Sliding 4 MiB | 30.44 | 71.19 | 6,320 | 15.82 | 0.516 |
| Point | Buffered | 400 μs | Sliding 4 MiB | 30.89 | 70.42 | 6,328 | 15.80 | 0.512 |
| Point | Direct | Dynamic | Sliding 4 MiB | 32.19 | 74.74 | 7,852 | 12.74 | 0.477 |
| Point | Direct | 400 μs | Sliding 4 MiB | 32.14 | 70.46 | 6,329 | 15.80 | 0.493 |
| Batch8 | V7 Parallel | — | — | 214.08 | 13.19 | 4,236 | 70.82 | 0.554 |
| Batch8 | Buffered | Dynamic | None | 144.27 | 17.50 | 2,962 | 101.28 | 0.866 |
| Batch8 | Buffered | 400 μs | None | 141.99 | 17.91 | 3,014 | 99.54 | 0.872 |
| Batch8 | Direct | Dynamic | None | 145.66 | 17.51 | 3,338 | 89.87 | 0.842 |
| Batch8 | Direct | 400 μs | None | 150.23 | 17.53 | 2,979 | 100.71 | 0.831 |
| Batch8 | Buffered | Dynamic | Sliding 4 MiB | 204.60 | 13.09 | 2,381 | 126.00 | 0.607 |
| Batch8 | Buffered | 400 μs | Sliding 4 MiB | 205.75 | 12.96 | 2,380 | 126.05 | 0.608 |
| Batch8 | Direct | Dynamic | Sliding 4 MiB | 217.35 | 13.35 | 3,138.5 | 95.60 | 0.563 |
| Batch8 | Direct | 400 μs | Sliding 4 MiB | 216.48 | 12.95 | 2,377 | 126.21 | 0.577 |

Factor effects use paired runs holding the other two V8 factors fixed. There
were 16 contrasts per factor/workload: four repetitions across the four
combinations of the other factors.

| Factor change | Point median throughput delta | Positive pairs | Batch8 median throughput delta | Positive pairs |
| --- | ---: | ---: | ---: | ---: |
| Buffered → Direct FRONTIER | +4.89% | 14/16 | +5.46% | 14/16 |
| Dynamic → fixed 400 μs | +0.61% | 9/16 | −0.13% | 7/16 |
| No prep → Sliding 4 MiB | +56.39% | 16/16 | +44.22% | 16/16 |

The default-to-candidate paired comparison (Buffered/Dynamic/None versus
Direct/400 μs/Sliding 4 MiB) improved Point throughput by a median 61.94%
(4/4 pairs) and Batch8 by 49.52% (4/4). Sliding `fallocate` alone, applied to
the default Buffered/Dynamic configuration, improved Point by 52.9% and
Batch8 by 41.8% using the four cell medians.

The Direct/Dynamic/Sliding configuration was marginally faster than
Direct/400 μs/Sliding in both workloads: 32.19 versus 32.14k puts/s for Point,
and 217.35 versus 216.48k puts/s for Batch8. The factorial contrasts do not
show a consistent throughput benefit from fixing the budget at 400 μs.

Against V7 in this same sweep, Direct/400 μs/Sliding remained 32.1% slower on
Point (0/4 paired wins). Batch8 was 1.0% faster by the paired median (3/4
wins), too small and too few runs to claim a stable lead.

## Interpretation

The default-versus-tuned gap is largely explained by extent preparation. The
sliding `fallocate` factor produced a positive throughput change in all 16
paired contrasts for each workload and reduced CPU per put. Direct FRONTIER
added a smaller, mostly positive effect. The fixed 400 μs coalescing override
had no consistent effect; the dynamic setting performed slightly better in
the fully prepared Direct configuration.

This explains much of the difference between the runner-default matrix and the
previous Direct/400 μs/Sliding results. It does not close the remaining Point
gap: the fully tuned V8 configuration still delivered about 68% of V7 Point
throughput in this sweep. Batch8 reached parity with V7 within this run's
variation. The remaining Point gap should be analyzed on the prepared V8
baseline, separately from the default-configuration penalty.

## Result validation and limits

The remote summary reported `complete: true` and 72/72 runs. All scheduled run
IDs had successful `result.json` and matching `run-metadata.json` files. The
transferred archive SHA-256 matched the remote checksum:

`afa9c8ab19ab54ff25dc17cc4bccf691bc106fd8f5d94bd4527b3ed26c812805`

The temporary sweep driver's convenience `metrics.tsv` used
`entries_per_sec`, which is null for these WAL records. Its throughput column
is therefore invalid and was excluded. Reported throughput and CPU figures
were recomputed offline from each verified `result.json` using
`result.ops_per_sec`, process CPU time, and the per-run put count. The corrected
derived table and analyzer remain local alongside the verified archive; their
SHA-256 values are recorded in local `analysis-metadata.json`. The raw archive
was not uploaded to GitHub.

The VM, restricted SSH rule, VPC, subnet, route, and Terraform state were left
in place. No cleanup or `terraform destroy` was run. The VM remains subject to
its original server-side expiry deadline.
