# RFC 024 Slice 7: Parallel WAL benchmark outcome

**Decision:** Keep the client-leader WAL as the default. The opt-in parallel
candidate reached the intended concurrency and overlapped writes with
`fdatasync`, but did not demonstrate a reliable end-to-end gain and regressed
several latency and throughput cases.

## Run manifest

- Date: 2026-09-26, Asia/Chongqing
- Current source: base revision `3e67e905072b58b2317aae64c6039e3c28c7005d`
  plus the Slice 7 `write-perf` changes in this worktree
- Current release binary: SHA-256
  `4612e15ec02c221eba4356ce658e1289689bf19b40ffb6f11ad61592d895fdd9`
- Historical source: pre-PITR revision `2f556ccb`, built in the same session
  with the installed toolchain; binary SHA-256
  `de4995c5c46d61323d4b66881c41a8eb2aabe471f34671edca3446a8fcc64a70`
- Toolchain: `nightly-2026-09-23`, rustc
  `1.100.0-nightly (6bb1652a0 2026-09-22)`, cargo
  `1.100.0-nightly (495c385d0 2026-09-16)`
- Kernel: Linux `6.18.9-arch1-2`
- Filesystems: `/tmp` tmpfs; workspace ext4 mounted from
  `/dev/nvme0n1p3` (device-backed NVMe filesystem)
- Runtime environment: `HOTPATH_METRICS_SERVER_OFF=true`
- Device queue depth: unavailable; reported SQE counts are software pipeline
  measurements only

The main regression case used the same current binary for both modes:

```text
--suite legacy --preset default --wal --bench wal_concurrent
--num 200000 --threads 4 --value-size 1024
--target-sst-size 1048576 --latency-sample-every 100 --output json
--wal-io-mode leader|parallel
```

Runs were non-profiled. The first pair was leader-first; pairs 02 and 04 were
leader-first, and pairs 03 and 05 were candidate-first. The separate WAL-
isolated matrix used 20,000 operations, a 1 GiB SST target, 1/4/8/16 writers,
and both `wal_concurrent` single puts and `wal_batch_concurrent` batches of 64.
It is exploratory, not a reproduction of the 1 MiB-SST regression case.

## Exact regression case

Rates are operations/second. `p99×` is candidate p99 divided by leader p99;
values above 1 mean worse candidate tail latency.

| Pair | Order | tmpfs leader | tmpfs parallel | tmpfs ratio | tmpfs p99× | ext4 leader | ext4 parallel | ext4 ratio | ext4 p99× |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 01 | leader first | 162,554 | 73,713 | 0.453 | 2.381 | 2,189 | 1,757 | 0.802 | 1.093 |
| 02 | leader first | 160,149 | 74,267 | 0.464 | 3.006 | 3,657 | 3,172 | 0.867 | 4.187 |
| 03 | parallel first | 168,041 | 72,380 | 0.431 | 2.360 | 3,594 | 3,001 | 0.835 | 3.813 |
| 04 | leader first | 189,393 | 74,673 | 0.394 | 2.123 | 2,258 | 4,097 | 1.814 | 0.204 |
| 05 | parallel first | 187,913 | 71,124 | 0.378 | 2.609 | 3,679 | 2,509 | 0.682 | 4.048 |

Paired median candidate/control throughput ratios were `0.431` on tmpfs and
`0.835` on ext4. A deterministic paired bootstrap of the median ratio (100,000
resamples, seed 24024) gave 95% percentile intervals of `0.378–0.464` on
tmpfs and `0.682–1.814` on ext4. The tmpfs result is a clear regression. The
ext4 interval includes parity and does not establish the required 10% gain;
four of five ext4 candidate runs were slower. The existing leader controls
also varied substantially on ext4 (2,189–3,679 ops/s), so pair 04's apparent
gain is not enough to justify adoption.

Median p50/p99 latency was `0.0168/0.1507 ms` for the tmpfs leader and
`0.0320/0.3801 ms` for the candidate. On ext4 it was `1.0809/2.7129 ms` for
the leader and `0.9423/10.9557 ms` for the candidate. The median paired p99
ratio was 2.381 on tmpfs and 3.813 on ext4. Candidate p50 improved on ext4,
but the p99 gate failed.

The same-binary tmpfs null pair measured 165,163 and 175,154 ops/s for two
leader runs, a 6.1% difference. The candidate's 57% lower paired-median rate
on tmpfs is far outside that observed null spread.

## WAL-isolated scaling matrix

Each cell is `leader → parallel ops/s`; `p99 Δ` is the candidate's percentage
change from leader p99. These are one exploratory pair per case, not adoption
statistics.

| Filesystem | Writers | Workload | Throughput, leader → parallel | p99 Δ | Candidate max in-flight groups |
| --- | ---: | --- | ---: | ---: | ---: |
| tmpfs | 1 | single puts | 115,736 → 58,774 | +105.8% | 1 |
| tmpfs | 1 | batch 64 | 532,830 → 384,142 | +115.3% | 1 |
| tmpfs | 4 | single puts | 181,751 → 97,388 | +21.5% | 4 |
| tmpfs | 4 | batch 64 | 957,240 → 798,993 | -3.3% | 4 |
| tmpfs | 8 | single puts | 188,586 → 124,753 | +172.5% | 8 |
| tmpfs | 8 | batch 64 | 1,132,995 → 868,606 | +46.2% | 8 |
| tmpfs | 16 | single puts | 173,647 → 136,195 | +101.2% | 8 |
| tmpfs | 16 | batch 64 | 1,272,759 → 738,231 | +36.6% | 8 |
| ext4 | 1 | single puts | 1,762 → 1,723 | -5.7% | 1 |
| ext4 | 1 | batch 64 | 93,622 → 86,898 | +1.5% | 1 |
| ext4 | 4 | single puts | 3,747 → 4,298 | -4.0% | 4 |
| ext4 | 4 | batch 64 | 198,161 → 190,553 | -1.9% | 3 |
| ext4 | 8 | single puts | 6,763 → 7,798 | -11.0% | 8 |
| ext4 | 8 | batch 64 | 374,940 → 227,444 | +1,210.1% | 7 |
| ext4 | 16 | single puts | 13,422 → 14,183 | +67.1% | 8 |
| ext4 | 16 | batch 64 | 570,262 → 504,887 | +1.5% | 8 |

The one-writer ext4 single-put case regressed 2.2% with a 5.7% p99 improvement;
the one-writer batch case regressed 7.2%. The matrix has isolated ext4 gains
for 4/8-writer single puts, but the candidate loses on tmpfs in every case and
on most ext4 batch cases. Single observations do not satisfy the confidence
gate.

## Pipeline, CPU, and sync diagnostics

For the exact 200k-put case, median pipeline counters were:

| Filesystem / mode | Commit groups | Solo groups | CQEs | Max in-flight groups | Max outstanding write SQEs | Syncs | Eventfd notification requests |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tmpfs leader | 93,978 | 18,944 | 200,000 | 1 | 4 | 93,978 | 0 |
| tmpfs parallel | 200,000 | 200,000 | 200,000 | 4 | 4 | 64,313 | 200,000 |
| ext4 leader | 86,612 | 17,669 | 200,000 | 1 | 4 | 86,612 | 0 |
| ext4 parallel | 200,000 | 200,000 | 200,000 | 4 | 4 | 88,879 | 200,000 |

| Filesystem / mode | Groups per sync | Preallocation ms | `fdatasync` ms | WAL wait ms | Memtable insert ms | WAL file / allocated MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tmpfs leader | 1.00 | 121.973 | 17.749 | 3,415.000 | 281.458 | 708.0 / 708.0 |
| tmpfs parallel | 3.11 | 254.375 | 24.209 | 7,986.293 | 382.743 | 584.0 / 584.0 |
| ext4 leader | 1.00 | 30.859 | 31,023.688 | 217,197.076 | 786.053 | 8.0 / 8.008 |
| ext4 parallel | 2.25 | 166.422 | 58,065.642 | 260,510.655 | 812.098 | 8.0 / 8.008 |

These are medians across the five exact-case runs. Wait and timer counters are
thread-accumulated durations, not wall-clock time. WAL file length and
filesystem allocation are point-in-time snapshots before drain/close; the
1 MiB SST target allows earlier WAL reclamation, so these values are not total
WAL bytes written. `st_blocks` includes preallocation and is not device write
traffic.

The candidate met the two-groups-in-flight requirement. It also formed more
commit groups in the exact case, while requesting one eventfd notification per
ticket. This, alongside the higher CPU use and slower tmpfs results, points to
group formation/notification overhead as the first optimization target. That
is an inference from the counters, not a separately isolated causal test.

Median process CPU for the exact case was 1,286 ms user / 1,229 ms system for
the tmpfs leader and 4,166 / 3,396 ms for the candidate. On ext4 it was 10,821
/ 5,829 ms for the leader and 16,692 / 11,272 ms for the candidate. No
block-device queue-depth measurement was available; the SQE counts above are
software outstanding requests, not device queue depth.

Profiled 20k-operation ext4 `wal_batch_concurrent` runs (4 writers, batch 64,
1 GiB SST target) were kept separate from adoption runs. Across 186 candidate
`fdatasync` observations, later SQEs were submitted during 175 calls; CQEs and
groups completed during 100 calls; and the written frontier advanced during
97 calls. The sync observations counted 268 submissions and 158 CQEs/groups
completed while sync was active, with up to four groups covered by one sync.
The leader's 159 observations showed no later SQE submission, CQE, group
completion, or written-frontier advance during sync. This confirms that the
candidate overlaps filesystem write progress with `fdatasync`, but the
overlap did not translate into a consistent end-to-end gain.

The profile runs also reported max in-flight groups of 4 for the candidate
and 1 for the leader. Mean `fdatasync` observation duration was 0.530 ms for
the candidate and 0.324 ms for the leader. Profile instrumentation adds
overhead and these timings are diagnostic only.

## PITR and pre-PITR controls

The PITR-on leader control used 20,000 single puts, four writers, 1 KiB
values, a 1 GiB SST target, and latency sampling every 100 operations. It
completed on both filesystems: tmpfs measured 164,470 ops/s with p50/p99
`0.0194/0.2731 ms`; ext4 measured 3,450 ops/s with p50/p99
`1.1292/2.6707 ms`. Candidate mode remains rejected with `--pitr`.

The pre-PITR `2f556ccb` reference used the exact 200k-put regression arguments
on the same session and toolchain. It measured 177,605 ops/s on tmpfs and
2,385 ops/s on ext4. Current leader medians were 168,041 ops/s on tmpfs and
3,594 ops/s on ext4. This single historical run does not establish a
regression gap: tmpfs is 5.4% lower, while ext4 is faster than the historical
point but within the current leader's broad 2,189–3,679 ops/s range. The
candidate did not close a reproducible pre-PITR gap in these measurements.
The historical binary did not emit the current latency and CPU fields.

## Adoption decision

Keep the leader path as default and leave the candidate opt-in. It misses the
required device-backed gain: the paired ext4 median ratio is below parity and
its 95% interval includes parity. It also loses consistently on tmpfs, has a
large p99 regression in the exact case, and fails the one-writer batch
throughput bound. The pipeline is real, but not beneficial enough to adopt.

If further work is approved, investigate coalescing adjacent tickets into
fewer I/O groups and batching worker notifications before trying a second
worker or ring. Retest with the same paired gate after that change.

Raw JSONL outputs from this session are in
`/tmp/rfc024-slice7-artifacts`; the persistent summary tables above record the
adoption-relevant measurements.
