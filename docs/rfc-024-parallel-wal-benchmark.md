# RFC 024: Parallel WAL benchmark outcomes

**Decision:** Keep the client-leader WAL as the default. Removing the parallel
path's dedicated packer thread improved it on tmpfs and left ext4 throughput
near parity with the previous parallel implementation. The current candidate
still loses to the leader on tmpfs, so parallel WAL remains opt-in.

## Worker-owned asynchronous data sync (rejected, 2026-09-28)

A prototype removed the sync thread from the parallel runtime. The I/O
worker consumed group completions, captured the contiguous written prefix,
and submitted one `IORING_OP_FSYNC` with `IORING_FSYNC_DATASYNC`. Only its
successful CQE acknowledged the captured prefix; later writes could continue
while it was outstanding. The moving admission cutoff and conditional
200-microsecond coalescing deadline were retained. This tested the removal of
an actual thread handoff, using the unchanged parallel path at `bd1dd558` as
the control. The leader implementation was not changed.

Five alternating pairs per case used release builds with `--features bench`,
1 KiB values, and latency sampled every 100 operations. Compilation and tests
finished before these pairs. Tmpfs used `/dev/shm`: an earlier exploratory
series hit `/tmp`'s user quota and is excluded from this table.

| Case | Puts / writers / SST target | Median paired throughput ratio, async/control | Median paired p99 ratio |
| --- | --- | ---: | ---: |
| Ext4, one writer | 5k / 1 / 1 GiB | 1.003 | 1.005 |
| Ext4, rotation | 20k / 4 / 1 MiB | 1.000 | 0.920 |
| Ext4, WAL-isolated | 50k / 16 / 1 GiB | 0.787 | 1.338 |
| Tmpfs, one writer | 20k / 1 / 1 GiB | 0.838 | 0.972 |
| Tmpfs, original case | 200k / 4 / 1 MiB | 0.913 | 1.007 |

The one- and four-writer ext4 pairs had whole-device write-latency averages
of roughly 0.15–0.16 ms. The 16-writer series encountered the known latency
cliff in both arms (0.11–1.60 ms per device write across the series), so its
ratio is not an isolated estimate of the code's cost. Neither stable ext4
case establishes a throughput gain. The prototype reached 16 in-flight groups
and 16 outstanding write SQEs in the high-concurrency runs.

On tmpfs, the one-writer median fell from 67,548 to 56,808 puts/s. The
control spent 2.86 ms in 20,000 synchronous data-sync calls; the prototype
spent 77.42 ms in its 20,000 submission-to-CQE-consumption intervals. These
measure different boundaries: the asynchronous interval also includes
scheduling and completion delivery. Removing the userspace sync thread did
not remove the sync round trip. In the original four-writer case, median
sync count also rose from 115,834 to 148,786, reducing useful coalescing.

The prototype passed 36 focused default-feature-plus-`bench` tests, including
recovery and WAL-full rotation. Full crash/failpoint adaptation was not
completed after this performance screen failed. The Rust changes were
reverted; no runtime selector or alternative production path was retained.
This rejects this asynchronous-sync design, not all ways of reducing
pipeline handoffs.

Raw paired results are in `/tmp/rfc024-async-sync-clean-bench.jsonl` and
`/tmp/rfc024-async-sync-16-bench.jsonl`. The temporary prototype patch is
`/tmp/rfc024-async-sync-prototype.patch`. The benchmark commands use:

```text
write-perf --suite legacy --preset default --wal --bench wal_concurrent \
  --num <puts> --threads <writers> --value-size 1024 \
  --target-sst-size <bytes> --latency-sample-every 100 --output json \
  --wal-io-mode parallel --path <disposable path on the selected filesystem>
```

## Current ext4 bottleneck: partly filled sync barriers

### Current-binary ext4 adoption-gate check (2026-09-27)

After the moving admission cutoff, the same release binary alternated leader
and parallel WAL on the device-backed ext4 mount. Each run used 1 KiB values
and sampled latency every 100 operations. The representative WAL-isolated
case used 50,000 puts, 16 writers, and a 1 GiB SST target (five pairs). The
original regression case used 200,000 puts, four writers, and a 1 MiB SST
target (five pairs). The one-writer guard used 5,000 puts and a 1 GiB target
(five pairs).

| Ext4 case | Leader median puts/s | Parallel median puts/s | Median paired throughput ratio | 95% bootstrap interval | Median paired p99 ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| WAL-isolated, 16 writers | 13,128 | 20,962 | 1.595 | 1.043–1.663 | 0.723 |
| Original, four writers | 2,825 | 5,979 | 1.650 | 1.084–2.644 | 0.811 |
| WAL-isolated, one writer | 1,761 | 1,722 | 0.975 | 0.390–0.994 | 1.013 |

The intervals are exact percentile intervals from resampling the five paired
ratios with replacement. The 16-writer median exceeds the 10% throughput
threshold and its interval excludes parity. The original four-writer paired
ratios were `1.084, 1.650, 2.425, 2.644, 1.112`; the one-writer ratios were
`0.975, 0.390, 0.994, 0.974, 0.988`. The one-writer median stays within the
5% throughput guard, but one run suffered a severe parallel sync stall.

Two same-binary leader/leader null pairs on the original workload changed
throughput by `0.708×` and `1.226×` and sampled p99 by `4.592×` and `0.272×`.
The worst inverse throughput swing was `1.412×`. The four-writer median
parallel gain exceeds that observed null swing, but its bootstrap lower bound
does not. Individual parallel/leader sampled-p99 ratios ranged from `0.149`
to `3.988` in the four-writer case, from `0.585` to `1.190` at 16 writers,
and from `0.984` to `7.783` at one writer. Thus the representative ext4
throughput criterion passes, while repeatability beyond the null spread and
the RFC's p99-within-10% matrix criterion do not have a clean passing result.
The current build also has no same-session pre-PITR comparison. Keep the
parallel path opt-in and the leader path as default.

### Ext4 sync-cliff device-counter check (2026-09-27)

Six more alternating pairs used the current release binary with 5,000 puts,
one writer, 1 KiB values, and a 1 GiB SST target. The benchmark driver sampled
`/sys/block/nvme0n1/stat` every 200 milliseconds while each run executed.
The counters cover the whole device, including unrelated I/O; they do not
identify the process that caused a slow interval.

Fast runs in both modes averaged `0.155`–`0.166` milliseconds per completed
device write and `0.325`–`0.335` milliseconds per WAL `fdatasync`. Slow runs
averaged `0.466`–`0.782` milliseconds per device write and `0.887`–`1.357`
milliseconds per WAL `fdatasync`. Device writes stayed near 15,000 and
sectors written near 110 MB per run in both regimes. Across the 12 runs, the
device-write-latency and WAL-sync-latency series had Pearson correlation
`0.989`; throughput moved in the opposite direction (`-0.972`). The slow
regime affected both leader and parallel WAL. For example, pair four measured
455 parallel versus 509 leader puts/s, while pair six measured 586 parallel
versus 1,795 leader puts/s as the device returned to its fast regime between
arms.

These counters localize the observed cliff to device-level write latency
experienced during the run. They do not distinguish external traffic from
filesystem, controller, or device behavior caused by the benchmark itself.
A code-only WAL change cannot be credited with removing this variability
without a quieter or dedicated device-backed comparison. No pipeline change
was made from this diagnostic.

### Post-cutoff depth and completion-map probes (2026-09-27)

After the moving admission cutoff landed, the 16-writer, 1-GiB-SST ext4
workload used about 3,300 sync calls for 50,000 puts: roughly 15 tickets per
sync at the existing 16-group cap. To check whether this cap now constrained
higher concurrency, five alternating 32-writer ext4 pairs raised it from 16
to 32. The candidate reached 32 in-flight groups and 32 outstanding write
SQEs, but candidate/baseline throughput ratios were `1.011, 1.013, 0.904,
1.108, 0.945` (median `1.011`). Median sampled-p99 ratio was `1.114` and
median sync calls were nearly unchanged (2,527 versus 2,510). The extra
depth was reverted; reaching a larger software queue did not translate into
more useful durability work.

The coordinator also receives one completion per mostly solo I/O group. Its
small `BTreeMap` of out-of-order completions was temporarily changed to a
`HashMap`, since it only inserts and removes by the exact frontier ticket.
Five alternating 16-writer ext4 pairs measured candidate/baseline throughput
ratios of `1.040, 0.992, 0.964, 0.974, 0.956` (median `0.974`) and a median
sampled-p99 ratio of `1.255`. The map change was reverted. These probes
leave the per-ticket producer-to-worker-to-coordinator handoff and the ordered
sync stream as the useful targets for the next implementation experiment.

### Bounded moving admission cutoff (2026-09-27)

The 200-microsecond coalescing window originally captured `next_ticket` once
at its start. Tickets admitted while it waited for write completions could
not join that sync, even if their CQEs arrived before the fixed deadline. The
opt-in coordinator now rereads the admission cutoff after each completion.
The deadline remains fixed, so continuous admission cannot keep a sync waiting
indefinitely. The group failure and contiguous written-prefix checks still
apply before `fdatasync` captures its target.

On the 50,000-put, 16-writer, 1 KiB-value ext4 workload with a 1 GiB SST
target, five alternating pairs gave moving/fixed throughput ratios of `2.836,
1.194, 1.164, 1.116, 1.191` (median `1.191`). The first fixed-cutoff run
hit the known ext4 latency cliff; excluding that pair, the four ratios were
`1.116`–`1.194`. Median sync calls fell from 4,611 to 3,314 per 50,000 puts.
The median paired sampled-p99 ratio was `0.874`; one of five pairs had a
slightly higher candidate p99 (`1.017`).

Six alternating pairs with a 1 MiB SST target gave throughput gains of
`6.9%`–`10.1%` (median `8.9%`). Their median paired sampled-p99 ratio was
`1.020`, with individual ratios from `0.839` to `1.281`; tail latency is not
yet stable enough to claim an improvement. Three four-writer ext4 pairs had
a median throughput ratio of `1.007`, with the known device stall affecting
both arms. Three four-writer tmpfs pairs had a median ratio of `1.008` but
ranged from `0.832` to `1.081`, so there is no established tmpfs gain.

Increasing the original fixed coalescing wait from 200 to 300 microseconds
was rejected: five 16-writer ext4 pairs had only a `1.018` median throughput
ratio and a `1.169` median paired sampled-p99 ratio. The moving cutoff keeps
the accepted 200-microsecond deadline and targets unfilled barriers rather
than adding a longer delay.

The opt-in pipeline now submits multiple groups concurrently, but durable
acknowledgement still passes through one ordered `fdatasync` coordinator. On a
50,000-put, 16-writer, 1 KiB-value workload with a 1 GiB SST target, the
100-microsecond sync-coalescing window took a median 2.962 seconds end to end.
The coordinator spent 1.787 seconds inside `fdatasync` (about 60% of elapsed
time) and made 5,087 sync calls, covering only 9.83 tickets per sync with
16 writer threads. The parallel worker formed 50,000 solo I/O groups and
submitted 50,000 write SQEs. Thus more outstanding SQEs alone cannot remove
the dominant durability barrier or the per-ticket handoff cost.

Increasing the conditional coalescing window from 100 to 200 microseconds let
already-admitted writes finish before the coordinator captured its sync target.
It still waits only after the preceding sync took at least 100 microseconds;
the admission cutoff remains fixed. Five alternating, non-profiled ext4 pairs
gave candidate/baseline throughput ratios of `1.082, 1.085, 1.091, 1.090,
1.075` (median `1.085`). Median sync calls fell from 5,087 to 4,522,
tickets per sync rose from 9.83 to 11.06, aggregate `fdatasync` time fell
from 1.787 to 1.576 seconds, and wall time fell from 2.962 to 2.744 seconds.
Median sampled p99 ratio was `0.874`, though one pair was 1.097. This is a
direct throughput response to fewer partly filled sync barriers, not evidence
that the NVMe hardware queue was full.

Five pairs with a 1 MiB SST target also favored the 200-microsecond arm. Three
fast pairs gained 9–11%; two pairs encountered the known ext4 sync-latency
cliff, one in both arms. Three 50,000-put, four-writer ext4 guard pairs gained
0.6–3.0%, with lower sampled p99 in all three. Five 200,000-put, four-writer
tmpfs pairs had a 1.012 median throughput ratio but ranged from 0.844 to
1.062; same-binary tmpfs null pairs ranged from 0.921 to 1.011. The tmpfs
tail remains noisy, and the leader WAL stays the default.

With the 200-microsecond window, three alternating same-binary ext4 pairs
measured about 18,501 puts/s and 4,462 syncs for parallel WAL versus 13,030
puts/s and 6,083 syncs for leader WAL (medians): a 1.42× throughput gain,
with median sampled p99 1.966 versus 2.508 ms. Parallel WAL is faster here,
but its ordered sync stream and solo I/O groups explain why a deeper write
pipeline does not produce a many-fold speedup.

## In-flight depth experiment (2026-09-27)

On the ext4 50,000-put, 1 KiB-value, 1 MiB-SST workload, the existing parallel
worker reached its eight-group/eight-outstanding-SQE cap with 16 writers. With
4 and 8 writers, the observed peaks were 4 and 8 respectively: synchronous
client writes cannot fill more groups than there are writers. Device-wide
weighted I/O time from `/sys/block/nvme0n1/stat` implied average I/O depths of
about 1.8, 2.2, and 2.7 at 4, 8, and 16 writers. This is whole-device Linux
accounting, not NVMe hardware queue depth or a WAL-only utilization measure.

Raising only `MAX_INFLIGHT_GROUPS` from 8 to 16 let a profiled 16-writer run
reach 16 in-flight groups and 16 outstanding write SQEs. Five alternating,
non-profiled ext4 pairs with the same arguments measured:

| Pair | Eight-group ops/s | Sixteen-group ops/s | Gain | p99 ms, eight → sixteen |
| --- | ---: | ---: | ---: | ---: |
| 1 | 13,307 | 15,242 | 14.5% | 2.813 → 3.378 |
| 2 | 13,282 | 15,527 | 16.9% | 3.151 → 6.874 |
| 3 | 13,512 | 14,885 | 10.2% | 3.741 → 3.492 |
| 4 | 13,275 | 15,284 | 15.1% | 2.857 → 2.831 |
| 5 | 13,424 | 15,186 | 13.1% | 3.365 → 3.230 |

The median paired throughput gain was 14.5%; median paired p99 ratio was 0.991.
Pair 2 had a large candidate p99 spike, so the tail-latency effect is not
settled by these five runs. One exploratory 4-writer control measured 5,905 →
6,012 ops/s and an 8-writer control 10,021 → 10,488 ops/s. Neither can exercise
the higher group cap, and neither is a statistical regression test. These
results support keeping the 16-group limit for the opt-in path; they do not
establish that the SSD itself is saturated or justify changing the default
from the client-leader WAL.

A follow-up held the workload at 50,000 puts and 16 writers but raised the SST
target to 1 GiB to remove frequent flush/rotation. Five alternating ext4 pairs
gave sixteen/eight-group throughput ratios of `1.123, 1.103, 1.137, 1.151,
1.128` (median `1.128`); paired p99 ratios were `0.894, 1.004, 0.952,
0.986, 0.955` (median `0.955`). This WAL-isolated result supports the added
software depth independently of the rotation-heavy case.

Three longer 200,000-put, 1 MiB-SST pairs exposed intermittent ext4 stalls in
both arms. One eight-group run measured 4,812 ops/s and 21.1 ms p99 while its
sixteen-group pair measured 15,025 ops/s and 3.6 ms; another pair reversed
that pattern (13,298/3.8 versus 4,959/22.0). The pair without a stall measured
13,160 versus 14,712 ops/s (an 11.8% gain). The slow runs cannot be attributed
to the group limit from these measurements. The cap change is retained for the
opt-in WAL; the device-backed tail-latency cliff still needs investigation.

### Ext4 long-run sync-stall control

With the sixteen-group cap, three alternating pairs reran 200,000 puts, 16
writers, 1 KiB values, and a 1 MiB SST target on ext4. Both arms kept the WAL
and memtable flushes; one used leveled compaction and the other disabled
background compaction. The table shows throughput, sampled p99, and aggregate
WAL `fdatasync` time divided by sync count:

| Pair | Leveled ops/s / p99 ms / sync ms | No compaction ops/s / p99 ms / sync ms |
| --- | ---: | ---: |
| 1 | 14,728 / 3.65 / 0.411 | 15,383 / 3.41 / 0.371 |
| 2 | 14,740 / 3.17 / 0.415 | 15,533 / 2.98 / 0.371 |
| 3 | 5,006 / 21.69 / 1.436 | 13,667 / 7.37 / 0.442 |

In the two fast pairs, disabling compaction improved throughput by about 4–5%
and reduced mean WAL sync time by about 0.04 ms. The third leveled run
reproduced the cliff: 29.8 seconds of aggregate WAL `fdatasync` time, compared
with roughly 8.5 seconds in the fast leveled runs. Its following no-compaction
run was also somewhat slower than its earlier controls. This is consistent
with storage pressure during compaction, but three pairs and one stall cannot
separate compaction from other device activity. No compaction scheduling change
was made from this control.

### Thirty-two-group depth follow-up (rejected)

With 32 synchronous writers and a 1 GiB SST target, a profiled 50,000-put
ext4 run reached the current sixteen-group/sixteen-SQE cap. A prototype that
raised only the group cap to 32 reached 32 groups and 32 outstanding write
SQEs, but groups per sync barely changed (16.19 → 16.41 in the profiled runs).
Five alternating, non-profiled pairs compared the two caps:

| Pair | Sixteen-group ops/s | Thirty-two-group ops/s | Candidate / baseline p99 |
| --- | ---: | ---: | ---: |
| 1 | 25,551 | 24,483 | 0.981 |
| 2 | 24,882 | 24,512 | 1.176 |
| 3 | 25,609 | 26,025 | 1.263 |
| 4 | 25,623 | 25,056 | 0.974 |
| 5 | 25,178 | 25,665 | 0.689 |

Candidate/baseline throughput ratios were `0.958, 0.985, 1.016, 0.978,
1.019` (median `0.985`); median sampled p99 ratio was `0.981`, with mixed
individual tails. The extra software depth did not improve this ext4 workload.
The prototype was reverted, leaving the opt-in path at sixteen groups. This
does not measure the NVMe hardware queue depth or rule out a benefit for a
different workload.

## Run manifest

- Date: 2026-09-26, Asia/Chongqing
- Current source: base revision `ca9e45670af2b6ae716d625e4a97ba25de9ab1f7`
  plus the producer-side queue-drain change in this worktree
- Current release binary: SHA-256
  `820433a84a1409c76fd52352c96a31c87eecdd1c4654722ed0415d7224993517`
- Previous 100-microsecond parallel baseline binary: SHA-256
  `9a65a859a55c931c9e2e29a75818efffcc908badab9955cba035349a6dde20a5`
- Slice 7 binary used for the original regression case: SHA-256
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

The original Slice 7 200k-write regression case used the Slice 7 binary for
both modes; it predates the producer-side queue-drain change:

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

The follow-up below explores adjacent-ticket packing and coalesced worker
notifications. It remains below the adoption gate on the exact workload; repeat
that case in a five-pair series before considering another worker or ring.

## Exploratory optimization follow-up

After the Slice 7 measurements, the candidate packer was changed to combine up
to eight already-queued contiguous tickets into one I/O group, without adding a
timed batching delay. It now constructs the group directly from the admission
queue, removing the intermediate batch vector and second admission-lock
acquisition. Worker eventfd notifications coalesce while one wake is pending,
and the benchmark reports both notification requests and actual eventfd writes.

Five paired 20k-operation ext4 WAL-focused runs (1 GiB SST target, four
writers) measured paired parallel/leader throughput ratios of 1.089, 1.129,
1.179, 1.135, and 1.053. The median paired gain was 12.9%, with a paired
bootstrap 95% percentile interval of 5.3%–17.9%. Median p99 was 2.266 ms for
parallel versus 2.333 ms for the leader. Median group counts were 18,136
parallel versus 8,916 leader; parallel groups were still mostly solo (16,315
solo groups). Median worker eventfd writes fell from 18,136 notification
requests to 13,866 syscalls.

The exact 200k-operation ext4 case (1 MiB SST target) was also run as five
pairs. Parallel/leader throughput ratios were 1.869, 0.687, 1.521, 1.780, and
0.657; the median paired ratio was 1.521, but the paired bootstrap 95%
percentile interval (0.657–1.869) crosses parity, and only three pairs favored
parallel. Per-mode median p99 was 8.795 ms for parallel and 11.516 ms for the
leader, but paired tail latency also varied sharply. The exact case therefore
does not establish a repeatable adoption gain; keep the leader as default.

### Conditional worker wakeup

The worker now skips eventfd signaling when it is not waiting for ring or
command progress. Five paired 20k-operation ext4 runs (four writers, 1 KiB
values, 1 GiB SST target) compared this with the previous coalesced-eventfd
behavior. Run order alternated by pair. Conditional/always-signal throughput
ratios were 0.960, 0.982, 1.014, 1.053, and 0.999; the median was 0.999 and
the paired bootstrap 95% percentile interval was 0.960–1.053. Median p99 was
2.283 ms with conditional wakeups and 2.292 ms with unconditional signaling.
This did not produce a measurable throughput or tail-latency gain.

The eventfd syscall count did fall substantially: median writes went from
13,969 to 1,509 per run, an 89% reduction. Median process CPU time (user plus
system) was 1,780 ms versus 1,805 ms. Keep this as a syscall-efficiency
improvement, not as evidence that the WAL is faster. The current candidate's
separate five-pair comparison against the leader measured a 9.7% median gain
(paired bootstrap 95% interval 5.2%–12.0%); median p99 was 2.269 ms versus
2.442 ms for the leader.

At 16 writers, five more alternating pairs produced a median
conditional/always-signal throughput ratio of 0.996 (−0.4%); its bootstrap
interval was 0.376–1.011 because the first conditional run was a large outlier
(0.376). Excluding that run, the remaining four paired ratios were 0.966,
0.996, 0.997, and 1.011, with no consistent throughput gain. Median p99 was
2.426 ms versus 2.431 ms, and median eventfd writes fell from 7,232 to 2,554
(65%). This reinforces the syscall-reduction result without establishing a
speedup.

A profiled candidate run showed the larger throughput opportunity: 20,000
operations used 8,253 `fdatasync` calls covering 17,835 groups (2.16 groups
per sync), while 88.2% of commit groups contained one ticket. `fdatasync`
accounted for 3.67 seconds of the 4.47-second run. This suggests testing a
latency-bounded packer or sync-coalescing window to raise groups per sync; it
does not establish that such a window will improve the workload, since added
batching delay can hurt p99 and low-concurrency cases.

Two no-delay coordinator alternatives were also tested in five alternating
pairs on the same 20k-operation ext4 workload. Sending all group completions
from one CQE drain as one channel message had a median parallel throughput
ratio of 0.933 versus scalar completion messages (−6.7%; paired bootstrap 95%
interval 0.897–1.049). Median p99 was effectively unchanged at 2.281 ms versus
2.274 ms, while median sync calls increased from 8,614 to 9,443 and groups per
sync fell from 2.116 to 1.915. The completion-vector change was discarded.

Suppressing the durability condition-variable notification after successful
group writes also failed the throughput check: median throughput ratio was
0.914 (−8.6%; paired bootstrap 95% interval 0.509–0.975), median p99 was
2.293 ms versus 2.257 ms, and median sync calls rose from 8,707 to 9,618. It
reduced median process CPU time by about 5.7%, but reduced groups per sync from
2.077 to 1.947; the notification behavior was retained. This indicates that
the current coordinator's useful coalescing depends on scheduling between
completed groups, so reducing wakeups in isolation is not an end-to-end win.

### Conditional sync coalescing follow-up

The initial coalescing candidate waited up to 50 microseconds only when a contiguous written
prefix is ready and tickets already admitted at that instant remain unwritten.
It freezes the ticket cutoff and deadline; new admission cannot prolong the
wait. It still syncs the captured written prefix after timeout, worker closure,
or poison. This differs from adding a delay to every group or to the
solo-leader path.

Five alternating paired ext4 runs of the 20k-operation, four-writer,
1-KiB-value, 1-GiB-SST workload measured coalescing/no-wait throughput ratios
of 1.529, 1.484, 1.434, 1.434, and 1.383 (median 1.434). Median sync calls
fell from 9,097 to 5,459 and groups per sync rose from 2.00 to 3.15. Three
additional pairs with 2,000 latency samples per run measured a median 1.412
throughput ratio and a median p99 ratio of 0.585. Four 5k-operation,
one-writer no-wait/coalescing pairs measured a median throughput ratio of
1.006, with no observed p99 regression; the one-writer path normally has no
later admitted ticket to wait for.

Five further alternating ext4 pairs compared the optimized parallel path with
the leader in the same binary: throughput ratios were 1.516, 1.500, 1.610,
1.626, and 1.627 (median 1.610; paired bootstrap 95% interval 1.500–1.627).
Median p99 was 1.677 ms for parallel versus 2.452 ms for the leader. Three
5k-operation one-writer leader/parallel pairs measured a median throughput
ratio of 0.965 and p99 ratio of 1.029. One earlier 20k one-writer pair was
discarded as inconclusive because mean `fdatasync` time changed from 0.47 to
1.36 ms between arms; the four shorter alternating pairs had stable mean sync
times near 0.33 ms.

This is a measured improvement for the WAL-isolated four-writer ext4 case.
The original 200k-operation, 1-MiB-SST regression case, tmpfs, and the full
writer-count/workload matrix have not been rerun with this optimization, so
the original decision to leave parallel WAL opt-in remains in force.

The 20k-operation, 1-MiB-SST rotation case was then rerun. Five ext4
leader/parallel pairs had throughput ratios 0.653, 2.472, 4.492, 1.408, and
1.434 (median 1.434), with large `fdatasync` latency swings between arms.
The same tmpfs case had ratios 0.304, 0.254, 0.447, 0.295, and 0.451 (median
0.304). A separate three-pair tmpfs comparison found that removing the
50-microsecond wait improved the parallel path by about 15% at the median,
but left most of its gap to the leader. A release-only removal of worker
invariant scans measured a 1.012 median ratio in five tmpfs pairs and was
discarded as noise.

To avoid the cheap-sync penalty, coalescing now requires the preceding
`fdatasync` to have taken at least 100 microseconds. Five alternating tmpfs
fixed/adaptive pairs measured adaptive throughput ratios 1.134, 1.078, 1.084,
0.833, and 1.049 (median 1.078). Ten additional alternating tmpfs pairs
measured a median ratio of 1.143, with a paired bootstrap 95% interval of
1.100–1.216; nine of ten favored adaptive coalescing. Five ext4 WAL-isolated
pairs measured
0.987, 1.005, 1.031, 1.025, and 0.996 (median 1.005); three one-writer
ext4 pairs measured a median ratio of 1.006 and p99 ratio of 1.007. A direct
five-pair tmpfs leader/adaptive comparison on the rotation case still had a
parallel/leader median of 0.468. The adaptive wait is a modest improvement
for fast sync, not a solution to the parallel pipeline's tmpfs CPU and
thread-handoff overhead.

### Adaptive candidate on the original 200k-write case

Three alternating leader/parallel pairs reran the original regression workload:
200,000 single puts, four writers, 1-KiB values, a 1-MiB SST target, and one
latency sample per 100 operations. The parallel/leader throughput ratios on
ext4 were 0.993, 1.400, and 2.234 (median 1.400); p99 ratios were 1.012,
0.876, and 0.198 (median 0.876). The wide variation, including a pair below
parity, does not satisfy the adoption gate. On tmpfs, throughput ratios were
0.499, 0.497, and 0.453 (median 0.497), while p99 ratios were 2.358, 1.818,
and 1.858 (median 1.858). The leader remains the default.

The tmpfs median process CPU time was 7.43 seconds for parallel versus 2.83
seconds for the leader. Median aggregate `fdatasync` time was only 25 ms versus
19 ms, respectively, so sync latency cannot explain the gap. A separate
50,000-put tmpfs profile measured roughly 61,000 parallel ops/s at 1.91 seconds
of process CPU versus 150,000 leader ops/s at 0.71 seconds. This points to
per-ticket CPU and handoff costs in the parallel pipeline as the next area to
reduce; the profile alone does not attribute the full difference to one call.

Three low-risk prototypes were measured and discarded. A 5-microsecond packer
wait reduced group count but had a 0.602 median throughput ratio against the
unchanged parallel path in five tmpfs pairs, and one ext4 pair also regressed.
Replacing the worker maps with `AHashMap` gave a 0.971 median ratio in five
tmpfs pairs. Bounding the worker command channel gave a 0.893 median ratio in
seven tmpfs pairs. None is retained. Further speedup likely requires reducing
the number of per-ticket producer/packer/worker/coordinator handoffs, while
preserving ordered offset allocation and the durability frontier. This is a
new implementation slice, not a conclusion that parallel WAL is faster on all
storage.

An additional inline-event prototype replaced per-completion event vectors
with `SmallVec`. Twenty alternating tmpfs pairs of the 50k-put WAL-isolated
workload had a 1.029 median throughput ratio against the unchanged parallel
path, with 13 of 20 pairs favoring the prototype. Five alternating ext4 pairs
of the 20k-put WAL-isolated workload measured 1.024, 0.905, 1.011, 0.889, and
0.925 (median 0.925). Because the device-backed case regressed, the prototype
and its direct dependency were reverted. Reducing worker allocations alone
did not address the dominant pipeline cost.

### Conditional sync window: 100 versus 50 microseconds

The same adaptive gate was retained: the coordinator waits only if the
preceding `fdatasync` took at least 100 microseconds and a written prefix has
already-admitted unfinished tickets behind it. The maximum wait increased
from 50 to 100 microseconds; the captured cutoff still cannot move.

Five alternating ext4 pairs on the 50k-put, four-writer, 1-MiB-SST workload
measured 100/50-microsecond throughput ratios of 1.127, 1.150, 1.157, 1.151,
and 1.153 (median 1.151). Median p99 ratio was 0.908. Median sync calls fell
from 16,170 to 13,056, and median aggregate `fdatasync` time fell from 6.28
to 4.80 seconds. Ten ext4 pairs on the 20k-put WAL-isolated workload had a
1.05 median throughput ratio, but two pairs experienced large device-latency
swings; the rotation-heavy result is the more stable comparison.

Three pairs of the original 200k-put ext4 workload measured throughput ratios
of 2.051, 2.092, and 1.134. The 50-microsecond arm varied from 2.72k to
4.97k ops/s, while the 100-microsecond arm stayed between 5.64k and 5.70k
ops/s. The first two pairs had much slower baseline syncs, so the apparent
twofold median gain should not be generalized. Sync counts were about 64–66k
for 50 microseconds and 53–54k for 100 microseconds. This is a comparison
between two parallel configurations, not a new parallel-versus-leader
adoption result.

Five tmpfs pairs of the same 200k-put workload had a 1.021 median throughput
ratio and a 1.082 median p99 ratio. Five one-writer ext4 pairs of 5k puts had
a 0.994 median throughput ratio and a 0.984 median p99 ratio. The larger
window is retained as a scoped device-backed improvement; the leader remains
the default because the parallel path is still much slower on tmpfs. Raw
50k-put ext4 and 200k-put ext4 JSON are under
`target/rfc024-sync-window-rotation` and `target/rfc024-sync-window-exact`;
the 200k-put tmpfs JSON is under `/tmp/rfc024-sync-window-exact`.

### Current 100-microsecond path and next optimization target

Five alternating leader/parallel pairs reran the 50k-put, four-writer,
1-KiB-value, 1-MiB-SST workload with the current 100-microsecond parallel
window. The same binary was used for both modes. On ext4, paired parallel /
leader throughput ratios were 1.580, 2.927, 1.570, 1.591, and 1.585 (median
1.585). The second leader run was a slow device outlier; the other four pairs
clustered around 1.58. Median sync calls were 13,073 parallel versus 21,725
leader. The five paired p99 ratios ranged from 0.654 to 1.295, so this run
does not establish a consistent tail-latency improvement.

On tmpfs, the five parallel / leader throughput ratios were 0.446, 0.515,
0.406, 0.534, and 0.423 (median 0.446). Median process CPU time was 2.51
seconds parallel versus 0.81 seconds leader, and paired p99 ratios ranged
from 1.443 to 2.621. The ext4 gain does not make the parallel path suitable
as the default while this fast-filesystem regression remains.

Two more isolated changes were rejected. Raising the conditional window from
100 to 200 microseconds gave a 1.020 median throughput ratio in five paired
ext4 50k-put runs, but a 0.983 throughput ratio and 1.063 p99 ratio in five
paired tmpfs 200k-put runs. Compiling out three admission-mutex reads used
only by assertions yielded a 0.983 median throughput ratio and 1.152 p99
ratio in five paired tmpfs 200k-put runs. Both experiments were reverted.

The tmpfs CPU profile sampled the worker command-channel receive, ordered
packer submission, and sync coordination. The next experiment removed the
dedicated packer thread while preserving ticket/offset order, preallocation
before submission, bounded in-flight groups, and the contiguous durability
frontier. Results follow.

### Producer-side queue draining

After admitting a ticket, a producer tries to acquire the packer state without
blocking while it still holds the admission mutex. If it succeeds, it releases
admission and drains ready tickets in order, preallocates outside the admission
mutex, and submits groups to the existing I/O worker. Other producers can
continue admission while that drain is in progress. The drainer checks for an
empty queue while holding admission and releases the packer state before
releasing admission, so a concurrent enqueue either becomes visible to the
current drainer or claims the packer itself. Close stops admission, acquires
the same packer state, and drains through its captured cutoff.

Five alternating pairs compared this candidate to the previous 100-microsecond
parallel path on the 50,000-put, four-writer, 1-KiB-value, 1-MiB-SST workload.
The candidate used one I/O group per ticket (50,000 groups in every run); the
previous path produced a median 42,059 groups on ext4 and 38,783 on tmpfs.

| Filesystem | Candidate / previous throughput | 95% bootstrap interval | Candidate / previous p99 | Candidate / previous process CPU |
| --- | ---: | ---: | ---: | ---: |
| ext4 | 0.994 | 0.968–1.076 | 0.908 | 0.952 |
| tmpfs | 1.320 | 1.168–1.334 | 0.673 | 0.761 |

Intervals enumerate all 3,125 bootstrap resamples of the five paired median
ratios, using nearest-rank 2.5th and 97.5th percentiles. The candidate was
effectively at parity on ext4 and improved tmpfs throughput by 32%, p99 by
33%, and process CPU by 24% against the previous parallel path.

Five fresh alternating leader/candidate pairs used the same candidate binary
for both modes. The table reports candidate divided by leader; p99 values
above 1 mean worse candidate tail latency. Throughput ratios by pair were
`1.570, 1.532, 1.563, 0.843, 1.551` on ext4 and
`0.871, 0.654, 1.004, 0.850, 0.688` on tmpfs.

The intervals in the next table use the same exhaustive bootstrap method.

| Filesystem | Median throughput ratio | 95% bootstrap interval | Median p99 ratio | Median process CPU ratio |
| --- | ---: | ---: | ---: | ---: |
| ext4 | 1.551 | 0.843–1.570 | 0.924 | 1.500 |
| tmpfs | 0.850 | 0.654–1.004 | 1.281 | 1.485 |

The candidate reached four in-flight groups on both filesystems. Its ext4
median was 55% above leader, but the paired interval includes parity and
process CPU was 50% higher, so the device-backed adoption gate is not
established. On tmpfs the candidate lost to leader in four of five pairs and
used about 49% more process CPU. Keep this optimization in the opt-in parallel
path; do not change the leader default.

The paired JSON summaries are `/tmp/rfc024-current-lp-ext4.json`,
`/tmp/rfc024-current-lp-tmpfs.json`, `/tmp/rfc024-200vs100-ext4.json`,
`/tmp/rfc024-200vs100-tmpfs.json`, and `/tmp/rfc024-lock-exact-tmpfs.json`.
The current CPU profiles are `/tmp/rfc024-current-parallel.perf` and
`/tmp/rfc024-current-leader.perf`.

The producer-side queue-drain comparisons are summarized in
`/tmp/rfc024-fused-packer-ext4.json` and
`/tmp/rfc024-fused-packer-tmpfs.json`. Current candidate/leader raw JSON is in
`target/rfc024-fused-leader-ext4-runs` and
`/tmp/rfc024-fused-leader-tmpfs-runs`.

### Production-build publication-lock experiment

**Run date:** 2026-09-27.

To avoid charging parallel WAL for benchmark-only profile counters, this
follow-up used `write-perf` built in release mode without the `bench` Cargo
feature. Five alternating 20k-operation pairs (four writers, 1-KiB values,
1-GiB SST target, tmpfs) tested a publication fast path that waits on the
atomic publication frontier before taking the publication mutex. Parallel
throughput ratios were `1.396, 0.977, 1.287, 1.051, 1.085`; the median was
1.085, but the paired bootstrap 95% interval was `0.977–1.396`. Median p99
increased by 16% and median process CPU fell by 12%. Five ext4 pairs at 5k
operations measured a 1.013 median throughput ratio (95% interval
`0.990–1.062`), with no established gain.

The 20k result did not reproduce at 100k operations with 1,000 latency samples
per run. Five tmpfs throughput ratios were `0.792, 0.882, 1.167, 1.004,
1.117`; the median was 1.004 and its 95% interval was `0.792–1.167`. Median
p99 and process CPU ratios were 0.994 and 0.993, respectively. This is too
variable to support the change. A second experiment reduced the publication
spin budget from 16,384 to 1,024 iterations; five 20k tmpfs pairs had a 0.942
median throughput ratio and 1.061 median CPU ratio. Both changes were
discarded, and the committed publication path remains unchanged.

An atomic active-group counter was also tested to avoid taking the worker slot
mutex when a group completes. On the 50k-operation, four-writer,
1-GiB-SST workload, five paired ratios against the prior worker were 0.931 on
tmpfs (95% interval `0.671–1.181`) and 1.006 on ext4 (95% interval
`0.982–1.012`); neither showed a repeatable gain, so that change was discarded
as well. These measurements reinforce that the next optimization needs to
reduce per-ticket coordination work as a whole; removing one lock or adjusting
the spin budget does not yet improve the end-to-end result reliably.

Raw JSONL outputs from the Slice 7 session are in
`/tmp/rfc024-slice7-artifacts`; the current 20k WAL-focused outputs are in
`/tmp/rfc024-packer-single-allocation-five-pair`, and the exact-case pairs are
in `/tmp/rfc024-packer-single-allocation-exact-pair` and
`/tmp/rfc024-packer-single-allocation-exact-followup`. The wakeup comparison
is in `/tmp/rfc024-wake-ab-five-pair`, the updated candidate/leader comparison
is in `/tmp/rfc024-worker-wait-five-pair`, and the diagnostic profile is in
`/tmp/rfc024-wake-profile.json` and `/tmp/rfc024-wake-profile.stderr`. The
16-writer wakeup comparison is in `/tmp/rfc024-wake-ab-16w-five-pair`. The
persistent summary tables above record the adoption-relevant measurements.
The completion-message comparison is in `/tmp/rfc024-cqe-batch-ab`, and the
condition-variable notification comparison is in
`/tmp/rfc024-no-written-wakeup-ab`. The conditional-coalescing JSON outputs
are under `target/rfc024-coalesce-paired`, `target/rfc024-coalesce-p99`,
`target/rfc024-coalesce-solo`, `target/rfc024-coalesce-leader`, and
`target/rfc024-coalesce-leader-solo`. The rotation comparison is under
`target/rfc024-coalesce-rotation` and `/tmp/rfc024-coalesce-rotation`; the
adaptive comparisons are under `/tmp/rfc024-adaptive-ab`,
`/tmp/rfc024-adaptive-ab-extended`,
`target/rfc024-adaptive-ab`, `target/rfc024-adaptive-solo`, and
`/tmp/rfc024-adaptive-leader-tmpfs`. The original 200k-write adaptive pairs
are under `target/rfc024-adaptive-exact` (ext4) and
`/tmp/rfc024-adaptive-exact` (tmpfs). The 50k-put profiles are
`/tmp/rfc024-isolated-parallel.perf` and
`/tmp/rfc024-isolated-leader.perf`.

### Atomic buffer-budget fast path (rejected)

**Run date:** 2026-09-27.

The experiment replaced the buffer-budget mutex on the normal reservation and
write-completion paths with an atomic byte counter. The mutex remained for
capacity waits, oversized-buffer fairness, and close wakeups. Five alternating
release-build pairs compared the change with the committed runtime on tmpfs
using 50,000 puts, four writers, 1-KiB values, a 1-GiB SST target, and one
latency sample per 100 operations. Candidate/baseline throughput ratios were
`0.677, 0.940, 1.105, 0.978, 0.842`; the median was `0.940` and the exhaustive
paired bootstrap 95% interval was `0.677–1.105`. Median p99 and process CPU
ratios were `1.476` and `1.076`. One ext4 screening pair measured `0.511`
throughput, `1.622` p99, and `1.214` process CPU ratios; that single pair is
not a device-backed estimate, but also gave no reason to continue the run.
The prototype was reverted. Reducing these per-buffer mutex acquisitions did
not improve end-to-end performance, so further work should focus on reducing
the pipeline's producer-to-worker/coordinator handoffs rather than replacing
another individual lock.

The raw JSONL from this screening session is
`/tmp/rfc024-buffer-budget-pairs.jsonl`.

### Inline storage for single-write groups

**Run date:** 2026-09-27.

The producer-side packer commonly sends one-write groups. Its packed write
vector and the worker's per-group request-ID vector each allocated on the
heap for that case. `WriteGroupBuffers` now stores the first write inline and
spills only additional writes to a `Vec`; request IDs are represented by the
contiguous range assigned to the group. This removes those two heap
allocations for each single-write group without adding a dependency. The
multi-write path remains covered by the parallel worker tests.

Alternating release-build pairs used 50,000 puts, four writers, 1-KiB values,
a 1-GiB SST target, a 100-operation latency sample interval, and the opt-in
parallel v4 WAL path. On tmpfs, five candidate/baseline throughput ratios were
`1.254, 1.003, 0.998, 1.571, 0.918`; the median was `1.003` and the exhaustive
paired bootstrap 95% interval was `0.918–1.571`. Median p99 and process CPU
ratios were `1.035` and `0.959`.

Ten ext4 throughput ratios were `1.017, 1.013, 1.016, 1.002, 1.014, 1.004,
0.986, 1.022, 4.991, 1.623`. The median was `1.015`; a 200,000-resample
paired bootstrap 95% interval was `1.004–1.320`. Median p99 and process CPU
ratios were `0.966` and `0.960`. The last two pairs included severe device
slowdowns: baseline throughput fell to 1,297 and 1,030 ops/s, compared with
roughly 6,300–6,500 ops/s in the first eight pairs. All completed pairs are
reported; this noise makes the upper confidence bound imprecise.

The result is a small ext4 improvement with lower process CPU, and no
measurable tmpfs throughput change. Keep the allocation reduction in the
opt-in parallel path; it does not satisfy the separate gate for changing the
leader default. Raw outputs are under `/tmp/rfc024-inline-group-tmpfs` and
`target/rfc024-inline-group-ext4`.

### Fixed-capacity worker command queue (rejected)

**Run date:** 2026-09-27.

A fresh, low-overhead `perf record` could not run alongside io_uring because
the perf mmap caused ring creation to fail with `ENOMEM`. A smaller mmap did
capture 51 user samples, with 8.93% lost; the worker command-channel receive
accounted for 13.73% of those samples. This is directional evidence only.

The experiment replaced the allocating unbounded crossbeam worker-command
channel with the existing `ArrayQueue`, sized for eight admitted groups plus
one shutdown command. The worker used its eventfd to wake from `poll` when
idle. Alternating release-build pairs used 50,000 puts, four writers, 1-KiB
values, a 1-GiB SST target, and the opt-in parallel v4 WAL path. Five tmpfs
candidate/baseline throughput ratios were `0.745, 0.435, 0.877, 0.998, 1.002`
(median `0.877`; exhaustive paired bootstrap 95% interval `0.435–1.002`).
Median p99 and process CPU ratios were `1.260` and `1.080`.

Three ext4 throughput ratios were `1.004, 0.992, and 0.993` (median `0.993`;
exhaustive paired bootstrap 95% interval `0.992–1.004`). Median p99 and
process CPU ratios were `0.993` and `0.912`. The ext4 screen was effectively
at throughput parity, while tmpfs regressed and the candidate added idle
`poll` wakeups. Revert the queue change; retain the unbounded channel.

The raw benchmark JSON is under `/tmp/rfc024-arrayqueue-bench`. A more
promising next experiment should reduce the number of per-group handoffs,
for example by batching already-formed groups into one worker command, rather
than replacing the worker's queue and wait primitive independently.

### Follow-up: bounded channel and one scheduler yield (rejected)

**Run date:** 2026-09-27.

On the current inline-group baseline, a bounded crossbeam command channel
retained the worker's blocking receive path. Five alternating tmpfs pairs of
100,000 puts measured candidate/baseline throughput ratios of `0.972, 1.056,
0.833, 0.795, 0.713` (median `0.833`). Three 10,000-put ext4 pairs measured
`0.976, 0.945, 1.024` (median `0.976`). This confirms the earlier bounded
channel rejection without changing the worker wakeup mechanism. The change was
reverted.

A separate candidate yielded the CPU once after taking the packer mutex and
releasing the admission mutex, giving other already-encoding writers a chance
to join the group without a fixed timer. With the same alternating setup,
tmpfs throughput ratios were `0.805, 1.078, 1.434, 0.818, 0.804` (median
`0.818`); ext4 ratios were `1.022, 0.986, 0.985` (median `0.986`). The yield
was also reverted. These small samples, especially tmpfs, have substantial
run-to-run variance, but neither candidate met the improvement gate.

For a separate 200,000-put tmpfs profile with four writers, the current
parallel path completed roughly 45,000 puts/s and used 6.65 seconds of system
CPU; the leader completed roughly 213,000 puts/s and used 1.18 seconds of
system CPU. The profile was sampled in user space only because this host's
`perf_event_paranoid=2` disallows kernel samples. The observed gap is consistent
with the parallel path's per-group submit/wakeup work, but these samples do not
identify which kernel operation dominates. More queue or lock substitutions
are unlikely to close it; a future slice should reduce complete per-ticket
handoffs and measure syscall counts and group formation before changing the
default.

A follow-up `/proc` thread-CPU sample of the same 200k-put tmpfs case measured
about 4.37 seconds of system CPU in the writer threads and 1.84 seconds in the
kernel io_uring worker for parallel WAL, versus about 0.87 seconds in writers
and 0.32 seconds across kernel io_uring workers for the leader path. These are
sampled thread totals, not syscall attribution. A 20k-put, 1-GiB-SST ext4
sample instead ran at about 6.38k puts/s parallel versus 3.77k leader. The
two filesystems therefore need separate adoption decisions.

Forcing the existing 100-microsecond sync coalescing window after every sync,
including cheap tmpfs syncs, did not solve the fast-filesystem cost. Three
alternating 20k-put, four-writer, 1-GiB-SST pairs gave candidate/baseline
tmpfs throughput ratios of 0.947, 1.038, and 0.842. Ext4 ratios were 1.002,
1.037, and 0.993. The change was reverted. This small screen does not support
extra delay as an optimization; the next candidate should reduce per-ticket
work or form larger groups without inserting a wait into every commit.

### Follow-up: assertion lock and 16-group cap (rejected)

**Run date:** 2026-09-27.

Moving two admission-mutex reads into their `debug_assert!` expressions did
not help the release build. Five 10,000-put, four-writer ext4 pairs measured
candidate/baseline throughput ratios of `1.036, 0.969, 0.978, 0.952, 0.935`
(median `0.969`). The five 100,000-put tmpfs pairs ranged from `0.515` to
`1.617`, too noisy to support a gain. The source change was reverted.

Doubling the worker's in-flight-group limit from eight to sixteen gave five
20,000-put, 16-writer ext4 ratios of `0.966, 1.007, 1.006, 0.994, 0.961`
(median `0.994`). Three four-writer ext4 pairs had median `1.014`; three
eight-writer ext4 pairs had median `1.003`. Thus more group slots did not
improve the device-backed throughput. Five 100,000-put, 16-writer tmpfs pairs
all favored the larger cap (median `1.226`), but four-writer tmpfs pairs also
showed a `1.248` median even though those writers cannot fill eight slots.
Same-binary tmpfs null pairs ranged from `0.692` to `1.175` with four writers
and `0.883` to `1.110` with sixteen. The tmpfs signal warrants a controlled
retest, not a cap increase that leaves ext4 and the main four-writer regression
unchanged. The limit remained eight after this shorter experiment. Later
50,000-put rotation-heavy and WAL-isolated comparisons above supported raising
it to sixteen; the two sets of measurements used different run lengths.

The principal cost remains group formation and handoff: the original
four-writer regression recorded about 200,000 solo parallel I/O groups versus
about 94,000 leader commit groups for 200,000 puts. Raising queue depth cannot
merge those groups. The completion-triggered candidate below tested one way
to accumulate tickets; it did not improve the device-backed workload.

### Completion-triggered packing (rejected)

**Run date:** 2026-09-27.

A candidate left one admission ticket queued while an earlier group was
active, packing when a second ticket arrived or when the coordinator consumed
the earlier group's completion. An idle WAL still packed immediately. The
focused parallel-WAL nextest suite passed all 68 selected tests, and the
all-features Clippy check passed, but paired release benchmarks rejected the
change.

With 100,000 puts and four writers, five tmpfs candidate/baseline throughput
ratios were `1.045, 0.828, 0.784, 0.716, 1.002` (median `0.828`); the median
sampled p99 ratio was `1.820`. With 20,000 puts and four writers on ext4, all
five ratios were below one: `0.945, 0.934, 0.915, 0.932, 0.931` (median
`0.932`), with higher system CPU in every pair. Three eight-writer ext4 pairs
had a `0.991` median. The single-writer ext4 runs experienced large device
latency swings and are not useful as a performance estimate. The source
change was reverted.

This experiment let the queue accumulate, but added a slot-state lookup to
admission and moved some preallocation and group submission onto the sync
coordinator's completion path. The measurements do not isolate either cost;
they establish that this handoff design is slower. A future group-formation
change should remove a pipeline handoff rather than wait for another one.

### Dedicated packer thread (rejected)

**Run date:** 2026-09-27.

Moving queue draining and group submission from writers to a dedicated packer
reduced a profiled 20,000-put run from 20,000 solo I/O groups to about 17,000
groups on both tmpfs and ext4. The focused parallel-WAL tests passed (12/12),
as did all-features Clippy. Three short paired screens were inconclusive:
tmpfs median candidate/baseline throughput was `1.127`, and ext4 was `0.998`.

Five pairs of the original 200,000-put, four-writer, 1-MiB-SST tmpfs workload
were decisive: throughput ratios were `0.796, 0.800, 0.817, 0.849, 0.798`
(median `0.800`); median p99 and process-CPU ratios were `1.238` and `1.257`.
The separate packer thread cost more than the modest reduction in I/O groups
saved, so the code was reverted. Raw paired output is in
`/tmp/rfc024-dedicated-packer-exact-tmpfs.json`.

### io_uring SQPOLL (rejected)

**Run date:** 2026-09-27.

This host permits SQPOLL. A prototype enabled a one-millisecond SQPOLL idle
period for the parallel WAL ring. The worker needed SQPOLL-specific submit
accounting: the kernel poller can consume an SQE before `submit()` samples the
queue, so a zero return does not mean that the staged write made no progress.

Three paired 100,000-put tmpfs runs favored SQPOLL, with throughput ratios
`1.510, 1.419, 1.524`. Ext4 ratios were `1.024, 0.939, 1.261`, amid large
device-latency swings. SQPOLL used substantially more CPU on ext4 and had
worse p99 in most pairs. In one run of the original 200,000-put tmpfs workload,
SQPOLL parallel WAL reached 111k puts/s and 0.374 ms p99, versus 144k puts/s
and 0.186 ms for leader WAL; measured process CPU was 2.16 times higher.
SQPOLL therefore did not close the leader gap or satisfy the latency and CPU
tradeoff, and the prototype was reverted. The paired screen is in
`/tmp/rfc024-sqpoll-ab.json`.

### Two in-flight groups (rejected)

**Run date:** 2026-09-27.

Reducing the parallel worker's group cap from eight to two was tested as a way
to let more admitted tickets accumulate before the producer-side packer runs.
Five paired 100,000-put, four-writer tmpfs runs had candidate/baseline
throughput ratios `1.072, 0.828, 1.022, 0.795, 1.250` (median `1.022`).
Measured process CPU fell by a median 23%, but throughput and p99 varied widely.
The non-instrumented build did not report group counts, so the proposed
group-formation mechanism was not verified.

Three paired 10,000-put ext4 runs had throughput ratios `0.741, 0.785, 0.719`
(median `0.741`), and p99 was worse in all three pairs. Process CPU was about
half of baseline because the cap allowed less work to overlap, while the
device-backed workload became slower. The eight-group cap was restored.
Raw results are in `/tmp/rfc024-cap2-ab-tmpfs.json` and
`/tmp/rfc024-cap2-ab-ext4.json`.

### Worker-owned packing (rejected)

**Run date:** 2026-09-27.

An experimental runtime sent ticketed buffers directly from admission to the
I/O worker. The worker packed up to eight contiguous tickets, preallocated the
range, and submitted the resulting group. This removed the producer-side
packer/group handoff while retaining the eight-group in-flight cap. The mode
was enabled only by `TOYKV_WAL_WORKER_PACK=1`; the existing path was the
same-binary control. Its focused parallel-WAL and failpoint tests passed
(13/13), as did a worker packing unit test and all-feature Clippy.

Three paired 100,000-put tmpfs runs with four writers and a 1-GiB SST target
showed worker/control throughput ratios `1.440, 1.439, 1.306` (median
`1.439`), with median process CPU ratio `0.671`. Three 10,000-put ext4 pairs
were near parity (`1.048, 0.976, 0.999`), without a clear p99 gain. A
profiled 20,000-put tmpfs run reduced I/O groups from 20,000 to 16,643 and
solo groups from 20,000 to 13,431.

The original 200,000-put, four-writer tmpfs case with a 1-MiB SST target
reversed the result. Three paired worker/control throughput ratios were
`0.891, 0.970, 0.975` (median `0.970`), and p99 ratios were `1.564, 1.002,
1.372`. A separate profiled 20,000-put run of that workload still reduced
groups from 20,000 to 15,359, but throughput fell from 84.8k to 64.8k
puts/s. Thus fewer groups did not improve the end-to-end regression case.
The prototype was reverted. Raw paired results are in
`/tmp/rfc024-worker-pack-ab.json` and `/tmp/rfc024-worker-pack-exact.json`.

### Release-build check of the original tmpfs regression

**Run date:** 2026-09-27.

The original 200,000-put, four-writer, 1-KiB-value, 1-MiB-SST tmpfs workload
was rerun in three pairs with both the `bench`-feature binary and a normal
release binary. Neither binary used `--profile`. The normal release build
removes the benchmark counters from the WAL hot path, so this comparison tests
whether instrumentation itself explains the parallel/leader gap.

| Build | Leader puts/s (three runs) | Parallel puts/s (three runs) | Median paired parallel/leader ratio |
| --- | --- | --- | --- |
| `bench` release | 148,738; 174,567; 155,441 | 96,015; 99,151; 104,845 | 0.646 |
| Normal release | 169,186; 151,652; 151,676 | 91,584; 93,444; 92,987 | 0.613 |

The normal release build did not close the gap. Its parallel runs consumed
about 5.6–5.8 seconds of process CPU versus 2.4–2.9 seconds for leader WAL,
and sampled p99 was 0.238–0.377 ms versus 0.162–0.177 ms. The paired raw
results are in `/tmp/rfc024-build-compare.json`.

A separate diagnostic run of this workload produced 200,000 solo parallel
groups and 85,529 syncs, versus 94,036 leader groups and syncs. Aggregate
`fdatasync` time was only 27 ms for parallel and 22 ms for leader; measured
preallocation and memtable insertion were also much smaller than the total
gap. User-space CPU sampling found WAL worker channel receive, request-map
hashing, and coordinator notification work, alongside SST building and MVCC
publication. Sampling did not cover kernel CPU, so these symbols alone cannot
apportion the full cost.

The next candidate should eliminate a per-ticket pipeline handoff while still
packing contiguous tickets and permitting multiple groups in flight. Merely
waiting to accumulate tickets has already failed on the exact workload, and
removing benchmark counters is not a performance fix. Any candidate must be
compared against the same normal release binary on this case and checked on a
device-backed filesystem before adoption.

### Shared WAL completion channel

**Run date:** 2026-09-27.

The parallel sync coordinator formerly selected between the I/O worker's
completion channel and a separate packer-failure channel for each result. Both
now send to one channel. The normal path uses one blocking receive and drains
ready results with `try_recv`; the packer-error sender is dropped before worker
shutdown, so channel disconnection still marks both producers finished.

Ten alternating pairs of the exact 200,000-put, four-writer, 1-MiB-SST tmpfs
workload used normal release binaries without `--profile`. Candidate/control
throughput had a median ratio of `1.062` and favored the candidate in nine of
ten pairs. Median sampled p99 ratio was `1.037`, with mixed individual runs.
Five 20,000-put pairs on the repository's ext4 mount had throughput ratios
`1.018, 1.011, 1.007, 1.023, 1.003` (median `1.011`); median sampled p99
ratio was `0.946`. Raw results are in `/tmp/rfc024-one-channel-tmpfs.json`,
`/tmp/rfc024-one-channel-tmpfs-repeat.json`, and
`/tmp/rfc024-one-channel-ext4-real.json`. The change passed 65 parallel-path
tests, all-feature Clippy, and formatting checks.

This removes a small coordinator cost; it does not fix the underlying solo-
group count, and parallel WAL remains slower than leader WAL on the original
tmpfs workload. The ext4 throughput result is near parity, so this is not an
adoption-gate result.

### Wake durability waiters only after durability or poison

**Run date:** 2026-09-27.

The coordinator previously called `notify_all` after every successful write
group completion, when only the written frontier had advanced. Durability
waiters cannot return until `fdatasync` advances the durable frontier. The
candidate retains immediate notification on group failure and the existing
notifications on successful sync, sync failure, and terminal shutdown.

Five alternating pairs of the exact 200,000-put tmpfs workload compared
normal release binaries without `--profile`. Candidate/control median ratios
were `1.140` for throughput, `0.830` for sampled p99, and `0.847` for process
CPU. All five throughput pairs favored the candidate. On the repository's ext4
mount, ten 20,000-put pairs had median ratios of `1.026` for throughput,
`1.002` for sampled p99, and `0.874` for process CPU. Ext4 had large external
latency swings: one candidate run and three runs spanning both arms fell far
below the usual 5.8–6.1k puts/s range. Those outliers limit the strength of
the device-backed throughput conclusion. Raw results are in
`/tmp/rfc024-durable-wake-tmpfs.json`, `/tmp/rfc024-durable-wake-ext4.json`,
and `/tmp/rfc024-durable-wake-ext4-repeat.json`.

The change passed 65 parallel-path tests, all-feature Clippy, and formatting
checks. It reduces futile writer wakeups, but does not reduce the solo-group
count or close the full tmpfs gap to leader WAL.

### Current candidate versus leader on ext4

**Run date:** 2026-09-27. After the shared completion channel and durability-
only waiter wakeup changes, normal release builds of the same source were run
in both WAL modes on the repository's ext4 mount. The 50,000-put, four-writer,
1-KiB-value, 1-MiB-SST case used five alternating leader/parallel pairs. The
parallel/leader throughput ratios were `1.602, 1.597, 1.585, 5.749, 1.569`.
The fourth leader run fell to 1,035 puts/s while the other four leader runs
were 3,727-3,851 puts/s; parallel stayed at 5,951-6,043 puts/s. Excluding
that device-latency outlier, the four paired gains were 57-60%. The median
paired sampled p99 ratio was `1.013`, with two pairs above 1.10, so this
measurement does not establish a tail-latency improvement.

Three alternating pairs of the original 200,000-put case used the same build
and parameters apart from operation count. Throughput ratios were 1.160, 0.921,
and 2.486; sampled p99 ratios were 3.806, 3.773, and 0.171. Leader throughput
varied from 2,254 to 3,780 puts/s and parallel throughput from 3,480 to
5,604 puts/s. This case has not cleared the adoption gate: one pair loses to
leader and the tail-latency result changes direction with the storage-latency
swings. Raw runs are under `/tmp/rfc024-current-*-sample.json`,
`/tmp/rfc024-current-direct-*`, and `/tmp/rfc024-current-200k-ext4-*`.

A separate profiled 200,000-put parallel run reached 5,770 puts/s with a
2.016-ms sampled p99. It submitted 200,000 solo I/O groups and completed
54,342 syncs, or 3.68 groups per sync; the maximum observed in-flight groups
and outstanding write SQEs were both four, matching the four synchronous
writers. Later write completions occurred during 7,246 sync calls (13.3% of
syncs). Mean `fdatasync` time was 0.378 ms, while its p50/p95/p99 were
0.311/0.696/0.984 ms. These counters demonstrate some write/sync overlap but
do not identify the cause of the unprofiled 200,000-put run-to-run variance.
The parallel path remains opt-in; further ext4 work needs paired end-to-end
results on the original case and a stable p99 bound, with tmpfs retained as a
regression check.

### Ext4 sync-latency cliff and writer concurrency

**Run date:** 2026-09-27. A second profiled run of the 200,000-put,
four-writer parallel case reproduced the slow state: 2,294 puts/s and
25.459-ms sampled p99, against 5,770 puts/s and 2.016-ms p99 in the earlier
profiled run. The slow run's `fdatasync` mean/p99 rose to 1.109/19.756 ms
from 0.378/0.984 ms; 1,533 sync calls exceeded 10 ms, versus five in the
fast run. Of those long syncs, 1,491 occurred in the eighth and ninth tenths
of the slow run's sync sequence. A subsequent profiled leader run reached
3,569 puts/s, 2.705-ms sampled p99, and 0.802-ms sync p99, with six syncs
over 10 ms. This localizes the parallel slowdown to sync latency during a
late-run interval, but does not establish whether parallel write pressure or
external device activity caused it. Profiles are in
`/tmp/rfc024-slow-profile-ext4-1.json` and
`/tmp/rfc024-leader-profile-ext4.json`.

The same-source benchmark build with `bench` enabled was also run without
`--profile` for three alternating 50,000-put ext4 pairs at eight writers.
Parallel/leader throughput ratios were 1.630, 1.638, and 1.608; sampled p99
ratios were 0.723, 0.661, and 0.851. Parallel reached 10,235-10,417 puts/s
versus 6,325-6,389 for leader. This confirms that the pipeline can use more
writer concurrency on this filesystem, but the eight-writer, shorter run
does not resolve the four-writer, 200,000-put latency cliff. Raw runs are in
`/tmp/rfc024-8writer-ext4-*`.

### Device counters and buffered-I/O prototype

**Run date:** 2026-09-27. A monitored 200,000-put ext4 pair showed that the
sync cliff can also hit leader WAL: leader ran at 2,243 puts/s with 11.726-ms
sampled p99 and 7.779-ms `fdatasync` p99, while the following parallel run
reached 5,775 puts/s with 2.126-ms sampled p99 and 0.995-ms sync p99. The
whole-device `/sys/block/nvme0n1/stat` samples showed average write-I/O
latency rising from 0.179 to 0.768 ms across the first three quarters of
the slow leader run; it stayed between 0.162 and 0.190 ms across the fast
parallel run. These are device-wide counters, so they do not attribute the
latency to this benchmark. They do show that a slow run is not unique to the
parallel implementation. Raw benchmark and 250-ms device samples are in
`/tmp/rfc024-device-counter-*`.

A benchmark-only prototype opened the parallel worker's WAL handle without
`O_DIRECT`, retaining io_uring writes and `fdatasync`. Five alternating
50,000-put ext4 buffered/direct pairs had throughput ratios 1.040, 1.016,
1.037, 2.091, and 2.344; the last two direct runs hit large storage-latency
outliers. The first three pairs suggest only a 1.6-4.0% gain. Five 200,000-put
tmpfs pairs had a median throughput ratio of 0.998 and mixed sampled p99.
On the original 200,000-put ext4 workload, three buffered/direct throughput
ratios were 1.034, 0.508, and 0.517. Buffered I/O hit the late-run latency
cliff in the latter two pairs (11-13-ms sampled p99), while direct I/O did
not. Buffered I/O therefore does not solve the adoption case; the prototype
was reverted. Raw comparisons are in `/tmp/rfc024-buffered-ext4-*`,
`/tmp/rfc024-buffered-tmpfs-*`, and `/tmp/rfc024-buffered-200k-ext4-*`.

### In-order completion fast path (rejected)

**Run date:** 2026-09-27.

A prototype advanced the written frontier directly when a successful group
completed at the current frontier, avoiding a `BTreeMap` insertion and removal.
It kept the map for out-of-order CQEs and the existing poison boundary.
Five pairs of the exact 200,000-put tmpfs workload had candidate/control
throughput ratios `1.010, 1.078, 0.973, 1.024, 1.008` (median `1.010`). Five
20,000-put ext4 pairs had ratios `0.978, 0.980, 1.004, 1.020, 1.009` (median
`1.004`), with mixed p99. The added retirement branch did not deliver a
repeatable throughput gain, so it was reverted. Raw results are in
`/tmp/rfc024-direct-retire-tmpfs.json` and `/tmp/rfc024-direct-retire-ext4.json`.

### Conditional idle eventfd drain (rejected)

**Run date:** 2026-09-27.

The idle worker's channel receive is followed by a nonblocking eventfd read.
A prototype skipped that read when no eventfd signal was pending, retaining
the drain for a signal racing with the ring-progress wait. In ten paired
normal-release runs of the exact tmpfs workload, the candidate/control median
throughput ratio was `1.014`; six pairs favored the candidate, and median
process CPU was unchanged. Ten ext4 pairs had a `1.015` median throughput
ratio, but both arms saw large storage-latency outliers. The effect is too
small and inconsistent to justify changing wakeup-race handling, so the code
was reverted. Raw results are in `/tmp/rfc024-conditional-eventfd-tmpfs.json`,
`/tmp/rfc024-conditional-eventfd-tmpfs-repeat.json`,
`/tmp/rfc024-conditional-eventfd-ext4.json`, and
`/tmp/rfc024-conditional-eventfd-ext4-repeat.json`.

### Candidate CPU attribution on the exact case (measurement)

**Run date:** 2026-09-27.

Two alternating pairs of the exact tmpfs regression case (200,000 puts, four
writers, 1 KiB values, 1 MiB SST target, WAL enabled, PITR disabled, normal
release build) reproduced the normal-release row above: parallel/leader
throughput was `99,112 / 161,684` and `110,001 / 183,389` ops/s, ratios `0.613`
and `0.600`. Process CPU in the first pair was 2,649 ms (1,178 user / 1,471
system) for the leader against 5,280 ms (2,537 / 2,743) for the candidate, or
3.25x CPU per completed operation (16.4 us against 53.3 us per put); `perf
stat` task-clock was 3,498 ms against 6,005 ms. The candidate is not
CPU-starved but it spends far more CPU per commit than the leader for fewer
commits.

Per-thread CPU was sampled from `/proc/<pid>/task/*/stat` every 50 ms, keeping
the largest `utime + stime` seen for each thread. This host's `perf stat
--per-thread` cannot be combined with a command, and `perf record` collected
only startup samples for these runs, so the sampler is the attribution used
here. One-writer pairs of the same workload isolate pipeline overhead because
no commit can coalesce with another:

| Thread | leader | parallel |
| --- | ---: | ---: |
| producing writer | 0.62 s | 1.12 s |
| engine runtime workers | 0.53 s | 1.17 s |
| `wal-io-worker` (three threads) | - | 0.02 s each |
| `wal-sync-coordinator` | - | below the reported set |

The candidate's dedicated WAL threads are cold: the I/O worker consumes about
0.02 s and the sync coordinator never reached the reported set, so the added
cost is on the producing writer thread and on the engine's runtime threads
rather than in the worker or coordinator loops. The single-writer pair lost
183,385 to 47,039 ops/s (`0.26`), with no second writer available to overlap
the round trip; that is the pipeline's per-commit latency showing through
unhidden. It also matches the exact case's 200,000 solo groups: the parallel
path pays a full handoff per commit instead of the leader's average of 2.1
commits per group.

The sampler is coarse: it polls at 50 ms and keeps per-thread maxima, so it
cannot see threads shorter than a poll interval and its totals are lower than
`perf stat`'s for the same runs. Leader throughput also moved between pairs
(161,684 to 183,389 ops/s), consistent with the null spread above, so each rate
in this section is a single pair and only the direction of the ratio repeated.

Reproduce with:

```text
perf stat -e task-clock,context-switches,instructions \
  ./target/release/write-perf --bench wal_concurrent --num 200000 --threads 4 \
  --value-size 1024 --wal --wal-io-mode {leader,parallel} \
  --target-sst-size 1048576 --output json --path <tmpfs path>
```

Artifacts from this session are under `/tmp/claude-perf/` and are transient;
the command above is the durable record.

### Idle-worker sync (rejected)

**Run date:** 2026-09-27.

The I/O worker was given the sync itself to run whenever it had nothing
staged, in flight, or queued, guarded by a one-sync-in-flight gate and skipped
once the worker's own poison boundary was set, which kept the poisoned path on
the coordinator. The intent was to drop the worker -> coordinator -> waiter
wakeup chain for batches that cannot overlap anything. All 1,232 library tests
passed.

Five paired tmpfs runs of the exact case gave parallel/leader ratios `0.573,
0.611, 0.645, 0.674, 0.699` (median `0.645`) against `0.613` and `0.600`
measured before the change, and five one-writer pairs gave `0.237, 0.246,
0.270, 0.290, 0.290` (median `0.270`) against `0.26`.

The one-writer case is the discriminating one, and it did not move. The inline
sync was gated on the absence of outstanding SQEs, but this experiment did
not report how often it actually took the inline path. A synchronous lone
writer necessarily completes its write before it can finish its durability
wait, so lack of an idle opportunity cannot be assumed. The four-writer range
straddles the pre-change pairs, and leader throughput drifted from 189k to
155k across the one-writer series, so the apparent gain is not separable from
drift. The code was reverted.

This prototype did not demonstrate a gain. Without path-activation counts,
it does not isolate the cost of the worker-to-coordinator handoff or prove
that moving durability coordination cannot help. The one-writer result still
shows substantial pipeline latency that needs a measured explanation.

## Measured bottleneck and where the work goes next

The candidate is a three-thread relay: a producer admits and packs, the I/O
worker submits and reaps, and the sync coordinator syncs and wakes. The leader
path does all three inline on the calling thread. On tmpfs a sync is nearly
free, so there is little for the overlap to win back, and what remains is the
relay's own per-commit cost:

- The candidate forms 200,000 solo I/O groups for 200,000 commits, against the
  leader's 93,978 groups at 2.1 commits each, so it pays a full pipeline round
  trip per commit rather than per group.
- One writer, which can coalesce nothing, runs at `0.27` of the leader
  (183,385 against 47,039 ops/s; `0.237`-`0.290` across five pairs). That is
  the round trip with nothing hiding it.
- The dedicated WAL threads are cold: `wal-io-worker` consumes about 0.02 s
  and the sync coordinator stays below the reporting floor, while the added
  cost sits on the producing writer thread and on the engine's runtime
  threads.
- The overlap the RFC hypothesised is real and measured - four groups in
  flight, writes completing during `fdatasync` - but it does not convert into
  throughput on a filesystem whose syncs are cheap.

The default leader path has the same shape of limit. Three runs per writer
count of the exact tmpfs case with the leader selected gave medians of
`163,713` at one writer, `153,569` at two, `155,549` at four, and `170,451` at
eight ops/s (one eight-writer run reached `204,425`). Four writers pushing at
the leader buy no throughput over one, which is what a serialized per-group
barrier predicts: the leader waits for its group's writes and `fdatasync`
before the next group submits, so the run's 1.24 s across 93,978 groups - about
13 us of wall per group - bounds it rather than the writers do. The
WAL-isolated matrix above reports the opposite shape (`115,736` at one writer
rising to `181,751` at four), but each of those cells is a single observation,
and its one-writer rate sits below the three-run medians here.

If the group loop is the ceiling, the only lever that raises it is more
commits per group - `2.13` today - rather than a faster sync, which on tmpfs
costs about `0.19` us per call (`17.749` ms over `93,978` calls). That is
consistent with the PITR encode work, where a cheaper batch encode raised
throughput by letting commit groups form larger rather than by saving CPU.

Every attempt to remove or relocate a pipeline handoff was rejected by
measurement: a dedicated packer thread (`0.800`), completion-triggered packing
(`0.828`), worker-owned packing (`0.970` on this case against `1.439` on the
WAL-isolated one), a two-group cap, SQPOLL, and the idle-worker sync above.
The changes that were kept - inline storage for single-write groups,
coalesced worker notifications, the shared completion channel, and
producer-side queue draining - cut per-thread cost without changing the
relay's shape, and they did not close the gap either. Pipeline savings appear
on the WAL-isolated workload and disappear on the exact case, which
interleaves flush and compaction with commits; that reading of the pattern is
an inference from the paired results, not a separately isolated test.

The current implementation has not met the exact tmpfs gate. Cheap syncs
leave little I/O latency to hide, but the rejected prototypes do not prove
that a redesigned parallel pipeline cannot meet it. The candidate's win is
on device-backed storage, where
it clears the gate on the WAL-isolated case (`12.9` median paired gain,
interval `5.3`-`17.9`).

At this stage, `wal_concurrent`'s post-PITR gap remained unaddressed by the
parallel path. Its measured cost was on the default leader
path, where commits are bound by the serialized submit: the elected leader
waits for its group's writes and `fdatasync` before the next group can submit,
which is why solo commit groups rose from 10,859 to 26,735 after the PITR
sequencer landed. Further optimization should target the parallel path and
measure it against an unchanged control; this observation does not justify
redirecting that work into leader-path optimization.

### Same-session pre-PITR ext4 comparison

**Run date:** 2026-09-27. The pre-PITR `2f556ccb` release binary and current
leader/parallel release binary ran the exact 200,000-put `wal_concurrent` case
on ext4: four writers, 1 KiB values, 1 MiB SST target, WAL on, PITR off,
latency sampled every 100 operations. Three triplets rotated execution order.
The historical binary lacks the current latency fields. `/sys/block/nvme0n1/stat`
write-time divided by completed device writes is a whole-device interval
average, not a per-WAL or per-request latency measurement.

| Triplet (run order) | pre-PITR ops/s (device ms/write) | leader ops/s (device ms/write) | parallel ops/s (device ms/write) |
| --- | ---: | ---: | ---: |
| 1 (pre-PITR, leader, parallel) | 3,886 (0.190) | 3,641 (0.182) | 2,174 (0.680) |
| 2 (leader, parallel, pre-PITR) | 2,393 (0.367) | 3,620 (0.181) | 5,989 (0.179) |
| 3 (parallel, pre-PITR, leader) | 3,840 (0.189) | 2,225 (0.336) | 5,984 (0.178) |

Each mode had two runs in a fast device interval (0.178-0.190 ms/write) and
one in a slow interval (0.336-0.680 ms/write). Among the fast runs, the
per-mode medians were 3,863 ops/s for pre-PITR, 3,630 for the leader, and
5,986 for parallel. Parallel was 1.55x the historical fast-run median and
1.65x the current leader fast-run median; the current leader was about 6%
below the historical median. These are **post-hoc device-state comparisons**,
not paired adoption estimates. The third triplet put both parallel and
pre-PITR in the fast interval and measured 5,984 versus 3,840 ops/s; the
other two triplets put one of those modes in a slow interval. Device write
counts were similar across runs (294k-316k), but this counter cannot identify
which I/O caused the latency change.

This session is consistent with the parallel path closing the apparent ext4
pre-PITR gap under fast device conditions. It does not establish a stable
same-session paired gain beyond the null spread: the device changed regimes
between adjacent arms, and the historical binary supplies no comparable p99.
Keep the leader default while the RFC's full throughput, one-writer, and p99
gates remain unresolved. Raw records, including complete benchmark JSON, are
in `/tmp/rfc024-pre-pitr-triplets.jsonl` (transient).

### Bounded durability wait (rejected)

**Run date:** 2026-09-27. To reduce the one-writer producer-to-coordinator
round trip, a prototype mirrored the durable frontier in an atomic and spun
for a bounded number of iterations before falling back to the existing
condition variable. The atomic was published after the same `fdatasync`
success as the locked frontier; the poison/error path remained locked. Each
configuration was compared against the unchanged release binary in five
alternating pairs per case. The cases used 1 KiB values and a 1 GiB SST target:
50,000 puts on tmpfs with one writer, 5,000 puts on ext4 with one writer, and
20,000 puts on ext4 with four writers.

| Spin iterations | Case | Median throughput ratio | Median p99 ratio | Median process CPU ratio |
| ---: | --- | ---: | ---: | ---: |
| 4,096 | tmpfs, one writer | 1.058 | 0.792 | 1.249 |
| 4,096 | ext4, one writer | 1.084 | 0.937 | 3.159 |
| 4,096 | ext4, four writers | 1.089 | 0.985 | 5.062 |
| 256 | tmpfs, one writer | 1.044 | 0.951 | 1.231 |
| 256 | ext4, one writer | 0.991 | 0.988 | 1.378 |
| 256 | ext4, four writers | 0.988 | 0.997 | 1.807 |

The long spin improved throughput, but its ext4 CPU cost was 3-5 times the
control, which is not an acceptable way to clear the gate. The short spin
kept a small tmpfs improvement but lost the ext4 gain while still using more
CPU. Two short-spin ext4 four-writer pairs crossed the device-latency cliff;
in the three fast-device pairs, the short-spin arm was also slightly slower.
Both prototypes were reverted. Even the long-spin tmpfs gain leaves the
one-writer parallel path far below the leader because each solo commit still
crosses the worker and coordinator. Raw paired records are in
`/tmp/rfc024-durable-spin-bench.jsonl` and
`/tmp/rfc024-durable-spin-256-bench.jsonl` (transient).

### Rotation-bound adaptive selector (rejected)

**Run date:** 2026-09-27. A prototype started each v4 WAL on the leader path,
recorded overlapping WAL-ticket waiters, and chose leader or parallel I/O for
the successor only at memtable/WAL rotation. Five alternating 50,000-put
`wal_concurrent` pairs used four writers, 1 KiB values, a 1 MiB SST target,
WAL on, and PITR off. Adaptive/leader median throughput was **1.665** on ext4
but only **0.787** on tmpfs; median p99 ratios were 0.613 and 1.054,
respectively. The first ext4 leader run crossed a slow whole-device interval
(0.788 ms/write), inflating its pair ratio to 5.850. The other four ext4
pairs were 1.638-1.684, though their device intervals were not perfectly
matched. Adaptive reached four in-flight groups in every four-writer run.

Separate five-pair one-writer runs with a 1 GiB SST target had median
adaptive/leader throughput ratios of 0.971 on tmpfs and 1.003 on ext4. Five
ext4 four-writer pairs against fixed parallel measured a median ratio of
0.983. A non-rotating WAL cannot change paths, so the selector does not help
the WAL-isolated concurrent workload. The prototype also exposed an
experimental mode in production builds. It was removed; leader remains the
normal path, and these results do not clear the RFC adoption gate. Raw
records are in `/tmp/rfc024-adaptive-bench.jsonl` and
`/tmp/rfc024-adaptive-leader-bench.jsonl` (transient).

### Yield before leader election after a slow sync (rejected)

**Run date:** 2026-09-27. A benchmark-only probe remembered whether the
preceding leader `fdatasync` took at least 100 microseconds. If so, a caller
with the sole pending v4 ticket yielded once **before** trying to become the
submit leader, allowing another writer to admit a ticket first. This moved
the delay outside the leader's exclusive `submitting` window. Five alternating
pairs compared it with the unchanged release build for each case; all used
1 KiB values and PITR off.

| Case | Median candidate/baseline throughput | Median p99 ratio | Median commit-group ratio |
| --- | ---: | ---: | ---: |
| ext4, four writers, 20k puts, 1 MiB SST | 1.009 | 0.973 | 1.008 |
| ext4, one writer, 5k puts, 1 GiB SST | 0.999 | 0.998 | 1.000 |
| tmpfs, four writers, 50k puts, 1 MiB SST | 1.085 | 1.282 | 1.012 |
| tmpfs, one writer, 20k puts, 1 GiB SST | 1.035 | 0.948 | 1.000 |

The three ext4 four-writer pairs where both device intervals were near
0.175-0.183 ms/write measured throughput ratios of 0.998, 1.009, and 0.995.
The other two pairs crossed the known device-latency cliff and cannot be
credited to the yield. The candidate also formed slightly *more* commit
groups in three of five ext4 four-writer pairs, so the intended coalescing
effect was absent. Tmpfs four-writer p99 worsened while throughput ratios
ranged from 0.814 to 1.230. The probe was removed; no production code or
public selector was added. Raw paired records are in
`/tmp/rfc024-leader-yield-bench.jsonl` (transient).

### Parallel WAL debug-invariant lock probe (rejected)

**Run date:** 2026-09-27. The parallel packer and sync coordinator read the
admission state for ticket/offset `debug_assert!` checks. A probe moved those
reads inside the assertions so release builds need not acquire an admission
mutex only to check a debug invariant. Five alternating pairs compared the
unchanged parallel WAL with that source change. All cases used 1 KiB values,
PITR off, and the same release profile and benchmark feature.

| Case | Median candidate/baseline throughput | Median p99 ratio | Median process CPU ratio |
| --- | ---: | ---: | ---: |
| tmpfs, four writers, 200k puts, 1 MiB SST | 0.984 | 0.986 | 1.027 |
| ext4, 16 writers, 50k puts, 1 GiB SST | 0.998 | 0.839 | 1.045 |
| ext4, four writers, 20k puts, 1 MiB SST | 0.988 | 1.032 | 1.063 |
| ext4, one writer, 5k puts, 1 GiB SST | 0.990 | 0.992 | 0.987 |

The tmpfs exact-case throughput ratios were 0.989, 0.985, 0.969, 0.984,
and 0.950. The ext4 16-writer ratios ranged from 0.971 to 1.028. Two ext4
four-writer pairs crossed the known device-latency cliff, so they cannot
establish a code effect. No workload showed a repeatable throughput gain;
the change was removed. This does not prove the source-level locks were on
the critical path: the compiler may already eliminate them in release code,
or their cost may be too small to measure against the pipeline relay. Raw
paired records are in `/tmp/rfc024-parallel-debug-lock-bench.jsonl`
(transient).

### Worker-drained admission queue (rejected)

**Run date:** 2026-09-28. The MVCC write-order lock currently spans WAL
admission and producer-side packing. A prototype shortened that critical
section to ticket assignment and enqueue, then let the existing parallel WAL
worker drain the admission queue directly. It removed per-group channel sends
and producer-side packer locking; the sync coordinator remained unchanged.
The final variant swapped queued batches into a reusable worker-local queue
under the admission mutex and packed/preallocated outside that mutex. An idle
worker used park/unpark; outstanding I/O retained the existing eventfd wakeup.
No leader-path changes or public mode selector were involved.

Three alternating baseline/candidate pairs per case used release builds with
`bench`, parallel mode in both arms, 1 KiB values, PITR off, and latency sampling
every 100 operations. Builds and tests completed before timing began.

| Case | Median paired throughput ratio | Median paired p99 ratio |
| --- | ---: | ---: |
| ext4, one writer, 5k puts, 1 GiB SST | 1.020 | 1.663 |
| ext4, four writers, 20k puts, 1 MiB SST | 1.003 | 1.108 |
| ext4, 16 writers, 50k puts, 1 GiB SST | 0.949 | 1.052 |
| tmpfs, one writer, 50k puts, 1 GiB SST | 0.979 | 0.853 |
| tmpfs, four writers, 200k puts, 1 MiB SST | 0.876 | 1.172 |

The ext4 four-writer intervals stayed near 0.154–0.164 ms/device write.
Group count fell from 20,000 to about 12,000, but sync count stayed near 5,040
and throughput ratios were 1.003, 1.002, and 1.017. Coalescing descriptors
therefore did not materially reduce the durability barriers in this workload.
The 16-writer pairs crossed device-latency regimes (0.126–2.776 ms/write),
with throughput ratios 0.904, 0.949, and 1.260; they cannot establish a code
benefit. The single-writer ext4 result also included a device-latency swing
and only 50 latency samples per run, so its p99 is not a reliable gate result.

The tmpfs four-writer throughput ratios were 0.915, 0.876, and 0.847. Although
the prototype reduced both groups and syncs, all three pairs regressed. Two
earlier variants (eventfd idle waiting and park/unpark with packing under the
queue lock) also regressed on that workload. These screens reject this
implementation, not the general possibility of improving admission or batching.

All 46 focused parallel WAL tests passed before the final screen, including
new prototype tests for shutdown draining more than one in-flight window and
notification between the idle check and park. The prototype was removed and
the baseline source restored. Transient reproduction artifacts are
`/tmp/rfc024_ingress_swap_bench.py`, `/tmp/rfc024-ingress-swap-bench.jsonl`, and
`/tmp/rfc024-ingress-swap-rejected.patch`; earlier variants are recorded in
`/tmp/rfc024-ingress-screen-bench.jsonl` and
`/tmp/rfc024-ingress-park-bench.jsonl`.

### Allocate only the growing WAL suffix (2026-09-28)

The parallel preallocator called `fallocate(fd, 0, 0, end)` on every 1 MiB
extension. It now uses the existing file length as the offset and `end - len`
as the length. This avoids revisiting the already allocated prefix. The cap,
file-size extension, unsupported-filesystem fallback, and poison handling are
unchanged. The leader path was not modified.

Five alternating pairs compared the parallel candidate with `75dd5f8a`, using
release builds with `bench`, 1 KiB values, PITR off, and latency sampled every
100 operations. No compilation or tests ran during measurement. Tmpfs used
`/dev/shm`; ext4 used disposable directories under `target`.

| Case | Puts / writers / SST target | Median paired throughput ratio | Median paired p99 ratio |
| --- | --- | ---: | ---: |
| ext4, one writer | 5k / 1 / 1 GiB | 1.002 | 1.003 |
| ext4, rotation | 20k / 4 / 1 MiB | 1.000 | 0.940 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 1.005 | 1.034 |
| tmpfs, growing WAL | 50k / 1 / 1 GiB | 1.192 | 0.915 |
| tmpfs, original case | 200k / 4 / 1 MiB | 1.017 | 0.904 |

The tmpfs growing-WAL ratios were 1.259, 1.185, 1.192, 1.332, and 1.180.
Median preallocation time fell from 189.9 ms to 29.1 ms (84.7% less).
The arms wrote the same 204.8 MB and ended with the same 205,520,896-byte file
length and allocated space. Median throughput was 63,922 versus 79,684 puts/s;
the ratio of these separate medians differs from the paired ratio above.

Ext4 16-writer preallocation time fell from 42.2 ms to 27.0 ms, but throughput
was effectively unchanged: 21,498 versus 21,677 puts/s. Device intervals stayed
between 0.115 and 0.131 ms/write. Four-writer ext4 throughput also stayed near
6,230 puts/s. This is a measurable allocation-cost reduction, not evidence
that the ext4 adoption gate has been met. Short-run p99 estimates, particularly
the 50 samples per single-writer ext4 run, are only a regression screen.

The existing preallocation test now checks repeated extension, preservation
of header and batch bytes, and that a smaller target does not shrink the file.
Raw records and the reproduction script are available transiently at
`/tmp/rfc024-suffix-bench.jsonl` and `/tmp/rfc024_suffix_bench.py`.

### Bounded geometric preallocation growth (rejected, 2026-09-28)

After suffix-only allocation landed in `80fa66d2`, a separate prototype grew
allocation increments geometrically from 1 MiB to a maximum of 8 MiB. The
hypothesis was that fewer allocation calls and file-size extensions would
reduce ext4 stalls. Both arms used parallel WAL, the same release/`bench`
profile, 1 KiB values, and latency sampling every 100 operations. No build or
test ran alongside timing.

An initial three-pair screen showed a 1.025 throughput ratio at 16 writers on
ext4, but regressions on tmpfs. Five additional alternating pairs checked those
cases independently:

| Case | Puts / writers / SST target | Median paired throughput ratio | Median paired p99 ratio |
| --- | --- | ---: | ---: |
| ext4, growing WAL | 50k / 16 / 1 GiB | 0.995 | 1.172 |
| tmpfs, growing WAL | 50k / 1 / 1 GiB | 0.920 | 1.009 |
| tmpfs, original case | 200k / 4 / 1 MiB | 0.987 | 1.102 |

Ext4 preallocation time fell from 27.2 ms to 4.6 ms, but paired throughput
ratios were 1.042, 0.983, 1.021, 0.995, and 0.992. The syscall-time saving did
not produce a repeatable end-to-end gain. Tmpfs single-writer preallocation
time increased from 29.0 ms to 32.5 ms. On the growing-WAL workload the
candidate reserved 209,715,200 bytes versus 205,520,896 bytes in the baseline.
The earlier ext4 rotation screen was near parity (1.006 median ratio), while
its single-writer case encountered the known device-latency cliff and cannot
isolate the code effect.

The larger growth increments were removed; suffix-only allocation remains.
The restored release binary was compared with the saved baseline. Transient
artifacts are `/tmp/rfc024-growth-bench.jsonl`,
`/tmp/rfc024-growth-confirm-bench.jsonl`,
`/tmp/rfc024_growth_confirm_bench.py`, and
`/tmp/rfc024-growth-rejected.patch`.

### Cooperative completion task work (2026-09-28)

The parallel worker owns ring creation, submission, and CQE consumption on one
thread. Its ring setup now requests `IORING_SETUP_COOP_TASKRUN` and
`IORING_SETUP_SINGLE_ISSUER`. Cooperative task work avoids forceful userspace
interruption for completions; work can run at kernel transitions in the existing
submit/poll loop. This does not enable deferred task work or SQPOLL. If setup
returns `EINVAL`, initialization retries the previous setup for older kernels.
Other setup errors remain errors. The benchmark host accepted both flags
(`0x1100`) in an independent setup probe.

Five alternating pairs per case compared the candidate with `51446a53`, in
parallel mode, release builds with `bench`, 1 KiB values, and PITR off. No
build or test ran during timing. Initial latency sampling was every 100 ops.

| Case | Puts / writers / SST target | Median paired throughput ratio | Median paired CPU-time ratio | Median paired p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, one writer | 5k / 1 / 1 GiB | 1.000 | 0.997 | 0.999 |
| ext4, rotation | 20k / 4 / 1 MiB | 0.994 | 1.007 | 1.011 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 1.002 | 0.990 | 1.180 |
| tmpfs, growing WAL | 50k / 1 / 1 GiB | 0.992 | 1.014 | 0.983 |
| tmpfs, original case | 200k / 4 / 1 MiB | 1.094 | 0.930 | 0.953 |

Ext4 encountered the known device-latency cliff in the fifth 16-writer pair
and three rotation pairs. The first four 16-writer pairs had throughput
ratios 1.002, 1.035, 1.018, and 0.997. The stalled pair was 0.520, with whole-device
write latency averaging 2.574 versus 1.771 ms; this is not an isolated code
measurement. These results do not establish an ext4 gain.

A second series used five new pairs per case and sampled every 10 ops:

| Confirmation case | Median paired throughput ratio | Median paired CPU-time ratio | Median paired p99 ratio |
| --- | ---: | ---: | ---: |
| ext4, 16 writers | 0.996 | 0.993 | 1.021 |
| tmpfs, original four-writer case | 1.029 | 0.964 | 0.973 |

The tmpfs confirmation throughput ratios were 1.029, 0.990, 0.990, 1.213,
and 1.049. Median sync calls fell from 111,916 to 104,731. Both series favor
the candidate on this workload, but the magnitude varies and two confirmation
pairs were slightly slower. Keep this as a modest tmpfs throughput/CPU
improvement, not a claim of a universal 9.4% gain or passage of the ext4
adoption gate. The denser ext4 series remained near parity.

All 44 focused parallel WAL tests passed before timing. After measurement,
`cargo make check` passed, including both Clippy configurations and all 1,403
tests. Raw measurements and scripts are transiently available as `/tmp/rfc024-coop-bench.jsonl`,
`/tmp/rfc024-coop-confirm-bench.jsonl`, `/tmp/rfc024_coop_bench.py`, and
`/tmp/rfc024_coop_confirm_bench.py`.

### Deferred task work with a single issuer (rejected, 2026-09-28)

At the user's request, a prototype replaced cooperative task work with
`SINGLE_ISSUER | DEFER_TASKRUN`, adding `TASKRUN_FLAG` for pending-work
notification. The host accepted `0x3200` in an independent setup probe.
The [io_uring setup contract](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
requires explicit `GETEVENTS` calls to drive deferred completions. The pinned
io-uring 0.6.4 crate's `submit_and_wait(0)` does not normally set that flag.

An initial implementation processed flagged task work with a nonblocking
`io_uring_enter(0, 0, GETEVENTS)` but retained the external ring/eventfd `poll`.
Some tests then progressed only after the one-second poll timeout. Making the
nonblocking call unconditional did not fix it: a range-tombstone rotation
test took 29.1 seconds. These stalled versions were not benchmarked.

The measured version instead registered eventfd readiness through a ring
`PollAdd` request and waited with `GETEVENTS` for either a write CQE or a
producer wakeup, retaining a one-second timeout only as a signaling-failure
fallback. Control CQEs were removed before write-completion accounting.
Nonblocking `GETEVENTS` also processed flagged work before CQE consumption.
This preserved admission wakeups while waiting and avoided per-group write
waits. All 44 focused tests passed in 0.054 seconds; the targeted rotation
test returned to about 0.04 seconds.

Five alternating pairs compared this driver with the cooperative baseline at
`72336a65`. Both used parallel mode, release/`bench`, 1 KiB values, PITR off,
and latency sampling every 10 ops. Builds and tests finished before timing.

| Case | Puts / writers / SST target | Median paired throughput ratio | Median paired CPU-time ratio | Median paired p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, one writer | 5k / 1 / 1 GiB | 0.995 | 0.973 | 0.991 |
| ext4, rotation | 20k / 4 / 1 MiB | 0.999 | 1.007 | 1.051 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 1.003 | 0.995 | 0.997 |
| tmpfs, growing WAL | 50k / 1 / 1 GiB | 1.071 | 0.943 | 1.227 |
| tmpfs, original case | 200k / 4 / 1 MiB | 0.980 | 1.013 | 1.075 |

Several ext4 rotation pairs crossed the known device-latency cliff, so their
large individual swings cannot establish a code effect. The ext4 16-writer
pairs showed no material throughput gain. Tmpfs single-writer throughput and
CPU time improved, but p99 worsened. The four-writer case used a median
112,633 sync calls versus 102,912 with cooperative task work, and its paired
throughput ratios were 0.980, 0.933, 0.937, 1.065, and 1.084. That tradeoff
and the additional completion-driver complexity do not justify replacing the
current implementation. This rejects this integration, not deferred task work
in general.

The prototype was saved at `/tmp/rfc024-defer-rejected.patch` and removed from
production source. Raw records and reproduction commands are in
`/tmp/rfc024-defer-bench.jsonl` and `/tmp/rfc024_defer_bench.py` (transient).
The restored release binary was compared with the saved cooperative baseline.

## Lazy parallel buffer pool experiment (rejected)

At `c0cfcfa3`, each parallel WAL eagerly allocates 64 aligned 256 KiB buffers
when it starts. The candidate removed this prefill and let the existing
budgeted allocation path populate the same bounded pool on demand. Buffers
were still recycled after write completion; admission, synchronization, and
the leader path were unchanged. This avoids allocating 16 MiB of buffer
capacity up front, but does not imply saving 16 MiB of resident memory:
`posix_memalign` does not initialize all those pages.

All 44 focused WAL tests passed before timing. Five alternating pairs used
parallel mode in both arms, release/`bench`, 1 KiB values, PITR off, and
latency sampling every 10 operations. No builds or tests ran during timing.

| Case | Puts / writers / SST target | Median paired throughput ratio | Median paired CPU-time ratio | Median paired p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, one writer | 5k / 1 / 1 GiB | 1.002 | 0.997 | 1.010 |
| ext4, rotation | 20k / 4 / 1 MiB | 1.004 | 0.996 | 1.085 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 1.011 | 0.957 | 1.006 |
| tmpfs, growing WAL | 50k / 1 / 1 GiB | 1.018 | 0.979 | 0.694 |
| tmpfs, original case | 200k / 4 / 1 MiB | 1.024 | 1.013 | 0.974 |

The targeted ext4 rotation case was effectively flat in throughput and had
higher sampled p99. Tmpfs four-writer throughput ratios ranged from 0.954
to 1.058; the small median gain is not compelling evidence of improvement.
These runs do not establish eager buffer allocation as the throughput
bottleneck or justify retaining the candidate as a performance optimization.
The production change was reverted.

Transient reproduction artifacts are `/tmp/rfc024_lazy_pool_bench.py`,
`/tmp/rfc024-lazy-pool-bench.jsonl`, and
`/tmp/rfc024-lazy-pool-rejected.patch`.

## Initializing allocated extents (rejected implementation, 2026-09-28)

An experiment at `39dd63cd` wrote aligned zeros to each newly allocated suffix
after `fallocate`, before submitting WAL batches into that suffix. Existing
bytes were never overwritten. The hypothesis was that converting unwritten
extents ahead of the commit writes could reduce their filesystem overhead.
Ext4's [direct-I/O completion path converts unwritten extents](https://kernel.googlesource.com/pub/scm/linux/kernel/git/mripard/linux/+/5a838c3b60e3a36ade764cf7751b8f17d7c9c2da/fs/ext4/inode.c);
these benchmarks test the optimization's effect, not attribution through a
kernel trace. The ordinary sync coordinator still acknowledged only its
captured written prefix. No leader-path or public mode-selection change was made.

The original 1 MiB extent variant passed all 44 focused WAL tests. Two further
screens changed only the extent increment, to 128 KiB and 8 MiB. Each cell
below uses five alternating baseline/candidate pairs, both in parallel mode,
release/`bench`, 1 KiB values, PITR off, and latency sampling every 10 ops.
Compilation and testing did not overlap benchmark timing. Ratios are medians
of paired ratios, not ratios of arm medians.

| Extent | ext4 workload (puts / writers / SST target) | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| 1 MiB | 5k / 1 / 1 GiB | 1.726 | 0.531 |
| 1 MiB | 20k / 4 / 1 MiB | 1.582 | 1.106 |
| 1 MiB | 50k / 16 / 1 GiB | 1.090 | 1.761 |
| 128 KiB | 5k / 1 / 1 GiB | 1.661 | 0.578 |
| 128 KiB | 20k / 4 / 1 MiB | 1.427 | 0.894 |
| 128 KiB | 50k / 16 / 1 GiB | 0.849 | 1.400 |
| 8 MiB | 5k / 1 / 1 GiB | 1.751 | 0.528 |
| 8 MiB | 20k / 4 / 1 MiB | 1.552 | 1.105 |
| 8 MiB | 50k / 16 / 1 GiB | 1.380 | 1.959 |

The 1 MiB single-writer median rose from 1,738 to 3,011 puts/s. Aggregate
`fdatasync` time fell from 1.640 to 1.250 seconds across 5,000 syncs, while
preallocation (now including initialization) rose from 0.7 to 26.7 ms.
This is a substantial throughput signal, unlike the earlier allocation-only
experiments. But synchronous initialization holds the packer during extra
writes. At 128 KiB, the 16-writer case spent 477 ms in preallocation versus
29 ms for baseline and needed 4,156 versus 3,252 syncs. Smaller initialization
writes reduced individual pauses but interrupted batching more frequently.

Several 1 MiB and 8 MiB 16-writer runs also crossed the known whole-device
latency cliff. All pairs remain in the table; their throughput ratios cannot
cleanly isolate a code effect. Even a normal-device 8 MiB pair was 1.193 in
throughput but 1.959 in p99. None of the tested sizes meets the latency guard
across these ext4 cases.

The all-filesystem 1 MiB prototype also regressed tmpfs: single-writer
throughput was 0.894 of baseline, and the original 200k/four-writer case was
0.882 with p99 at 1.618. Initializing storage that needs no extent conversion
adds work. The extra zero writes are counted in preallocation time, not the
WAL batch SQE/CQE counters; those counters alone understate total I/O here.

All production edits were reverted. A follow-up would need to prepare extents
away from the packer's critical path, with explicit initialization ownership,
failure handling, and shutdown ordering, and avoid this work on tmpfs. The
current results support investigating that design, not shipping the synchronous
prototype or claiming the adoption gate has passed.

Transient records: `/tmp/rfc024-init-extent-bench.jsonl`,
`/tmp/rfc024-init-128k-bench.jsonl`, `/tmp/rfc024-init-8m-bench.jsonl`.
Reproduction scripts use the corresponding `/tmp/rfc024_init_*_bench.py`
names. The three prototypes are saved as
`/tmp/rfc024-init-extent-{1m,128k,8m}-rejected.patch`.

## Asynchronous extent initialization (2026-09-29)

The parallel v4 path now prepares its next extent on a dedicated initializer
thread on ext-family filesystems. It retains the 1 MiB allocation increment,
but writes zeros in 128 KiB chunks using one reusable aligned buffer. A
bounded request/completion pair allows only one outstanding initialization
request. The packer consumes its completion before submitting WAL batches
into that range and requests one more extent while the current one is used.
Large batches can request preparation through their required end. All targets
remain bounded by the WAL file cap.

The initializer owns an `Arc<File>` and its buffer until synchronous writes
finish. Initialization never revisits the ready prefix. An initialization
error encountered by the packer uses the existing poison boundary; close also
reports errors from an unused lookahead request. Close drains admission and
packing, stops and joins the initializer, then finishes the existing worker
and sync-coordinator shutdown. Tests cover prefix preservation, initialization
failure, shutdown with an unread completion, and the file-cap boundary.

`fstatfs` selects ext2/3/4's shared filesystem magic; the measurements here are
on ext4 only. Other filesystems, or a failed filesystem query, retain allocation
without initialization. This avoids the measured tmpfs cost of zero-writing.
The optimization adds one thread and one 128 KiB buffer per open parallel WAL,
up to one extra prepared extent ahead of the packer's ready range, and extra
zero-write traffic. Empty WAL creation can also prepare the first extent.
`preallocation_ns` now measures readiness wait on this path, not background
initialization time. Batch SQE/CQE counters exclude the initializer's writes.

An initial all-filesystem prototype with 1 MiB initialization writes improved
ext4 16-writer throughput by 21.6%, but its median paired p99 ratio was 1.112
and tmpfs four-writer throughput was 0.764 of baseline. The retained variant
uses smaller writes and filesystem selection. Both arms below use parallel
mode; baseline is `e7a61188`. All runs use release/`bench`, 1 KiB values, PITR
off, and latency sampling every 10 operations. No builds or tests overlap
timing. Values are median paired ratios.

| Initial series | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, single writer | 5k / 1 / 1 GiB | 5 | 1.651 | 0.552 |
| ext4, rotation | 20k / 4 / 1 MiB | 5 | 1.534 | 0.889 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 5 | 1.200 | 1.086 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 5 | 0.970 | 1.055 |
| tmpfs, original case | 200k / 4 / 1 MiB | 5 | 0.907 | 1.038 |

| Independent confirmation | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, growing WAL | 50k / 16 / 1 GiB | 5 | 1.178 | 1.013 |
| ext4, original case | 200k / 4 / 1 MiB | 3 | 1.487 | 0.884 |
| tmpfs, original case | 200k / 4 / 1 MiB | 5 | 1.013 | 1.003 |

All five 16-writer confirmation throughput ratios were above parity:
1.157, 1.219, 1.148, 1.241, and 1.178. Their exact percentile bootstrap
95% interval for the paired median is 1.148–1.241. The corresponding p99
ratios were 0.940, 1.217, 0.984, 1.079, and 1.013: the median is within the
10% guard, but one pair is outside it. The initial tmpfs regression did not
repeat, so there is no claim of a tmpfs performance gain.

Device-latency spikes remain material. The long ext4 case's three throughput
ratios were 0.608, 1.623, and 1.487, with p99 ratios 5.174, 0.808, and 0.884.
The first candidate run had whole-device write latency of 0.582 ms versus
0.177 ms for baseline. A later baseline/candidate pair both had elevated
latency. All pairs are included; this does not establish a clean long-run
latency pass. The initial single-writer series also crossed the latency cliff.
These results justify retaining the optimization for the opt-in parallel path,
but do not constitute a new leader comparison or a complete adoption-gate pass.

Transient scripts and raw records are `/tmp/rfc024_ahead_bench.py`,
`/tmp/rfc024_ahead_chunk_bench.py`, `/tmp/rfc024_ahead_confirm_bench.py`, and
their `/tmp/rfc024-ahead{,-chunk,-confirm}-bench.jsonl` outputs.

Validation: `cargo make check` passed both Clippy configurations and all 1,407
tests; five tests retried after `/tmp` quota errors. A subsequent full nextest
run with `--retries 0` and a tmpfs temporary directory passed all 1,407 tests.
All 48 focused WAL tests then passed with an ext4 temporary directory and
two test threads, exercising filesystem-selected initialization. The final
code also validates initialization offsets/targets against alignment and the
file cap; those checks and the boundary test were added after benchmark timing.

## Coalescing after asynchronous extent preparation (2026-09-29)

With extent lookahead in `ec619c5e`, the 16-writer workload used roughly
3,700 syncs per 50,000 commits, versus 3,125 if every sync covered 16 commits.
The next experiment changed only `SYNC_COALESCE_WAIT` from 200 to 400
microseconds. The previous-sync-latency gate remains 100 microseconds; the
coordinator still returns immediately when the written prefix reaches the
current admission cutoff. It does not wait for future, unadmitted writers,
and moving admission cannot extend the deadline. A stalled admitted write
can now delay the next sync by up to 200 additional microseconds.

Five alternating pairs per case compared the candidate against `ec619c5e`,
with both arms in parallel mode, release/`bench`, 1 KiB values, PITR off,
and latency sampling every 10 ops. No compilation or tests overlapped timing.
Ratios below are medians of paired ratios.

| First series | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, single writer | 5k / 1 / 1 GiB | 0.993 | 0.919 |
| ext4, rotation | 20k / 4 / 1 MiB | 1.018 | 1.063 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 1.052 | 0.826 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 0.997 | 0.966 |
| tmpfs, original case | 200k / 4 / 1 MiB | 1.070 | 0.990 |

| Fresh confirmation, same workload sizes | Throughput ratio | p99 ratio |
| --- | ---: | ---: |
| ext4, rotation | 1.042 | 1.071 |
| ext4, growing WAL | 1.016 | 0.870 |
| tmpfs, original case | 1.015 | 0.953 |

The first five 16-writer throughput ratios were 1.060, 1.030, 1.051, 1.052,
and 1.069. Median sync count fell from 3,728 to 3,510; aggregate sync time
fell from 974 to 895 ms. Confirmation sync counts fell from 3,681 to 3,490.
The corresponding throughput ratios were 0.971, 1.084, 1.016, 1.034, and
0.457. The last candidate run crossed a whole-device latency spike
(0.690 ms/write versus about 0.081 for baseline), and the following baseline
rotation run was also slow. Every pair remains included. This is not evidence
of a repeatable 5.2% throughput gain: confirmation is substantially noisier.

Nine of ten 16-writer pairs had lower sampled p99, with median reductions of
17.4% and 13.0% in the two series. Retain the bounded-window change for fuller
sync batches and the repeated median tail-latency improvement. The four-writer
rotation p99 increased about 6–7%, so this is not a universal latency win.
There is no new full 200k-write ext4 run, leader comparison, or adoption-gate
claim in this experiment.

All 48 focused tests passed on ext4 before timing. After timing,
`cargo make check` passed both Clippy configurations and all 1,407 tests,
using a tmpfs temporary directory to avoid `/tmp` quota pressure.
Transient reproduction scripts are `/tmp/rfc024_window400_bench.py` and
`/tmp/rfc024_window400_confirm_bench.py`; raw records are
`/tmp/rfc024-window400-chunk-bench.jsonl` and
`/tmp/rfc024-window400-confirm-bench.jsonl`.

### Larger allocation increments with small initialization writes (rejected, 2026-09-29)

Compared against `055e7ea2`, this experiment rounded the extent initializer's
`fallocate` target up to 8 MiB instead of 1 MiB. Zero initialization still used
128 KiB writes and only advanced readiness/lookahead by 1 MiB. The hypothesis
was that fewer allocation and file-length updates would improve ext4 throughput
without the long zero-write bursts of the earlier synchronous experiment.

Five alternating pairs per case used parallel mode in both arms, release/`bench`,
1 KiB values, PITR off, and latency sampling every 10 operations. No compilation
or tests overlapped timing. Ratios are medians of candidate/baseline paired ratios.

| Workload | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, single writer | 5k / 1 / 1 GiB | 0.993 | 0.935 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 0.991 | 1.041 |
| ext4, rotation | 20k / 4 / 1 MiB | 1.026 | 0.855 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 1.056 | 0.886 |
| tmpfs, original case | 200k / 4 / 1 MiB | 0.995 | 1.104 |

The 16-writer throughput ratios were 1.057, 0.969, 1.039, 0.991, and 0.975;
there was no consistent throughput or latency gain. The last rotation candidate
crossed a device-latency spike: its throughput ratio was 0.351 and p99 ratio
4.186, with whole-device latency 0.616 ms/write versus baseline 0.089.
That pair remains included; the counter cannot establish the cause of the spike.
The tmpfs path does not use the changed initializer, so its variation is a
control observation rather than evidence of an allocation optimization.

Median reported allocated WAL bytes increased from 22,024,192 to 25,169,920
for the single-writer case, from 206,573,568 to 209,719,296 for 16 writers,
and from 10,493,952 to 16,777,216 for rotation. Packer readiness wait did not
improve consistently either: its 16-writer median rose from 0.863 to 1.079 ms,
and rotation rose from 14.017 to 14.222 ms. These timings exclude the background
initializer's own work.

Reject the change: the modest rotation median improvement does not justify
extra reserved space with no gain on the growing-WAL workloads. Restore the
1 MiB allocation increment. All 48 focused WAL tests passed on ext4 before
benchmarking; the restored source is identical to the previously checked baseline.
This experiment does not establish a leader comparison or pass an adoption gate.

Transient reproduction script: `/tmp/rfc024_reserve8m_bench.py`.
Raw records: `/tmp/rfc024-reserve8m-chunk-bench.jsonl`.
Rejected patch: `/tmp/rfc024-reserve8m-rejected.patch`.

### Larger background initialization writes (rejected, 2026-09-29)

Against `6329b202` (runtime unchanged from `055e7ea2`), increase the initializer's
zero buffer/write chunk from 128 to 256 KiB. Keep allocation and lookahead at
1 MiB. This halves the normal number of synchronous initialization writes per
extent without increasing initialized bytes. It also doubles the initializer's
buffer to 256 KiB per open parallel WAL. The hypothesis was lower initialization
I/O overhead with acceptable tail latency while batch writes and syncs proceed.

Five alternating pairs per workload used release/`bench`, parallel mode in both
arms, 1 KiB values, PITR off, and latency sampling every 10 operations. No tests
or compilation overlapped timing. Ratios are medians of paired candidate/baseline
ratios, not ratios of independently computed medians.

| Workload | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, single writer | 5k / 1 / 1 GiB | 1.049 | 0.950 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 0.988 | 0.989 |
| ext4, rotation | 20k / 4 / 1 MiB | 1.026 | 1.212 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 1.039 | 0.932 |
| tmpfs, original case | 200k / 4 / 1 MiB | 0.970 | 1.030 |

Single-writer ext4 runs crossed a slow-device interval in both arms. Their
throughput ratios were 1.003, 1.325, 2.378, 1.003, and 1.049; all remain included,
but this series does not demonstrate a stable single-writer gain. The 16-writer
ratios were 0.988, 1.026, 0.979, 1.081, and 0.972. Rotation p99 worsened in three
of five pairs, by 21%, 26%, and 36%. The initializer is bypassed on tmpfs, so its
variation is a control observation rather than an intended benefit or cost.

Median packer readiness wait decreased from 1.130 to 0.860 ms for 16 writers
and from 13.426 to 7.294 ms for rotation. These aggregate waits are small beside
sync time: 16-writer aggregate `fdatasync` time changed from 891.843 to 903.368 ms,
and rotation from 1,357.931 to 1,345.491 ms. Background initializer time is not
included in the readiness metric. Reducing readiness wait did not produce a
consistent end-to-end gain and came with worse rotation tails.

Reject and restore 128 KiB chunks. All 48 focused WAL tests passed on ext4 before
timing. The restored source matches the previously checked baseline. This is
not a leader comparison or an adoption-gate result.

Transient script: `/tmp/rfc024_init256_bench.py`.
Raw records: `/tmp/rfc024-init256-chunk-bench.jsonl`.
Rejected patch: `/tmp/rfc024-init256-rejected.patch`.

### Grace period for late admissions before sync (rejected, 2026-09-29)

An ext4 diagnostic run at `ba953229` used 50k puts, 16 writers, 1 KiB values,
a 1 GiB SST target, and `--profile`. Of 3,524 syncs, 2,883 covered 16 groups,
while 445 covered fewer than eight. Only five small syncs followed a sync
shorter than 100 microseconds. This suggested that changing the cheap-sync gate
would not remove most small batches. This diagnostic run was separate from
paired timing.

The prototype remembered the number of tickets acknowledged by the previous
successful sync. When the current written prefix caught up with admission but
covered fewer tickets than that previous batch, the coordinator waited up to
20 microseconds for another completion. It could repeat after progress, but
never extend the existing 400-microsecond coalescing deadline. Poison and
completion-channel disconnection still ended the wait; steady single-writer
traffic had no reason to enter the new grace period. The prior-sync latency
gate remained 100 microseconds. No durability target or acknowledgement rule
changed.

Five alternating pairs compared the prototype against `ba953229`, both in
parallel mode, release/`bench`, with 1 KiB values, PITR off, latency sampling
every 10 operations, and no detailed sync diagnostics during timing. No tests
or compilation overlapped the benchmark. Ratios are medians of paired ratios.

| Workload | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, single writer | 5k / 1 / 1 GiB | 1.007 | 1.045 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 1.005 | 1.005 |
| ext4, rotation | 20k / 4 / 1 MiB | 0.973 | 1.172 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 0.924 | 0.941 |
| tmpfs, original case | 200k / 4 / 1 MiB | 0.962 | 1.016 |

For 16 writers, median sync count decreased from 3,503 to 3,206 (8.5%), and
aggregate sync time from 916.774 to 847.576 ms (7.5%). Throughput improved only
0.5%, consistent with coalescing waits consuming most of the saved time rather
than yielding a useful end-to-end gain. This is an inference from the counters,
not a direct measurement of the new grace-period duration or scheduler delay.
Rotation sync count only decreased from 5,097 to 5,051. Its final three pairs
crossed slow-device intervals in both arms; all pairs remain included. Even
its first two pairs had worse p99 (ratios 1.172 and 1.516).

Reject the prototype and restore the original admitted-prefix coalescing rule.
Reducing sync count through additional timed waits is insufficient here. The
candidate passed all 48 focused WAL tests on ext4 before timing; restored source
matches the previously checked baseline. No adoption-gate claim is made.

Diagnostic record: `/tmp/rfc024-sync-profile-current.json`.
Transient script: `/tmp/rfc024_syncgrace_bench.py`.
Raw paired records: `/tmp/rfc024-syncgrace-chunk-bench.jsonl`.
Rejected patch: `/tmp/rfc024-syncgrace-rejected.patch`.

### One submission-queue handle per staged batch (rejected, 2026-09-29)

Against `6ca93f36`, the worker prototype held one mutable io_uring submission
queue handle while pushing all staged writes, instead of acquiring/dropping
one handle per SQE. The handle dropped before the existing submit call. This
removed repeated queue synchronization without introducing a timed wait,
changing write order, or altering buffer/CQE ownership.

Five alternating pairs per case used release/`bench`, parallel mode in both
arms, 1 KiB values, PITR off, and latency sampling every 10 operations. No
compilation or tests overlapped timing. Ratios are medians of paired ratios.

| Workload | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, single writer | 5k / 1 / 1 GiB | 1.007 | 0.988 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 0.977 | 1.037 |
| ext4, rotation | 20k / 4 / 1 MiB | 0.997 | 0.955 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 0.983 | 1.023 |
| tmpfs, original case | 200k / 4 / 1 MiB | 1.042 | 0.985 |

Four of five 16-writer ext4 pairs had lower throughput and higher p99. Their
throughput ratios were 1.013, 0.946, 0.977, 0.987, and 0.962. The tmpfs
four-writer gain did not translate to the primary ext4 workloads. Reject the
prototype: this small reduction in queue operations has not shown an ext4
benefit, and these measurements do not identify why the candidate was slower.

All 48 focused WAL tests passed on ext4 before timing. Restore the original
worker; no production change is retained. No leader comparison or adoption-gate
claim is made.

Transient script: `/tmp/rfc024_sqbatch_bench.py`.
Raw records: `/tmp/rfc024-sqbatch-chunk-bench.jsonl`.
Rejected patch: `/tmp/rfc024-sqbatch-rejected.patch`.

### Scheduling, sync overlap, and concurrency limit (measurement, 2026-09-29)

After the rejected queue-handle experiment, profile the unchanged `5a38eb90`
baseline rather than introduce another small timing adjustment. Sample each
live thread's `/proc/<pid>/task/<tid>/{comm,schedstat,wchan}` every 20 ms, keeping
its latest counters. Run the release/`bench` parallel WAL with `--profile`,
1 KiB values, PITR off, a 1 GiB SST target, and latency sampling every 10 puts.
Use 20k puts at one writer, 50k at four writers, and 200k at 16/32/64 writers.
No compilation or other benchmark ran concurrently.

| Writers | Profiled ops/s | Mean tickets covered per sync | Syncs with at least one write CQE during the syscall |
| ---: | ---: | ---: | ---: |
| 1 | 2,982 | 1.00 | 0.0% |
| 4 | 10,770 | 3.95 | 0.2% |
| 16 | 28,347 | 14.36 | 1.8% |
| 32 | 33,839 | 24.55 | 24.0% |
| 64 | 16,912 | 5.84 | 8.9% |

These are diagnostic observations, not paired throughput estimates. The
sampler and detailed sync bookkeeping add overhead. CQE overlap measures
software completion activity, not physical device queue depth.

At 16 writers, sampled I/O-worker CPU was 2.705 seconds and runnable queue
wait 0.010 seconds; the coordinator used 1.796 seconds of CPU and 0.063 seconds
of runnable queue wait. Its sampled wait sites included `submit_bio_wait`,
`futex_do_wait`, and `jbd2_log_wait_commit`. Thus lack of CPU scheduling time
for the dedicated workers does not appear to dominate this run. The low
write/sync overlap is consistent with synchronous producers arriving in waves:
most of the current writers are waiting for the captured sync rather than
supplying more writes. At 32 writers, overlap and throughput increase.

At 64 writers, threads named `write-perf` accumulated 126.423 seconds of CPU
and 5,419,938 scheduling slices, compared with 10.535 seconds and 372,755 slices
at 16 writers for the same 200k operations. This name includes the main thread
and producer threads. The sampler can miss short-lived threads and final
increments before exit; counters also include startup/shutdown, so these are
approximate whole-process-run attributions, not exact benchmark-phase totals.

Repeat without `--profile` or the sampler to check whether instrumentation
caused the collapse. Three trials used writer-count orders 16/32/64, 64/32/16,
and 32/16/64, with 200k puts and otherwise identical options. Every run remains
included. Independently computed medians are:

| Writers | Ops/s | p99 (ms) | Process CPU (s) | Sync count |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 28,190 | 1.300 | 14.789 | 13,911 |
| 32 | 33,016 | 2.878 | 29.895 | 8,132 |
| 64 | 10,110 | 20.925 | 186.069 | 30,336 |

The slowdown persists without diagnostics. This is not a controlled attribution
of the entire slowdown to one mechanism: device conditions can vary, and the
sampler does not identify individual contended mutexes. However, the large
increase in CPU and scheduling work establishes a software scaling concern,
not merely an absence of outstanding I/O.

The runtime's successful-sync path calls `durability.changed.notify_all()`;
all ticket waiters share that condition variable and state mutex, including
waiters whose tickets are beyond the newly durable frontier. A targeted next
experiment is ticket-aware notification that wakes only newly durable waiters
while preserving poison/shutdown notifications and preventing lost wakeups.
This is a hypothesis, not a demonstrated cause or a retained implementation.
Do not add more timed coalescing waits or increase depth solely from these
measurements. No runtime change, leader comparison, or adoption-gate claim is
part of this measurement commit.

Transient scripts: `/tmp/rfc024_scheduler_profile.py`,
`/tmp/rfc024_scheduler_profile_more.py`, and `/tmp/rfc024_concurrency_check.py`.
Per-run diagnostics and sampled threads: `/tmp/rfc024-scheduler-{1,4,16,32,64}.json`
and matching `-threads.json` files. Uninstrumented records:
`/tmp/rfc024-concurrency-check.jsonl`.

### Ticket-indexed durability wakeups (rejected, 2026-09-29)

Test the preceding notification hypothesis against `b2aa595a`. Replace the
single durability condition variable with 64 fixed condition variables indexed
by ticket modulo 64. After a successful sync, notify only slots intersecting
the newly durable ticket interval, bounded to all 64 slots for a larger advance.
Poison and shutdown still notify every slot. Waiters continue checking durability
and poison under the same mutex. Slot collisions can cause unrelated wakeups,
so this is a bounded approximation of per-ticket notification, not a full
per-ticket waiter registry. It adds no allocation per wait and no timed delay.

The candidate passed all 48 focused WAL tests on ext4, including existing
failure and shutdown coverage, before timing. Five alternating pairs per case
used release/`bench`, parallel mode in both arms, 1 KiB values, PITR off,
latency sampling every 10 operations, and no detailed sync diagnostics. No tests
or compilation overlapped timing. Ratios below are medians of paired ratios.

| Workload | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, high concurrency | 50k / 64 / 1 GiB | 1.016 | 0.943 |
| ext4, single writer | 5k / 1 / 1 GiB | 0.980 | 0.944 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 1.009 | 1.001 |
| ext4, rotation | 20k / 4 / 1 MiB | 0.982 | 0.973 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 0.956 | 1.087 |
| tmpfs, original case | 200k / 4 / 1 MiB | 0.987 | 1.018 |

The short 64-writer series reduced median process CPU from 34.734 to 33.508
seconds, with a median paired CPU ratio of 0.987. These small changes did not
resolve the large CPU cost. Repeat the original 200k-put, 64-writer workload
in three fresh alternating pairs to check the longer run:

| Pair | Throughput ratio | p99 ratio | Process CPU ratio |
| ---: | ---: | ---: | ---: |
| 0 | 0.992 | 1.029 | 1.013 |
| 1 | 1.037 | 0.983 | 0.969 |
| 2 | 0.815 | 1.147 | 1.195 |
| Median | 0.992 | 1.029 | 1.013 |

Median process CPU was 134.187 seconds for baseline and 135.849 seconds for the
candidate. Whole-device latency was about 0.16 ms/write in these full-length
runs; no pair was discarded. The result does not support retaining this
notification scheme. It also does not prove that every form of exact waiter
notification would fail, or identify which other shared lock dominates.
Restore the original condition variable and keep the high-concurrency slowdown
as an unresolved profiling target. No runtime change or adoption-gate claim
is retained.

Transient scripts: `/tmp/rfc024_ticketwake_bench.py` and
`/tmp/rfc024_ticketwake_confirm.py`.
Raw records: `/tmp/rfc024-ticketwake-chunk-bench.jsonl` and
`/tmp/rfc024-ticketwake-confirm.jsonl`.
Rejected patch: `/tmp/rfc024-ticketwake-rejected.patch`.

### Publication CPU attribution and shorter contention spin (rejected, 2026-09-29)

Profile `2883dcbb` with 200k puts, 64 writers, 1 KiB values, a 1 GiB SST target,
parallel WAL, and PITR off. Start the process first, wait 0.5 seconds for ring
startup, then attach `perf record -e cycles:u -F 99 -m 1 -p <pid>` for five
seconds. Attaching after startup with this small buffer succeeded where earlier
startup-time perf attempts conflicted with io_uring allocation. It collected
8,459 samples with zero reported loss. The hybrid CPU produced separate events:
`publish_commit_ts` accounted for 91.42% of sampled user cycles on the atom
PMU and 96.28% on the core PMU. These percentages exclude kernel cycles and are
not percentages of end-to-end wall time.

`perf annotate` places the hot instructions in the inlined publication frontier
spin: repeated atomic frontier loads/comparisons and `pause` instructions, with
an initial budget of 0x4000 (16,384) iterations. This is substantially stronger
CPU attribution than the earlier wait-site inference about WAL condition-variable
notifications. The producer must wait for earlier timestamps before reacquiring
the publication mutex and publishing its own timestamp; this ordered handoff
is downstream of successful WAL durability.

Test a shorter spin only under high contention: use 256 iterations when the
existing in-flight commit counter exceeds 32; otherwise retain 16,384. There
is no WAL mode switch, and the publication/durability predicates are unchanged.
The prototype changes the shared MVCC publication wait, with parallel WAL as
the performance target. An earlier unconditional reduction had hurt low-load
tmpfs performance, hence preserving that path here.

All 186 selected WAL, MVCC, and serializable transaction tests passed on ext4
before timing. Five alternating release/`bench` pairs per case used parallel
mode in both arms, 1 KiB values, PITR off, latency sampling every 10 operations,
and no detailed profiling. No compilation or tests overlapped the benchmark.
Ratios are medians of paired candidate/baseline ratios.

| Workload | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, high concurrency | 50k / 64 / 1 GiB | 0.317 | 1.599 |
| ext4, single writer | 5k / 1 / 1 GiB | 1.004 | 1.110 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 0.995 | 0.997 |
| ext4, rotation | 20k / 4 / 1 MiB | 1.005 | 0.940 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 1.026 | 0.887 |
| tmpfs, original case | 200k / 4 / 1 MiB | 0.986 | 0.975 |

At 64 writers, median process CPU fell from 34.128 to 20.543 seconds, but median
sync count rose from 8,765 to 30,633 per 50k writes. Throughput collapsed while
tails worsened. The first pair crossed a slow-device interval in both arms
(about 1.5 ms/write); later pairs still showed a large regression, including
0.311 and 0.298 throughput ratios with much lower device latency. Keep all pairs.

Reject and restore the spin budget. Merely parking sooner reduces CPU but makes
ordered publication progress worse in this workload. A stronger next design
hypothesis is to record completed publication work out of order, allowing one
publisher to advance the contiguous ready prefix for multiple timestamps rather
than requiring a separate thread handoff for every timestamp. That needs an
explicit correctness review of visibility, retired reservations, poison, and
barrier drain before implementation; this measurement does not change those
contracts. The original MVCC source is restored. No adoption-gate claim is made.

Transient profiler: `/tmp/rfc024_user_cpu_profile.py`.
Samples: `/tmp/rfc024-user-cpu.perf`; annotation:
`/tmp/rfc024-publication-annotate.txt`.
Benchmark script: `/tmp/rfc024_pubpressure_bench.py`.
Raw records: `/tmp/rfc024-pubpressure-chunk-bench.jsonl`.
Rejected patch: `/tmp/rfc024-pubpressure-rejected.patch`.

### Advance the contiguous ready publication prefix (retained, 2026-09-29)

The user-CPU profile above identified the publication handoff as the dominant
64-writer hotspot. Against `067d4b09`, register out-of-order completed memtable
publication in a `ready` timestamp set before waiting. A publisher or retirement
that closes the earliest gap advances the whole contiguous ready/retired prefix.
Followers no longer need to be scheduled one by one to advance their timestamps.
The existing spin budget remains unchanged; this removes dependent handoffs
rather than merely replacing spinning with parking.

Correctness boundaries:

- A timestamp enters `ready` only when its caller reaches `publish_commit_ts`,
  after WAL success and completed memtable insertion on WAL-backed write paths.
  The in-order path avoids inserting into the set.
- `current_ts` advances only to an actually published timestamp. Retired holes
  advance the ordering frontier but do not invent a visible commit timestamp.
- Every caller still waits until its own timestamp is inside the visible prefix.
  Its reservation remains in flight until that caller finishes, even when
  another publisher advanced its timestamp. Barrier draining is conservative.
- Advancement stops before `poisoned_at`. Earlier ready work can finish, including
  when retiring an earlier hole releases it; failed reservations remain unresolved.
- The set and frontiers remain under the existing publication mutex. Acquiring
  that mutex observes a ready publisher's preceding memtable work; publishing
  the reader-visible timestamp uses the existing release/acquire ordering.

This changes shared MVCC publication, not the WAL submission, durability, or
recovery protocol. It introduces no WAL mode selector and leaves PITR's WAL path
unchanged. Out-of-order publishers use a BTreeSet entry until the prefix reaches
them; this is extra bookkeeping proportional to outstanding ready publications.

Five alternating release/`bench` pairs per case used parallel WAL in both arms,
1 KiB values, PITR off, and latency sampling every 10 operations. No profiling,
compilation, or tests overlapped timing. Ratios are medians of paired ratios.

| Initial series | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, high concurrency | 50k / 64 / 1 GiB | 1.580 | 0.538 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 1.007 | 1.004 |
| ext4, single writer | 5k / 1 / 1 GiB | 0.992 | 1.066 |
| ext4, rotation | 20k / 4 / 1 MiB | 1.002 | 0.905 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 1.010 | 0.876 |
| tmpfs, original case | 200k / 4 / 1 MiB | 0.974 | 0.970 |

| Full-length confirmation, three fresh pairs | Puts / writers / SST target | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: |
| ext4, high concurrency | 200k / 64 / 1 GiB | 1.585 | 0.578 |
| ext4, medium concurrency | 200k / 32 / 1 GiB | 1.116 | 0.779 |
| ext4, original case | 200k / 4 / 1 MiB | 0.516 | 6.932 |

The 64-writer confirmation throughput ratios were 1.585, 1.596, and 0.976;
p99 ratios were 0.578, 0.568, and 1.573. The last pair crossed a slow-device
interval. Median sync count fell from 37,187 to 6,037; median process CPU fell
from 145.840 to 129.613 seconds, with paired CPU median ratio 0.889. At 32
writers, all three throughput ratios exceeded one (1.589, 1.116, 1.059), and
median process CPU fell from 30.434 to 27.513 seconds. These are observations
from this host, not a confidence-bound adoption result.

The full four-writer case is materially noisy and cannot be presented as a
clean pass. Its first two candidates crossed much slower device intervals,
with throughput ratios 0.516 and 0.313 and p99 ratios 6.932 and 12.433. The
third pair was near parity (0.996 throughput, 0.914 p99). To investigate, two
same-baseline-binary pairs produced throughput ratios 2.580 and 1.005: the
first unchanged run slowed to 3.8k ops/s, then the same binary reached 9.9k.
Fresh candidate/baseline pairs subsequently produced 2.516, 1.006, and 1.395,
with p99 ratios 0.142, 0.909, and 0.802. All pairs remain included. The large
same-binary swing proves substantial variability; it does not prove every
candidate slowdown is unrelated to the change. There is no reliable large
four-writer gain claim, and the adoption gate remains unproven.

Because publication is shared, a separate three-pair regression check used the
leader WAL mode with otherwise matching ext4 settings. Single-writer 5k puts
had throughput/p99 ratios 0.994/0.988; four-writer 20k rotation had 0.996/1.017.
This checks the shared change against its baseline, not parallel versus leader.

Retain the ready-prefix change for the repeated high-concurrency improvement,
with the above variability limits. Three new tests exercise ready followers
behind a missing head, retired gaps, poison boundaries, and a barrier draining
64 publications. All 189 focused WAL/MVCC/transaction tests passed on ext4.
`cargo make check` passed both Clippy configurations and all 1,410 tests, using
`TMPDIR=/dev/shm/rfc024-ahead-check-tmp` to avoid `/tmp` quota pressure.

Artifacts moved to `target/rfc024-ready-experiment/`: `bench.py`, `confirm.py`,
`rotation-null.py`, `rotation-confirm.py`, and `leader-check.py`, with matching
JSONL results (`bench.jsonl`, `confirm.jsonl`, `rotation-null.jsonl`,
`rotation-confirm.jsonl`, `leader-check.jsonl`). This ignored directory also
contains the baseline binary, original MVCC source, and validation logs.

### Shorter publication spin after ready-prefix advancement (retained, 2026-09-30)

Profile `478ce6d0` again after the ready-prefix change. A five-second
`cycles:u` capture during the 200k-put, 64-writer ext4 workload collected
9,044 samples with no reported loss. Publication still accounted for 91.69%
of sampled atom-PMU user cycles and 94.65% on the core PMU. Annotation places
the hot instructions in the 16,384-iteration frontier spin. These are user-CPU
shares, not wall-time shares.

Revisit the previously rejected shorter-spin experiment on this new baseline:
use 256 iterations when more than 32 commit reservations are in flight, and
retain 16,384 otherwise. Ready timestamps can now advance while their owners
are parked, so parking no longer requires every follower to resume before
the next timestamp can advance. The relaxed in-flight count is only a
scheduling hint; the acquire frontier load and locked completion/poison
predicates still determine the result. No durability or visibility rule changes.

A separate five-second candidate capture used 400k puts to keep writes active
throughout sampling. It collected 2,452 samples with no reported loss;
publication accounted for 25.05% of atom-PMU and 43.29% of core-PMU user
cycles. An initial shorter capture included final flushing and is not used
for this attribution. End-to-end CPU comparisons below come from unprofiled
runs with matching operation counts.

Both arms use release/`bench` builds, parallel WAL, PITR off, 1 KiB values,
and latency sampling every 10 operations. Run arms in alternating order;
profiling, compilation, and tests do not overlap timing. Ratios below are
medians of paired candidate/baseline ratios.

| Workload | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, initial screen | 50k / 64 / 1 GiB | 3 | 1.837 | 0.446 |
| ext4, full confirmation | 200k / 64 / 1 GiB | 5 | 1.773 | 0.464 |
| ext4, medium concurrency | 200k / 32 / 1 GiB | 5 | 1.041 | 0.929 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 5 | 1.000 | 0.986 |
| ext4, single writer | 5k / 1 / 1 GiB | 5 | 0.988 | 1.009 |
| ext4, rotation | 20k / 4 / 1 MiB | 5 | 0.981 | 1.003 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 5 | 1.008 | 0.901 |
| tmpfs, original case | 200k / 4 / 1 MiB | 5 | 1.006 | 1.002 |

For the full 64-writer case, median arm throughput was 25,195 versus 45,694
puts/s; median process CPU was 127.205 versus 22.528 seconds, with a paired
CPU ratio of 0.179. Peak outstanding groups and write SQEs remained 16.
The five throughput ratios were 1.041, 0.791, 1.841, 1.773, and 1.877.
The first two pairs encountered slow-device intervals: their p99 ratios were
2.104 and 1.028, versus 0.461, 0.464, and 0.456 in the remaining pairs.
All pairs remain included. The 32-writer arm also crossed device-latency
swings despite never selecting the shorter spin; its apparent gain is not
evidence for this high-contention optimization.

Because publication is shared, separately compare baseline and candidate
with leader WAL. Three initial 50k-put, 64-writer pairs had throughput/p99
ratios of 1.218/1.484, with whole-device write latencies ranging from 0.36 to
6.0 ms. Three same-baseline-binary pairs then had p99 ratios from 0.927 to
1.042. Five fresh candidate/baseline pairs, with similar device latency
between arms, had median throughput/p99 ratios of 1.534/0.485. The initial
tail regression was not reproduced, but those observations remain recorded.
Three-pair leader guards at one writer and four-writer rotation had ratios
of 1.011/0.985 and 0.992/1.032 respectively. These are shared-code regression
checks, not parallel-versus-leader comparisons.

Retain the change for the repeated 64-writer improvement and substantially
lower CPU use. This does not establish the original four-writer adoption
gate or change the default WAL path. All 335 selected WAL, MVCC, and
transaction tests passed before timing. Final `cargo make check` passed both
Clippy configurations, all 1,410 tests, formatting, dependency checks, and
typos, using tmpfs for test temporary files and four nextest test threads.

Artifacts: `target/rfc024-postready-experiment/` contains the baseline binary
and MVCC source, profiler scripts and samples, `bench.py` and `bench.jsonl`,
and the leader guard/null/confirmation scripts and JSONL records.

### Lower publication contention threshold (not retained, 2026-09-30)

Follow up on `eb973f81` by testing the shorter 256-iteration publication spin
when more than four reservations are in flight instead of more than 32.
The previous user-CPU profile still attributed substantial time to publication;
this experiment asks whether parking sooner also helps moderate concurrency.
Keep the completion, poison, and durability predicates unchanged.

Compare the saved previous candidate binary (the behavior retained in
`eb973f81`) against a release/`bench` candidate. Both arms use parallel WAL,
PITR off, 1 KiB values, a 1 GiB SST target, and latency sampling every ten
operations. Alternate arm order, with no compilation or tests during timing.
Ratios are medians of paired candidate/baseline ratios.

| ext4 workload | Pairs | Throughput ratio | p99 ratio |
| --- | ---: | ---: | ---: |
| 200k puts, 32 writers, initial screen | 3 | 0.931 | 1.490 |
| 200k puts, 32 writers, fresh confirmation | 5 | 1.041 | 0.747 |
| 200k puts, 32 writers, all observations | 8 | 0.986 | 1.119 |
| 50k puts, 16 writers | 3 | 1.001 | 0.963 |

The 32-writer throughput ratios, in observation order, were 0.931, 0.607,
1.069, 1.132, 1.041, 0.525, 0.414, and 1.119. Corresponding p99 ratios were
3.495, 1.490, 0.676, 0.684, 0.747, 5.009, 1.580, and 0.696. The four slower
candidate observations also had higher whole-device average write latency;
this counter is contextual and cannot establish that the change is innocent.
Both arms reached 16 outstanding groups and write SQEs. Keep every observation,
including the initial screen; the favorable fresh-run median alone is not a
sufficient reason to retain the change. Sixteen writers showed no meaningful
throughput gain.

Restore the threshold of 32. This experiment does not establish a reliable
improvement and leaves the earlier high-contention optimization intact. No
production-code change is retained, so validation is limited to the document
and confirming that the Rust source matches the starting revision.

Artifacts: `target/rfc024-spin-threshold-experiment/` contains both binaries,
the original MVCC source, benchmark scripts, `bench.jsonl`, `screen.log`,
`confirm.log`, and the candidate build log. The JSONL file appends both phases;
pair numbers restart in the confirmation phase, so preserve record order.

### Finish waiting publications without reacquiring the mutex (not retained, 2026-09-30)

The post-ready-prefix profile still showed publication and mutex contention.
Test removing the second publication-mutex acquisition after a writer has
successfully awaited its visible timestamp. The candidate decrements its
in-flight reservation and only reacquires the mutex to notify a drain waiter
when the sequentially consistent admission gate is closed. It leaves the
initial publication lock and visibility/poison predicates unchanged.

All 335 selected WAL/MVCC/transaction tests passed before timing. Compare the
saved `eb973f81` behavior against this release/`bench` candidate, with parallel
WAL in both arms, PITR off, 200k puts, 1 KiB values, a 1 GiB SST target, and
latency sampling every ten operations. Three pairs alternate arm order, with
no builds or tests during timing.

| Writers | Paired throughput ratios | Median throughput ratio | Median p99 ratio |
| ---: | --- | ---: | ---: |
| 64 | 0.991, 0.966, 1.025 | 0.991 | 0.997 |
| 32 | 0.259, 0.413, 0.977 | 0.413 | 5.020 |

The first two 32-writer pairs crossed slower device intervals in the candidate;
the third had similar whole-device write latency (0.090 versus 0.087 ms/write)
and still showed no gain. The 64-writer runs had similar device latency across
arms. Every observation is retained. Revert the optimization: even the quieter
pairs do not justify retaining the additional completion-path branch.

Artifacts: `target/rfc024-publication-finish-experiment/` contains the original
MVCC source, both binaries, `bench.py`, `bench.jsonl`, `screen.log`, and build
and test logs. This is an unsuccessful experiment, not an adoption-gate result.

### Thirty-two groups after publication fixes (retained, 2026-09-30)

Revisit the previously rejected 32-group limit after ready-prefix publication
and the high-contention spin reduction. The current 64-writer baseline repeatedly
reaches 16 outstanding groups and write SQEs. Raise only the parallel worker's
group limit to 32; the shared ring limit remains 256 SQEs, and buffer budgets,
ordered frontiers, failure handling, and the default leader WAL are unchanged.
This changes software pipeline capacity, not a measured NVMe queue depth.

Compare the saved `eb973f81` behavior against a release/`bench` candidate. Both
arms use parallel WAL, PITR off, 1 KiB values, and latency sampling every ten
operations. Alternate arm order and keep compilation, tests, and profiling out
of timed runs. Ratios are medians of paired candidate/baseline ratios.

| Workload | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, initial screen | 200k / 64 / 1 GiB | 3 | 1.021 | 0.961 |
| ext4, fresh confirmation | 200k / 64 / 1 GiB | 5 | 1.021 | 0.966 |
| ext4, all high-concurrency observations | 200k / 64 / 1 GiB | 8 | 1.021 | 0.964 |
| ext4, medium concurrency | 200k / 32 / 1 GiB | 3 | 1.035 | 0.980 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 5 | 0.979 | 0.999 |
| ext4, single writer | 5k / 1 / 1 GiB | 5 | 0.992 | 0.942 |
| ext4, rotation | 20k / 4 / 1 MiB | 5 | 1.012 | 0.920 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 5 | 0.993 | 1.021 |
| tmpfs, original case | 200k / 4 / 1 MiB | 5 | 1.008 | 0.937 |

Across all eight 64-writer pairs, median arm throughput was 46,684 versus
48,004 puts/s, and median arm p99 was 2.618 versus 2.535 ms. Paired throughput
ratios were 1.021, 1.059, 0.977, 1.046, 0.991, 1.039, 1.013, and 1.021;
all eight p99 ratios improved, ranging from 0.952 to 0.974. Both the initial
screen and fresh confirmation show the same modest effect. Outstanding groups
and write SQEs reached 16 in baseline and 32 in candidate. Median sync count
increased from 5,057 to 5,166 per 200k puts: the extra capacity does not mean
more tickets per sync.

Three same-baseline-binary control pairs at 64 writers had throughput ratios
0.998, 0.970, and 0.227, with p99 ratios 1.021, 1.017, and 9.889. The last
unchanged-binary pair slowed from 50,950 to 11,549 puts/s and reproduced the
large device/runtime variability. The third 32-writer candidate pair had the
opposite swing (6,511 versus 36,880 puts/s); its apparent 5.664 ratio is not
credited to the code. Single-writer ext4 also crossed a slow interval. Keep
all observations, including these extremes. The small throughput signal is
close to ordinary same-binary variation; the consistent high-concurrency p99
reduction is the stronger reason to retain this bounded tuning change.

Keep this as a separate, reversible optimization on the opt-in path. It does
not establish a 10% throughput gain, the original four-writer adoption gate,
or a new default. Update the worker-bound test to fill 32 groups with 17
writes each, reject a 33rd group, and complete waves of 256, 256, and 32 writes
while checking the shared SQE bound and buffer retirement.

Artifacts: `target/rfc024-postready-depth-experiment/` contains benchmark scripts,
`bench.jsonl` (screen followed by confirmation, with pair numbers restarting),
`null.jsonl`, `guards.jsonl`, their logs, the baseline binary, and original worker
source. The candidate binary and validation logs are preserved there as well.

Validation: `cargo make check` passed both Clippy configurations, all 1,410
tests with zero skips, formatting, dependency checks, and typos. Tests used
`TMPDIR=/dev/shm/rfc024-ahead-check-tmp` and four nextest threads, after all
benchmark runs completed.

### Compile worker invariant traversal only in debug builds (not retained, 2026-09-30)

Profile the `ce82d01c` behavior with 400k puts, 64 writers, parallel WAL, PITR
off, 1 KiB values, and a 1 GiB SST target on ext4. Attach a five-second
`cycles:u` capture after 0.5 seconds of startup, at 99 Hz with a one-page
perf buffer. The capture collected 2,334 samples without reported loss.
`WorkerCore::assert_invariants` accounted for 2.33% of atom-PMU and 2.17% of
core-PMU user cycles. Publication still accounted for 23.24% and 40.06%,
respectively. These are sampled user-CPU shares, not wall-time shares.

The invariant helper contains a vector allocation and adjacent-group lookups
outside its `debug_assert!` expressions. Test guarding that traversal with
`cfg(debug_assertions)` and using zipped iterators instead of collecting a
temporary vector. The candidate preserves debug ordering assertions; release
symbol inspection no longer finds the invariant helper. All 63 focused
parallel-WAL tests passed before timing.

Both benchmark arms use release/`bench`, parallel WAL, PITR off, 1 KiB values,
and latency sampling every ten operations. Alternate arm order, with no
profiling, builds, or tests during timing. Ratios are medians of paired
candidate/baseline ratios; retain every observation.

| Workload | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, high concurrency | 200k / 64 / 1 GiB | 5 | 0.982 | 0.987 |
| ext4, medium concurrency | 200k / 32 / 1 GiB | 5 | 1.001 | 0.995 |
| ext4, growing WAL | 50k / 16 / 1 GiB | 3 | 1.005 | 1.049 |
| ext4, single writer | 5k / 1 / 1 GiB | 3 | 1.006 | 1.008 |
| ext4, rotation | 20k / 4 / 1 MiB | 3 | 1.013 | 0.977 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 3 | 0.933 | 0.803 |
| tmpfs, original case | 200k / 4 / 1 MiB | 3 | 1.005 | 1.025 |

The first two 64-writer pairs and the second 32-writer pair crossed slower
device intervals in the candidate. The other three 64-writer throughput
ratios were 1.019, 0.997, and 0.982, also offering no repeatable throughput
gain. Median paired process CPU ratios were 1.041 at 64 writers and 1.034
at 32 writers. Removing the profiled helper did not reduce measured total
CPU in these runs; these measurements do not establish why. Do not retain
the optimization based solely on an eliminated symbol or allocation when
end-to-end evidence does not support it. Restore the original worker source.
The existing 32-group limit remains unchanged.

Artifacts: `target/rfc024-depth32-profile/` contains the baseline profiling
script, perf capture, profiler log, and workload JSON. The experiment directory
`target/rfc024-worker-invariants-experiment/` preserves both binaries and worker
sources, `bench.py`, `bench.jsonl`, `guards.py`, `guards.jsonl`, benchmark logs,
and the candidate build/test logs. No runtime change or adoption-gate claim
is retained. Final validation checks document spelling and whitespace and
confirms that the Rust source matches the starting revision.


### Distance-aware publication waiting (2026-09-30)

The 32-group profile still attributes 23.24% of atom-PMU and 40.06% of
core-PMU user cycles to publication. At 32 live reservations, the existing
contention hint retains a 16,384-iteration spin budget even for a caller far
behind a missing predecessor. Test the existing 256-iteration budget when
more than 16 reservations are live and the caller is at least 16 timestamps
ahead of the mirrored frontier. More than 32 reservations still selects the
short budget as before. Acquire frontier reads and locked completion/poison
predicates decide success; these hints only choose when to park.

An initial variant checked distance at every concurrency level. Its ten
32-writer pairs showed throughput/p99 ratios 1.073/0.796, but the original
four-writer tmpfs case regressed in two independent five-pair sets
(0.927/1.057 and 0.946/1.063). Do not retain that variant. The revised variant
avoids the additional frontier read with 16 or fewer live reservations.

Both arms use release/`bench`, parallel WAL, PITR off, 1 KiB values, and latency
sampling every ten operations. Alternate arm order; no builds, tests, or
profiling overlap timing. Compare against `ce82d01c` runtime behavior.
Ratios are medians of paired candidate/baseline ratios; retain every observation.

| Workload | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, 32 writers, initial | 200k / 32 / 1 GiB | 5 | 1.036 | 0.823 |
| ext4, 32 writers, fresh | 200k / 32 / 1 GiB | 3 | 1.046 | 0.840 |
| ext4, 32 writers, combined | 200k / 32 / 1 GiB | 8 | 1.040 | 0.831 |
| ext4, 64 writers | 200k / 64 / 1 GiB | 3 | 0.986 | 1.024 |
| ext4, 16 writers | 50k / 16 / 1 GiB | 3 | 1.008 | 0.980 |
| ext4, single writer | 5k / 1 / 1 GiB | 3 | 1.006 | 0.843 |
| ext4, rotation | 20k / 4 / 1 MiB | 3 | 1.005 | 1.078 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 5 | 1.015 | 0.864 |
| tmpfs, original case | 200k / 4 / 1 MiB | 5 | 0.981 | 1.004 |

Median paired process CPU at 32 writers is 0.835, with lower CPU and p99
in all eight pairs. Two baseline runs crossed slow-device intervals and
produced apparent 3.48x and 2.90x throughput gains; do not credit those to the
code. The other six throughput ratios are 0.962, 1.013, 1.045, 1.036, 1.046,
and 1.032. The 64-writer set includes a candidate slow interval (0.259
throughput, 8.159 p99); the other two throughput ratios are 0.996 and 0.986.
Whole-device write latency is contextual, not a critical-path attribution.

Because this changes shared MVCC code, run leader-mode regression controls.
The first three 32-writer pairs have throughput/p99 medians 0.326/7.618,
including two candidate slow intervals. A fresh three-pair set with reversed
starting order has medians 2.761/0.152, including two baseline slow intervals.
All six pairs combined have medians 1.002/1.001. The two pairs without these
large swings are 0.999/0.999 and 1.005/1.003. An identical-baseline-binary null
control also produces a 5.026/0.100 pair, followed by near-parity pairs; its
three-pair medians are 1.010/0.965. These controls expose substantial device
variability and do not establish a leader performance improvement. Leader
single-writer and rotation controls have three-pair throughput/p99 medians
1.002/0.954 and 1.005/0.990, respectively.

Retain the revised scheduling hint for review based on the repeated 32-writer
parallel CPU/p99 improvement and near-parity combined leader controls.
This experiment does not prove the device-backed adoption gate: the comparison
is candidate versus the existing parallel pipeline, and the device variability
limits the throughput evidence. Leader remains the default.

Artifacts: `target/rfc024-publication-distance-experiment/` contains the baseline
and first-variant binaries and sources, both build logs, focused test log,
`bench.jsonl`, `guards.jsonl`, solo/tmpfs confirmation logs, revised `v2.jsonl`
and `v2-confirm.jsonl`, leader controls, `v2-leader-null.jsonl`, and
`v2-leader-confirm.jsonl`. The revised candidate binary and source are preserved as `candidate-v2` and
`candidate-v2-mvcc.rs`. `cargo make check` passed, including default- and
all-feature Clippy checks and all 1,410 tests. A separate documentation spelling
check and whitespace check passed. Logs are `check.log` and `docs-check.log`
in the experiment directory.


### Shorter low-contention publication spin budget (2026-09-30)

Profile the retained distance-aware publication hint at 32 writers on ext4:
400k puts, 1 KiB values, 1 GiB SST target, parallel WAL, PITR off. Attach perf
after 0.5 seconds of startup, using `cycles:u`, 99 Hz, and a one-page perf
buffer for five seconds. The capture has 2,529 samples with no reported loss.
Publication accounts for 56.12% of atom-PMU and 62.77% of core-PMU user cycles;
instruction annotation concentrates samples in the repeated frontier reads,
comparisons, and spin-loop bookkeeping. These are sampled user-CPU shares,
not wall-time shares.

Test reducing the longer publication spin budget from 16,384 to 4,096 iterations.
Keep the 256-iteration contention/distance budget and all completion, poison,
and condition-variable predicates unchanged. Compare against `a4a6093e`, with
release/`bench`, alternating arm order, 1 KiB values, and latency sampling every
ten operations. No builds, tests, or profiling overlap benchmark timing.
Ratios are medians of paired candidate/baseline ratios; keep every observation.

| Workload | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: | ---: |
| ext4, 64 writers | 200k / 64 / 1 GiB | 5 | 1.002 | 0.997 |
| ext4, 32 writers, initial | 200k / 32 / 1 GiB | 5 | 1.003 | 0.898 |
| ext4, 32 writers, fresh | 200k / 32 / 1 GiB | 3 | 0.997 | 0.892 |
| ext4, 32 writers, combined | 200k / 32 / 1 GiB | 8 | 1.000 | 0.896 |
| ext4, 16 writers | 50k / 16 / 1 GiB | 3 | 1.001 | 0.984 |
| ext4, single writer | 5k / 1 / 1 GiB | 3 | 1.026 | 0.872 |
| ext4, rotation | 20k / 4 / 1 MiB | 3 | 1.012 | 0.980 |
| tmpfs, single writer | 50k / 1 / 1 GiB | 3 | 1.136 | 1.004 |
| tmpfs, original case | 200k / 4 / 1 MiB | 3 | 1.010 | 1.033 |

Across the eight 32-writer pairs, median process CPU is 0.972. Seven pairs
improve p99; the remaining pair crosses slow-device intervals in both arms,
with throughput/p99 ratios 0.762/1.237, and remains included. Six pairs reduce
CPU. At 64 writers, two candidate and two baseline runs cross slow intervals,
producing throughput ratios 0.290, 0.269, 3.176, and 4.111; do not credit these
swings to the code. The remaining pair is near parity. Whole-device latency
counters include unrelated and out-of-window I/O and are contextual only.
The tmpfs single-writer throughput difference is not established as an effect
of this hint: an in-order publication does not enter the changed wait loop.

Leader-mode regression controls compare baseline and candidate, not parallel
versus leader. The initial three 32-writer pairs have throughput/p99 medians
0.307/7.066, including two candidate slow intervals. A fresh reversed-order
set has medians 0.822/1.489; two candidate runs and one baseline run cross slow
intervals. Across all six pairs the medians are 0.678/2.862. The large
whole-device latency differences make attribution uncertain, but the repeated
comparison does not clear this shared change. Leader single-writer and rotation
three-pair throughput/p99 medians are 1.005/0.980 and 1.011/1.011.

Do not retain the shorter spin budget. The parallel p99 improvement is repeatable,
but throughput remains near parity and the leader regression control is unresolved.
Restore `a4a6093e` source and rebuild the release binary before further experiments.
No runtime optimization or adoption-gate claim is retained.

Artifacts: `target/rfc024-distance-profile/` contains the starting perf capture,
annotation, profiling script, and workload JSON. The experiment directory
`target/rfc024-spin4096-experiment/` preserves both binaries and MVCC sources,
build log, `bench.jsonl`, `guards.jsonl`, `leader.jsonl`, `leader-confirm.jsonl`,
benchmark scripts, and logs. Final checks verify spelling, whitespace, and
that the Rust source matches the starting revision.


### Shorter spin budget confirmation (2026-09-30)

Rerun the saved 4,096-iteration candidate after the initial rejection, at the
user's request. Compare the same baseline and candidate binaries from
`target/rfc024-spin4096-experiment/`, with fresh database paths, alternating arm
order, 200k puts, 32 writers, 1 KiB values, and 1 GiB SST target on ext4.
Keep PITR off and sample latency every ten operations. No builds, tests, or
profiling overlap timing. Ratios are medians of paired candidate/baseline
ratios; include all previous observations when reporting cumulative results.

| Comparison | Pairs | Throughput ratio | p99 ratio | Process CPU ratio |
| --- | ---: | ---: | ---: | ---: |
| Fresh identical-baseline leader control | 3 | 0.992 | 0.996 | 1.024 |
| Fresh leader regression control | 6 | 1.001 | 0.992 | 0.988 |
| Fresh parallel WAL comparison | 6 | 1.040 | 0.837 | 0.910 |
| All parallel 32-writer comparisons | 14 | 1.015 | 0.884 | 0.948 |

The identical-baseline control is stable in this session. The first two fresh
leader candidate runs still cross slow intervals, with throughput/p99 ratios
0.341/6.642 and 0.277/7.399. Four later pairs are near parity. WAL byte counts
are identical and sync counts nearly identical; the two slow candidates spend
15.3 and 19.2 seconds in fdatasync versus 4.2 seconds in their paired baselines.
This localizes the extra time but does not explain why it disproportionately
affects the candidate. The previous six leader pairs remain in the preceding
section. Across all twelve leader pairs, half contain slow candidate intervals;
the cumulative median is about 0.911 throughput and 1.247 p99. A near-parity
fresh median does not resolve that earlier regression evidence.

One fresh parallel baseline crosses a slow interval, producing an apparent
6.11x throughput gain and 0.051 p99 ratio; do not credit those magnitudes to
the code. The remaining five fresh throughput ratios are 1.011, 1.062, 1.036,
1.044, and 1.024, with lower p99 in every pair. Across all fourteen parallel
pairs, thirteen improve p99 and twelve reduce process CPU. This supports the
parallel latency improvement more strongly than the initial set alone.

Retain the shorter spin budget as a separate local optimization commit for
review, based on the repeated parallel results and the fresh near-parity leader
median. The candidate-specific leader latency spikes remain an unresolved
limitation, not a cleared regression or an adoption-gate result. The default
WAL selection remains leader; no public selection control is added.

Confirmation artifacts live under `target/rfc024-spin4096-confirmation/`:
`bench.py`, `null.py`, `null.jsonl`, `leader.jsonl`, `parallel.jsonl`, and their
logs. Baseline/candidate source and binary snapshots remain in the original
experiment directory. Final verification runs the full local check on the
retained candidate, with its log saved as `check.log` in the confirmation
directory.

The full local check passed, including default- and all-feature Clippy checks
and all 1,410 tests. Documentation spelling and whitespace checks also passed.


### Parallel worker request-map hashing (2026-09-30)

Profile the retained 4,096-iteration publication budget at 32 writers on ext4:
400k puts, 1 KiB values, 1 GiB SST target, parallel WAL, PITR off. Attach perf
after 0.5 seconds of startup with `cycles:u`, 99 Hz, and a one-page buffer for
five seconds. Publication still accounts for 52.46% of atom-PMU and 56.61% of
core-PMU user cycles. Request-ID hashing in the WAL worker accounts for
1.29% and 1.27%, respectively. These are sampled user-CPU shares, not wall time.

Test replacing only `WorkerCore::writes`'s standard hash map with the existing
`ahash::AHashMap`. IDs are private worker-generated request identities; keep
all group/permit maps, queue handling, completion states, ownership rules,
and MVCC publication unchanged. All 23 focused worker tests pass before timing.
Both benchmark arms use release/`bench`, PITR off, 1 KiB values, latency sampling
every ten operations, and alternate order. Compare against `925ed07d` runtime
behavior. No builds, tests, or profiling overlap timing. Retain every observation;
report medians of paired candidate/baseline ratios.

| Workload | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio | CPU ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| ext4-64-full, initial | 200k / 64 / 1 GiB | 5 | 1.008 | 0.994 | 1.014 |
| ext4-32-full, initial | 200k / 32 / 1 GiB | 5 | 1.012 | 0.993 | 1.021 |
| ext4-64-full, fresh | 200k / 64 / 1 GiB | 3 | 0.996 | 1.018 | 0.994 |
| ext4-32-full, fresh | 200k / 32 / 1 GiB | 3 | 1.014 | 0.999 | 1.016 |
| tmpfs-solo, fresh | 50k / 1 / 1 GiB | 3 | 1.071 | 1.130 | 0.958 |
| ext4-solo, fresh | 5k / 1 / 1 GiB | 3 | 1.022 | 0.899 | 0.988 |
| ext4-16, fresh | 50k / 16 / 1 GiB | 3 | 0.968 | 1.006 | 1.058 |
| ext4-rotation, fresh | 20k / 4 / 1 MiB | 3 | 0.985 | 0.909 | 1.043 |
| tmpfs-exact, fresh | 200k / 4 / 1 MiB | 3 | 0.990 | 0.985 | 0.989 |
| ext4-64-full, combined | 200k / 64 / 1 GiB | 8 | 1.002 | 0.995 | 1.014 |
| ext4-32-full, combined | 200k / 32 / 1 GiB | 8 | 1.013 | 0.996 | 1.019 |

The initial 64-writer set includes slow-device intervals in the second and
third baselines; the initial 32-writer set includes a pair where both arms
cross slow intervals. The second fresh 32-writer pair also has a slower
baseline and an apparent 1.413 throughput ratio. Do not credit apparent large
gains in those pairs to the code. Whole-device latency counters include unrelated and out-of-window
I/O and provide context rather than critical-path attribution. Keep those
observations in all medians.

Do not retain the request-map change. A small 32-writer throughput difference
comes with higher measured process CPU; the fresh 64-writer comparison is
near parity, while the 16-writer and tmpfs single-writer guards regress.
The sampled hash cost alone does not justify this tradeoff. Restore the worker
source and rebuild the release binary. The retained publication spin budget,
32-group limit, and existing parallel WAL behavior remain unchanged.

Artifacts: `target/rfc024-spin4096-profile/` contains the baseline profile,
script, and workload JSON. `target/rfc024-worker-ahash-experiment/` preserves
both binaries and worker sources, build and focused-test logs, `bench.py`,
`bench.jsonl`, `guards.jsonl`, and benchmark logs. Final checks verify spelling,
whitespace, and that the Rust source matches the starting revision.


### Sync coordinator empty-channel waiting (2026-09-30)

The 4,096-iteration publication-budget profile attributes 3.11% of atom-PMU
and 6.17% of core-PMU user cycles to the sync coordinator's Crossbeam list
channel receive. Inspect the local Crossbeam source: ordinary receive retries
with spin/yield backoff before registering a waiter; the selection path tries
once before registering. Test a single-channel `select_biased!` in the outer
coordinator receive loop. Keep coalescing receives, completion processing,
captured sync targets, poison handling, and shutdown draining unchanged.

All 63 focused parallel-WAL tests pass before timing. Compare against
`925ed07d` runtime behavior, with release/`bench`, parallel WAL, PITR off,
1 KiB values, latency sampling every ten operations, and alternating arm order.
No builds, tests, or profiling overlap timing. Retain every observation;
report medians of paired candidate/baseline ratios.

| Workload | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio | CPU ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| ext4-64-full, initial | 200k / 64 / 1 GiB | 5 | 1.013 | 0.978 | 1.037 |
| ext4-32-full, initial | 200k / 32 / 1 GiB | 5 | 0.978 | 0.991 | 1.097 |
| ext4-64-full, fresh | 200k / 64 / 1 GiB | 3 | 0.973 | 0.995 | 1.052 |
| ext4-32-full, fresh | 200k / 32 / 1 GiB | 3 | 0.963 | 0.980 | 1.112 |
| tmpfs-solo, fresh | 50k / 1 / 1 GiB | 3 | 0.968 | 1.060 | 0.994 |
| ext4-solo, fresh | 5k / 1 / 1 GiB | 3 | 1.013 | 1.012 | 0.954 |
| ext4-rotation, fresh | 20k / 4 / 1 MiB | 3 | 0.977 | 0.885 | 1.012 |
| tmpfs-exact, fresh | 200k / 4 / 1 MiB | 3 | 0.973 | 1.035 | 1.019 |
| ext4-64-full, combined | 200k / 64 / 1 GiB | 8 | 1.001 | 0.983 | 1.042 |
| ext4-32-full, combined | 200k / 32 / 1 GiB | 8 | 0.972 | 0.985 | 1.104 |

The initial 64-writer set includes slow intervals in both arms, and the final
initial 32-writer pair crosses slow intervals in both arms. The second fresh
rotation baseline is slower, yielding an apparent 3.568 throughput ratio.
Do not credit these large differences to the code. Whole-device latency
counters include unrelated and out-of-window I/O and are contextual only.
All observations remain in the reported medians.

Do not retain this scheduling change. Both 32-writer sets reduce throughput
and increase total process CPU; the fresh 64-writer and tmpfs comparisons also
regress. Removing a receive backoff does not establish that CPU is eliminated
rather than shifted elsewhere. These measurements do not explain the increased
CPU, but they do not support the candidate. Restore the original coordinator
and rebuild the release binary. No runtime optimization is retained.

Artifacts: `target/rfc024-spin4096-profile/` contains the baseline profile and
workload JSON. `target/rfc024-sync-select-experiment/` preserves baseline and
candidate binaries and runtime sources, build and focused-test logs, benchmark
script, `bench.jsonl`, `guards.jsonl`, and their logs. Final checks verify
spelling, whitespace, and source identity with the starting revision.


### Reused submission staging vector (2026-09-30)

`WorkerCore::stage_writes` allocates a vector with capacity up to 256 on every
worker pass, even when no write is ready. Test reusing one submission-metadata
vector for the worker loop. Clear it before staging and drain it after pushing
SQEs, preserving capacity. The vector never owns submitted buffers; request
state and buffer ownership remain in `WorkerCore`. A test-only wrapper preserves
the existing state-machine test interface.

All 64 focused parallel-WAL tests pass before timing, including a new regression
check for distinct staged requests, reused capacity, empty passes, reversed
completion order, and buffer retirement. Compare against `925ed07d` runtime
behavior with release/`bench`, parallel WAL, PITR off, 1 KiB values, latency
sampling every ten operations, and alternating order. No builds, tests, or
profiling overlap timing. Keep every observation and report medians of paired
candidate/baseline ratios.

| Workload | Puts / writers / SST target | Pairs | Throughput ratio | p99 ratio | CPU ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| ext4-64-full, initial | 200k / 64 / 1 GiB | 5 | 0.987 | 0.995 | 1.012 |
| ext4-32-full, initial | 200k / 32 / 1 GiB | 5 | 0.990 | 1.016 | 1.070 |
| ext4-64-full, fresh | 200k / 64 / 1 GiB | 3 | 1.011 | 0.995 | 1.019 |
| ext4-32-full, fresh | 200k / 32 / 1 GiB | 3 | 1.001 | 0.988 | 1.060 |
| ext4-16, fresh | 50k / 16 / 1 GiB | 3 | 1.006 | 1.027 | 0.993 |
| tmpfs-exact, fresh | 200k / 4 / 1 MiB | 3 | 1.014 | 1.080 | 0.981 |
| ext4-64-full, combined | 200k / 64 / 1 GiB | 8 | 0.993 | 0.995 | 1.015 |
| ext4-32-full, combined | 200k / 32 / 1 GiB | 8 | 0.994 | 1.002 | 1.065 |

Some 64-writer pairs cross slow-device intervals in both arms. All observations
remain in the medians; do not credit large throughput differences caused by
unequal device conditions to the code. Whole-device latency counters include
unrelated and out-of-window I/O and are contextual only.

Do not retain staging-vector reuse. Both 32-writer sets have higher process CPU,
without a repeatable throughput gain, and the tmpfs guard has worse p99.
Removing an allocation from the source is insufficient end-to-end evidence.
These measurements do not explain the increased CPU. Restore the worker and
rebuild the release binary; existing runtime optimizations remain unchanged.

Artifacts: `target/rfc024-stage-scratch-experiment/` preserves baseline and
candidate binaries and worker sources, build and focused-test logs, benchmark
script, `bench.jsonl`, `confirm.jsonl`, and their logs. Final checks verify
spelling, whitespace, and source identity with the starting revision.


### Cumulative optimization and current leader comparison (2026-09-30)

At the user's request, measure the complete retained optimization sequence in
one session rather than multiplying gains from separate experiments. The saved
pre-ready-prefix parallel binary represents `067d4b09`; the current binary
represents `925ed07d` runtime behavior, with subsequent documentation-only
commits. Snapshot the current binary before timing. `metadata.json` records
both SHA-256 digests. This is the baseline before the five recent retained
publication/group-depth changes, not the original RFC implementation or the
pre-PITR revision.

Use release/`bench`, ext4 on `/dev/nvme0n1p3`, PITR off, 1 KiB values, and latency
sampling every ten operations. Use 200k puts per case except 5k for the
single-writer guard. SST target is 1 MiB for four-writer rotation, otherwise
1 GiB. Run five pairs per comparison and writer count, alternating arm order
and also alternating the order of the two comparisons within each pair.
Each arm starts with a fresh database path. Three current-parallel-versus-itself
pairs at 32 writers precede the matrix. All 126 runs complete successfully;
none are excluded. No builds, tests, or profiling overlap timing.

Ratios are medians of paired current/control ratios. Absolute throughput is
the median for each arm, which need not have the same ratio as the paired
median. CPU is user plus system process CPU reported by the benchmark, not
a sampled thread or wall-time measure. Throughput intervals are exploratory
95% percentile bootstrap intervals over pairs: 20,000 resamples, seed 24.
Five pairs provide limited precision, particularly with the observed device
swings; these intervals do not establish reproducibility across devices.

Cumulative comparison: old parallel WAL versus current parallel WAL.

| Writers | Old puts/s | Current puts/s | Throughput ratio | p99 ratio | CPU ratio | Throughput 95% interval |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 2,914 | 2,983 | 1.021 | 1.002 | 1.019 | 0.937–1.037 |
| 4 (rotation) | 9,876 | 6,407 | 0.646 | 2.762 | 1.010 | 0.454–1.001 |
| 8 | 18,607 | 18,438 | 0.997 | 1.020 | 1.019 | 0.980–1.000 |
| 16 | 28,571 | 28,601 | 0.999 | 1.002 | 1.007 | 0.464–1.004 |
| 32 | 34,248 | 40,416 | 1.184 | 0.549 | 0.699 | 1.147–4.141 |
| 64 | 16,671 | 46,642 | 2.798 | 0.270 | 0.162 | 2.754–7.814 |

Current implementation comparison: current leader WAL versus current parallel WAL.

| Writers | Leader puts/s | Parallel puts/s | Throughput ratio | p99 ratio | CPU ratio | Throughput 95% interval |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1,748 | 1,480 | 1.674 | 0.779 | 2.460 | 0.461–2.809 |
| 4 (rotation) | 2,496 | 9,881 | 3.959 | 0.121 | 0.968 | 0.980–5.688 |
| 8 | 7,012 | 18,439 | 2.632 | 0.444 | 1.222 | 0.804–6.207 |
| 16 | 13,214 | 28,706 | 2.164 | 0.503 | 1.197 | 0.752–2.908 |
| 32 | 24,280 | 39,935 | 1.652 | 0.616 | 1.066 | 1.563–1.705 |
| 64 | 38,731 | 46,940 | 1.216 | 0.934 | 1.197 | 1.209–1.333 |

The cumulative high-concurrency improvement is substantial: 64 writers have
2.798x throughput, 73.0% lower p99, and 83.8% lower CPU; 32 writers have 18.4%
higher throughput, 45.1% lower p99, and 30.1% lower CPU. The cumulative 8- and
16-writer cases offer no practical throughput gain. Against current leader,
32- and 64-writer throughput intervals exclude parity, with paired median gains
65.2% and 21.6%. Parallel uses 6.6% and 19.7% more CPU in those comparisons.

The four-writer cumulative result is a regression, not a pass: throughput
ratio 0.646 and p99 ratio 2.762. Its five throughput ratios are 1.001, 0.454,
1.000, 0.646, and 0.583. Three current arms cross slow intervals while their
old-parallel controls stay near 9.9k puts/s. The two quieter pairs are near
parity. This does not identify a causal retained change, but the regression
must not be dismissed or hidden behind the high-concurrency gains. The current
leader comparison's large four-writer median gain is also qualified: its
interval includes parity, and several leader arms cross slow intervals.

The same-binary null throughput ratios are 1.013, 0.961, and 0.382; p99 ratios
are 0.945, 1.052, and 7.326. The third unchanged-binary arm slows from 40.0k
to 15.3k puts/s. This demonstrates device/runtime variability without a code
change, but does not prove the candidate-specific slow intervals are harmless.
Single-writer paired results are particularly unstable: current-versus-leader
has a positive paired throughput median despite lower absolute parallel median
throughput, and its interval spans 0.461–2.809. Do not claim a reliable
single-writer improvement. Whole-device latency counters include unrelated
and out-of-window I/O and are contextual, not per-request attribution.

The cumulative comparison is not a final RFC adoption pass. The four-writer
regression and lower-concurrency uncertainty remain unresolved. This session
also does not rerun the pre-PITR control or a four-writer same-binary null;
its null is at 32 writers. Current leader remains the default. No runtime
changes are made by this comparison.

Artifacts: `target/rfc024-cumulative-comparison/` contains the current binary
snapshot, `metadata.json`, `run.py`, `run.log`, all 126 records in `runs.jsonl`,
`summarize.py`, and `summary.json` with individual paired ratios and intervals.
The historical binary remains in `target/rfc024-ready-experiment/baseline`.
Final checks verify report spelling and whitespace; no Rust tests are rerun
because this change only records measurements.


### Low-concurrency comparison rerun (2026-09-30)

Rerun the preceding comparison at 4, 8, and 16 writers after the user questioned
the results. Reuse the exact historical and current binaries and SHA-256 digests
from that comparison: `067d4b09` versus `925ed07d` runtime behavior. Keep ext4,
PITR off, release/`bench`, 200k puts, 1 KiB values, latency sampling every ten
operations, and fresh database paths. Four writers use the 1 MiB SST rotation
case; eight and sixteen use 1 GiB. No runtime changes are made.

Collect six pairs per case for both cumulative and current-leader comparisons,
plus three current-parallel-versus-itself null pairs at each writer count.
Alternate arm order and comparison order, rotate writer-count order between
rounds, and interleave null pairs in the first three rounds. All 90 runs finish
successfully; none are excluded. No builds, tests, or profiling overlap timing.
A host process snapshot shows no competing benchmark among the busiest
processes; it does not establish that the device is otherwise isolated.

Use the preceding paired-median ratio and exploratory bootstrap procedure
(20,000 resamples, seed 24). Absolute throughput values are arm medians;
ratios are medians of within-pair ratios. These can differ substantially when
slow intervals affect different arms. Six pairs and three null pairs are too
few for a precise variability estimate. Whole-device write-time counters
remain contextual and cannot attribute a stall to a particular request.

| Comparison | Writers | Control puts/s | Current puts/s | Throughput ratio | p99 ratio | CPU ratio | Throughput 95% interval |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| null | 4 (rotation) | 9,876 | 9,894 | 1.002 | 0.941 | 0.981 | 1.001–2.623 |
| cumulative | 4 (rotation) | 4,148 | 7,028 | 1.678 | 0.351 | 0.885 | 0.397–2.531 |
| leader | 4 (rotation) | 3,642 | 7,010 | 1.924 | 2.102 | 1.104 | 1.168–2.824 |
| null | 8 | 18,435 | 18,525 | 0.992 | 1.023 | 1.040 | 0.382–1.139 |
| cumulative | 8 | 18,669 | 18,554 | 0.990 | 1.019 | 1.029 | 0.726–2.516 |
| leader | 8 | 6,937 | 18,481 | 2.661 | 0.427 | 1.161 | 2.647–6.864 |
| null | 16 | 28,830 | 28,197 | 0.978 | 0.996 | 1.042 | 0.969–1.007 |
| cumulative | 16 | 28,494 | 28,535 | 1.000 | 0.967 | 1.007 | 0.982–1.741 |
| leader | 16 | 13,139 | 28,566 | 2.168 | 0.494 | 1.229 | 2.163–2.186 |

The four-writer cumulative paired median flips from 0.646 in the previous
session to 1.678 here, but its interval spans 0.397–2.531. Individual throughput
ratios are 0.846, 0.365, 2.540, 2.521, 0.429, and 2.511: both old and current
parallel binaries enter slow intervals. The unchanged-current null ratios are
1.002, 1.001, and 2.623. The third null arm changes from 3,772 to 9,894 puts/s
and from 9.92 to 1.31 ms p99 without a code change. Thus the earlier 35.4%
regression is not a repeatable estimate; neither is this session's apparent
67.8% gain. The four-writer result remains inconclusive and is not an adoption
pass. Device/runtime variability is demonstrated, but its cause is not
identified and candidate-specific bad runs must not be dismissed.

Eight writers remain near cumulative parity: throughput -1.0%, p99 +1.9%,
and CPU +2.9%. One old arm and one current arm run slowly, and the null ratios
span 0.382–1.139. Sixteen writers also remain near cumulative parity:
throughput effectively unchanged, p99 -3.3%, and CPU +0.7%. Five cumulative
pairs stay close to parity; the sixth old-baseline arm slows to 11,706 puts/s,
versus current at 28,900, widening the interval. No reliable cumulative
low-concurrency throughput gain is established.

Current parallel is faster than current leader at eight and sixteen writers:
paired throughput gains are 166.1% and 116.8%, p99 reductions 57.3% and 50.6%,
and CPU increases 16.1% and 22.9%. Their throughput intervals exclude parity
in this session. These compare WAL implementations, not the incremental
benefit of the recent retained changes. Four-writer parallel-versus-leader
throughput improves 92.4%, but paired p99 is 110.2% worse; three pairs have
higher parallel p99. The throughput result alone cannot clear the gate.

Do not replace the original observations with this rerun or select only quiet
pairs. Both sessions remain recorded. Adoption remains unresolved; current
leader remains the default. Further investigation should isolate the source
of the intermittent ext4 stalls before assigning four-writer gains or
regressions to a retained change. This rerun does not measure the pre-PITR
baseline or the single-writer guard.

Artifacts: `target/rfc024-low-concurrency-confirmation/` contains binary
provenance in `metadata.json`, `run.py`, `run.log`, all 90 records in
`runs.jsonl`, `summarize.py`, and `summary.json` with individual pair ratios.
It reuses the binaries in the preceding comparison. Final checks verify
spelling and whitespace; no Rust tests are rerun for this measurement-only
report.
