# RFC 024: Parallel WAL benchmark outcomes

**Decision:** Keep the client-leader WAL as the default. Removing the parallel
path's dedicated packer thread improved it on tmpfs and left ext4 throughput
near parity with the previous parallel implementation. The current candidate
still loses to the leader on tmpfs, so parallel WAL remains opt-in.

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
