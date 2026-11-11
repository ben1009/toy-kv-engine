# RFC 026 V7/V8 CPU Affinity Results

## Summary

The initial affinity experiment showed a V8 gain when pinning persistent WAL
helpers, but it also changed io_uring worker placement. A V8-only 2×2 follow-up
separated those controls: pinning io-wq workers to CPU 8 alone raised median
throughput by 30.0%, while pinning WAL helpers alone changed it by 0.2%.
Pinning both raised throughput by 19.4%, below io-wq-only pinning by 8.2%.

The results make io-wq placement the strongest lead in this 16-writer Point
configuration. The io-wq-only arm reduced migrations and Sync count without
reducing mean `fdatasync` latency, so the gain is not explained by faster Sync
syscalls. This single-CPU placement result does not establish a general
placement policy or justify a dedicated durability-driver thread.

## Method

- Host: GCP `c4-standard-16-lssd`, `us-east1-b`; 16 logical CPUs arranged as
  eight SMT sibling pairs: `0,8` through `7,15`.
- Storage: two Local SSDs in RAID0, ext4 mounted with `noatime,stripe=256`.
- Workload: `wal_concurrent`, PITR V7 or V8 Parallel, 100,000 Point puts per
  run, 1 KiB values, no compaction, 2 GiB target SST size.
- V8 used Direct FRONTIER and a fixed 400 μs coalescing budget. In every scored
  V8 run, metadata confirmed DATA and FRONTIER were `O_DIRECT`, Sync was
  buffered, and all three handles referred to a stable inode.
- Three placement arms: A `none` (no affinity changes), B `wal` (pin persistent
  WAL helpers), and C `wal_writers` (pin helpers and every writer). Both V7 and
  V8 ran at 8 and 16 writers, with five counterbalanced repetitions per cell:
  60 scored runs total.
- Each arm used the same affinity capture and synchronized start barrier; the
  timer began after thread placement. No broad profiler or per-sync diagnostics
  were enabled. `perf stat` collected process CPU migrations and context
  switches. Because `perf_event_paranoid=4`, both perf and its benchmark child
  ran as root; the sysctl was not changed. Aggregate WAL counters and sampled
  latency remained enabled.
- Source commit: `a2c35377`. Binary SHA-256:
  `08352f8ca3eac0472d598a0e679d09908fc941293412a9f82f4b3768d9f7b5c6`.
  A six-case 8-writer smoke matrix passed before scoring. The scored matrix
  validated all 60 runs and exactly 100,000 operations per run.

## Throughput

Values are median puts/s across five runs. Parentheses show the median of the
five paired percentage changes against that format's no-pin arm; the paired
range is included to show run-to-run consistency.

| Writers | WAL | A: No pin | B: WAL helpers pinned | C: WAL helpers + writers pinned |
| ---: | :--- | ---: | ---: | ---: |
| 8 | V7 | 33.25k | 33.93k (+2.43%; +1.23% to +5.48%) | 33.95k (+2.83%; +0.57% to +6.69%) |
| 8 | V8 | 14.57k | 15.18k (+4.21%; +1.60% to +7.57%) | 15.51k (+6.45%; +4.93% to +7.99%) |
| 16 | V7 | 47.33k | 47.89k (+0.74%; -0.63% to +1.81%) | 47.54k (+1.55%; -0.19% to +4.31%) |
| 16 | V8 | 20.41k | 24.92k (+21.01%; +15.75% to +25.06%) | 24.23k (+17.70%; +14.56% to +25.11%) |

At 16 writers, all five V8 helper-pinned runs beat their paired no-pin runs.
The median V8 p50 fell from 0.763 ms to 0.608 ms in B and 0.632 ms in C.
Sampled p99 was noisy: the V8 medians were 2.715 ms, 2.122 ms, and 2.921 ms
for A, B, and C respectively, so this does not establish a stable tail-latency
improvement.

## CPU and WAL counters

Medians per run:

| Writers / WAL | CPU migrations A / B / C | Context switches A / B / C | Process CPU seconds A / B / C |
| :--- | ---: | ---: | ---: |
| 8 / V7 | 866 / 76 / 27 | 260k / 259k / 268k | 4.09 / 4.03 / 4.16 |
| 8 / V8 | 10,674 / 89 / 24 | 713k / 849k / 852k | 9.54 / 7.54 / 7.72 |
| 16 / V7 | 39,962 / 25,387 / 55 | 341k / 349k / 345k | 6.08 / 5.86 / 5.22 |
| 16 / V8 | 104,459 / 18,541 / 67 | 765k / 820k / 830k | 11.77 / 8.38 / 7.87 |

For V8 at 16 writers, B reduced migrations by 82% and process CPU by 29% while
raising median paired throughput by 21%. C reduced migrations by more than
99.9%, but its median throughput was 2.7% below B. Pinning clearly reduces migrations, but the
remaining context switches did not fall; at 16 writers they rose slightly in
both pinned V8 arms.

V8 WAL counters also changed with placement. At 16 writers, median Sync count
fell from 7,882 in A to 6,908 in B and 6,965 in C. Eventfd writes fell from
38,940 to 31,380 and 30,591 respectively. At 8 writers, Sync count stayed
nearly constant while eventfd writes fell. These counters suggest that
placement changed coordination and group formation as well as CPU migration.

## Interpretation and limits of the initial experiment

The affinity arms changed io_uring worker placement too. In A, every observed
`iou-wrk-*` thread was allowed CPUs 0–15. In B and C, every observed io_uring
worker was allowed only CPU 8, matching the pinned WAL I/O worker. This was not
an independently controlled variable, so the throughput gain cannot be
attributed to userspace CPU locality alone.

At 8 writers, helper CPUs 8–11 (V7) or 8–9 (V8) were SMT siblings of writer
CPUs 0–7. At 16 writers, C pinned writers across CPUs 0–15, so helpers shared
the same logical CPUs 8–11 (V7) or 8–9 (V8) with writers. This makes C a
placement stress case rather than a dedicated-core comparison, and may explain
why it failed to beat B at 16 writers.

The results support a controlled follow-up that pins WAL helpers while
explicitly controlling io_uring worker masks. They do not yet show whether the
benefit comes from helper locality, io_uring placement, changed group
formation, or their interaction. Since C did not outperform B at 16 writers,
there is not yet evidence that pinning the inline durability driver through its
writer thread adds throughput beyond pinning the persistent WAL helpers. A
fixed-driver-thread experiment should wait until these effects are separated.

## Follow-up: separate WAL-helper and io-wq affinity

This V8-only 2×2 experiment separates persistent helper placement from the
io-wq CPU mask. It used the same `c4-standard-16-lssd` host, Local SSD RAID0,
ext4, 16 writers, 100,000 Point puts per run, 1 KiB values, Direct DATA and
FRONTIER, buffered Sync, fixed 400 μs coalescing, and no compaction. The
benchmark ran with broad profiling and per-Sync diagnostics disabled; sampled
latency was collected every 100 operations, and `perf stat` recorded CPU
migrations and context switches. Five counterbalanced runs were completed per
arm using source commit `0ec10d6f`.

The harness used 32 synchronized warmup callers for 32,768 warmup puts. Each
run had at least 27 live io-wq workers before affinity changes; the final
affinity snapshot verified every surviving io-wq worker against the requested
mask. All 20 measured JSON records confirmed stable WAL handles on the Local
SSD inode, with DATA and FRONTIER `O_DIRECT` and Sync buffered.

| Arm | Persistent WAL helpers | io-wq allowed CPUs | Median puts/s (range) | p50 ms | sampled p99 ms |
| :--- | :--- | :--- | ---: | ---: | ---: |
| A | unpinned | 0–15 | 20.69k (20.47–20.83k) | 0.758 | 1.079 |
| B | I/O worker 8, completion reaper 9 | 0–15 | 20.72k (20.71–20.77k) | 0.757 | 1.245 |
| C | unpinned | CPU 8 | 26.89k (26.49–27.04k) | 0.579 | 0.838 |
| D | I/O worker 8, completion reaper 9 | CPU 8 | 24.70k (24.64–24.94k) | 0.619 | 1.153 |

Supporting counters are medians per run:

| Arm | Syncs | mean `fdatasync` μs | process CPU s | CPU migrations | context switches | eventfd writes |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 7,831 | 106.1 | 11.70 | 216,526 | 1,105,973 | 38,470 |
| B | 7,840 | 106.4 | 11.46 | 195,573 | 1,115,820 | 42,000 |
| C | 6,328 | 106.6 | 8.84 | 104,855 | 1,006,208 | 35,127 |
| D | 6,908 | 114.4 | 8.29 | 87,988 | 1,127,258 | 32,429 |

`p50` and sampled `p99` are medians of each arm's five per-run latency
percentiles. Process CPU is the median timed process CPU per run.

Against A, B changed throughput by +0.2% despite 9.7% fewer migrations.
Compared with A, C raised throughput by 30.0%, reduced migrations by 51.6%,
and reduced process CPU by 24.5%. Its Sync count fell by 19.2%, while mean
`fdatasync` duration stayed effectively unchanged (106.1 to 106.6 μs). C's
results are consistent with improved io-wq locality and changed group
formation; the experiment cannot separate those effects. Context switches
fell by 9.0% in C, far less than migrations, and were slightly higher in B and
D than A.

D was 8.2% slower than C even though it further reduced migrations. The
I/O worker shares CPU 8 with all io-wq workers in D, so CPU contention is one
possible explanation, but this test does not establish that cause. The sampled
p99 medians improved in C and worsened in D; they remain secondary to the
throughput result because each arm has five runs and p99 is sampled.

The next useful experiment is to repeat the io-wq-only arm on another CPU
selected from the C4 topology and include a matched V7 control. Keep the WAL
helpers unpinned while varying only the io-wq mask. Do not change production
defaults or introduce a dedicated driver based on this single CPU placement.

Raw JSON, affinity snapshots, and perf outputs are retained locally and were
not added to GitHub. The Terraform-managed VM, firewall, subnet and network were
destroyed after collection; Terraform state and GCP resource-list checks were
empty afterward.
