# V7 / V8 durability critical-path test

**Result:** 24/24 planned runs completed and passed local verification. This covered 15 unprofiled score runs (five counterbalanced repetitions per arm) and nine separate diagnostic runs (one sync-phase run, one syscall trace, and one block/writeback trace per arm).

- Source commit: `47cc03aa352a7e5d124785eec2c28f2442c69a33`
- Temporary diagnostic patch SHA-256: `211f4e672385eb93cbccd2c2b03199b177d2991af7e341d55d3c3634f440fabd`
- Result archive SHA-256: `799574c33fdbb2a53a658320aec7d4d6c53bdeb188b01a6394df9aacd05e7671`
- The raw result archive and checksum metadata are retained locally and are not included in this PR.

## Environment and test setup

GCP `c4-standard-16-lssd`, two 375 GiB Local SSD devices in RAID0 (749.7 GiB ext4 at `/mnt/gcp-wal-bench`), Linux `7.0.0-1017-gcp`, 16 logical CPUs / 8 cores / 2 threads per core. Mount options: `rw,noatime,stripe=256`.

All score runs used 16 writers, 100,000 Point puts, 1 KiB values, V8 1 MiB sliding `fallocate`, and dynamic V8 coalescing. The three arms were V7 Parallel, V8 Buffered/Dynamic, and V8 Direct/Dynamic. Scores had latency sampling every 100 puts, but no BPF, detailed sync diagnostics, or phase sampler. Each diagnostic arm ran separately; diagnostic throughput is not a score.

## Throughput results

Medians are across five runs. Latency columns are the median of each run's sampled percentile. Paired changes compare runs with the same repetition number.

| Arm | puts/s | p50 / p95 / sampled p99 (ms) | CPU / put (µs) | Syncs / 100k puts |
| --- | ---: | ---: | ---: | ---: |
| V7 Parallel | 48,864 | 0.297 / 0.549 / 0.680 | 59.86 | 8,278 |
| V8 Buffered / Dynamic | 31,341 | 0.501 / 0.656 / 1.063 | 69.59 | 6,335 |
| V8 Direct / Dynamic | 33,445 | 0.460 / 0.703 / 0.908 | 72.71 | 8,051 |

| Paired comparison | Median change | Range | Wins |
| --- | ---: | ---: | ---: |
| V8 Buffered vs V7 | −35.14% | −36.77% to −31.52% | 0/5 |
| V8 Direct vs V7 | −31.31% | −32.08% to −25.74% | 0/5 |
| V8 Direct vs V8 Buffered | +6.71% | +5.91% to +8.44% | 5/5 |

Direct/Dynamic consistently improved throughput over Buffered/Dynamic, but V8 remained about 31% behind V7. Direct/Dynamic also used about 27% more syncs than Buffered/Dynamic and about 4.5% more CPU per put. The sampled p95 moved in the wrong direction for Direct, while p50 and sampled p99 improved; treat the tail percentiles cautiously.

## Per-sync I/O and group formation

These are one-run detailed diagnostics per arm, not score runs. `write + fdatasync` is calculated per observed sync and then summarized; it is not the sum of separate stage percentiles. DATA bytes are framed WAL bytes, not logical value bytes.

| Arm | Sync observations | Tickets / sync (p50, mean) | DATA bytes / sync (p50) | FRONTIER write p50 / p95 (µs) | fdatasync wall p50 / p95 / p99 (µs) | fdatasync thread CPU p50 (µs) | Same-sync write + sync p50 / p95 / p99 (µs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| V7 | 8,254 | 15 / 12.12 | 60 KiB | 34.42 / 47.57 | 1.26 / 110.27 / 128.57 | 1.20 | 36.22 / 151.16 / 183.11 |
| V8 Buffered / Dynamic | 6,334 | 16 / 15.79 | 64 KiB | 3.19 / 5.99 | 161.17 / 185.58 / 200.15 | 22.40* | 164.73 / 190.14 / 207.42 |
| V8 Direct / Dynamic | 8,019 | 16 / 12.47 | 64 KiB | 40.83 / 54.40 | 100.93 / 120.55 / 141.52 | 9.41* | 142.00 / 170.81 / 268.23 |

`*` V8 fdatasync CPU is from the separate phase sample (98 Buffered and 125 Direct advances, sampled every 64 successful advances); the per-sync observation field is intentionally zero for unsampled cycles.

Across the diagnostic run, cumulative FRONTIER-write-plus-fdatasync wall time was 430 ms for V7, 1,043 ms for V8 Buffered, and 1,158 ms for V8 Direct. These totals are within-run sums of serial per-sync stages, not end-to-end benchmark time. Direct reduced the median cost of one write+sync pair by about 14%, but it performed more advances and had a smaller mean group, so its cumulative measured I/O time was higher than Buffered in this single diagnostic run.

## Full-cycle, lock, and wait measurements

V8 phase samples cover one completed durability advance each. V7 cycle timing starts after the written target is selected and ends after sync, frontier completion, durable-state publication, and `mark_durable`. V8 driver timing starts just before acquiring the frontier mutex and ends after post-publish retirement. These boundaries are close enough to locate costs, but not perfectly identical.

| Metric (p50 / p95, µs) | V7 | V8 Buffered / Dynamic | V8 Direct / Dynamic |
| --- | ---: | ---: | ---: |
| Full cycle / driver wall | 59.93 / 176.69 | 432.79 / 554.24 | 358.92 / 501.51 |
| Full cycle / driver thread CPU | 30.34 / 46.07 | 162.00 / 206.48 | 129.97 / 207.18 |
| Driver CPU per covered ticket, computed per sample | 2.31 / 13.23 | 10.21 / 12.91 | 10.70 / 24.65 |
| V8 frontier-lock wait | — | 0.66 / 2.88 | 0.81 / 3.71 |
| V8 frontier-lock hold | — | 432.16 / 553.74 | 356.80 / 500.08 |
| V8 durable publish to frontier unlock | — | 25.28 / 83.70 | 25.14 / 79.02 |
| Coalescing wall | — | 173.23 / 274.17 | 126.21 / 242.36 |
| Coalescing thread CPU | — | 55.36 / 98.06 | 37.51 / 91.79 |
| Condvar wait/reacquire thread CPU | — | 50.24 | 31.71 |
| DATA completion to FRONTIER start | 23.17 / 85.63 | 22.16 / 33.24 | 25.11 / 184.96 |

V8 sampled `tickets_covered` had p50 16 for both arms; its mean was 15.87 Buffered and 12.18 Direct. Pre-publish / post-publish retirement CPU p50 was 34.52 / 23.41 µs Buffered and 32.41 / 21.79 µs Direct. V7 does not have matching resource-retirement substage timings.

## Kernel traces

The syscall histogram was captured in a separate BPF run. It was system-wide rather than PID-filtered. The tracer printed the same histogram twice on shutdown; analysis used one copy, whose bucket count matched the syscall counter.

| Arm | fdatasync calls observed | Mean (µs) | Histogram p50 / p95 / p99 lower bucket (µs) | Calls below 2 µs |
| --- | ---: | ---: | ---: | ---: |
| V7 | 8,272 | 17.44 | 1 / 109 / 146 | 73.5% |
| V8 Buffered / Dynamic | 6,323 | 171.75 | 164 / 258 / 415 | 0% |
| V8 Direct / Dynamic | 8,111 | 101.01 | 97 / 120 / 314 | 0% |

Block tracepoints were also system-wide and not filtered by device, inode, or process. Counts were:

| Arm | Request issues / completions | Issued / completed sectors | writeback start / written / wait |
| --- | ---: | ---: | ---: |
| V7 | 86,121 / 86,042 | 1,782,600 / 1,781,544 | 0 / 0 / 0 |
| V8 Buffered / Dynamic | 99,434 / 99,355 | 1,090,704 / 1,089,936 | 7 / 7 / 0 |
| V8 Direct / Dynamic | 107,690 / 107,619 | 1,146,288 / 1,145,552 | 7 / 7 / 0 |

The seven V8 writeback events do not prove that those events belonged to the tested WAL file. The V8 sync descriptor remains buffered even in the Direct FRONTIER arm, so the test does not isolate sync-descriptor flags from inode/writeback state.

## Interpretation

1. **Disk sync latency is a real contributor, but not the whole V8 gap.** Kernel and in-process measurements both show much slower V8 fdatasync distributions. Direct FRONTIER lowers V8 fdatasync p50 from about 161 to 101 µs, but V8 Direct still trails V7 by 31% in score throughput.
2. **The measured wait is mainly coalescing, not frontier-mutex contention.** V8 coalescing costs 126–173 µs wall p50. Frontier-lock wait stays below 1 µs p50 and below 4 µs p95. The lock is held for most of the driver cycle, but measured lock acquisition delay is small. Publish-to-unlock is about 25 µs p50 and is a possible cycle-tail cost, not a demonstrated throughput bottleneck.
3. **V8 also spends more thread CPU per durability cycle.** The sampled V8 driver CPU p50 is 130–162 µs versus 30 µs for the V7 timer. V8 CPU per covered ticket is about 10–11 µs p50 versus 2.3 µs for V7. Cycle boundaries and instrumentation differ, so this is directional evidence; the lower-overhead score independently shows V8 Direct at about 21% more CPU per put than V7.
4. **Dynamic coalescing changes with FRONTIER mode.** Buffered reaches a mean group of 15.79 tickets; Direct drops to 12.47 and performs more syncs. Faster syncs therefore do not translate into equally fewer total cycle costs under the same dynamic policy.
5. This run did not capture follower-resume-to-WAL-return or WAL-return-to-MVCC completion, and did not use off-CPU stack tracing. Those remain separate questions; no MVCC or notification changes are justified by this result alone.

## Retention

The archive checksum (`799574c33fdbb2a53a658320aec7d4d6c53bdeb188b01a6394df9aacd05e7671`), complete summary (`complete=true`, 24 planned and completed), `remote-exit.json` (`failed=false`, 24 runs started), and every planned run's result/metadata pair all verify. The raw archive, checksum metadata, and extracted results are retained locally and are not included in this PR. Four earlier continuations aborted before starting any workload (temporary `cp`/metadata issues, a compile-scope error, and an unprivileged tracefs read); their logs and incomplete archives remain outside the verified result set. After collection, no cloud cleanup was initiated; the VM was left under its originally configured server-side 60-minute deletion deadline. At the time results were collected, no PR edits had been made.
