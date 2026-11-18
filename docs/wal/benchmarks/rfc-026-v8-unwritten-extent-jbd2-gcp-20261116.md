# GCP V8 unwritten extent / JBD2 diagnostic

Date: 2026-11-16 UTC

Source commit: `760acb1ff09cb41041c39289e00d97cf126c60bc`

Host: c4-standard-16-lssd; kernel `7.0.0-1017-gcp`; ext4 on /dev/md127, mount `rw,noatime,stripe=256`.

Workload: 100,000 Point puts, 16 writers, 1 KiB values, PITR Parallel WAL.

## Result

The inode-associated traces support a connection between V8's fallocate-only path, ext4 unwritten-extent handling, and JBD2 waits during WAL `fdatasync`. They do not establish that this mechanism explains the full V7/V8 throughput gap.

| Diagnostic arm | Diagnostic puts/s* | Runtime syncs | ext4 unwritten-handler events on WAL inode | JBD2 waits / ext4 sync entries (original probe) | JBD2 wait wall total / average |
|---|---:|---:|---:|---:|---:|
| V7 native | 42.54k | 8,297 | 3,393 | 1,124 / 8,298 (13.55%) | 143.100 ms / 127.3 μs |
| V8 fallocate-only | 29.08k | 6,336 | 112,673 | 6,338 / 6,338 (100.00%) | 683.403 ms / 107.8 μs |
| V8 initialize 512 MiB | 41.79k | 9,713 | 4,101 | 2 / 9,715 (0.02%) | 1.037 ms / 518.3 μs |

*Single-run BPF diagnostics, not formal throughput scores. Initialization took 2.301 s for 512 MiB, before the measured workload (2.393 s). Including both phases yields about 21.30k puts/s, below fallocate-only. This is not a production recommendation.

The ext4 sync-entry counts differ from runtime sync counters by one or two events; they are reported separately. These historical JBD2 counts came from the original probe, which filtered to the benchmark PID and associated waits with the most recent ext4 inode context in the current `fdatasync` syscall, but did not require the wait to occur between `ext4_sync_file_enter` and `ext4_sync_file_exit`. The counts therefore are not the strict active-sync classification emitted by the revised probe. Matching aggregate totals do not establish one wait per individual sync. All unique transaction IDs captured by the original inode-associated JBD2 probe had matching `jbd2_end_commit` events: V7 1124/1124, V8 fallocate 4024/4024, and V8 initialize 2/2.

V8 fallocate recorded 112,673 unwritten-handler events on its active WAL inode, split across raw flag values `1073742883` (6,336 events) and `1073742899` (106,337 events); flags are retained without semantic decoding. V8 initialization recorded 4,101 events over 2.302 s, matching its preparation interval. V7 recorded 3,393 events on the active WAL inode; the trace does not attribute those events to a userspace thread.

## Separate per-syscall trace

This was a separate run that printed one eBPF event per `fdatasync`. It adds diagnostic overhead and is only a latency-distribution cross-check, not throughput data.

| Arm | syscall events | p50 | p95 | p99 | mean | calls under 2 μs |
|---|---:|---:|---:|---:|---:|---:|
| V7 native | 9,056 | 4.0 μs | 127.4 μs | 1518.6 μs | 53.9 μs | 0 (0.00%) |
| V8 fallocate-only | 6,338 | 171.8 μs | 312.2 μs | 545.4 μs | 194.2 μs | 0 (0.00%) |
| V8 initialize 512 MiB | 10,023 | 41.7 μs | 130.5 μs | 177.8 μs | 49.1 μs | 0 (0.00%) |

This particular per-call trace captured no V7 calls below 2 μs; do not generalize this result or substitute it for other runs' histograms. It does show a much lower V7 median than V8 fallocate-only and a long V7 tail. Each arm was one instrumented run.

## Interpretation and limits

- The original probe reported 6,338 V8 fallocate-only JBD2 waits for 6,338 ext4 sync entries; V7 had 1,124/8,298 (13.5%); V8 initialization had 2/9,715. These legacy counts were not gated on the active `ext4_sync_file` interval and cannot be split retrospectively into correlated and uncorrelated waits. Use the revised probe for that distinction. The original inode-associated wait totals were 683.4 ms, 143.1 ms, and 1.0 ms, respectively. These are diagnostic waits, not isolated device service time.
- The ext4 unwritten-handler tracepoint includes device and inode. The JBD2 wait probe captures transaction IDs, all of which matched an end-commit event on the device. This is stronger than same-device temporal overlap, but it does not prove every V8 wait was caused solely by unwritten-extent conversion; other inode metadata, scheduling, and changed durability grouping may contribute.
- The first probe, `ext4_ext_convert_to_initialized_enter`, produced no events on this kernel/path. The follow-up used `ext4_ext_handle_unwritten_extents`, which did capture inode-correlated events. The empty first probe is not evidence of zero conversions.
- Initialization reduced JBD2 waits during measured WAL syncs but cost about 2.3 seconds up front and changed sync grouping. Its end-to-end one-shot throughput was lower than fallocate-only, so this is mechanism evidence, not a proposed optimization.
- These are single diagnostic runs; BPF can perturb timing. Do not interpret the diagnostic throughput as a benchmark score.

## Reproducing the kernel trace

The inode/JBD2 aggregate probe is retained at [`scripts/gcp_v8_unwritten_jbd2.bt`](../../../scripts/gcp_v8_unwritten_jbd2.bt). From the repository root, run it with the benchmark process ID, for example `sudo bpftrace scripts/gcp_v8_unwritten_jbd2.bt 12345 | tee trace.txt`. It emits aggregate maps at exit. `@syscall_unmatched_count` counts `fdatasync` calls without a matching ext4 sync entry. `@wait_correlated_count` counts JBD2 wait starts inside the active ext4 sync interval for a target `fdatasync`; the per-inode wait counts, histograms, and transaction IDs are recorded when those waits return. `@wait_unmatched_count` and `@wait_unmatched_tx` separately count target-process JBD2 wait starts outside that interval, with the unmatched histogram and total time recorded on return. Join matched transaction IDs against `@commit_end_ns` using device and transaction keys; do not interpret equal aggregate wait and sync totals as a one-to-one pairing. The revised classification has not been rerun for the historical results above. Kernel tracepoint fields and function probes vary by kernel, so verify availability before running.

## Validation and artifacts

All 6 planned runs have successful `result.json` and matching `run-metadata.json` in their own directories. `summary.json` reports complete 6/6; `remote-exit.json` reports no failures and 6 starts/completions. Three transferred result archives and their retained checksum sidecars passed SHA-256 verification. The source commit and workspace/vendor archive hashes remain in the local summary.

The report is committed here; run-level JSON, traces, and checksum-verified archives remain in the local benchmark result directory and are not uploaded to GitHub. No manual cloud cleanup or `terraform destroy` was run. The VM is now absent because its configured 60-minute server-side lifetime expired; the isolated VPC, subnet, three narrowly scoped SSH rules, and local Terraform state remain. The VM Local SSD disappeared with the instance, while the complete verified result set is retained locally.

## Follow-up: revised probe and sliding-initialization A/B (2026-11-16)

The probe fixes in source commit `f9b14572b8c378b83b6af65981071357c6698c61` were checked on the same C4/ext4 setup. The checked-in probe SHA-256 was `0e369ca0cdfa1d8535b4a4eadedf094550a0aa659655a5cdd938039bd57eadcf`. The installed bpftrace 0.20.2 did not accept `--dry-run`; syntax and attach checks used `-d` and a short `-c /bin/true`. Actual short benchmarks were launched with `-c` and a temporary `cpid`-substituted probe to avoid a PID-attach race.

The first V8 Fallocate trace hit the default 4,096-key map cap and is excluded. The retry used `BPFTRACE_MAX_MAP_KEYS=16384`. The following are three qualified, one-run diagnostics; diagnostic throughput is not a clean score.

| Arm | Runtime WAL syncs | fdatasync syscalls | WAL-inode calls | Unmatched syscalls | Correlated JBD2 wait starts | Unmatched wait starts | Completed WAL-inode waits | WAL-inode wait transactions joined to commit-end | WAL-inode unwritten-handler events |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| V7 Parallel | 8,599 | 8,600 | 8,599 | 0 | 918 | 51 | 917 | 917/917 (all inodes 918/918) | 3,401 |
| V8 Buffered + Sliding Fallocate 1 MiB | 6,318 | 6,318 | 6,318 | 0 | 6,318 | 75 | 6,318 | 6,318/6,318 | 112,637 |
| V8 Sliding Initialize 1 MiB | 9,596 | 9,596 | 9,596 | 0 | 3,439 | 75 | 3,439 | 3,439/3,439 | 3,426 |

The syscall counts reconcile: V8 totals match runtime sync counts; V7 has one additional ext4-associated syscall on a second inode, while its WAL inode has exactly 8,599. `@syscall_unmatched_count` is zero for every qualified arm. Correlated and unmatched JBD2 waits remain separate; waits are not assumed to be one-per-sync. All completed correlated `(device, transaction)` keys joined to `@commit_end_ns`. In this sample, Sliding Initialize sharply reduced unwritten-handler activity and correlated JBD2 waits versus Fallocate. This supports the extent/JBD2 mechanism but does not prove exclusive causality or explain the full throughput gap.

### Untraced throughput follow-up

Both score matrices used five counterbalanced blocks of V7, V8 Sliding Fallocate 1 MiB, and an initialization configuration. Each suite completed 15/15 runs; all measured initialization costs were included. The benchmark source remained at `f9b14572` except for the isolated temporary candidate patch described below.

| First matrix | Median puts/s | CPU μs/put | Syncs | Puts/sync | Average fdatasync |
|---|---:|---:|---:|---:|---:|
| V7 Parallel | 48.45k | 59.23 | 8,209 | 12.18 | 15.88 μs |
| V8 Sliding Fallocate 1 MiB | 31.56k | 70.23 | 6,345 | 15.76 | 162.35 μs |
| V8 existing Sliding Initialize 1 MiB | 33.64k | 79.57 | 9,577 | 10.44 | 75.68 μs |

Existing Sliding Initialize beat Fallocate in all five matched blocks, with a median paired gain of 6.72%, but remained 30.85% behind V7. Median preparation initialized 448.9 MB over 429 `fallocate` calls; initializer time was 274.2 ms and cumulative producer wait was 390.4 ms across threads.

The second matrix tested a temporary benchmark-only full-window lookahead patch. It requests preparation through the full 1 MiB window while a DATA writer waits only for its own required prefix, allowing initialization to run ahead without delaying that writer for the remaining suffix. Patch SHA-256: `74beffad8e90a295d5a3464e4b5db66d8942d696211ce2c780ea65a3a2adb9f5`; candidate binary SHA-256: `fa0163fa77bfc69943bbfeb45475720251e77b995a4822fc0fc21958e49702ce`.

| Full-window candidate matrix | Median puts/s | CPU μs/put | Syncs | Puts/sync | Average fdatasync |
|---|---:|---:|---:|---:|---:|
| V7 Parallel | 46.85k | 61.45 | 8,286 | 12.07 | 16.45 μs |
| V8 Sliding Fallocate 1 MiB | 30.54k | 70.37 | 6,327 | 15.81 | 170.52 μs |
| V8 full-window Initialize prototype | 38.21k | 79.69 | 9,823 | 10.18 | 52.98 μs |

The prototype beat Fallocate in all five matched blocks (+25.35% median paired), but remained 17.16% behind V7. Relative to Fallocate, CPU per put rose 13.2%, sync count rose 55.3%, and average `fdatasync` fell 68.9%. Median initialization was 449.8 MB, with 301.7 ms initializer time and 67.1 ms cumulative producer wait across threads. Since sync count and grouping also changed, the throughput gain cannot be attributed to `fdatasync` alone.

Separate one-run latency diagnostics sampled 1,008 operations per arm. V7 was 0.307/0.609/1.223 ms p50/p95/p99; Fallocate 0.493/0.672/2.489 ms; existing Initialize 0.455/0.804/3.955 ms; the prototype 0.350/0.978/2.488 ms. These samples are too few for stable tail-latency claims; the prototype p95 was higher than Fallocate in this run. Clean score runs did not sample operation latency. Their output includes average `fdatasync` time, while the separate BPF histograms provide only coarse syscall percentile buckets.

### Follow-up limits and retained results

At the time of these prototype matrices, the lookahead implementation was a temporary benchmark patch. The per-run archives, traces, and patch were retained locally and deliberately not added to GitHub; their archive SHA-256 values are `fe9ffc3bcb33a55fb41c6a3d049c4c2e751560a62db32c966d645b17c66d229d` (existing-initialization matrix) and `acbc6fbc73ac9f244df200ac1ab07dd69240de133bed7e9000d7421cd8b53808` (lookahead prototype matrix). The revised-probe archive SHA-256 is `fb56556d6b19ed5c0d5fb52fe2018b937bc8726ea1eff01f25d4116f7294a6f1`.

For these follow-up runs, the local `remote-exit.json` was not retained before the VM's configured lifetime elapsed. The archives and per-run results were checksum-checked, but that missing runner record leaves the strict cleanup verification incomplete. No manual cloud cleanup or `terraform destroy` was performed; local Terraform state and network configuration were retained. No archive was uploaded and no PR was merged.

### Default-off runtime candidate

The PR branch now contains a quota-aware full-window lookahead candidate. Production use is explicitly opt-in with `TOYKV_WAL_V8_EXTENT_LOOKAHEAD=1`; unset, `0`, and `false` preserve the existing production behavior. The option is accepted only when the Active WAL has a reservation owner, an ext4 filesystem, and an `O_DIRECT` DATA worker. The production lookahead window remains 1 MiB. The benchmark-only equivalent is `TOYKV_BENCH_V8_EXTENT_PREP=sliding-lookahead` with `TOYKV_BENCH_V8_EXTENT_PREP_BYTES` set to the window size.

The worker reserves quota for the speculative suffix before `fallocate`, initializes it in aligned 128 KiB chunks, and publishes only the initialized prefix. DATA and FRONTIER writers wait only until their required end is initialized. A preparation error is sticky and wakes waiters; quota shortage falls back to the required prefix. Reclamation serializes with the worker before truncating and releasing speculative quota. The default remains off.

## Runtime candidate qualification on GCP (2026-11-16)

The default-off runtime candidate from source commit `7554d43f635d32c7acaad105d87111377253c0e9` was tested on the same `c4-standard-16-lssd` host, kernel `7.0.0-1017-gcp`, and ext4 Local SSD RAID0 mount (`rw,noatime,stripe=256`). The benchmark binary used the `bench` feature. Its source included the fixture-limit correction and benchmark-only wait-counter instrumentation; the complete source patch SHA-256 is `6e800bdabafa0035801755e747454586a466c578228d51d1fc27a718b98f8f54`. The patch changes benchmark diagnostics and test fixture capacity only; it does not change the production (non-bench) behavior. The wait counter measures calls to `ensure_prepared_through` that enter its condition-variable wait loop; elapsed time includes sleep and mutex reacquisition.

The unprofiled score comparison used five counterbalanced blocks, each containing V7, V8 Sliding Fallocate 1 MiB, and V8 Sliding Lookahead 1 MiB. Every arm ran 100,000 Point puts with 16 writers and 1 KiB values. No `perf`, eBPF, operation-latency sampling, or sync/HOL timeline tracing was enabled. The benchmark's aggregate WAL counters and the new preparation-wait counters remained enabled, so these are lightly instrumented measurements rather than zero-instrumentation scores.

| Configuration | Median puts/s | CPU μs/put | Syncs / 100k puts | Puts/sync | Mean `fdatasync` |
|---|---:|---:|---:|---:|---:|
| V7 Parallel | 48.12k | 59.80 | 8,243 | 12.13 | 16.14 μs |
| V8 Sliding Fallocate 1 MiB | 30.91k | 71.39 | 6,327 | 15.81 | 168.74 μs |
| V8 Sliding Lookahead 1 MiB | 39.74k | 78.80 | 9,779 | 10.23 | 50.37 μs |

Lookahead beat Fallocate in all five matched blocks; the median paired gain was 26.81%. The median paired result remained 17.42% below V7. Lookahead therefore recovers a substantial part of the Point gap, but does not close it. Compared with Fallocate, lookahead used 10.4% more CPU per put and 54.5% more syncs, while its mean `fdatasync` fell by about 70.2%. The extra syncs show that initialization changes durability grouping as well as the sync latency; the throughput improvement cannot be attributed to `fdatasync` alone.

Across the five lookahead runs, the median initialized amount was 449.8 MB (about 429 MiB); accumulated background initialization time was 295.7 ms and `fallocate` time was 16.0 ms. The corrected counters recorded a median 456 preparation wait-loop entries and 66.8 ms accumulated wait time per run across callers. This is summed caller wait time, not elapsed benchmark wall time or CPU time.

The GCP release build succeeded. The focused lookahead suite passed 12/12 tests, including cross-window replay on the real `io_uring` host. `failpoint_parallel_writer_crashes_at_durability_boundaries` also passed; it exercises 48 combinations (two FRONTIER modes, six extent-preparation modes, and four crash points). The source commit, complete patch, benchmark runner, environment details, test logs, plan, summary, remote-exit record, and all per-run result/metadata pairs are retained in a locally checksum-verified archive (`8e39f69d5a49a0ba0d3df4029cd85c846e69fdea14c11c67d19bea5f85192fce`). The archive and checksum sidecars remain local and were not uploaded to GitHub.

This qualification covers only 16-writer Point throughput and the listed recovery tests. It does not qualify other writer counts or batch workloads, nor establish stable tail latency or production-default suitability. The lookahead option remains explicitly off by default. The VM and temporary cloud environment were retained for follow-up work as requested; no manual teardown was performed.

Local validation for the committed candidate and follow-up diagnostics includes bench and non-bench compilation, Clippy with warnings denied, the lookahead unit tests, and formatting. GCP-only tests covered cross-window recovery and the durability-boundary failpoint matrix, which the local sandbox cannot run because it denies `io_uring`.
