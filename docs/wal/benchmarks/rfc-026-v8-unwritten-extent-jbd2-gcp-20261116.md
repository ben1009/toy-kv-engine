# GCP V8 unwritten extent / JBD2 diagnostic

Date: 2026-11-16 UTC

Source commit: `760acb1ff09cb41041c39289e00d97cf126c60bc`

Host: c4-standard-16-lssd; kernel `7.0.0-1017-gcp`; ext4 on /dev/md127, mount `rw,noatime,stripe=256`.

Workload: 100,000 Point puts, 16 writers, 1 KiB values, PITR Parallel WAL.

## Result

The inode-correlated traces strongly support a connection between V8's fallocate-only path, ext4 unwritten-extent handling, and JBD2 waits inside the WAL `fdatasync` path. They do not establish that this mechanism explains the full V7/V8 throughput gap.

| Diagnostic arm | Diagnostic puts/s* | Runtime syncs | ext4 unwritten-handler events on WAL inode | JBD2 waits inside inode sync / ext4 sync entries | JBD2 wait wall total / average |
|---|---:|---:|---:|---:|---:|
| V7 native | 42.54k | 8,297 | 3,393 | 1,124 / 8,298 (13.55%) | 143.100 ms / 127.3 μs |
| V8 fallocate-only | 29.08k | 6,336 | 112,673 | 6,338 / 6,338 (100.00%) | 683.403 ms / 107.8 μs |
| V8 initialize 512 MiB | 41.79k | 9,713 | 4,101 | 2 / 9,715 (0.02%) | 1.037 ms / 518.3 μs |

*Single-run BPF diagnostics, not formal throughput scores. Initialization took 2.301 s for 512 MiB, before the measured workload (2.393 s). Including both phases yields about 21.30k puts/s, below fallocate-only. This is not a production recommendation.

The ext4 sync-entry counts differ from runtime sync counters by one or two events; they are reported separately. Wait counts are aggregate counts within the inode-scoped sync context, so matching totals do not establish one wait per individual sync. All unique transaction IDs captured at inode-correlated JBD2 waits had matching `jbd2_end_commit` events: V7 1124/1124, V8 fallocate 4024/4024, and V8 initialize 2/2.

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

- V8 fallocate-only had 6,338 JBD2 wait calls for 6,338 ext4 sync entries; V7 had 1,124/8,298 (13.5%); V8 initialization had 2/9,715. The aggregate counters do not prove a one-to-one wait-to-sync pairing. The inode-correlated wait totals were 683.4 ms, 143.1 ms, and 1.0 ms, respectively. These are diagnostic waits, not isolated device service time.
- The ext4 unwritten-handler tracepoint includes device and inode. The JBD2 wait probe captures transaction IDs, all of which matched an end-commit event on the device. This is stronger than same-device temporal overlap, but it does not prove every V8 wait was caused solely by unwritten-extent conversion; other inode metadata, scheduling, and changed durability grouping may contribute.
- The first probe, `ext4_ext_convert_to_initialized_enter`, produced no events on this kernel/path. The follow-up used `ext4_ext_handle_unwritten_extents`, which did capture inode-correlated events. The empty first probe is not evidence of zero conversions.
- Initialization reduced JBD2 waits during measured WAL syncs but cost about 2.3 seconds up front and changed sync grouping. Its end-to-end one-shot throughput was lower than fallocate-only, so this is mechanism evidence, not a proposed optimization.
- These are single diagnostic runs; BPF can perturb timing. Do not interpret the diagnostic throughput as a benchmark score.

## Validation and artifacts

All 6 planned runs have successful `result.json` and matching `run-metadata.json` in their own directories. `summary.json` reports complete 6/6; `remote-exit.json` reports no failures and 6 starts/completions. Three transferred result archives and their retained checksum sidecars passed SHA-256 verification. The source commit and workspace/vendor archive hashes remain in the local summary.

The report is committed here; run-level JSON, traces, and checksum-verified archives remain in the local benchmark result directory and are not uploaded to GitHub. No manual cloud cleanup or `terraform destroy` was run. The VM is now absent because its configured 60-minute server-side lifetime expired; the isolated VPC, subnet, three narrowly scoped SSH rules, and local Terraform state remain. The VM Local SSD disappeared with the instance, while the complete verified result set is retained locally.
