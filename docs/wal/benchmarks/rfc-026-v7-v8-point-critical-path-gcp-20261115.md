# RFC 026 V7/V8 Point critical-path comparison on GCP — 2026-11-15

Source commit: `eba976d764f53cac87dc7e9d5c53a304a61b7138`  
Source patch SHA-256: `cf5d1659a5a6ed2f620c29da79e764fc052f207b26e35945622a444d1c22ea13`

## Scope and setup

The run compared V7 Parallel with V8 Parallel at 16 writers, 100,000 Point puts per run, and 1 KiB values. V8 used Direct FRONTIER, a 400 μs coalescing override, and 4 MiB sliding `fallocate` preparation. The host was a GCP C4 16-vCPU machine with two Local SSDs in RAID0, Linux `7.0.0-1017-gcp`, and ext4 mounted with `rw,noatime,stripe=256`.

The suite completed five counterbalanced, untraced V7/V8 score pairs. These runs did not enable profiling, commit-order tracing, MVCC samples, or durability samples; latency was sampled every 100 operations. A separate diagnostic run for each WAL version captured 1,024 complete commit records and sampled MVCC publication every 64 commits. V8 also sampled durability phases every 64 advances. Diagnostic runs are not throughput scores.

The V8 score runs consistently reported DATA and FRONTIER `O_DIRECT=true`, the sync handle `O_DIRECT=false`, and all three handles on the same inode. The I/O metadata was stable from run start to end.

## Untraced throughput results

| Metric | V7 Parallel | V8 Direct/400 | V8 vs V7 |
| --- | ---: | ---: | ---: |
| Throughput median | 48.09k puts/s | 33.62k puts/s | −30.69% median paired change |
| Process CPU median | 6.058 s | 7.137 s | +19.15% median paired change |
| Sync count median | 8,289 | 6,338 | −23.5% by medians |
| Sampled p99 median | 0.832 ms | 0.907 ms | +11.22% median paired change |

V7 was faster in all five pairs. The p99 values use sampled operation latencies and are noisy; they are descriptive for these runs, not a stable tail-latency qualification.

## Diagnostic trace intervals

The following wall-time intervals come from the same 1,024 complete records per arm. Values are p50 / p95 in microseconds.

| Per-ticket interval | V7 | V8 Direct/400 |
| --- | ---: | ---: |
| Ticket admission → durable frontier publication | 440.9 / 1,123.6 | 523.8 / 857.5 |
| Durable publication → WAL return | 61.2 / 121.8 | 44.7 / 77.5 |
| WAL return → MemTable publish complete | 3.0 / 9.4 | 3.1 / 12.4 |
| MemTable publish complete → MVCC entry | 0.3 / 1.9 | 0.4 / 12.7 |
| MVCC entry → MVCC completion | 4.7 / 107.5 | 26.3 / 112.5 |
| MVCC completion → Put return | 3.7 / 60.4 | 2.6 / 40.6 |

MVCC publication diagnostics selected 1,562 samples per arm with no dropped samples. Out-of-order paths accounted for 818/1,562 V7 samples (52.4%) and 922/1,562 V8 samples (59.0%). Median sampled Thread CPU for the out-of-order path was similar: 18.67 μs for V7 and 18.53 μs for V8. In-order path CPU medians were 1.58 μs and 2.81 μs, respectively.

The diagnostic trace counted 1,300 WAL-return inversion pairs for V7 and 2,318 for V8, out of 523,776 comparable pairs per arm (0.248% and 0.443%). At Put return, the counts were 978 (0.187%) and 2,309 (0.441%). These pairwise comparisons are descriptive, not independent samples. Tickets covered by one durable-frontier publication share a timestamp; those timestamp ties are excluded from the inversion denominator.

V8's sampled durability diagnostics retained 99/99 advances. Their p50s were 407.4 μs Driver wall / 164.7 μs Thread CPU, 160.4 / 51.5 μs Coalescing, 36.0 / 35.8 μs pre-publish retirement, 41.6 / 32.4 μs post-publish retirement, and 99.2 / 9.3 μs `fdatasync`. V7 did not emit a matched durability-phase sample in this run, so these measurements are V8-only diagnostics and cannot establish a V7/V8 per-advance CPU difference.

## Interpretation and limits

The score runs confirm a roughly 31% V8 Point throughput gap despite fewer syncs. The trace places the largest median interval increase before the durable frontier is published, while V8's durable-publish-to-WAL-return interval is shorter than V7's. The V8 MVCC entry-to-completion median is higher, but the sampled out-of-order path CPU is similar between versions and its p95 is close. The trace also shows the additional WAL-return inversion is already present at WAL return and does not grow further by Put return.

This points to durability-group formation and MVCC publication ordering as the next places to separate wall wait from CPU work. It does not prove either is the cause: there was one instrumented trace per version, and the trace/samplers can perturb scheduling. The throughput score pairs were separately run without those diagnostics. No WAL ordering or default behavior was changed by this experiment.

## Result verification and retention

The runner reported `complete: true` with 12/12 runs. `remote-exit.json` reported `failed: false` and 12 started runs. Independent validation found a successful `result.json` and matching `run-metadata.json` for every planned run ID; the two trace runs also passed the trace and diagnostic-sample validators.

The transferred archive and its checksum metadata remain in the local result directory, not in GitHub. SHA-256: `47b8bf1e5480b35aba02123faf0e9a64ac62693228550abdb612fec28023cbd5`. The benchmark VM and restricted SSH firewall were retained after collection; no cloud or Terraform cleanup was performed.
