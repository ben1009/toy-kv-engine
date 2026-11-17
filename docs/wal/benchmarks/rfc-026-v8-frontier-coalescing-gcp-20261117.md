# RFC 026 V8 Point FRONTIER Coalescing Comparison on GCP — 2026-11-17

## Run identity

- Date: 2026-11-17 (UTC)
- Source commit: `398e8a0aee17993b13a7685161389af3b3408564`
- Binary SHA-256: `12722a7ea07cc36a6ba3c3ba487a690e028a834291a654057e501eda6ac84b4e`
- Source archive SHA-256: `ff43dd0c41f9c62f7f3cb02c1232de37871e78e4c335ed55d17273aeb3cc56c9`
- Counterbalanced matrix runner SHA-256: `5feff42abd091ca6a914c89364149a0b063f95d532ef6df1297c000b9ffa6823`
- Build/launch wrapper SHA-256: `6b1c5a789475109c546bd87157ca0eff36199b395839c2be006a023818df955c`

## Setup and method

GCP C4 `c4-standard-16-lssd`, 16 writers, 100,000 Point puts/run, 1 KiB values, ext4 on the Local SSD RAID0 device, Linux `7.0.0-1017-gcp`, mount `rw,noatime,stripe=256`. All V8 arms used buffered FRONTIER and 1 MiB preparation where specified. No detailed phase profiling was enabled for score runs; operation latency was sampled every 100 operations. Five counterbalanced blocks ran all five arms (25 successful score runs total).

- **A:** V8 Point-only Lookahead, dynamic coalescing.
- **B:** V8 Point-only Lookahead, fixed 100 μs.
- **C:** V8 Point-only Lookahead, fixed 400 μs.
- **D:** V8 Sliding Fallocate, dynamic coalescing.
- **E:** V7 reference.

The Lookahead setting was `sliding-point-lookahead` with a 1 MiB window. The experiment changed only the benchmark coalescing budget; it did not modify DATA Group Join Wait, WAL protocol, or production defaults.

## Score runs

Values are medians across five runs. Latencies are sampled per-operation percentiles; `fdatasync` and FRONTIER write are aggregate wall time divided by sync count.

| Arm | Throughput (puts/s) | CPU (μs/put) | Put p50 (ms) | p95 (ms) | p99 (ms) | Syncs | Puts/sync | fdatasync (μs/sync) | FRONTIER write (μs/sync) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A Lookahead / Dynamic | 36,587 | 80.75 | 0.376 | 1.056 | 2.732 | 9,733 | 10.27 | 61.83 | 24.07 |
| B Lookahead / 100 μs | 36,286 | 82.88 | 0.376 | 1.057 | 3.063 | 10,437 | 9.58 | 65.70 | 24.45 |
| C Lookahead / 400 μs | 37,116 | 72.08 | 0.391 | 0.942 | 2.151 | 6,356 | 15.73 | 62.89 | 15.05 |
| D Fallocate / Dynamic | 30,062 | 70.67 | 0.523 | 0.682 | 1.585 | 6,337 | 15.78 | 178.64 | 3.97 |
| E V7 reference | 44,589 | 60.28 | 0.309 | 0.704 | 1.169 | 8,239 | 12.14 | 17.73 | 46.14 |

Paired throughput change versus A, using the five counterbalanced blocks:

- B: median **−0.40%**, 1/5 wins.
- C: median **+0.46%**, 4/5 wins; per-block changes `+0.46%, −0.21%, +0.44%, +1.45%, +1.78%`.
- D: median **−18.38%**, 0/5 wins.
- E: median **+22.56%**, 4/5 wins. The ratio of arm medians is different from the median paired ratio; both are reported with their respective definitions.

C had fewer syncs and higher group coverage than A, but its throughput gain did not meet the pre-set +5% retention gate. B was slightly slower and used more CPU. The test therefore does **not** justify changing the production/default coalescing policy. C's p95 and sampled p99 were lower than A, while p50 was about 3.9% higher; retain the tail result as directional because each arm has five runs and sampled p99 is noisy.

## Separate diagnostic runs (not throughput scores)

One detailed run each for A and C enabled the durability and MVCC samplers (every 64 successful advances/commits). A produced 153 sampled durability advances and 1,562 MVCC/ACK samples; C produced 99 and 1,562. All reported dropped-sample counts were zero. These runs are diagnostic only and have instrumentation overhead.

Sampled durability phase p50 / p95, in μs (ticket and wait counts are counts):

| Metric | A Dynamic | C Fixed 400 μs |
|---|---:|---:|
| Tickets covered / advance | 11 / 16 | 16 / 16 |
| Driver wall | 271.84 / 517.26 | 350.24 / 731.13 |
| Driver thread CPU | 126.40 / 176.71 | 151.38 / 204.37 |
| Coalescing wall | 122.19 / 179.84 | 195.98 / 359.65 |
| Coalescing thread CPU | 34.30 / 65.16 | 59.09 / 103.63 |
| FRONTIER write wall | 4.35 / 64.56 | 3.22 / 25.77 |
| fdatasync wall | 41.38 / 184.80 | 37.33 / 189.95 |
| fdatasync thread CPU | 8.56 / 25.02 | 7.23 / 24.96 |
| Pre-publish retirement CPU | 26.29 / 37.61 | 36.17 / 39.72 |
| Post-publish retirement CPU | 19.43 / 29.69 | 24.12 / 34.04 |
| Coalescing Condvar waits / advance | 3 / 5 | 5 / 7 |

Sampled MVCC total wall p50/p95 was 10.14/80.69 μs for A and 10.18/81.72 μs for C; thread CPU was 9.62/55.00 μs and 9.10/47.37 μs. ACK `durable publish → WAL return` p50/p95 was 33.03/67.69 μs for A and 35.75/70.17 μs for C. `WAL return → Put internal return` was 18.12/90.72 μs and 17.32/92.82 μs respectively.

The fixed 400 μs run formed larger durability groups and reduced sync count, but sampled coalescing and total driver wall/CPU were higher. Sampled fdatasync p50 was similar, with a long p95 tail. MVCC and ACK distributions were also similar between these single diagnostic runs. This points to group formation as the parameter's main direct effect; it did not establish a throughput win or a new root cause for the remaining V8/V7 gap. Stage quantiles are separate samples and must not be summed into an end-to-end latency.

## Conclusion

The coalescing sweep is complete. Fixed 100 μs should be dropped. Fixed 400 μs may remain a diagnostic candidate because it increases tickets/sync and improves sampled p95/p99, but its paired throughput improvement over dynamic was only 0.46%, below the 5% acceptance threshold. Keep the existing default and continue with the next targeted FRONTIER/durability investigation only if needed; do not make a production change based on these results.

All **25/25 score runs** and **2/2 diagnostic runs** completed successfully. The local result archive checksum was verified: `e367bdbce3d1bdc83790a74ef3cd2ab621398cf6c3687fbc06957ee78a3dddae`.

The complete compressed result archive and checksum sidecar are retained locally at `bench-results/gcp-v8-frontier-coalescing-398e8a0a-20261117-run2/`; the archive is intentionally not uploaded or committed. SHA-256: `e367bdbce3d1bdc83790a74ef3cd2ab621398cf6c3687fbc06957ee78a3dddae`. The archive contains the per-run JSON, matching run metadata, benchmark plan, host environment, and validation inputs; the checked-in report publishes the aggregate and sampled diagnostic results. Local cleanup verification records that all temporary GCP resources were removed after the archive checksum and run set passed validation.

## Cleanup verification

After verifying the complete local result set and archive SHA-256, the benchmark VM and temporary SSH firewall were deleted and confirmed absent. Terraform destroyed the isolated VPC, subnet, and egress route (3 resources); GCP describe checks confirmed all three absent. Temporary Terraform state, scripts, and SSH credentials were removed. The result archive, SHA-256 sidecar, extracted run records, report, and cleanup verification remain local. No cloud resources remain from this run.
