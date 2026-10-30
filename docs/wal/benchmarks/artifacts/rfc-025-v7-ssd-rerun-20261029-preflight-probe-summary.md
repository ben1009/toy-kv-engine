# SSD stability preflight for v7 batch qualification

The only attached physical drive is the Solidigm SSDPFKNU010TZ. The repository is on ext4 at `/dev/nvme0n1p3`. Its exposed temperature before the probe was approximately 36.9°C; sampled host activity found no recognized competing builds, tests, profiles or benchmarks.

The probe initialized a dedicated 128 MiB file with aligned direct writes, synchronized it, and then overwrote the same file in four consecutive passes. Each pass issued 32,768 direct 4 KiB writes and 8,192 `fdatasync` calls, one sync after every four writes. The probe file was removed after measurement; no existing data was modified.

| Pass | Write p99 | Writes at least 5 ms | Longest write | Sync p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.707 ms | 280 | 56.9 ms | 0.545 ms |
| 2 | 0.648 ms | 260 | 161.9 ms | 0.544 ms |
| 3 | 0.700 ms | 281 | 34.3 ms | 0.545 ms |
| 4 | 0.647 ms | 267 | 15.2 ms | 0.543 ms |

All four passes failed the predeclared stability criteria: write p99 at most 0.2 ms, at most 0.5% of writes lasting at least 1 ms, and sync p99 at most 1.5 ms. The criteria were fixed before capture and apply to each pass. Among the 1,088 writes of at least 5 ms, 1,087 were the first write after a sync; the median gap between these events was 128 writes in each pass. This matches the previously observed direct-I/O pattern without identifying the SSD's internal cause. A low average or a low `fdatasync` p99 alone would miss the long writes.

The preserved candidate binary has SHA256 `bb8b4c54c81a18c74f8d635d53ef6072f635b3e5bcff0ff7e4e2e184760d98fa`; the retained v4/v7 benchmark binary has SHA256 `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8`. No WAL comparison was run in this failed preflight, and no candidate source was restored or modified.

Artifacts: [probe script](rfc-025-v7-ssd-rerun-20261029-preflight-probe.py), [frozen plan](rfc-025-v7-ssd-rerun-20261029-preflight-probe-plan.json), [complete results and host samples](rfc-025-v7-ssd-rerun-20261029-preflight-probe-result.json), [raw per-operation latencies](rfc-025-v7-ssd-rerun-20261029-preflight-probe-latencies.json.gz), and [console output](rfc-025-v7-ssd-rerun-20261029-preflight-probe.log).

The [captured probe script](rfc-025-v7-ssd-rerun-20261029-preflight-probe-captured.txt) is preserved verbatim. The linked Python reproducer hardens cleanup after capture so a failed exclusive open cannot remove another file; the measurement code is unchanged.
