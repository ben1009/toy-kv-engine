# V8 Point 3 μs DATA Group Join on GCP

The 3 μs candidate reduced worker submissions and eventfd writes, but Point16 throughput fell in all five pairs. The candidate was reverted after the experiment.

## Method

One GCP C4 host in us-central1-b with 16 vCPUs and two NVMe Local SSDs in ext4 RAID0. Both arms used the same V8 binary with Direct FRONTIER, 1 MiB Point Lookahead, and dynamic durability coalescing. Each run issued 100,000 puts with 1 KiB values. Five pairs per cell alternated which arm ran first.

The candidate used the existing DATA packer for a single-frame Point head only when a successor had already been admitted. The arm setting was `TOYKV_BENCH_V8_POINT_JOIN_US=0` or `3`. Both arms collected aggregate worker counters and sampled latency every 10 operations. Detailed tracing and CPU attribution were disabled.

## Results

Throughput is the median of five runs per arm. The change column is the median of the five within-pair differences.

| Workload | Immediate (k puts/s) | 3 μs Join (k puts/s) | Paired throughput change | Join wins |
| --- | ---: | ---: | ---: | ---: |
| Point8 | 31.51 | 31.32 | −2.64% | 1/5 |
| Point16 | 42.53 | 41.16 | −3.13% | 0/5 |
| Point1 | 7.52 | 7.50 | −0.44% | 2/5 |
| Batch64, 16 writers | 628.55 | 626.78 | −0.20% | 2/5 |

| Point16 metric | Immediate | 3 μs Join |
| --- | ---: | ---: |
| Worker groups per put | 0.947 | 0.658 |
| Notification requests per put | 0.947 | 0.658 |
| Eventfd writes per put | 0.317 | 0.255 |
| Single-ticket share of worker groups | 95.93% | 69.13% |
| Puts per sync | 9.93 | 10.12 |
| CPU per put | 78.05 μs | 77.88 μs |
| Sampled p99 | 0.926 ms | 0.933 ms |

The group-size counters cover every successful worker group send. At Point16, group sends fell by about 31% and eventfd writes by about 20%. The smaller reduction in sync frequency and flat CPU per put did not translate into throughput gains.

## Reproducibility and cleanup

- Source commit `f104de3ac552c3680d35098209453775620b6c2c`.
- Experiment patch SHA-256 `b59481e2271435d16da122f5b801e6cc3bdc5e50d959c191329037baac58aff1`.
- The first setup attempt stopped before measurement because the local Git diff driver produced an incompatible patch. The corrected unified patch was applied on the same VM.
- All 40 planned runs completed. The verified local archive remains at `/tmp/gcp-point-join/results.tar.gz` with SHA-256 `7db98ab05332fee54b0ae6d4bad889b8038dff13d131a740dd457f04713fb314`.
- The VM, boot disk, temporary firewall, VPC, subnet, and routes were verified absent after collection.
