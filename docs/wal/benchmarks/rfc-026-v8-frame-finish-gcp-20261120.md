# V7/V8 Point Frame Finish to DATA submission on GCP

Source commit `63f2c9ecc50d74acb1cc4fa6af0f1984fa285fb3` with a temporary diagnostic patch. All 12 runs completed and the result archive passed checksum and run validation. The test used a single C4 16-vCPU host, two NVMe Local SSDs in RAID0 with ext4, and three counterbalanced V7/V8 pairs at each writer count. Each run issued 100,000 Point puts with 1 KiB values and sampled every 128th operation. V8 used 1 MiB Point-only Lookahead, Direct FRONTIER, and Dynamic coalescing.

| Writers | V7 puts/s | V8 puts/s | V7 CPU/put | V8 CPU/put |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 34,041 | 30,937 | 39.99 µs | 52.07 µs |
| 16 | 48,288 | 41,529 | 60.88 µs | 78.21 µs |

The table reports medians across instrumented runs. At 16 writers, the median of run p50s was 10.46 µs thread CPU for frames ready through submission, 4.83 µs while holding the submission lock, and 3.91 µs in `drain_ready_locked`. The coordinator, guard map, and submission lock acquisition waits had wall p50s of 0.28, 0.27, and 0.23 µs respectively. The measured phases overlap and include instrumentation overhead. V7 has no directly comparable breakdown for these V8 stages.

The data points to work inside the submission lock as the largest measured substage. An uninstrumented A/B would be needed to establish whether reducing that work improves throughput.

Two earlier launches stopped at SSH readiness. The successful run refreshed the SSH source /32 and used a dedicated VPC. The VM and temporary SSH rule were verified absent after collection, and Terraform destroyed the temporary network resources.
