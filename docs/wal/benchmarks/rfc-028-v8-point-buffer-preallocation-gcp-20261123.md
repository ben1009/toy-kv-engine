# RFC 028 V8 Point buffer preallocation GCP A/B

November 23, 2026. Tested commit `c3990bcfafca5379d217ff2dfad691d77c65389e`.

## Method

One `c4-standard-16-lssd` host in `us-central1-b`, with two Local SSDs striped as
RAID0 and mounted as ext4. Both arms used the same release `write-perf` binary
with the `bench` feature, Cursor progress, Direct Completion, Direct FRONTIER,
1 MiB Point Lookahead, and Dynamic coalescing. Singleton submission and extent
scan diagnostics were disabled.

The sole A/B switch was `TOYKV_BENCH_V8_POINT_BUFFER_PREALLOC=0/1`. Each cell
ran five pairs with alternating arm order and a fresh database per run.
Each run used 100,000 puts of 1 KiB values. Point latency was sampled every
10 puts. The suite did not include a V7 sentinel or sequencer lock timing.

## Results

Throughput columns show arm medians. The paired change is the median of the
five within-pair percentage changes.

| Workload | Off puts/s | On puts/s | Paired change | On wins | CPU μs/put, off → on | Sampled p99 ms, off → on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point1 | 8,711 | 8,738 | +1.64% | 4/5 | 52.26 → 52.22 | 0.424 → 0.435 |
| Point8 | 32,503 | 35,602 | +9.59% | 5/5 | 51.71 → 48.20 | 0.724 → 0.705 |
| Point16 | 44,286 | 46,002 | +3.88% | 5/5 | 70.67 → 69.15 | 0.923 → 0.922 |
| Batch64, 16 writers | 623,225 | 625,526 | +0.28% | 3/5 | 4.92 → 4.95 | — |

Point16 median sync count fell from 9,711 to 8,860. Every enabled Point run
recorded 100,000 attempts, 100,000 hits, and zero fallbacks. Both Batch64 arms
and every disabled Point run recorded zero attempts.

Point8 exceeded the 5% throughput target. Point16 improved in all five pairs
with stable sampled p99 and fell short of the 5% target. These results support
keeping preallocation experimental and disabled by default.

## Verification and cleanup

All 40 planned runs completed. The local result archive checksum passed, and
each run had its result, metadata, and switch proof. Terraform removed the
environment, and GCP inventory confirmed that the VM, boot disk, SSH rule,
subnet, routes, and VPC were absent.

- Local artifact directory: `bench-results/prealloc-ab-c3990bcf/`
- Archive SHA-256: `45adce26660439c4b49294e319f24cab98a5bfdc11d0fe1d9da849d582d8109a`
- Benchmark binary SHA-256: `dd168ce26f2baa21a1415be2dcd2b57dff9a519358b661565b0822c77714ce43`
