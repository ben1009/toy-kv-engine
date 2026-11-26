# RFC 028 V7/V8 Point preallocation same-host GCP qualification

November 23, 2026. Benchmark source commit `024e1456d1c641724173185e17d8e2eefa3fb1cf`.

## Method

One `c4-standard-16-lssd` host in `us-central1-b`, with two NVMe Local SSDs
striped as RAID0 and mounted as ext4. The three Point arms used the same release
`write-perf` binary: V7 Parallel, V8 Point buffer preallocation off, and V8
Point buffer preallocation on. Point1, Point8, and Point16 each ran five
counterbalanced repetitions, using 100,000 puts of 1 KiB values per run. Point
latency was sampled every 10 puts. Batch64 ran five V8 off/on regression pairs
at 16 writers, without latency sampling. The 55-run matrix used fresh database
directories, with no CPU attribution or detailed WAL profiling.

V8 held Cursor progress (mode 2), Direct Completion, Direct FRONTIER, dynamic
coalescing, and 1 MiB Point-only extent lookahead fixed. Singleton submission
and extent scan diagnostics were disabled. The only V8 off/on change was
`TOYKV_BENCH_V8_POINT_BUFFER_PREALLOC=0/1`. The same variable was exercised for
Batch64 as a guardrail; both Batch64 arms must record zero Point-preallocation
attempts.

The default VPC was excluded after its ingress audit found an existing broad
SSH rule. The run used a dedicated VPC/subnet, a temporary SSH rule restricted
to the current public IPv4 `/32`, and a 60-minute VM lifetime. The read-only
capacity preflight passed before the isolated network or VM was created.

## Results

Throughput columns show the median of each arm. Paired change is the median of
the five within-pair percentage changes. p99 change is the paired median for
the sampled p99, not the ratio of the table's arm medians.

| Workload | V7 puts/s | V8 off puts/s | V8 on puts/s | On vs off, paired | On wins | CPU μs/put, off → on | p99 change, on vs off |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point1 | 6,402 | 8,648 | 8,645 | −1.38% | 2/5 | 53.52 → 53.75 | +4.25% |
| Point8 | 35,712 | 32,661 | 35,483 | **+9.78%** | 5/5 | 52.28 → 48.29 | −1.58% |
| Point16 | 48,136 | 44,099 | 45,928 | **+4.15%** | 5/5 | 71.72 → 69.99 | −2.20% |
| Batch64, 16 writers | — | 626,133 | 629,701 | +0.70% | 4/5 | 4.99 → 4.97 |

V8 preallocation on reached the V7 Point throughput reference at Point8
(paired median −0.19%, 2/5 wins) and came within 4.84% at Point16 (0/5 wins).
However, sampled p99 remained higher than V7: paired median +48.57% at Point8
and +17.45% at Point16. Point1 showed no preallocation benefit. Thus the
throughput gap is close to the suggested 5% stopping boundary at Point16, but
this run does not establish latency parity with V7.

At Point8, preallocation reduced CPU from 52.28 to 48.29 μs/put while sync
count and puts/sync were effectively unchanged (12,951 / 7.72 off versus
12,889 / 7.76 on). At Point16, sync count fell from 9,725 to 8,782 and
puts/sync rose from 10.28 to 11.39. These are observed alongside the throughput
change; this experiment does not isolate why the sync grouping shifted.

All 15 enabled Point runs recorded 100,000 preallocation hits and zero
fallbacks. Disabled Point runs recorded zero attempts. Batch64 recorded zero
attempts, hits, and fallbacks with the flag both off and on. Its 0.70% paired
throughput difference is within normal small-run variation and shows no
material regression.

## Conclusion

Point buffer preallocation is a strong Point8 candidate and gives a smaller,
consistent Point16 improvement. It has no Point1 benefit and did not materially
change Batch64. The Point16 throughput gap to V7 is now just under 5%, so this
result does not support another broad WAL restructuring for throughput alone.
The sampled p99 gap to V7 remains a qualification issue. Keep preallocation
bench-only and disabled by default until the intended production configuration
and its tail-latency target are explicitly qualified.

## Verification and cleanup

The runner reported all 55 planned runs complete. Independent local validation
confirmed `summary.json` and `remote-exit.json`, every run's `result.json` and
`run-metadata.json`, the exact schedule, and the preallocation hit evidence. The
transferred archive SHA-256 matched the remote checksum. The benchmark VM and
temporary SSH rule were absent, Terraform destroyed the isolated VPC and
subnet, final GCP inventory found no remaining VM, rule, VPC, subnet, or route,
and the external Terraform state is empty. No billing setting or API was
changed.

- Local artifact directory: `bench-results/gcp-v7-v8-point-preallocation-20261123T104238Z/`
- Archive: `collection-attempt-01/results.tar.gz`
- Archive SHA-256: `4df907aef21fad0c39b481d2fbe682b2a710b8bf77b9483f8f08aeda6b36da1e`
- Benchmark binary SHA-256: `84743494c0d74425126df1abf2a5364de78073eaa66c58d315abf2bd05ddd403`
- Local runner SHA-256: `648b2e63bc065bda533d0e8c114fa51380e8d83275171bd974613820a2631ba0`
- Matrix helper SHA-256: `7dc3ea361538d5c3b806ed06b5b4b3c63a2157f87419f8fcc878a5245fe1a749`
- Orchestrator SHA-256: `1c99aba97ceb049148d6706a7e199be3c73b53c677ee76495f46724deace0a95`
- Remote runner SHA-256: `d0d2c25b76701a1aeb3666f263770914eb2b59b76d4cceafda4cce2dc4c88600`

Local checks before provisioning: 180 GCP runner unit tests passed, remote Bash
syntax passed, and the targeted concurrent Rotation/Recovery test passed.
