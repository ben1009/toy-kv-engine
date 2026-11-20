# V7/V8 cumulative CPU by thread role on GCP

Twelve runs completed on one C4 host in us-central1-b with two NVMe Local SSDs striped into ext4 RAID0. Each run issued 100,000 Point Puts with 1 KiB values. Three alternating V7/V8 pairs ran at each of 8 and 16 writers on the same binary. V8 used Direct FRONTIER, 1 MiB Point Lookahead, and dynamic coalescing.

Writer CPU was measured using Linux schedstat across each writer's loop. Resident background threads were measured using the change in schedstat across the run, matching thread ID and name. V8 inline durability CPU was summed with the thread CPU clock around each invocation of the durability driver and subtracted from writer CPU. V7 used a separate durability coordinator and ordered packer. Writer CPU also includes work after WAL return, including MVCC and benchmark loop overhead. Detailed sampling was disabled. Two CPU clock reads per V8 driver invocation add diagnostic overhead whose size was not independently measured.

## Results

Values are median CPU μs per Put across three runs per arm. The difference column is the median of the three within-pair differences. Role differences are computed independently and need not sum to the median process difference.

| Role | V7 at 16 writers | V8 at 16 writers | Paired difference |
| --- | ---: | ---: | ---: |
| Writer outside inline durability | 40.60 | 55.82 | +15.22 |
| Ordered packer | 6.74 | 0.00 | −6.74 |
| Writer plus ordered packer | 47.39 | 55.82 | +8.42 |
| Durability driver / coordinator | 6.00 | 7.90 | +1.93 |
| I/O worker | 6.59 | 9.44 | +2.86 |
| Completion reaper | 0.00 | 4.86 | +4.86 |
| Extent preparation | 0.44 | 0.67 | +0.23 |
| Process total | 60.46 | 78.75 | +18.29 |
| Unattributed residual | 0.050 | 0.033 | −0.016 |

At 8 writers, the paired process difference was +11.66 μs/Put. Writers plus ordered packer accounted for a +4.07 μs/Put paired difference. The completion reaper added +4.15 μs/Put and the I/O worker rose by +1.99 μs/Put.

Median diagnostic throughput at 8 writers was 35.64k puts/s for V7 and 32.17k for V8. At 16 writers the medians were 49.21k and 42.14k. The largest comparable role gap at 16 writers was writers plus ordered packer. Completion handling is also worth separating further because V7 and V8 place it on different threads.

## Reproduction

- Base source commit `0bd68979b70ff67b589af039d5672265ca2eb1ef`.
- Local benchmark-only patch `/tmp/gcp-wal-role-cpu/candidate.patch` with SHA-256 `1c3e13f92dddd191a2d4a6f88600db9e29ff022ddc2dd343a4e4fea2592810f3`.
- Local results archive `/tmp/gcp-wal-role-cpu/results.tar.gz` with verified SHA-256 `6a8c5e87f384c758fe333418f360480f91568965d22530879bc6dc228747512b`.
- Guest bootstrap was resumed after skipping the optional profiling package download. The benchmark used Linux CPU counters and did not invoke those tools.
- The VM, boot disk, firewall, VPC, subnet, and routes were verified absent after collection.
