# RFC 025: producer framing for small DATA batches

**Not retained.** Large v7 DATA batches already perform framing and frame CRC
on producer threads. This experiment extends the existing producer job to
smaller batches, moving their frame construction and CRC away from the ordered
packer. The point-write SSD screen did not improve both throughput and p99 at
any writer count, so the existing 64-KiB threshold was restored.

The candidate leaves the v7 format, CRC implementation, prefix digest,
FRONTIER, sync policy and v4 path unchanged. It changes readiness and
scheduling: producers must finish small frames before publishing them. Its
[patch](artifacts/rfc-025-v7-small-producer-frame-20261030-candidate.patch.gz)
includes updated tests; the deferred-framing error fixture explicitly marks a
batch unframed to exercise the fallback error path. Thirty-one focused v7
runtime tests passed outside the sandbox without retries. This is not a full
correctness qualification of the candidate.

## Point-write SSD screen

Each ext4 run performs 20,000 1-KiB puts with Parallel v7 WAL and no
compaction. At each writer count the screen uses one warmup and three scored
alternating runs per arm. The table selects the highest eligible throughput
per exact binary and uses p99 and counters from that same invocation. A run is
flagged if mean FRONTIER-write time is at least 0.12 ms or mean `fdatasync` is
at least 0.6 ms; these averages can miss individual stalls.

| Writers | Arm | Puts/s | p99 (ms) | Syncs | DATA groups | Mean sync (ms) | Eligible |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Baseline | 2,371 | 0.730 | 20,000 | 20,000 | 0.240 | 3/3 |
| 1 | Candidate | 2,015 | 2.432 | 20,000 | 20,000 | 0.243 | 3/3 |
| 8 | Baseline | 16,256 | 1.151 | 2,535 | 11,403 | 0.253 | 3/3 |
| 8 | Candidate | 16,091 | 1.113 | 2,541 | 8,393 | 0.248 | 3/3 |
| 16 | Baseline | 24,762 | 1.385 | 1,300 | 10,768 | 0.279 | 3/3 |
| 16 | Candidate | 24,299 | 1.335 | 1,285 | 6,463 | 0.282 | 3/3 |

At one writer the candidate is 15.0% slower and its p99 is higher. At eight
and sixteen writers it is 1.0% and 1.9% slower respectively, while p99 is lower.
All six selected runs pass the aggregate timing filter, but that does not
prove an absence of short stalls. The changed group counts show a scheduling
change; these selected runs do not establish a causal attribution for the
throughput or p99 differences.

The [plan](artifacts/rfc-025-v7-small-producer-frame-20261030-plan.json),
[matrix](artifacts/rfc-025-v7-small-producer-frame-20261030-matrix.json),
[binary identities](artifacts/rfc-025-v7-small-producer-frame-20261030-binaries.json),
and [summary](artifacts/rfc-025-v7-small-producer-frame-20261030-summary.json)
retain the complete 24 invocations, flags and host audits. Batch guards were
not run because the point screen did not qualify. The experiment leaves the
retained large-batch producer framing in place.
