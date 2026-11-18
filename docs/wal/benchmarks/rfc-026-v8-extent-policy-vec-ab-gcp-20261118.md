# RFC 026 V8 extent-policy storage Vec A/B on GCP — 2026-11-18

## Conclusion

The Vec representation reduced measured extent-policy scan work by more than
99.96%. The uninstrumented Point throughput gains were modest but positive:
+2.19% at 8 writers and +1.01% at 16 writers by paired median, with 4/5 pairs
won in each cell. Batch16 and Batch64 were effectively unchanged.

Use Vec as the default extent-policy representation. The gain is small, but it
removes nearly all historical-window scans without a measured Batch regression.
This experiment does not show that the scan was a major end-to-end throughput
bottleneck. BTreeMap remains selectable in bench builds for future comparisons.

## Run identity

- Source commit: `d711d46da914f2b5f30eceeaaae8a0e2bddc6b53`.
- `write-perf` SHA-256, shared by all 44 runs:
  `5b5c74c15f8547a3b7d8ffbc5beddf617ad5bb5e9bd387eb33e9c6cebeb58be7`.
- Workloads: 100,000 1-KiB puts at 8 and 16 writers; 100,000 puts using
  Batch16 and Batch64 at 16 writers.
- Five counterbalanced pairs per score cell: 40 uninstrumented score runs,
  plus four Point scan-diagnostic runs. All 44 runs completed successfully.
- Score runs used the same release binary and differed only in
  `TOYKV_BENCH_V8_EXTENT_POLICY_STORAGE=btree|vec`. Scan diagnostics were
  disabled for score runs. Latency was sampled every 100 Point operations and
  every 10 Batch operations.
- Both arms used 1-MiB `sliding-point-lookahead`, Direct FRONTIER, Dynamic
  coalescing, Parallel I/O and O_DIRECT DATA. The Point direct-frame bench
  option was disabled. Lookahead preparation remained inside the timed run.
  Batch16/64 initialized zero extent bytes in every score run.

## Environment

- GCP `c4-standard-16-lssd`, 16 logical CPUs (8 cores with SMT), Intel Xeon
  6985P-C.
- Linux `7.0.0-1017-gcp`.
- Two Local SSD devices in an ext4 RAID0 (`/dev/md127`), mounted with
  `rw,noatime,stripe=256`.
- The shared default VPC failed the effective-ingress audit, so the run used a
  temporary isolated VPC and subnet with SSH restricted to the approved source
  CIDR. The isolated-network preflight passed before VM creation. The VM lifetime
  was limited to 60 minutes; the VM estimate was $0.98/hour, below the $2
  estimated-spend admission ceiling. These are admission limits, not a billing
  measurement or hard billing cap.

## Uninstrumented score results

Throughput columns are per-arm medians in k puts/s. The paired change is the
median of the five within-repetition Vec-vs-BTreeMap throughput changes, so it
may differ from the ratio of the two arm medians. CPU/Put is process user plus
system CPU divided by 100,000 puts; the table shows per-arm medians.

| Writers / workload | BTreeMap k puts/s | Vec k puts/s | Paired median change | Vec wins | CPU/Put BTreeMap → Vec |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 Point | 30.40 | 31.07 | +2.19% | 4/5 | 56.26 → 52.97 μs |
| 16 Point | 40.67 | 41.33 | +1.01% | 4/5 | 81.66 → 79.27 μs |
| 16 Batch16 | 372.83 | 373.59 | −0.03% | 2/5 | 9.05 → 8.97 μs |
| 16 Batch64 | 625.29 | 628.06 | +0.49% | 4/5 | 5.17 → 5.18 μs |

At 16 writers, the median Point CPU/Put fell 2.93%, while median Puts/Sync
changed from 9.76 to 9.92. The throughput result remains below the 5% gate.
Both Point arms initialized roughly 407–412 MiB during each 100,000-put run;
that preparation work was included in the score interval.

The sampled 16-writer Point latency medians were:

| Metric | BTreeMap | Vec |
| --- | ---: | ---: |
| p50 | 0.351 ms | 0.342 ms |
| p95 | 0.724 ms | 0.758 ms |
| p99 | 1.564 ms | 2.281 ms |

The p99 values are based on periodic samples from five runs and are noisy; the
Vec arm's higher observed p99 is not enough to establish a stable tail
regression, but it also does not support a latency qualification. Batch
throughput stayed within 0.5% by paired median, and Batch runs did not enter the
Point extent initialization path.

## Scan diagnostics

Each diagnostic cell is one separate run with scan counters enabled. The
maximum policy-map length was about 430 entries. Vec scan counts are the number
of entries actually visited by its bounded lookup.

| Writers | Storage | Check calls | Entries visited | Entries / check | Maximum entries |
| ---: | --- | ---: | ---: | ---: | ---: |
| 8 | BTreeMap | 214,709 | 47,463,522 | 221.06 | 443 |
| 8 | Vec | 215,404 | 12,751 | 0.059 | 442 |
| 16 | BTreeMap | 209,997 | 45,244,295 | 215.45 | 431 |
| 16 | Vec | 210,686 | 17,913 | 0.085 | 430 |

Vec reduced entries visited by 99.973% at 8 writers and 99.960% at 16 writers.
The number of checks was nearly unchanged. This confirms the data-structure
change removed historical-window scans, while the small score gain shows that
those scans were not the only significant cost in the Point pipeline.

## Correctness and result verification

The local differential test compares Vec and BTreeMap behavior across mixed
Point/Batch ownership, out-of-order registrations, sparse windows, boundaries
and truncation. It passed, as did the Point-only lookahead guard, the 120 GCP
runner unit tests, `cargo fmt --all -- --check`, the bench-feature `cargo check`
for `write-perf`, and `bash -n` for the remote runner.

The runner summary reports `complete: true`, 44 planned and 44 completed runs.
`remote-exit.json` reports `failed: false` and 44 runs started. Local
revalidation found one successful `result.json` and matching
`run-metadata.json` in each of the 44 planned run directories; all identities
and binary hashes matched the plan. The matrix validator reports no missing,
duplicate or invalid cells.

The verified result directory is retained locally at
`/tmp/gcp-v8-extent-policy-storage-d711d46d/benchmark-isolated-run/collected/results/`.
The archive is retained at
`/tmp/gcp-v8-extent-policy-storage-d711d46d/benchmark-isolated-run/collection-attempt-01/results.tar.gz`.
Its SHA-256 is
`e9ba5d4ba8ded08b48a885951fbc2d62601a66989224b3828d3f65f1e939a6fb`; the
retained bundle metadata matches both the remote and local digest. The raw
archive and checksum metadata are not added to the repository.

After result verification, the runner removed the VM and temporary SSH rule.
Terraform destroyed the isolated VPC, subnet and egress route. A final GCP
inventory confirmed the VM, firewall rule, VPC, subnet and route are absent;
the retained Terraform state is empty. No APIs, billing settings or quotas
were changed.
