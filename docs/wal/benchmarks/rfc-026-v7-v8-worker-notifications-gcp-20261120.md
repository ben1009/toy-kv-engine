# V7/V8 Worker Notification Diagnostic

Date: 2026-11-20 UTC  
Source: `dab85e2abbda0aa4a1a2a7bb6e362103f8c49d53` (PR #558 branch)  
Host: GCP C4 16 vCPU, 2 × NVMe Local SSD, ext4 RAID0; same host and binary for both arms  
Workload: 100,000 Point Puts, 1 KiB values; 8 and 16 writers; three counterbalanced repetitions per arm  
V8: 1 MiB Point-only Lookahead, Direct FRONTIER, Dynamic coalescing  
Diagnostic: CPU attribution every 64 tickets; completion timeline sampled every 32 tickets. Throughput/CPU below are instrumented diagnostics, not uninstrumented scores.

## Results

Counts are medians across three runs, normalized to 100k puts. Group-size values come from the ticket-triggered timeline sample, not a complete group histogram.

| Writers | Arm | Diagnostic throughput | CPU/put | Worker notify requests | Actual eventfd writes | Syncs | Puts/sync | Sampled ticket-group size (median / mean / p95) | Sampled 1-ticket share |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | V7 | 34.85k/s | 40.85 μs | 56,167 | 14,242 | 12,950 | 7.72 | 2 / 2.22 / 4 | 22.4% |
| 8 | V8 | 30.92k/s | 55.47 μs | 97,322 | 29,589 | 13,037 | 7.67 | 1 / 1.10 / 2 | 91.8% |
| 16 | V7 | 48.04k/s | 61.48 μs | 52,329 | 15,787 | 8,334 | 12.00 | 2 / 2.52 / 5 | 19.0% |
| 16 | V8 | 39.70k/s | 86.15 μs | 93,844 | 30,492 | 10,223 | 9.78 | 1 / 1.21 / 2 | 86.7% |

At 8 writers, V8/V7 ratios were 1.73× for notify requests and 2.08× for actual eventfd writes. At 16 writers they were 1.79× and 1.93×.

The fraction of notify requests that did not cause an eventfd write was 74.6% V7 vs 69.6% V8 at 8 writers, and 69.8% vs 67.5% at 16 writers. This is consistent with `signal_if_waiting()` suppressing writes when the worker is not waiting. It does not establish duplicate notifications within a worker cycle.

## Interpretation

Under this instrumented configuration, V8 submits substantially more, smaller worker groups: the ticket-triggered sample is dominated by one-ticket groups, while V7's sampled median is two tickets. This aligns with V8 making roughly 1.8× as many worker notification requests and nearly 2× as many actual eventfd writes.

This is evidence for smaller/finer Worker submissions, not proof that duplicate wakeups cause the throughput gap. The timeline is sampled by ticket every 32 tickets, so its group-size distribution is ticket-weighted rather than a full per-group histogram. CPU attribution can also perturb arrival timing. The next decisive measurement would be a low-overhead aggregate histogram of every Worker group's ticket count plus a worker-cycle counter that distinguishes signal attempts from actual wake transitions; then test uninstrumented throughput only if a safe coalescing opportunity appears.

## Run and cleanup verification

- Preflight passed in `us-central1-b`; `us-central1-a` could not allocate C4 + 2 Local SSDs (`ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS`), and no VM was created there.
- Alternate-zone run completed: 12/12 planned runs; `summary.complete=true`; `remote-exit.failed=false`; 12/12 per-run `result.json` and `run-metadata.json` verified.
- Result archive: `collection-attempt-01/results.tar.gz`, SHA-256 `3ea4f68737006ba603a0dba2e12c4a1593edfc29f323ecbfe563994e86be5bc4`; local checksum matched the remote manifest.
- Runner verified and deleted the VM and temporary SSH firewall. Terraform then destroyed the isolated VPC and subnet. Final checks found the VM, firewall, VPC, subnet, and routes absent; temporary Terraform state is empty.
