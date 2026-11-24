# RFC 028 V8 cursor publication D2a requalification — 2026-11-24

Source commit: `2a5a6eaa334c12a06a2c584176e6b68eac15ccb4`
Release binary SHA-256: `a1d3dd837f5a4db76fcf2b5503de8669f55c7dfb7467d0805f8fc2c2b39b18e4`

## Setup

- One `c4-standard-16-lssd` host with 16 vCPUs; two 375 GiB NVMe Local SSDs in RAID0.
- The observed benchmark filesystem was ext4 on `/dev/md127`, mounted at `/mnt/gcp-wal-bench` with `noatime,stripe=256`; usable RAID0 size was 749.7 GiB.
- Ubuntu 24.04, kernel `7.0.0-1017-gcp`.
- Two arms used the same source and release binary: `baseline` (D2a disabled) and `admission_fast_path` (enabled with the exact environment value `1`).
- Five counterbalanced pairs each for Point1, Point8, Point16, and Batch64: 40 runs total. The plan used `--num 100000`, 1 KiB values, and 64 entries per Batch64 operation. Point latency was sampled every 10 puts; CPU attribution and mutex diagnostics were off.
- Fixed configuration: Cursor mode 2, Direct Completion, Direct FRONTIER, dynamic coalescing, 1 MiB Point Lookahead, Stage A Point buffer preallocation, singleton submission off, retirement mode 0, D1 off, and D2b removed.
- Stage A was confirmed active in every Point run: 100,000 preallocation attempts, 100,000 hits, and zero fallbacks per run. Batch64 did not enter the Point preallocation path.

## Results

Throughput columns are the median per arm in k puts/s. The paired change is the median of the five per-pair percentage changes, so it need not equal the ratio of the two arm medians. CPU is the paired median change in μs/put; p99 is the paired median percentage change.

| Workload | Baseline k puts/s | D2a k puts/s | Paired throughput change | Wins | Paired CPU change | Paired p99 change | Puts/sync, baseline → D2a |
|---|---:|---:|---:|---:|---:|---:|---:|
| Point1 | 8.079 | 8.125 | −0.82% | 2/5 | +0.68 μs | +1.91% | 1.000 → 1.000 |
| Point8 | 32.619 | 33.109 | +0.54% | 3/5 | −1.24 μs | +4.06% | 7.669 → 7.669 |
| Point16 | 42.791 | 43.158 | +1.23% | 3/5 | −1.08 μs | +1.76% | 11.109 → 11.643 |
| Batch64 / 16 writers | 622.272 | 625.805 | +0.36% | 4/5 | +0.04 μs | Not sampled | 520.83 → 529.10 |

The runner's qualification gates require Point8 and Point16 each to gain at least 5% and win at least four of five pairs, with p99 regression no greater than 5%. Point1 and Batch64 must not regress by more than 2%. The p99 and control-workload gates passed, but the Point8 and Point16 throughput gate failed. The candidate is therefore **not qualified**.

D2a reduced paired CPU/put at Point8 and Point16, but the throughput medians were only +0.54% and +1.23%, each with three wins in five pairs. This does not meet the stated retention threshold. Keep D2a disabled and end this optimization experiment rather than tuning it further.

## Verification and cleanup

- `summary.json`: `complete=true`, 40 planned, started, attempted, and completed.
- `remote-exit.json`: `failed=false`, 40 runs started and attempted.
- The cursor-publication verifier passed: 40 planned / 40 observed, no errors. Every planned run directory contains a successful `result.json` and matching `run-metadata.json`.
- The remote SSH session closed with exit 255 after the run log recorded all 40 successful runs. The remote status file reports exit 0. The first archive collection attempt timed out during SSH banner exchange; the retry succeeded, and the result archive SHA-256 matched its remote metadata.
- The 300-file archive is retained locally at `bench-results/gcp-v8-cursor-publication-d2a-requalification-20261124-run1/collection-attempt-02/results.tar.gz` (52,276 bytes). SHA-256: `fb1a5fb99937af2153e897f761b0cb574abf1603399e8e3c3588ea69031166e8`. Its matching checksum metadata is `collection-attempt-02/bundle-metadata.json`; raw run data is not committed.
- Cleanup was verified after archive validation: the VM, temporary SSH firewall rule, egress route, subnet, and isolated VPC are absent. Terraform state has no remaining resources. No billing settings, APIs, or quotas were changed. The run used the supplied Free Trial attestation and a $1.50 estimated-spend guard under the $2 maximum; this estimate is not a billing cap.
