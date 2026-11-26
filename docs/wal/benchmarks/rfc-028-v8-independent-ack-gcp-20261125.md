# RFC 028 V8 Independent ACK Waiter GCP A/B — No throughput win, 2026-11-25

## Result

Independent ACK waiters did not improve throughput in this qualification. The
CallerDriven + Independent ACK arm regressed Point8 and Point16, with Point16
down 5.37% in paired median and no paired wins. Dedicated durability plus
Independent ACK recovered Point16 to a 2.28% paired gain, but Point1 regressed
15.02% and Point8 was nearly flat. Neither candidate supports enabling these
modes by default. Keep CallerDriven + Legacy ACK as the qualified baseline.

## Experiment

- Source commit: `b97b417a845e95af1431cfa34d3a982c4f15c1ed`
- Release `write-perf` SHA-256: `781e1d8b18406abc680658eddaa015378fc0e4542aa05751945cdac529bfa73b`
- Runner bundle SHA-256: `eaeb9f626aa8db58aaea7c0e1c0ed2338f0884d9dcab5b6e6561c8141090274b`
- Host: GCP `c4-standard-16-lssd`, 16 vCPUs
- Storage: two NVMe Local SSDs (750 GiB total), RAID0, ext4 mounted at
  `/mnt/gcp-wal-bench`
- Schedule: 60 unprofiled scoring runs; five counterbalanced rounds of three
  arms for Point1, Point8, Point16, and Batch64 / 16 writers
- Workload: 100,000 operations per run; 1 KiB Point values; Batch64 uses 64
  entries per operation
- Fixed configuration: Cursor progress mode, Direct Completion, dynamic
  coalescing, direct FRONTIER, ordered DATA submission, 1 MiB Point lookahead,
  Point buffer preallocation enabled, singleton submission disabled, and
  resource-retirement mode 0
- Arms: CallerDriven + Legacy ACK (baseline), CallerDriven + Independent ACK
  (D3b), and Dedicated Driver + Independent ACK (D3a+b)
- Detailed driver diagnostics and CPU attribution were disabled during scoring.

The run used one source revision and one release binary on the same host. The
matrix isolates the ACK mode for D3b; the D3a+b arm also changes the driver, so
it does not independently identify a driver/ACK interaction.

## Scoring results

Throughput, CPU, latency, and Puts/Sync show the median across five runs per
arm. Paired changes are the medians of the five within-round percentage
changes against CallerDriven + Legacy ACK. Latency is milliseconds.

| Workload | Baseline ops/s | D3b ops/s | D3b paired throughput change / wins | D3a+b ops/s | D3a+b paired throughput change / wins |
| --- | ---: | ---: | ---: | ---: | ---: |
| Point1 | 8,734 | 8,601 | −1.93% / 1 of 5 | 7,497 | −15.02% / 0 of 5 |
| Point8 | 35,067 | 33,079 | −3.66% / 0 of 5 | 34,791 | −0.48% / 2 of 5 |
| Point16 | 45,430 | 43,229 | −5.37% / 0 of 5 | 46,594 | +2.28% / 5 of 5 |
| Batch64 / 16 writers | 631,302 | 633,715 | +0.07% / 3 of 5 | 631,416 | +0.17% / 3 of 5 |

| Workload | CPU/Put, baseline → D3b → D3a+b | p99, baseline → D3b → D3a+b | Paired p99 change, D3b / D3a+b | Puts/Sync, baseline → D3b → D3a+b |
| --- | --- | --- | --- | --- |
| Point1 | 51.99 → 52.86 → 79.90 μs | 0.404 → 0.420 → 0.426 ms | +8.75% / +12.21% | 1.00 → 1.00 → 1.00 |
| Point8 | 48.87 → 50.77 → 48.40 μs | 0.758 → 0.705 → 0.668 ms | −5.94% / −11.53% | 7.72 → 7.60 → 7.58 |
| Point16 | 70.06 → 70.23 → 65.24 μs | 0.940 → 0.942 → 0.932 ms | +3.17% / +1.77% | 11.16 → 11.79 → 11.11 |
| Batch64 / 16 writers | 5.04 → 4.95 → 4.83 μs | Not sampled | — | 526.32 → 512.82 → 540.54 |

D3b lowered Point8 p99 but did not improve its throughput. At Point16, CPU/Put
was effectively unchanged and Puts/Sync increased, yet throughput fell in all
five paired rounds. These observations do not establish a single causal
mechanism. D3a+b's Point16 result was positive in every pair but below a 5%
throughput promotion threshold, and its Point1 regression makes it unsuitable
as a general default.

## Verification and cleanup

- `summary.json`: `complete: true`, 60 planned, started, attempted, and
  completed (60 scoring; no diagnostic runs)
- `remote-exit.json`: `failed: false`, 60 runs started and attempted
- Runner bundle verification: passed; actual and expected SHA-256 match
- D3b result verifier: passed with no errors; all planned run IDs have
  successful `result.json` and matching `run-metadata.json`
- Transferred archive SHA-256:
  `efef145105338a2c0d09918c82dd02bd533e2932d99c857558ae10f9e7e0a125`
- Local archive and extracted results:
  `bench-results/v8-d3b-independent-ack-20261125/verified-final/` and
  `bench-results/v8-d3b-independent-ack-20261125/collected/results/`
- The first SSH collection session disconnected while the remote matrix was
  still running. The VM and isolated network were retained; after reconnecting,
  the completed result set was collected, checksum-verified, and validated.
- Cleanup verification confirms the VM, temporary SSH firewall, isolated VPC,
  subnet, and egress route are absent; Terraform state is empty. The default
  VPC was not used or changed.

The raw archive and per-run artifacts remain in the ignored local
`bench-results` directory; only the summary and checksum are recorded here.
