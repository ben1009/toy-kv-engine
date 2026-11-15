# RFC 026 V8 Sliding Fallocate Window Comparison on GCP, 2026-11-15

## Result

Increasing the V8 sliding `fallocate` window from 1 MiB to 4 MiB produced no
material overall improvement in this comparison. Point and Batch8 were flat or
slightly slower with 4 MiB. Batch16 and Batch64 improved by less than 0.6% at
the paired median. The production-sized 1 MiB window is sufficient based on
these results.

| Workload | 1 MiB median ops/s | 4 MiB median ops/s | Paired 4 MiB change | 4 MiB wins | CPU/put, 1 → 4 MiB | Syncs, 1 → 4 MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 30.83k | 30.70k | −0.81% | 2/5 | 70.24 → 69.17 μs | 6,332 → 6,342 |
| Batch8 | 207.24k | 207.25k | −0.17% | 2/5 | 12.50 → 12.63 μs | 2,383 → 2,377 |
| Batch16 | 368.34k | 370.28k | +0.55% | 4/5 | 8.38 → 8.37 μs | 2,003 → 1,989 |
| Batch64 | 454.13k | 456.33k | +0.47% | 5/5 | 4.80 → 4.78 μs | 1,453 → 1,443 |

Throughput is `result.ops_per_sec`. Paired changes compare 1 MiB and 4 MiB
within each repetition. CPU/put is process user plus system CPU divided by the
workload operation count. Latency sampling remained enabled; sampled p99 values
are not treated as stable tail-latency results.

The small Batch16 and Batch64 gains do not establish a useful reason to increase
the production window. This experiment compares window sizes only; it is not a
V7 comparison and does not change the remaining V8 Point performance gap.

## Method

- One GCP `c4-standard-16-lssd` host, 16 writers, 1 KiB values, no compaction.
- V8 Parallel, PITR V8, Sliding Fallocate; 1 MiB versus 4 MiB windows.
- FRONTIER and coalescing overrides were unset, using Buffered FRONTIER and
  Dynamic coalescing defaults. No CPU pinning, perf profiling, or timeline
  tracing was enabled.
- Runtime `fallocate` cost was included in each measured run.
- Five counterbalanced repetitions per workload/window pair, 40 runs total:
  100k Point, 300k Batch8, 500k Batch16, and 750k Batch64 operations per run.
- Kernel `7.0.0-1017-gcp`; two NVMe Local SSDs in RAID0, ext4, mounted with
  `rw,noatime,stripe=256` at `/mnt/test-ssd`.

## Validation and retained results

The remote `summary.json` reported `complete: true` with 40 planned and
completed runs. `remote-exit.json` reported `failed: false` and 40 started runs.
Every planned run ID had a successful `result.json` and matching
`run-metadata.json`. The downloaded results archive checksum matched the
remote checksum:

`51f63c70a653e78521de93b8da583daedf1e3e3653b12f50b15e8f599bc1410b`

The per-run WAL metadata was stable in all 40 results. DATA used `O_DIRECT`;
FRONTIER and Sync used buffered I/O. All three handles referred to the same
device and inode at both measurement boundaries, and each result reported its
configured 1 MiB or 4 MiB window. The runtime preparation cost was included:
median cumulative `fallocate` time ranged from 4.8–129.7 ms per run, depending
on workload and window size.

The archive and checksum metadata remain in the local temporary results folder;
the raw archive was not uploaded to GitHub.

- Source commit: `8be0a5a47d0bb7b5255501296ec743511608add3`.
- Source build archive SHA-256:
  `527a35b1036e414dba8e5e0e92153b4f40f063c2cbbec6f7bba028bda6ad1efa`.
- Benchmark runner SHA-256:
  `caf54b4c82d790afc0fb972d0bd103a835057f377a64b4bb4f807783bd41fb18`.
- Release binary SHA-256:
  `e2557130bd74761e2b24268eb6b2cd8c0ac09a1329e65902a624657bcd3ea50f`.

The VM, restricted SSH rule, isolated network, and Terraform state were left in
place. No manual cleanup or `terraform destroy` was run. The VM remains subject
to its preconfigured 60-minute server-side expiry deadline.

## Limitations

Five repetitions on one host are enough to reject a large 4 MiB advantage here,
but not to establish sub-percent differences as generally significant. This
result applies to the measured host, workload sizes, and V8 configuration.
