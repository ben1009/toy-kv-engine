# V8 Point single-frame direct encoding — experiment v1 (2026-11-18)

## Scope

This **experimental** branch descends from the borrowed-Point revert/report head
`732e1c728f33d140ccca5493243a9b33af3d959c` (PR #556). It deliberately does
**not** restore the reverted borrowed-input encoder. It attacks the next copy:
the separate heap-allocated logical-batch buffer followed by a copy into the
already allocated 4 KiB aligned DATA frame.

This experiment is opt-in **only in `--features bench` builds**:

```sh
TOYKV_BENCH_V8_POINT_DIRECT_FRAME=1
```

With the flag unset (the default), and in non-bench builds, the old
`AdmittedWalV8Batch::encode()` -> `encode_data_frames_into()` path is used.

Only a **single canonical Point entry that fits one DATA frame** is eligible
(`WalV8WriteClass::Point`, one entry, one 4096-byte frame). Batch and
multi-frame Point writes retain the old implementation. The existing PITR
logical encoding, entry CRC, BLAKE3 digests, physical frame header and CRC,
ticket admission, reservation, poison-on-error and durability ordering remain.

The direct path is:
1. Admit and reserve the same ticket and 4096-byte direct DATA allocation.
2. Initialize that aligned allocation after the sequencer is released.
3. Encode the logical header and entry stream *in the final frame payload*.
4. Finalize fragment header, physical envelope, zero padding and frame CRC in place.
5. Publish digest readiness followed by frame readiness, then submit io_uring DATA.
6. Retain the ticket guard until durable publication as before.

It eliminates one heap allocation and the logical-payload-to-frame copy. It
does **not** eliminate the temporary `WalBatch` or the value prefix clone.
The latter was tested separately in the borrowed-input experiment without a
stable end-to-end win.

## Verification before performance conclusions

Run the two differential tests and existing WAL recovery/failure tests:

```sh
cargo test -p kv-engine --lib direct_point_frame_is_byte_identical_to_existing_encoder
cargo test -p kv-engine --lib multi_frame_point_cannot_use_direct_encoder
cargo test -p kv-engine --lib wal
```

Then compile a release bench binary using the repository-pinned nightly
toolchain. Compare two **otherwise identical** runs, with the flag off/on, on
one host and the same 1 KiB Point workload:

```sh
# Replace PATH with separate fresh DB paths for each run.
cargo build --locked --release -p kv-engine --features bench --bin write-perf
TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0 target/release/write-perf \
  --bench wal_concurrent --wal --pitr --pitr-format v8 \
  --wal-io-mode parallel --num 100000 --threads 16 \
  --value-size 1024 --target-sst-size 2147483648 \
  --compaction none --output json --path PATH
```

For an acceptance test, use five counterbalanced pairs at 1/8/16 writers
with the same Point-only 1 MiB Lookahead / Direct FRONTIER / coalescing setup,
and Batch16/64 and multi-frame Point guardrails. Capture unprofiled
throughput, process CPU/Put, allocated bytes/Put, Puts/Sync, latency p95/p99,
per-frame CRC/digests and recovery. Do not assume any improvement until
verified: previous borrowed-input A/B failed its 5% threshold.

## GCP qualification result (2026-11-18)

The corrected implementation was tested on one `c4-standard-16-lssd` host
(16 vCPUs, two NVMe Local SSDs in RAID0, ext4 mounted `rw,noatime,stripe=256`,
kernel `7.0.0-1017-gcp`). The workload used 100,000 operations and 1 KiB
values. The same release benchmark binary was used for both arms; only
`TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0|1` changed. Both arms used Point-only
1 MiB Lookahead, Direct FRONTIER, and dynamic coalescing. Runs were
counterbalanced, with five pairs per cell.

| Workload | Writers | Direct off kops/s | Direct on kops/s | Paired median change | Candidate wins | CPU/put off → on (μs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 6.989 | 7.168 | +2.56% | 4/5 | 64.20 → 63.18 |
| Point | 8 | 30.328 | 30.087 | −0.79% | 2/5 | 55.87 → 54.98 |
| Point | 16 | 39.864 | 40.477 | +1.54% | 5/5 | 83.72 → 80.97 |
| Batch16 | 16 | 383.392 | 385.424 | +0.53% | 2/5 | 9.10 → 9.17 |
| Batch64 | 16 | 626.944 | 629.718 | +0.44% | 3/5 | 5.25 → 5.37 |

| Workload | Writers | Sync count off → on | Operations/sync off → on |
| --- | ---: | ---: | ---: |
| Point | 1 | 100,000 → 100,000 | 1.00 → 1.00 |
| Point | 8 | 13,115 → 13,132 | 7.625 → 7.615 |
| Point | 16 | 10,131 → 10,135 | 9.871 → 9.867 |
| Batch16 | 16 | 542 → 550 | 184.50 → 181.82 |
| Batch64 | 16 | 193 → 197 | 518.13 → 507.61 |

Throughput values are medians of the five runs in each arm. Paired changes are
the median of the five pairwise percentage changes. Latency below is the
median of the per-run sampled percentiles; Point sampled every 100 operations
and Batch every 10.

| Workload | Writers | p50 off → on (ms) | p95 off → on (ms) | p99 off → on (ms) |
| --- | ---: | ---: | ---: | ---: |
| Point | 1 | 0.1324 → 0.1318 | 0.1688 → 0.1864 | 0.6533 → 0.5965 |
| Point | 8 | 0.2404 → 0.2416 | 0.4611 → 0.4551 | 0.8726 → 0.9195 |
| Point | 16 | 0.3649 → 0.3483 | 0.7707 → 0.7751 | 1.3481 → 1.5333 |
| Batch16 | 16 | 0.6450 → 0.6420 | 0.9422 → 0.9268 | 1.4405 → 1.1570 |
| Batch64 | 16 | 1.5959 → 1.6416 | 2.3030 → 2.4138 | 2.5691 → 2.6053 |

The runtime counter confirmed the intended path: every enabled Point run
encoded 100,000 direct frames, every disabled Point run encoded zero, and both
Batch workloads encoded zero in both arms. Point16 sync counts were nearly
identical (10,131 off vs 10,135 on; 9.87 vs 9.87 operations per sync), so the
small throughput change did not come from larger durability groups.

The candidate improved Point16 throughput in all five pairs, but the paired
median gain was only 1.54%, below the 5% qualification gate. Point8 was
slightly slower, and sampled Point16 p99 increased from 1.348 ms to 1.533 ms.
Batch throughput stayed within 0.6%, with no Direct-frame encodes. These data
support the correctness of the A/B wiring and show a modest CPU reduction at
16 writers, but do not establish a material or tail-latency-safe throughput
improvement.

### Provenance and decision

- Source revision: `56ce98ff29fe3b5edbc118816f2914987cecf0a3`.
- Binary SHA-256: `2c08297908330b0eb5b999f6805a42f4f60c7cba2b0240d2ca58c2b033cbb1f1`.
- Plan: 50 runs planned; `summary.json` reports `complete: true` and 50/50
  completed. `remote-exit.json` reports `failed: false` and 50 runs started.
- The runner validated each planned run's successful `result.json` and matching
  `run-metadata.json`. The collected archive checksum matched on transfer:
  `ac0bb31b4f00afca32811ade9e750f5435566e1ba8ce83be1259f4abcce78b4c`.
- Raw archive and run files are retained locally and are not committed because
  they contain per-run environment metadata.
- A first bootstrap attempt failed at SSH before starting the benchmark
  (`benchmark_attempted: false`); it is excluded from the 50 runs. The retry
  completed and verified all results. Its VM and temporary SSH firewall were
  confirmed absent after cleanup; the pre-existing isolated network was left
  intact.

Keep Direct-frame opt-in and bench-only; do not enable it by default based on
this result. This qualification does not cover crash fault windows beyond the
existing tests, mixed Point/Batch traffic, other hosts or filesystems, or
production performance.
