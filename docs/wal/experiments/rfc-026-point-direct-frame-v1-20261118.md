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

## Current status

Code and differential tests have been authored on the experimental branch,
but no Rust build, test execution or GCP A/B result has yet been verified.
This is **not** a production/default optimization; do not merge without passing
the existing WAL correctness/chaos tests and repeatable performance evidence.
