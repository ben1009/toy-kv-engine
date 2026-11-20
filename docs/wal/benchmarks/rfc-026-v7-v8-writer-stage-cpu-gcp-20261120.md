# V7/V8 cumulative writer-stage CPU on GCP

Thirty runs completed on one c4-standard-16-lssd VM in us-central1-b with two
bundled NVMe Local SSDs in ext4 RAID0. Each run issued 100,000 1 KiB Point Puts.
The same binary ran three alternating-order blocks each at 8 and 16 writers.
Each block ran V7 off/full, V8 off/full, and a V8 driver-only control. V8 used
1 MiB Point Lookahead, Direct FRONTIER, and dynamic coalescing. The off arms
still measured aggregate thread CPU at work boundaries. None used detailed
timeline tracing.

## 16-writer results

Values are medians of the three full-timing runs. Paired differences are medians
of per-block differences and need not add to the process-wide difference.

| Measured stage | V7 CPU μs/Put | V8 CPU μs/Put | Paired Δ | Timed calls/Put V7 → V8 |
| --- | ---: | ---: | ---: | ---: |
| Admission and reservation | 4.13 | 4.68 | +0.55 | 3.00 → 1.00 |
| Encoding and digest | 2.82 | 5.15 | +2.33 | 3.00 → 3.00 |
| Frame readiness and submission | 0.74 | 4.12 | +3.39 | 0.484 → 1.00 |
| MVCC publication | 13.82 | 13.80 | −0.01 | 1.00 → 1.00 |

At 8 writers, the paired gaps were +0.25, +1.77, +2.47 and +0.51 μs/Put
in the same order.

Admission includes two separately timed V7 preflight reservation calls and the
V7 admission call. V8 reserves within its admission call. V7 entry encoding,
frame encoding, and packer hashing all contribute to encoding and digest.
V7 logical metadata finalization remains inside admission. For submission,
V7 times group construction through worker send in the ordered packer. V8
times frame readiness through guard registration, extent checks and the
submission path. This compares the closest existing work boundaries, with V7
queue and readiness work outside the V7 submission timer. A timed call in
one version can cover different work or several tickets in the other.

## Timing controls

At 16 writers the timing-off medians were 48.88k puts/s and 59.44 μs/Put for
V7, and 41.34k puts/s and 79.36 μs/Put for V8. Full timing shifted median
paired process CPU by +4.41 μs/Put in V7 and +2.86 μs/Put in V8. Paired
throughput changes were −1.51k and −0.67k puts/s. The V8 driver-only arm
shifted process CPU by +0.30 μs/Put at 8 writers and −2.48 μs/Put at 16.
That spread across three pairs is insufficient to isolate the clock-read
cost. Use the measured stages to prioritize investigation, with throughput
decisions based on a separate uninstrumented candidate A/B.

## Completion and durability comparison

Reanalysis of the previous role-level archive combined V8 driver and reaper CPU
within each run before comparison with the V7 coordinator. The exact paired
median differences were +5.63 μs/Put at 8 writers and +6.79 μs/Put at 16.
The current experiment did not split this category into individual operations.

## Reproduction and limits

- Source: `3228f914357a7ae72d584b77cf873ae494441e50`.
- Temporary benchmark patch SHA-256: `ac22fc6c238ac7ab689c5cef44fd95544ec1c5336e8180a29a08797482322003`.
- Verified local raw archive SHA-256: `20bb48affb5c2e313958edbc276a64ff0ed02374b5e443973ba373ac2fd4f5b1`.
- The raw archive and detailed `phase-summary.txt` are retained locally; they are
  not included in this documentation-only commit.
- The disposable bootstrap skipped the optional `linux-tools-gcp` package.
- Verified the VM, boot disk, SSH firewall, VPC, subnet and routes were absent
  after teardown.

The stage timers have asymmetric boundaries between V7 and V8, and the small
sample count means these measurements identify investigation candidates rather
than establish causal CPU costs. In particular, the current experiment did
not further subdivide the completion/durability difference. No runtime change
or optimization claim is made here.
