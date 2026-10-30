# RFC 025 Stage 8: v7 Parallel versus v4 Parallel writer sweep, 2026-10-25

**Status: exploratory; not a qualification result.** This report combines the
1/4-writer screen from 2026-10-24 with an 8/16-writer follow-up from
2026-10-25. Both arms use the same candidate binary and Parallel I/O. The v4
arm has PITR disabled; the v7 arm enables PITR. This measures the total
steady-state v7/PITR cost against ordinary v4 writes, not just the format
change. Each cell has three paired repeats and no same-binary null controls or
confidence intervals.

The subsequent [ticket-boundary experiments](rfc-025-v7-ticket-boundary-experiments-20261025.md)
are rejected. Their initial relative CPU gain does not replicate in the final
build, and the final ext4 batch64 repeat loses 6.1% raw throughput while using
13.5% more CPU. The eight accepted optimizations are committed separately;
their source is restored after these trials.

The latest retained [extent readiness cache follow-up](rfc-025-v7-extent-readiness-cache-20261025.md)
avoids preparation locks and inode queries inside an already prepared range.
Its highest ext4 scores are 15,759 puts/s for single puts and 391,992 for
batch64, still 22.8% and 18.7% below the highest comparable v4 scores. Both
formats receive longer cooldowns in this experiment, and all stalled runs
remain included. A separate packer-wakeup replacement is rejected.

The preceding [ordered packer encoding follow-up](rfc-025-v7-packer-encoding-20261025.md)
moves DATA framing and logical hashing outside admission. It reported a
341,049 puts/s ext4 batch64 high, 75.7% above its previous retained v7 high.
That encoding change remains retained; parity is not established.

The earlier [shared extent initialization follow-up](#shared-v7-extent-initialization-on-ext4)
and [MVCC preparation follow-up](#mvcc-preparation-before-the-sequencer)
retain their measured binaries and protocols, including the 1/2/4/8-writer
tmpfs comparison. The intervening
[resource-retirement experiments](rfc-025-v7-resource-retirement-20261025.md)
regress the pooled batch64 comparison and remain reverted.

## Executable and environment

- Candidate source: `f08f4cd7f0bac424d9732837401a89a76ec5b5b6`.
- `write-perf` binary SHA-256:
  `d12902ba53e16f4a5c3b18dde6b2fd758169140e14ea9b6ab98f3f369c927023`.
- Build command:
  `cargo build --locked --release --package kv-engine --bin write-perf --features bench`
  with `rustc 1.100.0-nightly (6bb1652a0 2026-09-22)`.
- Host: x86_64 Linux 6.12.71-1-lts, Intel Core i9-13900T, 32 logical CPUs.
- Storage: `/dev/shm` tmpfs (32 GiB); ext4 on `/dev/nvme0n1p3`, the
  Solidigm SSDPFKNU010TZ.

## Protocol

| Writers | Puts | Value size | SST target |
| ---: | ---: | ---: | ---: |
| 1 | 5,000 | 1 KiB | 1 GiB |
| 4, 8, 16 | 50,000 | 1 KiB | 1 GiB |

Both arms explicitly select Parallel. V4 runs without PITR. V7 enables PITR
and explicitly selects format v7; PITR setup and the capacity-checked v6-to-v7
transition occur before the timed write interval. The archive interval is one
hour, and each run stays within one Active segment. Put latency is sampled
every 100 puts. This screen excludes setup time, rotation, archive work, and
competing archive I/O.

The 1/4-writer screen ran on 2026-10-24; its [manifest](artifacts/rfc-025-v7-vs-v4-first-leg-20261024-runs.tsv)
maps the commands to the [raw JSONL](artifacts/rfc-025-v7-vs-v4-first-leg-20261024.jsonl),
and its [driver](artifacts/rfc-025-v7-vs-v4-first-leg-20261024-run.sh) records
the run order. That artifact contains 8 warmups and 24 scored records; its
SHA-256 is `5ae1488c897274045d5588c6658c2f4322a919378c5549379e064947d38e528f`.
The manifest SHA-256 is
`51e4ea84601ec1284588b48be02b0950c444ee5fcd9496fb636fb3f071d0d14f`.

The 8/16-writer follow-up ran on 2026-10-25; its [manifest](artifacts/rfc-025-v7-v4-writers-8-16-20261025-runs.tsv)
maps commands to the [raw JSONL](artifacts/rfc-025-v7-v4-writers-8-16-20261025.jsonl),
and its [driver](artifacts/rfc-025-v7-v4-writers-8-16-20261025-run.sh) records
the follow-up order. It contains 8 warmups and 24 scored records. The JSONL
SHA-256 is
`e6244a32507ebbc8efd165c33b3b5457e246f9cee7b5a954ae3988d544a7dd97`; the
manifest SHA-256 is
`439854b6045e794d3f85be0389d056b524e45f08d7775185d5e1bb3b379a0c42`.

Each filesystem/workload cell used one unscored warmup per arm, then three
pairs ordered v4/v7, v7/v4, v4/v7, with a fixed 3-second idle between runs.
Every run used a fresh database. All scored v4 rows report format 4 and
Parallel; all scored v7 rows report format 7 and Parallel.

## Results

Arm throughput values are medians of three runs. Paired ratios are v7 divided
by v4 within each pair; the median follows the list. A throughput ratio below
1 means v7 is slower. Latency ratios above 1 mean v7 is slower.

| Filesystem / writers | v4 / v7 median puts/s | Paired throughput ratios (median) | Paired p50 ratios (median) | Paired p99 ratios (median) |
| --- | ---: | --- | --- | --- |
| tmpfs / 1 | 34,797 / 19,926 | 0.743, 0.537, 0.695 (0.695) | 1.264, 1.972, 1.243 (1.264) | 0.813, 0.910, 1.102 (0.910) |
| tmpfs / 4 | 161,160 / 64,360 | 0.436, 0.397, 0.381 (0.397) | 2.778, 2.616, 3.144 (2.778) | 0.681, 1.333, 1.102 (1.102) |
| tmpfs / 8 | 212,365 / 63,196 | 0.302, 0.307, 0.280 (0.302) | 4.353, 4.334, 4.806 (4.353) | 1.134, 1.112, 0.716 (1.112) |
| tmpfs / 16 | 227,457 / 58,341 | 0.262, 0.246, 0.256 (0.256) | 4.746, 5.048, 4.629 (4.746) | 1.593, 1.735, 1.117 (1.593) |
| ext4 / 1 | 752 / 452 | 0.621, 0.556, 1.337 (0.621) | 1.391, 1.993, 0.620 (1.391) | 1.182, 1.164, 1.296 (1.182) |
| ext4 / 4 | 9,206 / 6,222 | 0.653, 0.688, 0.679 (0.679) | 1.540, 1.484, 1.523 (1.523) | 1.090, 1.338, 1.144 (1.144) |
| ext4 / 8 | 17,666 / 10,592 | 0.615, 0.591, 0.577 (0.591) | 1.682, 1.784, 1.537 (1.682) | 1.205, 1.865, 1.060 (1.205) |
| ext4 / 16 | 28,296 / 15,363 | 1.421, 0.471, 0.565 (0.565) | 1.114, 2.320, 1.962 (1.962) | 1.527, 1.677, 1.492 (1.527) |

V7 has lower median throughput in every row. On tmpfs, its paired throughput
ratios decline from 0.695 at one writer to 0.256 at 16 writers. On ext4, the
1-writer results vary substantially and the 16-writer first pair favors v7,
but the other two 16-writer pairs favor v4; the median paired ratio is 0.565.
The 4/8/16-writer ext4 p50 and p99 ratios are above 1 in every pair.

For the 8/16-writer follow-up, v7 writes one frontier per completed durability
advance. On tmpfs it records about 32,000–36,000 syncs/frontiers per run,
compared with about 22,000–27,000 v4 syncs, and produces roughly 338–353 MB WAL
files versus 205,520,896 bytes for v4. On ext4, sync counts are similar between
formats: about 6,500–7,100 for eight writers and 3,500–3,700 for 16 writers.
The v7 WAL files are larger: about 233–236 MB at eight writers and 220–221 MB
at 16, versus about 206,569,472 bytes for v4. These counters accompany the
throughput loss but do not isolate its CPU or I/O causes.

## Interpretation and remaining gate

In these runs, v7 Parallel with PITR does not match ordinary v4 Parallel
throughput. The loss is consistent and large on tmpfs at all four writer
counts. The ext4 medians also favor v4 at all counts, although the 1- and
16-writer samples are variable. V7's p50 latency is higher in most cells, and
the ext4 4/8/16-writer p99 is higher in every pair.

This comparison measures v7/PITR overhead against ordinary v4 writes. It does
not establish that the v7 format alone causes the measured difference, and the
three-pair samples have no same-binary null controls or confidence intervals.
It is an exploratory result, not a qualification or adoption decision.

Stage 8 remains open. Batch64, value-size variants, rotation, checkpoints,
archive backpressure, reads/scans/mixed workloads, same-binary stability
controls, confidence intervals, archive-I/O controls, hash/index CPU
instrumentation, group/coalescing coverage, and reopen/RTO measurements remain
to be completed. No adoption decision follows from this comparison.

## Optimization follow-up: v7 encoding and frame reuse

An optimization pass moved canonical v7 entry-stream encoding and its CRC
calculation before the shared admission mutex. Ticket-dependent metadata is
still filled during ordered admission. Completed one-frame 4 KiB v7 buffers
now return to a separate bounded pool, avoiding repeated aligned allocations
without retaining small frames in the v4 256 KiB buffer pool.

The follow-up uses the same 8-writer workload, host, storage devices, build
profile, and three-pair order as the first run. It was built from the same
`f08f4cd7f0bac424d9732837401a89a76ec5b5b6` source revision with the worktree
optimization applied. The release binary SHA-256 is
`ffff46ff727c8b4e35b3babcf9d1775efeac7bd9685b97bafaad873bf24f4e29`.
The [run manifest](artifacts/rfc-025-v7-v4-parallel-opt-8writers-20261025-runs.tsv)
maps commands and run IDs to the [raw JSONL](artifacts/rfc-025-v7-v4-parallel-opt-8writers-20261025.jsonl).
Their SHA-256 values are respectively
`5c9f9947cf0e8b85bff250a7eccc2fc432024f3ce197102ca362b41f7d61e6e7` and
`3cd529afb7aea20ae2c1d0881666e4016554b06153b52aa8231c8c0dff251033`.

| Filesystem | Arm | Median puts/s | Paired p50 ratio | Paired p99 ratio | Syncs per run | v7 frontier bytes per run |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs | v4 Parallel | 208,486 | — | — | 23,450–24,846 | 0 |
| tmpfs | v7 Parallel + PITR | 65,524 | 4.146 | 1.591 | 30,097–32,656 | 123.3–133.8 MB |
| ext4 | v4 Parallel | 17,519 | — | — | 6,340–6,368 | 0 |
| ext4 | v7 Parallel + PITR | 11,399 | 1.622 | 1.519 | 6,392–6,421 | 26.18–26.30 MB |

The paired throughput ratios (v7/v4) were `0.283, 0.312, 0.315` on tmpfs
(median `0.312`) and `0.640, 0.651, 0.645` on ext4 (median `0.645`). Against
the earlier medians in the table above, v7 improved from 63,196 to 65,524
puts/s on tmpfs (`+3.7%`) and from 10,592 to 11,399 puts/s on ext4 (`+7.6%`).
The paired ratio improved from `0.302` to `0.312` on tmpfs and from `0.591` to
`0.645` on ext4. The v4 medians were within 1.8% of the earlier results.

This pass reduces the measured gap but does not remove it: v7 remains about
69% slower than v4 on tmpfs and 35% slower on ext4, with higher p50 and p99 in
these pairs. The v7 process also used substantially more CPU: median user plus
system CPU was 2.74 s versus 1.32 s for v4 on tmpfs, and 5.89 s versus 3.26 s
on ext4. Frontier writes and digest work remain costs to investigate. These
small exploratory samples do not qualify the format or the default.

## Fast-sync coalescing follow-up

The next pass adds a 100 microsecond coalescing window for v7 after a sync
measured below the existing 100 microsecond threshold. The existing 400
microsecond window remains in effect after slower syncs, and v4 behavior is
unchanged. The [driver](artifacts/rfc-025-v7-v4-fast-coalesce-8writers-20261025-run.sh)
ran the same 8-writer workload and alternating order. Its [manifest](artifacts/rfc-025-v7-v4-fast-coalesce-8writers-20261025-runs.tsv)
and [raw JSONL](artifacts/rfc-025-v7-v4-fast-coalesce-8writers-20261025.jsonl)
have SHA-256 values `d023620a40a78c672ec4db4ec67156d3d15d745070a32f3823315c678af8ec71`
and `417ddaf310510ed15f17177577d8016ea3e8e2a72295b5320435e82b981d53b6`.
It uses source revision `f08f4cd7f0bac424d9732837401a89a76ec5b5b6` with the
coalescing change applied. The binary SHA-256 is
`6cdaa499de8b63f5802056a358d9b155b423e2453c19a52c3d7482382ccdaefa`.

| tmpfs / 8 writers | Prior v7 optimization | Fast-sync coalescing | Change |
| --- | ---: | ---: | ---: |
| v4 median puts/s | 208,486 | 220,417 | +5.7% |
| v7 median puts/s | 65,524 | 81,456 | +24.3% |
| Paired throughput ratio (v7/v4) | 0.312 | 0.370 | +0.058 |
| Paired p50 ratio | 4.146 | 3.409 | lower |
| Paired p99 ratio | 1.591 | 1.596 | roughly unchanged |
| v7 sync/frontier count per run | 30,097–32,656 | 6,556–6,578 | about 79% fewer |
| v7 frontier bytes per run | 123.3–133.8 MB | 26.85–26.94 MB | about 79% fewer |

The fast-sync policy materially improves the tmpfs result and reduces frontier
write amplification while keeping the sampled p99 ratio similar. V7 remains
slower than v4 and has higher p50/p99 latency.

The ext4 leg is invalid for comparison. V7's first scored pair had p99 latency
above 10 seconds; in pairs 2 and 3, both arms fell to 3,214–5,032 puts/s and
both had p99 above 10 seconds. V4 had measured 17,832 puts/s in pair 1. The
run shows a shared storage stall, so it cannot establish the effect of this
policy on ext4. This optimization only changes v7 behavior after
sub-100-microsecond syncs; slower syncs and v4 continue using the existing
policy. An ext4 rerun under stable device conditions is still needed.

## Physical digest and prefix-finalization follow-up

The next pass removes the live physical SHA-256 update over every encoded v7
DATA and FRONTIER frame. The parallel runtime retains a checked physical append
cursor; sealing still computes the required digest by reading the frozen WAL
image. Logical entry bytes continue updating the logical SHA-256 state, but the
runtime no longer clones and finalizes that state for every ticket. It carries
the state with queued batches, retains only each packed group's endpoint state,
and finalizes it when writing that group's FRONTIER. A separate last-admitted
commit timestamp preserves strict timestamp-order validation while prefix
snapshots are reduced to group endpoints.

The [driver](artifacts/rfc-025-v7-v4-hash-hotpath-8writers-20261025-run.sh)
ran three paired 8-writer comparisons on tmpfs, with 50,000 1 KiB puts per arm,
alternating v4/v7 order and a fresh database for every run. It was built from
the same `f08f4cd7f0bac424d9732837401a89a76ec5b5b6` base with the worktree
changes applied. The release binary SHA-256 is
`a3a90e59baf1c7f6f7dde83a50ef4055bd51332a928f5bc14e0a8bc94aad596a`. The
[manifest](artifacts/rfc-025-v7-v4-hash-hotpath-8writers-20261025-runs.tsv)
and [raw JSONL](artifacts/rfc-025-v7-v4-hash-hotpath-8writers-20261025.jsonl)
SHA-256 values are respectively
`bd00e32b3c1c46c91fc28f899f87f9659a311fde2301e933caef1be0feeade7c` and
`07e58465acd53aa642c1175cba0ccd458c987da4272d5b233d22cbcd2413827a`.

| tmpfs / 8 writers | Prior fast-coalescing run | Physical-hash optimization | Change |
| --- | ---: | ---: | ---: |
| v4 median puts/s | 220,417 | 203,677 | -7.6% |
| v7 median puts/s | 81,456 | 101,858 | +25.0% |
| Paired throughput ratio (v7/v4) | 0.370 | 0.494 | +0.124 |
| Paired p50 ratio | 3.409 | 2.413 | lower |
| Paired p99 ratio | 1.596 | 1.012 | lower, noisy |
| v7 median user + system CPU | 2.10 s | 1.75 s | -16.6% |
| v7 syncs per run | 6,556–6,578 | 6,499–6,579 | unchanged |
| v7 frontier bytes per run | 26.85–26.94 MB | 26.62–26.95 MB | unchanged |

The arm and pair results support a v7 CPU improvement: paired throughput ratios
were `0.491, 0.494, 0.515`; p50 ratios were `2.413, 2.525, 2.380`; and p99
ratios were `0.945, 2.114, 1.012`. The v4 median also moved down 7.6%, so these
three pairs remain exploratory and do not isolate the full 25% v7 increase.
V7 still reaches only about half of v4 throughput and has roughly 2.4x p50
latency in this workload.

A 100,000-put, 8-writer perf profile on tmpfs captured 9,369 samples with no
lost samples. The prior profile reported `sha2::sha256::x86::digest_blocks`
at 16.42% of sampled cycles; after this change it was 4.36% in the atom-cycle
view and 3.94% in the core-cycle view. The profile still shows SHA work from
logical hashing, while channel waits, MVCC publication and mutex contention
are now among the larger sampled costs. This is directional evidence; the
hybrid CPU PMUs report separate percentages, and the benchmark is not an
adoption qualification. Ext4 and broader workload measurements remain open.

## Reservation-map follow-up

The next pass changes the two reservation-ID maps in `wal/v7/admission.rs`
from `BTreeMap` to the existing `AHashMap`: the resource ledger's reservation
charges and each installed image owner's reservation tokens. These maps are
accessed by ID; iteration order is not used for correctness. No dependency or
WAL I/O behavior changed.

This is a direct A/B against the prior physical-hash optimization build. Both
release binaries use the same worktree and v7 implementation; the sole source
variant is the reservation-map type. The BTreeMap and AHashMap versions of
`admission.rs` have SHA-256 values `a399327e26c935bc42756b800f87fe496aeee61f8dd102db1ccd7987b019f640`
and `bd70503ccb3c87d56407a431cdae9bcb5252aefc3bd8ba39189576a93a87354f`.
Their `write-perf` binary SHA-256 values are respectively
`a3a90e59baf1c7f6f7dde83a50ef4055bd51332a928f5bc14e0a8bc94aad596a` and
`069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8`.

The [driver](artifacts/rfc-025-v7-btree-vs-ahash-reservations-8writers-20261025-run.sh)
ran one warmup per binary and five alternating paired runs on tmpfs. Each run
used 50,000 1 KiB puts and eight writers. The [manifest](artifacts/rfc-025-v7-btree-vs-ahash-reservations-8writers-20261025-runs.tsv)
records the binary hash and command for each run; the [raw JSONL](artifacts/rfc-025-v7-btree-vs-ahash-reservations-8writers-20261025.jsonl)
contains the measurements. Their SHA-256 values are respectively
`68396f9cfd8ce3c7b70d2e9d7a03da7cf2335df9d55e7d0a5687ee7854766c23`,
`d8d1d430a156a4f9b1e018803bcfe2fd38283a4afa0760a19a9752930729bd81`, and
`b441fe3745dd863189514e32eae3e8d858cf97a450a288fe4f5e7a119d7d4723`.

| tmpfs / 8 writers | BTreeMap | AHashMap | Change |
| --- | ---: | ---: | ---: |
| Median puts/s | 98,386 | 104,210 | +5.9% |
| Median paired throughput ratio (AHashMap/BTreeMap) | — | 1.042 | +4.2% |
| Median paired p50 ratio | — | 0.918 | 8.2% lower |
| Median paired p99 ratio | — | 1.009 | roughly unchanged |
| Median paired process CPU ratio | — | 0.938 | 6.2% lower |
| Syncs per run | 6,495–6,654 | 6,499–6,580 | similar |
| Frontier bytes per run | 26.60–27.25 MB | 26.62–26.95 MB | similar |

AHashMap throughput was higher in all five pairs. Paired throughput ratios were
`1.125, 1.121, 1.033, 1.042, 1.032`; the first two pairs had larger gains
than the last three. This is a small exploratory sample without confidence
intervals or a same-binary stability control. It supports a modest CPU-side
gain for this 8-writer tmpfs workload, but does not establish an ext4 or
broader-workload effect. The reservation-map change is retained for review;
WAL sync counts and frontier volume did not materially change.

## Current AHashMap build: v7 Parallel versus v4 Parallel

This comparison uses the current AHashMap v7 build for both arms, so the
format and PITR path are the variables: v4 runs without PITR; v7 enables PITR
and writes format 7. Both explicitly use Parallel I/O. Each run performs
50,000 1 KiB puts with a fresh tmpfs database. There is one warmup per arm and
five scored alternating pairs at each writer count. The same release binary
(`069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8`) ran every
arm. The [driver](artifacts/rfc-025-v7-ahash-v4-parallel-1-2-4-8writers-20261025-run.sh)
records the protocol; the [manifest](artifacts/rfc-025-v7-ahash-v4-parallel-1-2-4-8writers-20261025-runs.tsv)
and [raw JSONL](artifacts/rfc-025-v7-ahash-v4-parallel-1-2-4-8writers-20261025.jsonl)
record each run. Their SHA-256 values are respectively
`ff004cc65057c3b0995f1496e31a6d4073bfa7c0bc02a750d6f78acf8b9a1a51`,
`077b46870d4d346e8678c5df998cc090e8d8c2591274aeddb0d9c93784e9c822`, and
`9d854d8cd4b6cac3630465adc0db7239ead730e0d496c7f7191b86bb9f96f38c`.

| Writers | v4 median puts/s | v7 median puts/s | Paired throughput ratios (v7/v4; median) | p50 ratio | p99 ratio | Process CPU ratio |
| ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 61,754 | 44,108 | 0.608, 0.651, 0.767, 0.804, 0.730 (0.730) | 1.425 | 1.260 | 1.169 |
| 2 | 99,728 | 67,287 | 0.732, 0.618, 0.677, 0.697, 0.575 (0.677) | 1.446 | 0.972 | 1.285 |
| 4 | 155,000 | 82,242 | 0.608, 0.479, 0.608, 0.422, 0.506 (0.506) | 1.938 | 1.938 | 1.506 |
| 8 | 202,586 | 103,196 | 0.497, 0.516, 0.521, 0.527, 0.510 (0.516) | 2.299 | 1.100 | 1.326 |

V7 was slower in every paired run. Its median throughput was 27% below v4 at
one writer, 32% below at two, 49% below at four, and 48% below at eight. Median
p50 latency was higher at every writer count. The p99 ratios vary: v7 was near
parity at two and eight writers, and about 1.94x v4 at four writers. Median
process CPU was 17–51% higher for v7.

V7's sync count fell with writer count: 50,000 at one writer, about 25,000 at
two, 12,600 at four, and 6,500 at eight. At four and eight writers this is
below v4's roughly 22,000–27,000 and 22,000–26,000 syncs respectively, yet v7
still has lower throughput and higher process CPU. The v7 frontier writes
accounted for 204.8 MB, 102.5 MB, 51.6 MB, and 26.7 MB respectively. This
points to remaining CPU and format/frontier work beyond sync count as a
material part of the gap. The results are tmpfs-only and exploratory; they do
not measure persistent SSD latency or qualify the format for adoption.

## Current AHashMap build: ext4 v7 Parallel versus v4 Parallel

This repeats the current-build comparison on ext4 (`/dev/nvme0n1p3`) rather
than tmpfs, using the same binary SHA-256
`069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8` for both
arms. V4 runs without PITR; v7 enables PITR and format 7. Both explicitly use
Parallel I/O. To limit the single-writer SSD run time, that cell uses 5,000
puts; the 2/4/8-writer cells use 50,000 puts. Every run uses 1 KiB values and a
fresh database. There is one warmup per arm and three alternating pairs per
writer count.

The [driver](artifacts/rfc-025-v7-ahash-v4-parallel-ext4-1-2-4-8writers-20261025-run.sh)
records commands and run order. The [manifest](artifacts/rfc-025-v7-ahash-v4-parallel-ext4-1-2-4-8writers-20261025-runs.tsv)
and [raw JSONL](artifacts/rfc-025-v7-ahash-v4-parallel-ext4-1-2-4-8writers-20261025.jsonl)
record the measurements. Their SHA-256 values are respectively
`e307d84ef5d8faa12e87b1cc8ec227e0147d5141177c28918f84dbd4b453a266`,
`469f421bf33cd8232b0446bbd10e3901a829adef03ff5c4c298a36cff4446557`, and
`d898ae5f72dac093074c6ca407879325365073962837e126821df7e26cf601c0`.

| Writers | Operations | v4 median puts/s | v7 median puts/s | Paired throughput ratios (v7/v4; median) | p50 ratio | p99 ratio | Process CPU ratio |
| ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 5,000 | 2,521 | 1,691 | 0.660, 0.669, 0.680 (0.669) | 1.491 | 1.199 | 1.173 |
| 2 | 50,000 | 3,157 | 1,031 | 0.416, 0.679, 0.290 (0.416) | 3.172 | 1.395 | 1.875 |
| 4 | 50,000 | 9,361 | 6,552 | 0.666, 0.702, 0.701 (0.701) | 1.467 | 1.509 | 1.412 |
| 8 | 50,000 | 17,361 | 12,286 | 0.715, 0.681, 0.706 (0.706) | 1.462 | 0.601 | 1.430 |

V7 was slower in every pair. At one, four, and eight writers, paired throughput
was consistently about 30% lower. Median p50 latency was about 1.5x higher and
process CPU was 17–43% higher at those writer counts. At four and eight
writers, v4 and v7 sync counts were also similar: about 12,500 and 6,300 per
run respectively. V7 wrote about 51.4 MB and 25.8 MB of frontier frames at
those counts.

Treat the two-writer result as unstable. Pair 1 had p99 latency of 8.68 s for
v4 and 10.09 s for v7; pair 3 had 11.73 s for v7 while v4 was 1.57 s. Pair 2
was substantially faster and had no corresponding tail spike. The three-pair
median therefore does not provide a reliable two-writer estimate; the raw
runs are preserved for inspection.

Compared with the tmpfs results above, the four/eight-writer ext4 throughput
ratios are higher (`0.701`/`0.706` versus `0.506`/`0.516`), but v7 remains
slower and uses more CPU. The ext4 run is still exploratory and does not
qualify performance under other devices or mixed workloads.

## Rejected optimization: increase the v7 packer group cap

The 8-writer ext4 follow-up raised only the v7 ordered-packer cap from 8 to 16
tickets. V4 kept the cap at 8 and was run as a shared-path control. Both builds
used the restored bounded-channel packer wakeup. The baseline binary was
`069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8`; the
candidate was
`099441b7a467040e066d237b27c33f7c597f1d7a6e0dad372f064567f8eed1cb`. Each run
performed 50,000 1 KiB puts with eight writers, a fresh database, and one
warmup followed by three alternating pairs.

The [driver](artifacts/rfc-025-v7-packer-group16-vs-8-ext4-8writers-20261025-run.sh)
records the commands; the [manifest](artifacts/rfc-025-v7-packer-group16-vs-8-ext4-8writers-20261025-runs.tsv)
and [raw JSONL](artifacts/rfc-025-v7-packer-group16-vs-8-ext4-8writers-20261025.jsonl)
preserve the measurements. Their SHA-256 values are respectively
`c3cf1270ab7e8acbf95b4fcea24704bdd4de19527835302a3cef3d18d890d33e`,
`3c47d273f05f3e44ed9f3f903a16b16abb15a6c4dd38db254115b485a77121b7`, and
`7a75ddeb5cd04a007c8345ce33b7420ffd3e17a18f0a2b402a5646c2df6d2b09`.

| Pair | v7 cap 8 puts/s | v7 cap 16 puts/s | Throughput ratio | p50 ratio | p99 ratio | Frontier writes (8 / 16) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 12,050 | 12,481 | 1.036 | 0.973 | 1.036 | 6,319 / 6,306 |
| 2 | 12,300 | 12,043 | 0.979 | 1.019 | 1.700 | 6,297 / 6,304 |
| 3 | 4,661 | 3,176 | 0.681 | 1.315 | 1.415 | 6,658 / 6,994 |

The median paired throughput ratio was `0.979`; the candidate did not improve
the result. V4's control medians were 16,842 and 17,266 puts/s for the baseline
and candidate binaries, respectively. Frontier-write counts and packed group
counts stayed effectively unchanged, so the larger packer cap did not widen
the actual durability advances. The cap increase is not retained. Pair 3 had
a large v7 tail-latency event; treat these ext4 measurements as exploratory.

## Rejected optimization: skip the repeated v7 entry-stream CRC

The next experiment tried a trusted typed path from the v7 entry encoder to
DATA-frame materialization. The canonical entry encoder had already computed
the entry-stream CRC used to build the logical-batch header, so this path
avoided decoding that just-built header and recomputing the same CRC before
writing frames. The normal frame encoder still performs full validation. The
fast path is not retained because it did not produce a measurable gain.

The [driver](artifacts/rfc-025-v7-entry-crc-shortcut-ext4-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-entry-crc-shortcut-ext4-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-entry-crc-shortcut-ext4-8writers-20261025.jsonl)
preserve five alternating ext4 pairs. Each run issued 50,000 1 KiB puts with
eight writers; v4 runs were shared-path controls. The baseline executable was
`069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8`; the
candidate executable was
`5345658602c23c72835060bf96792a6b4f753896d923a65ffbff41646f57d9a0`. Artifact
SHA-256 values are:

- Driver: `3f6ee50ba9b3f3fca506e2b2c68236b21508d0ba29aebaa8e6011b8056bb670d`
- Manifest: `13c6f6f743a6f4dbcca3e78066c65beeec559b9b19f842fc3dbbe4e44aaf6cfc`
- Raw JSONL: `f624f7de0862d2e7c3beff8fb63dc57d4fb9b7df7c6b89507cb5e4bb48a61128`

| Pair | v7 baseline puts/s | v7 CRC shortcut puts/s | Throughput ratio |
| ---: | ---: | ---: | ---: |
| 1 | 11,833 | 12,241 | 1.034 |
| 2 | 12,145 | 12,174 | 1.002 |
| 3 | 12,029 | 12,075 | 1.004 |
| 4 | 12,264 | 12,186 | 0.994 |
| 5 | 11,986 | 3,391 | 0.283 |

The median paired throughput ratio was `1.002`; median paired p50 and p99
ratios were `0.997` and `0.9998`. Median process CPU was also flat (4.84 s
baseline, 4.81 s candidate). Pair 5 had a severe candidate-side tail stall,
which makes the standalone candidate median misleading; the paired median
still shows no useful gain. Frontier and sync counts stayed near 6,300 per
run, so the CRC shortcut did not alter durability-group formation. V4 control
medians were 17,252 puts/s for the baseline executable and 17,292 puts/s for
the candidate, showing no shared-path regression or gain of note. This
optimization is rejected and its validation path has been restored.

## Rejected optimization: use a FIFO deque for v7 reservations

This experiment replaced the v7 outstanding-reservation `BTreeMap` with a
`VecDeque`, since ticket reservations are admitted in order and retired as a
completed prefix. The change compiled, but the paired results did not show a
repeatable gain after normalizing against v4 runs from the same binaries. The
implementation has been reverted.

The baseline binary was the existing AHashMap build
(`069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8`); the
candidate with the deque was
`54cdf6cf9833b5376219531bbab3a48dcedfaf206b1b5b015e0e086139f2be44`. Both
used the same v7 implementation and ran five alternating pairs of 50,000
1-KiB puts with eight writers. Each pair included v4 and v7 using both
binaries, providing a shared-path control. The [ext4 driver](artifacts/rfc-025-v7-reservation-vecdeque-ext4-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-reservation-vecdeque-ext4-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-reservation-vecdeque-ext4-8writers-20261025.jsonl)
preserve the SSD run; corresponding [tmpfs driver](artifacts/rfc-025-v7-reservation-vecdeque-tmpfs-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-reservation-vecdeque-tmpfs-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-reservation-vecdeque-tmpfs-8writers-20261025.jsonl)
preserve the CPU-side cross-check. Their SHA-256 values are:

| Artifact | ext4 | tmpfs |
| --- | --- | --- |
| Driver | `0afb05183c22054e97760c28302888f1894207bea9092088d0d1b3ddc37f1c52` | `f0d60fafcc0394ffe26d8b43f40e30588222ca8fa8470d1757fc32b5d3ffdd44` |
| Manifest | `c0a2c3d1494aa42c7f0f02feeab6409f3e2dfe63b21c5f982a4a613b880a7c90` | `3d27b2b62d70acb28d0e83a2d35487d7b741354d8e2d06c1f35262a4c6b3e5e4` |
| Raw JSONL | `e95458bc10d9f0b4c54be6ffe621b957b7aa8fc69527fad19a149c233e0db991` | `8fd27336471aa948d46c3a90b5c58baa11d998eaacef7189c0a59108530d1044` |

| Filesystem | v4 old/new median puts/s | v7 old/new median puts/s | Median old/new v7:v4 paired ratio | Median relative change in v7:v4 ratio |
| --- | ---: | ---: | ---: | ---: |
| ext4 | 16,689 / 16,714 | 11,329 / 11,599 | 0.673 / 0.706 | 1.006x |
| tmpfs | 203,000 / 210,395 | 105,478 / 108,844 | 0.510 / 0.502 | 0.985x |

The raw v7 throughput ratio (candidate/baseline) had a median of 1.024 on
ext4 and 1.015 on tmpfs, but the v4 controls also shifted by 1.001x and 1.036x.
Normalizing each binary's v7 result against its paired v4 result removes that
apparent gain: the median relative change is only 0.6% on ext4 and is a 1.5%
regression on tmpfs. Four ext4 pairs had a 1.028x median normalized change, but
one pair had severe v7 tail stalls in both binaries (p99 above 10 seconds),
including a worse candidate result. This is too noisy and too small to justify
changing the reservation structure. The candidate's normalized v7/v4 ratio
remains about 0.71 on ext4 and 0.50 on tmpfs, so it does not close the gap to
v4 Parallel.

## Rejected optimization: remove a redundant v7 admission lock pass

The v7 append path preflighted capacity under `admission` and then repeated the
open/capacity checks while assigning the ticket. This experiment removed that
successful-path preflight and combined the open, layout, and reservation-owner
checks in `prepare_v7_batch` under one lock. The final locked ticket assignment
and the capacity-error fallback check were unchanged. The code has been
reverted because the performance result was within run noise and regressed on
tmpfs after controlling for v4.

The baseline binary was `069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8`;
the candidate was `079e1a237b77e6e232124adee8ebfeae5e8407f28fd9d6657c01f1c9204ac502`.
Both ran five alternating paired 8-writer comparisons of 50,000 1-KiB puts,
with v4 and v7 on each binary. The [ext4 driver](artifacts/rfc-025-v7-admission-lock-ext4-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-admission-lock-ext4-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-admission-lock-ext4-8writers-20261025.jsonl)
preserve the SSD run; the [tmpfs driver](artifacts/rfc-025-v7-admission-lock-tmpfs-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-admission-lock-tmpfs-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-admission-lock-tmpfs-8writers-20261025.jsonl)
preserve the CPU-side cross-check. Artifact SHA-256 values are:

| Artifact | ext4 | tmpfs |
| --- | --- | --- |
| Driver | `786322bfd74a7e2d7b9c3eaf8c512dc6632154818cf5a34451bee15ba5534b30` | `ad4be925ae999b26ec6dd073b0705b860686ecdbe557523fe15404daccc1a3c6` |
| Manifest | `7598d41947f6271c9da4ec18b19b52b2a4da9a95f96871a014806184872f9713` | `f8e47b26ee2adc73e9ed2215e8f346e2ab1c4a81733c52038cac0f6d658375e3` |
| Raw JSONL | `9feeaf6aab3e6b2824934675e755b45875f0eec062d089e201ec67379ce4a1fa` | `9b8a474d95bbcd5e8519947b8f195127909fd185e0dc61e3d9b091c4a14685a1` |

| Filesystem | v4 old/new median puts/s | v7 old/new median puts/s | Median old/new v7:v4 paired ratio | Median relative change in v7:v4 ratio |
| --- | ---: | ---: | ---: | ---: |
| ext4 | 17,441 / 17,689 | 12,203 / 12,402 | 0.700 / 0.705 | 1.002x |
| tmpfs | 208,831 / 209,567 | 105,619 / 107,954 | 0.529 / 0.509 | 0.963x |

Ext4's fifth v7 pair had a severe tail stall in both binaries, with the
candidate worse; the other four pairs gave a 1.005x median relative change.
Tmpfs shows a 3.7% median regression after normalizing against its v4 controls.
The raw candidate/baseline v7 throughput medians alone (1.012x ext4, 1.022x
tmpfs) overstate the effect because the corresponding v4 controls also moved.
The candidate still reached only about 71% of v4 throughput on ext4 and 51% on
tmpfs.

A fresh 100,000-put, 8-writer tmpfs `perf record` on the baseline captured
9,253 samples with no lost samples. Its largest relevant symbols were
`publish_commit_ts` (5.49%), `parking_lot::RawMutex::lock_slow` (4.86%), SHA-256
blocks (4.37%), and CRC32 routines (5.36% combined). The [profile JSON](artifacts/rfc-025-v7-admission-perf-20261025.json)
and [perf report](artifacts/rfc-025-v7-admission-perf-20261025-report.txt)
preserve the output. This shifts the next optimization target toward MVCC
commit-publication contention; changing that sequence needs a correctness-led
design and a separate review slice.

## Rejected optimizations: move v7 materialization off admission

The v7 write path was profiled against v4, then two CPU-side changes were
measured on tmpfs with eight writers and five alternating pairs of 50,000
1-KiB puts. The first moved DATA-frame encoding/CRC work outside the global
admission mutex while retaining ticket-order publication. The second kept that
change and moved the ordered logical-prefix SHA-256 update to its own lock.
Both compiled and passed the focused v7 parallel tests, but neither produced a
repeatable improvement after normalizing each binary's v7 throughput against
its v4 control. Their median normalized changes were `1.007x` and `1.009x`,
respectively. Both runtime changes were reverted.

| Experiment | Candidate binary SHA-256 | Median v7 puts/s (control / candidate) | Median normalized change in v7:v4 ratio |
| --- | --- | ---: | ---: |
| Frame encode outside admission | `bb5b212854d05c2a08a1abd6eb3193ca7f77add3266e6f56e3210861aced01ab` | 107,279 / 107,920 | 1.007x |
| Hash update outside admission | `b43a060d0f30272967880b0bddef4177f2c71d4050ebd239fd62264a77b968a1` | 103,717 / 106,592 | 1.009x |

For the frame-encode run, v4 controls were 208,121 / 213,510 puts/s. For the
hash run they were 211,489 / 214,821 puts/s. The raw v7 medians move slightly,
but the v4 controls move too, leaving no significant normalized gain. The
[frame-encode driver](artifacts/rfc-025-v7-frame-encode-unlocked-tmpfs-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-frame-encode-unlocked-tmpfs-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-frame-encode-unlocked-tmpfs-8writers-20261025.jsonl),
and the [hash driver](artifacts/rfc-025-v7-hash-unlocked-tmpfs-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-hash-unlocked-tmpfs-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-hash-unlocked-tmpfs-8writers-20261025.jsonl)
preserve the runs.

Artifact SHA-256 values:

| Experiment | Driver | Manifest | Raw JSONL |
| --- | --- | --- | --- |
| Frame encode outside admission | `6a85bb88d6b28c8c05fddc13e8b26f912f71c998de25e0329fd9245c17740847` | `77dd21535cbb1caac0c4d80c3305716ac720fe16f7659b240149559780568351` | `6fc33cc59520a08b80e29fd64a5ab4439d214be816aa30758061c046a1eedc7c` |
| Hash update outside admission | `66740c8401744684456af6682fe33743824e322b2ff940a31d1094c1d3787edb` | `72ff11c70e8cd0dc5f38f2cb116fe7d81e9fcad2b63609c5af1321801fe4c292` | `559944d85d729c1bc094a0b1899c05f87a2aa5104c4012a7ffce3b62f19d125f` |
| Trusted-entry-CRC shortcut | `0e8e15ae46f315480286f95e9a947f521137c52de93bce84d17c4b846c78be00` | `f47f8fb6b538ebbaadd8483083caf52b78f9e19c999fbb1771c78cfaa16d8119` | `ee053107792fb7994aed25430ea7b76ee216924a518e9e90a0a23c3b34a3660d` |

The separately tested trusted-entry-CRC shortcut also failed its tmpfs
cross-check: its median normalized v7:v4 change was `0.989x`. This is
consistent with the profile: materialization and hashing are measurable CPU
costs, but moving them off the admission lock does not address the dominant
throughput gap. These results do not meet the goal of matching v4 Parallel;
the next slice should target per-ticket/frontier group formation and remaining
v7 protocol work rather than further lock relocation.

## Rejected optimization: spin before parking the coalescing coordinator

The v7 tmpfs profile recorded 420.6 ms of coalescing receive waits in a
1,043.1 ms measured run. This experiment polled the completion channel up to
64 times before parking after a fast v7 sync. Every poll checked the existing
fixed deadline. Five paired 8-writer, 50,000-put comparisons included both v4
controls. The candidate binary was
`774051211ba4c9d3055d92a5727c6281b0d289d2b7cf4cf878a720cdc34578c5`.

The normalized v7:v4 changes were `1.055, 0.921, 1.032, 0.970, 1.013`, with a
`1.013x` median. Process CPU rose by a median `1.024x`. This did not establish
a useful gain, so the polling change was reverted. The [driver](artifacts/rfc-025-v7-coalesce-spin-tmpfs-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-coalesce-spin-tmpfs-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-coalesce-spin-tmpfs-8writers-20261025.jsonl)
preserve the results.

## MVCC preparation before the sequencer

The synchronous point-write path held the MVCC `write_lock` through v7 entry
encoding, resource reservations, and frame-buffer allocation. The earlier
admission-lock experiments still held that enclosing MVCC lock. They did not
remove this work from the serialized write path.

The retained candidate prepares owned encoded entry bytes and their charged
DATA buffer before entering the MVCC sequencer. The sequencer assigns the
actual commit timestamp, ticket, recorded-time clamp, and physical offsets.
Admission rechecks segment capacity because another prepared writer may have
consumed the remaining space. Dropping unadmitted work releases its buffer
and persistent reservations. The prepared API also supports existing native
async callers without borrowing their original `WalBatch`.

The control binary was
`069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8`;
the candidate was
`b5078356212a72389b2920cd55f9dbfcbc6f69c52b10e2b09cd0db857ec1ef0b`.
Each filesystem comparison used five alternating pairs of 50,000 1-KiB puts
with eight writers, plus both binaries' v4 controls. Ratios below are medians
of paired ratios, rather than ratios of arm medians.

| Filesystem | v4 control / candidate median puts/s | v7 control / candidate median puts/s | Median v7 throughput change | Median normalized v7:v4 change | Median v7 CPU change |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs | 203,145 / 200,614 | 101,564 / 112,274 | 1.105x | 1.052x | 0.941x |
| ext4 | 17,201 / 17,340 | 12,100 / 12,263 | 0.996x | 0.982x | 0.974x |

Tmpfs's normalized changes were `1.009, 1.218, 1.038, 1.052, 1.191`.
Median paired p50 fell by 8.7%; p99 changed by `1.017x`. This supports a modest
CPU-side improvement in this sample. Ext4's normalized changes were
`0.982, 1.013, 1.061, 0.977, 0.288`. The final candidate run took 14.89 s
versus 4.30 s for its v7 control; its sampled p50/p99 were 1.483/11.392 ms
versus 0.598/2.195 ms. Median paired ext4 p99 changed by `1.426x`. All five
pairs are retained. No SSD throughput or tail-latency improvement is
established by this run.

The [tmpfs driver](artifacts/rfc-025-v7-mvcc-preparation-tmpfs-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-mvcc-preparation-tmpfs-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-mvcc-preparation-tmpfs-8writers-20261025.jsonl),
and the [ext4 driver](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025.jsonl)
preserve the comparisons. The [analysis script](artifacts/rfc-025-v7-mvcc-preparation-20261025-analyze.py)
verifies each raw record against its manifest hash and reproduces the
[summary](artifacts/rfc-025-v7-mvcc-preparation-20261025-summary.json), including
binary and artifact SHA-256 values.

The focused correctness run passed 95 tests, covering MVCC publication,
v7 resource ownership, capacity races, native cancellation, rotation, and I/O
failure handling. New cases verify cancellation before admission, consuming
prepared batches after dropping their inputs and in a different preparation
order, recorded-time clamping, recovery of their exact contents, and capacity
rejection after another prepared batch fills the segment. Three manually
completed crash fixtures now publish the same endpoint digest snapshot as the
production packer. Formatting and Clippy with warnings denied passed.

Ext4 still warrants a separate investigation. V4 enables its background
extent initializer; v7 currently disables that initializer. In the first
stable SSD pair, v4 recorded 1.639 s in `fdatasync`, while v7 recorded 2.008 s
plus 0.204 s writing FRONTIER frames. These counters motivate testing shared
extent initialization for v7 DATA and FRONTIER writers. They do not prove
that extent conversion accounts for the entire remaining gap.

### Current candidate: direct v4 comparison at 1, 2, 4, and 8 writers

This fresh tmpfs sweep uses the same candidate binary for v4 and v7, 100,000
1-KiB puts per run, one warmup per arm, and three alternating pairs per writer
count. Each ratio is computed within its pair. V4 runs without PITR; v7
enables PITR. This compares the resulting writer paths, including PITR costs.

| Writers | v4 median puts/s | v7 median puts/s | Paired throughput ratios (v7/v4; median) | p50 ratio | p99 ratio | CPU ratio |
| ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 66,314 | 42,520 | 0.637, 0.646, 0.642 (0.642) | 1.515 | 1.675 | 1.256 |
| 2 | 103,938 | 69,019 | 0.721, 0.508, 0.631 (0.631) | 1.519 | 1.680 | 1.372 |
| 4 | 160,288 | 93,262 | 0.577, 0.627, 0.491 (0.577) | 1.624 | 1.186 | 1.358 |
| 8 | 220,228 | 117,574 | 0.539, 0.534, 0.563 (0.539) | 2.042 | 1.142 | 1.251 |

V7 remains slower in every pair. This sweep does not establish parity with v4.
The [driver](artifacts/rfc-025-v7-mvcc-preparation-v4-parallel-tmpfs-1-2-4-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-mvcc-preparation-v4-parallel-tmpfs-1-2-4-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-mvcc-preparation-v4-parallel-tmpfs-1-2-4-8writers-20261025.jsonl)
preserve the measurements.

A separate five-pair, 50,000-put single-writer control/candidate comparison
gave median v7 throughput of 40,637 / 40,569 puts/s. Its median paired raw
change was `0.962x`; the median change after normalizing against v4 was
`1.018x` (pairs `1.069, 0.914, 1.195, 0.957, 1.018`). Median paired p50 was
`0.996x`, p99 `1.103x`, and process CPU `1.046x`. This shows no consistent
single-writer throughput direction and does not prove a latency or CPU
improvement. The [single-writer driver](artifacts/rfc-025-v7-mvcc-preparation-tmpfs-1writer-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-mvcc-preparation-tmpfs-1writer-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-mvcc-preparation-tmpfs-1writer-20261025.jsonl)
are included in the same analysis and provenance summary.

### Ext4 rerun with identical binaries

The complete ext4 comparison was repeated with the same two binary hashes,
8 writers, 50,000 1-KiB puts per run, four warmups, five alternating scored
pairs, both v4 controls, and a 3-second idle between runs. The original run is
preserved separately. All 20 scored runs are included.

| Arm | Median puts/s | Median sampled p50 (ms) | Median sampled p99 (ms) |
| --- | ---: | ---: | ---: |
| v4, previous build | 17,138 | 0.409 | 1.391 |
| v4, candidate build | 17,159 | 0.405 | 1.572 |
| v7, previous build | 11,971 | 0.606 | 2.208 |
| v7, candidate build | 11,980 | 0.604 | 2.482 |

| Pair | Previous v7 puts/s | Candidate v7 puts/s | Previous / candidate sampled p99 (ms) | Normalized v7:v4 change |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 11,971 | 12,103 | 1.553 / 1.496 | 0.964x |
| 2 | 12,201 | 11,980 | 2.151 / 1.299 | 0.989x |
| 3 | 3,180 | 3,197 | 11.543 / 15.434 | 1.413x |
| 4 | 4,164 | 3,189 | 12.906 / 12.247 | 0.760x |
| 5 | 12,114 | 12,596 | 2.208 / 2.482 | 1.018x |

The median paired raw v7 throughput change was `1.005x`; the v4-normalized
change was `0.989x` (-1.1%), versus `0.982x` in the first run. Combining all
ten normalized paired ratios gives `0.986x` (-1.4%). Median paired v7 p50,
p99, and CPU changes were `1.007x`, `0.963x`, and `0.993x` respectively.
Arm medians and paired-ratio medians use different aggregation, so dividing
the table medians does not reproduce these ratios.

The scored runs still show large swings: both v7 builds fell to roughly
3,200 puts/s in pair 3. This makes the earlier candidate-only spike
insufficient evidence to attribute the instability to the optimization.
The rerun still does not establish an SSD performance gain. The candidate
v7:v4 paired throughput ratio was `0.680x`, with p50 and p99 ratios of
`1.499x` and `1.082x`; v4 parity remains unmet.

The [rerun driver](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-rerun1-run.sh),
[manifest](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-rerun1-runs.tsv),
[raw JSONL](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-rerun1.jsonl),
[analysis script](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-rerun1-analyze.py),
and [summary](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-rerun1-summary.json)
preserve the complete comparison and verify per-record hashes and unchanged
binary hashes. Benchmark scratch databases were removed after completion.

## Shared v7 extent initialization on ext4

V4 initializes its reserved extents before small direct writes; v7 previously
used allocation alone. The initial candidate initializes active v7 extents
through one shared cursor and lock for DATA and FRONTIER preparation. Both
writers finish range preparation before using it. The cursor advances only
after successful initialization, so later preparation cannot zero an existing
DATA or FRONTIER frame. Frozen WALs and other filesystems skip this work.

Initialization uses one immutable, 4096-byte-aligned 128-KiB zero source. It
writes only the range already requested by v7 preallocation, stays within the
configured file cap, and introduces no background thread or extra lookahead.
A cap ending in a partial frame preserves its EOF while zeroing only complete
frames. This adds zero-write traffic; the `wal_commit_bytes` counter measures
DATA submission and does not include initialization writes. This experiment
does not separately attribute the throughput gain to filesystem operations.

The preserved baseline is
`b5078356212a72389b2920cd55f9dbfcbc6f69c52b10e2b09cd0db857ec1ef0b`;
the candidate is
`0d0198e623a4d92ddf155821efc8b645b35dae0751a153397db81df41c75b1b6`.
Both were built with the release `bench` configuration. The point-write
comparison uses ext4, eight writers, 50,000 1-KiB puts, one warmup per arm,
five alternating scored pairs, both binaries' v4 controls, and a three-second
idle between runs. Every scored run is retained.

| Arm | Highest puts/s | Median puts/s |
| --- | ---: | ---: |
| v4, preserved baseline | 17,054 | 16,810 |
| v4, candidate | 17,023 | 16,783 |
| v7, preserved baseline | 12,204 | 11,745 |
| v7, candidate | 14,942 | 14,094 |

The initial candidate's peak improves by 22.4% against this experiment's v7 baseline
peak. Median paired raw throughput improves by 18.0%; median paired
v4-normalized throughput improves by 18.3%. The five normalized changes are
`1.183, 0.574, 6.495, 1.172, 1.202`. Severe SSD stalls affect controls too:
pair 2's old v4 falls to 8,165 puts/s, and pair 3's old/new v4 and old v7
fall to 2,747 / 2,210 / 2,859 puts/s. The candidate v7 runs span
13,799–14,942 puts/s. These stalls explain the extreme normalized ratios;
the measurements do not establish removal of device stalls. Median paired
v7 p50 changes by `0.815x`, p99 by `1.269x`, and process CPU by `1.028x`.
Throughput improves here; a p99 or CPU improvement is not established.

### Initial prototype: highest comparable scored results

At the maintainer's request, the following initial-prototype comparison uses
the highest throughput under matching settings to reduce the influence of
stalled runs. The adjusted build is measured separately below.
All rows are ext4, eight writers, 50,000 single puts, 1-KiB values, Parallel
I/O, and a 1-GiB SST target. V7 enables PITR; v4 does not. Warmups are excluded.
Peak throughput is a best observed result, not a sustained-performance bound
or a substitute for the complete paired dataset.

| Build / format | Highest puts/s | Relative to highest v4 |
| --- | ---: | ---: |
| Historical v4 Parallel | 18,304 | 100.0% |
| Previous retained v7 Parallel | 12,596 | 68.8% |
| Shared extent initialization v7 Parallel | 14,942 | 81.6% |

The historical v4 peak comes from
`scored-ext4-write8-p2-v4.json` in the [earlier writer-sweep manifest](artifacts/rfc-025-v7-ahash-v4-parallel-ext4-1-2-4-8writers-20261025-runs.tsv),
binary `069ec960e569f2e9a3bbfe3cc952cd75e819a46ab98a60f8906e674ff920fba8`.
The previous retained v7 peak comes from
`scored-ext4-write8-p5-new_v7.json` in the [MVCC preparation rerun manifest](artifacts/rfc-025-v7-mvcc-preparation-ext4-8writers-20261025-rerun1-runs.tsv).
The new v7 peak is `scored-ext4-write8-p3-new_v7.json` in this experiment.
It improves by 18.6% over the previous retained v7's highest score, but remains
18.4% below the historical v4 peak. V4 parity is still unmet.

The [point-write driver](artifacts/rfc-025-v7-shared-extent-init-ext4-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-shared-extent-init-ext4-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-shared-extent-init-ext4-8writers-20261025.jsonl)
retain all measurements. The [analysis script](artifacts/rfc-025-v7-shared-extent-init-20261025-analyze.py)
checks raw-record hashes and reproduces the paired comparisons and pinned
historical peaks in the [summary](artifacts/rfc-025-v7-shared-extent-init-20261025-summary.json).

### Large-batch guard

A separate batch64 comparison uses eight writers and 100,000 1-KiB puts per
run, the same four arms, four warmups, and five alternating scored pairs.
Unconditional initialization reduces the highest observed v7 result from
188,322 to 177,703 puts/s (-5.6%). Median paired raw and normalized throughput
changes are `0.719x` and `0.717x`; SSD stalls also affect multiple arms.
This is sufficient reason to avoid zero filling every large batch. The
[initial batch64 driver](artifacts/rfc-025-v7-shared-extent-init-ext4-batch64-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-shared-extent-init-ext4-batch64-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-shared-extent-init-ext4-batch64-8writers-20261025.jsonl)
retain this failed control.

The adjusted policy follows v4's existing 64-KiB threshold: groups containing
small DATA writes initialize their requested allocation; groups containing
only large writes use allocation alone. The shared cursor advances across
skipped ranges too, so a later small write or FRONTIER can never zero a
range already handed to DATA I/O. The runtime does not infer initialization
from file length alone.

The final focused correctness run passes 60 tests outside the sandbox, including
recovery of exact batches across extent growth, preservation of earlier DATA
and FRONTIER bytes, initialization of a preallocated physical tail, partial
frame caps, close/rotation, and an injected initialization failure that
poisons later work while preserving the earlier durable prefix. Formatting
and all-target, all-feature Clippy with warnings denied pass.

### Retained policy: small writes only

The final binary is
`06efc8d7dc473ddfd770832c9075c6cfffdc9ab05df13460481d4f2302f2625b`.
The complete point-write and batch64 comparisons are repeated against the
same preserved baseline, with matching settings and both v4 controls. The
point-write results are:

| Arm | Highest puts/s | Median puts/s |
| --- | ---: | ---: |
| v4, preserved baseline | 16,977 | 16,790 |
| v4, retained candidate | 20,420 | 17,129 |
| v7, preserved baseline | 12,054 | 11,907 |
| v7, retained candidate | 15,256 | 14,592 |

The final candidate's highest score improves by 21.1% over the previous
retained v7's historical peak of 12,596 puts/s, and by 26.6% over the baseline
peak in this repeat. The v4 control establishes a new comparable high of
20,420 puts/s, replacing the earlier 18,304-put/s reference. Against that
highest v4 score, current v7 reaches 74.7%, leaving a 25.3% throughput gap.
This is the requested best-score comparison; it does not establish sustained
throughput or mitigate the underlying SSD stalls.

Across all five pairs, the median raw v7 improvement is 21.1%; the median
v4-normalized improvement is 15.7%. The normalized changes are
`1.053, 1.157, 1.267, 1.175, 0.671`. Pair 5 contains another slowdown, with
old/new v7 at 6,946 / 4,633 puts/s; it is included. Median paired v7 p50,
p99, and CPU ratios are `0.797x`, `1.113x`, and `0.929x`. The median paired
candidate v7:v4 throughput ratio is `0.839x`. The peak comparison and paired
median answer different questions; neither demonstrates v4 parity, and p99
improvement is not established.

The [retained point-write driver](artifacts/rfc-025-v7-small-extent-init-ext4-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-small-extent-init-ext4-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-small-extent-init-ext4-8writers-20261025.jsonl)
identify each run and executable. The shared analysis script verifies both
final datasets and the historical reference records. The tmpfs path does not
enable this extent initialization; these new performance measurements cover
ext4 only and do not replace the earlier tmpfs writer sweep.

The repeated batch64 results retain all scored runs:

| Arm | Highest puts/s | Median puts/s |
| --- | ---: | ---: |
| v4, preserved baseline | 482,410 | 330,832 |
| v4, retained candidate | 379,436 | 354,685 |
| v7, preserved baseline | 187,469 | 164,144 |
| v7, retained candidate | 181,171 | 175,007 |

The highest v7 score is 3.4% below the baseline peak in this repeat (3.8%
below the earlier baseline peak of 188,322). Median paired raw v7 throughput
changes by `0.990x`, while the v4-normalized change is `1.099x`; normalized
pairs are `1.119, 0.902, 0.901, 1.099, 1.259`. The four-arm controls vary
widely: the first two pairs have v4 throughput around 67,606–74,345 puts/s,
while the final baseline v4 reaches 482,410 puts/s. Both v7 builds also slow
to roughly 54,000–61,000 puts/s. The retained policy avoids unconditional
zero filling for large DATA groups, but this experiment does not establish
batch64 improvement, absence of regression, or parity with v4.

The [retained batch64 driver](artifacts/rfc-025-v7-small-extent-init-ext4-batch64-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-small-extent-init-ext4-batch64-8writers-20261025-runs.tsv),
and [raw JSONL](artifacts/rfc-025-v7-small-extent-init-ext4-batch64-8writers-20261025.jsonl)
are included in the same reproducible summary. All benchmark scratch
databases are removed after each run; reports and raw measurements remain.

Final validation uses:

```bash
cargo nextest run --package kv-engine --lib --all-features --no-fail-fast \
  -E 'test(wal::parallel_runtime::tests::) | test(v7_sync_write) | test(v7_native) | test(v7_rotation) | test(v7_async_large_batch)'
cargo clippy --package kv-engine --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Nextest run `b587afba-f6a2-47d2-bc42-c66048f1b4fc` executes outside the
sandbox and passes all 60 selected tests. The growth test verifies that
requesting initialization after a large DATA group preserves the entire
physical image. The injected-failure test fills an extent with small writes,
then fails its next initialization and recovers every earlier acknowledged
batch. Clippy, formatting, and targeted spelling checks pass.
