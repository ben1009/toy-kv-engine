# RFC 025 Stage 8: exploratory v7 Parallel versus baseline v6 Leader, 2026-10-25

**Status: exploratory; not a qualification result.** This is the final planned
comparison leg: candidate v7 Parallel with PITR enabled against the pinned
pre-v7 v6 Leader implementation. It measures the end-to-end change across
source revisions, not just the format change. There are three paired repeats
per workload and filesystem, without same-binary null controls or confidence
intervals.

## Executables and environment

- Baseline source: `c7aa39cd56f37b767352d946bab551727b64b4a5`, a pre-v7
  revision. At this revision `WAL_V5_VERSION` is the wire value 6 and is the
  PITR writer format.
- Candidate source: `f08f4cd7f0bac424d9732837401a89a76ec5b5b6`.
- Baseline `write-perf` binary SHA-256:
  `eec8b0e3e34315b4e5d991bc08c82ce10143a3486a4e1054591223d944d6b589`.
- Candidate `write-perf` binary SHA-256:
  `d12902ba53e16f4a5c3b18dde6b2fd758169140e14ea9b6ab98f3f369c927023`.
- Build command for both:
  `cargo build --locked --release --package kv-engine --bin write-perf --features bench`
  with `rustc 1.100.0-nightly (6bb1652a0 2026-09-22)`.
- Host: x86_64 Linux 6.12.71-1-lts, Intel Core i9-13900T, 32 logical CPUs.
- Storage: `/dev/shm` tmpfs (32 GiB); ext4 on `/dev/nvme0n1p3`, the
  Solidigm SSDPFKNU010TZ.

## Protocol

| Workload | Configuration |
| --- | --- |
| Single-writer WAL puts | 5,000 puts, 1 writer, 1 KiB values, 1 GiB SST target |
| Multi-writer WAL puts | 50,000 puts, 4, 8, or 16 writers, 1 KiB values, 1 GiB SST target |

Both arms enable PITR. The baseline uses its pinned v6 default and Leader I/O;
the candidate explicitly selects v7 and Parallel I/O. The old baseline JSON
reports `wal_io_mode=leader` but predates the `wal_format_version` measurement
field, so its v6 identity is established by the pinned source revision. The
candidate JSON reports format 7 and Parallel mode.

Each filesystem/workload cell had one unscored warmup per arm, then three pairs
ordered baseline/candidate, candidate/baseline, baseline/candidate, with a
fixed 3-second idle between runs. Every run used a fresh database. Put latency
was sampled every 100 puts. PITR setup and the v7 capacity-checked transition
occur outside the timed write interval. The archive interval is one hour, and
each run stays within one Active segment. This screen excludes setup time,
rotation, archive work, and competing archive I/O.

The [run manifest](artifacts/rfc-025-v7-parallel-vs-baseline-v6-20261025-runs.tsv)
maps exact commands and run IDs to the original JSON stdout records in the
[JSONL artifact](artifacts/rfc-025-v7-parallel-vs-baseline-v6-20261025.jsonl).
The [1/4-writer driver](artifacts/rfc-025-v7-parallel-vs-baseline-v6-20261025-run.sh)
and the [8/16-writer follow-up driver](artifacts/rfc-025-v7-parallel-vs-baseline-v6-20261025-writers-8-16-run.sh)
preserve the two run sequences. The artifact contains 16 warmups and 48 scored
records. Its SHA-256 is
`3d129706c6206208644ca59d94238eb08f93e3246c8b40257c54dbc659edf009`; the
manifest SHA-256 is
`2ef7c11f3f05232fe53129b5c15d5703f3f5c0eaf5ec5cd6cdcfdd4e130e9786`.

## Results

Arm throughput values are medians of three runs. Paired ratios are candidate
v7 Parallel divided by baseline v6 Leader within each pair; the median follows
the list. A throughput ratio above 1 favors the candidate. Latency ratios above
1 mean the candidate is slower.

| Filesystem / writers | Baseline / candidate median puts/s | Paired throughput ratios (median) | Paired p50 ratios (median) | Paired p99 ratios (median) |
| --- | ---: | --- | --- | --- |
| tmpfs / 1 | 61,953 / 21,475 | 0.387, 0.347, 0.320 (0.347) | 1.552, 1.734, 4.117 (1.734) | 3.109, 3.234, 1.130 (3.109) |
| tmpfs / 4 | 173,707 / 68,655 | 0.385, 0.408, 0.381 (0.385) | 3.142, 2.849, 3.132 (3.132) | 1.955, 0.719, 1.601 (1.601) |
| tmpfs / 8 | 173,999 / 64,007 | 0.332, 0.368, 0.432 (0.368) | 3.832, 4.153, 3.091 (3.832) | 2.049, 0.882, 2.003 (2.003) |
| tmpfs / 16 | 72,697 / 56,941 | 0.783, 0.772, 0.778 (0.778) | 1.518, 1.394, 1.446 (1.446) | 1.402, 0.999, 1.339 (1.339) |
| ext4 / 1 | 1,845 / 1,677 | 0.885, 0.909, 0.921 (0.909) | 1.114, 1.097, 1.089 (1.097) | 29.655, 1.165, 1.510 (1.510) |
| ext4 / 4 | 3,754 / 6,490 | 1.722, 1.251, 1.729 (1.722) | 0.566, 0.593, 0.556 (0.566) | 0.670, 0.769, 0.567 (0.670) |
| ext4 / 8 | 7,152 / 11,119 | 1.524, 1.626, 1.540 (1.540) | 0.646, 0.622, 0.646 (0.646) | 0.955, 0.689, 0.873 (0.873) |
| ext4 / 16 | 13,644 / 15,218 | 1.113, 1.117, 1.088 (1.113) | 0.895, 0.863, 0.876 (0.876) | 1.077, 1.020, 0.896 (1.020) |

The candidate is slower in every tmpfs throughput pair at all four writer
counts. Median paired throughput ratios are 0.347, 0.385, 0.368, and 0.778 for
1, 4, 8, and 16 writers respectively. On ext4, the single-writer ratio is
0.909, while the 4-, 8-, and 16-writer candidates are faster in every pair,
with median ratios of 1.722, 1.540, and 1.113. The ext4 four-writer second pair
is slower in absolute terms for both arms (1,403 baseline versus 1,754
candidate puts/s), consistent with a device disturbance; that pair remains
included.

The candidate's p99 for the first ext4 single-writer pair is an outlier at
34.2 ms, versus 1.15 ms for baseline. The other two candidate p99 values are
1.36 ms and 1.76 ms. All measurements are retained. The small sample has no
same-binary controls, so the outlier and the apparent multi-writer gains need
further qualification.

Counters show workload-dependent frontier coalescing. With one writer, both
arms issue 5,000 syncs, while v7 writes 5,000 frontiers (20,480,000 bytes). The
v7 WAL is about twice the v6 size: 41,955,328 bytes on tmpfs and 42,999,808 on
ext4, versus 20,975,616 bytes for v6. With 4 and 8 writers on tmpfs, v7 issues
about 35,000 syncs versus 25,000 and 16,000–17,000 for v6; with 16 writers the
counts are about 33,000–38,000 versus 16,000. The resulting v7 WALs are about
340–352 MB at 4/8 writers and 341–361 MB at 16 writers, versus 205,524,992
bytes for v6. On ext4, v7 issues fewer syncs at 4/8/16 writers: about
12,600/6,400/3,600 versus 23,100–24,200/12,400/6,200 for v6. V7 writes about
51–53 MB, 26 MB, and 15 MB of frontier data at those counts, with WAL sizes of
258–259 MB, 233 MB, and 221 MB respectively; v6 WALs are 205,524,992 bytes.

## Interpretation and remaining gate

This end-to-end screen shows no uniform performance result across 1/4/8/16
writers. V7 Parallel loses throughput at every writer count on tmpfs and loses
about 9% for single-writer ext4. Ext4 favors v7 at 4/8/16 writers, with the
largest gains at 4 and 8. The frontier and sync counters align with that split:
v7 issues more syncs than v6 on tmpfs at 4/8/16 writers, but fewer on ext4 at
those counts. The ext4 single-writer p99 outlier and missing same-binary
controls mean these measurements do not establish a general no-regression
claim or an adoption decision.

All five planned comparison legs now have exploratory reports, and this leg
covers all four planned writer counts for single puts. Stage 8 is still open:
batch64, value-size variants, rotation, checkpoints, archive backpressure,
reads/scans/mixed workloads, same-binary stability controls, confidence
intervals, archive-I/O controls, hash/index CPU instrumentation,
group/coalescing coverage, and reopen/RTO measurements remain to be completed.
No adoption decision follows from this comparison.
