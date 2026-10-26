# RFC 025: why v7 trails v4, 2026-10-26

The largest measured batch CPU difference is ordered logical hashing before
DATA submission. V7's packer spends about 78% of its sampled user CPU in
SHA-256 in the longer batch profile. V4 has no equivalent prefix hash on that
thread. Coalescing also delays the v7 critical path, but removing the fast-sync
wait increases point CPU and sampled tail latency, so that policy is rejected.
V4 parity remains unmet.

## Source and thread evidence

Both formats use the ordered packer, io_uring worker, and sync coordinator.
The retained v7 implementation adds work at these boundaries:

- Producers canonicalize PITR entries, reserve their resources, and prepare the
  seal index before ordered admission.
- The packer frames DATA, computes frame CRCs, and updates the logical SHA-256
  prefix in ticket order before submitting each group. V4 submits already
  encoded producer buffers.
- The coordinator writes a 4-KiB embedded FRONTIER before `fdatasync`. This is
  still one sync per durability advance.
- Reservation retirement and prefix bookkeeping run before the durable
  watermark is published.
- On ext4, v4 has background extent lookahead. V7 synchronously prepares its
  shared DATA/FRONTIER extent prefix. The allocation quantum is 1 MiB. Reusing
  v4's initializer directly would be unsafe: v7 requires shared tracking and
  exclusion so initialization cannot overwrite DATA or a FRONTIER.

Three unscored tmpfs profiles use the diagnostic executable
`target/release/write-perf-v7-fast-sync-wait`, SHA-256
`b2de5e1c73fdf8abccca325dd0ebd4f65bc3dac96dd2b3533df6575204be248d`.
Only v7's fast-sync wait is zero in this executable; v4 is the same control
implementation. Batch64 uses 750,000 puts; point uses 100,000. All use eight
writers, 1-KiB values, and a 2-GiB SST target. Sampling uses
`cpu-clock:u`, 997 Hz, and DWARF call graphs. Profiles include setup/close and
are excluded from scored comparisons.

| Profile | Timed wall ms | Timed process CPU ms | Sampled packer user CPU ms | Packer SHA self ms |
| --- | ---: | ---: | ---: | ---: |
| v4 batch64 | 466.9 | 2,656.4 | 43.1 | 0.0 |
| v7 batch64 | 815.3 | 2,567.8 | 492.5 | 383.1 |
| v7 point | 676.6 | 3,212.1 | 364.1 | 73.2 |

The v7 batch packer has 491 samples; 382 have SHA-256 as their leaf symbol.
This identifies serialized work on that thread, even though total timed
process CPU is lower than v4 in this capture. Samples are estimates of user
CPU, not a decomposition of wall time. V4's main-thread SHA/SST samples include
close work outside the timed writes; they do not establish a v4 write-path
prefix-hashing cost. CPU placement was not captured, so these profiles do not
support a P-core/E-core explanation.

The [profile analyzer](artifacts/rfc-025-v7-critical-path-20261026-analyze.py)
reproduces the [thread summary](artifacts/rfc-025-v7-critical-path-20261026-summary.json)
from compressed sample records. The [capture identities](artifacts/rfc-025-v7-critical-path-20261026-capture.json)
retain local perf-data hashes; the [driver](artifacts/rfc-025-v7-critical-path-20261026-profile.py),
[command manifest](artifacts/rfc-025-v7-critical-path-20261026-runs.tsv), and
[raw benchmark output](artifacts/rfc-025-v7-critical-path-20261026.jsonl)
remain available.

| Profile | Samples | Flat report | Call graph |
| --- | --- | --- | --- |
| v4 batch64 | [TSV.gz](artifacts/rfc-025-v7-critical-path-20261026-v4-batch64-samples.tsv.gz) | [text](artifacts/rfc-025-v7-critical-path-20261026-v4-batch64-flat.txt) | [text](artifacts/rfc-025-v7-critical-path-20261026-v4-batch64-calls.txt) |
| v7 batch64 | [TSV.gz](artifacts/rfc-025-v7-critical-path-20261026-v7-batch64-samples.tsv.gz) | [text](artifacts/rfc-025-v7-critical-path-20261026-v7-batch64-flat.txt) | [text](artifacts/rfc-025-v7-critical-path-20261026-v7-batch64-calls.txt) |
| v7 point | [TSV.gz](artifacts/rfc-025-v7-critical-path-20261026-v7-point8-samples.tsv.gz) | [text](artifacts/rfc-025-v7-critical-path-20261026-v7-point8-flat.txt) | [text](artifacts/rfc-025-v7-critical-path-20261026-v7-point8-calls.txt) |

## Coalescing ablation: rejected

The experiment changes only `V7_FAST_SYNC_COALESCE_WAIT` from 100 microseconds
to zero. Bounded completion capture, slow-sync coalescing, DATA/FRONTIER bytes,
poisoning, and one-sync ordering remain intact. It removes waiting; it does
not remove required hashing or checksum work.

Each CPU cell has four warmups and three alternating four-arm pairs:
retained/candidate binaries, each with v4 and v7. Settings match the profiles.
Batch latency sampling every ten batches provides 1,176 observations; point
sampling every 100 operations provides 1,000. Fresh tmpfs databases and
three-second gaps isolate runs. Builds, checks, and profiling do not overlap
scoring. All 24 scored records and eight warmups remain included.

| Cell | Paired v7 throughput | v4-normalized throughput | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Batch64 | 1.083x | 1.067x | 0.991x | 0.898x | 0.880x |
| Point | 1.169x | 1.239x | 1.176x | 0.808x | 1.861x |

Ratios are medians of paired ratios. Normalization compares each v7 rate
relative to its same-executable v4 control. CPU is timed process user plus
system time. Point CPU rises 17.6% and sampled p99 rises 86.1%, so the policy
is not adopted and the planned SSD qualification is cancelled.

| Tmpfs batch64 median | v4 control | Retained v7 | Zero-wait v7 |
| --- | ---: | ---: | ---: |
| Timed wall ms | 441.0 | 878.3 | 819.4 |
| Timed CPU ms | 2,253.3 | 2,553.5 | 2,538.7 |
| Coalescing wait ms | 0.0 | 435.1 | 0.0 |
| Sync count | 7,536 | 3,152 | 3,946 |

The table's v4 column is the retained executable's control. Pipeline counters
are reset after setup and captured before drain/close. Counters overlap other
work and their medians cannot be added into a causal wall-time decomposition.
Removing about 435 ms of recorded wait does not save 435 ms of wall time:
group size and sync frequency also change.

## Correctness and reproduction

The source base is `20b011b1`. The diagnostic release build passes the full
`cargo make check`: all 1,963 tests pass, with zero skips, retries, or slow
tests (nextest `d5b3bd0e-02f7-4638-91c1-642a8a1b1967`). The original runtime
is restored byte-for-byte with its modification time refreshed after rejection.
A fresh full check passes 1,963 tests, including one unrelated compaction
retry after a timeout (nextest `8168114b-0c79-47e3-9182-93d2d65ea836`). Logs
remain under `target/wal-opt/20261026-v7-scheduling-diagnosis`.

The [diagnostic patch](artifacts/rfc-025-v7-fast-sync-wait-20261026-diagnostic.patch),
[128-input source manifest](artifacts/rfc-025-v7-fast-sync-wait-20261026-source-sha256.tsv),
and [binary identities](artifacts/rfc-025-v7-fast-sync-wait-20261026-binaries.json)
identify the measured executable. The manifest SHA-256 is
`14ea7cef3fe3e854502f0bb3efca4609ab551e2f93bbc853413a0c45b179d4c4`.
The retained executable is
`target/release/write-perf-v7-validated-logical-batch`, SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.

The [comparison analyzer](artifacts/rfc-025-v7-fast-sync-wait-20261026-analyze.py)
verifies raw hashes, identities, settings, sample counts, and complete pairs to
reproduce the [summary](artifacts/rfc-025-v7-fast-sync-wait-20261026-summary.json).
The common [driver](artifacts/rfc-025-v7-fast-sync-wait-20261026-run.py) refuses
to overwrite captures and removes its disposable databases. No SSD runs are
represented as completed. All 35 diagnostic/profile run IDs are distinct.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs batch64 | [script](artifacts/rfc-025-v7-fast-sync-wait-tmpfs-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-fast-sync-wait-tmpfs-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-fast-sync-wait-tmpfs-batch64-20261026.jsonl) |
| tmpfs point | [script](artifacts/rfc-025-v7-fast-sync-wait-tmpfs-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-fast-sync-wait-tmpfs-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-fast-sync-wait-tmpfs-point8-20261026.jsonl) |

The next candidate can overlap ordered logical hashing with later DATA framing
and I/O while preserving exact hash bytes and the sync-before-publication gate.
This report does not qualify RFC Stage 8 or alter the format/default policy.

Source patches use zero-context diffs; apply them to the stated base with
`git apply --unidiff-zero`. The verifier also checks exact reconstructed bytes.

[WAL documentation](../README.md).
