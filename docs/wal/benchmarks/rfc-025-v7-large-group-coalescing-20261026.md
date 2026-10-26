# RFC 025: selective v7 group coalescing lookup, 2026-10-26

**Result: rejected at the CPU screen; no SSD qualification ran.**

The earlier [waiting-policy ablation](rfc-025-v7-critical-path-20261026.md)
helps batch throughput but increases point CPU and tail latency. This candidate
keeps the retained packer hashing and precomputes whether every logical batch
in each packed group is at least 64 KiB. After a cheap sync, it skips the short
coalescing wait only when every unsynced written group has that flag and an
endpoint snapshot exists. Point/mixed waves keep 100 microseconds, and slow
syncs keep 400. Bounded completion capture, encoded bytes, ownership, and
sync-before-ACK ordering remain unchanged.

Tests exercise complete unsynced waves, mixed membership, missing endpoints,
durable cutoffs, and fresh classification of successive groups using real
encoded logical inputs. `cargo make check` passes all 1,965 tests with zero
skips/retries and one unrelated slow leveled compaction test: nextest
`6b518b7a-3985-44e7-b483-42edf312824c`. Release build passes. The initial fixture
compile needed an explicit Option conversion before the successful full check.
Logs remain under `target/wal-opt/20261026-v7-large-group-coalescing`.

| Tmpfs cell | Paired v7 throughput | v4-normalized | Process CPU | Sampled p50 | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Batch64 | 1.082x | 1.023x | 0.977x | 0.888x | 1.214x |
| Point, 8 writers | 0.935x | 0.941x | 1.069x | 1.048x | 0.974x |

Each cell uses four warmups and three alternating four-arm pairs with
retained/candidate v4 and v7. Batch64 writes 750,000 puts with latency sampling
every ten batches (1,176 observations). Point writes 100,000 puts, sampling
every 100 operations (1,000 observations). Eight writers use 1-KiB values and a
2-GiB SST target. Databases are fresh tmpfs directories with three-second gaps;
checks/builds/profiling do not overlap scoring. Setup/close are outside the
timed interval, and pipeline counters are reset after setup and captured
before drain/close. All 24 scored records and eight warmups remain.

Ratios are medians of paired ratios, with each v7/v4 rate normalized to its
same-executable control. CPU is timed process user plus system time. Retention
requires CPU batch raw/normalized gains, batch CPU growth at most 5%, and point
raw loss at most 5% before the solo/SSD qualification may proceed. Point raw
throughput falls 6.5%, so the CPU gate fails and planned qualification stops.
There are no extra repeats or peak-selected records.

The new group lookup takes admission/durability locks in the coordinator's
point path. That introduces an avoidable contention opportunity. The follow-up
uses a cached negative hint so point-only waves avoid the lookup; positive
hints still require complete snapshot validation. This is a source difference
to test, not proof that it accounts for every measured change.

The source base is `20b011b1`. The candidate executable is
`target/release/write-perf-v7-large-group-coalescing`, SHA-256
`c697b978433cc91dd02c90a0ea0b900bab68a3a6223afa1da7582c8cf2d085b0`.
The [128-input source manifest](artifacts/rfc-025-v7-large-group-coalescing-20261026-source-sha256.tsv)
has SHA-256 `35ad438a541c6595024a0173345bcdb3345f26d4678e05ce0f821eff81f9eab9`.
The [rejected patch](artifacts/rfc-025-v7-large-group-coalescing-20261026-rejected.patch)
reconstructs those inputs. The retained executable is SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
Both release builds use `cargo build --release --package kv-engine --bin write-perf --features bench`.

The [driver](artifacts/rfc-025-v7-large-group-coalescing-20261026-run.py),
[binary identities](artifacts/rfc-025-v7-large-group-coalescing-20261026-binaries.json),
[analyzer](artifacts/rfc-025-v7-large-group-coalescing-20261026-analyze.py), and
[summary](artifacts/rfc-025-v7-large-group-coalescing-20261026-summary.json)
preserve the entire screen. All 32 run IDs are distinct.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs batch64 | [script](artifacts/rfc-025-v7-large-group-coalescing-tmpfs-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-large-group-coalescing-tmpfs-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-large-group-coalescing-tmpfs-batch64-20261026.jsonl) |
| tmpfs point | [script](artifacts/rfc-025-v7-large-group-coalescing-tmpfs-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-large-group-coalescing-tmpfs-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-large-group-coalescing-tmpfs-point8-20261026.jsonl) |

The eleven earlier optimizations remain. V4 parity and the complete RFC Stage 8
gate remain unmet.

Source patches use zero-context diffs; apply them to the stated base with
`git apply --unidiff-zero`. The verifier also checks exact reconstructed bytes.

[WAL documentation](../README.md).
