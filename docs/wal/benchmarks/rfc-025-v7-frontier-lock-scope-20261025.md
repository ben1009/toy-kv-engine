# RFC 025: encode FRONTIER frames outside admission, 2026-10-25

**Result: retain the shorter FRONTIER critical section; v4 parity remains unmet.**

The change improves median paired ext4 point throughput 3.7% and tmpfs batch64
throughput 9.8% in the initial screen. SSD batch64's raw throughput improves
3.1% across ten pairs, but its v4-normalized improvement is only 1.0%, and the
confirmation does not reproduce the screen's CPU decrease. These results are
exploratory and do not pass the RFC 025 performance gate.

## Change and correctness

Previously the v7 sync coordinator held WAL admission while allocating a
4-KiB FRONTIER buffer, finalizing the captured logical prefix, encoding the
frame/CRC, and hashing the complete frame. The coordinator now captures the
same predecessor and prefix, reserves the marker, and publishes its physical
end under admission, then releases that mutex before encoding and hashing.
Producers may assign DATA after the reserved marker while that work finishes.
The single coordinator still owns the entire marker/sync cycle, and encoding
or I/O failure poisons that reservation before any later ticket can be ACKed.

The DATA-completion target, predecessor, digest preimages, canonical wire bytes,
one-sync protocol, and recovery rules remain the same. No additional runtime
state or public configuration is introduced. The change is independently
committed as `f3ffb1c9`, following baseline `8a6e5317`.

`cargo make check` passes outside the io_uring sandbox: all 1,958 tests pass,
with zero skips and retries, alongside formatting, dependency ordering,
all-target/all-feature Clippy, unused-dependency checks, and typos. Nextest run:
`9407d8bf-bac3-4789-81d5-ad19072ea3a6`. The local log is
`target/wal-opt/20261025-v7-admission-gate/candidate-check.log`.

A fresh baseline CPU profile again shows producer mutex slow paths and
coordinator SHA work, motivating a shorter critical section. Its three
runs include setup/close, and v4 also drains its memtable after the measured
write interval. Their sampled percentages are diagnostic rather than an
attribution of throughput differences. All three are excluded from scoring.
The [commands and hashes](artifacts/rfc-025-v7-cached-extents-cpu-profile-20261025-runs.tsv),
[JSONL](artifacts/rfc-025-v7-cached-extents-cpu-profile-20261025.jsonl), and
[v7 point](artifacts/rfc-025-v7-cached-extents-cpu-profile-20261025-v7.txt),
[v4 point](artifacts/rfc-025-v7-cached-extents-cpu-profile-20261025-v4.txt), and
[v7 batch64](artifacts/rfc-025-v7-cached-extents-cpu-profile-20261025-v7-batch64.txt)
reports retain the evidence. Only display padding is stripped from the text
reports; the original perf captures/reports remain under the local task folder.

## Protocol and paired results

Both binaries are built with:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

- Baseline, retained extent cache: `61d59ba0e739ec67338979a4a4a259e06d1940c1e92c49ff72cf38bdd32fb75e`.
- Candidate, FRONTIER encoding outside admission: `5fad9ee139f12c9c5bff511750f4752e93a914f9fb716b1bc9954aea7323c6bb`.
- Candidate [source manifest](artifacts/rfc-025-v7-frontier-lock-20261025-source-sha256.tsv): `c387af4b7e69ee7f015f438222245424f43d0347c1c780ef61956dff131593cd`.

The host remains an Intel Core i9-13900T with 32 logical CPUs and Linux
6.12.71-1-lts, using tmpfs and ext4 on the Solidigm SSDPFKNU010TZ. Each cell has
one warmup per arm followed by five alternating four-arm pairs: old/new binary
with ordinary v4 Parallel, and old/new binary with PITR v7 Parallel. Every run
uses a fresh database, 1-KiB values, a 1-GiB SST target, and latency sampling
once per 100 operations. Single-put cells issue 50,000 puts; batch64 cells
issue 100,000 puts. Setup and close are outside the timed write interval.

Tmpfs runs idle three seconds between records. Ext4 runs cool for 30 seconds
initially and ten seconds between records. Builds, tests, and profile recording
finish before the scored comparisons. A fixed additional five-pair SSD batch64
confirmation follows the screen; no further SSD repeats are selected by score.
There are 120 scored records and 24 warmups across six cells. All remain
included, including unfavorable controls and stalls.

The table reports medians of paired candidate/baseline ratios. Its throughput
and CPU ratios are different from ratios of the displayed medians. CPU means
measured process user plus system time. The normalized ratio compares each
v7 result with its same-binary v4 control before comparing binaries.

| Comparison | Baseline / candidate v7 median puts/s | Raw throughput | v4-normalized throughput | CPU | Sampled p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs, 8 writers, single puts | 116,857 / 117,767 | 1.026x | 1.005x | 0.981x | 0.763x |
| ext4, 8 writers, single puts | 15,004 / 15,314 | 1.037x | 1.035x | 0.980x | 1.078x |
| tmpfs, 8 writers, batch64 | 628,084 / 658,463 | 1.098x | 1.183x | 0.917x | 1.085x |
| ext4, 8 writers, batch64, screen | 269,135 / 322,001 | 1.034x | 1.033x | 0.929x | 0.959x |
| ext4, 8 writers, batch64, confirmation | 314,216 / 325,737 | 1.027x | 0.987x | 1.019x | 1.040x |
| tmpfs, 1 writer, single puts | 38,611 / 40,537 | 1.021x | 0.961x | 0.955x | 1.049x |

Across all ten SSD batch64 pairs, raw throughput changes by `1.031x`, the
normalized ratio by `1.010x`, CPU by `1.008x`, and sampled p99 by `0.993x`.
The confirmation alone has a `0.987x` normalized ratio and `1.019x` CPU ratio;
the initial SSD CPU reduction therefore does not replicate. Sampled batch64
p50 rises 15.4% across ten pairs. Each batch64 record has only 16 latency
samples, so these batch latency results are sparse. Point-write records have
504 samples with eight writers, and 500 with one writer.

Solo tmpfs raw throughput rises 2.1%, while its v4-normalized throughput falls
3.9%; CPU falls 4.5%, and sampled p99 rises 4.9%. Tmpfs batch64's normalized
18.3% gain also includes movement in the v4 controls. The retained result is
a modest ext4 point improvement and a CPU-side batch improvement on tmpfs,
with mixed controls rather than uniform progress across workloads.

SSD stalls remain substantial. In the confirmation's second pair, candidate
v7 reaches only 54,773 puts/s while baseline v7 reaches 302,565. Several v4
batch64 controls fall below 75,000 puts/s. Recorded SSD temperature ranges
are retained in the analysis; temperature readings alone do not identify the
stall cause or establish absence of throttling.

## Highest comparable scored results

Warmups and profiled runs are excluded. The reference highs come from the
preceding [extent-cache report](rfc-025-v7-extent-readiness-cache-20261025.md)
and its pinned v4 references. This table preserves those stronger results
alongside this pass's matched controls, as requested.

| Build / format | Single puts, 8 writers | Batch64, 8 writers |
| --- | ---: | ---: |
| Highest comparable prior v4 Parallel | 20,420 | 482,410 |
| Highest prior retained v7 extent cache | 15,759 | 391,992 |
| Extent-cache baseline in this pass | 15,366 | 360,870 |
| FRONTIER lock candidate in this pass | 15,750 | 370,652 |

The candidate peaks are 22.9% and 23.2% below the highest v4 references.
The batch64 candidate peak is 2.7% above the matched baseline peak, but 5.4%
below the earlier retained v7 high. These measurements establish neither a
new historical v7 record nor parity with v4, and do not establish how the
candidate would compare with that earlier v7 run under identical conditions.
The lighter critical section is retained for the measured point and tmpfs
batch improvements; the full Stage 8 workload/recovery qualification remains
open.

## Reproduction artifacts

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs, 8 writers | [script](artifacts/rfc-025-v7-frontier-lock-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frontier-lock-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frontier-lock-tmpfs-8writers-20261025.jsonl) |
| ext4, 8 writers | [script](artifacts/rfc-025-v7-frontier-lock-ext4-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frontier-lock-ext4-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frontier-lock-ext4-8writers-20261025.jsonl) |
| tmpfs, batch64 | [script](artifacts/rfc-025-v7-frontier-lock-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frontier-lock-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frontier-lock-tmpfs-batch64-8writers-20261025.jsonl) |
| ext4, batch64 screen | [script](artifacts/rfc-025-v7-frontier-lock-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frontier-lock-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frontier-lock-ext4-batch64-8writers-20261025.jsonl) |
| ext4, batch64 confirmation | [script](artifacts/rfc-025-v7-frontier-lock-confirm-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frontier-lock-confirm-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frontier-lock-confirm-ext4-batch64-8writers-20261025.jsonl) |
| tmpfs, 1 writer | [script](artifacts/rfc-025-v7-frontier-lock-tmpfs-1writer-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frontier-lock-tmpfs-1writer-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frontier-lock-tmpfs-1writer-20261025.jsonl) |

The [analyzer](artifacts/rfc-025-v7-frontier-lock-20261025-analyze.py) verifies
binary and raw-record hashes, workload settings, operation/sample counts, and
reproduces the [summary](artifacts/rfc-025-v7-frontier-lock-20261025-summary.json),
including every winning run ID and pipeline counters. Disposable benchmark
databases are removed after every run. The retained candidate executable is
`target/release/write-perf-v7-frontier-lock`; the default `write-perf` executable
has the same candidate hash. All earlier kept optimizations remain in place.

[WAL documentation](../README.md).
