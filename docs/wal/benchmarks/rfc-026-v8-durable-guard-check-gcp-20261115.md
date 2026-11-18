# RFC 026 V8 Batched Durable-Guard Check GCP A/B — 2026-11-15

Baseline source commit: `daf92c6fa0938db287eb9ff98ebd434abfaa291f`

Candidate source commit: `9ae8ee2090953e6f472b03f65df812e198460e01`

Candidate runtime patch SHA-256: `048b6ff73fc5b67c609cf1c8418d3ef3344c048287c52b72b941efa162f6cccf`

## Scope and setup

This comparison measures the V8 producer-guard retirement change that checks a
completed durable ticket prefix once, rather than reacquiring the coordinator
state lock for each covered guard. The check retains the per-guard fail-safe
poison behavior for tickets outside the checked prefix.

Both builds ran on the same `c4-standard-16-lssd` host with ext4 on Local SSD
RAID0 (`rw,noatime,stripe=256`), Linux `7.0.0-1017-gcp`, and
`nightly-2026-09-23`. Each run issued 100,000 Point puts with 16 writers and
1 KiB values. V8 used Direct FRONTIER I/O, a fixed 400 μs coalescing override,
and 4 MiB sliding `fallocate` preparation. Profiling and detailed diagnostics
were off; operation latency was sampled every 100 puts.

The test comprised five counterbalanced baseline/candidate pairs (10 runs).
The baseline and candidate binaries were built on the same host:

- Baseline binary SHA-256: `cbc78803874f465d9bc85322b35e4d30961dea37e4d8a75d4884e422fe21af9a`
- Candidate binary SHA-256: `a312a6ff8920178a9429c2422885c8211fa551861236ca9d29a66ad354709c23`

## Results

Each cell is the median across the five runs for that variant. The paired
throughput delta is the median of the five within-pair percentage changes.

| Metric | Baseline | Batched guard check | Change |
| --- | ---: | ---: | ---: |
| Throughput | 34.068k puts/s | 33.978k puts/s | −0.26% between medians |
| Paired throughput delta | — | — | −0.16%; candidate won 2/5 pairs |
| Process CPU per put | 69.47 μs | 66.48 μs | −4.3% |
| Sync count | 6,335 | 6,318 | −0.27% |
| Put latency p50 | 0.463 ms | 0.468 ms | +1.0% |
| Put latency p95 | 0.614 ms | 0.607 ms | −1.3% |
| Sampled Put latency p99 | 1.748 ms | 0.787 ms | High run-to-run variation |

The paired throughput changes were `+17.11%`, `+0.80%`, `−0.43%`, `−0.95%`,
and `−0.16%`. The first baseline run was substantially slower than the other
baseline runs; its cause is unknown and it materially affects the paired
spread. The five-pair result does not establish a throughput improvement.
Lower process CPU per put is a directional signal, but this experiment does
not isolate which code path accounts for it. The sampled p99 values are too
volatile to support a tail-latency conclusion.

## Result verification and limits

The paired suite reported `complete: true` with 10/10 planned runs completed;
`remote-exit.json` reported `failed: false` and 10 started runs. Validation
confirmed every planned run ID had a successful `result.json` and matching
`run-metadata.json`. The transferred archive checksum was verified. The raw
archive and checksum file remain local and were not added to GitHub, as
requested. The verified archive SHA-256 is
`d7b377c4bd9a27965f4873a5588951df3bdf70219e56d08c69ce8ba5a641ce8e`.

The original built-in 64-run RFC 026 matrix attempt stopped on its first run
because the generated TSV used CRLF line endings, leaving a carriage return in
the latency-sampling argument. The runner was fixed in `bb5dbf50` to emit LF
line endings. A later 64/64 rerun completed and is reported in the [full matrix
report](rfc-026-v8-durable-guard-full-matrix-gcp-20261115.md). Those matrix
results are separate from the five-pair A/B table above. The GCP environment
was retained; no cloud cleanup was performed.

This result supports the change as a possible CPU-efficiency improvement, not
as a demonstrated Point-throughput optimization. More paired runs would be
needed to assess whether the CPU reduction is stable and whether it matters to
throughput.
