# GCP V8 small-ticket CPU profile

Run directly over SSH on the Free Trial project. Diagnostic-only capture: `--profile`, 10% Hotpath timing samples, and perf on point workloads; the reported rates and p99 are not RFC performance qualification results.

- Source commit: `54c1a7392f50b2809b123d7359b78e050bd2303d`
- VM: `c4-standard-16-lssd`, 16 vCPU, two 375 GiB Local SSDs, RAID0/ext4
- Region/zone: `us-central1-c`
- Workloads: point, batch8, batch16, batch64; writers 1, 8, 16
- Coverage: 28/28 benchmark cells; 7/7 nonempty point perf recordings; perf reports show 0 lost samples
- VM teardown: server-side after 60 minutes; left running for follow-up. No billing upgrade.

## Point workload, instrumented

| Writers | Arm | puts/s | p99 ms | Process CPU s | syncs |
|---:|---|---:|---:|---:|---:|
| 1 | v7 | 6,324 | 0.289 | 7.447 | 100000 |
| 1 | v8 | 3,150 | 0.565 | 10.081 | 100000 |
| 1 | leader | 3,556 | 0.544 | 12.608 | 100000 |
| 8 | v7 | 31,602 | 0.715 | 4.334 | 13092 |
| 8 | v8 | 13,525 | 1.073 | 10.040 | 12627 |
| 16 | v7 | 44,025 | 1.276 | 6.368 | 8408 |
| 16 | v8 | 19,258 | 1.553 | 12.355 | 8086 |

## Relevant sampled function results

- At 16 writers, self samples attributed `publish_commit_ts` 18.13% on V7 and 13.48% on V8; `parking_lot::RawMutex::lock_slow` was 2.93% vs 2.88%.
- V8 point profile sampled `_blake3_compress_in_place_avx512` at 1.29% self. This is too small to explain the roughly 2x point CPU/op gap by itself.
- V8 also sampled completion-channel receive, frontier coordination, CRC, and futex work; see `perf-self-symbols.txt` and per-run `perf-self-report.txt`.
- At 16 writers, V8 used 8,086 syncs vs V7 8,408, while process CPU was 12.355 s vs 6.368 s. The profile also accumulated 1,411.99 ms inside V8 `fdatasync` calls vs 129.79 ms for V7. So V8 did fewer syncs, but each measured sync path was much slower in this capture; this needs a same-state unprofiled follow-up before assigning causality.
- `--profile` appends Hotpath text after the JSON record in stdout. Raw `result.json` therefore has extra lines; `result.record.json` is the parsed first-line JSON and `profile-output.txt` contains the appended diagnostic text.

Raw per-cell JSON, stderr, timelines, pipeline counters, and perf captures are omitted. The report, `summary.tsv`, `matrix-validation.json`, and `perf-self-symbols.txt` contain the retained benchmark summary and findings. SHA-256 checksums are in `SHA256SUMS`.
