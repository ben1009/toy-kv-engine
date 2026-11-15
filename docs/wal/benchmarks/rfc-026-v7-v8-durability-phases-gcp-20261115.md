# RFC 026 V7/V8 Point Durability Phase Comparison on GCP — 2026-11-15

Source commit: `bbb30127d49d5ceeca8fb15a468f4f5e321ecc67`<br>
Benchmark-only source patch SHA-256: `6c38c076d1d568bf6b425cb0352a76c5aa80f50a3b5101894e4ea478da7a4586`

## Scope and setup

The experiment compared V7 Parallel with V8 Parallel using 16 writer threads, 100,000 Point puts per run, and 1 KiB values. V8 used Direct FRONTIER I/O, a fixed 400 μs coalescing override, and 4 MiB sliding `fallocate` preparation. The host was `c4-standard-16-lssd` with two Local SSDs in RAID0, Linux `7.0.0-1017-gcp`, and ext4 mounted with `rw,noatime,stripe=256`.

The suite completed five counterbalanced V7/V8 phase-sampled pairs plus two separate commit-order trace runs (12 runs total). Phase samples were taken every 64 completed durability advances; operation latency was sampled every 100 puts. The commit-order trace runs are excluded from the phase comparison.

The phase sampler is benchmark-only and was applied to both WAL versions. The source patch was built and smoke-checked before the GCP run. No production WAL behavior was changed in the PR branch for this experiment.

## Matched phase results

Values are microseconds. For each run, phase sample p50 and p95 were calculated first; each table cell is the median of the five run-level percentiles. Thread CPU is measured on the thread performing the sampled durability work.

| Phase | V7 wall p50/p95 | V8 wall p50/p95 | V7 thread CPU p50/p95 | V8 thread CPU p50/p95 |
| --- | ---: | ---: | ---: | ---: |
| Coalescing | 105.3/169.7 | 166.3/273.1 | 36.4/69.4 | 50.4/100.6 |
| FRONTIER preparation‡ | 4.8/11.4 | 6.2/18.7 | 4.7/10.3 | 6.2/17.9 |
| FRONTIER write | 35.0/70.3 | 38.7/59.6 | 6.2/11.9 | 8.9/16.3 |
| `fdatasync` | 1.3/112.5 | 101.2/156.2 | 1.3/10.0 | 9.3/10.9 |
| Retirement before publish§ | 13.0/17.2 | 35.3/42.3 | 12.9/17.0 | 35.1/42.1 |
| Durable-publish lock wait | 0.2/0.4 | 0.2/0.2 | 0.2/0.5 | 0.2/0.3 |
| Durable publish | 4.3/5.2 | 4.5/5.0 | 4.4/5.2 | 4.5/5.0 |

### Paired p50 differences

Each difference is the median of the five within-pair V8-minus-V7 differences. “Higher” counts how many of the five pairs had a positive V8-minus-V7 difference.

| Phase | Wall Δ | Thread CPU Δ |
| --- | ---: | ---: |
| Coalescing | +59.7 μs (5/5 higher) | +13.0 μs (5/5 higher) |
| FRONTIER write | +3.9 μs (5/5 higher) | +2.8 μs (5/5 higher) |
| `fdatasync` | +99.9 μs (5/5 higher) | +8.0 μs (5/5 higher) |
| Retirement before publish§ | +22.2 μs (5/5 higher) | +22.2 μs (5/5 higher) |
| Durable-publish lock wait | −0.008 μs (0/5 higher) | −0.014 μs (0/5 higher) |
| Durable publish | +0.170 μs (4/5 higher) | +0.182 μs (4/5 higher) |

## Intervals without a direct phase match

| Interval | V7 wall/CPU p50 | V8 wall/CPU p50 |
| --- | ---: | ---: |
| V7 written-prefix-to-durable vs. V8 driver interval† | 171.9/70.7 μs | 430.3/160.3 μs |
| V8 retirement after publish§ | — | 41.1/32.8 μs |

These intervals have different start/end anchors and must not be treated as a matched V7/V8 critical-path comparison. No paired percentage is calculated for them.

## V8 retirement substeps

I reanalyzed the five retained V8 score-run JSON files. Each run had 98–99 sampled advances. For each run, the p50 and p95 of each substep's accumulated thread CPU were calculated first; this table reports the median of those five run-level percentiles. Values are microseconds per sampled advance.

| Retirement substep | Before publish CPU p50/p95 | After publish CPU p50/p95 |
| --- | ---: | ---: |
| Collect covered tickets | 1.129/1.733 | 1.015/1.449 |
| Remove ticket entries | 4.179/4.745 | 6.157/10.170 |
| Release unused FRONTIER credit | 12.730/14.615 | — |
| Finish reservation / disarm guard | 7.065/8.426 | 13.579/21.023 |
| Complete retirement interval | 35.117/42.130 | 32.770/47.840 |

The largest pre-publish substep is releasing unused FRONTIER credit. After publish, guard disarming is the largest measured substep. Each sampled advance covers about 16 tickets. These per-advance CPU percentiles must not be added or treated as per-put costs. The post-publish `finish` measurement includes the current per-guard durable-state check and the per-substep timing overhead; it is a candidate for a batched prefix check, not proof that lock acquisition alone accounts for the entire cost.

## Findings and limits

- The clearest matched differences are V8 coalescing (+59.7 μs wall and +13.0 μs thread CPU at p50) and pre-publish retirement (+22.2 μs wall and CPU at p50); both were higher in all five pairs.
- V8 `fdatasync` p50 was about 100 μs higher, while thread CPU was about 8 μs higher. V7's p50 was only 1.3 μs but its p95 was 112.5 μs, so its sampled latency distribution was highly variable; the median alone does not represent every sync.
- Both V7 and V8 `fdatasync_once()` wrappers call `libc::fdatasync()` and retry `EINTR`. In this comparison V7's sync descriptor is cloned from its `O_DIRECT` file handle, while V8's sync descriptor is buffered; the recorded handles refer to the same respective WAL inode. The descriptor flag alone does not establish a different `fdatasync` durability guarantee. The p50 gap therefore needs syscall/writeback evidence before it is attributed to device service time or a runtime bug.
- Durable-publish lock wait and publication time were similar. These measurements do not support coordinator lock contention as a major difference in this experiment.
- V7 and V8 coalescing policies are not identical. FRONTIER preparation also represents different internal work: V7 prepares state/marker data, while V8 reserves and encodes a frontier. These are phase-boundary comparisons, not identical operations.
- V7 retirement combines the pre-publish work measured here; V8 also has a separate post-publish retirement interval, for which there is no direct V7 counterpart.
- Phase samples are diagnostic and may perturb timing. All five score pairs used phase sampling every 64 advances and latency sampling every 100 puts; the throughput context below is not an uninstrumented performance grade. The two trace runs use a global trace mutex and were excluded from phase metrics.
- Values summarize five run-level percentiles; the sampled phase observations within a run are not independent benchmark repetitions.

Diagnostic-run throughput context only: V7 median 46.39k puts/s; V8 median 31.73k puts/s. These runs include instrumentation and should not be compared directly with uninstrumented scores.

‡ V7 FRONTIER state/marker preparation and V8 reservation/encoding are not equivalent operations.

§ V7 combined pre-publish retirement is compared with V8 pre-publish retirement; V8 post-publish retirement has no direct V7 counterpart.

† V7 written-prefix-to-durable and V8 driver intervals use different anchors.

## Result verification and retention

The runner reported `complete: true` with 12/12 runs completed. `remote-exit.json` reported `failed: false` and 12 started runs. Validation confirmed that every planned run ID had a successful `result.json` and matching `run-metadata.json`. The phase samples totaled 646 for V7 and 491 for V8, with zero dropped samples.

The transferred results archive and checksum metadata are retained locally, not committed to GitHub. Archive SHA-256: `71c8ee70c3902c342d756a1912d0388bc8649226f4381854b778390106397d73`. The GCP VM and restricted SSH firewall were retained; no cloud or Terraform cleanup was performed.
