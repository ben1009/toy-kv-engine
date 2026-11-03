# RFC 026: held-out path calibration does not qualify a V7/V8 ratio

The calibration slice is complete. It adds an offline, path-conditioned model
and runs separate training, validation and fresh evaluation cohorts, with no
production WAL or capture instrumentation change. **No class passed the
predeclared calibration gate, so no V8/V7 performance ratio is reported.**
This is a negative calibration result, not a V8 performance regression or a
finding that the SSD lacks distinct latency regimes. RFC 026 remains unqualified
and V8 remains default-off.

## Fixed experiment

The [method](wal-state-calibration-method.md) was declared before measurement.
Each cohort used V7 → V8 → V8 → V7, first for 20,000 point puts with one writer,
then for 750,000 puts with 16 writers and batch64. Values were 1,024 bytes;
Parallel PITR, no compaction, a 2 GiB SST target, CPUs 0–15 and 100 ms application
counters were fixed. No state-dependent retries, cooldown selection, external
probes, device settings, allocator changes or runtime tuning were used.

The executable was the same frozen extent-enabled benchmark as the preceding
study:

```text
binary SHA256
9d35fcabc018c4815cc1b882af7f53d4c38f3724f04101a1aec977e0154c9747
study-plan SHA256
1947fb0a35f01f2e3175a38c9f625171eded7258d42e98f48d4bd04c3c4f97db
frozen-model SHA256
07e2f0dd96c4c3acfca4d5f1e5027aad1e4240f9d8a69ca07c9733edcb9e5a7c
```

Host: i9-13900T, kernel `6.12.71-1-lts`, perf `6.19-2`, ext4 on partition 3 of
`nvme0n1`, Solidigm P41 Plus 1 TB, firmware `004C`. The executable, boot, CPU mask,
workloads, capture sources and device layout match across all three cohorts.

Native monotonic chronology, in nanoseconds:

| Event | Timestamp |
|---|---:|
| Study declared | 2603447580867790 |
| First training counter | 2603518321293932 |
| First validation counter | 2603645247264223 |
| Model frozen | 2603796711637519 |
| Evaluation admitted, with model hash | 2603796803511485 |
| First evaluation counter | 2603797464141486 |

The model was frozen before evaluation started. Replaying the calibration input
hashes and model coefficients reproduced the frozen model. Evaluation data never
entered fitting or validation.

## Trace quality and the failed initial attempt

An initial eight-run training attempt used a whole-cohort quality gate. Its
`point-1-v8` trace had two same-key/same-timestamp events and two overlapping live
command IDs, despite zero lost events. The attempt stopped before fitting. It
is archived with its original declaration and source snapshots; none of it was
reused for the final study.

A new study was then declared with one explicit policy change: quarantine an
entire ambiguous trace as unknown, retaining all its evidence, while allowing
other traces to face the unchanged class-validation requirements. Three of the
24 new traces were quarantined:

| Cohort | Trace | Matching anomalies |
|---|---|---|
| Training | point-2-v8 | 1 completion without setup, 2 duplicate live keys, 3 same-key/same-timestamp events |
| Validation | point-0-v7 | 1 completion without setup, 1 same-key/same-timestamp event |
| Evaluation | point-1-v8 | 1 completion without setup, 1 same-key/same-timestamp event |

All 32 captures, including the failed initial attempt, have zero lost events.
Zero event loss alone does not establish unambiguous command pairing. The
quarantined traces contribute no calibration or performance evidence; their
original files are preserved. The matching ambiguity was not repaired or
reinterpreted in this slice.

The final study's 21 quality-passing traces provide:

| Cohort | Quality-passing runs | Usable post-FLUSH DATA observations | Observed routing classes |
|---|---:|---:|---:|
| Training | 7/8 | 64,554 | 111 |
| Validation | 7/8 | 65,707 | 141 |
| Evaluation | 7/8 | 65,496 | 145 |

Point DATA byte attribution remains 100%. Batch64 attribution is
99.8655–100.0000% across the final study. File ownership/role coverage is not the
limiting factor.

## What was calibrated

Routing includes first namespace-write role/control and size, intervening
FUA/ordinary writes and reads, DATA size, submission queue depth, peak overlap,
and queue depth at FLUSH completion. **Neither format nor latency is a routing
key.** The FLUSH→DATA gap is a feature rather than a routing bucket, so a delay
cannot hide itself by moving the sample into a different class.

The model uses 16-observation window medians of first namespace-write latency,
DATA latency and FLUSH→DATA gap. It fits two local latency modes, requiring
separation, sufficient support from both formats in both modes, and repetition
in an independent validation cohort. Throughput, p99 and CPU do not affect the
fit. Unsupported classes stay unknown rather than inheriting the old isolated
4 KiB thresholds. The complete policy is available in the
[study plan](artifacts/rfc-026-v8-calibrated-states-20261103-study-plan.json).

Only 16 classes supplied at least one bounded fitting window in training or
validation. Their frozen rejection counts were:

| Workload | Too few training windows | Declared fit did not pass mode separation | Accepted |
|---|---:|---:|---:|
| Point | 0 | 2 | 0 |
| Batch64 | 8 | 6 | 0 |
| Total | 8 | 8 | 0 |

Other observed classes were too sparse to produce a complete 16-observation
window within 250 ms. The separation result describes this declared fit, not a
proof that a raw distribution is unimodal. For example, V7 WAL-first training
windows have a namespace p90 of 577.635 µs despite their much lower median.
No alternate initialization or threshold was selected after seeing the data.

## Why the new data still cannot supply a common state/class comparison

Point-path support remains strongly format-specific:

| Point class | Training V7 windows | Training V8 windows | Validation V7 windows | Validation V8 windows |
|---|---:|---:|---:|---:|
| WAL-first ordinary 4 KiB, no overlap | 2,459 | 0 | 1,230 | 0 |
| Non-WAL-FUA-first 4 KiB, no overlap | 0 | 1,249 | 0 | 2,497 |

V7 does have occasional non-WAL FUA context, but it is too sparse to produce
these bounded point-class windows. Consequently, even a better-separated fit
would not meet the shared-format support requirement for these two classes.
A local low/high label on one path cannot simply be equated with a label on
the other path.

The separate validation data also captured a higher-latency path that the usable
V8 point training run did not represent:

| V8 point capture | First namespace-write median | WAL DATA median | FLUSH→DATA gap median |
|---|---:|---:|---:|
| Training, position 1 | 25.223 µs | 145.248 µs | 93.657 µs |
| Validation, position 1 | 634.761 µs | 146.877 µs | 721.919 µs |
| Validation, position 2 | 25.274 µs | 143.905 µs | 93.493 µs |

All usable observations in these three rows have non-WAL FUA first writes.
This repeats the earlier finding: DATA latency alone misses a large change in
the preceding durability path. The higher-latency validation observations were
not folded back into training. Batch64 validation likewise includes namespace
medians around 600 µs, while the training run medians were around 24–29 µs.

## Fresh evaluation and limits

Evaluation finished all eight scheduled runs. With zero accepted classes, all
742 application intervals remain unknown. No qualified episode or matched pair
was emitted. The full request log is retained; requests and slow latencies are
not removed to create eligible intervals. Application p99, CPU, and throughput
remain raw diagnostics in the evidence, not state-matched performance results.

The classifier identifies repeatable **observed latency modes**, not hidden
controller state. It additionally conditions on path/queue behavior that can
itself be affected by V7/V8. Even a future accepted pair would therefore remain
a conditional diagnostic, not unconditional proof of an engine speedup.

The measurement framework now enforces independent training, validation and
fresh evaluation. This particular calibration is insufficient. A subsequent
study needs representative training coverage of the latency modes in shared
path classes, plus untouched validation/evaluation data. More extent hooks,
looser post-hoc thresholds or comparing unlike paths would not establish that
missing evidence.

## Evidence and verification

- [Readable findings, per-class support and all run medians](artifacts/rfc-026-v8-calibrated-states-20261103-findings.json)
- [Frozen model and rejection decisions](artifacts/rfc-026-v8-calibrated-states-20261103-frozen-model.json)
- [Manifest with archive and individual input hashes](artifacts/rfc-026-v8-calibrated-states-20261103-manifest.json)
- [Failed initial attempt](artifacts/rfc-026-v8-calibrated-states-20261103-failed-attempt.tar.gz)
- [Training native traces and timelines](artifacts/rfc-026-v8-calibrated-states-20261103-training.tar.gz)
- [Validation native traces and timelines](artifacts/rfc-026-v8-calibrated-states-20261103-validation.tar.gz)
- [Evaluation native traces and timelines](artifacts/rfc-026-v8-calibrated-states-20261103-evaluation.tar.gz)
- [Study metadata, source snapshots and full evaluation](artifacts/rfc-026-v8-calibrated-states-20261103-study.tar.gz)

Archives exclude benchmark database images. They retain every native trace,
event definition, request timeline, capture receipt, extent/frame snapshot,
benchmark result, derived analysis and original decision. After extraction,
`wal_state_calibration.py verify --study-dir …/study --training …/training
--validation …/validation` checks the input hashes and reproduces the model.
Historical captures retain their original decisions.

Validation: **113 Python tests pass**, including run quarantine, source/policy
identity, independent capture chronology, immutable model outputs, model replay,
format-specific mode rejection, causal confirmation, future-completion handling,
whole-request interval conservation, cross-transition rejection and best-episode
selection with its own p99. Typos checks pass. Rust and capture instrumentation
were not changed or rebuilt.
