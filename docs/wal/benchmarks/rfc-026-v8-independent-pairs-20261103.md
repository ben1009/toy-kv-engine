# RFC 026: independent pairs and bounded stable-state admission

The next benchmark protocol qualifies each V7/V8 pair independently. It no
longer discards a compatible pair because another pair in the same session
crossed storage states. It also requires compatible admission probes before
starting a pair. WAL runtime, wire format, digest, DATA grouping, durability
policy and device features remain unchanged.

This full cohort still supplies **zero compatible pairs**. Fast state was
unavailable during its scheduled fast attempts; the admitted slow attempt
then crossed to fast during V7. The reverse slow attempt found only fast
probes. All attempts, including skipped ones, are preserved. No attempt is
retried and no historical capture is reclassified under the new rules.

## Predeclared protocol

[The method](paired-v7-v8-method.md) describes protocol
`kv-engine.wal-paired-diagnostic.v2`. Before any probe, the
[plan](artifacts/rfc-026-v8-independent-pairs-20261103-plan.json) fixes:

| Attempt | Required state | Order |
|---|---|---|
| 0 | fast | V7 → V8 |
| 1 | fast | V8 → V7 |
| 2 | slow | V7 → V8 |
| 3 | slow | V8 → V7 |

Admission stops at the **first** two consecutive compatible probes in the
required state, within six probes. If none occurs, that scheduled attempt
is skipped without running either format. Once admitted, its final admission
probe becomes the first of exactly three measurement boundaries:

```text
probe → A → probe → B → probe
```

All three must match the admitted state and satisfy the unchanged drift
limits pairwise. A pair receives at most one measurement attempt. Ratios
depend on probe validity and the saved workload/identity checks, never on
whether the performance result looks favorable. The analyzer separates fast
and slow results and combines ratios within a state only when both orders
have valid pairs. Missing directions/states remain missing.

These rules are deliberately bounded. They do not guarantee both states
will become available, and they do not opportunistically change an attempt's
target when a different state appears. Such a schedule would need its own
declaration before another cohort. No controller-state manipulation or
unbounded retry loop is introduced here.

## Fixed workload and execution

The same preserved instrumented executable runs both formats, SHA256
`fbb90ea8a6f20bf476760e79ae60e04cda3edae04138d45ae5a9e6346da299f1`.
Its [source instrumentation and build provenance](rfc-026-v8-producer-attribution-20261102.md)
are unchanged. Each full capture has 750,000 puts, sixteen writers, batch64,
1 KiB values, Parallel WAL, no compaction and a 2 GiB SST target. Both formats
inherit whole-process CPU mask `0-15` on the same boot. Placement within that
mask is not separately sampled in this cohort.

Each probe performs 1,024 aligned 4 KiB O_DIRECT writes with fdatasync every
four writes, using the existing initialized 8 MiB scratch inode on ext4
`/dev/nvme0n1p3`. The full cohort has 22 distinct probes, 88 MiB of scratch
writes, and two completed benchmark captures. The first measurement boundary
duplicates the final admission record in the JSON; it is not another I/O
probe. Output and phase JSON stay in RAM until each attempt ends. No build,
test, profiler or admin query runs alongside measurement.

The short smoke uses 4,096 puts per capture. Its fast attempts also exhaust
their probe limits; both slow orders are admitted and complete successfully.
Four smoke captures verify both formats, phase records and digest accounting.
The smoke's twenty probes write 80 MiB. Its small-run ratios are not used as
performance evidence or combined with the full cohort.

## Full-cohort observations

| Attempt | Admission probes | Observed state | Outcome |
|---|---:|---|---|
| 0, fast V7 → V8 | 6 | slow throughout | Not admitted |
| 1, fast V8 → V7 | 6 | slow throughout | Not admitted |
| 2, slow V7 → V8 | 2 | slow before V7; fast afterward | Rejected |
| 3, slow V8 → V7 | 6 | fast throughout | Not admitted |

The first twelve probes have post-fdatasync first-write medians
640.229–640.345 µs. They cannot admit a fast-state pair. Attempt 2 admits
after medians 640.322/640.348 µs, but its later boundaries are
28.874/28.983 µs:

| Rejected attempt 2 | Puts/s | p99, ms | Process CPU, s | Syncs |
|---|---:|---:|---:|---:|
| V7 | 138,593 | 27.802 | 6.862 | 1,441 |
| V8 | 673,191 | 2.933 | 5.706 | 895 |

The same pair contains a slow-to-fast transition, so its apparent performance
difference is not a V8/V7 comparison. The reverse slow attempt's six probes
instead have medians 28.796–28.894 µs. Its required slow state is unavailable,
so neither format runs there. The
[full summary](artifacts/rfc-026-v8-independent-pairs-20261103-summary.json)
therefore contains no full-cohort pair ratios or state-level combined ratios.
This does not establish what either format would achieve under a compatible
fast or slow pair, nor identify the transition's internal cause.

## Validation and limits

The runner validates saved probe summaries against their raw latency arrays.
Capture identities must match the saved binary hash, boot ID, mask and
workload plan; reported ratio metrics must match the original benchmark and
phase records. The analyzer checks that admission did not continue beyond an
earlier successful window. These checks prevent an archive from silently
selecting a later favorable admission window.

Forty tests pass, including independent pair acceptance, all-three-probe
comparisons, bounded/first-success admission, skipped attempts, reversed
ratio direction, state separation and unchanged legacy block rejections.
The [check record](artifacts/rfc-026-v8-independent-pairs-20261103-checks.json)
records native test and data validation. All probe statistics, phase estimates,
digest byte counts and the exact
[executed runner](artifacts/rfc-026-v8-independent-pairs-20261103-runner-as-executed.py)
hash are checked. Reanalysis reproduces both the full and smoke summaries.
The [hash manifest](artifacts/rfc-026-v8-independent-pairs-20261103-hashes.json)
covers the report, tooling and raw records.

The previous whole-block data and their two rejections remain unchanged;
the legacy analyzer is retained. Compatible pairs would still be diagnostic,
with the old per-run storage flags and RFC qualification reported separately.
Endpoint probes cannot exclude a transition and return during one workload,
and they cannot provide identical completion schedules. Controlled runtime
completion replay remains future work. No runtime optimization or RFC
performance qualification follows from this slice.
