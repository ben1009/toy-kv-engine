# Dynamic v7 group formation: retained comparison

## Result

The dynamic window substantially reduces solo groups, but this screen does not
qualify it as a performance improvement. The best eligible 8-writer follow-up
had 16% fewer groups and 80% fewer solo groups than retained v7, while its
throughput was 17% lower. The 16-writer results also show larger groups, but
the fastest eligible retained and candidate measurements came from different
cohorts. SSD stalls left only one eligible candidate measurement for each
high-concurrency case. Keep the implementation on the draft branch for further
evaluation; do not infer a merge-ready performance win from this screen.

No WAL source, grouping policy, frontier, sync, or on-disk format was changed
for these measurements.

## Method

The workload was 750,000 puts, 1 KiB values, batch size 64, Parallel WAL, no
compaction, on the same ext4 SSD used in prior reports. A frozen binary of the
previously retained v7 implementation was compared with the fixed admitted
window and the current dynamic window. The first cohort used one warmup and
three scored runs per arm at 8, 16, and 1 writer. A separately planned follow-up
used one warmup and two scored runs each for retained v7 and the dynamic window
at 8 and 16 writers. The scored order rotated. Both cohorts used a 30-second
initial pause and five-second gaps. All runs and flags are retained below.

A run was flagged if mean frontier write time was at least 0.12 ms or mean
`fdatasync` time was at least 0.6 ms, as in earlier screens. These mean-based
filters can miss short stalls and passing them does not prove a run was
stall-free. The highest eligible throughput is selected for each exact binary;
its p99 and grouping counters come from that same invocation. Comparisons
between independently selected peaks, particularly across cohorts, are
descriptive rather than controlled evidence of a source effect.

## Best eligible fresh results

`Buffers/group` is `commit_buffers / commit_groups`. The eligible column counts
scored runs across both fresh cohorts; the fixed-window and 1-writer arms appear
in the first cohort only. A dash means no scored run passed the stall filter.

| Writers | Implementation | Puts/s | p99 (ms) | Groups | Solo groups | Buffers/group | Eligible |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Retained v7 | — | — | — | — | — | 0/3 |
| 1 | Fixed window | 81,443 | 1.220 | 11,719 | 11,719 | 1.000 | 2/3 |
| 1 | Dynamic window | 83,417 | 1.200 | 11,719 | 11,719 | 1.000 | 2/3 |
| 8 | Retained v7 | 448,033 | 2.502 | 8,069 | 5,154 | 1.452 | 2/5 |
| 8 | Fixed window | 323,584 | 2.431 | 9,802 | 8,066 | 1.196 | 1/3 |
| 8 | Dynamic window | 314,220 | 2.836 | 6,033 | 816 | 1.943 | 1/5 |
| 16 | Retained v7 | 590,273 | 4.002 | 6,281 | 3,250 | 1.867 | 1/5 |
| 16 | Fixed window | — | — | — | — | — | 0/3 |
| 16 | Dynamic window | 517,761 | 5.857 | 5,579 | 728 | 2.102 | 1/5 |

The fresh 8-writer peaks were from different cohorts. In the follow-up's only
eligible 8-writer pair, retained v7 reached 380,031 puts/s and 3.043 ms p99,
with 7,187 groups, 4,068 solo groups, and 1.631 buffers/group. Dynamic reached
314,220 puts/s and 2.836 ms p99, with 6,033 groups, 816 solo groups, and 1.943
buffers/group. There was only one eligible measurement per arm in that cohort.

At 16 writers the best eligible dynamic run had 11% fewer groups and 78% fewer
solo groups than the best eligible retained run. The dynamic peak was 12% lower
throughput and 46% higher p99, but the runs came from different cohorts, and
each arm had only one eligible sample out of five. All dynamic 16-writer
follow-up samples were flagged. The observed differences cannot be attributed
to the code with confidence.

## Historical v4 context

These are previous best eligible v4 Parallel runs of the same 750,000-put
batch64 workload from the [historical index](artifacts/rfc-025-v7-ssd-rerun-20261029-best.json),
not fresh paired comparisons. The best fresh dynamic results above were 2.6%,
19.1%, and 13.1% below these v4 throughput peaks at 1, 8, and 16 writers,
respectively. The difference includes machine and storage variation.

| Writers | Historical v4 puts/s | Historical v4 p99 (ms) | Best fresh dynamic puts/s | Dynamic p99 (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 85,618 | 1.195 | 83,417 | 1.200 |
| 8 | 388,372 | 2.511 | 314,220 | 2.836 |
| 16 | 595,941 | 2.967 | 517,761 | 5.857 |

## Evidence

The [first plan](artifacts/rfc-025-v7-dynamic-groups-20261030-plan.json) and
[matrix](artifacts/rfc-025-v7-dynamic-groups-20261030-matrix.json) were frozen
before capture. Its [summary](artifacts/rfc-025-v7-dynamic-groups-20261030-summary.json)
contains all run IDs, thresholds, flags, counters, and artifact hashes; the
corresponding `ext4-batch{1,8,16}` JSONL, host-audit JSONL, and run manifests
share the same filename prefix.

The independently planned [follow-up](artifacts/rfc-025-v7-dynamic-groups-retry-20261030-plan.json)
and its [summary](artifacts/rfc-025-v7-dynamic-groups-retry-20261030-summary.json)
retain all 8- and 16-writer measurements. Both analyzers revalidated all 48
captured invocations and binary identities. The
[environment record](artifacts/rfc-025-v7-dynamic-groups-20261030-environment.json)
binds the tested binaries and candidate source to the branch state. No point
workload was remeasured, so this batch-only screen cannot satisfy the full
performance guard.
