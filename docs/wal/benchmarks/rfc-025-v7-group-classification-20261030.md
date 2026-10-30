# RFC 025: classify large tickets across DATA groups

## Result

The experiment confirms that the large-wave classification affects sync
formation. At eight writers, the best eligible candidate reduced syncs from
2,328 to 1,561 and improved p99, but throughput fell 7%. At sixteen writers,
the separately planned follow-up reduced syncs from 1,430 to 1,200 while
throughput fell 15% and p99 rose 7%. Both eligible rounds in each comparison
showed lower candidate throughput.

The longer existing coalescing policy increased measured wait time. This
experiment did not meet the throughput-and-p99 retention criteria, so its WAL
source changes have been restored. The patch and all measurements remain
available for review.

## Candidate

The worker propagates the minimum DATA-write size in a completed group. The
coordinator recognizes an all-large wave when the write count matches the
ticket count and the minimum is at least the existing 64-KiB threshold. V7's
current packer supplies one aligned DATA buffer per logical ticket, so this
checks every ticket without using a group average. A mixed group remains on
the shorter policy, and a count mismatch is treated conservatively. The
threshold is the existing encoded, aligned DATA-buffer size, not the raw value
size.

No new coalescing window was added. DATA grouping, producer framing, hash
overlap, the FRONTIER format, and the sync/acknowledgment protocol are
unchanged. The [compressed candidate patch](artifacts/rfc-025-v7-group-classification-20261030-candidate.patch.gz)
includes the code and tests; current WAL source is restored to the dynamic
grouping baseline at `1b8cbde3`.

## Measurements

Each ext4 SSD run writes 750,000 1-KiB puts in batch64, using Parallel v7 WAL
with compaction disabled. The initial screen used one warmup and three scored
alternating rounds per arm at 8, 16, and 1 writer. Every initial 16-writer
sample was flagged, so a separately planned follow-up used one warmup and two
scored alternating rounds per arm at 16 writers. Samples were flagged when
mean FRONTIER write time was at least 0.12 ms or mean `fdatasync` time was at
least 0.6 ms; these means can miss short stalls.

The table selects the highest eligible throughput per arm **within each named
cohort**, with p99 and counters from the same invocation. At eight writers
both arms had two eligible runs out of three. At sixteen writers the table
uses the follow-up, where both arms had two eligible runs out of two. The
single-writer baseline had one eligible run out of three, and the candidate
had two. These samples do not eliminate storage or scheduling variation.

| Writers | Version | Puts/s | p99 (ms) | Syncs | DATA groups | Syncs/group | Wait calls | Wait time (ms) |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Dynamic baseline | 84,431 | 1.212 | 11,719 | 11,719 | 1.000 | 0 | 0.0 |
| 1 | All-large classification | 82,511 | 1.246 | 11,719 | 11,719 | 1.000 | 0 | 0.0 |
| 8 | Dynamic baseline | 333,443 | 2.984 | 2,328 | 6,047 | 0.385 | 3,940 | 615.0 |
| 8 | All-large classification | 310,261 | 2.657 | 1,561 | 6,076 | 0.257 | 4,531 | 857.3 |
| 16 | Dynamic baseline | 567,320 | 4.006 | 1,430 | 5,549 | 0.258 | 3,500 | 498.5 |
| 16 | All-large classification | 481,888 | 4.274 | 1,200 | 5,763 | 0.208 | 4,449 | 729.7 |

FRONTIER counts matched sync counts in all selected runs. Syncs per DATA
buffer fell from 0.199 to 0.133 at eight writers and from 0.122 to 0.102 at
sixteen writers. At eight writers, total measured `fdatasync` time fell from
759.1 to 510.8 ms while coalescing wait time rose from 615.0 to 857.3 ms. At
sixteen writers, `fdatasync` time fell from 500.0 to 451.1 ms while coalescing
wait rose from 498.5 to 729.7 ms. These intervals overlap with work on other
threads and are not an additive decomposition of end-to-end latency.

The highest eligible earlier eight-writer result for the exact dynamic
baseline binary was 348,734 puts/s and 2.726 ms p99 in the
[submitted-join screen](artifacts/rfc-025-v7-submitted-sync-join-20261030-summary.json).
That older peak and the candidate peak above come from different cohorts. The
previous historical v4 Parallel bests of the same workload were 85,618,
388,372, and 595,941 puts/s at 1, 8, and 16 writers respectively; they were not
paired with this experiment.

## Validation and artifacts

After correcting a test fixture that initially exceeded the WAL per-value
limit, the candidate passed 97 WAL coordinator and worker tests outside the
sandbox, without retries. Tests cover out-of-order completion, all-large and
mixed groups, an oversized-plus-small group whose average exceeds the
threshold, and recovery after the resulting durability cycles. Point-workload
guards were not run because the batch screen did not qualify the candidate.

The frozen [initial plan](artifacts/rfc-025-v7-group-classification-20261030-plan.json),
[matrix](artifacts/rfc-025-v7-group-classification-20261030-matrix.json), and
[summary](artifacts/rfc-025-v7-group-classification-20261030-summary.json)
preserve every initial run and flag. The separately planned
[16-writer follow-up](artifacts/rfc-025-v7-group-classification-retry-20261030-plan.json)
and its [summary](artifacts/rfc-025-v7-group-classification-retry-20261030-summary.json)
retain its full samples. Raw JSONL, host audits, run manifests, capture logs,
binary identities, and the
[environment record](artifacts/rfc-025-v7-group-classification-20261030-environment.json)
share those prefixes. Both analyzers revalidated all 30 invocations and their
artifact hashes after capture.
