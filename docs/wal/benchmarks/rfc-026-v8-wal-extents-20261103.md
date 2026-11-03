# RFC 026 WAL extent attribution, 2026-11-03

Extent attribution works on real point and bulk WAL traffic. All eight full
captures have zero lost events and no NVMe matching anomalies. The analyzer
positively attributes 100% of point DATA bytes and 99.84–100% of batch64 DATA
bytes. It recovers 19,999 usable V8 point DATA observations per run, where the
previous namespace-only classifier had effectively none.

A second result matters for calibration: V8 DATA latency stayed near 149 µs
across two point runs while first namespace-write latency changed from about
636 µs to 25 µs. Most first writes were non-WAL FUA commands. Simply filtering
the journal away would discard evidence of that latency change. This cohort
therefore exports calibration distributions and 100 ms timelines, with no
fast/slow labels or V7/V8 performance ratio.

Only benchmark hooks and analysis tools changed. Production WAL runtime,
digest, format, DATA grouping, fsync and affinity policy remain unchanged.

## Protocol

The [method](wal-extent-attribution-method.md) combines read-only FIEMAP,
`ext4_ext_map_blocks_exit`, `ext4_free_blocks`, the four existing NVMe/block
events, and frame inventories read after timing stops. Post-only extent maps
are not used as evidence for earlier I/O. Frame roles are checked using offset,
physical header binding and local CRC; earlier initialization writes remain
separate. The first snapshot precedes all application counters; the second
follows them and precedes engine close. Neither snapshot requests file sync.

Same P41 Plus 1 TB, firmware 004C, ext4 partition 3, i9-13900T, whole-process
CPUs 0–15, kernel 6.12.71-1-lts and perf 6.19. The recorded partition offset is
54,761,881,600 bytes, with 512-byte namespace LBAs and 4 KiB filesystem blocks.
The benchmark executable SHA256 is
`9d35fcabc018c4815cc1b882af7f53d4c38f3724f04101a1aec977e0154c9747`.

Two smoke captures preceded a full V7 → V8 → V8 → V7 schedule for each workload.
Point uses 20,000 puts with batch size 1 and one writer. Batch64 uses 750,000
puts and 16 writers. Both use 1 KiB values, Parallel PITR, no compaction and a
2 GiB SST target. Counters are sampled every 100 ms. Trace/timeline/stdout/stderr
streams stay in RAM through the entire cohort. No endpoint probe, raw-device
I/O or controller change runs. File reads after each measurement remain an
observer effect, not assumed free of influence on later runs.

## Attribution coverage

| Run | Inventory DATA bytes | Positively attributed DATA bytes | Coverage | Usable post-FLUSH DATA observations |
|---|---:|---:|---:|---:|
| Point V7, first | 81,920,000 | 81,920,000 | 100% | 19,998 |
| Point V8, first | 81,920,000 | 81,920,000 | 100% | 19,999 |
| Point V8, reverse | 81,920,000 | 81,920,000 | 100% | 19,999 |
| Point V7, reverse | 81,920,000 | 81,920,000 | 100% | 19,998 |
| Batch64 V7, first | 816,054,272 | 814,776,320 | 99.8434% | 1,450 |
| Batch64 V8, first | 816,054,272 | 815,931,392 | 99.9849% | 1,392 |
| Batch64 V8, reverse | 816,054,272 | 816,054,272 | 100% | 892 |
| Batch64 V7, reverse | 816,054,272 | 814,907,392 | 99.8595% | 1,349 |

Each point run also has exactly 20,000 attributed FRONTIER writes. The bulk
coverage shortfall is left unresolved, largely because some WAL-range commands
were submitted under another issuer. Such commands are not silently called
journal writes or promoted into positive DATA observations. Extent-tree metadata
frees appear in the diagnostic counters without any observed conflicting
file-data mappings. All inventories match the DATA bytes expected from every
logged request, including each producer's final partial batch.

The result supplies 1,239 application counter intervals. The calibration table
has four request-shape classes observed in both formats for point and 44 for
batch64; shared presence does not itself establish adequate samples or valid
state calibration.

## DATA alone misses part of the latency change

These are descriptive observations from the two V8 point runs, not a performance
A/B or a qualified fast/slow classification:

| Observation | First V8 run | Reverse V8 run |
|---|---:|---:|
| Usable DATA observations | 19,999 | 19,999 |
| WAL DATA latency median | 149.464 µs | 149.575 µs |
| First namespace-write median, for these observations | 636.063 µs | 24.894 µs |
| FLUSH-to-DATA setup gap median | 718.687 µs | 85.838 µs |
| Total fdatasync time | 26.950 s | 8.467 s |
| Sync count | 20,000 | 20,000 |
| Non-WAL FUA writes in the application window | 20,012 | 20,001 |

For 19,996 of the first run's usable observations, the first namespace write
was a non-WAL 4 KiB FUA command; the reverse run had that pattern in all 19,999
usable observations. The broader window counts above include additional
namespace traffic. Block trace context shows filesystem journal and metadata
activity; exact ownership of every non-WAL command is not established by WAL
extents, and the report does not assign all background bytes to the engine.

The two V7 point runs had 160 and 161 non-WAL FUA writes in the application
window. Their ordinary post-FLUSH DATA path is therefore a different calibration
class from V8's common intervening-FUA path. This is consistent with different
filesystem metadata behavior, but does not prove its complete cause. In
particular, neither seeing FUA in the trace nor observing faster FUA on one run
establishes that switching application WAL writes to FUA would help.

Both formats start with only 8 KiB WAL files in the initial snapshot, and their
physical mappings grow during the run. That is why recording ext4 mapping events
was necessary rather than assuming the entire final FIEMAP was valid initially.

## What this establishes and what remains

File membership, DATA/FRONTIER separation, initialization filtering, bulk request
size and overlap distributions, and fine counter joins are now measured. The
counter CSV also includes both DATA and first namespace-write medians so that
intervening durability traffic remains visible over time.

The remaining work is **validated calibration**, not more identical namespace-only
captures. The first cohort was explicitly declared as calibration-only. Its
classes have no independently validated state labels, so the analyzer deliberately
leaves every state unknown. A future protocol must account for the first-write
role/control and intervening durability path as well as size/overlap, freeze its
calibration before fresh evaluation, and preserve the reversed controls. It must
not label V8 as stable merely because DATA latency stayed flat here.

## Verification and artifacts

158 benchmark binary tests and 87 Python tests passed, including 18 extent tests,
29 original trace tests and 40 paired-runner tests. All-features Clippy passed
with warnings denied; the benchmark also checks without default features.
Formatting, spelling, source/stream hashes and compressed replay are checked
separately. No production engine source is modified.

Artifacts use prefix `rfc-026-v8-wal-extents-20261103-`:

- [Predeclared plan](artifacts/rfc-026-v8-wal-extents-20261103-plan.json),
  [event formats](artifacts/rfc-026-v8-wal-extents-20261103-formats.json), and
  [summary](artifacts/rfc-026-v8-wal-extents-20261103-extent-summary.json).
- [100 ms metrics](artifacts/rfc-026-v8-wal-extents-20261103-interval-metrics.csv)
  and [per-class calibration observations](artifacts/rfc-026-v8-wal-extents-20261103-wal-calibration.csv).
- Eight raw `.perf.gz` streams, request/extent timelines, full analysis JSON,
  benchmark/capture records, helper/runner snapshots, checks and a hash manifest.
  The as-executed helper matches the plan's source hash. The current offline
  analyzer additionally exports first namespace-write diagnostics, conservative
  partial-write handling and inventory conservation checks; no calibration
  thresholds or state eligibility were relaxed after capture.
