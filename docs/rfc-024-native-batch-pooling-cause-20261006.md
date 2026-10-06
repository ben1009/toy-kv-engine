# RFC 024: native batch pooling root-cause investigation

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Shared skip-list metadata is a substantial insertion bottleneck exposed by
bounded batch pooling. A private change that separates its three atomic
fields improves isolated eight-thread insertion throughput by **88.2%** and
reduces CPU per put by **48.9%**. Its single-thread change is only **+0.3%**.
In the native sixteen-client pipeline, the same intervention reduces the
recorded insertion phase by **28.6%**; observed throughput changes are much
smaller and do not qualify a retained optimization.

The preparation increase also includes a shift in memory first-touch work:
minor-fault samples move from publication copies into input pooling. The
[previous pooling screen](rfc-024-native-batch-pooling-20261006.md) remains
rejected. All dependency variants and pooling changes in this investigation
are private experiments; production code, dependencies, and the existing
parallel default are unchanged.

The subsequent [metadata-only native screen](rfc-024-native-metadata-padding-20261006.md)
tests padding against the current implementation without pooling or an external
profiler. Sixteen-writer paired throughput changes by -1.5%, despite 11.7%
lower insertion time; all three target repeat controls pass. The candidate
fails its frozen retention rule and stays private.

## Scope and controls

The native workload is the previous B candidate: pooled input keys/values
and separately pooled encoded internal keys, each allocation bounded at
128 KiB requested capacity. It uses native async parallel v4 WAL, batch64,
1 KiB user values, 524,288 puts, eight Tokio workers, sixteen clients,
NoCompaction, PITR off, and a 1 GiB memtable target. There is no timed
rotation. WAL files reside on ext4 at `/dev/nvme0n1p3`.

The host is an Intel i9-13900T with P-cores on logical CPUs 0–15 and E-cores
on 16–31, running Linux `6.12.71-1-lts`. Initial CPU profiles compare
unrestricted placement with eight distinct P-cores, CPUs
`0,2,4,6,8,10,12,14`. The subsequent interventions use those P-cores.
The governor and all device settings are unchanged.

Perf attaches after engine/ring initialization. FIFO enable/disable controls
bound sampling and counters to the writer window, excluding runtime drop,
flush and close. CPU sampling uses `cycles:u` at 199 Hz; separate counter
runs use cycles, instructions, cache misses, branches and branch misses,
with raw counts and per-PMU running percentages retained. No global
allocation-counting allocator is present. CPU affinity and profiler runs
are diagnostics, not production settings or performance qualification.

Each intervention uses fixed ABBA and BAAB blocks, with two observations
per arm per block. Comparisons below are medians of the two block ratios,
computed from each arm's geometric mean. Originals are retained, including
large stalls; none is replaced by a favorable retry. Native runs have five
seconds idle afterward. No build, test or concurrent trace overlaps them.

## The insertion bottleneck

The insertion increase persists on P-cores. In one original CPU-profile
block, baseline insertion is 42.1–45.3 microseconds per batch; the pooled
candidate is 96.0–98.7. Restricting placement therefore does not explain
away the insertion regression.

In the pooled insertion symbol, sampled cycles concentrate near the shared
random-height seed load and the atomic entry-count increment. Together,
those instruction neighborhoods account for about 77–86% of that symbol's
sampled period in the unrestricted trace, rather than that proportion of
all application cycles. Instruction attribution alone is a hypothesis;
the independent layout interventions provide the stronger evidence.

In [crossbeam-skiplist 0.1.3](https://docs.rs/crossbeam-skiplist/0.1.3/src/crossbeam_skiplist/base.rs.html),
`HotData` contains three neighboring `AtomicUsize` fields:

| Field | Access in insertion/search |
| --- | --- |
| `seed` | Load and store for each random-height calculation |
| `len` | Atomic increment for each inserted entry |
| `max_height` | Read by searches and height generation; updated when a higher tower appears |

The enclosing `CachePadded<HotData>` separates this structure from other
data, but the fields still share a cache line with one another. The host's
cache line is 64 bytes. Frequently written fields invalidate the line also
used by unrelated metadata accesses, including the mostly read-only height
hint. Separating these fields strongly implicates cache-line contention.

Three private dependency interventions isolate this mechanism:

- **Local RNG:** Keep the xorshift operations and tower-height rules, but
  use a thread-local seed after one initialization from the shared seed.
  Random-height scheduling/distribution changes, limiting this comparison.
- **Hint padding:** Pad only `max_height`; leave `seed` and `len` together.
- **All-field padding:** Put each field inside its own `CachePadded` wrapper.
  Keep the RNG, atomic operations and memory orders unchanged.

Both padded variants preserve the operations under investigation. Two
existing raw-pointer tower-index autorefs are spelled explicitly in every
private fork to compile with the pinned nightly. No dependency patch is
installed in the production workspace.

## Storage-independent insertion test

An independent probe prebuilds the same batch-owned `Bytes` shape before
measurement: 64 records per pool, 26-byte encoded keys and 1,025-byte
prefixed values. Threads start together and move entries into
`SkipMap<Bytes, Bytes>`. The measured window includes insertion and its node
allocation; it excludes input allocation/encoding, WAL, MVCC publication,
map validation and destruction. It uses the native probe's locked versions:
bytes 1.11.1, crossbeam-skiplist 0.1.3, crossbeam-epoch 0.9.18 and
crossbeam-utils 0.8.19. Every run verifies map length and a complete
iteration of all 524,288 entries, including key/value lengths.

These observations have no profiler attached. Eight inserters represent
the possible concurrent publishers on the eight-worker runtime; they are
not eight async clients or an engine throughput measurement.

| Intervention versus original metadata | Inserters | Insertion throughput | CPU per put | Blocks with repeats within 5% throughput |
| --- | ---: | ---: | ---: | ---: |
| Local RNG | 8 | +32.5% | -25.4% | 0/2 |
| Hint padding | 8 | +34.1% | -27.3% | 1/2 |
| All-field padding | 8 | +88.2% | -48.9% | 2/2 |
| Local RNG | 1 | +1.4% | -1.4% | 2/2 |
| All-field padding | 1 | +0.3% | -0.3% | 2/2 |

The all-field eight-thread blocks improve throughput by 88.1% and 88.4%,
with both repeats of each arm within 5%. A direct comparison also finds
hint-only padding about 30.0% slower than all-field padding, with both
blocks passing that repeat check. Isolating the hint helps, but contention
between the other fields still matters. The large concurrent benefit and
negligible single-thread change support contention as the mechanism.

## Native pipeline counterfactuals

These runs have hardware counters attached. Throughput is descriptive and
must not be substituted for an uninstrumented retention screen.

| Native sixteen-client intervention | Insertion phase | Total publication phase | Throughput blocks |
| --- | ---: | ---: | --- |
| All-field padding, pooled B path | -28.6% | -16.6% | +0.05%, +4.27% |
| Hint-only padding, pooled B path | -16.0% | -4.0% | -4.35%, +1.22% |
| All-field padding, unchanged native baseline | -10.4% | -4.5% | +3.19%, +1.67% |

All-field padding lowers pooled insertion from 91.1–103.8 to 65.4–72.1
microseconds per batch across its counter block observations. Both
throughput repeat controls fail: candidate repeat spreads are 5.025% and
8.742%. These runs therefore establish neither a retained throughput gain
nor an RFC performance pass. The local-RNG native runs also contain large
stalls and fail repeat controls; their throughput cannot establish a gain.

Padding helps the unchanged baseline less than the pooled path. This
supports the interpretation that removing publication copies exposes an
existing shared-metadata limit. It does not establish that metadata is the
sole remaining difference between the paths.

The sync coordinator remains active for 61–70% of the measured writer
window in the all-field native comparison. There is one coordinator, so
this is summed fdatasync wall time divided by window duration, not a sum of
multiple concurrent calls. Writes may overlap these calls; this fraction
is not a claim that the same fraction is unavoidable serialized latency.
Sync counts stay near 1,070 and both arms reach 16 in-flight groups and
16 outstanding write SQEs. These software counts are not device queue depth.
Improving insertion CPU cost cannot translate directly into an equal
end-to-end throughput gain.

## Why preparation became more expensive

Four additional gated profiles sample `minor-faults:u` every 128 faults,
with DWARF caller stacks. Categories use named Rust ancestors; short or
unresolved stacks remain unclassified. Sampling loss is zero in the
retained reports. These runs contain storage stalls and their timings are
excluded from optimization qualification.

| Path and observation | Samples | Publication-copy ancestry | Input-pooling ancestry | Other input-preparation ancestry | Unresolved/other |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline first | 1,236 | 58.1% | 0% | 24.8% | 16.6% |
| Pooled first | 1,143 | 0% | 82.1% | 7.1% | 10.2% |
| Pooled second | 1,142 | 0% | 82.5% | 7.3% | 9.5% |
| Baseline second | 1,238 | 56.7% | 0% | 23.7% | 18.8% |

The remaining 0.5–0.7% of each observation is attributed to WAL encoding.
The pooled buffers become the retained memtable storage, so their memory
is first populated during preparation. Baseline publication instead
allocates and copies final key/value storage after durability. The samples
confirm that much of this first-touch work changes phase. Fault counts are
not fault latency or a complete attribution of preparation CPU cost.

For example, in the original P-core counter block, median preparation plus
publication-copy wall time falls from 80.1 to 50.8 microseconds per batch,
even though preparation alone rises. Publication includes its subphases;
do not add its total again to copy or insertion. The larger preparation
timer alone is therefore insufficient to conclude that pooling added the
same amount of total work.

## Outcome and next implementation target

The next justified target is the shared skip-list metadata layout, followed
by an uninstrumented native batch comparison with the existing repeat,
throughput and tail guards. A dependency fork also needs the appropriate
upstream/integration correctness validation before adoption. Reducing
allocation counts further, changing Tokio scheduling, or increasing WAL
in-flight capacity would not directly address the measured metadata limit.

This investigation explains a substantial portion of the insertion cost
and the preparation phase shift. It does not prove all remaining overhead
comes from these fields, and it does not qualify the parallel WAL adoption
gate. The bounded pooling candidates remain rejected and no dependency
experiment is shipped.

## Evidence and verification

There are **116 completed diagnostic observations**: 24 initial CPU/counter
profiles, 20 native pooled metadata interventions, 36 insertion isolation
runs/profiles, 8 native baseline counterfactuals, 4 minor-fault profiles,
16 hint isolation observations, and 8 native hint counterfactuals.
All raw output, sources, binaries, protocols and drivers are retained under
ignored `target/rfc024-native-batch-pooling-cause-20261006/`.

The reproducible analysis reconciles all observations, completed write
CQEs, commits, native mode/worker counts, map validation, throughput,
source hashes, executable hashes and driver hashes. All **114 production
input hashes match**. Owned databases and temporary RAM stages are removed.
All retained perf reports show zero lost samples. A cache-transfer trace
attempt was denied when opening its events; it is retained as a failed
attempt and provides no HITM evidence. Initial profiler-control/lifecycle
failures and pilots are also retained outside the completed comparisons.

- Recomputed summary: `target/rfc024-native-batch-pooling-cause-20261006/diagnostic-summary.json` and analysis driver: `target/rfc024-native-batch-pooling-cause-20261006/analysis.py`.
- Initial protocol: `target/rfc024-native-batch-pooling-cause-20261006/protocol.json` and CPU/counter observations: `target/rfc024-native-batch-pooling-cause-20261006/profiles/records.json`.
- Metadata protocol: `target/rfc024-native-batch-pooling-cause-20261006/causal-protocol.json` and native observations: `target/rfc024-native-batch-pooling-cause-20261006/causal-metadata/records.json`.
- Insertion isolation protocol: `target/rfc024-native-batch-pooling-cause-20261006/isolation-protocol.json` and observations: `target/rfc024-native-batch-pooling-cause-20261006/isolation/records.json`.
- Hint isolation observations: `target/rfc024-native-batch-pooling-cause-20261006/hint-isolation/records.json`, native baseline counterfactuals: `target/rfc024-native-batch-pooling-cause-20261006/baseline-metadata/records.json`, and native hint counterfactuals: `target/rfc024-native-batch-pooling-cause-20261006/hint-native/records.json`.
- Minor-fault attribution: `target/rfc024-native-batch-pooling-cause-20261006/faults/fault-attribution.json` and raw observations: `target/rfc024-native-batch-pooling-cause-20261006/faults/records.json`.
- Production input hashes: `target/rfc024-native-batch-pooling-cause-20261006/production-input-hashes.json`.
