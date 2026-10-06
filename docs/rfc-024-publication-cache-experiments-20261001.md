# RFC 024: Publication cache experiments, 2026-10-01

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

**Decision:** Reject both publication-frontier prototypes. Padding does not
establish an ext4 gain and has an unresolved tmpfs throughput regression.
Skipping unchanged frontier stores shows a small initial 64-writer gain that
does not satisfy the fixed confirmation rule. Restore the original MVCC source
and Cargo manifests. Parallel WAL remains opt-in; the
[full qualification gate](rfc-024-wal-qualification-20261001.md) remains unmet.

## Motivation and scope

The preceding [baseline CPU capture](rfc-024-wal-optimization-followup-20261001.md#worker-integer-maps)
attributes 23.88% of atom-PMU and 31.87% of core-PMU sampled user cycles to
MVCC publication. Annotating `publish_commit_ts` shows repeated polling of
the mirrored frontier at struct offset `0xd8`, beside the live-reservation
counter at `0xe0` and waiter state. These offsets suggest testing cache
contention; they do not prove the fields share a physical cache line in every
allocation or identify the cause of publication waits.

Both prototypes change shared MVCC code used by parallel and leader WAL.
Neither changes the WAL worker, sync coordinator, spin budgets, ready-prefix
ordering, poison boundary, or default backend. A leader case checks for
regressions in the shared change.

## Method

Use release `write-perf` builds with `--features bench`, PITR off, 1 KiB values,
a 1 GiB SST target, and fresh paths. The baseline is `e7f6a554`, whose runtime
matches `d7171c80`. Snapshot binaries and source, then freeze hashes, workload
parameters, ABBA/BAAB order, and time limits before each comparison. Rotate
case order between three rounds; each case has four scored runs per round,
with each arm appearing twice.

Single-put ext4 parallel cases use 200,000 puts at 16, 32, and 64 writers,
sampling every tenth operation, with one 50,000-put warmup per arm per case.
The eight-writer ext4 batch case uses 524,288 puts in 64-entry commits,
sampling every commit, with one 262,144-put warmup per arm. Tmpfs uses
`/dev/shm`, one writer, 50,000 single puts, sampling every tenth operation,
and one 20,000-put warmup per arm. The 16-writer leader guard uses 200,000
puts and 50,000-put warmups for padding, and 100,000 puts and 25,000-put
warmups for the unchanged-store prototype, with every tenth operation sampled.

A block ratio is the geometric mean of its two candidate measurements divided
by that of its two baseline measurements. Tables report medians of block
ratios. Repeat controls require each arm's throughput max/min to be at most
1.05 and its p99 max/min to be at most 1.10. Preserve every block, including
stalls and failed controls. No builds, tests, tracing, or device-admin polling
overlap scored timing. Raw outputs, progress logs, and executable copies stay
in RAM during timing; persist them afterward. Remove each disposable database
after its run.

Both prototypes pass all 101 focused MVCC, serializable-transaction, and
parallel-WAL tests before timing. Each initial comparison completes 72 scored
runs and 12 warmups. These are exploratory measurements without an adoption
confidence-interval claim.

## Pad the mirrored publication frontier

Wrap only `frontier` in `crossbeam_utils::CachePadded<AtomicU64>`, adding the
already-transitive `crossbeam-utils` crate as a direct dependency. Atomic
operations and their ordering remain unchanged. This separates the polling
field from adjacent reservation and waiter updates.

| Case | Writers | Throughput change | p99 change | CPU/put change | Passing control blocks |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4 parallel single puts | 16 | +0.0% | -1.2% | -0.6% | 2/3 |
| Ext4 parallel single puts | 32 | +0.5% | +2.5% | -1.2% | 2/3 |
| Ext4 parallel single puts | 64 | -0.8% | -4.3% | -1.4% | 2/3 |
| Ext4 parallel batch 64 | 8 | -0.6% | -0.9% | +0.0% | 1/3 |
| Tmpfs parallel single puts | 1 | -10.4% | +3.7% | +3.7% | 0/3 |
| Ext4 leader single puts | 16 | +0.2% | +0.1% | -1.2% | 2/3 |

The two passing 64-writer blocks have throughput ratios 0.988 and 0.992.
The other block has large tail swings in both arms and a ratio of 1.580; it
remains included. The final 16-writer block similarly includes a very slow
baseline run. Leader throughput falls by roughly half between earlier and
final blocks in both arms, while its final paired ratio stays near parity.
These observations limit comparisons of absolute throughput across rounds.
The small ext4 medians and unresolved tmpfs loss do not support retaining
the padding. No padding confirmation is run.

## Skip unchanged frontier stores

After restoring the original layout and manifests, condition the mirrored
frontier's release-store on `next_to_publish != previous_frontier` inside
`advance_ready_publications`. A ready arrival behind a missing predecessor
leaves the visible prefix unchanged. Prefix advancement still performs the
release-store; wakeup conditions remain equivalent. This reduces redundant
atomic stores without adding a dependency or altering admission or visibility.

The initial 64-writer median is +2.6% throughput and -1.4% CPU/put. Its three
throughput ratios are 1.026, 1.045, and 0.997, with all repeat controls passing.
The last block is at throughput parity; the 32-writer CPU/put median increases
3.0%. Run one fixed confirmation with three more blocks each for 32 writers,
64 writers, ext4 batch 64, and tmpfs single-writer. It completes all 48 scored
runs and eight warmups using the same binaries and workload parameters.

Before confirmation, require that every new 64-writer block lowers CPU/put,
that its combined median throughput does not regress, and that combined
guard medians stay within 5% throughput and 10% p99 regression limits. Specify
no further extension and combine all initial and confirmation blocks.

| Case | Writers | Combined throughput change | Combined p99 change | Combined CPU/put change | Passing control blocks |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4 parallel single puts | 16 | -0.4% | +2.5% | -0.2% | 2/3 |
| Ext4 parallel single puts | 32 | +0.0% | -1.3% | +1.0% | 4/6 |
| Ext4 parallel single puts | 64 | +0.5% | -0.8% | -1.3% | 5/6 |
| Ext4 parallel batch 64 | 8 | -0.4% | +2.4% | +0.8% | 2/6 |
| Tmpfs parallel single puts | 1 | -1.1% | -5.4% | +0.7% | 0/6 |
| Ext4 leader single puts | 16 | +0.1% | +0.4% | -1.3% | 2/3 |

The three confirmation 64-writer throughput ratios are 0.998, 1.009, and
1.001; CPU/put ratios are 0.989, 0.986, and 1.023. The final block fails tail
repeatability and reverses the CPU improvement. It remains included and
fails the predefined retention rule. Sixteen-writer and leader rows retain
their three initial blocks. The small combined gain and unresolved guard
controls do not support retaining the unchanged-store prototype.

## Restoration and artifacts

Restore `mvcc.rs`, `kv-engine/Cargo.toml`, and `Cargo.lock` byte for byte from
baseline snapshots, update their timestamps so Cargo rebuilds, and verify
the rebuilt release executable matches the saved baseline. No runtime change
or new dependency is retained. Disposable database paths and RAM executable
copies are cleaned up.

Local artifact directories contain frozen protocols, run scripts, source
snapshots, both binaries, rejected patches, raw outputs, run and block records,
summaries, and SHA-256 manifests:

- `target/rfc024-frontier-padding-20261001/`
- `target/rfc024-frontier-store-20261001/`
- `target/rfc024-frontier-store-confirmation-20261001/`

The last directory also contains the combined summary, decision, artifact
verification script, and restored-build provenance. Protocol SHA-256 values
are, in directory order:

```text
41c45b663668f7acf1459db6e8ae9fa158de86f555300a5ef930c87f6db7066b
46662abfde954ad258b0730948b86637763e378e14a3d65c96a0a3a41678baac
63742eea3c11fe1cb699aed6d705c6f56953fbfc435e10375afebdb299f04707
```
