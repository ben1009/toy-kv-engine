# RFC 024: batch preparation follow-up, 2026-10-04

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

The initial screening retained no additional production change. Three
isolated experiments targeted allocation and synchronization costs in
parallel WAL batch writes. None passed the frozen incremental retention rule.
A subsequent code inspection retains the debug-only-lock cleanup described
below, without claiming a throughput gain. The
[validated extent-preparation improvement](rfc-024-large-batch-preallocation-20261004.md)
from `422d6616` remains intact. These screens establish neither another
throughput gain nor the RFC adoption gate relative to leader WAL.

## Protocol and results

Native io_uring outside the sandbox, fresh databases on the existing
NVMe-backed ext4 filesystem, PITR off, release builds with `bench`, batch64,
16 writers, 1 KiB values, 262,144 puts, and a 1 GiB SST target. Both arms use
parallel WAL. Each experiment has three 32,768-put warmups per arm, then
fixed ABBA/BAAB/ABBA blocks with two scored observations per arm per block.
Executables run from RAM, output is captured in memory during execution,
and five seconds idle follows each run. Builds, tests, profiling, and
device administration do not overlap scored timing.

| Candidate | Paired median throughput | Paired median batch p99 | Passing repeat controls | Initial decision |
| --- | ---: | ---: | ---: | --- |
| Defer `Bytes` shared metadata until the first clone | +0.8% | -2.4% | 0/3 | Revert |
| Remove release-mode admission reads used only by debug assertions | -0.8% | +0.9% | 0/3 | Revert |
| Share one encoded prefix for repeated batch values | +2.3% | -2.8% | 1/3 | Revert |

Reported changes are medians of the three fixed block-level geometric-mean
candidate/baseline ratios. A repeat control requires each arm's within-block
throughput spread at most 5% and p99 spread at most 10%. Retention requires
at least +5% target paired median throughput, all three target blocks
positive, at least two passing target controls, and independent guards no
worse than -5% throughput or +10% p99. None qualified for guard testing.
This incremental rule is separate from RFC adoption against leader WAL.

The repeated-value candidate's block throughput changes were **-4.5%,
+2.3%, and +8.9%**. Its fastest individual observation reached 614,385
puts/s, but neither that observation nor the favorable final block
qualifies a repeatable improvement.

## What each experiment changed

The first candidate used `Bytes::copy_from_slice` for publication copies
in parallel batches with at least 64 entries. The existing helper reserves
an extra byte to create shared metadata immediately; the candidate delays
that metadata allocation until the first clone. Publication still owns a
copy of every key and value. Its small median throughput change failed
repeat controls; a first-clone read cost would also need evaluation before
retention.

The second candidate removed admission-mutex reads whose values are used
only by `debug_assert!` in the packer, group completion, and successful
synchronization paths. Debug builds preserve those assertions and their
locked reads. Release disassembly confirms removal of two lock instructions
per function; runtime checks, queue admission, and durability behavior
remain unchanged. That mechanical reduction did not yield a qualified
end-to-end gain.

The third candidate encoded one shared value prefix for ordinary parallel
MVCC batches with at least 64 entries when every value borrows the same
immutable slice and keys are strictly increasing. Mixed operations,
different values, duplicate keys, and PITR retain the existing builder.
The WAL still encodes every logical entry, and memtable publication still
copies keys and values after durability. The candidate reduces preparation
allocations without changing the WAL format, but its measured gain did
not repeat across blocks.

Two separate baseline profiles reported **105/105 ms** batch preparation,
**1,146/1,160 ms** publication copying, and **263/307 ms** `fdatasync`.
Two repeated-value candidate profiles reported **107/85 ms**, **1,457/991 ms**,
and **267/287 ms**, respectively. These are instrumented aggregate phase
wall times, including concurrent threads and waiting; they are diagnostic
observations, not paired CPU savings or scored throughput results.

### Follow-up inspection: retain debug-only-lock cleanup

The three admission-mutex reads serve only invariant checks. Queue draining
already reads admission under its mutex, worker submission and completion
use synchronized channels, and frontier updates hold the durability mutex.
The extra reads mutate no admission state and establish no required release
synchronization. Admission, barrier cutoffs, poison handling, unassigned-ticket
validation, and durability notification retain their existing locks.

The cleanup removes one extra acquisition per packed group, processed group
completion, and successful sync in release builds. Debug builds retain the
same invariant reads under the admission mutex. Release disassembly confirms
two fewer `lock cmpxchg` instructions in each of the three functions: one
acquisition and one release per removed mutex guard.

This narrow cleanup is retained separately after inspection. The earlier
**-0.8% throughput, 0/3 controls** remains the recorded screening result;
it establishes neither a repeatable throughput gain nor a regression.

The retained cleanup passes `cargo make check`, including default and all-feature
Clippy and **1,421 tests with no skips**. Another **104 selected tests** pass in
release mode, exercising the paths with debug assertions disabled; 1,144
unselected tests are filtered out. Inspection and check artifacts are saved
under `target/rfc024-debug-admission-locks-20261004/`.

## One-second regression retries

All originals and retries remain in the artifacts. Retries do not replace
the fixed primary blocks or rescue a failed retention decision.

The first candidate encountered latency cliffs in both arms: primary
block 1 reached 16.8 ms baseline p99 and 10.4 ms candidate p99; block 2
reached 14.9 ms and 16.6 ms, respectively. After an additional one-second
pause, both affected blocks were rerun. Their retry throughput changes
were **+4.2% and -3.0%**, with **zero passing controls**. The recovered
latency range was 3.05-3.66 ms. The pause helped recover normal latency
in these observations, but did not establish an optimization benefit.

The third experiment additionally retries a scored observation once if
p99 exceeds 6 ms, or, after two previous primary observations of its arm,
throughput drops more than 10% or p99 rises more than 20% relative to
their median. Baseline block 2 triggered this rule: **487,661 puts/s,
3.585 ms p99** became **536,936 puts/s, 3.403 ms** after the additional
one-second pause. The original remains in the primary calculation. No
retry trigger fired in the debug-assertion experiment.

There are **63 benchmark observations**: 18 warmups, 36 primary scored
runs, and nine retry scored runs. The four instrumented diagnostic
profiles are separate.

## Validation and restoration

Selected tests passed outside the sandbox: **196** for deferred shared
metadata, **104** for debug-only lock reads, and **128** for repeated-value
preparation. These are filtered suites, not full-workspace checks. The
temporary repeated-value test covers caller-buffer mutation after commit,
serializable and ordinary writes, duplicate-key last-write-wins fallback,
and close/recovery. It is archived with the rejected patch.

All raw records, reported rates, completed CQEs, commit buffers, mode
settings, fixed block calculations, source snapshots, and binary checksums
were verified. At the end of screening, all **118 Rust files** matched the
baseline manifest. The normal release executable was rebuilt and matched
the saved baseline; `cargo fmt --all --check` passed. Disposable databases and
RAM staging directories were removed. Each candidate has a unified rejected patch
that applies cleanly to the restored baseline.

Baseline executable SHA-256:
`9d9091390f8ba7fdff6f2aaf5c0846a6701b647c6e94c5c571e50d4594690031`.

Artifacts retain binaries, source manifests, drivers, protocols, raw
outputs, test logs, profiles or disassembly, and rejected patches under:

- `target/rfc024-batch-bytes-20261004/`
- `target/rfc024-batch-bytes-screen-20261004/`
- `target/rfc024-batch-assertion-locks-20261004/`
- `target/rfc024-batch-assertion-locks-screen-20261004/`
- `target/rfc024-batch-repeated-value-20261004/`
- `target/rfc024-batch-repeated-value-screen-20261004/`

The final directory also contains the record/provenance validator and
restoration checksum. These three screens did not compare against leader WAL.
