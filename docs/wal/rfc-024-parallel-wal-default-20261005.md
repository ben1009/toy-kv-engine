# RFC 024: parallel WAL default adoption

**Decision:** 2026-10-05. **Last updated:** 2026-10-08.

Ordinary v4 MVCC WALs now use the parallel pipeline by default. This applies
when WAL is enabled; it does not enable WAL for configurations that disable it.
The change follows the maintainer's explicit adoption decision on 2026-10-05.
It is not a claim that the original RFC performance gate passed.

The proposed [RFC 025](../../rfcs/025-parallel-wal-embedded-frontiers.md) extends
the default policy to future formats, with narrowly documented exceptions.
PITR v7 embeds DATA/FRONTIER frames, with one WAL sync per advance and an
explicit Active candidate-validation/fallback contract. Durable manifest and
sealed archive boundaries remain strict. The proposal requires implementation
and qualification; the runtime selection below remains the shipped behavior.

## Runtime selection

`WalIoMode::default()` is `Parallel`. Synchronous `KvEngine::open`,
`open_async`, and `open_repairing` use that default. Direct `Wal` and
`MemTable` create/recovery entry points do likewise. Ordinary v4 successor
WALs retain the engine's selected mode through rotation and reopen.

The default pipeline uses a dedicated ordered packer, one io_uring worker,
and an independent durability coordinator, with 32 in-flight group slots and
256 ring entries. Ext-family filesystems also use an extent-initialization
thread to prepare allocation ahead of writes. Ticket order, physical offset
order, contiguous durability, and ordered MVCC publication remain the same.
Each admitted batch gets a monotonic ticket; the packer combines contiguous
tickets into ordered I/O groups. Durability advances only across a contiguous
ticket prefix. Non-serializable async point writes await buffer budget,
durability, and publication natively. Range writes and transaction commits
retain their blocking async boundary while their v4 WAL uses the parallel
pipeline. A handle moved out of its constructor `Arc`, or rewrapped in another
`Arc`, uses the owned blocking compatibility path.

After a sync takes at least 100 microseconds, the coordinator may coalesce
for at most 400 microseconds. It refreshes the optional admission cutoff when
the written prefix catches it, using only the original deadline's remaining
time. A bounded completion drain follows, then the actual `fdatasync` target
is captured once. Public sync and rotation cutoffs remain fixed. The worker
uses `SINGLE_ISSUER` and `COOP_TASKRUN`; deferred task execution, forced async
SQEs, notification experiments, pooling, and dependency padding are unretained.

Freeze drains memtable guards and async leases, then closes and retires the
old WAL runtime before installing its successor. Explicit close drains both
healthy and poisoned WALs. Unused speculative extent-preparation failures do
not invalidate healthy close; a write requiring the failed suffix still
poisons that ticket boundary. Kernel-owned buffers remain retained through
terminal completions or proven-safe ring teardown.

PITR v5/v6 WALs and older MVCC formats retain the leader path. Legacy unframed
WALs retain buffered I/O. Format detection selects the compatible path on
recovery; the default change does not rewrite existing WAL formats. There is
no automatic fallback when io_uring or O_DIRECT initialization fails.

Leader remains available through `KvEngine::open_with_wal_io_mode(...,
WalIoMode::Leader)` for controls and explicit rollback. This existing selector
is public but hidden from generated API documentation. `write-perf` defaults
to parallel for its v4 WAL comparisons; use `--wal-io-mode leader` for an
explicit leader control. PITR benchmark configuration selects and reports
leader regardless of the v4 selector.

## Evidence and limits

The [October 5 direct comparison](benchmarks/rfc-024-native-async-vs-leader-20261005.md)
measured revision `d7d124b8` before the default change. It explicitly selected
each arm, so its measurements remain valid for those paths. One-put peak
throughput improves by 36.3%–129.8% at 1, 4, 8, and 16 writers, with lower p99
in the selected runs. Process CPU per put increases in every paired one-put
case. Batch64 peak throughput changes range from -8.8% to +8.9%; its
sixteen-writer paired throughput gain is +7.1%.

The [latest batch64 rerun](benchmarks/rfc-024-native-batch64-leader-rerun-20261006.md)
measures the retained completion-drain and cutoff-refresh backend. It reports
-32.4% and +6.9% paired throughput at eight and sixteen writers, with +138.6%
and -0.7% paired p99. Only one of five repeat blocks passes at each count;
the eight-writer Leader null fails. Independently selected peaks change by
-7.5% and +10.4%. These maxima do not establish a repeatable gain. The input
manifest includes then-pending default edits, and null pairs ran after all
scored blocks instead of the frozen protocol's earlier position. Both arms
explicitly select their mode.

Both direct comparisons include the public API and execution model as well
as the WAL mode: synchronous Leader client threads versus native async
Parallel tasks. They do not isolate Tokio scheduling or WAL I/O alone.
The later correctness fixes for moved handles and unused extent failures
have not been performance-measured.

Repeatability is incomplete. The October 5 batch64 one-writer throughput
guard regresses 5.6%, and its eight-writer p99 guard regresses 13.8%.
The full RFC performance gate remains **UNQUALIFIED**. The adoption decision
accepts these measured limits. Future reports must retain originals and
failed controls, distinguish peaks from paired evidence, and compare
explicit modes in the same executable. The
[benchmark overview](benchmarks/rfc-024-parallel-wal-benchmark.md) links the retained
and rejected experiments; incremental parallel comparisons do not replace
the direct Leader data.

Earlier RFC 024 experiment reports retain their original measurements and
recommendations. Their leader-default and opt-in statements describe the
revision measured at the time; this document records the current default.

## Validation

`cargo make check` passes on the final default-selection revision: default
and all-feature Clippy under `-D warnings`, formatting, dependency order,
unused-dependency checks, spelling, and all **1,469 tests without skips or
retries**. io_uring tests run outside
the sandbox with `TMPDIR=/dev/shm`. Focused default-feature checks exercise
WAL create/recovery, synchronous open, rotation, native async writes, async
reopen, repairing open, benchmark selector reporting, and PITR v5 leader/header
preservation. `cargo doc --workspace --all-features --no-deps` also passes
with `RUSTDOCFLAGS="-D warnings"`, including the corrected method links,
public/private item references, and HTML markup.

Tests that inspect follower waits, the leader's empty-ticket behavior, or
failpoints inside leader submission now explicitly select leader. Default
pipeline coverage and parallel crash-prefix/lifetime tests remain enabled.
All current and historical RFC 024 reports link this adoption note; historical
measurements and their recorded recommendations are preserved.
