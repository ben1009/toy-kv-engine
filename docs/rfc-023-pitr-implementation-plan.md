# RFC 023: Point-in-Time Recovery Implementation Plan

**RFC:** [RFC 023: Point-in-Time Recovery](../rfcs/023-point-in-time-recovery.md)  
**Status:** Planned  
**Last updated:** 2026-09-10

## Purpose

This document turns RFC 023 into reviewable implementation slices. The order is
intentional: commit ordering and the WAL format become independently correct
before PITR lifecycle state, repository publication, backup integration, or the
public API depends on them.

No public PITR API ships until the synchronous lifecycle, status, verification,
retention, restore compatibility, and publication outcomes are complete.

## Current-Code Constraints

1. MVCC commit timestamps are currently calculated as `current_ts + 1` while a
   short write lock is held, but `current_ts` advances only after WAL durability
   and memtable publication. Concurrent writers can therefore reserve the same
   timestamp. The ordered commit sequencer is a correctness prerequisite.
2. The WAL already supplies tickets, parallel direct-I/O submission, group
   `fdatasync`, poisoning, and ordered ticket completion. The sequencer must
   extend those mechanisms without serializing WAL I/O.
3. Point entries and range tombstones currently use separate WAL encoders and
   recovery aggregation. WAL v5 requires one canonical mixed-operation batch
   representation shared by live recovery and PITR replay.
4. WAL files are named using memtable/SST IDs and are removed after a durable
   flush. PITR segment identity must be independent, and reclamation must become
   archive-pin aware.
5. Manifest v6 contains immutable SST/vLog identities but no PITR source
   lifecycle. Manifest v7 must preserve the complete PITR state in every
   snapshot.
6. RFC 022 already provides descriptor-relative repository access, immutable
   object publication, catalog recovery, pinned-descriptor restore handoff, and
   explicit publication outcomes. PITR extends those primitives rather than
   adding a second filesystem safety layer.

## Pre-Implementation RFC Clarifications

Resolve these points before the corresponding format code lands:

1. Record configured maximum key, value, entry-count, batch, seal-index, and
   catalog-frame sizes in one normative limits table so encoders, decoders,
   admission, status, and verification use identical bounds.
2. Define the archive limiter's exact I/O scope. Charge source WAL/seal reads
   and repository WAL/seal writes performed by the background archiver; do not
   charge repository catalog replay, verification, publication revalidation,
   restore, or `verify_pitr` reads to that bucket.
3. Obtain a normative RFC decision for runtime behavior before an operator
   replaces non-persisted options after reopen/resume. The committed RFC does
   not define a complete default `PitrRuntimeOptions` value or pass runtime
   options to `resume_pitr`. Implementation must not choose silently between a
   documented default, paused archival, or an API that accepts runtime options.

RFC 023 now defines the slice-1 wire decisions: canonicalization preserves the
relative caller order of all retained point and range entries after removing
earlier duplicate point operations, and `header_crc32` covers batch-header bytes
`0..28`. The codec and golden vectors implement those normative choices; these
two items no longer block live WAL activation in a later slice.

## Implementation Slices

### 1. Canonical contracts and WAL v5 fixtures

Add crate-private types without changing live writes:

- `CommitTs`, `CommitTicket`, and `CommitReservation`;
- canonical `WalBatch` and `WalEntry::{Put, PointDelete, RangeDelete}`;
- `WalV5Header` and `WalV5BatchHeader`;
- `TimelineId`, `ArchiveEpochId`, and `SegmentId`;
- `ChainAnchor`, `SegmentAnchor`, and `CommitTimeHighWater`.

Add golden byte fixtures for file headers, all entry kinds, batch headers,
negative system times, CRCs, zero alignment gaps, and malformed inputs. Keep
decoding bounded before allocation.

**Acceptance:** encoder/decoder round trips and exact golden bytes pass; live
WAL behavior is unchanged.

### 2. Ordered commit sequencer

Replace `current_ts + 1` allocation with a sequencer that owns:

- unique commit timestamp and ticket reservation;
- definite pre-durability failure retirement;
- unknown-durability poisoning;
- per-ticket durable and published completion state;
- contiguous reader-visible publication frontier;
- admission cutoff and drain for rotation/barriers.

Canonicalize and checksum the payload before reservation. Preserve parallel WAL
submission and group `fdatasync`; only reservation, recorded-time finalization,
and frontier advancement are logically ordered.

Migrate every MVCC write path in the same slice: single writes, public batches,
range deletes, serializable transactions, and async transactions.

**Acceptance:** concurrent reservations are unique; out-of-order completion
cannot expose a later timestamp first; unknown durability blocks admission;
recovery reconstructs a safe next timestamp; `cargo make check` passes. Capture
a 1/4/8/16/32-writer performance baseline before WAL v5.

### 3. Dormant WAL v5 codec and recovery substrate

Implement WAL v5 behind crate-private, explicitly selected test paths. Do not
make v5 the live WAL format or create a v5 file until manifest v7 enablement has
persisted its timeline, epoch, segment, and predecessor identity.

Route the dormant encoder and decoder through `WalBatch` and implement:

- the fixed v5 file and batch headers;
- mixed point/range entry encoding;
- ticket-ordered `recorded_at` clamping and header finalization;
- logical length independent of preallocated file size;
- incremental WAL digest and bounded seal-index accumulation;
- strict flags, reserved-byte, length, CRC, alignment, and trailing-byte checks;
- retained v2-v4 normal recovery, with those formats ineligible for PITR.

The v5 recovery substrate must return whole validated batches rather than
separate point and range collections. Existing live writes and ordinary v2-v4
recovery remain unchanged in this slice.

**Acceptance:** normal recovery and a model applier produce identical state for
v5 fixture WALs containing puts, deletes, TTL values, range deletes, duplicate
keys, and mixed batches. No production open or write path can select v5 yet.

### 4. Manifest v7 PITR state machine

Introduce one persisted `PitrState` structure embedded in every v7 snapshot. It
contains configuration, repository/timeline/epoch identity, active segment,
predecessor anchor, recorded-time high-water, last commit anchor, obligations,
and enabled/disabled/reconciliation state.

Add serialization, validation, replay reduction, and snapshot preservation for:

- enable intent and finalization;
- sealing intent;
- sealed segment and successor installation;
- archive completion;
- source reclamation completion;
- clean disable;
- forced coverage gap.

Manifest replay must validate and reduce these records before the live engine
is constructed. This slice is a dormant persistence substrate: it does not
expose or execute `enable_pitr`, create a v5 successor WAL, start archival, or
advertise a recovery interval.

**Acceptance:** failpoints at every transition recover exactly one active
segment and preserve all obligations in synthetic manifest fixtures. Existing
databases continue writing their current manifest and WAL formats, and no live
v3-v6 to v7 enable transition is reachable.

### 5. Segment manager and pin-aware source reclamation

Add an engine-owned `PitrSegmentManager` responsible for rotation, sealing,
source pins, and spool reservations. Segment IDs are independent of SST IDs,
even when rotation also freezes the current memtable.

Use one coalescing rotation path for size, timer, backup, explicit barrier, and
shutdown requests. The admission pause must not rescan or rehash the WAL.

Replace unconditional post-flush WAL deletion with:

```text
ordinary WAL and unpinned       -> delete after durable flush
PITR obligation present        -> retain WAL and seal
archive + source state durable -> delete pair, fsync directory, release space
```

Treat the source WAL and `.seal` as one reclamation unit.

**Acceptance:** concurrent write/freeze/flush/rotation tests show no missing or
duplicate batch, two active segments, or premature WAL deletion. Reopen retries
partial cleanup without releasing its reservation early. These tests use an
internal lifecycle harness over the dormant v5/v7 substrate; no production
enable or reopen path selects PITR yet.

### 6. Backpressure and seal-boundary barrier

Implement accounting for:

- logical active/sealed/unarchived WAL bytes;
- physical WAL preallocation and allocated extents;
- seal files and temporary files;
- manifest obligations and snapshot replacement;
- one shared terminal-maintenance reserve.

All source-manifest writers use the shared physical-spool allocator with the
RFC-defined lock ordering. Reject writes before WAL admission when their atomic
reservation would exceed a bound.

Add a crate-private seal-boundary barrier primitive:

1. stop admission at a sequencer boundary;
2. drain earlier reservations and publication;
3. seal and install the successor;
4. release later writes;
5. return the sealed boundary to a caller-owned test harness.

This is not yet `create_recovery_point`: repository archival and its typed
publication decision are completed in slice 7.

**Acceptance:** bounds cannot be exceeded by concurrent writers; oversized
batches fail without a gap; terminal maintenance can always seal or reclaim;
concurrent seal boundaries coalesce without weakening their individual
boundaries. No public durability claim is made from sealing alone.

### 7. PITR repository catalog and archiver

Extend `BackupRepository` with immutable WAL/seal objects and a framed
`PITR_CATALOG`. Reuse RFC 022 locking, descriptor validation, object staging,
no-replace publication, fsync, replay, and ambiguous-publication revalidation.

Implement:

- prepare/commit and coverage-break catalog records;
- predecessor-chain and timestamp validation;
- bounded retry with the runtime token-bucket limiter;
- exact charging of archive source reads and repository writes, including
  retry I/O, while excluding verification/revalidation reads;
- immediate application of online limiter changes to the next bounded I/O
  chunk, including while a larger WAL/seal object remains in flight;
- source-manifest `Archived` publication before pin release;
- catalog snapshot/compaction and orphan accounting;
- repository/source identity reconciliation after reopen.

Complete the crate-private `create_recovery_point` state machine by composing
the slice 6 seal boundary with archive object/catalog publication and the
source-manifest `Archived` transition. Only its durable outcome closes the
reported archive gap.

**Acceptance:** reopen reconstructs the same longest valid chain and rejects
forks, gaps, overlaps, regressions, corruption, duplicate commits, and mixed
timeline/epoch identities. Limiter tests account for every charged archive I/O
path and prove catalog replay, verification, and revalidation reads are not
charged. A runtime update leaves only an already granted bounded chunk under the
old policy; every later chunk uses the new limiter.

### 8. PITR enablement and PITR-aware base backup

Extend RFC 022 generation metadata with repository, timeline, epoch, included
commit high-water, boundary anchor, base time anchor, WAL replay version, and
persistence-affecting compatibility metadata.

Integrate the previously dormant components into the executable `enable_pitr`
transition. Under stopped admission, persist the enable intent, freeze legacy
state, create and fsync the identity-bound v5 successor, and publish manifest v7
before writes resume. Start the archiver, but advertise no recovery interval
until the mandatory first base commits. Crash or ambiguity handling must never
allocate a second identity while the first enable decision is unknown.

At backup capture:

1. stop admission and rotate the boundary segment;
2. flush all boundary memtables while state mutation is excluded;
3. capture canonical state and immutable-file pins;
4. release exclusion before long object copies;
5. publish the base and its PITR binding consistently.

Existing RFC 022 generations remain ordinary backups but are never selected as
PITR bases.

**Acceptance:** a base restores independently to exactly its included boundary,
contains no later commit, and binds the exact next segment predecessor. Kill
tests at every v3-v6 to v7 enable step recover either the legacy state or exactly
one timeline/epoch/v5 successor, with no advertised pre-base interval.

### 9. Exact `CommitTs` restore prototype

Implement crate-private exact-timestamp restore before the public API:

1. validate the selector, catalogs, base, and complete required chain;
2. pin all required descriptors before releasing the repository lock;
3. validate every required segment fully, including the final segment tail;
4. restore the base through RFC 022 staging;
5. assign a new timeline and sanitize inherited PITR lifecycle state;
6. apply whole batches through the target with a private recovery applier;
7. flush, persist the timestamp frontier, remove recovery WALs, and close;
8. write `RECOVERY_INFO` and atomically publish the destination.

The private applier preserves commit timestamps and encoded TTL expiration, but
does not allocate timestamps, run OCC, call user code, or append archived bytes
to a live WAL.

**Acceptance:** restores at the base, every commit, and numeric timestamp gaps
match model snapshots. No transaction, batch, or range operation is partially
visible. Corruption and gaps fail before publication.

This is the Phase 1 prototype milestone. It remains crate-private.

### 10. Complete synchronous public operations

Expose the RFC's public types and synchronous operations only after the exact
prototype is stable:

- enable, resume, clean disable, and forced-gap disable;
- recovery-point creation;
- exact, `Latest`, and wall-clock restore;
- bounded paginated status;
- shallow and deep verification;
- paired backup/PITR retention;
- compatibility resolution through `ImplementationRegistry`;
- all durable, non-durable, unknown, and cleanup-incomplete outcomes.

Before implementing automatic reopen or `resume_pitr`, resolve the runtime
option contract tracked in the pre-implementation decisions. Tests then cover
the chosen behavior before and after `set_pitr_runtime_options` and verify that
neither operation changes the archive epoch.

RFC 022 `purge(retain)` must reject PITR repositories without mutating them.

**Acceptance:** all supported formats and features validate before staging;
status never advertises an unbased or broken interval; paired retention cannot
expose mixed catalogs or delete an object needed by an advertised target.

### 11. Async APIs and cancellation

Wrap the proven synchronous state machines in engine-owned tasks. Cancellation
is checked only between bounded streaming operations, never during a catalog
commit, source-manifest transition, or final rename decision.

Make `close()` and `close_async()` delegate to PITR close semantics when PITR is
active. Drop remains best effort and makes no durability claim.

**Acceptance:** cancellation has one typed terminal outcome; a possibly
published destination or catalog is never automatically retried or removed.

### 12. Chaos, compatibility, and performance gate

Complete the RFC 023 test matrix with emphasis on:

- process kills around every seal, manifest, object, catalog, cleanup, and
  rename durability boundary;
- external durable-operation oracle comparison at every recovery target;
- source loss at every reported archive-lag state;
- ENOSPC, repository unavailability, clock rollback, and cleanup failure;
- weak-memory sequencer publication tests;
- bounded catalog/status/verification allocation;
- vLog, TTL, range tombstone, compaction-filter, and future-format rejection.

Run the benchmark matrix with 1, 4, 8, 16, and 32 writers for PITR disabled and
enabled/caught-up modes. Separately measure archive lag/backpressure, same-device
and separate-device repositories, configured rate limits, and rotation pause
components.

**Acceptance:** the complete RFC acceptance criteria pass, benchmark baselines
are reported honestly, and `cargo make check` succeeds.

## Pull Request Sequence

Keep the review surface narrow with this approximate sequence:

1. canonical batch contracts and WAL v5 fixtures;
2. ordered commit sequencer;
3. dormant WAL v5 codec and recovery substrate;
4. manifest v7 state machine;
5. segment manager and pin-aware reclamation;
6. backpressure and seal-boundary barrier;
7. PITR repository catalog and archiver;
8. PITR enablement and PITR-aware base backups;
9. exact restore prototype;
10. public synchronous operations and compatibility;
11. async operations and cancellation;
12. chaos completion, benchmarks, and documentation.

Each PR must include its own crash/recovery tests and pass the normal repository
gate. Format- or durability-changing PRs must not be merged with knowingly
uncovered recovery windows deferred to a later PR.

## Immediate Next Slice

Start with slice 1, the canonical internal contracts and WAL v5 fixtures, as an
independent PR with no live-write change. Follow it with slice 2, the ordered
commit sequencer, as a separate PR. The sequencer closes a current concurrency
correctness gap and establishes the single ordered durability/publication
frontier required by every later PITR component. Do not expose PITR
configuration or repository APIs in either slice.
