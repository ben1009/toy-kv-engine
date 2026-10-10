# RFC 025 Implementation Plan: Parallel WAL with Embedded Frontiers and One Sync

**RFC:** [Parallel WAL with Embedded Frontiers and One Sync](../../rfcs/025-parallel-wal-embedded-frontiers.md)

**Status:** In progress — Stage 2 codec and persistence-model implementation is
complete. Stage 3 now includes bounded backward FRONTIER discovery, single-pass
candidate-prefix verification, authority/anchor selection, the Active
fresh-inode normalization primitive, and an authority-required coordinator.
Stage 4 now includes the v7 resource-ledger primitive, framed-batch reservation
helper, and serial-model integration. The model reserves before allocating
framed DATA buffers or writing frames, retains the charge through uncertain
sync, and retires DATA buffer memory on successful publication. It retains
WAL/spool/index charges; recovery reconstructs actual retained DATA/FRONTIER
bytes and seal-index capacity, counting coalesced markers only once. Serial
commits reuse the buffer cap, and recovered batches require no outstanding
DATA buffers. Exact-cap recovery and continued append cover coalesced groups.
Limit rejection is checked to leave ticket/offset state and WAL bytes unchanged.
Live segment lifecycle reconstruction is now wired before PITR runtime attach.
Reconciliation updates the recovered bookkeeping after successful manifest
sync, releases reclaimed source charges, and consumes completed sealing's
pending successor reservation while preserving live pins and reservations.
Flush-driven reclamation also releases recovered source charges after its
manifest record is durable, with delayed retirement scoped to the same archive
epoch. Recovery-point publication serializes its manager transition with the
durable Reclaimable record, and both manifest projections publish under the
state lock before concurrent flushes can advance them.
If cleanup loses an empty source before reclamation
is recorded, recovery can retire its memtable registration only with a durable
Reclaimable obligation whose logical length proves it was header-only; missing
nonempty sources still fail recovery.
If a failed sync has already advanced the manifest projection, the poisoned
manifest handle rejects further mutation or sync, and retries retain the pending
bookkeeping and unreclaimed source files until reopen. Source cleanup checks
manifest health under the shared state lock and retains that guard through the
reclamation append, excluding concurrent manifest failures between those steps.
Recovery rewrites the exact accepted snapshot and
manifest bytes to fresh inodes, syncs them, atomically replaces their paths, and
syncs the directory before exposing replayed state. Reconciliation then applies
the recovered bookkeeping without appending duplicate lifecycle records or a
PITR snapshot. Successor WAL registration is made durable before rotation exposes
the new memtable to writers.
Its current source-spool check is a provisional estimate: it counts logical WAL
lengths and a 4 KiB sidecar allowance, but does not include larger seal indexes
or actual filesystem allocation. The shared reservation ledger and WAL admission
integration remain; this estimate is not the final hard-bound enforcement.
Production WAL-open and manifest integration also remain. Stage 5 has started with
recovered-runtime cursor bootstrap: an installed Active image supplies the v7
DATA start, append offset, and next ticket, and the shared runtime seeds its
admission, packer, and durability coordinates from that state. V7 startup stays
closed and performs no extent initialization or buffer-pool warmup until later
runtime slices provide live reservations and the v7 writer path.
The serial persistence model covers repeated normalization, continued append,
and crash/reopen for empty, nonempty, fallback, and trailing-control prefixes,
including a selected marker behind speculative DATA. The production writer
remains v6.

**Last updated:** 2026-10-10

**Source baseline:** `30a64fa855126596316ab6b9237e6348b0e41fd2`, which merged RFC 025 in PR #379.

## Purpose and delivery scope

Implement PITR WAL v7 with embedded DATA/FRONTIER frames, one WAL sync per
durability advance, and Parallel as the default for eligible new segments.
Deliver the codec, recovery, runtime, PITR lifecycle, migration, correctness
coverage, and adoption evidence together. Split development into reviewable
commits with the dependencies below. Intermediate commits may expose a
development selector; activate production creation and migration only after
the final gate.

The RFC is the protocol authority. This plan identifies code changes and
acceptance criteria; it does not change the wire format or recovery contract.
New types and module paths named below are proposed implementation boundaries.
The current writer remains PITR v6 while these stages are incomplete.

The delivery includes synchronous and asynchronous engine entry points,
explicit v7 Leader comparison mode using the same frontier protocol, and
mixed v5/v6/v7 archive chains. Ordinary v4 retains its existing format and
Parallel default. Legacy formats retain their current recovery semantics.
The format registry also establishes the required compatibility and default
checks for future formats.

## Contracts to preserve throughout implementation

| Contract | Implementation requirement |
| --- | --- |
| Ticket versus MVCC timestamp | Segment tickets are physical/durability order; `commit_ts` remains the MVCC coordinate. Preserve timestamp allocation, conflict detection, and ordered publication. |
| Written versus durable | Complete DATA writes only advance the written prefix. A complete FRONTIER followed by successful `fdatasync(WAL)` advances the captured durable prefix. |
| Physical versus covered end | Marker offset and EOF are allocation coordinates. `durable_end = E(T)` describes exactly the retained DATA/control prefix for tickets `[0, T)`. |
| Physical prefix eligibility | For nonempty prefixes, `E(T) = max(D(T), previous_frontier_offset + 4096)`. Every frame below that end must belong to the covered tickets or their canonical control chain. |
| One frontier chain | Every canonical FRONTIER in the retained prefix plus its candidate forms one chain in physical-offset order. A predecessor must be the immediately preceding retained FRONTIER. |
| Two digest domains | The logical prefix digest hashes the immutable header and logical batches in ticket order. The archive `wal_digest` hashes the entire frozen v7 physical image. |
| Active recovery | Select the newest fully verified candidate by physical marker offset; fallback must respect every durable logical anchor. Read/I/O errors are fatal. |
| Immutable recovery | Sealing intent and sealed/archive references bind the frozen image. They prohibit fallback and Active normalization. |
| Recovery installation | Rewrite to a fresh inode, sync it, atomically replace the canonical path, and sync the directory before publication or service. Preserve the header/incarnation. |
| Resource admission | Reserve DATA, worst-case marker, index, buffer, and recovery-workspace growth before assigning a ticket. Rejection leaves no ticket or offset hole. |
| Failure and ownership | An uncertain FRONTIER write or sync ends frontier production in that runtime. Buffers survive until I/O teardown/completion and hash consumption are both safe. |
| Client success | Durability is followed by memtable insertion and ordered MVCC publication. Cancellation must retain the owners needed to finish accepted work. |

Active fallback has the RFC's explicit storage-fault limitation: unanchored
Active bytes cannot always distinguish an interrupted sync from later damage
to previously synced DATA. Tests must document this boundary without weakening
durable anchors or the strict immutable-object rules.

## Existing code seams

| Existing seam | Required work |
| --- | --- |
| [`wal.rs`](../../kv-engine/src/wal.rs): `Wal::create_with_io_mode`, `create_v5`, `new_recovered`, `recover_v5`, `sync`, `close` | Replace v4-only runtime dispatch with a checked descriptor; add explicit v7 creation/recovery and recovered runtime state. Preserve legacy dispatch. |
| [`pitr/mod.rs`](../../kv-engine/src/pitr/mod.rs): header/batch codecs, `WalDigestRule`, `WAL_V5_VERSION` | Keep v5/v6 codecs readable, introduce v7 codecs, and distinguish each digest domain. The historical `WAL_V5_VERSION` currently selects v6; changing that constant alone cannot implement v7. |
| [`wal/parallel/runtime.rs`](../../kv-engine/src/wal/parallel/runtime.rs): admission, packing, extent initialization, sync coordinator | Parameterize data start and recovered ticket state; support control-frame allocations, fixed cutoff snapshots, and single-sync frontier publication. |
| [`wal/parallel/admission.rs`](../../kv-engine/src/wal/parallel/admission.rs) | Extend the deterministic admission model and separately apply its contracts to the live runtime. This file alone does not change live admission. |
| [`wal/parallel/worker.rs`](../../kv-engine/src/wal/parallel/worker.rs): `WriteBuffer`, write groups, CQE handling | Submit noncontiguous DATA ranges without overwriting intervening FRONTIER frames; track full marker completion and ownership. |
| [`wal.rs`](../../kv-engine/src/wal.rs): `PitrSealAccumulator` | Replace the single append/hash assumption with ordered logical and physical hash streams plus bounded cutoff/index snapshots. |
| [`pitr/backpressure.rs`](../../kv-engine/src/pitr/backpressure.rs), [`pitr/segment.rs`](../../kv-engine/src/pitr/segment.rs), [`wal.rs`](../../kv-engine/src/wal.rs) | The spool accountant currently provides helper/model contracts; live batch limits are checked in `Wal::put_v5_batch` and segment accounting grows at lifecycle boundaries. Install and reconstruct a shared live ledger for admission, preallocation, recovery, pins, and cleanup. |
| [`manifest.rs`](../../kv-engine/src/manifest.rs), [`pitr/manifest.rs`](../../kv-engine/src/pitr/manifest.rs) | Version Active logical anchors and immutable Sealing boundaries; extend replay, snapshots, validation, and interrupted-transition recovery. |
| [`mem_table.rs`](../../kv-engine/src/mem_table.rs), [`lsm_storage.rs`](../../kv-engine/src/lsm_storage.rs), [`lsm_storage/write_gate.rs`](../../kv-engine/src/lsm_storage/write_gate.rs) | Carry the descriptor and owned reservations through writes, freeze, successors, reopen, and async ownership. |
| [`mvcc.rs`](../../kv-engine/src/mvcc.rs), [`mvcc/txn.rs`](../../kv-engine/src/mvcc/txn.rs) | Replace v5-only PITR write routing with descriptor-aware preparation; hand sampled v7 recorded time to the ordered packer and preserve legacy clamping, OCC, and timestamp retirement. |
| [`pitr/seal.rs`](../../kv-engine/src/pitr/seal.rs), [`pitr/archive.rs`](../../kv-engine/src/pitr/archive.rs), [`pitr/catalog.rs`](../../kv-engine/src/pitr/catalog.rs), [`pitr/restore.rs`](../../kv-engine/src/pitr/restore.rs) | Add strict v7 physical-image verification, DATA offsets in seal indexes, explicit format metadata, and mixed-chain restore. |
| [`checkpoint.rs`](../../kv-engine/src/checkpoint.rs), [`backup.rs`](../../kv-engine/src/backup.rs), [`pitr/base.rs`](../../kv-engine/src/pitr/base.rs) | Capture verified logical or immutable boundaries and retain the required certificate and file pins. |
| [`pitr/api.rs`](../../kv-engine/src/pitr/api.rs), [`bin/write-perf.rs`](../../kv-engine/src/bin/write-perf.rs) | Expose migration deferral and separate DATA/marker/workspace accounting; report actual format/mode and qualification metrics. |

## Stage dependencies

Stage 2 codec and persistence-model implementation is complete. Stage 4 is in
progress; production WAL-open and manifest integration, stages 5–8, and final
qualification remain planned. Each stage adds its own regression coverage, and
stage 8 assembles the complete qualification evidence.

| Stage | Depends on | Reviewable result | Production writer |
| --- | --- | --- | --- |
| 1. Format and persisted contracts | Existing baseline | Explicit format registry, anchor types, compatibility fixtures | v6 |
| 2. Codec and persistence model | 1 | Byte-exact v7 fixtures and serial protocol driver | v6 |
| 3. Recovery and installation | 1, 2 | Verified candidate selection and crash-safe fresh-inode recovery | v6 |
| 4. Reservations and metadata | 1–3 | Atomic admission budgets and ordered digest/index snapshots | v6 |
| 5. Runtime and API barriers | 2–4 | Development v7 Parallel and Leader with one sync | v6 |
| 6. PITR lifecycle and capture | 3–5 | Strict sealing/archive/restore and anchor-aware backup/repair | v6 |
| 7. Migration and constructor coverage | 4–6 | Development migration, capacity deferral, and complete entry-point dispatch | v6 |
| 8. Qualification and activation | 1–7 | Correctness evidence, performance report, and eligible v7 defaults | v7, with RFC legacy deferral |

### 1. Format registry and persisted boundary contracts

- Add a checked `WalFormatDescriptor` in a proposed `wal/format.rs`. Use
  explicit version matching for unframed, v2/v3, v4, v5/v6, and v7. Describe
  framing, alignment, DATA start, digest/recovery policy, supported modes,
  and default mode. Reject unknown versions, enforce supported new-format
  modes, and preserve legacy mode selection. Require every new writer
  registration to provide Parallel recovery coverage or a reference to its
  narrowly accepted exception RFC and alternative-protocol coverage.
- Thread the descriptor through creation, recovery, sealing, restore, and
  successor selection. Remove scattered v4-only checks without sending
  legacy formats through the v7 parser or weakening strict v5/v6 recovery.
- Separate provisional candidate metadata from a proposed installed
  `RecoveredWalState`, constructed only after the recovery installation barrier.
  Carry the accepted logical boundary, current installed frontier's offset/
  generation/digest, append/hash cursors, resumable logical/physical hash states,
  seal index, and recorded-time state. Seed admission, packing, written, and
  durable ticket frontiers at accepted `T`; do not reset them to zero or derive
  append offset from preallocated EOF.
- Define distinct persisted `ActiveBoundary` and `ImmutableBoundary` types.
  Active stores timeline/epoch/segment/incarnation, ticket end, covered
  physical-prefix end, last commit timestamp, and logical prefix digest.
  Immutable adds sealed end, terminal frontier digest, and whole-image digest.
  Persisted Active references exclude frontier offset/generation/frame digest.
- Specify the next explicit source-manifest schema and compatibility decoder
  before emitting it. Both manifest constants are currently version 7; they
  belong to a separate version domain from WAL v7. Update root manifest
  snapshots/records and PITR replay together, preserving existing manifests
  and rejecting unknown required schemas. Version any backup/catalog reference
  whose serialized contract changes.
- Retain the current v4 and v5/v6 fixtures and capture a baseline constructor,
  recovery, and archive-digest compatibility matrix.

**Exit:** All legacy fixtures and entry points retain their recorded behavior;
unknown versions fail explicitly. New boundary codecs can be exercised without
emitting v7 from a production constructor. Backend initialization failures
remain errors instead of silently changing the selected mode.

### 2. Byte-exact codec and deterministic persistence model

- Add proposed `wal/v7/codec.rs` primitives for the 4096-byte immutable header,
  64-byte common frame header, DATA fragmentation, 48-byte logical batch
  header, and 104-byte FRONTIER body. Require canonical reserved fields and
  padding, actual-offset binding, immutable-header binding, checked arithmetic,
  and existing entry/payload limits after framing overhead.
- Encode all multibyte integers as big-endian and signed integers as
  two's-complement. Preserve raw arrays, magic, digests, and padding as bytes.
  Fragment indices must satisfy `0 <= index < count`; nonfinal fragments
  carry 4008 bytes and final fragments carry exactly the remaining bytes.
- Implement distinct logical-prefix and physical-frame/image digest helpers.
  Validate generation zero at offset 4096; the empty installed image is 8192
  bytes and DATA begins at 8192. Bind the fresh nonzero incarnation to every
  frame through the immutable-header digest.
- Validate nonzero-generation frontier bounds and ordering explicitly:
  `prev_offset + 4096 <= durable_end <= frame_offset`, generation increases
  by one, and ticket end, covered end, and last commit timestamp strictly
  increase. Check predecessor identity/digest as well as numeric fields.
- Add independently specified golden bytes for a complete file header,
  DATA frame, fragmented logical batch, and FRONTIER. Include an integer
  `0x0102030405060708`, negative `recorded_at_secs`, nonzero offsets, CRC
  coverage, zero padding, and SHA-256 preimages. Encoder/decoder round trips
  supplement these fixtures; they cannot be the only codec oracle.
- Add a positive DATA fixture whose user value contains a complete marker-shaped
  byte sequence with plausible magic, CRC, and digest fields. The value must
  round-trip exactly, and discovery must not recognize payload bytes as a
  FRONTIER instead of inspecting codec-owned aligned frame headers.
- Build a proposed test-only persistence model and serial driver. Model
  volatile/cached bytes separately from stable bytes, arbitrary unsynced
  persistence, interrupted/failed sync, successful-sync guarantees, process
  restart without reboot, file replacement, directory persistence, and cleanup.
  Retain the successful-write/ACK history separately from file contents.
- Exercise creation of header plus generation zero, file sync, directory
  sync, and eventual durable Active-manifest installation. An incomplete
  never-installed file follows orphan cleanup; a damaged installed file fails.

**Exit:** Golden fixtures fix every wire field and digest domain. The serial
driver demonstrates DATA plus FRONTIER followed by one sync, including a
complete stable marker with incomplete DATA after an interrupted sync. It is
a protocol oracle, not the released v7 writer.

### 3. Independent recovery and durable fresh-inode installation

Stage 3 includes bounded backward discovery of structurally valid FRONTIER
candidates, one bounded forward pass to verify their logical batches and
retained control frames, authoritative selection against every durable Active
anchor, and strict Sealing/Sealed image checks. Active recovery normalizes to a
fresh inode, reconstructs retained batches and append/hash/index state, and the
serial model exercises continued writes and crash/reopen. The current slice
adds an authority-required Active recovery coordinator that runs canonical
header validation, discovery, candidate selection, and fresh-inode installation
as one operation, returning both diagnostics and installed state. The
coordinator does not infer authority from the WAL and is not yet called from
production WAL open or manifest publication. Immutable images are not
normalized.

- Add proposed `wal/v7/recovery.rs`, receiving authoritative Active/Sealing/
  Sealed context and all durable anchors. A discovered seal sidecar cannot
  promote an Active segment to immutable authority.
- Discover aligned FRONTIER headers backward in batched reads, independently
  of forward DATA decoding. Continue across holes, zeros, preallocation,
  broken DATA, and a partial final frame. Validate candidate offset/binding/
  CRC/bounds before following previous offsets; memoize frame/chain results.
- Verify candidates using one streaming forward pass and bounded metadata
  sorted by named prefix end. Check whole batches, fragments, canonical
  payloads, timestamp order, every intermediate frontier's own covered prefix,
  `E(T)`, exact ticket count/end/digest, and the complete list of retained
  control frames. A reachable-ancestor walk alone cannot prove the single chain.
- Reject forks, siblings, duplicate generations, skipped predecessors, and
  orphan canonical controls inside a retained prefix. Stop usable-prefix
  advancement at the first invalid physical frame. Evaluate controls outside
  a candidate's covered prefix independently; a stray control in its
  speculative tail does not invalidate that candidate.
- Select by physical marker offset after full verification. Active fallback
  may discard an invalid candidate only above verified durable floors. Verify
  every exact anchored prefix even if the selected candidate has a greater
  ticket. Aggregate anchor coverage requirements once, then check each candidate
  in constant time; same-ticket anchors with different physical ends still
  require a higher-ticket candidate. Reject read errors, a damaged header, or
  a missing, damaged, or noncanonical generation-zero marker; reject wrong
  incarnation, conflicting anchors, and immutable-image disagreement.
- Expose one Active recovery entry point that requires caller-supplied
  manifest authority and composes canonical header validation, bounded
  discovery, candidate selection, and fresh-inode installation. Return the
  selected diagnostics with the installed state. Reject immutable authority;
  do not derive a durable anchor or predecessor from the WAL bytes. Keep
  database WAL-open wiring and durable manifest publication as a later slice.
- For Sealing/Sealed, require the exact terminal marker, logical boundary,
  sealed length, and whole-image digest. Never fall back to an older marker or
  normalize an immutable image.
- Return bounded diagnostics for the selected boundary, rejected candidates,
  discarded ranges, and fallback reasons. Keep diagnostics separate from the
  authoritative recovered state and from raw payload bytes.
- Implement proposed `wal/v7/install.rs` under exclusive recovery ownership.
  Drain old I/O, reserve workspace, copy the original header and
  `[4096, durable_end)` unchanged to a fresh same-directory inode, and append
  the recovery frontier at `durable_end`, extending the last retained frontier.
  After canonical selection, remove only matching orphan recovery images and
  sync the directory before reusing their space. For an empty prefix retain
  generation zero. Omit the old selected certificate and speculative suffix.
  On an in-process failure, remove the temporary image and sync its directory;
  if cleanup fails, propagate both errors and leave the workspace charged for
  later durable cleanup.
- Verify writes and successfully sync the fresh file even when its length
  would be unchanged; atomically replace the canonical path and sync its
  directory before opening service or publishing recovered state. Creation's
  existing `install_pitr_file_no_replace` is a different operation; replacement
  needs its own installation/cleanup protocol.
- Rebuild resumable logical hash state and the seal index from verified retained
  batches. Hash the fresh normalized image in physical order during copy,
  including its replacement recovery marker; control bytes do not change the
  logical hash. Publish this installed state after the file/directory barrier,
  using the replacement marker's offset/generation/digest as the predecessor
  for resumed append and `durable_end + 4096` as its physical end/hash cursor.
  Discard source-candidate marker identity and physical hash state that include
  omitted bytes. For an empty image, preserve the generation-zero marker and
  header-only logical hash; immutable-image hashes follow the exact frozen bytes.
- For Sealing, fresh installation may copy only the exact frozen intent image;
  it cannot relocate/rechain its terminal marker. Keep failed temporary files
  and pinned old images charged until verified durable cleanup. A copy/sync/
  replace/directory-sync failure fails open and cannot trigger older fallback.
- Restore next ticket, append offset, timestamp allocation floors, recorded-time
  clamp, and accepted DATA. Complete unmarked batches cannot establish new
  permanent v7 recovery coordinates. Repeat normalization must be byte-identical
  without growing the frontier chain.
- The serial model exercises two normalizations followed by append, sync, and
  crash/reopen for empty, nonempty, fallback, and trailing-control prefixes.
  It includes a selected source marker behind speculative DATA and independently
  compares continued hashes/index, anchors, tickets/offsets/time state,
  contents, and explicit pre-crash ACK history. Recovered durable tickets do
  not imply that the prior client observed an ACK. Extend the same cases through
  real seal/archive/restore in stage 6 and final qualification; those integration
  legs are not stage 3 prerequisites.

**Exit:** The serial persistence model passes candidate/floor/normalization
crash cases before Parallel is connected. Recovery uses bounded discovery plus
one prefix pass, with no whole-prefix rescan per candidate and no service before
the installation barrier.

### 4. Admission reservations and ordered metadata

The current slices add a thread-safe reservation ledger for logical WAL bytes,
source spool and recovery workspace, buffer memory, and seal-index capacity.
Each reservation owns all of its charges and releases them atomically. The
framed-batch helper derives DATA frame count from the codec, reserves one
worst-case FRONTIER per admitted batch, and returns the owned reservation before
the caller assigns a ticket or offset. The engine now reconstructs its segment
manager's retained obligations and a source-spool reservation estimate before
attaching a PITR runtime. Durable reconciliation keeps this manager in step with
completed seals and source cleanup without replacing live pins or reservations.
The estimate uses logical WAL lengths and a 4 KiB
sidecar allowance, so it does not yet cover larger seal indexes or actual
filesystem allocation and must not be treated as the final hard-bound check.
The separate reservation ledger is not yet reconstructed from filesystem
allocation or connected to live WAL admission; those integrations remain in
this stage.

- Promote or replace the `PitrSpoolAccountant` helper/model with a shared live
  ledger installed by the engine's PITR lifecycle. Reconstruct its charges
  from durable obligations and filesystem allocation before reopen permits
  admission. Wire the same ledger into WAL admission, preallocation, recovery
  installation, rotation, pin release, and durable cleanup; changing the model
  helper alone cannot enforce live limits. Keep recovery workspace charged
  when installer cleanup or its directory sync fails, and release it only
  after a later durable cleanup succeeds.
- Give the live ledger and its model owned reservations for
  framed DATA, worst-case one marker per admitted batch, seal-index capacity,
  buffer memory, and recovery-workspace growth. Maintain logical WAL usage
  separately from actual filesystem allocation, reservations, and pinned/
  temporary/orphan images. Include embedded markers in logical WAL usage.
- Reserve a shared maintenance workspace for the largest Active/Sealing image
  that must be copied. Grow its bound before admission or preallocation grows
  the possible image. Include generation zero, successor installation,
  terminal marker, seal/index temporary files, and manifest work in headroom.
  Bound rounded preallocation and all controls by the configured segment cap
  and the 1 GiB whole-image cap.
- Validate and prepare before atomically assigning a ticket, reserving DATA
  offsets, and enqueueing. A reservation/allocation/index failure consumes no
  ticket or offset. Transfer owned reservations with accepted work across
  cancellation and completion; audit accounting in both the segment manager
  and spool accountant to prevent duplicate charges or premature release.
- Return a distinct rotation-required preparation result when the current
  segment lacks DATA-plus-marker space or seal-index capacity but the batch
  fits a fresh legal segment. Define the ownership handoff for the engine's
  Wal-full retry path: release unadmitted preparation reservations and the
  active-memtable guard/lease before rotation, retire any reserved timestamp,
  and repeat required OCC validation on retry. Qualify the result and handoff
  in the serial driver here; connect full v7 lifecycle rotation/retry in stage 6.
  Distinguish this result from global spool backpressure and a batch that
  cannot fit any legal segment; oversized input must not cause endless rotation.
- Document the lock/reservation order across the sequencer, memtable leases,
  spool accounting, admission, and durability. Release admission/publication
  locks before waiting for archive progress or new budget; inject full-spool
  contention to check that maintenance can still make progress.
- Change the ordered packer to finalize the recorded-time clamp, canonical
  logical headers, and ticket-bound fragments in ticket order. Feed an
  incremental logical hash and tentative first-DATA-frame seal index. Retain
  bounded snapshots for group ends and fixed API cutoffs inside a group.
- Maintain a separate incremental physical hash in allocation order, including
  header, DATA frames, and FRONTIER frames. A missing earlier extent delays
  hash advancement, without blocking submission of earlier eligible writes.
  Bound retained buffers/metadata and release a buffer only after both terminal
  I/O ownership and hash consumption permit it.
- Consume one marker reservation when coalescing covered batches; release
  unused reservations only after the covered protocol completes. Reserve the
  final marker in advance so full-spool freeze/close does not need new capacity.
  Reuse recovery workspace only after prior replacements and cleanup settle.

**Exit:** Ledger reconstruction, tiny-limit/concurrent-admission models, and
serial-driver tests have no holes, cap overruns, accounting leaks, or maintenance
deadlocks. The serial persistence model reserves a batch before framed DATA
allocation/write, retains uncertain-sync ownership, and restores charges from
the installed verified image. Successful publication retires transient DATA
buffer charges, and recovery charges each retained DATA/control frame once,
including coalesced groups; rejected admission leaves its coordinates and WAL
image untouched. Digest/index snapshots agree with independent codec
verification; no stopped-admission payload rescan is required. Exact-boundary,
marker-overhead,
and seal-index tests produce rotation-required before ticket assignment, distinct
from global spool pressure and permanent oversize. Real engine allocation checks
belong to stage 5, and full v7 lifecycle rotation/retry checks belong to stage 6;
neither is a prerequisite for this stage.

### 5. Parallel runtime, one-sync coordinator, and API barriers

- Derive a checked `WalV7RuntimeSeed` from the installed Active image and the
  format descriptor. Validate its final recovery marker, logical boundary,
  ticket count, append offset, and DATA start. Seed the shared runtime's next
  admission/packer ticket and written/durable frontiers from it. Keep this v7
  runtime closed, with no extent initialization or prewarmed direct buffers;
  runtime admission opens only after later slices install the live reservations
  and v7 write protocol. The existing v4 startup keeps its current behavior.
- Initialize the runtime from the descriptor and recovered state, including
  v7's DATA start and continued segment ticket numbering. Retain the existing
  32 group slots and 256 ring entries unless a separately measured change is
  justified. Apply protocol state tests to the actual runtime as well as its
  pure admission/completion models.
- Split recovery decoding/materialization from writable-runtime startup.
  The current `Wal::new_recovered` starts Parallel before the engine classifies
  recovered memtables, and its extent initializer can immediately allocate and
  zero-fill. Materialize Sealing/Sealed segments without an append pipeline or
  initializer; their controlled recovery installation still follows stage 3.
  Start v7 runtime only for the authoritative Active image after installation,
  live ledger/limits, installed hash/ticket state, and MVCC/publication state
  are ready. Keep admission closed until startup completes, and reserve every
  startup extent before requesting allocation or zero filling.
- Serialize DATA and FRONTIER reservations through one physical allocator.
  Reserve each FRONTIER at the current allocation tail and never move existing
  DATA. Permit a ticket group to contain DATA ranges separated by a marker;
  split its write buffers/SQEs at the gap. Remove the live packer's unconditional
  ticket-and-offset contiguity assumption only where a verified control
  allocation accounts for the gap.
- After bounded optional coalescing, choose an eligible exclusive marker target
  `T` no greater than the contiguous written frontier and prove its exact `E(T)`
  from metadata. Capture the final `T` before constructing its canonical marker,
  verify full marker completion,
  then issue one `fdatasync(WAL)` on the same inode. Publish the captured durable
  ticket and seal metadata only after success. Later writes may overlap sync
  but cannot enlarge that captured target.
- Allow at most one marker/sync cycle. Coalescing has finite deadline and
  batch/byte bounds and makes progress with one writer. Optional gathering may
  observe newer work before marker capture. Separately capture a fixed API
  cutoff `C` for sync, freeze, checkpoint, synchronous/asynchronous recovery
  points, and close; continuous later admission cannot change `C`.
- Cover the stopped-admission `D0 | D1 | F_A(D0)` case: the next frontier may
  cover completed D1 and retained F_A via `E(T)`. If the physical prefix contains
  tickets at or above a proposed `T`, wait for necessary already admitted work
  and recapture an eligible marker target before construction. Merely waiting
  cannot make the same smaller target eligible. Predecessor containment may
  require `T > C`; covering those tickets leaves the caller's `C` unchanged.
  Never rely on future admission, or change `T` during marker write/sync.
  Poison must terminate otherwise impossible waits.
- On DATA failure, finish a healthy earlier prefix only if it remains marker
  eligible below poison. On a failed/short/uncertain marker write or failed sync,
  close admission and prohibit every subsequent FRONTIER append in that runtime.
  Fail/wake unresolved waiters and settle CQE/hash ownership before teardown.
  Exclusive recovery alone may reconcile the uncertain image.
- Implement explicit v7 Leader scheduling through the same codec, allocator,
  frontier, sync, and recovery contracts. It serializes scheduling while keeping
  the same on-disk protocol; choosing Leader cannot downgrade a file's format.
- Update the PITR branches in `mvcc.rs` and transaction delegation to route
  point, TTL, delete, batch, range, and OCC writes by the actual descriptor
  instead of `uses_wal_v5()`. Carry sampled recorded time in prepared v7 work
  for final clamping by the ordered packer; retain `next_pitr_recorded_at`
  behavior for v5/v6. Preserve timestamp/ticket ordering and retire sequencer
  timestamp reservations on pre-admission rejection without creating WAL holes.
- Wire synchronous and async durability waits through `MemTable` and the engine
  sequencer. Extend native async point preparation where v7 eligibility permits;
  keep range/OCC paths using their owned bounded blocking protocol until a native
  path is separately qualified. Every path uses the same v7 durability contract.
  Preserve memtable insertion followed by ordered MVCC publication, leases,
  preparation permits, cancellation ownership, and close/freeze draining.

**Exit:** Out-of-order/multi-SQE groups, captured-target races, stopped-admission
drain, poison, lost-wakeup, and cancellation tests pass with the actual worker.
No write reports success before durability and MVCC publication. Instrumented
steady-state v7 advances perform one WAL sync and create no side journal.
Public engine API tests prove all supported write shapes emit v7 DATA and
wait for its durability/publication barrier, including timestamp retirement.
An ext-family reopen test proves frozen WAL bytes and length stay unchanged;
an Active startup test proves preallocation is charged before it occurs.
Development v7 engine writes and forced preallocation prove marker/workspace
reservations enforce the hard spool bound before ticket assignment or growth.
These checks use the runtime/write path without requiring the sealing/successor
integration scheduled for stage 6.

### 6. PITR sealing, archive, restore, backup, and repair

- Make freeze stop admission, drain blocking/async leases and publication,
  and require a terminal frontier covering all admitted tickets before durable
  Sealing intent. Persist the full immutable boundary, then install one durable
  successor. Finalize hashes/indexes from saved metadata; omit unused
  preallocation from `sealed_end`.
- Connect stage 4's rotation-required outcome to the complete v7 lifecycle:
  release unadmitted preparation and memtable ownership, rotate without holding
  admission locks, then retry against the durable successor. Retire any reserved
  timestamp and repeat required OCC validation. Exercise exact-boundary,
  marker-overhead, and seal-index exhaustion through real engine APIs, including
  concurrent/cancelled retries and distinct global-backpressure/oversize outcomes.
- Extend `PitrManifestRecord::SealStarted`, its obligation state, replay, and
  snapshot validation with the versioned immutable boundary. Persist Active
  references using only logical anchors. Allocation-only timestamp watermarks
  must not be interpreted as durable batch claims.
- Make the seal codec and verifier format-aware. Retain seal sidecar version 1
  only if its existing fields can represent v7 explicitly and readers are
  updated consistently. An empty v7 segment is 8192 bytes with zero index entries;
  a nonempty index names each batch's first DATA frame. Its count equals the
  terminal ticket end, and its physical digest covers `[0, sealed_end)`.
- Update segment metadata, archive preparation/completion, catalog verification,
  and `decode_restore_wal_batches_for_segment` to use explicit descriptors.
  Archive only the exact frozen WAL image and seal; verify marker chain,
  logical/physical digests, index, identity, predecessor, and timestamps.
  Preserve each legacy predecessor's actual digest rule in mixed chains.
- Apply strict Sealing recovery against durable intent before publishing a seal,
  successor, or archive object. An unreferenced/conflicting seal cannot change
  authority or permit fallback. Recovery publication and restore staging must
  preserve existing atomic publication and reconciliation rules.
- Prefer freeze/seal for checkpoint and incremental-backup capture. Any helper
  copying Active bytes must pin a consistent image and selected certificate,
  copy the required covered DATA plus marker through its end, and verify it.
  Copying only through `durable_end` loses the certificate. Keep transient
  physical pins separate from persisted logical Active anchors, and charge
  replaced pinned inodes until release.
- Route normal open, repairing open, verified backup materialization, and
  recovered capture paths through the same boundary rules. Repair cannot
  bypass floors, suppress read errors, or normalize frozen objects.

**Exit:** Freeze/reopen/archive/restore/checkpoint round trips preserve the same
committed data and recovery coordinates. Active anchors survive relocation;
immutable references reject any physical-image mismatch. Empty and mixed-format
chains validate, with no stopped-admission payload rescan. Valid batches that
fit a fresh segment rotate and retry without ticket holes or cap overruns;
permanently oversized batches do not loop through empty successors. Complete
stage 3's continuation cases through real sealing, archive, and restore here.

### 7. Capacity preflight, migration, and constructor coverage

- Add a shared migration/preflight decision used by enablement, reopen, and
  rotation. Calculate capacity for the largest configured v7 physical image
  plus fresh recovery workspace, current obligations/pins/orphans, and all
  transition headroom. Current file length or an empty successor is insufficient
  evidence. Use checked arithmetic and report required/available capacity.
- Make preflight atomically acquire owned transition reservations in the live
  ledger before publishing durable migration Sealing intent. Hold them through
  successor installation, manifest publication, and required durable cleanup;
  a capacity snapshot cannot guarantee space against concurrent allocations.
  Reconstruct outstanding transition charges from replayed obligations before
  allowing admission after a crash. Reservation failure before intent follows
  the capacity outcomes below; durable intent requires transition recovery.
- For eligible existing epochs, recover the old segment under its original
  rules, freeze/seal it durably, install the v7 header plus generation zero,
  sync file/directory, and publish the successor in durable manifest state
  before accepting v7 writes. Keep the epoch and actual predecessor digests.
  A format-only migration must not manufacture a new epoch or rewrite history.
- Reconcile crashes at intent, seal, file install, directory sync, and manifest
  publication. Retain exactly one active segment and prohibit append to a
  predecessor after durable Sealing intent. Orphan cleanup remains accounted.
- Implement the following capacity outcomes explicitly:

| Situation | Required result |
| --- | --- |
| Healthy persisted v5/v6 database cannot support v7 | Reopen and PITR writes succeed; retain Leader and normal v6 successor rotations. Report migration deferral and capacity details. |
| New PITR enablement cannot support v7 | Reject before durable enable intent; do not silently create v6. |
| Already installed Active v7 lacks required workspace | Fail reopen; do not downgrade to legacy or reduce its recovery guarantees. |
| Capacity must be increased for a legacy database | Follow clean disable/re-enable with new epoch/base under RFC 023. Resume cannot silently override persisted limits. |

- Cover the persisted 1 GiB segment / 1.3 GiB spool example, temporary shortfalls
  from pinned/orphan obligations, and preflight against the maximum allowed
  image. Do not raise limits, reduce segment size, or disable legacy writes as
  an automatic migration side effect.
- Extend status with active format/mode, migration deferral reason and required/
  available capacity, DATA/marker usage, and reserved/allocated workspace.
  Update API serializers/renderers consistently and keep status read-only.
- Audit ordinary/PITR constructors, `open`, `open_async`, repairing open,
  enable/resume, background rotation, recovered pending successors, and
  checkpoint/restore opens. Honor explicit Leader only for formats supporting
  it. Unknown versions and backend initialization errors remain explicit errors.

**Exit:** Every entry point produces the same format/mode decision; interrupted
migration has a deterministic authoritative segment. Capacity deferral remains
writable and observable, eligible migration is crash-safe, and installed v7
never takes the legacy deferral path. Concurrent attempts to consume capacity
between preflight and intent cannot steal the transition's reserved space.

### 8. Complete qualification, defaults, and adoption record

- Complete the correctness matrix below using the persistence model, real
  runtime injection, and process-level integration. Regress ordinary v4,
  strict v5/v6, TTL, recorded-time rollback/clamping, range deletion, OCC,
  checkpoints, async cancellation, and close together.
- Extend `write-perf` with actual format/mode output and an explicit isolated
  v6 creation control. Remove its unconditional PITR-to-Leader comparison
  assumption for v7; do not reinterpret on-disk version fields. Record the
  required comparisons, devices, coalescing controls, and recovery metrics.
- After correctness qualification, activate v7/Parallel in every eligible
  production creation and migration path. Retain documented legacy capacity
  deferral. A released v7 writer must already support Parallel; a development
  serial driver or Leader-only intermediate cannot become its default.
- Record performance results and any explicit maintainer decision accepting
  material regressions before activation. Historic RFC 024 results and tmpfs
  measurements alone cannot qualify v7.
- Update current-behavior documentation in `AGENTS.md`, root/crate READMEs,
  docs indexes, WAL/PITR adoption notes, format/restore guidance, benchmark
  documentation, and the plan's completion status. Explain Active fallback,
  recovery copy/RTO and capacity costs, legacy deferral, and old-binary rejection.
  Explicit Leader is a scheduler comparison, not an on-disk rollback; restoring
  a compatible backup is required for downgrade after v7 installation.

**Exit:** All correctness cases have reproducible evidence; required measurements
and adoption decisions are recorded. Default/constructor tests prove v7 Parallel
selection where eligible, and the documentation matches the shipped behavior.

## Correctness and crash qualification matrix

Use a durable-state oracle independent of the implementation under test:
successful sync covers its DATA/marker; client ACK also requires publication;
unknown client outcomes may replay; verified anchors are hard floors. Enumerate
unsynced persistence orders before each crash. Process termination tests alone
cannot simulate a stable C group with missing B or a marker surviving its DATA.
Derive the expected boundary from persisted certificates, independently verified
prefixes, and anchors, choosing the newest valid candidate. ACK history adds a
minimum required prefix; it is not the sole oracle. After successful WAL sync,
the captured prefix must survive even if no publication or ACK followed.

| Case family | Required oracle or assertion |
| --- | --- |
| Golden bytes and invalid codecs | Independently fixed byte order, signed encoding, offsets, padding, CRC ranges, and digest preimages; reject overflow and noncanonical frames. |
| Fragment boundaries and limits | Exact fragment count/index/length, complete noninterleaved batches, maximum admitted payloads, and rejection before ticket assignment. |
| Marker-shaped user values | DATA containing a complete marker-shaped value preserves its bytes; discovery recognizes only codec-owned aligned FRONTIER headers. |
| Creation crashes | Header/F0/file-sync/rename/directory-sync/manifest cuts; installed damage is fatal, uninstalled incomplete creation is an accounted orphan. |
| Reordered DATA persistence | A and C complete with B torn; only a validated, anchored-eligible prefix replays and every acknowledged batch survives the stated sync guarantees. |
| FRONTIER interrupted sync | Complete marker with missing DATA falls back for Active above floors; cached complete bytes still require fresh writes before service. |
| Successful sync before publication/ACK | Crash after WAL sync but before durable-ticket, memtable/MVCC publication, or ACK; recovery must include the captured prefix and choose the newest fully verified candidate. |
| Discovery damage and tail controls | Scan beyond broken DATA, zero/hole/preallocation and partial EOF; stray speculative control may be discarded, interior fork/orphan may not be skipped. |
| Chain invariants | Duplicate generation, sibling/orphan/skip-predecessor, wrong binding/digest, and physical containment fail even after fixture CRCs are recomputed. |
| Fixed targets and stopped admission | Continuous work cannot change API cutoff `C` for sync/close/freeze/checkpoint/recovery points; predecessor containment may require marker `T > C`, fixed before construction. `D0 \| D1 \| F_A(D0)` drains; ineligible poisoned targets terminate. |
| Real CQEs and ownership | Short/negative, reversed, stale, missing, and ambiguous completions cannot publish beyond a proven prefix or release kernel/hash-owned buffers. |
| Failed marker cycle | No further FRONTIER append in that runtime, including attempts that would not ACK; all waiters/leases reach terminal outcomes. |
| Durable floors | Validate every exact logical anchor; never fall below it, reuse required timestamps, or reinterpret allocation watermarks as committed coordinates. |
| Recovery installation crashes | Copy/write/sync/replace/directory-sync failures, unchanged-length rewrite, restart without reboot, and crash during repeated normalization; canonical old/new image remains verifiable, with no early service. |
| Recovery followed by continued writes | Normalize twice, append at restored ticket/offset extending the installed recovery marker, sync, crash/reopen, then seal/archive/restore. Empty/nonempty/fallback/trailing-control variants independently agree on digests, index, anchors, data, and accounting. |
| Immutable and archive objects | Exact Sealing image, terminal marker, whole-image digest and seal index; no fallback/normalization; mixed legacy digest rules and empty v7 seals remain valid. |
| Runtime startup after recovery | Frozen Sealing/Sealed files acquire no append pipeline or initializer and retain exact bytes/length; only installed authoritative Active state starts v7 runtime, with startup allocation reserved before mutation. |
| Async, MVCC, and capture | Public point/TTL/delete/batch/range/OCC APIs use v7 encoding and ordered time clamping; rejected timestamp reservations retire. Cancellation and freeze/close/checkpoint/recovery-point races retain owners and the correct visible prefix. |
| Capacity and pins | Real engine admission/preallocation uses the shared ledger; full spool still permits terminal marker/recovery; workspace grows before allocation; old/temp/pinned files stay charged until durable cleanup. |
| Segment/index exhaustion | Exact-boundary DATA-plus-marker and seal-index limits trigger rotation before ticket assignment for a batch that fits a fresh segment; retry preserves caps and ordering. Global spool pressure does not masquerade as segment-full, and oversized batches cannot loop through empty successors. |
| Legacy upgrade | Insufficient maximum-image capacity defers with writable v6 rotations; transition reservations precede intent and survive concurrent capacity consumption/replay; eligible transition installs one v7 active; new enable fails before intent; installed v7 cannot downgrade. |
| Compatibility and ordinary workloads | Legacy fixtures keep exact behavior; ordinary v4 read/write/mixed workloads retain semantics and no v7 frames or recovery-copy requirement. |

Place pure v7 codec/recovery/model tests with proposed `wal/v7/` modules and
golden files under `kv-engine/src/tests/fixtures/`. Extend live WAL tests in
`kv-engine/src/tests/wal.rs`, PITR tests alongside their existing modules,
checkpoint/async tests under `kv-engine/src/tests/`, and chaos integration
under `kv-engine/integration_tests/`. Every test arming a failpoint must use a
`failpoint_*` name so the repository's plain-Cargo sanitizer filtering remains
correct.

Run focused nextest coverage after each implementation slice. Before activation,
run `cargo make check`, the all-features/all-targets nextest suite, and the
repository's sanitizer and process-crash jobs. Record executed commands,
revisions, failing seeds, and fault points; a planned test is not passing evidence.

Required v7 activation cases must run on a supported backend, assert that the
selected fault hooks were reached, and propagate recovery/oracle failures.
Skipped cases and lossy smoke checks cannot count as qualification. Use the
strict backend probe and hook-reachability pattern from
`failpoint_parallel_wal_crash_boundaries` in
[`chaos_integration.rs`](../../kv-engine/integration_tests/chaos_integration.rs).
Record a case-to-test/fault-point evidence table with expected outcome and
observed coverage; every required case must have a non-skipped successful run.

## Performance and operational qualification

Record a clean preimplementation baseline and compare all five RFC legs:

| Comparison | What it measures |
| --- | --- |
| Baseline v4 Parallel vs candidate v4 Parallel, PITR off | Ordinary read/write/mixed-workload regressions from shared changes. |
| Candidate v7 Parallel, PITR on, vs candidate v4 Parallel, PITR off | Total PITR overhead relative to the ordinary default. |
| Candidate v7 Parallel vs candidate v7 Leader, PITR on | Parallel scheduling benefit with identical format and durability rules. |
| Candidate v7 Leader vs candidate v6 Leader, PITR on | Format/frontier-protocol cost with Leader scheduling held constant. |
| Candidate v7 Parallel vs baseline v6 Leader, PITR on | End-to-end effect of upgrading the current PITR implementation. |

The v7/v4 comparison includes recorded times, fragmentation, both hashes,
indexing, markers, and archive activity; it cannot isolate marker cost or prove
scheduler gains. Use the same candidate binary for within-candidate comparisons,
explicit modes, and a separate recorded baseline revision for baseline legs.

Run tmpfs and a named physical device with 1/4/8/16 writers, single puts and
batch64, small and large values, rotation, checkpoints, and archive backpressure.
Include reads, scans, and mixed read/write ratios with PITR off and on. Retain
archive-idle and competing-archive-I/O controls, zero-added-wait coalescing, and
bounded-wait candidates with their exact deadline/batch/byte limits.

Report throughput and p50/p99 together, especially single-writer device latency.
Add group occupancy/overlap, batches and groups per frontier, coalescing wait,
marker writes, sync counts/latencies, DATA/marker bytes, hash CPU, buffer/spool
peaks, each admission rejection reason, and rotation pauses. Distinguish software
SQEs from physical device queue depth. Report reserved marker/workspace bytes
separately from actual allocation.

Measure reopen independently: backward discovery bytes/time, verification and
fresh-copy bytes/time, replacement syncs, maximum-size Active image, and peak
temporary/orphan/pinned allocation. A nearly full Active WAL may require a
nearly full rewrite even after a process-only restart; make that RTO and space
cost visible in status and the adoption report.

Retain paired runs, confidence intervals, failed controls, exact commands,
revisions, kernel/filesystem/device details, and archive/coalescing settings.
One sync per advance does not promise v4/v6 latency parity. Material regressions
need a recorded maintainer adoption decision; performance does not silently
create a permanent v7 Leader default or a new format exception.

## Delivery checklist

- [ ] Stages 1–3: legacy compatibility, byte-exact codec, independent validation,
  and serial-model crash qualification.
- [ ] Stages 4–5: reservations, dual hashes/index snapshots, Parallel and Leader,
  fixed API barriers, and complete ownership/failure handling.
- [ ] Stages 6–7: strict PITR lifecycle/capture, versioned manifests/references,
  mixed restore, capacity preflight, writable deferral, and migration recovery.
- [ ] Stage 8: full correctness and sanitizer/process evidence, all five
  benchmark comparisons, operational recovery measurements, and adoption record.
- [ ] Activate eligible v7 Parallel constructors/successors and update all
  current-behavior documentation together.

[WAL documentation](README.md) · [All documentation](../README.md).
