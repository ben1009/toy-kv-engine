# RFC 025 Implementation Plan: Parallel WAL with Embedded Frontiers and One Sync

**RFC:** [Parallel WAL with Embedded Frontiers and One Sync](../../rfcs/025-parallel-wal-embedded-frontiers.md)

**Status:** In progress — Stage 2 codec and persistence-model implementation is
complete. Stage 3 now includes bounded backward FRONTIER discovery, single-pass
candidate-prefix verification, authority/anchor selection, the Active
fresh-inode normalization primitive, an authority-required coordinator, and
durable replay of logical Active v7 manifest boundaries. Old PITR snapshots
default to having no v7 Active boundary. Production Active v7 selection and
fresh-inode installation are now wired into WAL open using the persisted
manifest anchor, with recovery workspace charged and normalized image/index
limits checked before installation. An exclusive process lock covers manifest
authority replay and remains held while the v7 Active WAL is open. A manifest
v7 boundary requires a canonical v7 Active header before legacy WAL recovery
can run; replacing that file with a legacy image cannot bypass its durable
floor. At startup, the recovered Active image remains commit-fenced until PITR
resume validates repository identity and reconciles archive obligations.
Successful resume now opens admission for synchronous and asynchronous public
point writes through the v7 batch durability path. Async v7 Parallel point
commits use the bounded preparation permit to build and validate canonical PITR
entries, and reserve their scratch memory, before waiting for the MVCC write
sequencer. Ordered admission assigns the commit timestamp and clamps the sampled
recorded time before final encoding; async callers then await WAL durability and
ordered MVCC publication. Synchronous writes and v7 Leader retain serialized
preparation. Async v7 Parallel range-delete batches now use the same bounded
preparation and ordered durability/publication path. V7 batch limits are checked
before copying caller ranges, and the temporary owned range/PITR buffers are
charged before allocation. Cancellation ownership is covered through close and
recovery. Other range formats and OCC retain their existing blocking paths.
The `put_with_ttl_async` convenience API now uses the same native point-batch
path where available and preserves whole-second TTL rounding; Leader and legacy
formats continue through the existing blocking write path.
The ordinary memtable size target does not freeze the
Active v7 memtable. Synchronous public point writes now retry pre-ticket v7
segment-capacity rejections by sealing, archiving, and rotating through the
durable recovery-point path; Leader and Parallel modes share this bounded
retry, with regression coverage at an exact segment boundary. Native async v7
Parallel point writes now retain the exclusive preparation permit while their
blocking rotation runs, then retry against the installed successor. Native
async Parallel point writes also have a public API regression for a shared
seal-index reservation rejection on a nonempty Active WAL; the rejected write
consumes no ticket or successor, and a later write succeeds on the same Active
WAL after capacity returns and survives reopen. Native
async v7 Parallel range deletes now retry capacity rejections through the same
sealing and successor rotation while retaining the exclusive preparation permit.
A one-blocking-worker regression verifies queued point writers cannot lease the
source during rotation and confirms range deletion and reopen durability. Native
seal and successor-verification buffers have protected ledger headroom separate
from ordinary batch/range buffers. Synchronous and asynchronous transaction
commits now retry v7 capacity rejections through bounded sealing
and successor rotation. Serializable commits retain their snapshot and repeat
OCC validation after rotation; tests cover successful sync/async retries and a
competing conflicting write during rotation. Nonserializable v7 Parallel async
transactions acquire the native preparation permit before worker dispatch and
retain it through capacity rotation, then release it after admission. A regression
queues a native point writer behind such a transaction on one blocking worker
and verifies successor retry and reopen durability. A synchronized failpoint
test also makes two synchronous Parallel writers reject the same full Active
segment before either enters the lifecycle boundary lock; they share one
successor rotation and both writes survive reopen. A native point-write
regression aborts the caller while its owned capacity retry is paused, verifies
close drains it before closing MVCC admission, and checks reopen durability. The
retry retains its original lifecycle admission while it rotates the Active WAL.
Leader-mode async retries use the same close-drain allowance; a forced-capacity
regression verifies rotation, close, and reopen durability.
Background PITR maintenance now rotates Active v7 segments when they reach the
configured segment target or archive interval; regressions cover both triggers.
Maintenance retries also finish durable Sealing/Sealed transitions before
reconciling their archive obligations, without requiring manual resume.
Queued maintenance checks lifecycle under the PITR barrier and cannot replace
the Active WAL or reopen admission after close establishes its final boundary.
Already-flushed v7 sources remain schedulable while cleanup is outstanding,
including after unlink or directory-sync errors; archive reconciliation finishes
their durable cleanup and releases source-spool/index quota before resuming writes.
Canceled v7 range and asynchronous transaction callers retain their accepted
capacity retries through close, and the committed updates survive reopen.
V7 capacity failures now distinguish segment rotation from shared resource
pressure and batches too large for an empty segment. Sync and async point-write
regressions verify oversized batches fail before ticket assignment, do not rotate
to empty successors, and leave the Active WAL usable. Serializable transactions
rejected for shared capacity keep their snapshot and buffered writes for explicit
retry; sync/async regressions verify successful retry and renewed OCC validation
in both I/O modes. Source-spool rejection assigns no ticket. Sync and async paths
retry admission when shared-ledger capacity is released or in-flight source work
finishes. Failed attempts release their
memtable/commit guards before waiting.
Native async point/range writers also drop their preparation permits. Async
transactions await quota in an owned coordinator, retaining their transaction
claim, snapshot, engine owner, and any preparation permit across retries so a
subsequent rotation cannot race native preparation. Only a direct
batch-reservation rejection produces a deferred quota wait.
Rotation and seal failures return to the caller even when caused by spool
pressure, allowing PITR resume and explicit transaction retry when commit
admission has stopped. Under source-spool pressure,
writers flush one immutable memtable below the normal count threshold and retry
released quota before attempting another Active boundary. If only the populated
Active WAL can release capacity, they archive and flush it. Independent scratch
buffers do not block this reclamation, and batch completion wakes waiters even
when its source bytes remain charged. Writers report a capacity error when no
releaser exists or maintenance cannot make progress. PITR scheduling,
reclamation, and flush use one reserved
blocking-executor slot when the executor has more than one slot, so quota release
can run when ordinary blocking work waits on that same quota or a live
immutable-memtable pin. Point/range quota waiters exit when lifecycle close
begins. Admitted transactions can flush and reconcile already-authorized PITR
cleanup before retrying during close; they return a capacity error if that
cleanup cannot release quota. Reconciliation preserves stopped commit admission
after `close_pitr` publishes its final point. The bounded final close-time check
runs with the blocking cleanup task, so caller runtimes without a time driver
are supported.
Public close drains and syncs
the Active v7 WAL for recovery without emitting a final recovery point. It stops
and drains MVCC commit admission, persists the latest synced logical boundary in
the PITR manifest, and a concurrent resume cannot reopen admission after close
begins. Public `sync()` and `sync_async()` now share a fixed-cutoff barrier
that drains MVCC admission, preserves an existing pause, and persists the empty
floor plus latest synced logical boundary in the PITR manifest. Explicit
recovery-point creation now seals and rotates Active v7 WALs, durably records
the successor boundary, archives the sealed source, and advances it to
Reclaimable. Tests cover recovery-point rotation and reopen in both Leader and
Parallel modes. Resume now completes a durable `Sealing` obligation from its
Active source, including rebuilding a missing sidecar. `SegmentSealed` records
the exact empty successor boundary before the successor WAL is installed; if a
crash lands between those steps, startup validates the sealed source and
recreates the authorized successor. Archived v7 sources release logical WAL
quota at the durable `Archived` transition while retaining physical spool
charges until cleanup. Queued async range preparations retain their scratch
charges across rotation without blocking archived logical-quota release.
Source reservation retirement waits for batch I/O; preparation guards own their
scratch charges independently and do not retain reclaimed spool or index quota.
Async range scratch reservation pins the sampled Active memtable until its token
owns the charge, so concurrent rotation cannot retire that owner between lookup
and reservation. The pin is released before waiting for native preparation.
Archive completion reclaims sources already flushed during publication, including
empty sources, and releases their retained spool and seal-index charges.
Resume also completes cleanup of already-flushed v7 sources after making their
Reclaimable transitions durable, releasing spool and seal-index quota before
reporting success.
Segment bookkeeping retains each installed successor's
reservation owner across later rotations and flushes; archive and reconciliation
transitions replay from the latest manifest projection under the shared mutation
lock, so concurrent reclamation cannot be resurrected. V7 forced flush now
establishes an archive boundary before flushing its source. Forced flush and
recovery-point requests reject unknown manifest publication before mutation,
including when the Active v7 WAL is empty. Uncertain archive publication requires
explicit PITR resume before another forced flush, regardless of successor writes.
Empty successors also reject unresolved archive obligations until PITR resume
settles the boundary. Clean and gap disable reject Active v7 WALs before
publishing new boundaries; ordinary post-PITR WAL rotation remains unsupported.
Recovery-point and forced-flush boundaries recheck the lifecycle under the PITR
barrier before mutation. Closed handles remain unable to modify a v7 database
after its exclusive open lock is released, including once another engine reopens
the directory. Synchronous flush and drain also retain lifecycle admission until
their work completes, matching the asynchronous APIs.
Explicit PITR close holds the operation and barrier locks through its final
recovery point and transition to Closing, then releases them before shutdown
drains admitted operations. Repeated PITR close calls reuse the cached final
archived point and storage-close result without another boundary. An ordinary
v7 storage close does not certify archive durability; a later `close_pitr`
requires reopening before it can publish the final archived boundary.
Standalone checkpoint capture now supports Active v7 WALs: it establishes a
fixed cutoff, seals and rotates a nonempty source WAL, flushes the captured
state, then pins the immutable file set before reopening admission. The
checkpoint manifest is detached from the source PITR repository because the
checkpoint omits that repository and source WAL. PITR base-backup capture also
supports Active v7: both the current and deprecated sync/async backup entrypoints
use the sealed-and-archived base boundary before pinning the manifest snapshot.
The deprecated synchronous entrypoint captures through its borrowed engine
owner. Async entrypoints require a retained owner when PITR is enabled and needs
boundary capture; other PITR modes retain the generic capture path, which still
rejects Active v7 WALs.
Deep verification coverage confirms that repeated captures retain a valid source
chain in both WAL I/O modes. The generic low-level backup capture still rejects
Active v7 so it cannot persist an Active source reference without its WAL and
repository. Remaining public write shapes (including other OCC APIs) and broader
lifecycle rotation remain. Manifest replay now accepts a legacy v5/v6 source seal
that installs an empty v7 Active successor boundary; the capacity-gated
installation path remains unwired. PITR manifest replay and
snapshots now retain and validate a v7 format marker and the full immutable
boundary in Sealing obligations;
seal completion checks the WAL digest against that boundary. A v7 v2 seal
sidecar codec now persists the immutable boundary and each ticket's first DATA
offset; building and verifying a seal runs strict Sealed recovery against the
exact WAL image. PITR catalog metadata now has a v7
seal-format-2 descriptor carrying the immutable boundary; v5/v6 seal-format-1
catalog records retain their original byte layout. Catalog encode/replay checks
the v7 boundary's identity, ticket/time coordinates, exact terminal position,
image size and ticket capacity, and required digests. V7 boundary allocations
count toward decoded-state limits without storing the full extension inline
in legacy records. Production source discovery now recognizes v7 v2 sidecars,
requires a matching manifest sealing obligation, rebuilds them against the
exact frozen WAL through the seekable verifier, and returns the validated
immutable catalog descriptor. A guarded v7 Active seal-emission primitive now
requires stopped commit admission, retains the state and active-memtable guards
in the established lock order, matches the persisted logical boundary to
the writer cursor, and prepares a strict v7 seal from the exact terminal-
frontier prefix without mutating the source WAL. Only after a matching durable
`SealStarted` obligation and certain manifest durability does installation
truncate and sync the source WAL to the immutable seal length before installing
the sidecar. Its spool reservation rounds up to the destination filesystem
allocation unit and remains held through reclamation. Seal preparation reserves
bounded decode/canonicalization workspace, recovery scan metadata, sidecar
memory, and sidecar source-spool bytes before building. The Active resource
ledger's transient-memory cap is sized for the maximum preparation estimate
under its persisted segment limit. Public recovery-point creation now uses this
primitive to persist `SealStarted`, install the v7 sidecar and successor, and
publish the source through `SegmentSealed`, archive, and `Reclaimable`.
Restart recovery now strictly verifies a durable v7 Sealing source against its
immutable boundary and any existing canonical seal sidecar, copies the exact
frozen bytes to a fresh inode, syncs and atomically installs that image, and
keeps commit admission closed while that source is still the active engine
WAL. Restart recovery now verifies retained v7 sources against durable Sealed
and Archived obligations, including their required v7 format after memtable
flush. Reclaimable sources remain strictly verified and replayed while a
nonempty memtable awaits its durable flush; partial source cleanup is accepted
once replay is no longer needed. Ticket-ordered batches recover into read-only
immutable memtables. Reconciliation now completes a v7 Sealing transition after
its empty successor is installed: it verifies the successor identity and
predecessor anchor, then stores the successor's initial Active boundary inside
the same `SegmentSealed` manifest record. A recovered manifest prefix cannot
expose the sealed state without that boundary. Rotation publishes the record
before successor creation; resume finishes `Sealing`, and Active startup
recreates a missing successor only when it has no durable memtable registration
and after strict source and sidecar validation. A missing registered Active WAL
causes open to fail instead of synthesizing an empty image.
Seal preparation
now additionally reserves the successor's persistent image and atomic-install
staging extents before `SealStarted`; the installed sidecar transition retains
those charges until successor installation or cleanup. An ambiguous sidecar
install retains its spool reservation, and retries reuse the per-segment charge
marker. A pre-existing sidecar without a marker is charged into retained segment
resources before further preparation; reopen reconstructs that marker from an
allocated v7 seal sidecar. The guarded
successor-install primitive creates a fresh v7 header plus generation-zero
FRONTIER, consumes the pre-reserved extents, registers the successor in the
engine manifest, and swaps it in only after commit admission is stopped. The
sealed source's Parallel runtime shuts down before successor creation or
registration, so a shutdown failure cannot create duplicate Active registrations
on retry. Before
registration, a successor already authorized by `SegmentSealed` must match its
persisted Active boundary, including the incarnation of any existing image.
Before successor publication it reserves its own bounded verification workspace,
rebuilds the seal from the exact frozen WAL image, and compares the installed
sidecar in fixed-size chunks for byte-for-byte agreement.
Frozen v7 WALs remain archive sources after
their memtables flush and are deleted only through PITR reclamation.
Archive preparation now validates v7 seal descriptors
against catalog metadata and strictly verifies the exact frozen WAL image;
source-object digest checks use whole-image SHA-256 for v7 while retaining the
recorded v5/v6 digest rules. Repository base-interval derivation now decodes
v7 seal indexes and binds them to their catalog descriptors, while retaining
the v5/v6 sidecar path. Deep repository verification now validates v7 seal
sidecars against the exact frozen WAL images and reports structural corruption
with the segment locator and last verified commit in the page. Deep interval
derivation uses the verified segment prefix, including when a base is attached,
without reading failing or later seals. Exact restore now validates each v7 WAL
and seal pair against its immutable descriptor, then decodes ticket-ordered
batches before replay; v5/v6 continue to use their legacy decoder.
The live
v7 Parallel runtime now admits reserved DATA batches and writes embedded
FRONTIERs before the single WAL sync. Public recovery-point creation now
performs v7 seal/rotation/archive in both Leader and Parallel modes; fixed-cutoff
barriers for the remaining APIs and broader Active lifecycle integration remain.
Version 8 engine manifests and standalone PITR envelopes require explicit
obligation format markers. Version 9 engine manifests require the empty Active
boundary for every sealed v7 successor. Production recovery selects legacy
snapshot migration from the persisted engine version and publishes a v9
snapshot before new writes. A v8 snapshot that predates the embedded successor
boundary is accepted only after the actual Active v7 WAL header validates; its
empty boundary is reconstructed before service resumes. Ambiguous pre-v8
snapshots or event logs with outstanding obligations fail closed; reopen them
with a v7-compatible build, drain seal/archive/reclaim work, publish a compacted
canonical snapshot, then retry the upgrade. Snapshot replacement also removes
incompatible historical Sealing records from drained pre-v8 v7 event logs.
Stage 4 now includes the v7 resource-ledger primitive, framed-batch reservation
helper, and serial-model integration. The model reserves before allocating
framed DATA buffers or writing frames, retains the charge through uncertain
sync, and retires transient batch-buffer memory on successful publication. It retains
WAL/spool/index charges; recovery reconstructs actual retained DATA/FRONTIER
bytes and seal-index capacity, counting coalesced markers only once. Serial
commits reuse the buffer cap, and recovered batches require no outstanding
DATA buffers. Exact-cap recovery and continued append cover coalesced groups.
Limit rejection is checked to leave ticket/offset state and WAL bytes unchanged.
Installed Active recovery now computes its retained WAL and seal-index charge
from the normalized image, and the persistence model reserves that exact charge
before resuming writes. Engine manifest-authority WAL selection and live
admission are connected. Active recovery now reconstructs obligation logical
bytes, allocated source bytes grouped by segment, startup staging files,
unattributed recovery images, and its Active image/workspace on one ledger
before installing the normalized inode. The recovered reservation owner shares
that ledger with segment bookkeeping; source charges retire after durable
cleanup, obligation charges after durable manifest transitions, and startup
staging charges after unlink plus directory sync. Public Parallel write routing
and full Active lifecycle integration remain outstanding.
Prepared resource releases include deferred manifest obligations, so a successful
retry after a complete append before sync retires already-cleaned source charges.
Live segment lifecycle reconstruction is now wired before PITR runtime attach.
Reconciliation updates the recovered bookkeeping after successful manifest
sync, releases reclaimed source charges, and consumes completed sealing's
pending successor reservation while preserving live pins and reservations.
Flush-driven reclamation also releases recovered source charges after its
manifest record is durable, including opens with no segment manager attached.
Delayed image-owner retirement remains scoped to the same archive epoch.
Recovery-point publication serializes its manager transition with the
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
Active v7 recovery now budgets the Active source and other durable, orphaned,
and interrupted-install PITR WAL/seal files, including orphaned recovery images
for other WALs, by their allocated filesystem blocks before reserving the
fresh-inode workspace. Same-WAL stale recovery images reuse that workspace:
the installer durably removes them before allocating the replacement.
Manifest logical lengths still determine the unarchived-WAL charge. The general
segment manager remains a provisional estimate: it does not account for
worst-case per-batch embedded
FRONTIER reservations, larger seal indexes, or ongoing pinned allocations. The
recovered Active v7 owner now restores its source, obligation, staging, and
recovery-workspace charges on one shared ledger before admission opens.
Successful PITR resume now reopens admission for synchronous and asynchronous
public point writes through the recovered v7 batch writer. Public Parallel
barriers and full Active lifecycle integration remain.
The low-level serial Leader writer now requires a reservation owner for its
installed inode. Creating the owner charges the recovered prefix exactly once. The writer
validates that the owner belongs to its installed image, precharges the
temporary canonical point-operation index before allocating it, then reserves
DATA/FRONTIER/index and encoded/frame-buffer
charges before materializing the logical batch or advancing the allocator. It
retains persistent charges through errors and unwinds across writer-handle
replacement until the lifecycle releases the owner after cleanup. Fresh-inode
recovery creates a replacement owner from the installed prefix. An installed
Active image can now be attached to a recovered `MemTable`: its point and range
operations replay, ticket and recorded-time state resume, and ordinary MVCC
point writes use the v7 batch adapter. The retained seal index advances in
ticket order with each resumed append, and the image-bound reservation owner
remains reachable for segment lifecycle retention. MVCC poisons its publication
sequencer when a v7 append fails after admission, including write/sync or
post-sync cursor-publication failures; pre-admission rejections remain retryable.
An unwind guard also poisons an outstanding v7 commit reservation on panic,
so a caller that catches the panic cannot leave commit capture waiting forever.
Engine startup now keeps recovered Active v7 admission closed until PITR resume
validates repository identity and reconciles obligations. Successful resume
enables synchronous and asynchronous public point writes through the v7 batch
writer. Public close drains and syncs the retained Active WAL, leaving PITR
enabled for recovery on reopen. Guarded v7 seal preparation and installation
are connected to explicit recovery-point sealing and successor
rotation/publication. Recovery covers crashes after `SealStarted` and after
`SegmentSealed` but before successor installation. Logical quota release is
coupled to durable archive publication; source-spool reservations remain until
durable local cleanup.
Stage 8 also injects a returned manifest sync failure specifically after the
`SegmentSealed` append, verifies write admission stays closed and the successor
is not installed, then reopens and reconciles the appended transition.
Recovered and rotated Active v7 owners and their shared ledger are retained by
PITR segment bookkeeping through durable WAL/seal cleanup; retirement fences new
admission and retains charges for in-flight admission or preparation guards
until they finish.
The serial Leader
classifies DATA-plus-FRONTIER segment exhaustion before the persistent batch
reservation or coordinate assignment. It may first reserve temporary canonicalization
scratch, which is released when preparation or capacity admission fails. Batches that
fit a fresh segment return `RotationRequired`, while batches that cannot fit any fresh
legal segment return `BatchTooLarge`; shared-ledger failures retain their distinct
reservation errors. Point-write retry and rotation are now wired in Stage 6, with
public API coverage for physical segment rotation and shared Parallel seal-index
pressure without rotation. Native async range retries are now wired through rotation
and covered for queued-writer exclusion and reopen durability. Native transaction
retries are also wired in Stage 6. Source-spool pressure waits for quota release;
other shared-ledger categories still return without rotation. Stage 5 has
started with recovered-runtime cursor bootstrap: an installed Active image
supplies the v7 DATA start, append offset, and next ticket. The shared runtime
seeds its admission, packer, and durability coordinates from that state. The seed now
preserves the verified terminal FRONTIER body, frame offset, frame digest,
logical/physical hash states, and recorded-time clamp. Seed validation checks
the logical digest against the recovered frontier, verifies the physical hash
cursor reaches the normalized image end, and validates the recorded-time and
seal-index cursors against recovered batches. The allocator accepts a synced
successor only when its generation, ticket and timestamp ordering, physical
bounds, predecessor offset, and predecessor digest continue that cursor. V7
startup performs no extent initialization or buffer-pool warmup. Admission
opens only when the validated image-bound reservation owner is attached;
ownerless diagnostic runtimes remain closed. The potentially large seal index remains owned by the recovered WAL
lifecycle rather than being copied into the runtime seed.
Recovery hash-continuation, malformed-metadata rejection, and paused-runtime
retention tests cover this handoff for both empty and nonempty installed images.
The production logical-batch encoder now emits the v7 ticketed header and
canonical RFC 023 entry stream with both v7 CRCs. It validates borrowed
canonical survivors before allocating encoded bytes and does not copy their
keys or values during preparation. A production frame materializer now emits
the complete aligned DATA-frame sequence into caller-owned storage after
validating the logical header and assigned ticket. Live Parallel admission now
uses this materializer and accounts for each batch's persistent DATA, marker,
index, and transient-buffer reservations before assigning its ticket.
A checked physical allocator now assigns DATA tickets and ranges and reserves
FRONTIER slots at the allocation tail. It splits a ticket group's DATA ranges
across every reserved FRONTIER frame and poisons further allocation after an
uncertain marker cycle. Each uncovered ticket retains one marker frame's
capacity; placement consumes one credit and successful sync releases only the
unused credits of its captured prefix. The parallel worker now accepts an
explicit physical allocation span tiled by DATA writes and reserved FRONTIER
offsets, advances its ordering cursor across those controls, and reports DATA
bytes separately from physical span length. The recovered v7 runtime now owns a
physical allocator, and its ordered packer can consume allocator-issued DATA
tokens and build worker spans that account for marker gaps, including a marker
at the beginning of a later group. Live admission now assigns and queues v7
batches against those allocations. The coordinator appends a canonical
FRONTIER, performs one WAL `fdatasync`, advances the allocator cursor only after
success, and poisons the runtime after an uncertain marker cycle. Active
recovery transfers its verified terminal FRONTIER cursor into the runtime.
The coordinator defers an ineligible written prefix without consuming a marker
slot or sync, then recaptures a larger target after already admitted DATA
completes. DATA poison that prevents an eligible target fails every remaining
unacknowledged waiter. V7 DATA buffers use their exact reserved frame capacity
and retire without entering the v4 buffer pool. Deterministic runtime tests
cover recapture, a healthy prefix below later DATA poison, impossible recapture
after DATA failure, exact buffer caps, and live append/recovery with a full
ledger at small segment limits.
Close drains the worker and coordinator before retiring their failed/discarded
batch operation guards. Each v7 DATA allocation pins its batch reservation:
terminal buffer retirement releases transient charges while the image keeps
persistent reservations, but an allocation retained for ambiguous submission
keeps its charge and owner alive across close, WAL-handle drop, and image
retirement. Failure tests retain the closed WAL handle and verify terminal
retirement and repeated close after DATA and sync errors. A nonterminal buffer
test verifies that the memory cap remains enforced until the allocation is
actually released.
Tests cover resuming allocation from an installed nonzero cursor and poisoning
further allocations when a post-sync cursor has a mismatched predecessor digest
or unaligned durable boundary.
V7 runtime startup now checks that the worker, sync, and preallocator
descriptors all refer to the recovered Active device/inode, that the installed
append cursor fits the current file length, and that the physical length stays
within the segment cap before allocating buffers, initializing extents, or
spawning workers. A mismatched descriptor or invalid file length is rejected
without changing the WAL; admission opens only when the validated image-bound
reservation owner is supplied.
Recovered Active v7 attachment now selects the requested I/O mode: Leader uses
the serial v7 writer, while Parallel starts from the installed runtime seed
without extent initialization or buffer prewarming, using the retained
image-bound reservation owner.
The paused runtime now carries persisted `max_segment_bytes` into its physical
allocator, file-length validation, and preallocation bounds, rather than
inheriting the larger global WAL cap. Tests cover capacity classification,
rejection of installed images larger than their configured segment, and actual
packer preallocation under small and unaligned segment limits.
The allocator now derives the exact captured physical prefix `E(T)` from
pending per-ticket DATA ends and the previous FRONTIER end. Each marker
reservation stores that value, and successful sync retires only DATA-end cursors
covered by its ticket target. The stopped-admission `D0 | D1 | F_A(D0)` case
proves the next target includes `F_A` even though D1 ends before it.
Targets whose retained predecessor also includes excluded DATA are rejected
before reserving a marker. Tests cover recapturing a larger eligible target
without requiring further admission, while leaving post-marker DATA speculative.
Post-sync cursor tests also reject aligned cutoffs that omit covered fragments
or include excluded DATA, preserving the prior frontier and poisoning admission.
The serial persistence model covers repeated normalization, continued append,
and crash/reopen for empty, nonempty, fallback, and trailing-control prefixes,
including a selected marker behind speculative DATA. At this point in the
implementation sequence, the production writer remained v6. Stage 5 now also
has a low-level serial v7 Leader writer primitive:
it resumes from installed metadata, preflights DATA-plus-FRONTIER segment
capacity, writes one ticket's DATA and linked FRONTIER frames, performs one
WAL `fdatasync`, and advances hash/cursor state only after success. Any failure
after physical allocation poisons that writer. Recovery seeds carry the
installed device/inode identity, which the writer checks alongside the terminal
FRONTIER; preallocated tail bytes remain excluded by the seed append cursor
and bounded by the segment's physical cap. Append-mode descriptors are rejected
before admission because Linux positional writes on those handles ignore their
offsets. Focused Leader tests cover fragmented commit/recovery/resume, time
clamping and image hashing, preallocated tails, capacity rejection, failed-sync
poisoning, stale recovery descriptors, and invalid descriptor/physical caps.
The writer caps caller-supplied batch bounds to the live recovery limits before
admission, while retaining stricter bounds. Seed snapshots require a clean
ownership handoff without any subsequent admission; failed write/sync outcomes
require a fresh recovery installation. A serial v7 Leader adapter now connects
that primitive to `Wal`'s PITR batch append API: it seeds assigned and durable
ticket cursors from the installed image, publishes each ticket only after the
DATA/FRONTIER sync, and routes submit/sync/close through serial barriers without
an io_uring ring. Whole-WAL sync/close wait for in-flight appends and inspect
writer poison, including after unwind; later failures preserve success for
earlier durable ticket barriers. Focused tests cover resumed ticket state,
recovery, pre-admission capacity rejection, concurrent sync/close, caught
panics, and durable-prefix preservation after sync failure. `MemTable` can now
attach an already installed Active image, replay its recovered entries, route
ordinary MVCC point writes through the v7 batch adapter, and extend its seal
index in ticket order while retaining the image-bound reservation owner.
MemTable tests cover replay and reopen, resumed MVCC tickets and seal indexes,
retryable validation/capacity rejection, and returned errors or caught panics
during v7 sync across all seven MVCC write paths. Uncertain append failures
poison MVCC before returning or unwinding.
Successful PITR resume now enables synchronous and asynchronous public point
writes through the recovered v7 batch writer. Public Parallel barriers, the
v6-to-v7 migration, recovery-point rotation, and checkpoint/backup boundaries
now use the v7 lifecycle path. Active v7 reservation owners and their shared
ledger are retained through durable source cleanup and then retired from segment
bookkeeping. Constructor defaults and full crash/performance qualification remain.

**Last updated:** 2026-10-22

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
Fresh PITR segments still begin as v6. Capacity-eligible nonempty legacy Active
segments migrate to v7 on resume or maintenance rotation; final default
selection and qualification remain later work.

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
| [`manifest.rs`](../../kv-engine/src/manifest.rs), [`pitr/manifest.rs`](../../kv-engine/src/pitr/manifest.rs) | Active logical anchors and v7 immutable Sealing boundaries are versioned, serialized, and validated in manifest replay/snapshots; recovery now resumes interrupted Sealing and successor installation. |
| [`mem_table.rs`](../../kv-engine/src/mem_table.rs), [`lsm_storage.rs`](../../kv-engine/src/lsm_storage.rs), [`lsm_storage/write_gate.rs`](../../kv-engine/src/lsm_storage/write_gate.rs) | Carry the descriptor and owned reservations through writes, freeze, successors, reopen, and async ownership. |
| [`mvcc.rs`](../../kv-engine/src/mvcc.rs), [`mvcc/txn.rs`](../../kv-engine/src/mvcc/txn.rs) | Replace v5-only PITR write routing with descriptor-aware preparation; hand sampled v7 recorded time to the ordered packer and preserve legacy clamping, OCC, and timestamp retirement. |
| [`pitr/seal.rs`](../../kv-engine/src/pitr/seal.rs), [`pitr/archive.rs`](../../kv-engine/src/pitr/archive.rs), [`pitr/catalog.rs`](../../kv-engine/src/pitr/catalog.rs), [`pitr/restore.rs`](../../kv-engine/src/pitr/restore.rs) | Add strict v7 physical-image verification, DATA offsets in seal indexes, explicit format metadata, and mixed-chain restore. |
| [`checkpoint.rs`](../../kv-engine/src/checkpoint.rs), [`backup.rs`](../../kv-engine/src/backup.rs), [`pitr/base.rs`](../../kv-engine/src/pitr/base.rs) | Capture verified logical or immutable boundaries and retain the required certificate and file pins. |
| [`pitr/api.rs`](../../kv-engine/src/pitr/api.rs), [`bin/write-perf.rs`](../../kv-engine/src/bin/write-perf.rs) | Expose migration deferral and separate DATA/marker/workspace accounting; report actual format/mode and qualification metrics. |

## Stage dependencies

Stages 1–6 are implemented for the production v7 Active path. Stage 7 migration
and constructor coverage remain in progress; Stage 8 completes crash/performance
qualification and default adoption. Each stage adds its own regression coverage,
and stage 8 assembles the complete qualification evidence.

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
coordinator does not infer authority from the WAL. LSM WAL open now builds
Active authority only from the persisted logical boundary and predecessor,
holds an exclusive process lock from before manifest replay, reserves the
source and a full-segment fresh-inode workspace within the remaining spool
allowance, checks normalized image and seal-index bounds before replacement,
and attaches recovered batches and the inode-bound owner to the active
memtable. The lock remains held until close or drop. Engine startup keeps
admission closed until PITR resume validates repository identity and
reconciles obligations; successful resume enables sync/async public point
writes through the v7 batch writer. Capacity-driven rotation and freeze
remain later lifecycle work. Immutable images are not normalized. PITR manifest state
represents the current Active v7 logical boundary and validates monotonic
updates against its segment identity; the boundary intentionally omits
physical FRONTIER identity.

- Persist `ActiveBoundaryAdvanced` as a PITR manifest record. Require the first
  boundary for a segment to be its empty header prefix, then accept only exact
  duplicate records or strict ticket/end/timestamp advances for the same
  segment incarnation. Require Sealing records and snapshots to retain space
  through that boundary and a terminal FRONTIER; a sealed commit anchor must
  cover any nonempty Active timestamp floor before the boundary is cleared.
  Nonempty Active timestamps must advance the preceding sealed high-water, and
  clean disable cannot discard a nonempty Active boundary before sealing.
  Reconciliation snapshots require a persisted gap whose start covers any
  known nonempty Active commit floor before that floor can be discarded.
  An obligation naming that Active segment may be Sealing, or Abandoned during
  explicit gap reconciliation; immutable/reclaimable obligations cannot name it.
  Production gap emission must derive the exact first uncovered timestamp from
  verified WAL/seal data; the manifest timestamp check supplies only a bound.
  Preserve old snapshots by defaulting the new boundary field to absent, and
  clear it when a sealed successor becomes active. WAL open consumes the
  boundary, and explicit Active v7 `sync()`/`sync_async()` now persist the
  empty floor and latest synced logical boundary. A confirmed-absent append
  may resume previously open admission only while the manifest remains healthy;
  unresolved boundary durability blocks retries until reopen. Automatic
  publication from other lifecycle transitions remains later integration work.

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
  database WAL-open wiring is implemented; durable manifest publication
  remains a later slice.
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
crash cases before Parallel is activated for engine writes. Recovery uses bounded discovery plus
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
The estimate uses logical WAL lengths and one 4 KiB control-frame allowance per
segment, so it does not yet cover worst-case per-batch embedded FRONTIER
reservations, larger seal indexes, or actual filesystem allocation and must not
be treated as the final hard-bound check.
The separate reservation ledger now gates the low-level serial v7 Leader
writer's batch admission and retains persistent charges in an inode-bound
owner. Active v7 recovery reconstructs filesystem allocation for existing
obligation, orphan, and install-staging WAL/seal files and other-WAL recovery
images, and charges the Active source plus recovery workspace before
normalization. Same-WAL stale recovery images are durably reclaimed before
workspace reuse. The recovered Active owner and PITR segment manager share
the reconstructed ledger, and durable manifest/source transitions release
their associated charges. Public Parallel write routing and the v6 to v7
transition remain.

- Promote or replace the `PitrSpoolAccountant` helper/model with a shared live
  ledger installed by the engine's PITR lifecycle. Extend the current Active
  recovery preflight, which accounts allocated blocks for obligation, orphan,
  install-staging WAL/seal files, and other-WAL recovery images, to owned
  reservations for durable obligations and pinned images before reopen permits
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
  segment lacks DATA-plus-marker space but the batch
  fits a fresh legal segment. Define the ownership handoff for the engine's
  Wal-full retry path: release unadmitted preparation reservations and the
  active-memtable guard/lease before rotation, retire any reserved timestamp,
  and repeat required OCC validation on retry. Qualify the result and handoff
  in the serial driver here; connect full v7 lifecycle rotation/retry in stage 6.
  Distinguish this result from shared-ledger pressure, including aggregate
  seal-index charges retained until durable source cleanup, and a batch that
  cannot fit any legal segment; oversized input must not cause endless rotation.
  The serial Leader currently classifies DATA-plus-marker segment exhaustion
  before the persistent batch reservation or coordinate assignment; temporary
  canonicalization scratch may be reserved first and is released on rejection.
  The engine retry/rotation handoff is now connected in Stage 6. Shared-ledger
  failures, including seal-index exhaustion, return capacity errors without
  rotation. Source-spool exhaustion waits after active-memtable/commit guards
  unwind, then retries without rotation. Native async point/range writers also
  release their preparation permit before awaiting. Async transactions retain
  their claim and any preparation permit while their owned coordinator awaits
  quota, so a subsequent rotation cannot race native preparation. Explicit
  attempt outcomes distinguish direct batch quota waits from rotation/seal
  failures; the latter return to the caller with the snapshot and buffered
  writes retained for PITR resume and transaction retry.
- Source-spool wait ordering is implemented across reservation, memtable
  admission, and durability: failed attempts release memtable/commit guards
  before waiting, and native async point/range writers also release their
  preparation permit. Ordinary async transaction quota waits release the blocking
  worker and reject concurrent mutations while preserving their commit claim.
  The bounded final check during close runs with cleanup and needs no Tokio time
  driver. A cancellation regression verifies that a detached transaction keeps its engine
  alive, finishes after quota release, and survives reopen. Full-spool
  regressions verify the active-memtable exclusive gate is
  available to maintenance while a writer waits for released quota. PITR
  scheduling, reclamation, and flush use reserved executor capacity, covered by
  an executor saturation regression. Sync and native async regressions verify
  source-spool pressure flushes a Reclaimable immutable segment below the
  configured memtable-count threshold and the waiting write completes without
  a manual quota release. Sync and native async regressions also verify that a
  blocked point/range source-spool writer exits when close begins. Admitted
  transactions instead finish authorized source cleanup during close or return
  a capacity error when cleanup cannot release quota.
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
verification; no stopped-admission payload rescan is required. Exact-boundary
and marker-overhead cases in the serial Leader produce rotation-required before
ticket assignment; a batch too large for a fresh segment and shared-ledger resource
pressure remain distinct. Stage 6 now exercises physical segment rotation and
shared Parallel seal-index reservation errors without rotation. Range and
transaction retries and cancellation are also covered in Stage 6; source-spool
release and retry is now covered, while logical, buffer, and seal-index caps
remain non-rotating errors. Full lifecycle rotation/retry checks belong to Stage 6; they are not
prerequisites for this stage.

### 5. Parallel runtime, one-sync coordinator, and API barriers

- Derive a checked `WalV7RuntimeSeed` from the installed Active image and the
  format descriptor. Validate its final recovery marker, logical boundary,
  ticket count, append offset, DATA start, and verified terminal FRONTIER
  cursor. Seed the shared runtime's next admission/packer ticket, written/durable
  frontiers, and physical allocator predecessor from it. Validate each later
  successfully synced FRONTIER as the immediate canonical successor, including
  generation and ticket/timestamp ordering, physical bounds, and predecessor
  offset/digest. Carry the verified immutable-header digest in the runtime
  seed, and reread/canonical-decode the installed worker inode's header before
  starting workers. Keep extent initialization and direct-buffer prewarming
  disabled for this recovered runtime. Open admission only when a validated,
  image-bound live reservation owner is attached; ownerless diagnostic runtimes
  stay closed. The existing v4 startup keeps its current behavior. V7 startup also receives persisted
  `max_segment_bytes` and applies it to physical allocation, file validation,
  and preallocation bounds.
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
  split its write buffers/SQEs at the gap. The worker now validates explicit
  physical spans tiled by DATA buffers and reserved FRONTIER offsets while
  retaining strict contiguous groups for legacy submissions. Remove the live packer's
  unconditional ticket-and-offset contiguity assumption only where a verified
  control allocation accounts for the gap. The checked allocator primitive,
  per-ticket marker headroom, accounted-gap range splitter, worker group
  validation, and allocator-backed packer-to-worker span translation are
  implemented. The allocator captures exact `E(T)` in the marker reservation
  and rejects targets whose predecessor retention includes excluded DATA.
  Tests cover predecessor containment and recapturing an eligible target.
  The live producer now reserves resources before buffer allocation and ticket
  assignment, materializes and queues ticketed DATA, and tracks logical/physical
  digest snapshots. The sync coordinator appends one canonical FRONTIER at the
  allocation tail before the WAL `fdatasync`, completes the allocator cursor
  only after sync, retires covered transient reservations, and poisons the
  runtime after marker or sync uncertainty. Public API routing, fixed-cutoff
  barriers, v7 Leader parity, and full Active lifecycle integration remain.
  Written targets are checked for eligibility before reserving or writing a
  marker: later completions recapture a larger target, and impossible recapture
  below a DATA poison boundary terminates unacknowledged waiters. Runtime tests
  exercise both paths using real DATA/FRONTIER bytes and verify the resulting
  Active recovery boundary. Exact-sized v7 DATA buffers bypass the v4 pool.
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
  eligible below poison. On a failed/short/uncertain marker write, failed sync,
  or failed post-sync cursor validation,
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
  sequencer. Native async v7 Parallel point preparation now runs under its
  bounded preparation permit before the MVCC write lock; the ordered sequencer
  supplies the commit timestamp, and the ordered WAL packer clamps the sampled
  recorded time. Native async v7 Parallel range-delete batches now follow the
  same bounded preparation, ordered durability, and publication protocol. They
  preflight encoded limits and reserve input/PITR temporary memory before
  copying. A cancellation test verifies that close drains an admitted range
  commit and recovery preserves its tombstones. Other range formats and OCC
  remain on their existing blocking paths pending qualification. Every path
  uses the same v7 durability contract.
  Preserve memtable insertion followed by ordered MVCC publication, leases,
  preparation permits, cancellation ownership, and close/freeze draining.

The public `KvEngine::sync()` and `sync_async()` APIs share a barrier that stops
and drains MVCC commit admission for an Active v7 WAL before syncing that
memtable. The asynchronous path carries its lifecycle admission into the blocking
worker. The barrier resumes commit admission only if the sequencer was open
beforehand and the WAL sync succeeds; a failed sync leaves admission closed for
recovery. Public close applies the same fixed-cutoff rule while keeping
admission closed: it syncs the Active WAL and persists its resulting logical
boundary before tearing down storage. Explicit recovery-point creation and v7
forced flush use the sealing and successor-rotation path. Standalone checkpoint
capture now uses the v7 sealing and successor-rotation path, flushes the fixed
cutoff, and pins its immutable file set before reopening admission. Its
manifest snapshot is detached from the source PITR repository because the
checkpoint omits that repository and source WAL. Incremental-backup capture
now routes Active v7 through PITR base capture, sealing and archiving the source
chain before the backup snapshot is pinned. The deprecated synchronous and
asynchronous backup APIs use the same path. The generic low-level capture helper
continues to reject Active v7 because it cannot independently preserve the source
chain. PITR base capture is qualified in both Leader and Parallel modes with
repeated-backup and deep archive-verification coverage. The deprecated sync API
uses its borrowed owner directly. Async entrypoints report an explicit error if
the owner has been moved out of its `Arc` while enabled PITR requires boundary
capture; other PITR modes retain generic capture, which rejects Active v7 WALs.
If checkpoint capture fails, commit admission resumes only when the sequencer,
manifest durability, active WAL, and PITR publication state remain healthy;
uncertain state stays fail-closed for recovery.

Regression tests exercise both public sync APIs with a commit reserved before
its WAL ticket exists and preserve an existing admission pause. A separate test
gives the shared helper a fully admitted lifecycle guard, begins close before
executing sync, and verifies that sync finishes without reopening writes. An
async cancellation test aborts the caller after its blocking worker stops
admission, then verifies that the worker retains its lifecycle guard until the
pending commit drains and only then resumes admission.

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
The production encoder now builds canonical v7 logical batches with their
assigned segment tickets and the shared RFC 023 entry stream, including the v7
data/header CRCs. Golden bytes and borrowed-preparation tests cover limit
rejection before output allocation and discarding superseded oversized values.
A separate production codec materializes those logical batches into complete
aligned DATA-frame sequences in caller-owned storage. It validates the logical
header and ticket before changing output. Fixed single-frame and multi-frame
golden vectors, fragmentation-boundary tests, and unchanged-output rejection
tests cover the materializer. It is connected to live Parallel admission and
FRONTIER scheduling; public engine barriers and sealing/successor integration
remain in later stage 5/6 work.

### 6. PITR sealing, archive, restore, backup, and repair

- Make freeze stop admission, drain blocking/async leases and publication,
  and require a terminal frontier covering all admitted tickets before durable
  Sealing intent. Persist the full immutable boundary, then install one durable
  successor. Finalize hashes/indexes from saved metadata; omit unused
  preallocation from `sealed_end`.
- Synchronous public point writes now connect the pre-ticket segment-capacity
  rejection to the durable v7 sealing/archive/successor path and retry against
  the successor in Leader and Parallel modes. Exact-segment-boundary regression
  coverage exercises `put`, point `write_batch`, and `delete`, including reopen.
  Native async v7 Parallel point retries now keep the sole preparation permit
  through the blocking rotation, leaving queued preparers without active
  memtable leases while the exclusive freeze runs. Native async v7 Parallel
  range retries now retain that permit through rotation as well. One-blocking-
  worker regressions queue point writers during point and range capacity retries,
  then verify successor writes and reopen durability. Synchronous and
  asynchronous transaction commits now route v7 capacity rejections through the
  same bounded rotation path. Serializable transactions repeat OCC validation
  after rotation while retaining their snapshot; regressions cover successful
  sync/async commits and a conflicting write during the retry window.
  Nonserializable v7 Parallel async transactions also acquire the native
  preparation permit before blocking dispatch and retain it across capacity
  rotation. Successful admission releases it before durability, preserving
  concurrent groups. A one-blocking-worker regression verifies that a queued
  native point writer cannot lease the source ahead of transaction rotation;
  both writes finish against the successor and survive reopen.
  A test-only ledger limit override now forces `SealIndexLimit` through the real
  Parallel reservation path for `put_async` on a nonempty Active segment; the
  write returns shared pressure without ticket assignment or rotation. After
  the limit is restored, a later write uses the same Active WAL and survives
  reopen. Serializable transactions also retain their snapshot and buffered
  writes after shared-capacity rejection. Sync/async tests in both I/O modes
  verify caller retries succeed when uncontended and repeat OCC validation
  when another writer changes a read key. A
  synchronized failpoint test forces two synchronous Parallel writers to
  reject the same full Active segment before rotation; stale-source retries
  share one successor and both writes survive reopen. A native point-write
  regression aborts the caller while the owned retry is paused, verifies close
  waits for it, and checks reopen durability. A Leader-mode async batch held at
  the active-memtable gate finishes its capacity retry after close starts and
  survives reopen. Background maintenance now archives and rotates Active v7
  segments at either the configured physical segment target or archive
  interval, including an idle reopened WAL, with regressions for both triggers.
  A transient failure after `SealStarted` or before successor installation is
  completed by subsequent maintenance; failure injection covers both boundaries
  in Leader and Parallel modes and verifies writes resume and survive reopen.
  A queued retry held until close finishes its final Active boundary preserves
  the stopped admission fence in both I/O modes; resume after reopen completes
  the pending rotation.
  Already-flushed sources get a second cleanup pass after archive reconciliation.
  Reclaimable sources without a live memtable remain maintenance work even with
  an empty successor; injected publication, unlink, and directory-sync failures
  verify automatic retry, quota release, and resumed writes in both I/O modes.
  Canceled native v7 range and async transaction callers retain their accepted
  capacity retries through close and recovery. V7 capacity failures distinguish
  segment-local rotation, shared resource pressure, and batches too large for a
  fresh segment. Oversized sync and async point writes are rejected before
  ticket assignment without rotating. Sync and native async writers now wait on
  source-spool capacity and work-completion generations; tests verify no ticket
  is assigned, the active-memtable gate stays available, and each write resumes on quota release
  without rotating when released capacity suffices. Additional regressions
  verify batch completion before waiter registration is observed, Active
  reclamation proceeds while independent scratch stays owned, and a pressure
  flush does not trigger an unnecessary Active rotation.
- Extend `PitrManifestRecord::SealStarted`, its obligation state, replay, and
  snapshot validation with the versioned immutable boundary. Persist Active
  references using only logical anchors. Allocation-only timestamp watermarks
  must not be interpreted as durable batch claims.
- The standalone v7 seal codec uses sidecar version 2 because v1 has no field
  for a 64-bit first-DATA-frame offset. It persists the full immutable boundary,
  explicit WAL version, physical identity, and one ticket/index/time entry per
  batch. Its builder and verifier require the exact terminal FRONTIER and a
  successful strict Sealed recovery check against `[0, sealed_end)`. The builder
  accepts a seekable source and exact terminal-marker end, streaming the digest,
  strict recovery verification, and ticket-index construction without staging
  the whole WAL in memory; unused preallocation is excluded. Active boundary
  capture obtains the physical terminal cursor from either writer mode and
  verifies it against the on-disk FRONTIER before persisting the logical
  boundary. The v5/v6 sidecar version 1 layout remains byte-compatible.
  Production v5/v6 seal publication, catalog descriptor readers, strict archive
  verification, and restore batch decoding are connected. Restore validates
  the immutable WAL/seal pair and applies its
  DATA decode limits before allocating logical batch buffers. An empty v7 segment is
  8192 bytes with zero index entries; a nonempty index names each batch's first
  DATA frame, and its count equals the terminal ticket end. Decoding bounds the
  entry count and total sidecar bytes by both the persisted segment limit and
  the global WAL file limit before allocating the decoded entries. The builder
  checks the global image cap before hashing. Seal-path regression coverage
  includes fragmented DATA followed by a retained frontier whose physical
  offset is after later covered DATA.
- Catalog metadata now supports the explicit v7 immutable descriptor while
  retaining the v5/v6 seal-format-1 layout. Update production segment discovery,
  archive preparation/completion, catalog object verification, and
  `decode_restore_segment_batches` to consume these descriptors.
  Production source discovery now recognizes v7 v2 sidecars, requires their
  matching manifest immutable boundary, checks the exact frozen source length
  and persisted segment limit, rebuilds the seal through the seekable verifier,
  and returns a v7 descriptor bound to that boundary. WAL verification streams
  from the seekable source without loading the full image. Local and archived
  sidecar readers validate magic and format-specific size caps before allocating
  the body, then perform a bounded read for both v5 and v7 seals.
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

The first Stage 7 slice adds a checked capacity assessment for the v7 image,
fresh-inode recovery workspace, maximum legacy and v7 seal sidecars, an empty
v7 successor and its install staging image, and terminal manifest headroom
bounded by the encoded handoff records. Existing-source assessments size the
legacy seal from the recovered Active WAL image, which may exceed the configured
segment maximum. Before a new Active WAL exists, enablement uses the configured
segment bound plus the header for the legacy logical image. It rounds that image
up to Leader's 1 MiB preallocation blocks and then to the filesystem allocation
unit for retained source-spool usage; seal sizing keeps the logical image bound.
The future v7 image and recovery workspace use the configured v7 bound.
It compares that transition requirement with caller-reconstructed retained
source-spool charges and reports eligible or deferred outcomes. New PITR
enablement now checks the requested configuration against this estimate before
repository bootstrap or durable enable intent; disabled-state source files,
staging files, and recovery temporaries are included. Legacy reopen now performs
the read-only assessment and logs deferred reason and capacity details while
keeping a healthy v5/v6 Active WAL writable with Leader I/O; legacy rotation
now attempts the same capacity-gated migration before making another legacy
successor. A deferred or unavailable assessment leaves the existing v5/v6
Leader recovery-point rotation path in place. The migration module now exposes an owned
reservation helper that atomically charges its source-spool and
recovery-workspace requirements on one live ledger. Active v7 recovery uses the
same filesystem-rounded workspace charge for both its reservation and ledger
limit. Callers must still reconstruct
all live charges, retain this reservation through the transition, and rebuild
in-flight migration charges from durable obligations after a crash.

On Linux, the read-only PITR status API reports the Active WAL's format and
scheduler, plus a v7 migration capacity estimate for Active v5/v6 WALs. The retained charge
combines visible filesystem allocations with live segment-manager source-spool
charges for tracked segments and any pending successor. It takes the greater of
the visible allocation and manager charge per segment, then adds staging and
unattributed allocations. The estimate is omitted on non-Linux builds, when
the Active WAL is not a legacy format, the manifest has no Active segment or
names a different segment, or capacity snapshot construction fails. A matching
legacy WAL may still report an estimate during other lifecycle states. This
advisory snapshot does not replace the atomic reservation. It exposes the Active legacy image's
logical length and allocated bytes; retained segment, staging, and
unattributed source usage; and the persisted configured segment limit. V7
image and recovery-workspace estimates cap that limit at the maximum supported
WAL size, while still reporting an oversized configured limit as deferred. It
reports the estimated v7 image, recovery workspace, both seal sidecars,
successor image and staging, and terminal manifest headroom as separate
transition charges. File extents are rounded to the allocation unit where
applicable. The status also reports the complete required total alongside
available source-spool capacity.
Manifest replay accepts the mixed-format handoff record from a sealed legacy
source to an empty v7 Active successor, while retaining the source obligation's
legacy format. Startup can recreate a missing empty successor after revalidating
the legacy source WAL and seal against the manifest anchor. Validation reserves
decoded/canonicalized batch and seal workspace before reading, and bounds the
sidecar by the manifest's WAL image length. The PITR envelope v9 adds a
`MigrationStarted` record that durably fixes the empty v7 successor boundary
before a legacy source is sealed, and replay requires `SegmentSealed` to publish
that exact boundary. The source manifest advances to v10 so older engines reject
compacted snapshots containing this intent. PITR v7/v8 envelopes and source
manifests v3–v9 remain readable. Forced-gap reconciliation retains the
intended successor identity until its abandoned obligation is cleaned up.
PITR resume now completes a durable `MigrationStarted` transition whether
the legacy seal sidecar was installed before the interruption or still needs to
be created. It re-syncs and validates the legacy source, checks the intended
successor header against the sealed predecessor, publishes the matching
`SegmentSealed` record, and installs the v7 successor before reopening
admission. Recovery counts the installed legacy seal and reconstructed
seal/successor reservations once against the transition budget, and verifies
and syncs an existing seal plus its parent directory on every retry before
publishing `SegmentSealed`. Regression coverage includes exact-capacity reopen
with and without the sidecar, source-tail truncation, a second reopen after v7
writes, and repeated seal directory-sync failure. Production resume now starts
the transition for an eligible, non-empty legacy Active WAL. An empty Active WAL
stays on its current format to avoid creating an empty sealed source; it can
migrate on a later resume after it has commits. The transition stops commit
admission, reserves the assessed retained and transition charges, persists
`MigrationStarted`, then uses the existing seal and successor-install recovery
path. If capacity defers or reservation fails before intent, resume leaves the
legacy WAL on Leader I/O and reopens admission. A replayed durable intent
reacquires its transition reservation before sealing and retains it through
successor installation and archive reconciliation. Intent publication replays the
current manifest projection under the state lock, preserving concurrent flush
reclamation. Successor installation also retires reconstructed charges for sources
reclaimed before its resource ledger becomes visible, under the same lock used to
publish that ledger. Sealed retries count the source seal and existing
successor/recovery images once against their transition allowances. Regression
coverage includes exact-capacity Sealed retries with and without retained successor
images, plus flush reclamation before migration intent and after recovery
reservation, checking source-spool charges and successful reopen. Maintenance-driven
legacy rotations use the same capacity assessment and reservation: eligible
nonempty sources transition to v7, while deferred capacity retains the normal
v5/v6 Leader rotation path. The transition keeps admission stopped through
successor installation and archive reconciliation, then checks manifest,
publication, archive-obligation completion, and Active-WAL health before
reopening writes. A second reconciliation cleans up migrated legacy sources
that already finished flushing; idle successors also schedule cleanup retries
for legacy source obligations. Standalone checkpoints select their capture
protocol under the PITR lifecycle locks after forced-boundary preparation, so
a migration during preparation uses the v7 admission cutoff and produces a
standalone snapshot without a source Active-WAL dependency. Linux regression
coverage exercises automatic migration through synchronous, asynchronous, and
repairing open, verifying the recovered data, subsequent write admission, and
persistence of both legacy and v7 writes across a second reopen.
Default activation and complete crash qualification remain Stage 8 work.

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

The first Stage 8 slice adds a process-level v7 Parallel crash case for a
successful WAL sync before durable publication or client acknowledgement.
The child resumes PITR, writes an acknowledged batch into the eligible legacy
Active WAL if clean close left it empty, then resumes again to install the v7
Parallel successor. It pauses at the post-`fdatasync` crash hook and is killed;
recovery must retain the acknowledged values and the synced candidate. This
qualifies that single boundary only. The second Stage 8 slice adds an engine-open
regression that corrupts DATA covered by the persisted Active manifest boundary.
Recovery must reject fallback below that hard floor, preserve both WAL and
manifest bytes on failure. The third Stage 8 slice adds an engine-level
continuation regression: it damages a synced candidate above the manifest floor,
checks fallback normalization, then appends and syncs new batches across repeated
reopens in both Leader and Parallel modes. It verifies that the discarded ticket
is reused and the speculative value never reappears. This does not complete the
process-crash recovery matrix. The remaining matrix and adoption gates are still
open.

The fourth Stage 8 slice crashes a child after the normalized Active recovery
image has been synced but before it is renamed over the source. It verifies the
source WAL remains byte-identical, then restarts and checks that stale temp
cleanup and ordinary recovery preserve the synced candidate. This covers only
the pre-rename installation cut; other copy/write/sync cuts, rename failures,
directory-sync failures, and repeated-normalization crash cases remain open.

The fifth Stage 8 slice crashes a child after the normalized recovery image has
been renamed over the Active WAL but before the parent directory sync. It checks
that the source path exposes the normalized image, no staging image remains, and
a later process restart recovers the acknowledged and synced values. This
process-kill case does not simulate loss of the unsynced directory entry after a
power failure; the remaining installation and repeated-normalization cuts stay
open.

The sixth Stage 8 slice injects a one-shot parent-directory sync error after the
normalized image has been renamed. It verifies installation returns an error,
failed-attempt cleanup leaves no staging image, and a retry revalidates and
recovers the same acknowledged prefix. The retry test does not simulate loss of
the unsynced directory entry after a power failure; other copy/write/sync cuts,
rename failures, and repeated-normalization crash cases remain open.

The seventh Stage 8 slice injects a one-shot error at the normalized Active
image's file sync before rename. It verifies the canonical source remains
byte-identical, failed-attempt cleanup removes the staging image, and a retry
revalidates and installs the same acknowledged prefix. The failure is injected
before the OS sync call.

The eighth Stage 8 slice injects an error after writing the staging image's
header and another after syncing the completed image but immediately before
rename. Both cases verify the source remains byte-identical, failed-attempt
cleanup removes the staging image, and a retry installs the same acknowledged
prefix. These injected errors do not simulate process crashes during copying or
the OS rename operation. The ninth slice below covers repeated normalization at
post-sync and post-rename crash boundaries; the remaining process/power-loss
matrix stays open.

The ninth Stage 8 slice extends the process-level installation scenario: after
one normalization completes, a later reopen crashes after syncing a second
temporary image but before rename. The test verifies the already-installed
canonical image remains byte-identical and on the same inode, then retries
normalization, crashes after replacement, and checks a final restart recovers
the same acknowledged and synced values. This covers repeated normalization
across process restarts, but does not simulate power loss or the remaining
correctness matrix.

The tenth Stage 8 slice continues from that recovered Active v7 WAL. A child
appends a distinct write after recovery and is killed after WAL `fdatasync` but
before publication; the next process must recover that write alongside the
prior acknowledged values. This covers process-level continuation and replay
after recovery.

The eleventh Stage 8 slice closes that recovered engine through a durable PITR
boundary, restores the latest archived point, and checks the acknowledged,
pre-publication, and post-recovery writes in the restored database. This covers
one nonempty continuation-to-seal/archive/restore path and dispatches v7 WAL
digest verification to the frozen image's whole-object digest. Empty,
trailing-control, and power-loss variants remain open; the twelfth slice below
adds the fallback-to-archive-restore case.

The twelfth Stage 8 slice adds a process-level Active fallback-to-restore case.
The child first acknowledges a write in the new v7 Active WAL, then crashes
after syncing a speculative write before publication. The test corrupts the
speculative DATA above the manifest-authorized floor. Recovery must reject that
candidate, normalize to the preceding frontier, and retain the acknowledged v7
write through sealing, archival, and Latest restore. Restore must include both
the earlier legacy segment and the fallback v7 WAL segment. This does not
qualify empty-image archive/restore, trailing speculative control frames, or
power-loss behavior. The thirteenth Stage 8 slice below adds the successful
empty v7 archive/restore lifecycle.

The thirteenth Stage 8 slice migrates an acknowledged nonempty legacy segment
to an empty v7 Parallel successor and verifies that Active image is exactly its
header and generation-zero FRONTIER. It then creates a recovery point to seal
and archive that zero-batch v7 segment, writes a later v7 successor, and closes
PITR. Latest restore must verify the empty segment as a proof before publishing
the restored database, preserve values from both nonempty segments, and fail if
the empty proof WAL is corrupted. This engine-level lifecycle test does not
qualify empty-image crashes, other trailing speculative control layouts, or
power-loss behavior. The fourteenth slice below adds one trailing-control case.

The fourteenth Stage 8 slice extends the fallback-to-archive-restore case with a
second complete, offset-bound, CRC-correct copy of the synced candidate
FRONTIER in a zero-filled speculative tail, followed by another zero frame. A
copied-database check first invalidates the original marker while leaving its
covered DATA intact; recovery must scan through the zero tail, discover the
trailing marker, and retain its candidate write. The main case then corrupts
that DATA, so recovery must reject both candidate copies after prefix
verification, normalize to the preceding manifest-authorized frontier, and
preserve that fallback through seal, archive, and Latest restore. This qualifies
this one process-level trailing-control layout; empty-image crashes and
power-loss behavior remain open.

The fifteenth Stage 8 slice adds process-restart coverage for two empty v7
successor installation boundaries. One child stops after `SegmentSealed` has
durably authorized the empty successor but before its WAL image is installed;
reopen must recreate the canonical header-plus-generation-zero-FRONTIER image
and preserve the sealed predecessor. A second child stops after that image has
been renamed into place but before its parent directory is synced; reopen must
verify and sync the existing image before resuming, again retaining the
predecessor. Both reopens must finish PITR reconciliation; after the second
recovery, a new write must succeed and survive another reopen. These
process-only cases do not simulate power loss or qualify partial image writes
and manifest-sync interruptions.

The sixteenth Stage 8 slice extends the empty-successor installation case with
a process crash after a post-recovery DATA write has synced but before it is
published. Reopen must recover that write and preserve the sealed predecessor;
the test then seals, archives, and restores the continued history. This covers
the empty-successor variant of recovery followed by continued writes for this
sync boundary. It does not cover power loss, other continuation variants, or
the remaining correctness and adoption matrix.

The seventeenth Stage 8 slice adds a process crash after an empty successor's
staging WAL image is synced but before it is renamed to the canonical path.
Reopen must remove the stranded install temp file, recreate the same
manifest-authorized empty successor, and preserve its sealed predecessor. The
existing post-rename crash case then proceeds to the following successor ID.
This covers only the synced-temp-before-rename process-crash boundary; it does
not simulate power loss or qualify partial image writes and manifest-sync
interruptions.

The eighteenth Stage 8 slice adds a process crash after all bytes of an empty
successor staging image have been written but before the staging file is synced.
The test verifies the complete image remains only at the temporary path, then
reopen removes it, recreates the manifest-authorized successor, and preserves
the sealed predecessor. This process-kill case does not simulate power loss or
claim that the unsynced staging bytes survive a machine crash.

The nineteenth Stage 8 slice adds a process crash after an empty successor's
staging image header is written but before its generation-zero FRONTIER is
written. It verifies the temp file contains only the exact canonical header,
then checks reopen removes it, recreates the authorized empty successor, and
preserves the sealed predecessor. This covers one partial-write process cut;
it does not simulate power loss or qualify manifest-sync interruptions.

The twentieth Stage 8 slice crashes after the `SegmentSealed` manifest record is
appended and before its sync returns. It checks the empty successor is not
installed by the child, then reopens and verifies manifest recovery durably
accepts the complete record before recreating the successor and preserving its
sealed predecessor. This process-kill case does not model power loss that drops
the appended bytes.

The twenty-first Stage 8 slice injects a returned manifest sync failure after
the `SegmentSealed` append. It verifies the append is classified as published
but not durable, write admission stays closed, no successor is installed, and
reopen reconciles the transition. This injected failure does not simulate power
loss that drops the appended bytes.

The twenty-second Stage 8 slice injects a parent-directory sync failure after
the empty successor image has been synced and renamed into place. It verifies
the canonical image remains visible, then reopen verifies the existing image,
syncs its directory entry, and preserves the sealed predecessor. This returned
error case does not simulate power loss that removes the unsynced directory
entry.

The twenty-third Stage 8 slice corrupts the generation-zero FRONTIER of an
installed, manifest-authorized empty Active v7 successor. Reopen must fail
closed without replacing or normalizing the WAL, changing either manifest file,
or leaving a staging image. This verifies the Active logical anchor remains a
hard recovery floor even when no acknowledged DATA batches exist.

The twenty-fourth Stage 8 slice injects a returned error immediately before
the no-replace rename of a fully written and synced empty successor image. It
verifies that the canonical successor remains absent, the staging image is
cleaned up, writes stay blocked while the durable `SegmentSealed` transition
awaits installation, and reopen installs the authorized successor while
preserving the predecessor. This does not simulate process termination inside
the rename syscall or power loss after a successful rename.

The twenty-fifth Stage 8 slice injects a returned error after writing the full
empty successor staging image but before its file sync. It verifies that the
canonical path remains absent, the unsynced staging image is cleaned up, writes
stay blocked while the durable `SegmentSealed` transition awaits installation,
and reopen installs the successor while preserving the predecessor. This does
not simulate an OS sync error or power loss.

The twenty-sixth Stage 8 slice covers both a returned short write and a process
crash after a proper prefix of the `SegmentSealed` manifest record reaches the
file. Runtime append revalidation truncates and syncs the short suffix; reopen
discards an EOF-truncated final JSON value only when its bytes identify a
`SegmentSealed` for an already durable matching `SealStarted` or `MigrationStarted`
intent (including a sealing obligation retained in a validated snapshot), then
completes the transition from that intent. Generic incomplete records, including
`FlushV3`, and other manifest parse errors remain fatal.
Unrelated older-segment archive or reclamation records and valid recorded-time
updates do not cancel that pending intent. A regression also checks that earlier
hits skipped by the crash occurrence selector finish their append exactly once
before the selected hit.

The twenty-seventh Stage 8 slice injects a positive short `pwrite` result while
writing a v7 Parallel FRONTIER. The fault hook writes one byte and returns that
short count, so the test exercises an actual partial frame without relying on
filesystem-specific partial-write behavior. It verifies the durability cycle
fails, the runtime is poisoned, neither rejected writes nor close append another
FRONTIER, and Active recovery falls back to the prior durable anchor while
discarding the uncommitted DATA.

The twenty-eighth Stage 8 slice exercises the v7 Leader FRONTIER `fdatasync`
failure hook directly. It verifies DATA and FRONTIER were written before the
injected error, the durable cursor does not advance, the writer is poisoned,
and later appends leave the uncertain image unchanged. This complements the
descriptor-based sync failure test with deterministic fault injection.

The twenty-ninth Stage 8 slice kills a process after a successful v7 Leader
WAL `fdatasync` but before in-memory frontier publication. Reopen must recover
both the earlier acknowledged value and the candidate covered by the completed
sync. This is a process-crash test; it does not model power loss or storage
that violates successful-sync durability.

The thirtieth Stage 8 slice injects a positive short positional write while
writing a v7 Leader FRONTIER. It writes a real one-byte prefix through a
buffered descriptor for the same inode, then verifies the durability cycle
fails, the writer is poisoned, subsequent appends leave the uncertain image
unchanged, and Active recovery falls back to the prior durable anchor. This
complements the equivalent v7 Parallel short-FRONTIER-write coverage. The test
checks the exact one-byte FRONTIER tail after a structurally valid DATA frame
and verifies a previously committed batch and its logical frontier survive.

The thirty-first Stage 8 slice injects a positive short positional write while
writing v7 Leader DATA. It verifies the partial DATA image does not advance the
durable frontier, the writer rejects later appends without changing the image,
and Active recovery returns to the prior durable anchor. This complements the
Leader FRONTIER short-write case and the existing post-DATA unwind coverage.
The test checks the exact one-byte DATA tail and preservation of the previous
committed batch, ticket boundary, and prefix digest after recovery.

The thirty-second Stage 8 slice embeds a complete structurally valid FRONTIER
frame in a DATA value. Discovery must report only the actual aligned
generation-zero and tail FRONTIER frames, and Active recovery must return the
value byte-for-byte unchanged.

The thirty-third Stage 8 slice extends `write-perf` for the qualification
matrix. With `--pitr`, the harness requests v7 by default, seeds the initial v6
Active WAL outside the timer, and invokes the existing capacity-checked
migration; `--pitr-format v6` keeps the isolated v6 Leader control unmigrated.
PITR rows report the Active format version and I/O mode read from `pitr_status`,
and fail if the requested format was deferred or the selected mode was not
installed. Ordinary v4 comparison rows report the selected v4 format and mode.
Format selection never edits an on-disk version field.
The default PITR preset uses 20,000 entries. Workload sizing includes fixed v7
frames, DATA fragmentation, and one FRONTIER per effective commit; oversized
runs fail before creating artifacts instead of crossing the 1 GiB segment cap
or including rotation in the measurement. The five comparison
legs, tmpfs and named-device requirements, coalescing controls, and reopen
metrics below remain qualification requirements; this harness change records
controls and metadata, not performance results.
V7 rows explicitly mark legacy WAL runtime counters unavailable and omit the
unimplemented pipeline diagnostics; sync/coalescing instrumentation must be
added before those rows can supply qualification evidence.

The thirty-fourth Stage 8 slice verifies that a recovered Sealed v7 source
remains read-only and has no attached WAL writer, scheduler, ring, or reservation
owner across open, close, and reopen in both Leader and Parallel selection
modes. It also checks that the installed Active successor uses the requested
mode and that the frozen source WAL and seal bytes remain unchanged. Sealing
startup, process-crash qualification, and the remaining correctness matrix are
still open.

- Complete the correctness matrix below using the persistence model, real
  runtime injection, and process-level integration. Regress ordinary v4,
  strict v5/v6, TTL, recorded-time rollback/clamping, range deletion, OCC,
  checkpoints, async cancellation, and close together.
- `write-perf` now reports actual format/mode for v4 and PITR comparison rows,
  supports an explicit isolated v6 Leader control, and selects v7 Parallel or
  Leader without an unconditional PITR-to-Leader rewrite. It verifies the
  Active format/mode instead of reinterpreting on-disk version fields. Record
  the required comparisons, devices, coalescing controls, and recovery metrics
  listed below before treating any run as qualification evidence.
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
| Segment exhaustion and shared pressure | Exact-boundary DATA-plus-marker exhaustion triggers rotation before ticket assignment for a batch that fits a fresh segment; retry preserves caps and ordering. Shared-ledger pressure, including aggregate spool and seal-index quota, does not trigger rotation, and oversized batches cannot loop through empty successors. |
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
