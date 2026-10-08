# RFC 025: Parallel WAL with Embedded Frontiers and One Sync

| Field | Value |
| --- | --- |
| Status | Proposed |
| Date | 2026-10-08 |
| Author | kv-engine Contributors |
| Builds on | [RFC 024](024-dedicated-wal-pipeline.md), [RFC 023](023-point-in-time-recovery.md) |

## 1. Summary

Every WAL format introduced after this RFC MUST support the dedicated,
ticket-ordered parallel pipeline and select `WalIoMode::Parallel` by default.
This includes new PITR formats. A narrow exception is permitted only when the
format's RFC documents a correctness or format constraint, an alternative
durability/recovery protocol and default mode, and explicit maintainer
acceptance. An unimplemented pipeline or a performance preference alone is
insufficient. PITR v7 has no exception.

The first extension is PITR WAL **v7**. It adds persistent batch tickets and
fixed-size `DATA` and `FRONTIER` frames inside the WAL. The coordinator writes
a frontier for a contiguous completed ticket prefix, then one successful
`fdatasync(WAL)` covers both data and marker before durability is published.
Group submission remains parallel throughout; there is no `.frontier` file
or second steady-state sync.

Active recovery locates markers independently of the forward data scanner
and selects the newest candidate whose complete covered prefix validates.
A marker's CRC alone does not prove that its sync succeeded. Active v7 may
fall back when an interrupted sync left a complete marker but incomplete
data; durable manifest/sealing boundaries and immutable archive objects
remain strict. This is an explicit v7 recovery contract and fault-model
tradeoff, described in section 6.

Existing files keep their format-specific recovery semantics: ordinary v4
already defaults to Parallel; PITR v5/v6 and older MVCC files retain Leader;
unframed files retain buffered I/O. New PITR segments use v7 after a durable
rotation when persisted capacity can support its recovery workspace. A healthy
legacy database with insufficient capacity remains writable using v5/v6 Leader
until an explicit capacity change permits migration (section 7.2). This is a
proposed format and adoption policy; the current writer still creates PITR v6.
WAL remains optional through `enable_wal`.

## 2. Findings from the existing RFCs and implementation

[RFC 012](012-parallel-wal.md) introduced aligned, framed v4 writes using
io_uring and `O_DIRECT`. Its original proposal must be read alongside its
adoption note: the current Leader path can submit several writes within a
group, but completes that group's durability cycle before the next group.
`O_DIRECT` and queued requests alone do not prove physical device overlap or
durability. Linux documents the distinction between direct I/O and synchronous
storage guarantees in [open(2)](https://man7.org/linux/man-pages/man2/open.2.html).

RFC 024 supplies overlap between groups, ordered admission and packing, an
independent sync coordinator, and a contiguous durable ticket frontier.
The shipped pipeline has 32 in-flight group slots and 256 ring entries.
An admitted batch receives one ticket; a group covers contiguous tickets;
one group may require several write SQEs. These units remain distinct.

The remaining format restriction is deliberate:

- `Wal::from_recovered_parts` in `kv-engine/src/wal.rs` selects the requested
  runtime only for v4; `Wal::create_v5` installs Leader for PITR v5/v6.
- `Wal::recover_v5` fails when a damaged batch has a valid later batch.
  Concurrent groups can leave complete groups A and C with a torn group B
  after a crash, even though no ticket in B or C was acknowledged.
- `walk_v5_segment` in `kv-engine/src/pitr/seal.rs` validates the segment in
  physical order. v5 hashes the whole aligned prefix; v6 hashes the immutable
  file header and batch headers/payloads, excluding alignment padding.
- The parallel runtime currently assumes v4 framing, a 4096-byte data start,
  and tickets starting at zero. PITR also requires ordered recorded times,
  seal indexing, predecessor chains, and shared spool reservations.

Changing the selector alone would expose a normal interrupted parallel write
as corruption under v5/v6. RFC 024 explicitly requires a persisted prefix or
an equivalent generation protocol before extending the pipeline to PITR.

## 3. Default and compatibility policy

| File format | Default after implementation | Creation and recovery policy |
| --- | --- | --- |
| Legacy unframed | Buffered | Preserve legacy recovery. |
| Older MVCC v2/v3 | Leader | Recover with the existing codec; successors use the current writer format. |
| Ordinary v4 | Parallel | Preserve the existing contiguous valid-prefix recovery rule. |
| PITR v5/v6 | Leader | Preserve strict recovery and each version's digest rule; preflight capacity before migration, or continue legacy Leader writes and rotations (section 7.2). |
| PITR v7 | Parallel | Use typed frames and the single-sync frontier protocol specified here. |
| Future registered formats | Parallel, unless an explicit RFC exception is accepted | Define and qualify parallel recovery, or the alternative protocol required by the documented constraint. |
| Unknown versions | Reject | A higher version number is not evidence of compatibility. |

Replace scattered `version == 4` runtime checks with a checked format
descriptor shared by creation, reopen, encoding, recovery, sealing, and
restore. It supplies framing/alignment, data-start offset, digest rule,
recovery policy, and supported I/O modes. A new writer descriptor cannot be
registered without Parallel support and its recovery coverage, or a reference
to the accepted exception RFC and coverage of its alternative protocol. An
exception must be scoped to named formats and cannot silently change v4/v7 or
other registered defaults. Readers still recognize only explicitly supported
versions and reject unknown flags.

The policy applies to direct WAL/memtable constructors, `open`, `open_async`,
`open_repairing`, PITR enable/resume, and successor creation during rotation.
The explicit Leader selector remains available on compatible new formats for
comparison and operational rollback. Both modes use the same on-disk protocol;
Leader on v7 also persists the boundary before acknowledgement. Initialization
errors remain errors, following RFC 024's existing io_uring/direct-I/O policy.

Ordinary databases can continue creating v4: enabling Parallel does not itself
require a format bump. Any future ordinary format must satisfy this policy,
whether it keeps v4-style prefix recovery or adopts a stricter boundary rule.

## 4. PITR v7 representation

### 4.1 Immutable header and typed frames

The v7 WAL keeps a 4096-byte big-endian file header. WAL format versions are
independent of source-manifest versions. Header bytes `0..128` retain the
v5/v6 identity and predecessor layout from RFC 023, with version `7` and
flags `0`. Bytes `128..144` contain a fresh, nonzero 128-bit segment
incarnation; bytes `144..148` contain CRC32 over `0..144`; all remaining
header bytes are zero. The incarnation is created once. All digest preimages
use raw bytes without text encoding.

Every following physical frame is exactly 4096 bytes, starts at a 4096-byte
aligned offset, and has this 64-byte common header:

```text
magic[8] = "TKVW7FR1" | frame_version:u16 = 1 |
kind:u16 = 1 (DATA) or 2 (FRONTIER) | header_len:u16 = 64 |
flags:u16 = 0 | wal_header_digest[32] | frame_offset:u64 |
body_len:u32 | frame_crc32:u32
```

`wal_header_digest` is SHA-256 of the complete immutable WAL header and binds
the timeline, archive epoch, segment ID, and incarnation. `frame_offset` must
equal the actual offset. `frame_crc32` covers `0..60` followed by
`64..4096`, including the body and zero padding. CRC32 uses RFC 023's
`crc32fast` definition. Unknown versions/kinds/flags, noncanonical lengths,
nonzero padding, and arithmetic overflow reject.

The common header belongs to the codec; a value can never occupy a frame
header. Recovery considers FRONTIER only at these aligned header positions.
Searching arbitrary payload bytes for marker magic is forbidden. This avoids
mistaking a user value containing an entire marker-shaped byte string for a
physical control frame. Checksums and hashes detect accidental damage and
provide no keyed authentication.

### 4.2 DATA fragmentation and logical batches

A DATA body begins with this 24-byte fragment header:

```text
segment_ticket:u64 | fragment_index:u32 | fragment_count:u32 |
batch_bytes:u64 | batch_fragment[...]
```

The fragment header leaves 4008 bytes per frame for logical batch bytes.
A batch owns a contiguous, noninterleaved range of DATA frames. Each
`fragment_index` MUST satisfy `0 <= fragment_index < fragment_count`, and
the indices in physical order MUST cover `[0, fragment_count)` exactly once.
All fragments repeat the batch's ticket, count, and total length.
The count equals `ceil(batch_bytes / 4008)`; every
nonfinal fragment carries 4008 batch bytes and the final one carries the exact
remainder. `body_len` includes the 24-byte fragment header. The implementation
checks all lengths before allocating or slicing and preserves the existing
batch/value limits after allowing for frame overhead.

The reassembled logical batch has this 48-byte header and its payload:

```text
segment_ticket:u64 | commit_ts:u64 | recorded_at_secs:i64 |
recorded_at_nanos:u32 | entry_count:u32 | data_len:u32 |
data_crc32:u32 | header_crc32:u32 | reserved:u32
```

`header_crc32` covers bytes `0..40`, including `data_crc32`; `reserved`
is zero, and `batch_bytes` equals `48 + data_len`. Payload encoding, canonical
mixed-operation ordering, nonempty-batch rules, and recorded-time validation
follow RFC 023. The logical ticket must match every fragment header.
Tickets are segment-local ordinals `0, 1, ...`;
commit timestamps are strictly increasing but may have gaps. Control frames
consume physical offsets and never consume a ticket or commit timestamp.

The logical `prefix_digest(T)` is SHA-256 over the complete WAL header and
each covered logical batch header/payload in ticket order. Fragment headers,
FRONTIER frames, and alignment padding are excluded from this logical hash,
but their canonical encoding and CRCs are validated separately.
`D(T)` is the aligned end of the last covered DATA frame. For a nonempty
frontier, `E(T)`, named `durable_end` on disk, is the end of its verified
physical prefix:

```text
E(T) = max(D(T), previous_frontier_offset + 4096)
```

The prefix `[4096, E(T))` MUST contain exactly the complete DATA batches with
tickets in `[0, T)` and canonical earlier FRONTIER frames. It MUST NOT contain
an incomplete batch or any DATA ticket at or above `T`. It may end with an
earlier FRONTIER rather than DATA; those control bytes do not change the
logical digest. Generation zero covers only the immutable header, so
`E(0) = 4096`. Section 4.3 defines the required physical ordering.

### 4.3 FRONTIER frames and physical allocation

A FRONTIER body is exactly 104 bytes:

```text
generation:u64 | ticket_end:u64 | durable_end:u64 |
last_commit_ts:u64 | prefix_digest[32] |
previous_frontier_offset:u64 | previous_frontier_digest[32]
```

The record digest is SHA-256 of the complete canonical 4096-byte frame.
`ticket_end` is exclusive and equals the covered batch count.
Generation zero is mandatory at offset 4096: `ticket_end = 0`,
`durable_end = 4096`, `last_commit_ts = 0`, `prefix_digest =
wal_header_digest`, and both previous-frontier fields are zero. The first
DATA allocation therefore starts at 8192; an empty v7 image is 8192 bytes.

For every non-generation-zero FRONTIER `F`, the writer, decoder, and recovery
validator MUST enforce all of the following against its named predecessor:

```text
F.previous_frontier_offset + 4096 <= F.durable_end <= F.frame_offset
F.generation == prev.generation + 1
F.ticket_end > prev.ticket_end
F.durable_end > prev.durable_end
F.last_commit_ts > prev.last_commit_ts
```

The previous offset MUST be aligned and name an earlier canonical FRONTIER
in the same WAL incarnation; `previous_frontier_digest` MUST equal that
frame's record digest. All arithmetic MUST be checked. The chain MUST terminate
at the mandatory generation-zero frame and cannot contain a cycle.
The `durable_end` formula and exact-prefix rules in section 4.2 also apply.
In particular, the entire previous frontier MUST lie inside the current
verified prefix; monotonic tickets or a valid CRC alone are insufficient.

DATA ranges and FRONTIER slots use one checked physical allocator.
Reservation of a frontier slot at the current allocation tail is serialized
with DATA admission; no previously assigned offset moves. At marker offset
`M`, all already allocated DATA ranges end at or before `M`, and
`durable_end <= M`. Later DATA can be allocated at or beyond `M + 4096`.
There is at most one frontier write/sync cycle in progress per WAL; prior
frontier frames remain immutable.
One ticket group can span DATA ranges separated by a control frame. Split its
write SQEs at those boundaries; a packed DATA write must never cover or
overwrite a FRONTIER slot. Group completion still accounts for every DATA SQE.

A frontier can follow speculative DATA:

```text
D0 | D1 incomplete | D2 complete | FRONTIER(ticket_end=1, durable_end=end(D0))
```

Only D0 is covered. Neither `M` nor the physical EOF is the replay boundary.
The marker's physical end, `M + 4096`, is the extent needed to retain its
recovery evidence. Section 6 defines how to remove speculative DATA without
destroying that evidence.

A later frontier can retain the preceding marker at the end of its prefix:

```text
D0 | D1 | F_A(ticket_end=1, durable_end=end(D0)) |
          F_B(ticket_end=2, durable_end=end(F_A))
```

After D1 completes, `F_B` covers D0 and D1 and retains all of `F_A`, even
though D1's DATA end precedes `F_A`. This permits freeze/close to drain after
admission has stopped without requiring a new DATA allocation. Until D1 is
complete, no candidate covering it is eligible. More generally, a target
whose required retained prefix includes DATA at or above its ticket end is
ineligible; section 5.2 requires waiting for those already admitted tickets,
or failing them under section 5.3.

At most one nonempty frontier is appended per newly covered batch, giving
`frontier_bytes <= 4096 * (1 + batch_count)` during normal append, including
generation zero. Recovery replaces discarded markers as described below and
does not grow the image on every reopen. Marker slots must be reserved before
admission; preallocation of WAL capacity is not evidence of a marker or data.

### 4.4 Seal digest

The v7 logical prefix digest verifies the selected DATA sequence. Its archive
`wal_digest` is a separate SHA-256 over the entire final immutable WAL image
`[0, sealed_end)`, including frame headers, all retained FRONTIER frames,
checksums, and padding. `sealed_end` ends immediately after the terminal
frontier, which covers every DATA batch in the sealed image.

This distinct rule binds the physical archive representation as well as its
logical history. v5 continues hashing its aligned prefix and v6 continues
hashing its header and logical batches without padding. The version registry
must select these rules explicitly.

## 5. Admission and durability

### 5.1 Admission and ordered metadata

Preparation reserves buffer memory, framed DATA capacity, PITR spool space,
seal-index capacity, worst-case marker allocation, and the recovery workspace
growth required by section 7.3 before ticket assignment. Rejection creates
no ticket or offset hole. Admission atomically assigns a ticket and a
nonoverlapping DATA range and publishes the batch to the ordered queue,
following RFC 024's handoff rules. FRONTIER reservation uses the same allocator
and does not enter the data-ticket queue.

The ordered packer finalizes the recorded-time clamp, logical batch header,
and fragment headers, then advances logical prefix hash and seal index state
in ticket order. Hash/index state for prepared or written tickets is tentative.
Pending group boundaries and fixed barrier cutoffs retain bounded digest
snapshots and index ends, including a cutoff inside a group. Sealing and
publication can access only state covered by the durable frontier.

Maintain a separate physical-image hash in allocation order, including control
frames, so the final seal digest is ready after drain. Gaps in frame preparation
may defer that hash's advancement; they must not block earlier writes or marker
completion. Bounded pending state follows the in-flight/memory limits.
Recovery rebuilds both hashes in a streaming pass.
No payload rescan is permitted in the stop-admission rotation section.

### 5.2 One ordered sync stage

Let `T` be a captured exclusive ticket end no greater than the contiguous
written frontier, and `E(T)` its verified physical-prefix end from section 4.2.
The coordinator:

1. Confirms full write completion for every DATA fragment of every ticket
   below `T`. Short writes, missing CQEs, and ambiguous submission do not
   count as completion. It MUST also verify that `[4096, E(T))` contains
   exactly those tickets and canonical control frames, including the complete
   previous FRONTIER, using ordered allocation/codec/completion metadata.
   A target whose required retained prefix includes a ticket at or above `T`
   is ineligible. Wait for the necessary already admitted tickets to complete
   and capture an eligible target; never rely on future admission to make a
   stopped-admission drain possible.
2. Reserves a FRONTIER frame at the current physical allocation tail and
   writes exactly `(T, E(T), prefix_digest(T))` with the saved metadata and
   previous-frontier binding. It verifies full marker write completion.
3. Calls `fdatasync(WAL)` and waits for success, covering the previously
   completed DATA writes and FRONTIER write on that same file.
4. Publishes the durable ticket end and corresponding seal metadata with
   the existing synchronization rules, then wakes covered waiters.

```text
DATA writes complete -> FRONTIER write complete -> fdatasync(WAL) -> publish
```

No live durable/publication frontier advances merely because a marker write
completed. Writers may submit later groups while the marker write or sync
runs. Incidental persistence of later bytes does not extend `T`.
Capture the final `T` before marker construction; coalescing cannot enlarge
its coverage during sync. Public sync, freeze, checkpoint, recovery-point,
and close barriers retain fixed admission cutoffs.
An eligible marker may cover additional already admitted tickets needed to
retain its predecessor; this does not change a barrier's captured cutoff.
An ineligible target cannot be acknowledged merely to satisfy that cutoff.

One advance SHOULD cover multiple completed groups when available, sharing
one marker and one WAL sync. The normal coalescing policy MUST document a
finite maximum wait and batch/byte bounds; arrivals cannot repeatedly extend
its deadline. It must make progress with one writer and cannot wait
indefinitely for a target occupancy. Deliberate wait is charged to latency.

The durability guarantee relies on a successful file sync, as described by
[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html). Creation writes
the immutable header and generation-zero frame, successfully syncs the WAL,
syncs its directory entry, and durably installs Active source-manifest state
before any batch can be acknowledged. Directory and manifest syncs also remain
required for rotation and other metadata transitions; the one-sync claim is
per normal durability advance.

Existing memtable insertion and ordered MVCC publication still precede API
success. Native async waiters wait through marker write, WAL sync, and
publication. Cancellation after admission leaves owned buffers and memtable
leases alive until retirement; PITR backpressure never waits for archival
while holding admission locks.

### 5.3 Errors and ownership

A short/failed marker write or failed WAL sync cannot advance the live durable
frontier and poisons new admission. Reopen reconciles the unknown outcome.
A complete marker may survive while its covered DATA did not persist, because
both preceded the same interrupted or failed sync. Section 6 therefore treats
an active marker as a candidate until its covered prefix validates.

An earlier durable boundary remains valid when a later DATA group fails.
A healthy contiguous prefix below the earliest failed ticket may complete a
marker/sync cycle under RFC 024's poison rules only if it also satisfies the
retained-prefix eligibility rule in section 5.2. It cannot cross a failed or
uncertain group. If retaining the previous FRONTIER would include such a
group, fail the remaining unacknowledged waiters; do not wait indefinitely
for an eligible marker after admission has stopped. A failed marker/sync
cycle cannot be followed by another acknowledged cycle in that runtime.
Buffers remain owned until terminal CQEs or proven safe teardown and until
any required hash consumption completes. A later failure never changes
acknowledged outcomes.

After a process-only restart, complete DATA and FRONTIER bytes may still be
readable in cache without having reached stable storage. Buffered writeback
errors can clear dirty-page state and be missed by a reopened descriptor;
merely retrying sync is insufficient in that case. See
[Linux writeback error handling](https://docs.kernel.org/filesystems/vfs.html#handling-errors-during-writeback)
and the [fsync failure study](https://www.usenix.org/conference/atc20/presentation/rebello).
Recovery MUST rewrite the accepted image to a fresh inode, successfully
`fdatasync` it even when no length change was needed, and durably install it
before service. This recovery barrier is specified in section 6.3.

## 6. Active recovery and corruption rules

### 6.1 Independent marker discovery and prefix validation

Recovery obtains Active/Sealing/Sealed status from durable source-manifest
state or a verified backup/catalog reference. Finding a seal sidecar cannot
upgrade an Active WAL to Sealed. Validate the immutable header and mandatory
generation-zero frame first. Missing or damaged generation zero in a
manifest-installed segment is an error, including for an empty segment.
Only a file never installed in durable manifest state can be incomplete
creation and cleaned up through the normal orphan protocol.

For an Active segment, inspect fixed aligned frame headers from the bounded
physical tail toward offset 4096, independently of DATA decoding. EOF may
include preallocation, zeros, holes, or one partial final frame. Discovery
cannot stop at a broken DATA frame and cannot treat preallocated length as a
logical boundary. Batched backward reads bound I/O overhead; the 1 GiB limit
bounds total discovery work. Any optional allocation/offset hints are
optimizations, not recovery authority.

A candidate must be a complete canonical FRONTIER with the correct header
binding, actual offset, CRC, valid bounds, and a valid previous-frontier chain
back to generation zero. Decoder and recovery MUST enforce section 4.3's
physical-prefix containment, generation, ticket, end, timestamp, and predecessor
digest invariants for every nonzero generation. Follow previous offsets only
after validating the current frame; they accelerate chain checks but cannot
locate a torn last marker whose pointer is unreadable. Backward discovery
remains the fallback.
Never accept a foreign frame or marker-shaped DATA payload.
Memoize chain checks by physical offset rather than walking the complete
chain anew for every candidate.

For each candidate, validate the complete physical prefix `[0, durable_end)`:
all DATA fragments, exact ticket order, logical batch CRCs and canonical
operation encoding, zero padding, timestamp/recorded-time ordering, and every
interspersed control frame. Compare its exact covered count, verified
physical-prefix end, last timestamp, and logical prefix digest, including
section 4.2's `E(T)` formula. Validate intermediate frontiers against their
own named prefixes, even when speculative DATA physically precedes those
markers. Every frontier in a candidate's chain must have a valid covered
prefix; all predecessors lie wholly within the candidate's retained prefix.

Use bounded candidate metadata sorted by named physical-prefix boundary and
one streaming forward verification pass with incremental hashes. Track prefix
ends after complete control frames as well as DATA batches; a trailing FRONTIER
advances the verified physical end without feeding the logical digest. Stop
usable prefix advancement at the first invalid physical frame; no candidate
crossing it can validate. Do not rescan the whole prefix separately for every
marker. Bound metadata by the maximum frame count and use checked allocations.

Choose the newest fully validated candidate by physical marker offset.
A checksum-invalid/torn marker or a complete marker with an invalid covered
prefix can be rejected in favor of an older fully validated candidate.
DATA above the accepted ticket end is discarded even when its bytes are
complete and lie before the selected marker. Rejected candidate ranges and
reasons must be reported. An actual read/I/O error fails recovery; it is not
permission to reinterpret unread bytes as an interrupted suffix.

### 6.2 Durable anchors and the explicit fault model

A durable source-manifest or checkpoint reference that claims an Active WAL
boundary MUST persist a logical anchor with these fields:

```text
ActiveDurableAnchor = (
    timeline, archive_epoch, segment_id, incarnation,
    ticket_end, durable_end, last_commit_ts, prefix_digest
)
```

This is a hard floor. Recovery MUST validate the exact anchored boundary,
including its identity and logical digest, within any accepted candidate's
prefix. A later candidate must contain that verified boundary; merely having
a greater ticket or timestamp is insufficient. A candidate below or
conflicting with the anchor cannot be accepted.
An Active durable anchor MUST NOT require a physical `frontier_offset`,
generation, or frontier record digest. Section 6.3 can relocate and rechain
the selected marker while preserving the immutable header and exact logical
anchor; those physical marker fields are not stable Active identity.

Durable Sealing intent and Sealed/immutable references instead bind the
frozen physical image:

```text
ImmutableBoundary = (
    ActiveDurableAnchor, sealed_end, terminal_frontier_digest, wal_digest
)
```

The terminal marker offset is `sealed_end - 4096`. These references MUST
validate the complete image and terminal certificate, forbid fallback and
normalization, and retain the strict archive/index checks in section 7.1.
A conflicting durable seal/catalog reference is corruption, never an excuse
to lower the boundary. Allocation-only timestamp high-water marks still
prevent timestamp reuse and do not invent a claim that an unmarked DATA batch
was durably committed.

The normal guarantee assumes bytes covered by a successful sync and synced
directory publication survive subsequent crashes. Because no newer cycle
can be acknowledged after a marker/sync failure, a previous successfully
synced frontier and its DATA remain recoverable under that assumption.
A complete marker and valid covered DATA may also survive without an API
success; recovery may include that unknown client outcome.

One sync does not record whether the syscall returned success. An interrupted
sync can persist the marker before its covered DATA. Without an independent
durable anchor, this has the same on-disk appearance as later storage damage
to a successfully synced active prefix. Active v7 deliberately permits
fallback to a fully validated older frontier and cannot promise detection of
all such post-sync loss. Checksums do not remove this ambiguity. Sealing,
sealed archives, and durable manifest claims remain strict. v5/v6 retain
their current corruption contract; `open_repairing` must not lower a known
durable anchor or apply v7 fallback to historical formats.

### 6.3 Durable recovery installation

The selected boundary remains provisional. Stop new admission, retire old
I/O ownership, and hold the exclusive recovery lock. Never truncate the
original file to `durable_end` and sync it before installing a replacement
certificate: its selected FRONTIER may lie after speculative DATA and would
be erased, leaving a second crash with no certificate for the selected boundary.
A retained predecessor certifies an older ticket end; it cannot prove the
newly selected boundary on its own.

Normalize an Active image in a fresh temporary inode in the same directory:

1. Copy the immutable header and the verified exact bytes
   `[4096, durable_end)` with bounded streaming reads/writes. This preserves
   DATA offsets and all complete control frames inside the retained prefix.
2. Append a recovery FRONTIER at `durable_end` naming the accepted ticket
   end, last timestamp, and logical digest. Link it to the last retained
   frontier, with generation one greater than that retained frame. Its complete
   predecessor MUST be inside the copied prefix, and the replacement MUST
   satisfy section 4.3 while preserving the exact `ActiveDurableAnchor`.
   The prefix may end with that predecessor rather than DATA. If empty,
   emit the original generation-zero frame at 4096 instead.
   The old selected marker and every frame outside the retained prefix are
   omitted. Generation numbers may be reused only by this exclusive
   replacement of discarded frames; tickets and commit timestamps retain
   their recovered meaning.
3. Verify every write and MUST successfully `fdatasync` the fresh complete
   image before treating its frontier as the recovered durable boundary.
   This is mandatory even when the original file already had exactly this
   length or no truncation was necessary. A no-op sync of the old inode
   cannot satisfy the requirement.
4. Atomically replace the canonical WAL pathname, successfully sync the
   containing directory, then install recovered durability/publication state
   and permit service, append, seal, archive, or backup to depend on it.

The normalized physical end is `durable_end + 4096`; new physical allocations
start there, after the retained recovery marker. The next data ticket is the
accepted `ticket_end`, and MVCC timestamps/recorded-time clamps are rebuilt
from retained batches and existing manifest anchors. Repeating recovery on
the normalized image does not append an extra marker or change its bytes.

For a fixed Sealing obligation, copy and validate the exact intent-bound
image through its terminal frontier without changing frame bytes, generations,
or seal digest; sync and replace it through the same installation barrier.
Sealed immutable objects use strict validation and are never normalized.

A copy/write, required sync, replacement, or directory-sync failure fails
reopen and exposes no provisional state. Do not retry by silently choosing an
older boundary after such a failure. The original canonical file remains
the authority until replacement; temporary filenames are never candidates.
A crash during replacement recovers from the old or new canonical image under
the qualified filesystem's atomic replacement guarantees and repeats the
installation barrier. Orphan temporary files are accounted and durably
cleaned only after canonical validation. Old inodes/pins remain charged until
their readers and I/O owners are gone.

A v7 permanent commit coordinate requires DATA covered by an accepted frontier.
A complete unmarked batch has never reached publication and does not by itself
consume a permanent coordinate after restart. This refines RFC 023 for v7
only; never reuse a timestamp required by an existing manifest anchor.

| Crash point or observed state | Recovery result |
| --- | --- |
| D0 and D2 complete, D1 incomplete, marker covers only D0 | Find the marker independently and retain only D0. |
| Complete newest marker covers DATA missing after interrupted sync | Reject the candidate; choose the newest fully validated older one above all durable anchors. |
| Newest marker torn, zeros/preallocation or later DATA at physical tail | Continue aligned discovery; validate an older marker and its DATA prefix. |
| Marker and its covered DATA complete before acknowledgement | May retain that candidate after validation and durable recovery installation. |
| WAL sync and ordered publication complete | Every acknowledged batch is within a recoverable prefix under the stated storage assumptions. |
| Valid cached image after a failed sync, even with unchanged length | Rewrite to a fresh inode, sync, atomically replace, and sync the directory before service. |
| Recovery crashes before replacement | The original certificate remains available; temporary images are not authority. |
| Recovery crashes during/after replacement | Validate the canonical old/new image and repeat the installation barrier. |
| Copy, recovery sync, replacement, or directory sync fails | Fail reopen; no provisional publication and no fallback through the error. |
| No candidate satisfies a durable manifest/Sealing floor | Report corruption. |
| Sealed object has any length, frame, index, or digest mismatch | Report corruption; no active fallback. |

## 7. PITR integration and migration

### 7.1 Freeze, seal, archive, and backup

Freeze stops admission, drains blocking guards and async memtable leases,
completes marker/write/sync through its fixed ticket cutoff, and drains ordered
publication. Before durable Sealing intent, every admitted DATA batch must be
covered by the terminal frontier. Its end is `sealed_end`; preallocated tail
bytes are excluded. Finalize saved logical/physical hashes and the index,
then durably record Sealing intent before installing a successor.

An empty segment has a real header, generation-zero frame, and predecessor
anchor. Its sealed length is 8192, batch count zero, and it invents no commit
coordinate. Seal sidecar v1 may retain its byte layout with an explicitly
registered `wal_format_version = 7`, but readers, empty-length checks, digest
selection, and index validation must become format-specific. The v7 WAL digest
is the exact physical-image hash from section 4.4; its count equals the
terminal ticket end and its index names the first DATA frame of each batch.

Sealing intent MUST persist the complete `ImmutableBoundary` from section 6.2:
the logical `ActiveDurableAnchor`, sealed end, terminal frontier digest, and
final WAL digest. Extend source-manifest records/versioning explicitly if the
current schema cannot represent them. A recovered Sealing obligation validates
all frames and its terminal marker against this intent and any durable seal;
it cannot fall back, promote an unmarked suffix, or rewrite that physical image.

After seal and Sealed source-manifest publication are durable, the immutable
WAL/seal pair supplies the recovery boundary. FRONTIER frames stay in the WAL;
there is no companion journal to delete. The archived WAL is exactly
`[0, sealed_end)` with its seal. Strict archive/restore verifies every frame,
terminal coverage, exact length, count/index, identity, predecessor, and whole
image digest. Do not apply Active fallback to immutable objects.

Checkpoint/backup capture uses drained sealed boundaries. A helper copying
an Active WAL must pin its identity and selected certificate consistently,
copy through that marker's physical end, and include all its covered DATA.
A bare copy stopping at `durable_end` is insufficient. Prefer the existing
freeze/seal capture protocol; a seal temporary cannot bypass Active recovery.
Any physical marker offset/digest pinned by an Active-copy helper is transient
copy bookkeeping, not a persisted Active durable anchor. Persisted Active
references MUST use section 6.2's logical tuple; physical-image authority
requires the durable Sealing/immutable transition.

### 7.2 Existing databases

Reopen reads v5/v6 with their current strict scanners and digest rules.
Before committing a migration, it MUST preflight the persisted capacity
configuration and reserve the transition's maintenance space. The budget
must support the largest permitted v7 physical image and its fresh-inode
recovery workspace, existing obligations and pinned/orphan allocations, and
successor, seal/index, and terminal manifest headroom. Checking only the
current short WAL or an empty 8192-byte successor is insufficient. This
preflight and the transition reservations MUST precede durable migration
Sealing intent; section 7.3 governs ongoing reservations.

If a healthy legacy database's capacity cannot support v7, reopen MUST succeed
with the existing v5/v6 Active WAL using Leader, subject to its existing
recovery and capacity rules. PITR writes remain enabled; normal legacy
rotations create v6 Leader successors until v7 capacity becomes eligible.
Report migration deferral, its reason, and required/available capacity in
status. Do not silently raise spool limits, lower the configured segment
maximum, or disable PITR writes to force an upgrade.
For example, a persisted 1 GiB segment maximum and 1.3 GiB source-spool limit
cannot budget two maximum-size images plus maintenance; upgrading the binary
alone MUST leave that otherwise healthy legacy database on Leader.

Capacity changes follow RFC 023's persisted safety-configuration rules:
limits are immutable within an archive epoch, and `resume_pitr` cannot override
them. Increasing them requires clean disable followed by enablement with a
new epoch and base. Once suitable capacity is durably configured, enablement
or the next eligible rotation/migration installs v7. With unchanged eligible
capacity, migration completes recovery and sealing of the old active segment
using Leader, installs a v7 successor durably, then accepts new PITR writes
using Parallel. That successor names the predecessor's actual WAL/seal
digests, allowing mixed v5/v6/v7 archive chains without rewriting old bytes
or beginning a new epoch solely for the format change.

New PITR enablement MUST reject insufficient v7 capacity before durable enable
or migration intent; it does not create a legacy fallback writer. An already
installed Active v7 WAL MUST fail reopen if its required recovery workspace
cannot be obtained, rather than downgrade to v6 or skip recovery installation.
Legacy capacity deferral is not an exception to the v7 Parallel default.
Enablement on an ordinary database retains RFC 023's epoch/base requirements.

Interrupted migration follows the source-manifest Active/Sealing/Sealed state
machine. It cannot install two active WALs or reopen an old segment for append
after durable sealing intent. Existing readers reject v7; switching to Leader
does not make v7 readable by an older binary. Binary downgrade requires a
compatible backup/restore path, not a scheduler toggle.

### 7.3 Bounds and reservations

The 1 GiB WAL cap and configured `max_segment_bytes` bound the whole physical
image: header, every DATA/control frame, terminal marker, and rounded
preallocation. `max_unarchived_bytes` retains its meaning as WAL logical bytes;
v7 includes its embedded markers. Actual filesystem allocation, outstanding
reservations, and temporary/pinned images remain charged under the hard
`max_source_spool_bytes` bound. Report DATA, marker, and recovery workspace
bytes separately in status.

Before assigning a ticket, each batch reserves its fragmented DATA range and
one marker frame's worst-case physical allocation and logical capacity.
The allocator consumes marker reservations when placing coalesced frontiers;
release unused reservations only when the covered protocol completes.
A 4 KiB DATA batch can temporarily require another 4 KiB marker reservation.
Coalescing reduces actual marker bytes but does not remove admission pressure
until reservations are safely released. The final marker must fit without
a fresh allocation at full capacity.

Recovery installation additionally requires temporary space for the largest
Active or Sealing image that must be copied. Maintain a shared maintenance
workspace reservation sufficient for that worst-case image's filesystem
allocation, growing it before admission or preallocation increases the bound.
Recovery processes replacements sequentially and may reuse the workspace only
after old/orphan images are durably cleaned and accounting is released.
This can reserve roughly another full active image; it is a recovery-space
cost of the fresh-inode protocol, separate from marker bytes.
A fully occupied spool cannot make recovery depend on unreserved space.

Per-segment/maintenance headroom also includes generation zero, successor
creation, seal/index temporary files, and terminal manifest work. Configurations
unable to represent one admissible batch plus marker, recovery workspace, and
successor/terminal headroom MUST reject new v7 enablement/migration explicitly.
For a healthy v5/v6 database, migration preflight failure instead defers the
upgrade and retains legacy Leader service as specified in section 7.2.
Insufficient workspace for an already installed v7 recovery fails reopen;
it cannot trigger a legacy downgrade. Migration cannot silently enlarge
persisted limits. Count/capacity exhaustion triggers rotation before admission.
Physical reservation release follows existing allocation verification, durable
cleanup, and pin-release rules.

## 8. Implementation and default activation

Implement in reviewable stages:

1. Add the format registry, parameterize framing/data-start/digest/runtime
   startup, and preserve the complete legacy behavior matrix.
2. Add the v7 DATA/FRONTIER codec, discovery/validation, durable recovery
   replacement, creation protocol, and deterministic persistence model with a
   serial I/O driver. Qualify the crash matrix and Active fallback contract
   before connecting Parallel.
3. Integrate ordered logical/physical hashes, seal-index snapshots, the
   single-sync coordinator, marker/recovery reservations, fixed sync/close/
   freeze cutoffs, and async ownership.
4. Extend Sealing intent, archive/catalog verification, restore, backup,
   repair, logical Active anchors, and crash-safe v5/v6-to-v7 migration with
   capacity preflight and writable legacy deferral.
5. After correctness coverage is complete, switch the current PITR writer to
   v7 and activate Parallel in every eligible default constructor and successor
   path, preserving section 7.2's legacy capacity deferral. Update
   current-behavior docs and record the new adoption measurements.

Intermediate commits may expose v7 through a development selector.
A released default writer must support Parallel or name the accepted format
exception from sections 1 and 3. PITR v7 must not depend on Leader for
correctness. Future format RFCs must specify their recovery/frontier/digest
compatibility before activation.

## 9. Required validation and performance evidence

Correctness is an activation requirement. Cover at least:

- Default-mode assertions for constructors, reopen/repair, PITR enable/resume,
  ordinary/PITR rotation, and explicit Leader controls.
- Legacy v2/v3/v4/v5/v6 fixtures and mixed-version archive chains; empty
  segments, timestamp gaps, backward wall-clock movement, TTL bytes, canonical
  point/range ordering, and serializable transactions.
- DATA fragmentation, boundary lengths, forged marker-shaped value bytes,
  noncanonical/foreign frames, overflow checks, multi-SQE groups, and physical
  allocation races between DATA and FRONTIER, including a marker separating
  two DATA ranges in one group without being overwritten by packing.
- FRONTIER physical-prefix containment, exact generation increments, strict
  ticket/end/timestamp monotonicity, and predecessor digest binding. Recompute
  CRCs on malformed fixtures so rejection exercises the explicit invariants.
  Cover prefixes ending with an earlier FRONTIER and the stopped-admission
  `D0 | D1 | F_A(D0) | F_B(D0,D1)` drain without a new ticket. Exercise
  ineligible targets that would retain speculative DATA and terminal failure
  of unacknowledged waiters when poison prevents an eligible marker.
- Out-of-order groups; short/negative/stale/missing CQEs; partial/ambiguous
  submission; an earlier healthy prefix finishing while a later group fails;
  buffer ownership and poisoned close.
- Crashes after every creation/migration, marker write, WAL sync, publication,
  copy, replacement/directory sync, seal, and manifest transition. Model
  persistence of C without B and a complete marker without covered DATA.
  Process kills alone cannot simulate every allowed persistence order; use
  a storage model or controlled fault injection.
- Backward discovery across holes, speculative DATA, partial frames, zeros,
  and maximum preallocation. Torn final markers, invalid chains/pointers,
  bad covered DATA, and explicit fallback to older valid candidates.
  Verify that no acknowledged batch is lost under the stated sync guarantees.
- Hard manifest/Sealing floors, conflicting seals, and strict immutable
  validation; no fallback on read errors or below known durable boundaries.
  Include post-sync damage cases documenting Active v7's detection limits.
- Logical Active anchors surviving marker relocation/rechaining and repeated
  normalization. Reject wrong identity/incarnation, ticket, physical-prefix
  end, timestamp, or logical digest, including a higher candidate that does
  not verify an anchored prefix. Sealing/Sealed references must reject any
  physical image or terminal-marker mismatch and prohibit normalization.
- Recovery after a failed sync with cached complete DATA/FRONTIER and clean
  page state: force fresh writes, including the unchanged-length case.
  Fail copy/sync/replace/directory-sync and crash at each recovery step.
  No service/publication before the installation barrier; repeat recovery
  must retain the certificate and stable ticket/physical-offset allocation.
- Archive/restore logical/physical digest and index agreement, v7 empty seal
  length, recovery-point/checkpoint boundaries, concurrent freeze/close/
  cancellation, and pinned image cleanup.
- Tiny limits, simultaneous admissions, maximum batches/frame counts,
  seal-index exhaustion, marker reservations, recovery temporary/orphan
  allocation, and no full-spool maintenance/reopen deadlock.
  Tests arming failpoints follow the repository's `failpoint_*` rule.
- Insufficient legacy persisted capacity: successful reopen, continued PITR
  writes, v6 Leader rotations, and visible migration-deferral reasons. Cover
  the 1 GiB/1.3 GiB case, maximum-image rather than current-length preflight,
  pinned/orphan obligations, and all transition reservations. Verify eligible
  migration after an explicit RFC 023 capacity transition, rejection of
  insufficient new v7 enablement before durable intent, no implicit limit
  overrides, and no legacy downgrade after durable Sealing intent or v7
  installation. Missing workspace for installed v7 must fail reopen.

The normal protocol adds a marker write but retains **one** WAL sync per
durability advance. Physical framing, two hash streams, marker allocation,
recovery copying, and contention still have costs. Ordinary v4 Parallel is
the current default without PITR and MUST be included as a baseline.
Retain all of these comparisons:

| Comparison | Purpose |
| --- | --- |
| Baseline v4 Parallel vs candidate v4 Parallel, PITR off | Detect regressions in ordinary reads, writes, and mixed workloads caused by shared runtime/format-registry changes. |
| Candidate v7 Parallel, PITR on, vs candidate v4 Parallel, PITR off | Measure the total cost of enabling PITR relative to the ordinary default. |
| Candidate v7 Parallel vs candidate v7 Leader, PITR on | Measure scheduling and group-overlap benefits with the same format and durability protocol. |
| Candidate v7 Leader vs candidate v6 Leader, PITR on | Measure format/frontier-protocol cost with Leader scheduling in both legs. |
| Candidate v7 Parallel vs baseline v6 Leader, PITR on | Measure the end-to-end PITR upgrade against the currently shipped PITR path. |

The v7-versus-v4 comparison includes PITR recorded times, fragmentation,
logical/physical hashes, indexing, markers, and archive work when active.
Report total PITR overhead; it cannot isolate marker cost or establish a
parallel-scheduling gain. Specify archive activity and retain controlled
cases with and without competing archive I/O. Ordinary v4 keeps its current
protocol and receives no v7 frames or recovery-copy requirement.

Use the same candidate binary and explicit modes for comparisons within it,
plus a recorded baseline revision for comparisons across revisions.
The harness may retain an explicit v6 creation selector in isolated databases;
production writers follow v7 adoption policy. Never reinterpret a file's
version to manufacture a benchmark leg. Run on tmpfs and a named physical
device with 1/4/8/16 writers, single puts/batch64, small/large values, rotation,
checkpoints, and archive backpressure. Include point reads, scans, and mixed
read/write ratios in ordinary regression checks and PITR contention runs.

Retain a zero-added-wait coalescing control and bounded-wait candidates with
exact deadlines and batch/byte limits. Measure one-writer device latency
separately from concurrent throughput and retain p50/p99 regressions even when
throughput improves. Tmpfs alone cannot qualify physical-device latency;
one sync per advance does not establish parity with v4 or v6.

Report throughput, p50/p99 latency, group occupancy, measured group overlap,
batches/groups per frontier, coalescing wait, marker-write and WAL-sync
counts/latencies, DATA/marker bytes per batch, CPU/hash cost, buffer/spool
peaks, and rotation pause. Report reserved marker/workspace bytes alongside
actual allocated bytes and admission rejections from each reservation.
Measure backward discovery bytes/latency, recovery-copy bytes/latency, peak
temporary/orphan allocation, and replacement syncs separately from normal
commit latency. A fresh-inode recovery can copy nearly a full active WAL;
this cost and its reserved workspace must be visible.

Retain paired runs, confidence intervals, failed runs, controls, binary
revisions, device/kernel details, and command lines. Outstanding SQEs are not
a claim about physical device queue depth.

RFC 024's original performance gate remains unqualified; historical results
do not qualify v7. Parallel is the architectural default for new formats after
correctness qualification, with no universal speedup claim. Material measured
regressions require an explicit maintainer adoption decision and documented
results before activation, following the
[existing adoption note](../docs/wal/rfc-024-parallel-wal-default-20261005.md).

## 10. Alternatives considered

- **Enable Parallel on v5/v6 without a format change.** Their scanners cannot
  distinguish interrupted overlap from interior corruption. Preserve those
  formats and introduce v7 with an explicit recovery contract.
- **Two serial barriers with a side frontier journal.** Syncing DATA before
  submitting a marker lets a complete marker imply that covered DATA was
  previously synced. It retains stronger Active corruption classification
  under the storage assumptions but adds a second serial sync, another file
  lifecycle, and journal reservations. RFC 025 chooses one WAL barrier with
  prefix-validated candidate fallback instead.
- **Accept an in-WAL marker from its CRC alone.** A failed/interrupted sync may
  persist the marker first. Validate all covered DATA and the logical digest
  before acceptance; any durable external floor still applies.
- **Use a forward DATA scanner to discover markers.** It cannot reach a
  frontier behind an earlier hole. Independent aligned discovery is required.
- **Scan raw aligned payload for magic without typed DATA frames.** User bytes
  can resemble a marker. Codec-owned frame headers disambiguate physical
  control records.
- **Truncate the original WAL to the accepted prefix end immediately.** This
  erases the selected boundary's certificate before another crash; a retained
  predecessor certifies only an older boundary. The initial protocol
  replaces a synced fresh image; its recovery latency and workspace cost are
  explicit. An alternative that retains old certificates or avoids full copying
  needs its own failure/writeback proof and crash qualification.
- **Overwrite alternating frontier pages.** This bounds metadata size, but
  introduces overwritten-generation and stale-slot recovery rules.
  Initial v7 keeps normal marker writes append-only and reserves their growth.
- **Keep Leader as the permanent PITR default.** Leader remains an explicit
  comparison mode with the same v7 protocol; new defaults use Parallel.
