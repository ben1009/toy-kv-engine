# RFC 025: Parallel WAL by Default for All New Formats

| Field | Value |
| --- | --- |
| Status | Proposed |
| Date | 2026-10-08 |
| Author | kv-engine Contributors |
| Builds on | [RFC 024](024-dedicated-wal-pipeline.md), [RFC 023](023-point-in-time-recovery.md) |

## 1. Summary

Every WAL format introduced after this RFC MUST support the dedicated,
ticket-ordered parallel pipeline and select `WalIoMode::Parallel` by default.
This includes new PITR formats. Parallel support is a requirement for shipping
a new writer format.

The first extension is PITR WAL **v7**. It adds persistent batch tickets and
an append-only durable-frontier journal, allowing recovery to distinguish
data covered by a durable boundary from an unfinished out-of-order suffix.
The coordinator syncs the covered WAL data, writes and syncs the boundary,
then publishes durability. Group submission remains parallel throughout.

Existing files keep their format-specific recovery semantics: ordinary v4
already defaults to Parallel; PITR v5/v6 and older MVCC files retain Leader;
unframed files retain buffered I/O. New PITR segments use v7 after a durable
rotation. This is a proposed format and adoption policy; the current writer
still creates PITR v6. WAL remains optional through `enable_wal`.

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
| PITR v5/v6 | Leader | Preserve strict recovery and each version's digest rule; seal before installing a v7 successor. |
| PITR v7 | Parallel | Use the prefix-journal protocol specified here. |
| Future registered formats | Parallel | Their format RFC must define and qualify recovery under out-of-order writes. |
| Unknown versions | Reject | A higher version number is not evidence of compatibility. |

Replace scattered `version == 4` runtime checks with a checked format
descriptor shared by creation, reopen, encoding, recovery, sealing, and
restore. It supplies framing/alignment, data-start offset, digest rule,
recovery policy, and supported I/O modes. A new writer descriptor cannot be
registered without Parallel support and its recovery coverage. Readers still
recognize only explicitly supported versions and reject unknown flags.

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

### 4.1 WAL header and batches

The v7 WAL keeps a 4096-byte big-endian file header and 4096-byte batch
alignment. WAL format versions are independent of source-manifest versions.
Header bytes `0..128` retain the v5/v6 identity and predecessor
layout from RFC 023, with version `7` and flags `0`. Bytes `128..144` contain
a fresh, nonzero 128-bit segment incarnation; bytes `144..148` contain CRC32
over `0..144`; all remaining header bytes are zero. An incarnation is created
once and prevents a frontier file from being reused for a replacement WAL.
All digest preimages in this RFC use raw bytes without text encoding.

Each batch has this 48-byte header:

```text
segment_ticket:u64 | commit_ts:u64 | recorded_at_secs:i64 |
recorded_at_nanos:u32 | entry_count:u32 | data_len:u32 |
data_crc32:u32 | header_crc32:u32 | reserved:u32
```

`header_crc32` covers bytes `0..40`, including `data_crc32`; `reserved` is zero.
Payload encoding, canonical mixed-operation ordering, nonempty-batch rules,
recorded-time validation, and zero padding follow RFC 023. Tickets are
segment-local ordinals `0, 1, ...`; commit timestamps are strictly increasing
but need not be numerically consecutive. Tickets never substitute for MVCC
timestamps or range-operation ordinals.

The v7 prefix digest is SHA-256 over the full immutable WAL header followed
by each covered batch header and payload in ticket order. Alignment padding
is excluded from the hash, following v6, but is still validated as zero.
The covered `logical_end` includes each batch's padding and excludes the
preallocated tail. An empty prefix ends at offset 4096.

### 4.2 Active frontier journal

Every active v7 WAL has a required `<segment>.frontier` companion. It has a
4096-byte header followed by fixed 4096-byte records. All fields are big-endian;
unknown versions, nonzero reserved bytes, invalid lengths, and arithmetic
overflow reject. CRC32 uses RFC 023's `crc32fast` definition.

The journal header fields, in order, are:

```text
magic[8] = "TKVPFX01" | version:u16 = 1 | header_len:u16 = 4096 |
record_len:u32 = 4096 | wal_header_digest[32] |
timeline_id[16] | archive_epoch_id[16] | segment_id:u64 |
incarnation[16] | header_crc32:u32 | zero padding to 4096
```

`wal_header_digest` is SHA-256 of the complete v7 WAL header. `header_crc32`
covers the preceding 104 bytes. All identities must match the WAL header.

Each frontier record has these fields, in order:

```text
magic[8] = "TKVPFXR1" | version:u16 = 1 | reserved:u16 = 0 |
wal_header_digest[32] | generation:u64 | ticket_end:u64 |
logical_end:u64 | last_commit_ts:u64 | prefix_digest[32] |
previous_record_digest[32] | record_crc32:u32 | zero padding to 4096
```

`record_crc32` covers the preceding 140 bytes. The record digest is SHA-256
of the complete canonical 4096-byte record, including checksum and padding.
`ticket_end` is exclusive and equals the covered batch count. Generation zero
covers the empty WAL header: `ticket_end = 0`, `logical_end = 4096`,
`last_commit_ts = 0`, `prefix_digest = wal_header_digest`, and an all-zero
previous-record digest. Subsequent records increment generation by one, bind
the preceding record digest, and strictly increase ticket end, logical end,
and last commit timestamp. Every end must coincide with a complete batch end.

There is at most one frontier-record write/sync in progress per WAL. Records
are appended serially and never overwrite previous records. The journal is
not preallocated, so an interrupted append cannot create nonzero later
records after a torn record. At most one nonempty record is added per batch;
with generation zero and the header, journal logical bytes are bounded by
`8192 + 4096 * batch_count`. Coalescing normally uses fewer records.

Checksums and the hash chain detect accidental damage; they provide no keyed
authentication. The source manifest and archive catalog retain their existing
authority; a file copied from another segment cannot supply its frontier
merely because its byte offsets look plausible.

## 5. Admission and durability

### 5.1 Admission and ordered metadata

Preparation reserves buffer memory, aligned WAL capacity, PITR spool space,
seal-index capacity, and worst-case frontier-record allocation before ticket
assignment. Rejection creates no ticket or offset hole. Admission atomically
assigns the ticket and nonoverlapping physical range and publishes the batch
to the ordered queue, following RFC 024's handoff rules.

The ordered packer finalizes the recorded-time clamp and v7 batch header,
then incrementally advances the prefix hash and seal index in ticket order.
Hash/index state for prepared or written tickets is tentative. Pending group
boundaries and fixed barrier cutoffs retain bounded digest snapshots and index
ends, including a cutoff inside a group. Sealing and publication can access
only the state covered by the durable frontier.
No payload rescan is permitted in the stop-admission rotation section.

### 5.2 Two ordered sync stages

Let `T` be a captured exclusive ticket end no greater than the contiguous
written frontier, and `E(T)` its aligned byte end. The coordinator:

1. Confirms full write completion for every ticket below `T`. Short writes,
   missing CQEs, or ambiguous submission cannot count as completion.
2. Calls `fdatasync` on the WAL and waits for success.
3. Appends one frontier record for exactly `(T, E(T), digest(T))`, using the
   saved ordered metadata, and verifies the full record write.
4. Calls `fdatasync` on the frontier journal and waits for success.
5. Publishes the durable ticket end and corresponding seal metadata with the
   existing synchronization rules, then wakes covered waiters.

Writers may submit later groups while either sync runs. Incidental persistence
of later WAL bytes does not extend the captured boundary. A marker cannot name
bytes admitted during the WAL sync unless a later WAL sync covers them.
Normal coalescing may choose `T` before step 2; public sync, freeze, checkpoint,
recovery-point, and close barriers retain their fixed admission cutoffs.

The ordering relies on successful file syncs and explicitly synced directory
publication, as described by
[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html). Syncing a new WAL
and journal is insufficient to publish their directory entries: creation must
sync both headers and generation zero, sync the directory, then durably install
the active pair in the source manifest before any batch can be acknowledged.

After both sync stages, the existing memtable insertion and ordered MVCC
publication steps still precede API success. Native async waiters wait through
both sync stages and publication. Cancellation after admission leaves owned
batch resources and memtable leases alive until the commit protocol retires
them; PITR backpressure never waits for archival while holding admission locks.

### 5.3 Errors and ownership

A failed WAL sync cannot create a new frontier record. A short frontier write
or failed frontier sync cannot advance the live durable frontier. These errors
poison new admission; reopen reconciles their unknown outcome. A complete
record may survive even if its sync returned an error or the client saw no
success, so recovery may include that complete, safely data-synced prefix.

An earlier durable boundary remains valid when a later group fails. A healthy
contiguous prefix below the earliest failed ticket may complete both sync
stages, following RFC 024's poison rules; no failed or uncertain group is
crossed. Buffers remain owned until terminal CQEs or proven safe teardown.
Acknowledged outcomes are never changed by a later failure.

## 6. Active recovery and corruption rules

Recovery first validates the WAL header, frontier header, identity binding,
and mandatory generation-zero record. Missing or mismatched frontier metadata
for a manifest-installed Active v7 segment is an error, including for an empty
segment. Only a pair never installed in durable manifest state can be treated
as incomplete creation and cleaned up through the normal orphan protocol.

Recovery scans the journal serially, checking record checksums, generation,
previous-record hashes, increasing boundaries, and format limits. A short,
zero-filled, or checksum-invalid **final append** may be discarded only when
EOF falls within or exactly at the end of that one record slot. Any bytes
after an invalid slot make it interior corruption, including extra zero pages;
the journal has no preallocated tail. A complete checksummed record with
an invalid identity, generation, chain, or boundary is corruption even at the
tail. Generation zero cannot be discarded. The journal's append ordering is
essential to these rules.

In one streaming WAL pass, recovery verifies every covered batch, exact ticket
order, CRC, canonical operation encoding, zero alignment padding, timestamp
and recorded-time ordering, and the digest/count/timestamp at each accepted
journal boundary. Any mismatch inside the final covered prefix is fatal;
recovery cannot fall back to an older marker to hide damaged covered data.
Persisted sealing intent or manifest high-water state requiring a newer
boundary also makes an older recovered boundary an error.

All WAL bytes after the accepted boundary are unacknowledged suffix bytes.
They may contain valid later batches or holes and are discarded together;
they are never replayed or archived independently. Before append resumes,
recovery durably truncates the WAL to `logical_end` and the journal to its
accepted record end, with all old I/O ownership retired. The next v7 ticket
is the recovered `ticket_end`. MVCC timestamps and the recorded-time clamp are
reconstructed from retained batches and existing manifest anchors.

For v7, a durable commit coordinate requires a complete batch covered by an
accepted frontier record. A complete but unmarked batch has never reached
MVCC publication and does not by itself consume a permanent coordinate after
restart. This refines RFC 023's timestamp-reconciliation rule for v7 only;
existing v5/v6 coordinates and all persisted manifest high-water anchors keep
their current meaning. Never reuse a timestamp required by those anchors.

| Crash point | Recovery result |
| --- | --- |
| A and C written, B incomplete, no newer marker | Replay the previous marked prefix; discard the unmarked suffix. |
| WAL sync succeeds, marker not written | Replay the previous marked prefix. |
| Final marker append is torn | Replay the previous marked prefix after validating the journal tail. |
| Complete marker reaches storage before acknowledgement | Replay its complete covered prefix; the client outcome may have been unknown. |
| Marker sync and publication complete | Every acknowledged batch is within the recoverable prefix. |
| Covered WAL data or an interior journal record is damaged | Report corruption; do not truncate through it. |
| Wrong or missing frontier for a manifest-installed active WAL | Report corruption; do not reconstruct acknowledgement from later valid batches. |

The guarantee assumes successful syncs preserve their covered bytes and
directory entries across a crash. CRCs/hashes detect damage in an intact
marked prefix; they cannot prove acknowledgement if storage later erases or
rolls back the newest synced frontier records themselves. As with an
interrupted final batch in existing WAL recovery, such loss can be
indistinguishable from an unfinished append. This RFC does not claim repair
of post-sync storage loss. `open_repairing` must preserve these boundaries and
report a conflicting manifest or seal rather than silently lowering it.

## 7. PITR integration and migration

### 7.1 Freeze, seal, archive, and backup

Freeze stops admission, drains blocking guards and async memtable leases,
completes the two-stage frontier through its cutoff, and drains ordered
publication. Sealing captures that exact boundary, finalizes the saved digest
and index, and durably records `Sealing` intent before installing a successor.
An empty segment still has a real header, generation-zero frontier, and
predecessor anchor; it does not invent a commit coordinate.

Seal sidecar v1 can retain its existing byte layout with an explicitly
registered `wal_format_version = 7`. Its WAL digest uses the v7 rule, its batch
count equals the frontier ticket end, and its index must match those batches.
The WAL digest also binds the incarnation in the v7 header. No v5/v6 digest
interpretation is changed. Recovery of a fixed `Sealing` obligation validates
the frontier against the intent and any durable seal; the journal remains
required until `Sealed` is durable. Recovery cannot promote an unmarked suffix
when rebuilding a seal.

Once the seal is durable and `Sealed` is durable in the source manifest, the
immutable WAL/seal pair supplies the recovery boundary. The active journal may
be deleted only after that transition and all journal readers release their
pins; deletion and directory sync precede accounting release. Before this
transition it remains required, even if a seal temporary exists.

Recovery obtains Active/Sealing/Sealed status from durable source-manifest
state or a verified backup/catalog reference. Merely finding a seal sidecar
does not authorize treating an Active WAL as sealed and bypassing its journal.

Archive objects remain the exact WAL prefix `[0, logical_end)` and its seal;
the active frontier journal is not a third archive object. Archived v7 objects
are verified strictly to the seal's length, count, index, identity, predecessor,
and digest. Restore does not apply active-tail truncation to immutable objects.
Checkpoint/backup capture uses the drained sealed boundary. Any helper that
copies an Active v7 WAL must capture its frontier consistently and include it
in the recoverable artifact; copying the WAL alone is insufficient.

### 7.2 Existing databases

Reopen reads v5/v6 with their current strict scanners and digest rules. Before
accepting a new PITR write, it completes recovery and sealing of the old active
segment using Leader, installs a v7 successor durably, then starts Parallel.
The successor names the predecessor's actual WAL/seal digests, allowing mixed
v5/v6/v7 archive chains without rewriting old bytes or beginning a new epoch.
Enablement on an ordinary database retains RFC 023's epoch/base requirements.

Interrupted migration follows the source-manifest Active/Sealing/Sealed state
machine. It cannot install two active WALs or reopen an old segment for append
after durable sealing intent. Existing readers reject v7; switching to Leader
does not make v7 readable by an older binary. Binary downgrade requires a
compatible backup/restore path, not a scheduler toggle.

### 7.3 Bounds and reservations

The existing 1 GiB WAL cap and configured `max_segment_bytes` apply to the
aligned WAL prefix, including its header. `max_unarchived_bytes` retains its
RFC 023 meaning as WAL logical bytes. The journal is charged separately under
the hard `max_source_spool_bytes` bound, including actual filesystem allocation
and outstanding reservations. Report journal bytes separately in status.

Each batch reserves at least one journal record's worst-case physical
allocation before receiving a ticket. A coalesced boundary releases unused
record reservations only when the covered protocol completes. Per-segment
and maintenance reserves include the journal header, generation zero,
successor creation, seal/index temporary files, and terminal manifest work.
No sync or final seal may need an unreserved allocation at a full spool.
Journal growth is additionally bounded by batch count and the formula in
section 4.2; capacity/count exhaustion triggers rotation before admission.

Configurations that cannot represent the v7 empty segment, one admissible
batch, and successor/terminal headroom fail explicitly. Migration does not
silently enlarge persisted limits. Release of physical reservations continues
to require the existing allocation verification and durable cleanup rules.

## 8. Implementation and default activation

Implement in reviewable stages:

1. Add the format registry, parameterize encoding/alignment/runtime startup,
   and preserve the complete legacy behavior matrix.
2. Add the v7 codec, frontier journal, creation protocol, strict boundary
   recovery, and a deterministic durability-state model.
3. Integrate ordered hash/index snapshots, the two-stage coordinator, spool
   reservations, sync/close/freeze cutoffs, and async ownership.
4. Extend sealing, archive/catalog verification, restore, backup, repair, and
   crash-safe v5/v6-to-v7 migration.
5. After correctness coverage is complete, switch the current PITR writer to
   v7 and activate Parallel in every default constructor and successor path.
   Update current-behavior docs and record the new adoption measurements.

Intermediate implementation commits may expose v7 only through a development
selector. A released default writer must not create a new format that still
depends on Leader for correctness. Future format RFCs inherit this rule and
must specify their recovery/frontier/digest compatibility before activation.

## 9. Required validation and performance evidence

Correctness is an activation requirement. Cover at least:

- Default-mode assertions for all constructors, reopen/repair, PITR
  enable/resume, ordinary and PITR rotation, and explicit Leader controls.
- Legacy v2/v3/v4/v5/v6 fixtures and mixed-version archive chains, including
  empty segments, timestamp gaps, backward wall-clock movement, TTL bytes,
  canonical point/range ordering, and serializable transactions.
- Out-of-order groups and multi-SQE groups; short/negative/stale/missing CQEs;
  partial or ambiguous submission; errors while an earlier healthy prefix
  finishes; buffer ownership and poisoned close.
- A crash after every creation, migration, data-sync, marker-write,
  marker-sync, publication, seal, journal deletion, and manifest transition.
  Model crashes that preserve C but not B. A process kill alone cannot simulate
  every allowed persistence order; use a storage model or controlled fault
  injection in addition to process tests.
- Torn trailing versus damaged interior journal records; valid-checksum
  malformed markers; foreign/stale journals; corruption before/after the
  marked WAL boundary; no marker-based downgrade of a covered corruption.
- Recovery followed by append using the recovered ticket ordinal, repeated
  recovery/truncation, both sync failures, and unacknowledged complete markers.
- Archive/restore digest and index agreement, strict sealed-length checks,
  recovery-point/checkpoint boundaries, concurrent freeze/close/cancellation,
  and immutable-object pins during frontier cleanup.
- Tiny configured limits, simultaneous admissions, maximum batches, seal-index
  exhaustion, journal allocation overhead, and no full-spool maintenance
  deadlock. Tests arming failpoints follow the repository's `failpoint_*` rule.

The new protocol normally adds a frontier write and a second file sync per
durability advance. Its cost must be visible. Ordinary v4 Parallel is the
current default without PITR and MUST be included as a baseline. Retain all
of these comparisons:

| Comparison | Purpose |
| --- | --- |
| Baseline v4 Parallel vs candidate v4 Parallel, PITR off | Detect regressions in ordinary reads, writes, and mixed workloads caused by shared runtime/format-registry changes. |
| Candidate v7 Parallel, PITR on, vs candidate v4 Parallel, PITR off | Measure the total cost of enabling PITR relative to the ordinary default. |
| Candidate v7 Parallel vs candidate v7 Leader, PITR on | Measure scheduling and group-overlap benefits with the same format and durability protocol. |
| Candidate v7 Leader vs candidate v6 Leader, PITR on | Measure the format/frontier-protocol cost with Leader scheduling in both legs. |
| Candidate v7 Parallel vs baseline v6 Leader, PITR on | Measure the end-to-end PITR upgrade against the currently shipped PITR path. |

The v7-versus-v4 comparison includes PITR framing, recorded times, hashing,
seal indexing, frontier persistence, and archive work when active. Report it
as total PITR overhead; it cannot isolate the second sync or establish a
performance gain from parallel scheduling. Specify archive activity for each
run and retain separate controlled cases with and without competing archive
I/O. Ordinary v4 stays on its existing protocol and incurs no journal sync.

Use the same candidate binary and explicit modes for comparisons within the
candidate, plus a recorded baseline revision for comparisons across revisions.
The benchmark harness may retain an explicit v6 creation selector in isolated
databases; production writers still follow the v7 adoption policy. Never
reinterpret a file's version to manufacture a benchmark leg. Run on tmpfs and
a named physical device with 1/4/8/16 writers, single puts and batch64, small
and large values, rotation, checkpoints, and archive backpressure. Include
point reads, scans, and mixed read/write ratios in the ordinary regression
checks and in measurements of contention while PITR is active.

Report throughput, p50/p99 latency, group occupancy, measured overlap between
groups, both sync counts/latencies, marker bytes per batch, CPU, buffer/spool
peaks, and rotation pause. Retain paired runs, confidence intervals, failed
runs, controls, binary revisions, device/kernel details, and command lines.
Software outstanding SQEs are not a claim about physical device queue depth.

RFC 024's original performance gate remains unqualified; its historical
benchmarks do not qualify v7. This RFC makes Parallel the architectural default
for new formats after correctness qualification, without claiming a universal
speedup. Material measured regressions require an explicit maintainer adoption
decision and documented results before activation, following the
[existing adoption note](../docs/wal/rfc-024-parallel-wal-default-20261005.md).

## 10. Alternatives considered

- **Enable Parallel on v5/v6 without a format change.** Their recovery scanner
  and seal builder cannot distinguish interrupted overlap from interior
  corruption. Preserve those formats and introduce v7.
- **Stop at the first invalid v7 batch without a persisted boundary.** This
  permits ordinary prefix recovery but cannot enforce the stricter PITR
  validation of the recorded durable prefix.
- **Write data and its frontier before their durability barrier.** A crash may
  persist the boundary before the covered data, even if the marker is embedded
  in the WAL and both are later covered by one sync. The WAL sync must succeed
  before the marker is submitted; reducing the two stages needs a separately
  justified ordering protocol.
- **Overwrite alternating frontier pages.** This bounds metadata size, but
  recovery must reason about overwritten generations and stale fallback.
  The initial design keeps previous boundaries immutable and charges the
  resulting journal growth explicitly.
- **Keep Leader as the permanent PITR default.** This retains the current
  format restriction and prevents new formats from sharing the pipeline.
  Leader remains an explicit comparison mode on v7 with the same safety rule.
