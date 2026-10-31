# RFC 026 Implementation Plan

This plan fixes the initial v8 wire identity before any v8 WAL bytes are
written. It stages implementation so digest primitives and codecs can be
reviewed before they are connected to WAL creation, recovery, or migration.

## Fixed v8 identity

- WAL header magic: the existing four bytes `WAL2`.
- WAL version: unsigned 16-bit big-endian value `8` at bytes `[4, 6)`.
- Flags at `[6, 8)` and the reserved 16-bit field at `[10, 12)`: zero.
- Header length: 4096 bytes, encoded at `[8, 10)`. Timeline, epoch, segment,
  and predecessor identity fields retain v7 offsets `[12, 128)`. The nonzero
  incarnation is at `[128, 144)`, the big-endian CRC32 of `[0, 144)` is at
  `[144, 148)`, and `[148, 4096)` is zero-filled.
- Frame size and alignment: 4096 bytes. The initial generation-zero FRONTIER
  remains at offset 4096, and DATA allocation begins at offset 8192.
- Frame magic: the eight bytes `TKVW8FR1`; frame version `1`; DATA kind `1`;
  FRONTIER kind `2`. Frame field offsets, CRC coverage, payload capacity,
  fragment header and frontier body layout otherwise follow RFC 025.
- Logical batch header: 48 bytes, with the RFC 025 v7 byte layout and
  big-endian multi-byte integers.
- Digest scheme: WAL version 8 selects exactly RFC 026's unkeyed BLAKE3-256
  entry-stream, batch and ordered-prefix chain. There is no independent
  on-disk scheme selector; an unknown version must be rejected.
- Persisted archive descriptor: v8 uses `seal_format_version = 3` with the
  same immutable-boundary fields as the v7 catalog descriptor. Its sidecar
  identity is `TKVSEAL3` / version `3`; archive publication remains disabled
  until that sidecar is strictly verified against the frozen WAL image.
- Physical header binding remains `SHA256(W)` in frame headers. The logical
  digest seed remains `BLAKE3(HEADER || W)`. Existing v7 FRONTIER, seal,
  archive and object digest meanings remain unchanged.
- V8 starts with a distinct empty-prefix digest `P_0` and an empty generation
  zero frontier. Persisted anchor encodings must carry/derive the WAL format
  version before they can interpret a prefix digest; v7 anchor bytes retain
  their current SHA-256 meaning.

These identifiers are implementation decisions for the proposed v8 format,
not a change to existing files. The writer remains disabled until header,
frame, recovery, persisted metadata, migration and compatibility paths all
dispatch on version 8.

The v7 codec currently equates the physical header digest with the
generation-zero FRONTIER prefix and reconstructs that frame when validating its
first successor. V8 must instead validate `SHA256(W)` as the frame header's
physical binding and `P_0` as the FRONTIER's logical prefix, including when
reconstructing or linking generation zero. The v7 helpers cannot be reused
unchanged for those checks.

## Old-reader compatibility gate

An older binary tested at `18c5f0a8ccfb` can rewrite `MANIFEST` during
recovery before checking its numeric format version. It can also create the
WAL open-lock file and open the value log before parsing the manifest. A
version-8 WAL header or a higher manifest version alone therefore does not
establish RFC 026's rejection-before-mutation requirement. The old binary
rejected unknown top-level enum records and snapshot wrappers, while ignoring
an unknown field added to a known record.

The guard publisher now syncs the existing v7 WAL open-lock file and the
`vlog/` directory (and the database directory entry) before publishing a
manifest guard. It syncs the unknown `MANIFEST` marker before creating the
generation-wrapped `ENGINE_MANIFEST.tmp`; otherwise a temp wrapper could be
mistaken for an authoritative snapshot while the manifest is empty, before
older readers are blocked. It then installs the matching wrapper and compacts
the log to that generation. Guarded snapshot recovery cuts the old log at the
matching generation and preserves records after that cut. Focused tests
exercise generation cuts, missing `MANIFEST`, process interruption at each
publication boundary, and rejection of the new JSON variants by a pre-v8
enum. Opt-in process tests also run the historical CLI at
`18c5f0a8ccfb` against guarded Active v7 publication states, a durable v7-to-v8
migration intent, a migration stopped after `SegmentSealed` but before
successor registration, and a completed v8 migration with a synced v8 WAL
record. They verify rejection without changing database entries, including the
database root's metadata, and check that the error identifies a v8 manifest
variant. Re-run the full process matrix with:

```bash
TOY_KV_PRE_V8_CLI=/path/to/kv-engine-cli \
TOY_KV_PRE_V8_VLOG_CLI=/path/to/vlog-enabled-pre-v8-reader \
TOY_KV_EARLY_PRE_V8_CLI=/path/to/earlier/kv-engine-cli \
TOY_KV_EARLY_PRE_V8_VLOG_CLI=/path/to/vlog-enabled-earlier-reader \
cargo test --package kv-engine --lib --features chaos-testing \
  pre_v8 -- --ignored --test-threads=1
```

The filter runs eighteen process cases: six guard-publication cases covering the
normal `MANIFEST`, guard append, synced `ENGINE_MANIFEST.tmp`, snapshot rename,
missing `MANIFEST` with an installed snapshot, and missing `MANIFEST` with a
pending snapshot; the durable migration-intent, post-`SegmentSealed`, and
completed-v8 cases against the `18c5f0a8ccfb` CLI; a vLog-enabled published-
guard case against a small reader harness built from the same historical
library revision with WAL and value separation enabled; and the published
guard against the earlier historical library with vLog both disabled and
enabled, plus its stock CLI against guard append, synced snapshot temp,
snapshot rename, durable migration-intent, post-`SegmentSealed`, and completed-
v8 cases. The vLog cases verify the older startup paths reject the v8 guard
without touching a real `.vlog` file, even though vLog initialization precedes
manifest parsing.

The earlier historical reader at `3420eb05b23c5b6ddd22694a4a2f84698589655a`
is tested in both configurations against the fully published database. Its
stock CLI is also tested against guard-append, synced-snapshot-temp,
snapshot-rename, durable migration-intent, post-`SegmentSealed`, and completed-
v8 states:

```bash
TOY_KV_EARLY_PRE_V8_CLI=/path/to/earlier/kv-engine-cli \
TOY_KV_EARLY_PRE_V8_VLOG_CLI=/path/to/vlog-enabled-earlier-reader \
cargo test --package kv-engine --lib --features chaos-testing earlier_pre_v8 \
  -- --ignored --test-threads=1
```

Both vLog-enabled harnesses are small binaries built from their respective
historical library sources with WAL and value separation enabled. Their
stock CLIs do not expose a vLog-enabled open configuration.

That earlier reader is qualified only with the durable `MANIFEST` guard
present. A probe with `MANIFEST` absent and either an installed or pending
snapshot showed that this binary initializes a new database instead of
recovering from the snapshot, mutating the directory. Those snapshot-only
states are unsupported corruption cases for that reader and are not reachable
through the guard publication sequence, which durably appends the guard before
installing or replacing `ENGINE_MANIFEST`. The `18c5f0a8ccfb` reader is tested
against both snapshot-only recovery fixtures.

The publisher requires a token holding both PITR lifecycle barriers;
the migration caller must retain it through the v7-to-v8 transition so a
concurrent resume cannot reopen commit admission between guard publication
and WAL migration.

Local probe results (2026-10-31): the six publication-state cases first passed
against a release CLI built offline from `18c5f0a8ccfb` with
`nightly-2026-09-23`. Its
SHA-256 was
`e83ea47c4d385f160a5a28722706c374b25448b1dc31e8fa2a512b91bb92eb97`. The
published-guard case also passed against the earlier CLI built from
`3420eb05b23c5b6ddd22694a4a2f84698589655a`; its SHA-256 was
`15121655cdd8e69eb9d1f260adb7a996dd85933a6a936e05025ac83519fd76a0`. Each
case verified rejection on an unknown manifest variant and unchanged file
contents, entry names, metadata for recursively enumerated entries and
subdirectories, and database-root metadata. In the `3420eb05...` reader's
guard-append and synced-snapshot-temp cuts, that reader rejects the older
`ActiveBoundaryAdvanced` v7 PITR-manifest record before reaching the v8 guard;
the later pre-v8 reader reaches and rejects the guard itself. After adding earlier-
reader publication and migration states and vLog-enabled historical readers,
the expanded eighteen-case process matrix passed. The vLog reader harness for
`18c5f0a8ccfb` was built offline from that revision's library sources with WAL
and value separation enabled; its SHA-256 was
`1417e8107c1faedbde4dcf7ccb5e365168983d8edae8b542cbf90400ae985819`. The
vLog reader harness built from `3420eb05b23c5b6ddd22694a4a2f84698589655a`
with the same options has SHA-256
`d8a7b704246773d23ea3fe2617faca2321176e9036abd61f7c867cbfaedb8b3d`. This
covers only the exercised fixtures and states.

This is partial compatibility qualification. The `18c5f0a8ccfb` reader is
covered for ordinary manifest replay and installed/pending snapshot recovery;
the earlier reader is covered only while its required manifest remains
present. The complete supported historical-binary matrix and startup ordering
remain unqualified, as does the performance matrix, so v8 creation stays
disabled.

## Slices

1. **Digest primitives:** add the BLAKE3 dependency and a standalone v8 digest
   module for header seed, bounded incremental entry hashing, batch digest and
   the fixed-size ordered link. Keep the existing golden vector as the
   conformance target. Do not select v8 in the writer or recovery registry yet.
2. **Wire codec and registry:** implement v8 header, DATA and FRONTIER codecs;
   register version 8 while keeping its reader and writer unavailable. Add
   byte-exact golden vectors for the valid header, frames and frontier, and
   verify the distinct physical header digest and logical `P_0` at generation
   zero and its first successor.
3. **Streaming batch preparation:** reuse canonical operation encoding while
   feeding entry CRC and BLAKE3 from the same emitted byte ranges. Admit and
   publish Parallel tickets before payload encoding, then gate DATA submission
   on independent frame/digest readiness.
4. **Runtime durability and recovery:** add the v8 writer and reader using the
   RFC 025 single-sync frontier protocol, v8 logical digest folding,
   intermediate hard-anchor checks, Active fallback and fresh-inode recovery
   normalization. The MemTable adapter replays a normalized Active image and
   resumes serial appends from its installed ticket and digest boundary.
   High-level LSM startup now validates the manifest-bound Active v8 identity,
   reserves recovery workspace, normalizes and attaches the WAL, and publishes
   updated logical Active boundaries after `sync` and graceful close. A
   high-level reopen test verifies replay, cursor continuity, and anchor
   publication. Recovery keeps a compact per-ticket replay index and decodes
   one batch at a time into the memtable under the reserved workspace, rather
   than retaining a second WAL-sized collection of decoded entries. Active v8
   strict Sealing recovery now verifies the immutable logical boundary, checks
   any existing v8 sidecar, and installs an exact fresh-inode copy of the
   accepted physical image. High-level startup can attach that recovered
   image as a read-only memtable, keep commit admission fenced, and close
  without advancing the immutable boundary. Startup can also recreate an
  empty v7 or v8 successor when a durable `SegmentSealed` record names its exact
  Active boundary but the process stopped before engine-manifest registration;
  an existing unregistered image must match the canonical empty image. The
  sealed predecessor and sidecar are strictly revalidated first. Stopped-source
  v8 seal preparation and installation primitives now follow the v7 reservation
   and manifest-authority sequence, including byte-exact validation of any
   pre-existing sidecar before `SealStarted`. Explicit `resume_pitr` can now
   finish a durable Active v8 Sealing obligation through `SegmentSealed` and
   install only its exact manifest-authorized empty successor. A failed
   post-`SegmentSealed` install is retried from that durable boundary. Explicit
   `create_recovery_point` now completes the Active v8 rotation, installs its
   v8 successor and archives the sealed source. Automatic v8 rotation, general
  format-registry selection and new v7-to-v8 migration initiation remain
  disabled; explicit resume only completes an already-published intent.
5. **Persistence and transitions:** version persisted seal/manifest/backup
   digest references, validate v8 archive objects, support v5/v6/v7/v8 chain
   transitions and the old-reader compatibility gate above. Add capacity and
   migration preflight without relabeling or rewriting existing segments in
   place. The catalog recognizes the v8 `seal_format_version = 3` descriptor
   and binds an empty v8 prefix to its header. Archive publication now verifies
   the frozen v8 WAL and sidecar, and shallow/deep repository verification and
   ticket-ordered restore dispatch by format. Tests cover source-path archival,
   catalog reopen, sealed-source startup recovery, and exact restore from
   persisted archive objects across a v7-to-v8 chain. The PITR manifest state
   machine can durably represent a v7-to-v8 migration intent that binds the
  immutable v7 source boundary and exact empty v8 successor. Explicit
  `resume_pitr` now rechecks and reserves migration capacity after restart,
  then finishes a pre-seal intent through the existing v7 sealing path and
  installs only its exact authorized v8 successor. It keeps commit admission
  fenced throughout. New migration initiation and automatic v8 adoption remain
  disabled pending compatibility qualification. Once `SegmentSealed` durably
   binds the v7 source and exact empty v8 boundary, startup can strictly
   validate that source and recreate/register the v8 successor if interrupted
   before engine-manifest registration. `PitrStatus` reports a
   read-only v7-to-v8 capacity estimate only for a settled Active v7 boundary
   that matches the on-disk WAL header, including retained source allocations,
   growth allowance to the configured segment limit, and rounded transition
   workspace; this estimate does not reserve capacity or enable v8 creation. A
   matching reservation primitive now atomically books reconstructed retained
   source charges and transition source-spool/recovery-workspace charges; a
   stale or deferred estimate leaves the ledger unchanged. Fenced intent
   preparation now syncs and persists the exact v7 Active boundary, reserves
   that complete transition budget, and only then publishes the old-reader
   compatibility guard followed by `V8MigrationStarted`; the returned owner
  retains the reservation. No automatic runtime path invokes this preparation;
  explicit resume only completes an already-published durable intent.
   Active recovery
   normalization is tested with repeated process
   interruption after temp-image write, temp sync, rename, and directory sync;
   retries preserve the hard anchor. Returned errors before sync, rename, and
   directory sync are also covered. Automatic v8 creation remains disabled
   pending the compatibility and qualification gates.
6. **Qualification and adoption:** complete the RFC 026 crash, recovery,
   compatibility and performance matrix. Keep v7 as the default unless the
   measured results justify an explicit later adoption decision.

Each slice must leave existing v2-v7 reads, writes, recovery and digest
interpretation unchanged. Tests for each completed slice are part of its
review evidence; end-to-end crash and performance qualification remains a
separate gate before v8 is enabled.
