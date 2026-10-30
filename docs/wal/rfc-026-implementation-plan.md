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
enum. An opt-in process test also runs the historical CLI at
`18c5f0a8ccfb` against a guarded Active v7 database with WAL enabled; it
verifies that the old binary rejects the database and that recursive file
contents, entry names and file metadata remain unchanged. It also checks that
the error identifies the unknown v8 manifest variant. Re-run it with:

```bash
TOY_KV_PRE_V8_CLI=/path/to/kv-engine-cli cargo test --package kv-engine --lib \
  v8_compatibility_guard_rejects_pre_v8_cli -- --ignored
```

The first filter runs six process cases: the guarded database with its normal
`MANIFEST`; after the guard append but before snapshot installation; after the
new wrapper is synced as `ENGINE_MANIFEST.tmp`; after the wrapper is renamed to
`ENGINE_MANIFEST` but before `MANIFEST` compaction; with `MANIFEST` removed and
the installed `ENGINE_MANIFEST`; and with `MANIFEST` missing while the guarded
snapshot remains pending as `ENGINE_MANIFEST.tmp`.

An additional process case runs the fully published database against the
earlier historical CLI at `3420eb05b23c5b6ddd22694a4a2f84698589655a`:

```bash
TOY_KV_EARLY_PRE_V8_CLI=/path/to/earlier/kv-engine-cli cargo test --package kv-engine --lib \
  v8_compatibility_guard_rejects_earlier_pre_v8_cli_without_mutation -- --ignored
```

The publisher requires a token holding both PITR lifecycle barriers;
the migration caller must retain it through the v7-to-v8 transition so a
concurrent resume cannot reopen commit admission between guard publication
and WAL migration.

This is partial compatibility qualification. Before enabling v8 creation, run
the remaining process-level tests against the actual older binaries for normal
and snapshot recovery, missing manifest, interrupted migration, WAL-lock and
value-log startup, and compare database files and directory entries before and
after rejection. The complete historical-binary matrix and startup ordering
remain unqualified, so v8 creation stays disabled.

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
   without advancing the immutable boundary. PITR resume and
   rotation/successor installation still fail closed for Active v8 Sealing;
   v8 creation and general format-registry selection remain disabled.
5. **Persistence and transitions:** version persisted seal/manifest/backup
   digest references, validate v8 archive objects, support v5/v6/v7/v8 chain
   transitions and the old-reader compatibility gate above. Add capacity and
   migration preflight without relabeling or rewriting existing segments in
   place. The catalog recognizes the v8 `seal_format_version = 3` descriptor
   and binds an empty v8 prefix to its header. Archive publication now verifies
   the frozen v8 WAL and sidecar, and shallow/deep repository verification and
   ticket-ordered restore dispatch by format. Tests cover source-path archival,
   catalog reopen, sealed-source startup recovery, and exact restore from
   persisted archive objects across a v7-to-v8 chain. `PitrStatus` reports a
   read-only v7-to-v8 capacity estimate only for a settled Active v7 boundary
   that matches the on-disk WAL header, including retained source allocations,
   growth allowance to the configured segment limit, and rounded transition
   workspace; this estimate does not reserve capacity or enable v8 creation. Active recovery
   normalization is tested with repeated process
   interruption after temp-image write, temp sync, rename, and directory sync;
   retries preserve the hard anchor. Returned errors before sync, rename, and
   directory sync are also covered. V8 creation remains disabled pending the
   compatibility and qualification gates.
6. **Qualification and adoption:** complete the RFC 026 crash, recovery,
   compatibility and performance matrix. Keep v7 as the default unless the
   measured results justify an explicit later adoption decision.

Each slice must leave existing v2-v7 reads, writes, recovery and digest
interpretation unchanged. Tests for each completed slice are part of its
review evidence; end-to-end crash and performance qualification remains a
separate gate before v8 is enabled.
