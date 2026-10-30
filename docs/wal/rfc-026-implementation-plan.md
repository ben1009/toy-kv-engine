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

Existing binaries can rewrite `MANIFEST` during recovery before checking its
numeric format version. They can also create the WAL open-lock file or open
the value log before parsing the manifest. A version-8 WAL header or a higher
manifest version alone therefore does not establish RFC 026's rejection before
mutation requirement.

Before enabling v8 creation, establish and durably publish a compatibility
guard that every older reader lacking v8 support rejects before it can mutate
the database. Preserve the guard across manifest logs, snapshot replacement and
interrupted migration for as long as v8 history remains. A marker that relies
on an ignored JSON field is insufficient. Verify the guard against actual older
binaries with process-level tests covering normal and snapshot recovery,
missing manifest, interrupted migration, WAL lock and value-log startup, and
compare database files and directory entries before and after rejection. The
guard and startup ordering are not yet proven; if an older binary cannot be
made to satisfy the requirement, v8 creation remains disabled pending an
explicit compatibility decision and any necessary RFC amendment.

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
   normalization. Keep v8 creation disabled until this slice is complete.
5. **Persistence and transitions:** version persisted seal/manifest/backup
   digest references, validate v8 archive objects, support v5/v6/v7/v8 chain
   transitions and the old-reader compatibility gate above. Add capacity and
   migration preflight without relabeling or rewriting existing segments in
   place. The catalog recognizes the v8 `seal_format_version = 3` descriptor
   and binds an empty v8 prefix to its header. Archive publication now verifies
   the frozen v8 WAL and sidecar, and shallow/deep repository verification and
   ticket-ordered restore dispatch by format. Tests cover source-path archival,
   catalog reopen, sealed-source startup recovery, and exact restore across a
   v7-to-v8 chain. Active recovery normalization is tested with repeated process
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
