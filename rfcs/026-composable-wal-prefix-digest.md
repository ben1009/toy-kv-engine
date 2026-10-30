# RFC 026: Composable WAL Prefix Digests

| Field | Value |
| --- | --- |
| Status | Proposed |
| Date | 2026-10-30 |
| Author | kv-engine Contributors |
| Builds on | [RFC 023](023-point-in-time-recovery.md), [RFC 024](024-dedicated-wal-pipeline.md), [RFC 025](025-parallel-wal-embedded-frontiers.md) |

## 1. Summary

This RFC proposes a new PITR WAL format, provisionally **v8**, whose logical
prefix uses **BLAKE3-256 entry-stream digests, batch digests, and an ordered
chain**. Producers hash canonical entry bytes during encoding, then bind the
completed logical header to that entry digest. The ordered coordinator hashes
only a fixed-size link for each ticket. The physical DATA/FRONTIER layout and
single-sync durability protocol follow RFC 025, with format-specific validation
and a new logical digest definition.

This is a proposal, not an implemented format, adoption decision, or claim of
v4/v7 performance parity. In particular, it does not alter the digest of any
existing v7 segment. V8 MUST support Parallel by default under RFC 025. Leader
and Parallel MUST compute the same logical digest for the same header and
ordered batches, and encode identical DATA frames at the same offsets. Their
FRONTIER placement and frequency may differ with group formation.

## 2. Motivation and scope

RFC 025 defines v7's logical digest as SHA-256 over the WAL header followed by
all logical batch bytes in ticket order. Its rolling state cannot be advanced
for ticket `i + 1` until the complete bytes of ticket `i` have been hashed.
Producers already frame large DATA batches, while the packer hashes the logical
stream in order. The [batch attribution](../docs/wal/benchmarks/rfc-025-v7-batch-attribution-20261029.md)
measured substantial SHA work before later pipeline changes; its timings are
not measurements of the current pipeline. The [direct-frame CPU screen](../docs/wal/benchmarks/rfc-025-v7-direct-frame-cpu-20261030.md)
found only a small isolated gain from removing a logical-to-frame copy.

Performance is the primary goal. V8 MUST remove payload-sized cryptographic
work from the ordered coordinator and SHOULD avoid a second full-buffer
cryptographic scan after canonical entry encoding. The entry-stream digest
allows this without waiting for the logical header's entry-stream CRC to be
known: encode and hash entries first, then hash the completed header and the
32-byte entry digest. There is no need for a payload CRC prepass solely to
start the cryptographic hash in header-first order.

[BLAKE3](https://c2sp.org/BLAKE3@v1.0.0) provides incremental hashing, a 32-byte
output and a tree structure suitable for SIMD implementations. Independent
producer jobs provide concurrency across batches; this RFC does not require
an additional thread pool inside each hash. BLAKE3's
[Rust API](https://docs.rs/blake3/latest/blake3/struct.Hasher.html#method.update)
notes that update size limits SIMD parallelism. Replacing one bulk update with
many tiny field updates may lose that benefit and MUST be measured.

Every canonical entry byte still enters a cryptographic hash once. Encoding
with incremental updates avoids revisiting the entire completed logical
buffer; it does not eliminate payload hashing, CRC work or every memory read.
The fixed-size hashes add work, and total CPU can still increase. Producer
contention, update granularity, cache behavior, grouping and sync timing can
erase any gain. End-to-end qualification is required.

Goals are deterministic independent verification, a fixed-size ordered step,
and unambiguous backward compatibility. This RFC does not change frame CRCs,
canonical batch encoding, timestamp or ticket assignment, active fallback,
sealed-object strictness, or introduce a cryptographic authenticity guarantee.

## 3. Wire identity and digest definition

V8 is a distinct, explicitly registered WAL format. Its 4096-byte header,
DATA and FRONTIER framing, logical batch encoding, ordering rules, and other
fields use RFC 025's v7 definitions except where this RFC explicitly changes
the format identifier or digest rule. Header/version validation and all
format-specific object validators MUST dispatch by the actual format. The
exact numeric version assignment, magic, flags and registry entries MUST be
specified in the implementation plan before any v8 files are written; `v8`
below is the proposed format identity, not permission to reuse the v7 format
version identifier.

A registered v8 identity selects exactly the BLAKE3-256 entry-chain scheme
defined here. Changing the primitive, hash mode, tags or preimages requires a
distinct registered format or scheme identity. A reader that does not support
v8 MUST reject it before changing any files or persistent metadata. Unknown
versions and unsupported digest schemes MUST NOT be guessed from digest length
or treated as v7.

Let `W` be the complete immutable 4096-byte v8 WAL header, including its format
identity, header checksum and reserved bytes. For segment-local ticket `i`:

- `Q_i` is the exact 48-byte canonical logical batch header from RFC 025,
  including ticket, commit timestamp, recorded time, entry count, `data_len`,
  `data_crc32`, `header_crc32` and the zero reserved field.
- `S_i` is the exact canonical entry stream following that header. It includes
  all operation tags, lengths, keys and values in canonical mixed-operation
  order. Hashing source records before canonicalization is insufficient.
- `n_i = len(S_i)` MUST equal the header's `data_len`, and `len_i = 48 + n_i`
  MUST equal the logical batch length repeated in every DATA fragment header.
  Thus `L_i = Q_i || S_i` remains the complete logical batch on disk, but the
  digest computation does not require a contiguous `L_i` allocation.

Exclude physical fragment headers, frame headers, FRONTIER frames and padding
from these logical inputs. Validate their canonical encoding separately.
A batch MUST be valid under the v8 logical codec before contributing to a
verified prefix.

`BLAKE3` below means the regular **unkeyed hash mode with exactly 32 output
bytes**. Use neither keyed mode nor `derive_key` mode. Inputs are byte strings;
`u64be` encodes an unsigned 64-bit big-endian integer, all digest outputs remain
raw bytes, and BLAKE3's internal word order does not change the WAL byte order.
The following tags are exact ASCII bytes, including one final NUL byte;
`||` denotes byte concatenation:

```text
HEADER  = "toy-kv-engine/wal-prefix/v8/header\0"
ENTRIES = "toy-kv-engine/wal-prefix/v8/entries\0"
BATCH   = "toy-kv-engine/wal-prefix/v8/batch\0"
INIT    = "toy-kv-engine/wal-prefix/v8/init\0"
LINK    = "toy-kv-engine/wal-prefix/v8/link\0"

H       = BLAKE3(HEADER || W)
E_i     = BLAKE3(ENTRIES || u64be(n_i) || S_i)
B_i     = BLAKE3(BATCH || Q_i || E_i)
P_0     = BLAKE3(INIT || H)
P_(i+1) = BLAKE3(LINK || P_i || u64be(i) || u64be(len_i) || B_i)
```

Ticket `i` starts at zero for each segment. `P_T` is the digest after exactly
`T` contiguous batches and is the v8 FRONTIER `prefix_digest` for exclusive
`ticket_end = T`. The generation-zero FRONTIER carries `P_0`. This differs from
v7's generation-zero value, `SHA256(W)`. The link's ticket MUST match `Q_i` and
every fragment header. The length and fixed-width fields make the input
unambiguous. All arithmetic, length conversions and ticket increments MUST be
checked against the existing wire and configured limits; overflow or a ticket
discontinuity is an error. Each link hashes exactly 113 bytes regardless of
payload size. There is no cross-segment hash-state continuation: each segment
starts from its own header, whose predecessor identity and archive chain
continue to be validated by the existing PITR rules.

`H` is solely the logical chain's header binding. The common frame header's
`wal_header_digest` remains `SHA256(W)`; FRONTIER record digests, sealed-image
digests and archive-object digests retain their existing SHA-256 definitions.
Their purpose and input bytes differ from `H`, `E_i`, `B_i` and `P_T`. A v8
logical digest MUST NOT be substituted into one of those physical fields or
into a v7 rolling SHA state.

`E_i` and `B_i` are ephemeral computation results, not new on-disk records or
trusted recovery shortcuts. A correct encoder may cache them with its owned
bytes, but recovery MUST recompute them from actual validated content. Neither
a persisted or externally supplied digest nor entry CRCs alone prove the
payload. `E_i` MUST cover every canonical entry byte directly.

### 3.1 Digest conformance vector

This vector exercises every hash preimage. Its synthetic header is
`W[j] = j mod 256` for `0 <= j < 4096`; it is not a valid WAL header. `Q_0 || S_0`
encodes ticket 0, commit timestamp 1, recorded seconds 1, nanos 0, and one PUT
of key `k` and value `v`. Both logical-header CRCs are included.

```text
H   = 53edd07fe6e3d0dda540770b400ece2538228897a535415cf8958be5a1bb922f
Q_0 = 0000000000000000000000000000000100000000000000010000000000000001
      00000010c4459325224cded900000000
S_0 = 01000000000a000000016b0000000176
n_0 = 16
len_0 = 64
E_0 = 15c56b2a4fef9ab6690b5f8c68e10b8750ad63a677951adcc5a3aeefa3f45fb4
B_0 = 0bdffcb250aaf751128e25262a7942f608fa99d7857eb43c54c5abf922532c48
P_0 = 308b9c35ac3fa99809a80e99ad7dde4de8d653dd782ee9adc0dd390ed9bb87e3
P_1 = b0d28130323ebce6e10591137ab6623bdb6533e9863b39ef56e9b0d6f59f2d53
```

The values agree between the official BLAKE3 C implementation and Rust
reference implementation at
[revision 93a431c](https://github.com/BLAKE3-team/BLAKE3/tree/93a431c78a52d7ccf0f366f106467f5070e6075e),
including incremental update sizes of 1, 7, 64, 4008 and 65536 bytes.
Implementations MUST add independently generated byte-exact golden vectors for
valid v8 headers, multiple batches, frames, FRONTIERs and persisted anchors,
including negative timestamps and fragment boundaries. A round trip using the
same encoder and decoder is not sufficient.

## 4. Parallel writer and durability

Admission retains RFC 025's segment-local ticket, physical offset, resource
reservation, timestamp and recorded-time rules. The intended preparation path
is:

1. Canonicalize the operations and obtain the checked encoded entry length
   `n_i` from their sizes. A size calculation need not serialize or hash the
   payload. Reserve preparation resources before encoding.
2. Initialize an entry hasher with `ENTRIES || u64be(n_i)`. As canonical byte
   ranges are encoded, feed those same bytes in order to `E_i` and the existing
   entry-stream CRC computation. Hash each range exactly once; no skipped,
   repeated or pre-canonical bytes are permitted. Use bounded contiguous
   ranges to preserve efficient hash updates. Check the emitted byte count
   against `n_i`, then finalize `E_i`.
3. After admission assigns the final ticket and ordered timestamp metadata,
   complete `Q_i`, including `data_crc32` and `header_crc32`. Compute
   `B_i = BLAKE3(BATCH || Q_i || E_i)` without rereading `S_i`.
4. Prepare DATA frames on producers and publish frame-ready and digest-ready
   results independently from worker DATA-completion events. The ordered
   coordinator folds `B_i` using the 113-byte link input and performs no payload
   hashing.

Entry encoding and `E_i` may finish before ticket assignment because `S_i`
does not depend on ordered metadata. `B_i` MUST wait for the final `Q_i`.
Changing any encoded entry invalidates the cached `E_i` and `B_i`; changing
the logical header invalidates `B_i`. The digest and the DATA bytes MUST come
from the same owned immutable encoding. Admission determines group visibility;
digest or frame readiness only gates submission or durability eligibility and
MUST NOT hide admitted tickets from group formation.

Digest work may overlap framing or I/O, but a caller MUST NOT mutate, release
or reuse bytes while any encoder, hash or write can still read them. V8 MUST
charge and bound digest jobs, incremental hash state, logical buffers, frame
buffers and completed out-of-order results using the existing
admission/backpressure model. Shutdown and cancellation MUST retire ownership
only after all consumers finish. This RFC allows existing intermediate buffers;
direct-to-frame encoding is not required to obtain the digest's streaming
benefit.

Digest completion and DATA completion are independent events. They may arrive
out of order. The ordered coordinator MUST fold only the contiguous digest-ready
prefix, once per ticket, into `P_T`. Duplicate completion, a conflicting digest,
a missing ticket, or a digest for another segment incarnation MUST NOT advance
the prefix. A candidate FRONTIER for `T` is eligible only when:

1. Every DATA write for `[0, T)` completed successfully and its physical
   prefix meets RFC 025's allocation and retained-frontier invariants.
2. Every logical batch in `[0, T)` has its correct digest and the coordinator
   has folded exactly that range into `P_T`.
3. The frontier contains the matching `ticket_end`, `durable_end`, timestamp
   and `P_T`, and satisfies the physical FRONTIER chain rules.

The normal durability cycle remains DATA complete and digest ready, write the
FRONTIER, successfully `fdatasync(WAL)`, publish the durable watermark, then
acknowledge. A digest result alone, a successful DATA CQE, a complete marker,
or a submitted sync MUST NOT publish durability. V8 does not add a second
steady-state sync. RFC 025's failed marker/sync poisoning and prohibition on
further FRONTIER appends in that runtime remain mandatory.

A post-admission hash or frame failure, panic, dropped job, invalid completion,
or write failure MUST follow the existing ordered poison rules, wake waiters,
and prevent acknowledgement at or beyond the failed ticket. A later ready
batch cannot skip an unfinished earlier one. An implementation MUST document
its fail-stop assumption for a producer that neither completes nor fails and
MUST ensure close, cancellation, and poisoning do not create an unbounded
resource leak or admit an incorrectly durable prefix. This RFC does not require
work stealing or promise a timeout for an otherwise healthy stalled producer.

## 5. Recovery, anchors and sealed objects

Active recovery independently discovers aligned FRONTIER candidates as in RFC
025. For each candidate, it MUST validate the entire covered physical prefix,
all retained control frames and their unique linear chain, canonical fragments,
logical batch CRCs, contiguous tickets, commit timestamps and recorded times.
It MUST recompute each `E_i` from the actual canonical `S_i`, compute `B_i`
using the complete validated `Q_i`, fold the chain from `P_0`, and compare `P_T`
with the candidate and every applicable hard anchor. Reassembly or streaming
validation MUST produce identical results regardless of fragmentation or
buffer size. An invalid or missing batch, wrong digest,
reordered/duplicated/skipped ticket, or conflicting anchor rejects that
candidate. Recovery MUST NOT trust a marker merely because its own CRC or
predecessor chain is valid.

Recovery SHOULD validate the digest without allocating a complete `L_i`:
validate each physical frame CRC, collect the fixed 48-byte `Q_i`, check its
header CRC and limits, then stream the canonical entry bytes through the
entry-stream CRC, entry parser and `E_i` hasher. Count exactly `n_i` bytes,
validate entry count, ordering and all logical constraints, compare
`data_crc32`, then compute `B_i` and the link. Fields and entries may cross
fragment or input-buffer boundaries. Bounded parser buffering and any owned
records needed for replay remain permitted; parsing streamed bytes MUST NOT
publish them before the applicable batch and candidate validation succeeds.
Neither stored intermediate digests nor CRC values may replace hashing the
actual entry bytes.

V8 inherits RFC 025's explicit Active fault model: an interrupted single sync
can leave a complete marker but missing DATA, indistinguishable from some
post-sync storage damage. Active recovery may fall back to an older fully valid
candidate only if every durable logical anchor remains satisfied. I/O errors,
corruption of required initial state, and violated anchors are errors, not
permission to fall back below the hard floor. Accepted active prefixes MUST be
normalized into a fresh inode, synced, atomically installed, and followed by a
directory sync as required by RFC 025 before service. Normalization retains the
same logical `P_T` and may change physical marker identity.

An Active durable anchor binds segment identity/incarnation, format or digest
scheme, exclusive ticket end, durable physical end, last commit timestamp, and
`P_T`. The format may be obtained from an independently validated bound segment
identity; any standalone persisted anchor that cannot derive it MUST encode it
explicitly in a versioned record. It MUST NOT rely on an Active frontier offset,
generation or frontier-record digest as its logical identity. Existing v7
anchors retain their exact v7 meaning. A v8 anchor cannot validate a v7 segment
or vice versa, even if 32-byte values happen to match.

Sealing, sealed segments, archived objects, manifests, catalogs, checkpoints,
backup/restore and PITR replay MUST select the format-specific digest rule,
carry or unambiguously bind the format identity, and check the anchored logical
boundary. Sealing and sealed/archived images remain frozen and strict: no
fallback or active normalization may silently replace their physical identity.
Existing terminal FRONTIER, physical WAL and archive-object digest rules remain
separate and MUST be checked. Empty segments validate the v8 `P_0` rule in all
these paths. A format change MUST NOT weaken backup pinning, spool accounting,
retention, point-in-time filtering, or cross-segment predecessor checks.

## 6. Compatibility and rollout

V5/v6/v7 remain readable using their original rules. Migration to v8 occurs
only by creating a **new** segment at a durable rotation boundary, with a new
v8 header and `P_0`; existing WALs, archived objects, seal indexes, checkpoints
and their anchors MUST NOT be rewritten or relabeled. V8 must coexist with
historical versions in a single PITR chain, and replay and restore must dispatch
per segment. Rollout MUST define versioned descriptors for any persisted
manifest, seal, catalog or backup encoding that cannot currently express v8,
including reader behavior for mixed chains. Simply changing a writer constant
is insufficient.

Before v8 creation, require successful format/capacity preflight and durable
publication of the predecessor and successor identities under the applicable
RFC 025 rotation protocol. V8 must preserve the fresh-inode recovery workspace
and maintenance reservations. Existing healthy databases that cannot satisfy
v8's workspace or format prerequisites MUST remain writable in their supported
current format, without a partially published migration. The exact operator
control, persisted eligibility and enablement mechanism are implementation-plan
work and MUST be specified before default adoption.

A binary lacking v8 support MUST fail safely on a v8 reference before mutation;
installing an older binary is not a way to read or recover v8 data. Merely
rotating back to v7 does not make retained v8 segments, backups or archives
readable by the older binary. Any operational rollback requires retaining a
reader capable of validating all referenced v8 history until that history is
no longer needed, and an explicit, tested transition procedure. No automatic
downgrade is proposed.

## 7. Integrity and security

The chain commits to segment header identity, batch boundaries, order, ticket,
length, metadata and all canonical logical bytes, subject to BLAKE3-256's
collision resistance. Separate tags distinguish header, entry-stream, batch,
initial and link digests. `E_i` directly binds the payload; retaining CRCs alone
would not provide that cryptographic binding. Reordering, truncation,
duplication, substitution, cross-segment replay, header or length tampering
MUST be rejected against a trusted frontier or hard anchor. A complete,
self-consistent malicious rewrite of the WAL and all anchors is outside this
unkeyed hash's protection. Using regular unkeyed BLAKE3 does not add a MAC,
signature or encryption. The physical
SHA-256 digests and CRCs also do not protect against an attacker who can rewrite
every trusted record. The RFC 025 single-sync Active fallback ambiguity remains;
a chain digest cannot prove that a particular sync succeeded.

## 8. Implementation and validation gates

Implementation begins only after this RFC is accepted. Before default adoption,
the implementation plan MUST cover at least:

- Exact v8 wire identifiers, every format-dispatch and persisted descriptor
  change, checked size limits, cross-version chains, and old-reader rejection.
- Independent digest vectors, a reference implementation, multi-ticket
  prefix checks, exact tags and unkeyed 32-byte output, duplicate, reordered or
  omitted batches, fragment and CRC corruption, header/identity binding and
  overflow. Compare one-shot and encode-time entry hashing with streaming
  recovery at field, entry, BLAKE3 chunk and DATA fragment boundaries. Mutate
  payload bytes while recomputing CRCs to ensure the cryptographic digest still
  rejects. Exercise empty and nonempty segments, canonical mixed operations,
  and both Leader and Parallel writers.
- Deterministic out-of-order digest/DATA completion, failure and panic,
  cancellation, backpressure, lost-wakeup, close and resource-retirement tests.
  No acknowledged ticket may precede its data, digest, marker and successful
  sync. Test recovery of every acknowledged prefix after injected crashes at
  each boundary, including a complete marker with incomplete DATA.
- Active fallback above durable anchors, repeated interrupted normalization,
  atomic rename and directory-sync failures, anchor relocation, malformed
  frontier forks, strict Sealing/Sealed/archive validation, checkpoint/backup
  and PITR restore across v5/v6/v7/v8 transitions. Unknown or incompatible
  readers must fail before mutation. Exercise insufficient recovery workspace.
- Predeclared, reproducible v7 Parallel versus v8 Parallel comparisons, with
  v8 Leader as a control and v4 Parallel as context. Cover point writes and
  batch sizes including 64, with 1, 2, 4, 8 and 16 writers on ext4 SSD; report
  throughput, p50/p99, process CPU, hash/framing time, sync and group counts,
  and recovery time. Record exact binaries, configuration, samples, storage
  stalls and memory/backpressure. Compare matching workloads and report both
  best eligible and the sample distribution; a best score is not a substitute
  for repeated evidence. Check normal read/write and replay workloads too.
- CPU attribution that distinguishes changing the hash primitive from moving
  work: report `payload_hash_bytes`, `payload_hash_cpu_ns`,
  `ordered_link_hash_cpu_ns`, and `post_encode_hash_reread_bytes`. Count all
  canonical entry bytes fed to cryptographic hashes and sum hashing CPU across
  producers; report retries or discarded preparations separately. The reread
  metric counts entry bytes consumed by a separate post-encoding hash pass,
  excluding immediate encode-time range updates, CRC and framing reads. Also
  report update sizes/counts, header/batch hash CPU, peak hash/buffer memory
  and any thread-pool overhead. The ordered coordinator MUST hash zero payload
  bytes. A streaming encode path should have zero post-encode full-stream hash
  rereads; incremental updates still read their byte ranges and do not imply
  zero memory traffic. Isolate bulk versus encode-time hashing, SHA-256 versus
  BLAKE3, and the final pipeline before attributing a gain to one change.
  Document and measure any path that retains a separate full-stream hash scan.
- An explicit adoption decision based on correctness, performance and resource
  evidence. A fixed-size ordered step and avoiding full-buffer rereads do not
  prove an end-to-end gain. If throughput or tail latency is not favorable,
  keep v7 as the default.

## 9. Alternatives

Keep v7's rolling SHA and improve scheduling: no format migration, but the
payload-sized ordered SHA dependency remains. An in-memory batch hash cannot
reconstruct `SHA256(header || L_0 || ... || L_i)` and therefore cannot replace
the v7 state. Merkle trees enable other proofs and parallel tree construction
but add tree shape, storage and recovery complexity beyond this ordered-prefix
need. A weaker or non-cryptographic hash reduces CPU but changes integrity
properties and is not proposed.

An independent `SHA256(BATCH || len_i || L_i)` plus a small ordered chain removes
the serial payload dependency, but naturally requires a full logical-buffer
scan after the CRC-dependent header is complete. The entry-stream/header split
permits encode-time hashing without that dependency. Using SHA-256 with the
same split is a useful benchmark control; BLAKE3 is the proposed primitive,
with any speedup subject to measured update granularity and CPU features.
Buffered scatter/gather and direct-to-frame encoding address different costs
and do not remove the ordered hash dependency by themselves.
