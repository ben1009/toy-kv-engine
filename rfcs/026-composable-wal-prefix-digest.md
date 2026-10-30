# RFC 026: Composable WAL Prefix Digests

| Field | Value |
| --- | --- |
| Status | Proposed |
| Date | 2026-10-30 |
| Author | kv-engine Contributors |
| Builds on | [RFC 023](023-point-in-time-recovery.md), [RFC 024](024-dedicated-wal-pipeline.md), [RFC 025](025-parallel-wal-embedded-frontiers.md) |

## 1. Summary

This RFC proposes a new PITR WAL format, provisionally **v8**, whose logical
prefix digest is a chain of independently computed batch digests. A producer
can hash a complete logical batch after its ordered metadata is fixed; the
ordered coordinator then hashes a fixed-size link for each ticket. The
physical DATA/FRONTIER layout and single-sync durability protocol follow RFC
025, with format-specific validation and a new digest definition.

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

V8 moves the payload-sized SHA operation to independent per-batch work. The
ordered chain still performs one SHA-256 per batch; total payload bytes still
need hashing and total CPU can increase. Producer contention, cache behavior,
grouping and sync timing can erase any latency or throughput gain. This design
requires end-to-end qualification rather than assuming parallel work is free.

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
A reader that does not support v8 MUST reject it before changing any files or
persistent metadata. Unknown versions and unsupported digest schemes MUST NOT
be guessed from digest length or treated as v7.

Let `H = SHA256(header)` over the complete immutable 4096-byte v8 WAL header,
including the v8 identity. Let `L_i` be the complete canonical logical batch
header and entry stream for segment-local ticket `i`, including its logical
header, entry CRC and header CRC. Exclude physical fragment headers, frame
headers, FRONTIER frames and padding, exactly as for v7's logical input. Let
`len_i` be `len(L_i)` in bytes, encoded as an unsigned 64-bit big-endian
integer. A batch MUST be valid under the v8 logical codec and have ticket `i`
before it contributes to the chain.

`SHA256` returns exactly 32 raw bytes. The following tags are exact ASCII bytes,
including one final NUL byte; `||` denotes byte concatenation:

```text
INIT  = "toy-kv-engine/wal-prefix/v8/init\0"
BATCH = "toy-kv-engine/wal-prefix/v8/batch\0"
LINK  = "toy-kv-engine/wal-prefix/v8/link\0"

P_0     = SHA256(INIT || H)
B_i     = SHA256(BATCH || u64be(len_i) || L_i)
P_(i+1) = SHA256(LINK || P_i || u64be(i) || u64be(len_i) || B_i)
```

Ticket `i` starts at zero for each segment. `P_T` is the digest after exactly
`T` contiguous batches and is the v8 FRONTIER `prefix_digest` for exclusive
`ticket_end = T`. The generation-zero FRONTIER carries `P_0`, **not** `H` as
in v7. The ticket is also present in `L_i`; both copies MUST agree. The length
and fixed-width fields make the input unambiguous. All arithmetic and ticket
increment operations MUST be checked; overflow or a ticket discontinuity is
an error. There is no cross-segment hash-state continuation: each segment starts
from its own header, whose predecessor identity and archive chain continue to
be validated by the existing PITR rules.

`B_i` is an ephemeral computation result, not a new on-disk record or a trusted
recovery shortcut. A correct implementation may cache it in memory while the
batch is owned, but MUST NOT accept a persisted or supplied `B_i` as proof of
the payload. A v8 batch digest or chain value MUST NOT be substituted for a v7
rolling SHA state, a frame-record digest, a sealed-image digest, or an archive
object digest.

### 3.1 Digest conformance vector

This vector exercises the byte-level formula only. `H` is a synthetic 32-byte
input, not the digest of a valid WAL header. `L_0` encodes ticket 0, commit
timestamp 1, recorded seconds 1, nanos 0, and one PUT of key `k` and value
`v`. Its two logical-header CRCs are included.

```text
H   = 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
L_0 = 0000000000000000000000000000000100000000000000010000000000000001
      00000010c4459325224cded90000000001000000000a000000016b0000000176
len_0 = 64
P_0 = 4d03b59ab6e18fe4a553a1113df059d15c538b2cfa30c8010fd3b8135d76b603
B_0 = 58ca4d4b04c6dddb4e13ec78bada4b29608cfb798fd36809d0a5dffa1e8c853d
P_1 = c944f20de0cdd94b5633c396a0107d48e90e3f4b9ec2ce1e535ae4da212465a0
```

Implementations MUST add independently generated byte-exact golden vectors for
valid v8 headers, multiple batches, frames, FRONTIERs and persisted anchors,
including negative timestamps and fragment boundaries. A round trip using the
same encoder and decoder is not sufficient.

## 4. Parallel writer and durability

Admission retains RFC 025's segment-local ticket, physical offset, resource
reservation, timestamp and recorded-time rules. A batch digest MUST be
computed from the immutable final `L_i`, after admission has chosen the ticket
and ordered metadata and the canonical logical header and CRCs are complete.
Pre-hashing entries alone cannot replace `B_i`. Producer hashing may run
alongside framing or I/O, but a caller MUST NOT mutate, release or reuse bytes
while any hash or write can still read them. V8 MUST charge and bound digest
jobs, logical buffers, frame buffers and completed out-of-order results using
the existing admission/backpressure model; shutdown and cancellation MUST
retire their ownership only after all consumers finish.

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
It MUST recompute each `B_i` from the validated complete `L_i`, fold the chain
from `P_0`, and compare `P_T` with the candidate and every applicable hard
anchor. Reassembly or streaming validation MUST produce the identical ordered
logical bytes, regardless of fragmentation or buffer size. An invalid or
missing batch, wrong digest, reordered/duplicated/skipped ticket, or conflicting
anchor rejects that candidate. Recovery MUST NOT trust a marker merely because
its own CRC or predecessor chain is valid.

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
length, metadata and all canonical logical bytes, subject to SHA-256's
collision resistance. Separate tags prevent confusing a batch digest with an
initial or link digest. Reordering, truncation, duplication, substitution,
cross-segment replay, header or length tampering MUST be rejected against a
trusted frontier or hard anchor. A complete, self-consistent malicious rewrite
of the WAL and all anchors is outside this unkeyed hash's protection: SHA-256
and CRCs are not MACs, signatures, encryption, or independent protection
against an attacker who can rewrite every trusted record. The RFC 025
single-sync Active fallback ambiguity remains; a chain digest cannot prove that
a particular sync succeeded.

## 8. Implementation and validation gates

Implementation begins only after this RFC is accepted. Before default adoption,
the implementation plan MUST cover at least:

- Exact v8 wire identifiers, every format-dispatch and persisted descriptor
  change, checked size limits, cross-version chains, and old-reader rejection.
- Independent digest vectors, a reference implementation, multi-ticket
  prefix checks, duplicate/reordered/omitted batches, fragment and CRC
  corruption, header/identity binding and overflow. Exercise both empty and
  nonempty segments and both Leader and Parallel writers.
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
- An explicit adoption decision based on correctness, performance and resource
  evidence. A producer hash chain increases work and coordination; if the
  throughput or tail-latency tradeoff is not favorable, keep v7 as the default.

## 9. Alternatives

Keep v7's rolling SHA and improve scheduling: no format migration, but the
payload-sized ordered SHA dependency remains. An in-memory batch hash cannot
reconstruct `SHA256(header || L_0 || ... || L_i)` and therefore cannot replace
the v7 state. Merkle trees enable other proofs and parallel tree construction
but add tree shape, storage and recovery complexity beyond this ordered-prefix
need. A weaker or non-cryptographic hash reduces CPU but changes integrity
properties and is not proposed. Buffered scatter/gather and direct-to-frame
encoding address different costs and do not remove the ordered SHA dependency.
