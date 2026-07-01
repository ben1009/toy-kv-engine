# RFC 013: Chaos Testing for kv-engine

**Status:** Implemented
**Date:** 2026-06-29
**Author:** kv-engine Contributors
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

This RFC proposes adding a dedicated chaos-testing harness for kv-engine.

The engine already has strong unit, integration, and codec-level recovery tests
for WAL, manifest, memtable recovery, range tombstones, compaction filters, and
vLog metadata. What is still missing is a repeatable way to validate the whole
database process under abrupt termination and injected durability failures.

The proposed work has two layers:

1. **Process-level crash chaos tests (MVP).**
   A test driver spawns a child process that performs real kv-engine operations,
   kills it at arbitrary points, reopens the database, and checks crash
   invariants against a persisted operation log.
2. **In-process failpoint chaos tests (follow-up).**
   Opt-in debug failpoints inject errors after specific internal steps such as
   WAL batch append, WAL sync, manifest append, manifest snapshot handoff, SST
   sync, and memtable freeze/flush boundaries.

The goal is not random destruction for its own sake. The goal is to exercise
the exact places where LSM engines usually lose data, publish undurable state,
replay partial metadata, or reopen into subtly inconsistent state.

---

## 2. Motivation

kv-engine now has several crash-sensitive subsystems:

1. WAL batch durability and recovery, including the v4 parallel WAL path.
2. Manifest replay and manifest snapshot replacement.
3. Memtable freeze and immutable-memtable flush sequencing.
4. SST install ordering relative to fsync and manifest records.
5. Range tombstone persistence and recovery.
6. vLog file creation, reference tracking, and GC metadata.
7. Background compaction and filter-driven rewrites.

The current test suite covers many of these at the function or single-file
level. That is necessary, but not sufficient.

There are still important failure modes that only show up when a real process is
killed between steps:

1. A write becomes visible before its durability boundary.
2. A flushed SST exists on disk but was never safely published.
3. Manifest snapshot handoff leaves an ambiguous or stale restart state.
4. WAL replay succeeds, but the reopened engine violates logical invariants such
   as missing durable writes, resurrected deletes, or duplicate state.
5. Background threads and foreground writes interact badly across restart.

An educational storage engine should make its crash contract executable. Chaos
tests are the missing bridge between RFC-level invariants and implementation
reality.

---

## 3. Goals

1. Add a repeatable crash-test harness that exercises kv-engine through its
   public and test-exposed admin APIs plus the restart path.
2. Validate crash semantics at real process boundaries, not only via in-memory
   mocks or file corruption snippets.
3. Check both **safety** and **recovery** properties:
   1. the reopened engine must not panic or corrupt metadata;
   2. durable writes must survive;
   3. non-durable writes may be lost but must not appear partially applied;
   4. deletes and range tombstones must not resurrect covered data.
4. Cover key configuration axes that materially change persistence behavior:
   `enable_wal`, compaction mode, value separation on/off, and manifest
   snapshotting enabled/disabled. `serializable`-mode transactional visibility
   chaos is valuable, but is deferred until after the non-transaction harness
   is stable.
5. Make failures reproducible with a seed, scenario name, and persisted trace.
6. Keep the MVP practical enough to run locally and in targeted CI jobs.
7. Provide a clean path for follow-up failpoint-based fault injection.

## 4. Non-Goals

1. Simulating kernel, disk firmware, or filesystem bugs.
2. Proving linearizability under all concurrent schedules.
3. Exhaustively model-checking the engine.
4. Running heavyweight chaos jobs in every default PR check.
5. Injecting arbitrary memory unsafety; sanitizers remain a separate tool.
6. Replacing existing unit, integration, or benchmark tests.

---

## 5. Existing Baseline

Today the repository already includes useful recovery-focused coverage:

1. WAL truncation, CRC mismatch, corrupted magic, append-after-recovery, mixed
   point/range recovery, and multi-writer group commit tests in
   `kv-engine/src/tests/wal.rs`.
2. Memtable reopen and recovery checks in `kv-engine/src/tests/memtable.rs`.
3. Manifest recovery and snapshot logic in `kv-engine/src/manifest.rs`.
4. Recovery-sensitive RFCs that document ordering invariants:
   1. RFC 005 (MVCC),
   2. RFC 009 (compaction filters),
   3. RFC 010 (delete range),
   4. RFC 012 (parallel WAL).

That baseline is good, but it is still mostly **single-artifact** testing:

1. recover one WAL file;
2. recover one manifest;
3. reopen one memtable;
4. check one corrupted footer;
5. test one explicit edge case at a time.

The missing class is **whole-engine crash state**:

```text
open DB -> run real operations -> maybe freeze/flush/compact -> kill process
-> reopen DB -> validate durable prefix and metadata invariants
```

That is the scope of this RFC.

---

### 5.1 External Reference: RocksDB `db_crashtest`

RocksDB's `tools/db_crashtest.py` is the closest practical reference for this
RFC.

Its design is useful not because kv-engine should copy it wholesale, but
because it demonstrates a durable pattern for storage-engine crash testing:

1. separate **blackbox** and **whitebox** crash modes;
2. run the database repeatedly under kill/reopen loops rather than one-off
   corruption tests;
3. maintain an external expected-state oracle (`expected_values_dir`) instead of
   inferring correctness from the crashed DB itself;
4. vary configuration aggressively, but sanitize incompatible combinations so
   failures stay actionable;
5. alternate between kill-heavy phases and verification phases.

kv-engine should borrow the shape of that system while staying much narrower in
scope:

1. start with a small deterministic scenario matrix instead of RocksDB's very
   broad parameter randomization;
2. use kv-engine-specific invariants around WAL batches, manifest snapshots,
   range tombstones, and vLog state;
3. add a follow-up whitebox/failpoint layer only after the process-level
   harness is stable.

The MVP proposed in this RFC is therefore intentionally closer to a minimal
`db_crashtest` than to a fully randomized stress framework.

---

## 6. Crash Model

The harness should encode kv-engine's intended crash contract explicitly.

### 6.1 Durable Prefix Model

For any test run, operations fall into two sets from the harness's point of
view:

1. **Externally committed operations**:
   operations whose external durability marker was persisted before the crash.
2. **Externally uncommitted operations**:
   operations that started but did not cross that external marker before the
   crash.

After restart:

1. Every externally committed operation must be reflected in the reopened
   logical state.
2. Externally uncommitted operations may be absent.
3. Some externally uncommitted operations may still appear after restart if
   they became WAL-durable inside the engine before the crash. This is the
   current durable-before-ack window documented by RFC 005, and the chaos
   oracle must treat it as allowed behavior.
4. No operation may appear partially applied.
5. Metadata may lag, but it must remain self-consistent and reopenable.

### 6.2 Durability Boundaries by Operation Class

The full chaos-testing design should eventually cover the boundaries
kv-engine already exposes today. Phase 1 starts with the subset called out in
Sections 14 and 15.

1. **WAL-backed point write batch**:
   durable after the write path finishes the WAL sync/commit boundary.
2. **Range delete batch**:
   durable after the range-tombstone WAL batch reaches the same boundary.
3. **Flush/compaction outputs**:
   durable only after the SST artifact is synced and the corresponding manifest
   publish step is durable.
4. **Manifest snapshot replacement**:
   durable only after the snapshot temp file, manifest truncation, rename, and
   parent directory sync are complete.

### 6.3 What the Harness Must Reject

1. Reopen panic or permanent startup failure.
2. Durable write lost after clean reopen.
3. Durable delete or range tombstone lost, causing key resurrection.
4. Partial batch visibility.
5. Manifest pointing at unreadable or absent SST state.
6. WAL replay duplicating or reordering committed batches.

---

## 7. Design Overview

### 7.1 Two-Layer Approach

The RFC intentionally splits chaos testing into two layers.

#### Layer A: Process-Level Crash Harness (MVP)

This layer uses real child processes and `SIGKILL` to emulate abrupt process
death with no cleanup handlers.

Properties:

1. Tests actual reopen behavior.
2. Exercises foreground and background threads together.
3. Requires no production failpoint dependency to be useful.
4. Matches the user-visible crash contract most directly.

This is analogous to RocksDB's blackbox crash loop: real process execution,
external state tracking, repeated kill/reopen, then explicit verification.

#### Layer B: Internal Failpoints (Follow-Up)

This layer adds deterministic error injection at narrow internal steps that are
difficult to hit reliably with pure kill timing.

Properties:

1. Reaches rare error paths such as "write succeeded, follow-up fsync failed".
2. Produces smaller, faster, more targeted repros.
3. Complements, but does not replace, process killing.

This is inspired by RocksDB's broader whitebox idea of internal crash/fault
injection, where internal kill sites expand coverage beyond timing-based
process termination alone.

The MVP should implement Layer A first.

---

## 8. Process-Level Harness Design

### 8.1 Binary Layout

Add a small test-only binary, for example:

```text
kv-engine/src/bin/chaos-child.rs
```

This child process:

1. opens a database path with options provided by CLI or environment;
2. executes a scripted workload;
3. appends operation-intent and operation-commit records to a separate control
   log outside the database directory;
4. optionally sleeps/yields at selected checkpoints;
5. either exits normally or is killed by the parent harness.

The parent harness lives in the test suite, for example:

```text
kv-engine/src/tests/chaos.rs
```

The parent:

1. creates temp directories for the database and control log;
2. launches the child with a seed and scenario;
3. waits until a target point or timeout;
4. kills the child with `SIGKILL`;
5. reopens the database in-process;
6. replays the control log to compute the minimum required post-crash state;
7. validates invariants.

### 8.2 Why a Separate Control Log

The harness must not infer durability from the database files after the crash;
that would be circular.

Instead, the child writes a **test control log** to a file that is separate from
the kv-engine directory and fsynced independently. The control log records:

1. operation id;
2. operation kind;
3. logical payload summary;
4. `intent_started`;
5. `durability_boundary_passed`.

Only records whose `durability_boundary_passed` marker was persisted before the
kill are required to appear after restart.

Records without that marker are not automatically required to be absent. Some
may already be durable inside the engine at crash time and may legitimately
reappear on recovery, depending on where the crash landed relative to WAL
durability and external acknowledgment.

This gives the parent a conservative, externally visible oracle.

That choice is directly informed by RocksDB's use of an external expected-state
directory. The exact representation can be simpler in kv-engine, but the core
rule should stay the same: the oracle must live outside the database under
test.

### 8.3 Operation Model

The full process-level harness should eventually drive a small set of real
operations.

For Phase 1, prefer the current public/test-exposed `KvEngine` surface over
private `LsmStorageInner` hooks so the harness stays runnable from a standalone
child binary without special in-process privileges.

Recommended operations:

1. `put(key, value)`
2. `delete(key)`
3. `write_batch(point writes / tombstones)`
4. `delete_range(start, end)`
5. `sync()`
6. `force_flush()`
7. `drain_flush()`
8. `force_full_compaction()`
9. `add_compaction_filter` / `remove_compaction_filter` in targeted scenarios

The workload need not be large. What matters is crossing crash-sensitive state
transitions frequently.

### 8.4 Scenario Families

The full process-level harness should cover deterministic scenario families
rather than one giant random fuzzer. This differs from RocksDB's broader
parameter randomization on purpose: kv-engine should begin with narrow,
explainable crash contracts before expanding into larger stress matrices.

Recommended families:

1. **WAL-only restart**
   many small writes, kill during active write traffic, reopen, check durable
   prefix.
2. **Range tombstone restart**
   mix puts with `delete_range`, kill before and after explicit sync points,
   reopen, ensure covered keys stay deleted.
3. **Flush boundary**
   generate immutable memtables, trigger flush, kill around freeze/flush timing,
   reopen, check no durable keys disappear and no unpublished SST leaks into the
   logical state.
4. **Manifest snapshot boundary**
   set `manifest_snapshot_threshold_bytes` to a small threshold such as `1024`
   bytes, force snapshot churn, kill during repeated reopen-sensitive metadata
   updates, and validate black-box restart behavior under snapshot churn.
   Precise internal windows such as snapshot rename and parent-directory sync
   become directly targetable only in the later failpoint layer.
5. **Compaction boundary**
   under leveled and tiered compaction, create overlap, let background
   compaction run, kill mid-workload, reopen, and validate black-box logical
   state. Precise internal compaction publish boundaries become directly
   targetable only in the later failpoint layer.
6. **vLog restart**
   enable value separation, write large values, flush and compact, kill, reopen,
   ensure no dangling or resurrected values.

Each scenario should run with a reproducible seed and bounded runtime.

Phase 1 implements only the subset listed in Sections 14 and 15: WAL-only,
flush-boundary, and manifest-snapshot churn scenarios. Range tombstone,
compaction, and vLog scenarios are explicit follow-up work for later phases.

The matrix must also sanitize combinations that are invalid in the current
engine. In particular, `delete_range` is currently unsupported with
`value_separation` enabled and unsupported in `serializable` mode, so those
combinations should be excluded from MVP chaos runs rather than reported as
coverage gaps.

### 8.5 Configuration Matrix

The full harness matrix should stay small enough for normal local use.

Recommended full-harness matrix:

1. `enable_wal = true`, `value_separation = None`, `serializable = false`
2. `enable_wal = true`, `value_separation = Some(enabled)`, `serializable = false`
3. `enable_wal = true`, `delete_range` scenarios enabled
4. `enable_wal = true`, `manifest_snapshot_threshold_bytes = 0` for the
   no-snapshot baseline
5. `enable_wal = true`, `manifest_snapshot_threshold_bytes = 1024` for
   manifest-snapshot churn
6. `compaction = NoCompaction | Leveled | Tiered`

`enable_wal = false` is lower priority for crash semantics because the engine
does not promise durability without WAL; it can still be covered by a small
sanity scenario to ensure reopen does not panic.

Phase 1 uses the subset required by Sections 14 and 15 rather than the entire
matrix above.

---

## 9. Follow-Up Failpoint Layer

### 9.1 Motivation

Some bugs are hard to hit with timing-based `SIGKILL` alone:

1. write syscall succeeds but sync fails;
2. manifest record append succeeds but directory sync fails;
3. SST file exists but publish step errors;
4. background task returns error after partially preparing state.

Those cases justify lightweight internal failpoints.

### 9.2 Failpoint Shape

The failpoint mechanism should be:

1. test-only;
2. opt-in behind a feature flag so it is usable from the standalone child
   binary;
3. explicit by string name;
4. able to inject either:
   1. process abort,
   2. panic,
   3. returned error.

Conceptually:

```rust
chaos::hit("wal.after_write_before_fsync")?;
chaos::hit("manifest.after_append_before_sync")?;
chaos::hit("flush.after_sst_sync_before_manifest")?;
```

### 9.3 Initial Failpoint Targets

1. `wal.after_batch_encode`
2. `wal.after_submit_before_wait`
3. `wal.after_fsync_before_publish`
4. `manifest.after_append_before_sync`
5. `manifest.after_snapshot_tmp_sync`
6. `manifest.after_truncate_before_rename`
7. `manifest.after_rename_before_dir_sync`
8. `flush.after_sst_sync_before_manifest`
9. `compaction.after_output_sync_before_manifest`
10. `vlog.after_append_before_index_publish`

The exact list can evolve, but these cover the highest-value boundaries.

---

## 10. Validation Strategy

### 10.1 Logical Oracle

The parent harness should reconstruct the expected post-crash state from the
control log, then compare it against reopened reads.

For the MVP, keep validation simple and strong:

1. use a bounded/generated key universe for each scenario;
2. maintain a reference map in the parent from externally committed
   operations, while also tracking operations that may legitimately appear due
   to the durable-before-ack window;
3. after reopen, reconcile the full bounded key universe rather than only the
   subset of touched keys;
4. verify visible values and tombstones;
5. for range-delete scenarios, verify keys inside covered spans are absent and
   keys outside remain intact.

If the initial implementation cannot yet reconcile the full bounded key
universe, the RFC should treat that as an explicitly weaker Phase 0 oracle
rather than presenting it as equivalent to RocksDB-style expected-state
verification.

### 10.2 Structural Checks

In addition to logical reads, the harness should assert:

1. reopen succeeds without manual repair;
2. repeated reopen succeeds twice in a row;
3. `sync()` and `close()` after reopen succeed;
4. optional follow-up write after reopen succeeds.

These extra checks catch latent metadata inconsistencies that a single `get()`
pass might miss.

### 10.3 Reproducibility

Every failure report must include:

1. scenario name;
2. seed;
3. config options;
4. child stdout/stderr;
5. control log path;
6. number of committed operations;
7. exact parent kill trigger;
8. child checkpoint/event stream up to the kill;
9. operation id at or near the kill point.

If a chaos test cannot be replayed deterministically, it is not very useful.

---

## 11. Test Placement and Execution

### 11.1 Repository Placement

Recommended layout:

```text
kv-engine/src/tests/chaos.rs
kv-engine/src/tests/chaos/
  child.rs
  control_log.rs
  scenarios.rs
kv-engine/src/bin/chaos-child.rs
```

If the child logic can be shared as a library module instead of a standalone
binary, that is acceptable, but the parent must still be able to kill a real
process.

### 11.2 Command Surface

The chaos tests should run under normal Rust test tooling, for example:

```bash
cargo nextest run --workspace --all-features chaos
```

Optionally add a cargo-make task later:

```bash
cargo make test-chaos
```

### 11.3 CI Policy

Do not put the full chaos matrix into the default fast path immediately.

Recommended rollout:

1. local-only during initial development;
2. one or two short deterministic chaos scenarios in CI;
3. a larger nightly or manual job once stable.

---

## 12. Alternatives Considered

### 12.1 Only Unit Tests

Rejected because unit tests cannot validate actual process death and reopen
semantics across multiple files and background threads.

### 12.2 Only Random File Corruption

Rejected as the primary approach because it tests parser robustness more than
the engine's declared crash contract.

### 12.3 Only Failpoints

Rejected for the MVP because failpoints are excellent for narrow paths but can
miss real cross-thread and restart behavior unless backed by actual process
death tests.

### 12.4 External Jepsen-Style Distributed Testing

Out of scope. kv-engine is a local embedded engine, not a distributed service.

---

## 13. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Flaky timing-based kills | Low signal, hard-to-reproduce failures | Use deterministic scripted checkpoints plus seeds; keep scenarios bounded |
| Control log becomes the weak link | False positives or false negatives | Write it separately and fsync each durability marker |
| Chaos jobs are too slow for normal iteration | Developers stop using them | Keep MVP scenarios short and matrix narrow |
| Too much internal test-only plumbing | Maintenance burden | Start with process-level harness; add failpoints only for proven gaps |
| Assertions are too weak | Bugs slip through | Reconcile the full bounded key universe, not just reopen success |

---

## 14. Implementation Plan

### Phase 1: MVP Harness

1. Add control-log format and parser.
2. Add child-process driver for a small deterministic workload language.
3. Add parent test harness that kills child processes and reopens the DB.
4. Implement a bounded-key-universe oracle that reconciles full post-crash
   state for the selected scenarios, including the durable-before-ack window.
5. Implement WAL-only, flush-boundary, and manifest-snapshot scenarios.
6. Treat manifest-snapshot coverage in Phase 1 as black-box churn coverage under
   repeated kill/reopen loops, not as precise internal boundary injection.
7. Add reproducible failure reporting.

### Phase 2: Feature Coverage

1. Add range tombstone scenarios.
2. Add vLog scenarios.
3. Add compaction scenarios for leveled and tiered modes.
4. Add a second reopen/write/reopen validation pass.

### Phase 3: Failpoints

1. Add feature-gated failpoint registry usable from the standalone child
   binary.
2. Instrument high-value durability boundaries.
3. Add narrow tests for injected error paths that process killing cannot target
   reliably.

### Phase 4: RocksDB-Style Seeded Stress Testing

1. Add `stress` module with deterministic seeded workload generation covering
   randomized configurations (compaction mode, WAL, serializable, vlog, SST size,
   manifest snapshot threshold).
2. Implement alternating stress/verify cycle planning with flush/compact
   interleaving.
3. Add bounded key universe, randomized value sizes, and write-batch operations.
4. Add crash-loop smoke test (`phase4_stress_crash_loop_smoke`) with full
   oracle reconciliation, structural checks, and failure artifact capture.
5. Add `chaos-child` binary support for stress scenario replay and
   cycle/time-bounded runs.
6. Add CI integration: nightly chaos-stress job, nextest timeout overrides,
   and Makefile tasks.

---

## 15. Acceptance Criteria

This RFC is accepted when:

1. The repository has at least one committed chaos-test entry point that kills a
   real kv-engine process and validates restart semantics.
2. The harness covers, at minimum:
   1. WAL-backed point writes,
   2. manifest snapshot churn,
   3. flush/reopen behavior.
3. Phase 1 includes the bounded-key-universe reconciliation oracle described in
   Section 10.1, not only touched-key validation.
4. Phase 1 manifest-snapshot coverage is judged as black-box churn coverage
   under repeated kill/reopen loops, not boundary-precise injection.
5. Failures report enough information to reproduce locally from a seed.
6. The tests run reliably enough for repeated local execution.
7. At least one short chaos scenario is suitable for CI after stabilization.

This acceptance bar is intentionally scoped to Phase 1. Compaction, range
tombstone, and vLog chaos coverage remain required follow-up work from the RFC,
but are not required to complete the initial landing of the harness.

---

## 16. Open Questions

1. Should the control log record only operation-level durability, or also
   intermediate checkpoints such as "freeze started" and "flush finished" for
   sharper diagnostics?
2. Do we want a lightweight model of expected MVCC visibility in the chaos
   oracle now, or should MVCC-specific scenarios wait until the non-transaction
   harness is stable?
3. Should failpoints use a small homegrown mechanism, or a crate such as
   `fail` behind a test-only feature?

---

## 17. Recommendation

Implement the process-level crash harness first.

That gives the best value quickly because it validates the initial reopen
contract slice of kv-engine as shipped: WAL recovery, manifest replay, and SST
publish ordering under black-box kill/reopen loops. After that, expand into
range tombstone, compaction, and vLog restart coverage, and add failpoints only
where real bugs or coverage gaps justify the extra instrumentation.
