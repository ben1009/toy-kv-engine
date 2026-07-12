# Chaos Testing — TODOs & Known Issues

## Resolved: flush crash-path false failure

**Original symptom:** After repeated `force_flush()` calls followed by SIGKILL, the chaos harness reported `LostDurableWrite` for committed keys.

**Root cause:** This was **not** a flush durability failure. It was a combination of:

1. Persisted SST whole-key bloom hashing used `ahash` in a way that was not stable across processes, so a child-process write plus parent/restart-process reopen could make `get()` false-negative while `scan()` still saw the data.
2. Empty immutable memtables could be flushed into invalid empty SSTs.
3. Range-only / zero-point SSTs were encoded/opened with assumptions that made point/iterator paths treat them like point-bearing SSTs.

**Fix status:** Resolved on this branch.

**Validated:**
- WAL-only chaos scenario passes after SIGKILL.
- Manifest-snapshot chaos scenario passes after repeated flushes + SIGKILL.
- Cross-process persisted-bloom regression passes.
- Full `cargo test -q --package kv-engine` passes.

**Current workaround / policy:** Timing-sensitive flush-boundary crash windows should still move to deterministic Phase 3 failpoints instead of relying on process kill timing.

## Phase 2: Scenario Expansion (complete)

- [x] Range tombstone scenario — write + delete_range, kill, ensure covered keys stay deleted
- [x] vLog scenario — enable value separation, write large values, kill, ensure no dangling values
- [x] Compaction scenarios — leveled + tiered compaction, kill mid-compaction, validate
- [x] Second reopen pass — reopen, write more, reopen again to catch latent metadata issues

## Phase 3: Whitebox Failpoints (complete)

- [x] Feature-gated failpoint registry (`chaos::failpoint` module behind `chaos-testing`)
- [x] Instrumentation targets:
  - `wal.after_batch_encode`
  - `wal.after_submit_before_wait`
  - `wal.after_fsync_before_publish`
  - `manifest.after_append_before_sync`
  - `manifest.after_snapshot_tmp_sync`
  - `flush.after_sst_sync_before_manifest`
  - `compaction.after_output_sync_before_manifest`
- [x] Deterministic flush-boundary tests using failpoints instead of SIGKILL timing
- [x] Deterministic manifest-snapshot tests using failpoints

## Phase 4: RocksDB-Style Stress Testing (complete)

RocksDB's `db_crashtest.py` operates in kill/reopen *loops* with randomized parameters and alternating stress/verify phases. Our current harness does one-shot kill+validate. Phase 4 closes the gap.

### Kill/Reopen Loops
- [x] Multi-cycle loop: `open → write → kill → reopen → verify → write → kill → ...` (N cycles)
- [x] Configurable cycle count (seed-driven, default 10-100 cycles)
- [x] Per-cycle operation count randomization within bounds

### Randomized Workloads
- [x] Random operation sequence generator (put/delete/delete_range/flush/compact) driven by seed, constrained by active config
- [x] Serializable mode uses transaction blocks and retries on serialization conflicts
- [x] Random key selection from a bounded universe (not sequential)
- [x] Random value sizes (small/large/mixed, triggers value separation)
- [x] Random inter-operation timing (flush after N ops, compact after M flushes)

### Randomized Configuration
- [x] Random SST target size from a set: {4KB, 64KB, 256KB, 2MB}
- [x] Random compaction mode: {NoCompaction, Leveled, Tiered, Simple}
- [x] Random WAL setting: {on, off} (off = sanity-only/clean-reopen, or requires oracle adjustment to tolerate memtable data loss)
- [x] Random serializable mode: {on, off}
- [x] Random value separation: {on, off} with random min_value_size from {128B, 512B, 1KB, 4KB}
- [x] Random manifest snapshot threshold: {0, 256, 1024, 65536}
- [x] Config sanitizer to exclude incompatible combos (delete_range + vlog, delete_range + serializable)

### Stress/Verify Phase Alternation
- [x] Stress phase: kill-heavy, loose oracle (reopen must succeed, no crash)
- [x] Verify phase: kill-light, strict oracle (full key-universe reconciliation)
- [x] Alternation driven by cycle parity or explicit phase transitions

### Extended Runtime
- [x] Time-bounded mode: "run for 60 seconds, kill/reopen as fast as possible"
- [x] Cycle-bounded mode: "100 kill/reopen cycles"
- [x] Progress reporting: cycles completed and phase

### Reproducibility
- [x] Single seed deterministically generates workload, config, and kill points
- [x] Failure report includes: seed, cycle number, op sequence, config, LSM structure dump, control log path
- [x] Replay mode: `--seed 42` reproduces the same failure

### CI Integration
- [x] `cargo make test-chaos-stress` task (long-running, nightly only)
- [x] Short deterministic stress smoke test for PR CI (3-5 cycles, 10s limit)
- [x] Archive full DB directory as a CI artifact on failure

## Minor Improvements

- [x] Pass file paths as `OsStr` instead of `to_str().unwrap()` in parent harness
- [x] Add nextest config overrides for chaos tests (slow-timeout, retries)
- [x] Add `cargo make test-chaos` task
- [x] Run chaos tests in nightly CI only (not PR default)

## Current Entry Points

- `cargo make test-chaos` runs the failpoint, process-kill, and integration
  chaos suites.
- `cargo make test-chaos-stress` runs the longer stress smoke path.
- `kv-engine/tests/cross_process_bloom.rs` covers the persisted Bloom-filter
  regression that originally looked like a flush-durability bug.
