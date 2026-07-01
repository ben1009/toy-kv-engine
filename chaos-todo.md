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

## Phase 3: Whitebox Failpoints (not started)

- [ ] Feature-gated failpoint registry (`chaos::failpoint` module behind `chaos-testing`)
- [ ] Instrumentation targets:
  - `wal.after_batch_encode`
  - `wal.after_submit_before_wait`
  - `wal.after_fsync_before_publish`
  - `manifest.after_append_before_sync`
  - `manifest.after_snapshot_tmp_sync`
  - `flush.after_sst_sync_before_manifest`
  - `compaction.after_output_sync_before_manifest`
- [ ] Deterministic flush-boundary tests using failpoints instead of SIGKILL timing
- [ ] Deterministic manifest-snapshot tests using failpoints

## Minor Improvements

- [ ] Pass file paths as `OsStr` instead of `to_str().unwrap()` in parent harness
- [ ] Add nextest config overrides for chaos tests (slow-timeout, retries)
- [ ] Add `cargo make test-chaos` task
- [ ] Run chaos tests in nightly CI only (not PR default)
