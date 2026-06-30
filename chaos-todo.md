# Chaos Testing — TODOs & Known Issues

## Resolved: flush crash-path false failure

**Original symptom:** After repeated `force_flush()` calls followed by SIGKILL, the chaos harness reported `LostDurableWrite` for committed keys.

**Root cause:** This was **not** a flush durability failure. It was two read-path / SST-encoding bugs:

1. MVCC SST point-get used whole-key bloom pruning on reopened flushed SSTs, so `get()` could false-negative while `scan()` still saw the data.
2. Empty immutable memtables could be flushed into invalid empty SSTs, and range-only SSTs were encoded/opened with assumptions that made point/iterator paths treat them like point-bearing SSTs.

**Fix status:** Resolved on this branch.

**Validated:**
- WAL-only chaos scenario passes after SIGKILL.
- Manifest-snapshot chaos scenario passes after repeated flushes + SIGKILL.
- Full `cargo test -q --package kv-engine` passes.

**Current workaround / policy:** Timing-sensitive flush-boundary crash windows should still move to deterministic Phase 3 failpoints instead of relying on process kill timing.

## Phase 2: Scenario Expansion (not started)

- [ ] Range tombstone scenario — write + delete_range, kill, ensure covered keys stay deleted
- [ ] vLog scenario — enable value separation, write large values, kill, ensure no dangling values
- [ ] Compaction scenarios — leveled + tiered compaction, kill mid-compaction, validate
- [ ] Second reopen pass — reopen, write more, reopen again to catch latent metadata issues

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
