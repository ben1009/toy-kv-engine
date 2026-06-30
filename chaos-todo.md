# Chaos Testing — TODOs & Known Issues

## Known Issue: force_flush + SIGKILL → data loss

**Symptom:** After `force_flush()` (or auto-flush via small `target_sst_size`) followed by SIGKILL, reopening the database returns **0 keys**. All WAL-recovered data is lost.

**Status:** Reproducible. Not yet root-caused.

| Crash method | force_flush | Result |
|---|---|---|
| `drop(engine)` (in-process) | Yes | 100/100 keys recovered |
| SIGKILL (real process death) | No | 100/100 keys recovered (WAL-only) |
| SIGKILL | Yes | 0/keys recovered |

**Hypothesis:** io_uring O_DIRECT write may not survive SIGKILL even after CQE completion + `fdatasync(2)`. The v4 WAL uses registered fds and fixed buffers. When the process is killed, the kernel cancels in-flight I/O. Completed CQEs might report success before data reaches the storage device under O_DIRECT semantics.

**Workaround:** All current chaos scenarios avoid `force_flush()` and use small values (below `target_sst_size`) so no auto-flush triggers. Flush-boundary testing is deferred to the Phase 3 whitebox failpoint layer.

**Investigation needed:**
- Reproduce with a minimal io_uring O_DIRECT test program (no kv-engine involved)
- Test with `sync_file_range` vs `fdatasync` after O_DIRECT writes
- Check if the issue reproduces on ext4 vs XFS vs btrfs
- Check kernel version dependency

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
