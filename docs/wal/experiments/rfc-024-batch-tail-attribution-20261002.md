# RFC 024: ext4 batch tail — attribution and the gate design

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

**Status:** attribution complete; the gate was built, measured, and rejected — see
[the gate record](rfc-024-bg-io-gate-20261002.md). The attribution below stands:
the remaining levers are flush-event count (SST size) and nothing else tried.

## Attribution (all ext4, batch64, 8 writers, 65,536 puts, 1,024 commits)

| experiment | result | excludes |
| --- | --- | --- |
| leader vs parallel arm | p99 23.2 vs 22.8 ms | the WAL submit path and the parallel pipeline |
| SST target 1 / 2 / 4 / 8 / 64 MiB / 1 GiB | p99 17.9 / 23.2 / 21.8 / 28.9 / 3.4 / 2.35 ms; ops/s 162k / 187k / 211k / 203k / 292k / 375k | "intrinsic" tail - it tracks rotation/flush pressure, non-monotonically in SST size |
| compaction `leveled` vs `none` (1 MiB) | p99 19.2 vs 18.6 ms | compaction |
| memtable limit 2 vs 16 (1 MiB) | p99 16.9 vs 34.9 ms | writer blocked on the memtable limit - more flush backlog is worse |

**Conclusion:** these rotating-workload measurements support device-latency
amplification from concurrent flush I/O in both arms. They do not invalidate
the qualification's isolated batch64 results: that matrix used a 1 GiB SST
target, while the flush-pressure experiments here use smaller targets. The
one-writer +42.1% and eight-writer +16.7% qualification results remain recorded
with their original repeat controls; attribution of the latter is unresolved.
The engine's default SST target
(2 MiB) sits near the non-monotone part of the curve, so raising the default to
8 MiB was measured and rejected (p99 28.9 ms vs 23.2 ms, max 42.2 vs 24.7 ms).

Device context: `/sys/block/nvme0n1/queue/scheduler` is `none` (I/O priorities
are not honoured) with WBT active at `wbt_lat_usec=2000`, which is looser than
this SSD tolerates under flush load.

## The fix: a foreground-aware background-I/O gate

1. **One background write burst at a time** - flush and compaction acquire a
   shared token before an SST write burst, so at most one background stream is
   in flight however many imm memtables exist. This decouples pacing from the
   memtable limit, which today is the only throttle and works by stalling
   writers.
2. **A sync gate against the WAL** - the burst waits for no outstanding WAL
   `fdatasync` before starting; the WAL side signals at a chunk boundary. This
   targets the measured coupling directly.
3. **Idle bypass** - with no writes in flight the gate stays open, so compaction
   catch-up and bulk load are not slowed (the memtable-limit-2 regime is the
   failure mode to avoid).

### Hook points

- New `bg_io` module: an `AtomicUsize` sync counter plus a `parking_lot::Mutex`
  burst token, both `const`-constructible statics for the prototype; an env
  switch (`KV_WAL_IO_GATE`) selects gated vs ungated so both arms run on one
  binary. Shippable form: share an `Arc` gate between `LsmStorageInner` and
  `Wal` instead of a process global.
- `kv-engine/src/wal.rs`, `submit_sqes_and_poll`: RAII guard around the
  `libc::fdatasync` loop (the leader's sync).
- `kv-engine/src/lsm_storage.rs`, `force_flush_next_imm_memtable` (line ~11713):
  burst guard around the SST write + fsync only, leaving the PITR segment-WAL
  unlink logic and the manifest update untouched. Compaction's SST write takes
  the same token.

### Acceptance

Same-binary A/B, three ABBA blocks on the failing shape (batch64, 8 writers,
ext4, 2 MiB SST): p99 down from ~23 ms toward the ~3 ms the device shows when
unloaded, throughput not below the 187k ops/s baseline, and no regression on the
canonical tmpfs `wal_concurrent` case. Null-pair offset on this host is ~1.1, so
three blocks are the minimum; report every block, not a passing subset.

### Risk

An over-strict pacer recreates the limit-2 stall regime - throughput loss, not
just latency. Measure; do not assert.
