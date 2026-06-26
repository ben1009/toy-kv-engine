# RFC 012: Parallel WAL with Multiple Loggers

**Status:** Proposed
**Date:** 2026-06-26
**Author:** kv-engine Contributors
**Reference:** SpanDB (FAST 2021) — "SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage"

---

## 1. Summary

This RFC proposes adding parallel WAL (Write-Ahead Log) logging to kv-engine,
inspired by the SpanDB paper's multi-logger design. Instead of a single WAL file
per memtable with one leader thread performing all writes and fsyncs, the engine
would maintain N independent WAL shards per memtable, each with its own file,
buffer pool, and group commit mechanism. Writers are distributed across shards
using round-robin assignment, allowing multiple fsyncs to proceed in parallel.

The design provides:

1. N parallel WAL loggers per memtable (configurable, default 1 for backward compatibility).
2. Independent group commit per shard — concurrent writers on different shards do not block each other.
3. Parallel fsync across all shards during `submit_and_commit()`.
4. Full backward compatibility — single-shard mode uses the existing WAL file format and naming.
5. Compatible with MVCC, vLog, compaction, and all existing WAL features.

---

## 2. Motivation

### Current Bottleneck

The current WAL implementation (`wal.rs`) uses a single file per memtable with a
group-commit mechanism:

```
Writer threads → ready_queue (SegQueue) → leader drains → BufWriter → fsync
                                                    ↑
                                          single file Mutex
```

Under high concurrency, this creates a bottleneck:

- **Single writer:** One leader thread serially writes all batched buffers and fsyncs.
- **Mutex contention:** The file Mutex is held during the entire drain + write + fsync cycle.
- **Followers wait:** All non-leader threads block on the condvar until the leader finishes.

The SpanDB paper measured that RocksDB spends **81% of write request time** waiting
on the synchronous group logging path when using Optane via SPDK. While this engine
uses standard filesystem I/O (not SPDK), the same principle applies: the single-writer
bottleneck limits throughput under high concurrency.

### What Parallel WAL Achieves

With N shards, N groups of writers can commit simultaneously:

```
Shard 0: writers → ready_queue_0 → leader_0 → write → fsync  ─┐
Shard 1: writers → ready_queue_1 → leader_1 → write → fsync  ─┤ parallel
Shard 2: writers → ready_queue_2 → leader_2 → write → fsync  ─┘
```

Benefits:
- **Pipelining:** While shard A fsyncs, shard B collects new writes.
- **Reduced contention:** Writers on different shards never contend on the same mutex.
- **SSD parallelism:** NVMe SSDs have internal parallelism (multiple channels); parallel fsyncs on different files can utilize this.

The paper's "3L3R" configuration (3 loggers × 3 concurrent requests) achieved
peak WAL throughput on Intel Optane.

---

## 3. Goals

1. Implement N parallel WAL shards per memtable, each with independent group commit.
2. Keep backward compatibility — `num_wal_shards=1` uses the existing `{id:05}.wal` format.
3. No changes to the public `KvEngine` API.
4. Recovery must correctly merge entries from all shards.
5. WAL cleanup (during flush) must delete all shard files.
6. Configuration via `LsmStorageOptions::num_wal_shards`.

## 4. Non-Goals

1. SPDK integration (separate concern, see RFC 002 for io_uring).
2. Changing the WAL file format — each shard uses the existing v3 format.
3. Cross-shard ordering — MVCC timestamps handle correctness.
4. Dynamic shard count — fixed at engine creation time.

---

## 5. Design

### 5.1 New `ParallelWal` Struct

```rust
pub struct ParallelWal {
    /// N independent WAL shards.
    shards: Vec<Wal>,
    /// Atomic counter for round-robin shard assignment.
    next_shard: AtomicUsize,
}
```

Each shard is a full `Wal` instance with its own:
- File (`Arc<Mutex<BufWriter<File>>>`)
- Buffer pool (`ArrayQueue<Vec<u8>>`, 16 × 64KB)
- Ready queue (`SegQueue<Vec<u8>>`)
- Group commit state (`commit_waiters`, `committed_gen`, `leader_active`, etc.)

### 5.2 WAL File Naming

| Shard Count | File Names |
|-------------|-----------|
| 1 (default) | `{id:05}.wal` (unchanged) |
| N > 1 | `{id:05}.wal.0`, `{id:05}.wal.1`, ..., `{id:05}.wal.{N-1}` |

### 5.3 Write Path

```rust
impl ParallelWal {
    pub fn put_batch(&self, data: &[(&[u8], &[u8])], commit_ts: u64) -> Result<()> {
        let shard_idx = self.next_shard.fetch_add(1, Ordering::Relaxed) % self.shards.len();
        self.shards[shard_idx].put_batch(data, commit_ts)
    }

    pub fn put_range_tombstone_batch(
        &self,
        tombstones: &[(&[u8], &[u8])],
        commit_ts: u64,
    ) -> Result<()> {
        let shard_idx = self.next_shard.fetch_add(1, Ordering::Relaxed) % self.shards.len();
        self.shards[shard_idx].put_range_tombstone_batch(tombstones, commit_ts)
    }
}
```

Each batch is written to exactly one shard. The round-robin distribution ensures
even load across shards.

### 5.4 Group Commit (Parallel Fsync)

```rust
impl ParallelWal {
    pub fn submit_and_commit(&self) -> Result<()> {
        if self.shards.len() == 1 {
            return self.shards[0].submit_and_commit();
        }

        // Trigger all shards' group commits in parallel.
        let results: Vec<Result<()>> = std::thread::scope(|s| {
            let handles: Vec<_> = self.shards.iter()
                .map(|shard| s.spawn(|| shard.submit_and_commit()))
                .collect();
            handles.into_iter()
                .map(|h| h.join().unwrap_or_else(|_| anyhow::bail!("shard panic")))
                .collect()
        });

        // Return Ok only if all shards succeeded.
        for r in results {
            r?;
        }
        Ok(())
    }
}
```

Key points:
- Each shard's `submit_and_commit()` runs independently — writers on shard 0 do not wait for shard 1.
- The `submit_and_commit()` call returns only when ALL shards have fsynced.
- If any shard fails, the error is propagated.

### 5.5 Recovery

```rust
impl ParallelWal {
    pub fn recover(
        base_path: impl AsRef<Path>,
        num_shards: usize,
        skiplist: &SkipMap<Bytes, Bytes>,
    ) -> Result<(Self, u64)> {
        let mut shards = Vec::with_capacity(num_shards);
        let mut max_ts: u64 = 0;

        for i in 0..num_shards {
            let shard_path = if num_shards == 1 {
                base_path.as_ref().to_path_buf()
            } else {
                PathBuf::from(format!("{}.{}", base_path.as_ref().display(), i))
            };

            if shard_path.exists() {
                let (wal, ts) = Wal::recover(&shard_path, skiplist)?;
                shards.push(wal);
                max_ts = max_ts.max(ts);
            } else {
                // Shard file doesn't exist — create a new one.
                shards.push(Wal::create(&shard_path)?);
            }
        }

        Ok((Self {
            shards,
            next_shard: AtomicUsize::new(0),
        }, max_ts))
    }
}
```

Recovery correctness:
- Each shard is recovered independently using the existing `Wal::recover()` / `Wal::recover_with_range_tombstones()`.
- All shards replay into the same skiplist. Since the skiplist is a concurrent
  `SkipMap`, this is safe — MVCC timestamps handle ordering.
- `max_ts` is the maximum across all shards.

### 5.6 MemTable Integration

The `MemTable` struct changes from `wal: Option<Wal>` to `wal: Option<ParallelWal>`:

```rust
pub struct MemTable {
    // ...
    wal: Option<ParallelWal>,  // was: Option<Wal>
    // ...
}
```

Methods updated:
- `create_with_wal()` → `ParallelWal::create(path, num_shards)`
- `write_wal_batch()` → `wal.put_batch()` (round-robin internally)
- `commit_wal()` → `wal.submit_and_commit()` (parallel across shards)
- `recover_from_wal()` → `ParallelWal::recover(path, num_shards, skiplist)`

### 5.7 WAL Cleanup

During flush (`force_flush_next_imm_memtable`), delete all shard files:

```rust
fn cleanup_wal_files(path: &Path, id: usize, num_shards: usize) {
    if num_shards <= 1 {
        let wal_path = path.join(format!("{id:05}.wal"));
        let _ = std::fs::remove_file(wal_path);
    } else {
        for i in 0..num_shards {
            let wal_path = path.join(format!("{id:05}.wal.{i}"));
            let _ = std::fs::remove_file(wal_path);
        }
    }
}
```

### 5.8 Configuration

Add to `LsmStorageOptions`:

```rust
pub struct LsmStorageOptions {
    // ... existing fields ...
    /// Number of WAL shards per memtable. Default: 1 (single WAL, backward compatible).
    /// Values > 1 enable parallel WAL logging (recommended: 2-4 for NVMe SSDs).
    pub num_wal_shards: usize,
}
```

Default: `1` — no behavior change for existing users.

---

## 6. Trade-offs

### Advantages

1. **Parallel fsync** — Multiple shards fsync simultaneously, utilizing NVMe internal parallelism.
2. **Pipelining** — While one shard fsyncs, others collect writes.
3. **Reduced contention** — Writers on different shards never share a mutex.
4. **Simple per-shard logic** — Each shard reuses the existing, well-tested `Wal` implementation.
5. **Backward compatible** — `num_wal_shards=1` is identical to current behavior.

### Disadvantages

1. **Multiple files per memtable** — N WAL files instead of 1. Increases file descriptor usage.
2. **Recovery complexity** — Must read and merge N files instead of 1.
3. **No cross-shard ordering guarantee** — Relies on MVCC timestamps for correctness.
4. **SSD firmware serialization** — On some SSDs, parallel fsyncs may be serialized by the firmware. The pipelining benefit still applies.
5. **Increased disk space** — Each shard has its own WAL header and padding.

### When It Helps Most

- High concurrency writes (many writer threads).
- Fast NVMe SSDs where fsync latency is low but the software overhead dominates.
- Workloads where the single-writer group commit is the bottleneck.

### When It Helps Least

- Low concurrency (1-2 writer threads) — little contention to eliminate.
- SATA SSDs or HDDs — I/O latency dominates, not software overhead.
- Small memtables — frequent freezes negate the batching benefit.

---

## 7. Implementation Plan

| Phase | Description | Files |
|-------|-------------|-------|
| 1 | Add `ParallelWal` struct to `wal.rs` | `wal.rs` |
| 2 | Add `ParallelWal::recover()` and `recover_with_range_tombstones()` | `wal.rs` |
| 3 | Update `MemTable` to use `ParallelWal` | `mem_table.rs` |
| 4 | Update `lsm_storage.rs` recovery path | `lsm_storage.rs` |
| 5 | Update WAL cleanup to delete all shard files | `lsm_storage.rs` |
| 6 | Add `num_wal_shards` to `LsmStorageOptions` | `lsm_storage.rs` |
| 7 | Add tests | `tests/wal.rs` |

---

## 8. Testing Strategy

1. **Backward compatibility:** All existing tests pass with `num_wal_shards=1`.
2. **Multi-shard write/read:** Write with `num_wal_shards=2` and `4`, verify all data readable.
3. **Concurrent writers:** Multiple threads writing to the engine with parallel WAL.
4. **Recovery:** Write with N shards, close engine, reopen, verify all data.
5. **Mixed workloads:** put + delete + delete_range with parallel WAL.
6. **Benchmark:** `write-perf --workload wal_concurrent --threads 8` with varying shard counts.

---

## 9. Open Questions

1. **Optimal shard count:** The paper uses 3 loggers for Optane. Should we default to 2 or 4? Recommendation: 2 for NVMe, 1 for SATA/HDD.
2. **Rayon vs std::thread::scope:** For parallel fsync, should we use rayon (already a transitive dependency via crossbeam?) or `std::thread::scope`? Recommendation: `std::thread::scope` — no new dependency, and the scope is tiny (N joins).
3. **Shard assignment strategy:** Round-robin is simple but may not be optimal if batches vary in size. Weighted assignment based on buffer pool depth could be better. Recommendation: start with round-robin, optimize later if benchmarks show imbalance.
