# RFC 012: Parallel WAL with Multiple Loggers

**Status:** Proposed
**Date:** 2026-06-26
**Author:** kv-engine Contributors
**Reference:** SpanDB (FAST 2021) — "SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage"

---

## 1. Summary

This RFC proposes adding parallel WAL (Write-Ahead Log) logging to kv-engine,
inspired by the SpanDB paper's multi-logger design. The paper's parallel WAL
combines three techniques:

1. **Log batching** — multiple concurrent writers' entries collected into a single batch per shard (group commit).
2. **Parallel writers** — N independent WAL shards fsync simultaneously across shards.
3. **Atomic page allocation ordering** — within each shard, batches receive sequential file offsets via atomic allocation, preserving write ordering without serializing I/O.

Instead of a single WAL file per memtable with one leader thread performing all
writes and fsyncs, the engine would maintain N independent WAL shards per memtable.
Each shard retains group commit for batching efficiency, while different shards
commit in parallel.

The design provides:

1. N parallel WAL loggers per memtable (configurable, default 1 for backward compatibility).
2. Group commit per shard — batching multiple concurrent writers into one fsync.
3. Parallel fsync across all shards — N group commits proceed simultaneously.
4. Atomic offset ordering — batches within a shard are ordered by sequentially allocated file offsets.
5. Full backward compatibility — single-shard mode uses the existing WAL file format and naming.
6. Compatible with MVCC, vLog, compaction, and all existing WAL features.

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

### Why Not Just Group Commit or Just Parallel Shards?

**Group commit alone** (current design) batches writers efficiently but all shards
share one fsync — N writers wait for one fsync.

**Parallel shards alone** (no batching) allows parallel fsync but writers on the
same shard serialize — each writer does its own fsync.

**SpanDB's design** combines both: batch within each shard (group commit), then
fsync all shards in parallel. The result is N batches fsyncing simultaneously,
each batch amortizing fsync across its own group of writers.

```
                    ┌─ Shard 0: writers ─→ batch ─→ leader writes ─→ fsync ─┐
                    │                                                        │
All writers ────────┼─ Shard 1: writers ─→ batch ─→ leader writes ─→ fsync ─┤ parallel
(round-robin)       │                                                        │
                    └─ Shard 2: writers ─→ batch ─→ leader writes ─→ fsync ─┘
```

The paper's "3L3R" configuration (3 loggers × 3 concurrent requests per logger)
achieved peak WAL throughput on Intel Optane.

---

## 3. Goals

1. Implement N parallel WAL shards per memtable, each with group commit.
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

### 5.1 SpanDB's Three Techniques

#### 5.1.1 Log Batching (Group Commit per Shard)

Each shard implements the existing leader-follower group commit:

1. Writers push encoded buffers to the shard's `ready_queue`.
2. The first writer becomes the leader, drains the queue, writes to the file, and fsyncs.
3. Other writers wait on the shard's condvar, then return when the leader signals completion.

This amortizes fsync cost across concurrent writers on the same shard. The existing
`Wal::submit_and_commit()` logic is reused unchanged per shard.

#### 5.1.2 Parallel Writers (Cross-Shard Parallelism)

The `ParallelWal::submit_and_commit()` triggers all shards' group commits
simultaneously using `std::thread::scope`. Each shard's leader independently
drains its queue, writes to its file, and fsyncs. Writers on different shards
do not block each other.

```
Shard 0: leader_0 drains → write → fsync ─┐
Shard 1: leader_1 drains → write → fsync ─┤ all in parallel
Shard 2: leader_2 drains → write → fsync ─┘
```

#### 5.1.3 Atomic Page Allocation Ordering

Within each shard, batches must be written to the file in the order they were
submitted, even though multiple writers contribute buffers concurrently. The current
design handles this via the `ready_queue` (FIFO) and a single leader doing the
write — the leader drains all buffers in queue order and writes them sequentially.

To support future I/O parallelism (e.g., io_uring with multiple in-flight writes),
each batch can be assigned a sequential file offset atomically:

```rust
/// Atomic offset allocator for a single WAL shard.
/// Each batch gets a unique, sequential region in the file.
struct OffsetAllocator {
    next_offset: AtomicU64,
}

impl OffsetAllocator {
    /// Reserve `size` bytes at the current end of the WAL.
    /// Returns the starting offset for this batch.
    fn allocate(&self, size: u64) -> u64 {
        self.next_offset.fetch_add(size, Ordering::AcqRel)
    }
}
```

This ensures:
- Batches are assigned non-overlapping, sequential file regions.
- Multiple writers can prepare their buffers concurrently.
- The leader writes buffers at their allocated offsets (currently sequential; future io_uring can submit in parallel with offset ordering).

**Current implementation:** The offset allocator is implicit — the leader drains the
FIFO queue and writes sequentially. The atomic offset tracking is added for correctness
documentation and future io_uring support.

### 5.2 New `ParallelWal` Struct

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

### 5.3 WAL File Naming

| Shard Count | File Names |
|-------------|-----------|
| 1 (default) | `{id:05}.wal` (unchanged) |
| N > 1 | `{id:05}.wal.0`, `{id:05}.wal.1`, ..., `{id:05}.wal.{N-1}` |

### 5.4 Write Path

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
even load across shards. Within each shard, the batch enters the shard's
`ready_queue` for group commit.

### 5.5 Group Commit (Parallel Fsync Across Shards)

```rust
impl ParallelWal {
    pub fn submit_and_commit(&self) -> Result<()> {
        if self.shards.len() == 1 {
            return self.shards[0].submit_and_commit();
        }

        // Trigger all shards' group commits in parallel.
        // Each shard's leader independently drains, writes, and fsyncs.
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
- Each shard's `submit_and_commit()` runs its own group commit independently.
- All shards' leaders write and fsync in parallel.
- The call returns only when ALL shards have fsynced.
- If any shard fails, the error is propagated.

### 5.6 Recovery

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

### 5.7 MemTable Integration

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
- `commit_wal()` → `wal.submit_and_commit()` (parallel group commit across shards)
- `recover_from_wal()` → `ParallelWal::recover(path, num_shards, skiplist)`

### 5.8 WAL Cleanup

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

### 5.9 Configuration

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

1. **Best of both worlds** — Group commit amortizes fsync within each shard; parallel shards amortize across shards.
2. **Parallel fsync** — Multiple shards fsync simultaneously, utilizing NVMe internal parallelism.
3. **Pipelining** — While one shard fsyncs, others collect writes.
4. **Reduced contention** — Writers on different shards never share a mutex.
5. **Simple per-shard logic** — Each shard reuses the existing, well-tested `Wal` group commit.
6. **Backward compatible** — `num_wal_shards=1` is identical to current behavior.

### Disadvantages

1. **Multiple files per memtable** — N WAL files instead of 1. Increases file descriptor usage.
2. **Recovery complexity** — Must read and merge N files instead of 1.
3. **No cross-shard ordering guarantee** — Relies on MVCC timestamps for correctness.
4. **SSD firmware serialization** — On some SSDs, parallel fsyncs may be serialized by the firmware. The pipelining benefit still applies.
5. **Increased disk space** — Each shard has its own WAL header and padding.

### Comparison: Group Commit vs Parallel Shards vs SpanDB

| Approach | Fsync per batch | Parallelism | Complexity |
|----------|----------------|-------------|------------|
| Group commit only (current) | 1 fsync for N writers | None (single fsync) | Low |
| Parallel shards only (no batching) | 1 fsync per writer | N fsyncs in parallel | Low |
| **SpanDB (batching + parallel)** | 1 fsync for M writers per shard | N fsyncs in parallel | Medium |

With SpanDB's design and 8 writer threads, 4 shards:
- Each shard has ~2 writers → 1 fsync per shard (group commit)
- 4 shards → 4 fsyncs in parallel
- Total: 4 fsyncs instead of 8 (no batching) or 1 (current)

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
2. **Rayon vs std::thread::scope:** For parallel fsync, should we use rayon or `std::thread::scope`? Recommendation: `std::thread::scope` — no new dependency, and the scope is tiny (N joins).
3. **Shard assignment strategy:** Round-robin is simple but may not be optimal if batches vary in size. Recommendation: start with round-robin, optimize later if benchmarks show imbalance.
4. **io_uring integration:** The atomic offset allocator (Section 5.1.3) prepares for future io_uring support where multiple batches can be submitted as parallel writes to the same shard. This is deferred to RFC 002.
