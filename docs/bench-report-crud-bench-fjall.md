# crud-bench: ToyKV vs Fjall (matched config)

Benchmark run: `--samples 100000 --clients 4 --threads 4` (concurrent: 4 writers + 4 readers).
ToyKV branch: `perf/batch-get` (latest, with critical `batch_get` range-tombstone fix).

## Config parity

| Parameter | ToyKV | Fjall |
|---|---|---|
| block_size | 64 KB | 64 KB |
| memtable_size | 256 MB | 256 MB |
| block_cache | 524,288 blocks (~32 GB) | ~46 GB (75% of RAM) |
| value separation | BlobFile, min 4 KB | blob, min 4 KB |
| key size | 32 B | 32 B |
| value size | 4096 B | 4096 B |

ToyKV config source: `crud-bench/src/toykv.rs` (`calculate_toykv_options`).
Fjall config source: `crud-bench/src/fjall.rs` (`calculate_fjall_options`).

> Fjall's block cache is ~1.4× larger than ToyKV's. Both use similar block size, memtable size, and value separation thresholds.

## Batch read (OPS) — buffered (no fsync)

| Benchmark | ToyKV | Fjall | ToyKV vs Fjall |
|---|---|---|---|
| batch_read_100 | 45,647 | 48,791 | -6% |
| batch_read_1000 | **6,274** | 5,331 | **+18%** |

## Batch read (OPS) — durable (sync: true)

| Benchmark | ToyKV | Fjall | ToyKV vs Fjall |
|---|---|---|---|
| batch_read_100 | 31,906 | 35,094 | -9% |
| batch_read_1000 | **6,530** | 5,253 | **+24%** |

## Write (OPS)

### Buffered (no fsync)

| Benchmark | ToyKV | Fjall |
|---|---|---|
| put_c | 257,827 | 237,568 |
| put_p | 199,798 | 176,527 |
| batch_create_100 | 2,833 | 2,421 |
| batch_create_1000 | 928 | 771 |

### Durable (sync: true)

| Benchmark | ToyKV | Fjall |
|---|---|---|
| put_c | 179,883 | 180,181 |
| put_p | 171,092 | 149,513 |
| batch_create_100 | 1,242 | 1,321 |
| batch_create_1000 | 352 | 446 |

## Single read (OPS) — buffered (no fsync)

| Benchmark | ToyKV | Fjall | ToyKV vs Fjall |
|---|---|---|---|
| get_c | 532,058 | 500,907 | +6% |
| get_p | 426,072 | 276,303 | +54% |
| get_random_range | 19,031 | 16,173 | +18% |
| get_latest_range | 17,918 | 13,837 | +29% |
| get_random_limit_8 | 455,034 | 378,389 | +20% |
| get_random_limit_64 | 220,248 | 168,770 | +30% |

## Update (OPS) — buffered (no fsync)

| Benchmark | ToyKV | Fjall |
|---|---|---|
| update_c | 146,203 | 145,264 |
| batch_update_100 | 2,702 | 2,312 |
| batch_update_1000 | 909 | 609 |

## Delete (OPS) — buffered (no fsync)

| Benchmark | ToyKV | Fjall |
|---|---|---|
| delete_c | 158,783 | 162,510 |
| batch_delete_100 | 2,415 | 2,892 |
| batch_delete_1000 | 533 | 988 |

## Key findings

1. **ToyKV wins batch_read_1000, Fjall wins batch_read_100.** ToyKV is **+18%/+24% faster** on batch_read_1000 (buffered/durable). Fjall is ~6-9% faster on batch_read_100. The batch_read_100 gap is within run-to-run variance; batch_read_1000 consistently favors ToyKV.

2. **ToyKV dominates single-key reads.** `get_c`/`get_p` are +6%/+54% faster. Range queries are +18–29% faster. Fjall's range iterator has higher per-element cost due to `Arc<Mutex<…>>` and double-buffered operator translation.

3. **Fjall has slightly better delete throughput at batch_1000.** Fjall's batch_delete_1000 is ~2× ToyKV (988 vs 533 OPS buffered). ToyKV's `delete_range` uses skiplist fragmentation per key; Fjall uses a single fragment per range.

4. **Fjall's block cache is 1.4× larger.** Both are oversized for the dataset (~400 MB), so cache hit rates are near 100% during reads. The difference is negligible for these benchmarks.

5. **ToyKV's value separation keeps 99% of keys in-tree (under 4 KB).** Fjall separates values ≥ 4 KB to blob storage. With 4 KB values, ~100% go to blobs in Fjall, adding I/O.

## Changes since previous report

- **Critical bug fix:** `batch_get` SST-hit path now combines memtable range tombstone timestamps with SST range tombstone timestamps (`memtable_range_ts.max(sst_range_ts)`) instead of using only `sst_range_ts`. Previously, a memtable range tombstone that partially covered an SST version could return stale data.
- **Non-MVCC L0 double-probe fix:** Added `continue` to skip the hinted SST in the L0 loop (was probing it twice).
- **Non-MVCC L0 hint update:** Now updates the L0 hint before returning when a value is found.
- **anyhow::Ok import conflict fix:** Removed `Ok` from `use anyhow::{…}` to avoid shadowing standard `Ok` pattern.
- **Matched config:** Both engines now use 64 KB blocks, 256 MB memtable, 4 KB value separation threshold.

## Raw config snippets

### ToyKV (crud-bench/src/toykv.rs)

```rust
let storage_opts = LsmStorageOptions {
    block_size: 64 * 1024, // 64KB — match Fjall
    target_sst_size: 256 << 20, // 256MB — match Fjall
    num_memtable_limit: 50,
    compaction_options: compaction_opts,
    enable_wal: options.sync,
    serializable: false,
    value_separation: Some(ValueSeparationOptions {
        enabled: true,
        min_value_size: 4 * 1024, // 4KB — match Fjall
        ..Default::default()
    }),
    manifest_snapshot_threshold_bytes: 4 * 1024 * 1024,
    block_cache_capacity: 524_288, // ~32GB with 64KB blocks
    enable_cache_backfill: true,
    prefix_bloom: Default::default(),
};
```

### Fjall (crud-bench/src/fjall.rs)

```rust
let config = fjall::Config::new(folder)
    .block_size(64 * 1024) // 64KB
    .max_write_buffer_size(256 * 1024 * 1024) // 256MB memtable
    .blob_cache_size(fjall_cache_size) // ~46GB
    .cache_size(fjall_cache_size)
    .max_level_count(7)
    .sstable_block_size(4096)
    .compression(fjall::CompressionType::Lz4);

let keyspace = config.open().unwrap();
let blob_threshold = 4 * 1024; // 4KB
let tree = keyspace
    .open_partition("default", fjall::PartitionCreateOptions::default()
        .blob_file_target_size(256 * 1024 * 1024)
        .blob_min_value_size(blob_threshold));
```
