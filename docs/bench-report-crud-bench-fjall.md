# crud-bench: ToyKV vs Fjall (matched config)

Benchmark run: `--samples 100000 --clients 4 --threads 4` (concurrent: 4 writers + 4 readers).
Focused post-optimization rerun: `--samples 100000 --clients 1 --threads 1 --skip-scans --skip-indexes`
(ToyKV and Fjall, sync and no-sync).
ToyKV branch: `perf/batch-get` (latest, with critical `batch_get` range-tombstone fix and `write_batch`
unique-key fast path).
Accepted focused rerun artifacts:
`/tmp/result-toykv_batch_opt_sync_100k.csv`, `/tmp/result-fjall_compare_sync_100k.csv`,
`/tmp/result-toykv_batch_opt_nosync_100k.csv`, `/tmp/result-fjall_compare_nosync_100k.csv`.

Note: later `/tmp/*opt2*` CSVs came from a rejected small-batch duplicate-scan experiment that regressed
`batch_create_100` and `batch_delete_100`; those numbers are intentionally not used here.

## Config parity

| Parameter | ToyKV | Fjall |
|---|---|---|
| block_size | 64 KB | 64 KB |
| memtable_size | 256 MB | 256 MB |
| block_cache | 524,288 blocks (~32 GB) | ~46 GB (75% of RAM) |
| value separation | BlobFile, min 4 KB | blob, min 4 KB |
| key size | 32 B | 32 B |
| value size | 4096 B | 4096 B |

ToyKV config source: `crud-bench/src/toykv.rs`.
Fjall config source: `crud-bench/src/fjall.rs`.

> Fjall is configured with a shared cache budget of about 46 GB, versus ToyKV's ~32 GB block cache. Both use the same
> 64 KB data-block size, 256 MB memtable target, and 4 KB value-separation threshold.

## Batch read (OPS) — buffered (no fsync)

| Benchmark | ToyKV | Fjall | ToyKV vs Fjall |
|---|---|---|---|
| batch_read_100 | **50,969** | 48,480 | **+5%** |
| batch_read_1000 | **6,553** | 5,042 | **+30%** |

## Batch read (OPS) — durable (sync: true)

| Benchmark | ToyKV | Fjall | ToyKV vs Fjall |
|---|---|---|---|
| batch_read_100 | **27,132** | 26,255 | **+3%** |
| batch_read_1000 | **5,966** | 5,352 | **+11%** |

## Write (OPS)

### Buffered (no fsync)

| Benchmark | ToyKV | Fjall |
|---|---|---|
| put_c | 257,827 | 237,568 |
| put_p | 199,798 | 176,527 |
| batch_create_100 | 2,833 | 2,421 |
| batch_create_1000 | 928 | 771 |

### Durable (sync: true, historical full-suite snapshot before `write_batch` fast path)

| Benchmark | ToyKV | Fjall |
|---|---|---|
| put_c | 179,883 | 180,181 |
| put_p | 171,092 | 149,513 |
| batch_create_100 | 1,242 | 1,321 |
| batch_create_1000 | 352 | 446 |

### Durable focused rerun after `write_batch` optimization

| Benchmark | ToyKV | Fjall | ToyKV vs Fjall |
|---|---:|---:|---:|
| put_c | **275,683** | 143,399 | **+92.2%** |
| delete_c | **943,063** | 221,160 | **+326.4%** |
| batch_create_100 | **3,059** | 2,872 | **+6.5%** |
| batch_create_1000 | **316** | 298 | **+6.0%** |
| batch_delete_100 | **10,146** | 7,991 | **+26.9%** |
| batch_delete_1000 | **1,738** | 1,401 | **+24.1%** |

## Single read (OPS) — buffered (no fsync)

| Benchmark | ToyKV | Fjall | ToyKV vs Fjall |
|---|---|---|---|
| get_c | 532,058 | 500,907 | +6% |
| get_p | 426,072 | 276,303 | +54% |
| get_random_range | 19,031 | 16,173 | +18% |
| get_latest_range | 17,918 | 13,837 | +29% |
| get_random_limit_8 | 455,034 | 378,389 | +20% |
| get_random_limit_64 | 220,248 | 168,770 | +30% |

## Scans (OPS) — buffered (no fsync)

| Scan | ToyKV | Fjall | ToyKV vs Fjall |
|---|---|---|---|
| count | 267 | 88 | **+3×** |
| limit select(id) | 319,851 | 109,764 | **+3×** |
| limit select(*) | 283,234 | 102,415 | **+3×** |
| start_limit select(id) | 8,419 | 1,292 | **+6.5×** |
| start_limit select(*) | 8,354 | 1,212 | **+6.9×** |

`where` scans not supported by either engine.

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

### Buffered focused rerun after `write_batch` optimization

Source CSVs: `/tmp/result-toykv_batch_opt_nosync_100k.csv` and `/tmp/result-fjall_compare_nosync_100k.csv`.

| Benchmark | ToyKV | Fjall | ToyKV vs Fjall |
|---|---:|---:|---:|
| put_c | **332,689** | 147,034 | **+126.3%** |
| batch_create_100 | **3,237** | 2,911 | **+11.2%** |
| batch_create_1000 | **342** | 295 | **+15.9%** |
| delete_c | **1,401,111** | 223,789 | **+526.1%** |
| batch_delete_100 | 8,138 | **11,379** | -28.5% |
| batch_delete_1000 | **2,071** | 1,301 | **+59.2%** |

## Key findings

1. **ToyKV wins all batch_read benchmarks.** After fixing a critical range-tombstone bug in `batch_get`, ToyKV is **+5%/+30% faster** (buffered) and **+3%/+11% faster** (durable) than Fjall with matched configs.

2. **ToyKV also leads the read-heavy non-batch workloads in this run.** `get_c`/`get_p` are +6%/+54% faster, range reads are +18–29% faster, and the buffered scan variants are roughly 3× to 7× faster.

3. **The focused post-optimization sync rerun wins all targeted durable write/delete cases.** With the `write_batch`
   unique-key fast path, ToyKV beats Fjall on sync `put_c`, `delete_c`, `batch_create_100`, `batch_create_1000`,
   `batch_delete_100`, and `batch_delete_1000` under the matched focused command.

4. **The remaining targeted loss is buffered `batch_delete_100`.** In the focused no-sync rerun, ToyKV wins
   `delete_c` and `batch_delete_1000`, but Fjall remains ahead on `batch_delete_100` (11,379 vs 8,138 OPS).

5. **Cache sizing is not a likely explanation for the read gap here.** Fjall's configured cache budget is larger, but both caches are far above the dataset size, so these runs are primarily measuring engine/read-path behavior rather than cache-capacity pressure.

6. **The value-separation threshold is matched, so this report should not claim ToyKV keeps 4 KB values inline.** ToyKV separates values when `value.len() >= 4 KiB`, and Fjall is configured with the same 4 KiB threshold, so both engines are exercising separated-value paths for payloads at or above that boundary.

## Changes since previous report

- **Critical bug fix:** `batch_get` SST-hit path now combines memtable range tombstone timestamps with SST range tombstone timestamps (`memtable_range_ts.max(sst_range_ts)`) instead of using only `sst_range_ts`. Previously, a memtable range tombstone that partially covered an SST version could return stale data.
- **Non-MVCC L0 double-probe fix:** Added `continue` to skip the hinted SST in the L0 loop (was probing it twice).
- **Non-MVCC L0 hint update:** Now updates the L0 hint before returning when a value is found.
- **anyhow::Ok import conflict fix:** Removed `Ok` from `use anyhow::{…}` to avoid shadowing standard `Ok` pattern.
- **Matched config:** Both engines now use 64 KB blocks, 256 MB memtable, 4 KB value separation threshold.
- **Write-batch unique-key fast path:** `write_batch` now avoids building and sorting a last-op map when the batch has
  unique keys, while preserving duplicate-key last-op-wins semantics.

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
let memory = calculate_fjall_memory();
let db = OptimisticTxDatabase::builder(DATABASE_DIR)
    .manual_journal_persist(!options.sync)
    .cache_size(memory)
    .max_journaling_size(1024 * 1024 * 1024)
    .worker_threads(num_cpus::get().min(8))
    .open()?;

let blob_opts = KvSeparationOptions::default()
    .separation_threshold(4 * 1024);
let keyspace_opts = KeyspaceCreateOptions::default()
    .data_block_size_policy(BlockSizePolicy::all(64 * 1_024))
    .max_memtable_size(256 * 1024 * 1024)
    .with_kv_separation(Some(blob_opts));

let keyspace = db.keyspace("default", || keyspace_opts)?;
```
