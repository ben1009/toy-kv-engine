mod wrapper;

use std::io::Write as _;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use wrapper::kv_engine_wrapper;

use anyhow::Result;
use kv_engine_wrapper::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions},
    vlog::ValueSeparationOptions,
};
use rand::prelude::*;
use rand::rngs::StdRng;

fn make_options(vlog: bool, wal: bool) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 1 << 20,
        num_memtable_limit: 2,
        compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
            level_size_multiplier: 2,
            level0_file_num_compaction_trigger: 4,
            max_levels: 6,
            base_level_size_mb: 128,
        }),
        enable_wal: wal,
        serializable: false,
        value_separation: if vlog {
            Some(ValueSeparationOptions {
                enabled: true,
                min_value_size: 1024,
                max_value_size: 64 * 1024,
                max_vlog_file_size: 64 * 1024 * 1024,
                gc_threshold_ratio: 0.4,
                max_open_vlog_files: 4,
                value_cache_capacity_bytes: 16 * 1024 * 1024,
            })
        } else {
            None
        },
        manifest_snapshot_threshold_bytes: 4 << 20,
        block_cache_capacity: 8192,
    }
}

fn bench_scan(path: &str, num_entries: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; 256];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.force_flush()?;
    engine.force_full_compaction()?;

    // Full scan
    let start = Instant::now();
    let mut count = 0u64;
    let mut iter = engine.scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
    while iter.is_valid() {
        count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    println!(
        "  full scan: {} entries in {:?} ({:.0} entries/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    // Partial scan (10%)
    let hi = num_entries / 10;
    let start = Instant::now();
    let mut count = 0u64;
    let mut iter = engine.scan(
        std::ops::Bound::Included(format!("key{:08}", 0).as_bytes()),
        std::ops::Bound::Excluded(format!("key{:08}", hi).as_bytes()),
    )?;
    while iter.is_valid() {
        count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    println!(
        "  10% scan: {} entries in {:?} ({:.0} entries/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_concurrent_rw(
    path: &str,
    num_writers: usize,
    num_readers: usize,
    duration_secs: u64,
    val_size: usize,
    wal: bool,
) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, wal))?;
    let value = vec![b'x'; val_size];
    for i in 0..10_000 {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.force_flush()?;

    let stop = Arc::new(AtomicBool::new(false));
    let write_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));
    let scan_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for t in 0..num_writers {
        let eng = engine.clone();
        let val = value.clone();
        let stop = stop.clone();
        let wc = write_count.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(t as u64);
            let mut local = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..50_000));
                if eng.put(key.as_bytes(), &val).is_ok() {
                    local += 1;
                }
            }
            wc.fetch_add(local, Ordering::Relaxed);
        }));
    }

    for t in 0..num_readers {
        let eng = engine.clone();
        let stop = stop.clone();
        let rc = read_count.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(1000 + t as u64);
            let mut local = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..10_000));
                let _ = eng.get(key.as_bytes());
                local += 1;
            }
            rc.fetch_add(local, Ordering::Relaxed);
        }));
    }

    // Scanner thread
    {
        let eng = engine.clone();
        let stop = stop.clone();
        let sc = scan_count.clone();
        handles.push(std::thread::spawn(move || {
            let mut local = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if let Ok(mut iter) =
                    eng.scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
                {
                    while iter.is_valid() {
                        if iter.next().is_err() {
                            break;
                        }
                        local += 1;
                    }
                }
            }
            sc.fetch_add(local, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("thread panicked");
    }

    let wc = write_count.load(Ordering::Relaxed);
    let rc = read_count.load(Ordering::Relaxed);
    let sc = scan_count.load(Ordering::Relaxed);
    println!(
        "  {}W/{}R+scanner, {}s: {} writes ({:.0}/s), {} reads ({:.0}/s), {} scan_entries",
        num_writers,
        num_readers,
        duration_secs,
        wc,
        wc as f64 / duration_secs as f64,
        rc,
        rc as f64 / duration_secs as f64,
        sc,
    );

    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_wal_throughput(path: &str, num_keys: usize, val_size: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, true))?;
    let value = vec![b'x'; val_size];
    let start = Instant::now();
    for i in 0..num_keys {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    let elapsed = start.elapsed();
    println!(
        "  WAL seq: {} puts ({}B) in {:?} ({:.0} ops/sec)",
        num_keys,
        val_size,
        elapsed,
        num_keys as f64 / elapsed.as_secs_f64()
    );
    engine.force_flush()?;
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_wal_concurrent(
    path: &str,
    num_keys: usize,
    val_size: usize,
    nthreads: usize,
) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, true))?;
    let value = vec![b'x'; val_size];
    let per_thread = num_keys / nthreads;
    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..nthreads {
        let eng = engine.clone();
        let val = value.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..per_thread {
                eng.put(format!("key{:08}", t * per_thread + i).as_bytes(), &val)
                    .expect("put failed");
            }
        }));
    }
    for h in handles {
        h.join().expect("thread panicked");
    }
    let elapsed = start.elapsed();
    println!(
        "  WAL conc({}T): {} puts ({}B) in {:?} ({:.0} ops/sec)",
        nthreads,
        num_keys,
        val_size,
        elapsed,
        num_keys as f64 / elapsed.as_secs_f64()
    );
    engine.force_flush()?;
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_vlog_gc(path: &str, num_rounds: usize, entries_per_round: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(true, false))?;
    let value = vec![b'x'; 4096];

    println!(
        "  vLog GC: {} rounds x {} entries (4KB)",
        num_rounds, entries_per_round
    );
    for i in 0..entries_per_round {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.force_flush()?;
    engine.force_full_compaction()?;
    if let Ok(stats) = engine.vlog_stats() {
        println!("    round 0: {:?}", stats);
    }

    let padding = "x".repeat(4000);
    for round in 1..=num_rounds {
        let start = Instant::now();
        let value = format!("v{}_{}", round, padding);
        for i in 0..entries_per_round {
            engine.put(format!("key{:08}", i).as_bytes(), value.as_bytes())?;
        }
        let we = start.elapsed();
        engine.force_flush()?;
        engine.force_full_compaction()?;
        let gs = Instant::now();
        let gc_count = engine.trigger_gc()?;
        let ge = gs.elapsed();
        if let Ok(stats) = engine.vlog_stats() {
            println!(
                "    round {}: writes {:?}, GC {} files in {:?}, {:?}",
                round, we, gc_count, ge, stats
            );
        }
    }

    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_vlog_concurrent_gc(path: &str, duration_secs: u64) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(true, false))?;
    let value = vec![b'x'; 4096];
    for i in 0..5_000 {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.force_flush()?;

    let stop = Arc::new(AtomicBool::new(false));
    let wc = Arc::new(AtomicU64::new(0));
    let rc = Arc::new(AtomicU64::new(0));
    let gcc = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Writer
    {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = wc.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(42);
            let val = vec![b'y'; 4096];
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..5_000));
                if eng.put(key.as_bytes(), &val).is_ok() {
                    c += 1;
                }
            }
            wc.fetch_add(c, Ordering::Relaxed);
        }));
    }
    // Reader
    {
        let eng = engine.clone();
        let stop = stop.clone();
        let rc = rc.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(100);
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..5_000));
                if eng.get(key.as_bytes()).is_ok() {
                    c += 1;
                }
            }
            rc.fetch_add(c, Ordering::Relaxed);
        }));
    }
    // GC
    {
        let eng = engine.clone();
        let stop = stop.clone();
        let gcc = gcc.clone();
        handles.push(std::thread::spawn(move || {
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = eng.trigger_gc();
                c += 1;
                std::thread::sleep(Duration::from_millis(500));
            }
            gcc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("thread panicked");
    }

    let w = wc.load(Ordering::Relaxed);
    let r = rc.load(Ordering::Relaxed);
    let g = gcc.load(Ordering::Relaxed);
    println!(
        "  vLog concurrent {}s: {} writes ({:.0}/s), {} reads ({:.0}/s), {} GC rounds",
        duration_secs,
        w,
        w as f64 / duration_secs as f64,
        r,
        r as f64 / duration_secs as f64,
        g,
    );
    if let Ok(stats) = engine.vlog_stats() {
        println!("    final: {:?}", stats);
    }
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

// --- RocksDB-style workloads (fillseq, fillrandom, readrandom, readwhilewriting,
//     readrandomwriterandom, seekrandom) ---

fn bench_fillseq(path: &str, num_entries: usize, val_size: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    let start = Instant::now();
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    let elapsed = start.elapsed();
    println!(
        "  fillseq: {} entries ({}B) in {:?} ({:.0} ops/sec)",
        num_entries,
        val_size,
        elapsed,
        num_entries as f64 / elapsed.as_secs_f64()
    );
    engine.force_flush()?;
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_fillrandom(path: &str, num_entries: usize, val_size: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    let mut rng = StdRng::seed_from_u64(42);
    let mut key_buf = [0u8; 12]; // "key" + 8 digits
    let start = Instant::now();
    for _ in 0..num_entries {
        let n = rng.gen_range(0..num_entries as u64);
        // Format key directly into buffer to avoid allocation
        key_buf[..3].copy_from_slice(b"key");
        write!(&mut key_buf[3..], "{:08}", n)
            .expect("key_buf is 11 bytes; 8-digit format always fits");
        engine.put(&key_buf, &value)?;
    }
    let elapsed = start.elapsed();
    println!(
        "  fillrandom: {} entries ({}B) in {:?} ({:.0} ops/sec)",
        num_entries,
        val_size,
        elapsed,
        num_entries as f64 / elapsed.as_secs_f64()
    );
    engine.force_flush()?;
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_readrandom(
    path: &str,
    num_entries: usize,
    num_reads: usize,
    val_size: usize,
) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.force_flush()?;
    engine.force_full_compaction()?;

    let mut rng = StdRng::seed_from_u64(123);
    let start = Instant::now();
    let mut found = 0u64;
    for _ in 0..num_reads {
        let key = format!("key{:08}", rng.gen_range(0..num_entries as u64));
        if engine.get(key.as_bytes())?.is_some() {
            found += 1;
        }
    }
    let elapsed = start.elapsed();
    println!(
        "  readrandom: {} reads over {} entries in {:?} ({:.0} ops/sec, {} found)",
        num_reads,
        num_entries,
        elapsed,
        num_reads as f64 / elapsed.as_secs_f64(),
        found
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_readwhilewriting(
    path: &str,
    num_entries: usize,
    num_readers: usize,
    duration_secs: u64,
    val_size: usize,
) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.force_flush()?;

    let stop = Arc::new(AtomicBool::new(false));
    let wc = Arc::new(AtomicU64::new(0));
    let rc = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // 1 writer thread
    {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = wc.clone();
        let val = value.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(42);
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..num_entries as u64));
                if eng.put(key.as_bytes(), &val).is_ok() {
                    c += 1;
                }
            }
            wc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    // N reader threads
    for t in 0..num_readers {
        let eng = engine.clone();
        let stop = stop.clone();
        let rc = rc.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(1000 + t as u64);
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..num_entries as u64));
                if eng.get(key.as_bytes()).is_ok() {
                    c += 1;
                }
            }
            rc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("thread panicked");
    }

    let w = wc.load(Ordering::Relaxed);
    let r = rc.load(Ordering::Relaxed);
    println!(
        "  readwhilewriting: {}s, 1W/{}R: {} writes ({:.0}/s), {} reads ({:.0}/s)",
        duration_secs,
        num_readers,
        w,
        w as f64 / duration_secs as f64,
        r,
        r as f64 / duration_secs as f64,
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_readrandomwriterandom(
    path: &str,
    num_entries: usize,
    num_threads: usize,
    duration_secs: u64,
    val_size: usize,
) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.force_flush()?;

    let stop = Arc::new(AtomicBool::new(false));
    let wc = Arc::new(AtomicU64::new(0));
    let rc = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for t in 0..num_threads {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = wc.clone();
        let rc = rc.clone();
        let val = value.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(t as u64);
            let mut writes = 0u64;
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..num_entries as u64));
                if rng.gen_bool(0.5) {
                    if eng.put(key.as_bytes(), &val).is_ok() {
                        writes += 1;
                    }
                } else if eng.get(key.as_bytes()).is_ok() {
                    reads += 1;
                }
            }
            wc.fetch_add(writes, Ordering::Relaxed);
            rc.fetch_add(reads, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("thread panicked");
    }

    let w = wc.load(Ordering::Relaxed);
    let r = rc.load(Ordering::Relaxed);
    println!(
        "  readrandomwriterandom: {}s, {} threads: {} writes ({:.0}/s), {} reads ({:.0}/s)",
        duration_secs,
        num_threads,
        w,
        w as f64 / duration_secs as f64,
        r,
        r as f64 / duration_secs as f64,
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_seekrandom(
    path: &str,
    num_entries: usize,
    num_seeks: usize,
    seek_nexts: usize,
) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; 256];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.force_flush()?;
    engine.force_full_compaction()?;

    let mut rng = StdRng::seed_from_u64(999);
    let start = Instant::now();
    let mut total_nexts = 0u64;
    for _ in 0..num_seeks {
        let key = format!("key{:08}", rng.gen_range(0..num_entries as u64));
        if let Ok(mut iter) = engine.scan(
            std::ops::Bound::Included(key.as_bytes()),
            std::ops::Bound::Unbounded,
        ) {
            for _ in 0..seek_nexts {
                if !iter.is_valid() {
                    break;
                }
                if iter.next().is_err() {
                    break;
                }
                total_nexts += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    println!(
        "  seekrandom: {} seeks ({} nexts each) in {:?} ({:.0} seeks/sec, {} total nexts)",
        num_seeks,
        seek_nexts,
        elapsed,
        num_seeks as f64 / elapsed.as_secs_f64(),
        total_nexts
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

// --- Additional RocksDB-style workloads ---

fn bench_overwrite(path: &str, num_entries: usize, num_ops: usize, val_size: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    // Populate
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    drain_all_memtables(&engine)?;

    // Overwrite existing keys randomly
    let mut rng = StdRng::seed_from_u64(42);
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    let new_val = vec![b'y'; val_size];
    let start = Instant::now();
    for _ in 0..num_ops {
        let n = rng.gen_range(0..num_entries as u64);
        write!(&mut key_buf[3..], "{:08}", n)
            .expect("key_buf is 11 bytes; 8-digit format always fits");
        engine.put(&key_buf, &new_val)?;
    }
    let elapsed = start.elapsed();
    println!(
        "  overwrite: {} ops over {} entries ({}B) in {:?} ({:.0} ops/sec)",
        num_ops,
        num_entries,
        val_size,
        elapsed,
        num_ops as f64 / elapsed.as_secs_f64()
    );
    engine.force_flush()?;
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

/// Drain all memtables to SSTs before read benchmarks.
fn drain_all_memtables(engine: &KvEngine) -> Result<()> {
    engine.drain_flush()
}

fn bench_readseq(path: &str, num_entries: usize, val_size: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    drain_all_memtables(&engine)?;
    engine.force_full_compaction()?;

    let start = Instant::now();
    let mut count = 0u64;
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded)?;
    while iter.is_valid() {
        count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    println!(
        "  readseq: {} entries ({}) in {:?} ({:.0} entries/sec)",
        count,
        val_size,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_readreverse(path: &str, num_entries: usize, val_size: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    drain_all_memtables(&engine)?;
    engine.force_full_compaction()?;

    // NOTE: reverse iteration is not supported by the engine yet.
    // This measures forward scan as a baseline placeholder.
    let start = Instant::now();
    let mut count = 0u64;
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded)?;
    while iter.is_valid() {
        count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    println!(
        "  readreverse (placeholder, forward scan): {} entries ({}) in {:?} ({:.0} entries/sec)",
        count,
        val_size,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_readmissing(
    path: &str,
    num_entries: usize,
    num_reads: usize,
    val_size: usize,
) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    // Populate even-numbered keys only. After compaction, the SST covers
    // key range "key00000000".."key00199998" (even max). Odd keys like
    // "key00000001" fall within this range but were never inserted,
    // so SsTable::point_get passes the range check and hits the bloom filter.
    for i in (0..num_entries).step_by(2) {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    drain_all_memtables(&engine)?;
    engine.force_full_compaction()?;

    // Probe odd keys within the SST range — bloom filter should reject them.
    let mut rng = StdRng::seed_from_u64(777);
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    let start = Instant::now();
    let mut found = 0u64;
    for _ in 0..num_reads {
        let n = rng.gen_range(0..num_entries as u64) | 1; // always odd
        write!(&mut key_buf[3..], "{:08}", n)
            .expect("key_buf is 11 bytes; 8-digit format always fits");
        if engine.get(&key_buf)?.is_some() {
            found += 1;
        }
    }
    let elapsed = start.elapsed();
    println!(
        "  readmissing: {} reads over {} entries in {:?} ({:.0} ops/sec, {} found)",
        num_reads,
        num_entries,
        elapsed,
        num_reads as f64 / elapsed.as_secs_f64(),
        found
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_seekrandomwhilewriting(
    path: &str,
    num_entries: usize,
    num_seeks: usize,
    seek_nexts: usize,
    val_size: usize,
) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; val_size];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    drain_all_memtables(&engine)?;

    let stop = Arc::new(AtomicBool::new(false));
    let wc = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Writer thread
    {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = wc.clone();
        let val = value.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(42);
            let mut key_buf = [0u8; 11];
            key_buf[..3].copy_from_slice(b"key");
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let n = rng.gen_range(0..num_entries as u64);
                write!(&mut key_buf[3..], "{:08}", n)
                    .expect("key_buf is 11 bytes; 8-digit format always fits");
                if eng.put(&key_buf, &val).is_ok() {
                    c += 1;
                }
            }
            wc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    // Seek thread
    let mut rng = StdRng::seed_from_u64(999);
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    let start = Instant::now();
    let mut total_nexts = 0u64;
    for _ in 0..num_seeks {
        let n = rng.gen_range(0..num_entries as u64);
        write!(&mut key_buf[3..], "{:08}", n)
            .expect("key_buf is 11 bytes; 8-digit format always fits");
        if let Ok(mut iter) = engine.scan(Bound::Included(&key_buf), Bound::Unbounded) {
            for _ in 0..seek_nexts {
                if !iter.is_valid() {
                    break;
                }
                if iter.next().is_err() {
                    break;
                }
                total_nexts += 1;
            }
        }
    }
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("thread panicked");
    }
    let w = wc.load(Ordering::Relaxed);

    println!(
        "  seekrandomwhilewriting: {} seeks ({} nexts each) in {:?} ({:.0} seeks/sec, {} total nexts, {} writes)",
        num_seeks,
        seek_nexts,
        elapsed,
        num_seeks as f64 / elapsed.as_secs_f64(),
        total_nexts,
        w
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_deleterandom(path: &str, num_entries: usize, num_deletes: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    let engine = KvEngine::open(path, make_options(false, false))?;
    let value = vec![b'x'; 1024];
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    drain_all_memtables(&engine)?;

    let mut rng = StdRng::seed_from_u64(42);
    let mut key_buf = [0u8; 11]; // "key" + 8 digits
    key_buf[..3].copy_from_slice(b"key");
    let start = Instant::now();
    for _ in 0..num_deletes {
        let n = rng.gen_range(0..num_entries as u64);
        write!(&mut key_buf[3..], "{:08}", n)
            .expect("key_buf is 11 bytes; 8-digit format always fits");
        engine.delete(&key_buf)?;
    }
    let elapsed = start.elapsed();
    println!(
        "  deleterandom: {} deletes over {} entries in {:?} ({:.0} ops/sec)",
        num_deletes,
        num_entries,
        elapsed,
        num_deletes as f64 / elapsed.as_secs_f64()
    );
    engine.force_flush()?;
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn bench_compact(path: &str, num_entries: usize, val_size: usize) -> Result<()> {
    let _ = std::fs::remove_dir_all(path);
    // Use NoCompaction to prevent background compaction from merging SSTs
    // before we start timing.
    let mut opts = make_options(false, false);
    opts.compaction_options = CompactionOptions::NoCompaction;
    let engine = KvEngine::open(path, opts)?;
    let value = vec![b'x'; val_size];
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    for i in 0..num_entries {
        write!(&mut key_buf[3..], "{:08}", i)
            .expect("key_buf is 11 bytes; 8-digit format always fits");
        engine.put(&key_buf, &value)?;
    }
    drain_all_memtables(&engine)?;

    let start = Instant::now();
    engine.force_full_compaction()?;
    let elapsed = start.elapsed();
    println!(
        "  compact: {} entries ({}B) in {:?}",
        num_entries, val_size, elapsed
    );
    engine.close()?;
    let _ = std::fs::remove_dir_all(path);
    Ok(())
}

fn main() -> Result<()> {
    // --- Original workloads ---
    println!("=== 1. Scan ===");
    bench_scan("/tmp/perf-scan", 100_000)?;

    println!("\n=== 2. Concurrent R/W (no WAL) ===");
    bench_concurrent_rw("/tmp/perf-rw-nowal", 2, 4, 5, 256, false)?;

    println!("\n=== 3. Concurrent R/W (WAL) ===");
    bench_concurrent_rw("/tmp/perf-rw-wal", 2, 4, 5, 256, true)?;

    println!("\n=== 4. WAL throughput ===");
    bench_wal_throughput("/tmp/perf-wal1", 50_000, 256)?;
    bench_wal_throughput("/tmp/perf-wal2", 20_000, 4096)?;
    bench_wal_concurrent("/tmp/perf-wal3", 50_000, 256, 4)?;

    println!("\n=== 5. vLog GC pressure ===");
    bench_vlog_gc("/tmp/perf-vgc", 3, 5_000)?;

    println!("\n=== 6. vLog concurrent R/W+GC ===");
    bench_vlog_concurrent_gc("/tmp/perf-vgc2", 5)?;

    // --- RocksDB-style workloads ---
    println!("\n=== 7. fillseq (200k entries, 1KB) ===");
    bench_fillseq("/tmp/perf-fillseq", 200_000, 1024)?;

    println!("\n=== 8. fillrandom (200k entries, 1KB) ===");
    bench_fillrandom("/tmp/perf-fillrandom", 200_000, 1024)?;

    println!("\n=== 9. readrandom (200k entries, 100k reads, 1KB) ===");
    bench_readrandom("/tmp/perf-readrandom", 200_000, 100_000, 1024)?;

    println!("\n=== 10. readwhilewriting (200k entries, 4 readers, 5s) ===");
    bench_readwhilewriting("/tmp/perf-rww", 200_000, 4, 5, 1024)?;

    println!("\n=== 11. readrandomwriterandom (200k entries, 4 threads, 5s) ===");
    bench_readrandomwriterandom("/tmp/perf-rwrw", 200_000, 4, 5, 1024)?;

    println!("\n=== 12. seekrandom (200k entries, 10k seeks, 10 nexts) ===");
    bench_seekrandom("/tmp/perf-seek", 200_000, 10_000, 10)?;

    // --- Additional RocksDB-style workloads ---
    println!("\n=== 13. overwrite (200k entries, 200k ops, 1KB) ===");
    bench_overwrite("/tmp/perf-overwrite", 200_000, 200_000, 1024)?;

    println!("\n=== 14. readseq (200k entries, 1KB) ===");
    bench_readseq("/tmp/perf-readseq", 200_000, 1024)?;

    println!("\n=== 15. readreverse (200k entries, 1KB) ===");
    bench_readreverse("/tmp/perf-readrev", 200_000, 1024)?;

    println!("\n=== 16. readmissing (100k populated, 100k reads, 1KB) ===");
    bench_readmissing("/tmp/perf-readmiss", 200_000, 100_000, 1024)?;

    println!("\n=== 17. seekrandomwhilewriting (200k entries, 1k seeks, 10 nexts, 256B) ===");
    bench_seekrandomwhilewriting("/tmp/perf-seekww", 200_000, 1_000, 10, 256)?;

    println!("\n=== 18. deleterandom (200k entries, 200k deletes) ===");
    bench_deleterandom("/tmp/perf-delrand", 200_000, 200_000)?;

    println!("\n=== 19. compact (200k entries, 1KB) ===");
    bench_compact("/tmp/perf-compact", 200_000, 1024)?;

    Ok(())
}
