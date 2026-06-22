//! Micro-benchmark isolating memtable versioned lookup.
//!
//! Compares:
//! 1. New approach: stack-buffer encoding + `Bound<&[u8]>` range (zero heap alloc)
//! 2. Old approach: `Bytes::from(encode_internal_key(...))` range (heap alloc per seek)

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use kv_engine::key;
use kv_engine::mem_table::MemTable;

fn bench_memtable_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_seek");

    for key_size in [4, 8, 16, 32] {
        let label = format!("key_{}B", key_size);

        // Set up a memtable with entries.
        let entries = 10_000;
        let memtable = MemTable::create(0, false);
        for i in 0..entries {
            let k = make_key(key_size, i);
            let encoded = key::encode_internal_key(&k, i as u64 + 1);
            memtable.put(&encoded, b"value_data_123456").unwrap();
        }

        // Query keys that exist (positive lookups).
        let query_keys: Vec<Vec<u8>> = (0..entries)
            .step_by(10)
            .map(|i| make_key(key_size, i))
            .collect();

        // --- New: Bound<&[u8]> (zero alloc) ---
        group.bench_with_input(
            BenchmarkId::new("bound_ref", &label),
            &query_keys,
            |b, keys| {
                let mut idx = 0;
                b.iter(|| {
                    let user_key = &keys[idx % keys.len()];
                    idx += 1;
                    black_box(seek_bound_ref(&memtable, user_key, u64::MAX));
                });
            },
        );

        // --- Old: Bytes::from (heap alloc per seek) ---
        group.bench_with_input(
            BenchmarkId::new("bytes_alloc", &label),
            &query_keys,
            |b, keys| {
                let mut idx = 0;
                b.iter(|| {
                    let user_key = &keys[idx % keys.len()];
                    idx += 1;
                    black_box(seek_bytes_alloc(&memtable, user_key, u64::MAX));
                });
            },
        );
    }

    group.finish();
}

/// New approach: stack buffer + `Bound<&[u8]>` range. Zero heap allocation.
fn seek_bound_ref(memtable: &MemTable, user_key: &[u8], read_ts: u64) -> Option<Bytes> {
    let mut seek_buf = [0u8; 64];
    let seek_key = key::encode_internal_key_inline(&mut seek_buf, user_key, u64::MAX);
    let seek_prefix = key::encoded_user_key_prefix(seek_key).unwrap();
    let mut range = memtable.raw_map().range::<[u8], _>((
        std::ops::Bound::Included(seek_key),
        std::ops::Bound::Unbounded,
    ));
    for entry in range.by_ref() {
        let found_key = entry.key();
        if let Some(found_user_key) = key::encoded_user_key_prefix(found_key) {
            if found_user_key != seek_prefix {
                break;
            }
        } else {
            break;
        }
        let ts = key::extract_ts(found_key).unwrap_or(0);
        if ts > read_ts {
            continue;
        }
        return Some(entry.value().clone());
    }
    None
}

/// Old approach: `Bytes::from(encode_internal_key(...))` range. Heap alloc per seek.
fn seek_bytes_alloc(memtable: &MemTable, user_key: &[u8], read_ts: u64) -> Option<Bytes> {
    let seek_key = Bytes::from(key::encode_internal_key(user_key, u64::MAX));
    let seek_prefix = key::encoded_user_key_prefix(&seek_key).unwrap();
    let mut range = memtable.raw_map().range::<Bytes, _>(seek_key.clone()..);
    for entry in range.by_ref() {
        let found_key = entry.key();
        if let Some(found_user_key) = key::encoded_user_key_prefix(found_key) {
            if found_user_key != seek_prefix {
                break;
            }
        } else {
            break;
        }
        let ts = key::extract_ts(found_key).unwrap_or(0);
        if ts > read_ts {
            continue;
        }
        return Some(entry.value().clone());
    }
    None
}

fn make_key(size: usize, idx: usize) -> Vec<u8> {
    let mut key = vec![0u8; size];
    let bytes = (idx as u64).to_le_bytes();
    let copy_len = size.min(8);
    key[..copy_len].copy_from_slice(&bytes[..copy_len]);
    key
}

criterion_group!(benches, bench_memtable_lookup);
criterion_main!(benches);
