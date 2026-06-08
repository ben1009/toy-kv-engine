use std::sync::Arc;

use bytes::Bytes;
use tempfile::{TempDir, tempdir};

use crate::{
    iterators::StorageIterator,
    key::{KeySlice, KeyVec},
    table::{SsTable, SsTableBuilder, SsTableIterator},
};

#[test]
fn test_sst_build_single_key() {
    let mut builder = SsTableBuilder::new(16);
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"233"), b"233333")
        .unwrap();
    let dir = tempdir().unwrap();
    builder.build_for_test(dir.path().join("1.sst")).unwrap();
}

#[test]
fn test_sst_build_two_blocks() {
    let mut builder = SsTableBuilder::new(16);
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11")
        .unwrap();
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22")
        .unwrap();
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"33"), b"11")
        .unwrap();
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"44"), b"22")
        .unwrap();
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"55"), b"11")
        .unwrap();
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"66"), b"22")
        .unwrap();
    assert!(builder.meta.len() >= 2);
    let dir = tempdir().unwrap();
    builder.build_for_test(dir.path().join("1.sst")).unwrap();
}

fn key_of(idx: usize) -> KeyVec {
    KeyVec::from_user_key_ts(format!("key_{:03}", idx * 5).as_bytes(), 0)
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{idx:010}").into_bytes()
}

fn num_of_keys() -> usize {
    100
}

fn generate_sst() -> (TempDir, SsTable) {
    let mut builder = SsTableBuilder::new(128);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(key.as_key_slice(), &value[..]).unwrap();
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    (dir, builder.build_for_test(path).unwrap())
}

#[test]
fn test_sst_builder_is_empty() {
    let mut builder = SsTableBuilder::new(128);
    assert!(builder.is_empty(), "fresh builder should be empty");

    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"a"), b"v")
        .unwrap();
    assert!(!builder.is_empty(), "builder with data should not be empty");

    // Also after block seal + re-add path: fill a block, then add more.
    let mut builder = SsTableBuilder::new(16);
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11")
        .unwrap();
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22")
        .unwrap();
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"33"), b"33")
        .unwrap();
    assert!(
        !builder.is_empty(),
        "builder with sealed+re-added data should not be empty"
    );
}

#[test]
fn test_sst_builder_tiny_block_size() {
    // Verify that SSTs build successfully even with very small block_size.
    // BlockBuilder::add always accepts the first entry regardless of size
    // (skips the size check when is_empty()), so the bail! path in add_inner
    // is unreachable in correct code. This test exercises the block-seal +
    // re-add path with block_size=8.
    let mut builder = SsTableBuilder::new(8);

    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"k"), b"v")
        .unwrap();

    // Forces a block seal. The fresh BlockBuilder accepts this as its first entry.
    builder
        .add(KeySlice::for_testing_from_slice_no_ts(b"k2"), b"v2")
        .unwrap();
    let dir = tempdir().unwrap();
    let result = builder.build_for_test(dir.path().join("1.sst"));
    assert!(
        result.is_ok(),
        "tiny block_size SST should build: {:?}",
        result.err()
    );
}

#[test]
fn test_sst_build_all() {
    generate_sst();
}

#[test]
fn test_sst_decode() {
    let (_dir, sst) = generate_sst();
    let meta = sst.block_meta.clone();
    let new_sst = SsTable::open_for_test(sst.file).unwrap();
    assert_eq!(new_sst.block_meta, meta);
    assert_eq!(
        new_sst.first_key().for_testing_key_ref(),
        key_of(0).for_testing_key_ref()
    );
    assert_eq!(
        new_sst.last_key().for_testing_key_ref(),
        key_of(num_of_keys() - 1).for_testing_key_ref()
    );
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[test]
fn test_sst_iterator() {
    let (_dir, sst) = generate_sst();
    let sst = Arc::new(sst);
    let mut iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
    for _ in 0..5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key.for_testing_key_ref(),
                key_of(i).for_testing_key_ref(),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(key_of(i).for_testing_key_ref()),
                as_bytes(key.for_testing_key_ref())
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.next().unwrap();
        }
        iter.seek_to_first().unwrap();
    }
}

#[test]
fn test_sst_seek_key() {
    let (_dir, sst) = generate_sst();
    let sst = Arc::new(sst);
    let mut iter = SsTableIterator::create_and_seek_to_key(sst, key_of(0).as_key_slice()).unwrap();
    for offset in 1..=5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            // println!(
            //     "expected key: {:?}, actual key: {:?}",
            //     as_bytes(key_of(i).for_testing_key_ref()),
            //     as_bytes(key.for_testing_key_ref())
            // );
            assert_eq!(
                key.for_testing_key_ref(),
                key_of(i).for_testing_key_ref(),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(key_of(i).for_testing_key_ref()),
                as_bytes(key.for_testing_key_ref())
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.seek_to_key(
                KeyVec::from_user_key_ts(&format!("key_{:03}", i * 5 + offset).into_bytes(), 0)
                    .as_key_slice(),
            )
            .unwrap();
        }
        iter.seek_to_key(KeyVec::from_user_key_ts(b"k", 0).as_key_slice())
            .unwrap();
    }
}

// --- MVCC multi-version point-get tests ---

/// Build an SST with the same user key at multiple timestamps.
/// Keys are inserted newest-first (higher ts) so they sort first in byte order.
fn generate_multi_version_sst(dir: &tempfile::TempDir, block_size: usize) -> SsTable {
    let mut builder = SsTableBuilder::new(block_size);
    // key "A" at ts=10 (newest), ts=5, ts=1 (oldest)
    // key "B" at ts=8, ts=3
    // key "C" at ts=6
    let entries: Vec<(&[u8], u64, &[u8])> = vec![
        (b"A", 10, b"A_v10"),
        (b"A", 5, b"A_v5"),
        (b"A", 1, b"A_v1"),
        (b"B", 8, b"B_v8"),
        (b"B", 3, b"B_v3"),
        (b"C", 6, b"C_v6"),
    ];
    for (uk, ts, val) in &entries {
        builder
            .add_raw(KeyVec::from_user_key_ts(uk, *ts).as_key_slice(), val)
            .unwrap();
    }
    builder
        .build_for_test(dir.path().join("multi_version.sst"))
        .unwrap()
}

#[test]
fn test_sst_multi_version_point_get_newest() {
    let dir = tempdir().unwrap();
    let sst = generate_multi_version_sst(&dir, 128);
    // Seek key must be an encoded internal key (not raw user key).
    // Use ts=u64::MAX to seek to the newest version of "A".
    let seek = crate::key::encode_internal_key(b"A", u64::MAX);
    let (val, found_key) = sst
        .point_get_with_hash_and_key(&seek, crate::table::bloom::hash_key(b"A"), None)
        .unwrap()
        .unwrap();
    assert_eq!(&*val, b"A_v10");
    assert_eq!(crate::key::extract_ts(&found_key).unwrap(), 10);
}

#[test]
fn test_sst_multi_version_point_get_with_read_ts() {
    let dir = tempdir().unwrap();
    let sst = generate_multi_version_sst(&dir, 128);
    let bh = crate::table::bloom::hash_key(b"A");
    // Seek key uses read_ts so the binary search lands at the right version.
    let seek = |ts: u64| crate::key::encode_internal_key(b"A", ts);

    // read_ts=7 → should return ts=5 version (newest <= 7)
    let (val, found_key) = sst
        .point_get_with_hash_and_key(&seek(7), bh, Some(7))
        .unwrap()
        .unwrap();
    assert_eq!(&*val, b"A_v5");
    assert_eq!(crate::key::extract_ts(&found_key).unwrap(), 5);

    // read_ts=10 → should return ts=10 version
    let (val, found_key) = sst
        .point_get_with_hash_and_key(&seek(10), bh, Some(10))
        .unwrap()
        .unwrap();
    assert_eq!(&*val, b"A_v10");
    assert_eq!(crate::key::extract_ts(&found_key).unwrap(), 10);

    // read_ts=2 → should return ts=1 version
    let (val, found_key) = sst
        .point_get_with_hash_and_key(&seek(2), bh, Some(2))
        .unwrap()
        .unwrap();
    assert_eq!(&*val, b"A_v1");
    assert_eq!(crate::key::extract_ts(&found_key).unwrap(), 1);

    // read_ts=0 → below all versions, returns None
    assert!(
        sst.point_get_with_hash_and_key(&seek(0), bh, Some(0))
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_sst_multi_version_cross_block_read_ts() {
    // Use tiny block_size so versions of "A" span multiple blocks
    let dir = tempdir().unwrap();
    let sst = generate_multi_version_sst(&dir, 16);
    let bh = crate::table::bloom::hash_key(b"A");
    let seek = crate::key::encode_internal_key(b"A", 7);

    // read_ts=7 → should still find ts=5 even if it's in a later block
    let (val, found_key) = sst
        .point_get_with_hash_and_key(&seek, bh, Some(7))
        .unwrap()
        .unwrap();
    assert_eq!(&*val, b"A_v5");
    assert_eq!(crate::key::extract_ts(&found_key).unwrap(), 5);
}

#[test]
fn test_sst_multi_version_key_ordering() {
    // Verify newer versions sort first in byte order
    let k_new = KeyVec::from_user_key_ts(b"A", 10);
    let k_mid = KeyVec::from_user_key_ts(b"A", 5);
    let k_old = KeyVec::from_user_key_ts(b"A", 1);
    assert!(k_new.as_key_slice() < k_mid.as_key_slice());
    assert!(k_mid.as_key_slice() < k_old.as_key_slice());
}

/// Verify that MVCC point lookup finds a version in a leftward SST even when
/// a rightward SST has a higher max_ts but does not contain the target key.
///
/// This exercises the leveled SST scan loop where we iterate from right to
/// left. Using `break` (instead of `continue`) when `max_ts <= best_ts`
/// could prematurely skip SSTs that contain the version we're looking for
/// when `max_ts` is not monotonically ordered across the scan direction.
#[test]
fn test_sst_leveled_scan_finds_key_across_different_max_ts() {
    let dir = tempdir().unwrap();

    // SST-right: contains key "X" at ts=200 → max_ts=200
    // Does NOT contain key "Z".
    let mut builder_right = SsTableBuilder::new(128);
    builder_right
        .add_raw(KeyVec::from_user_key_ts(b"X", 200).as_key_slice(), b"v200")
        .unwrap();
    let sst_right = builder_right
        .build_for_test(dir.path().join("right.sst"))
        .unwrap();
    assert_eq!(sst_right.max_ts(), 200);

    // SST-left: contains key "Z" at ts=80 → max_ts=80
    // Lower max_ts than SST-right, but has the key we want.
    let mut builder_left = SsTableBuilder::new(128);
    builder_left
        .add_raw(KeyVec::from_user_key_ts(b"Z", 80).as_key_slice(), b"v80")
        .unwrap();
    let sst_left = builder_left
        .build_for_test(dir.path().join("left.sst"))
        .unwrap();
    assert_eq!(sst_left.max_ts(), 80);

    // Simulate the leveled scan: right-to-left (right first, then left).
    // SSTs are sorted by key range; "Z" > "X", so SST-left is to the right
    // of SST-right in key-order. But for the scan we process SST-right first.
    let ssts = [&sst_right, &sst_left];
    let seek = crate::key::encode_internal_key(b"Z", u64::MAX);
    let bh = crate::table::bloom::hash_key(b"Z");
    let read_ts: u64 = 200;

    let mut best: Option<(Bytes, u64)> = None;
    for sst in &ssts {
        // max_ts skip: skip if this SST can't beat the current best.
        // With `continue`: skip this SST, keep scanning.
        // With `break`: stop entirely — would miss SST-left's version.
        if let Some((_, best_ts)) = best
            && sst.max_ts() <= best_ts
        {
            continue;
        }
        if let Some((raw, found_key)) = sst
            .point_get_with_hash_and_key(&seek, bh, Some(read_ts))
            .unwrap()
        {
            let ts = crate::key::extract_ts(&found_key).unwrap_or(0);
            if best.as_ref().is_none_or(|(_, best_ts)| ts > *best_ts) {
                best = Some((raw, ts));
            }
        }
    }

    let (val, ts) = best.expect("should find key Z at ts=80");
    assert_eq!(ts, 80);
    assert_eq!(val.as_ref(), b"v80");
}

#[test]
fn test_sst_max_ts_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("max_ts.sst");
    let mut builder = SsTableBuilder::new(128);
    // Keys with timestamps 5, 10, 3
    builder
        .add_raw(KeyVec::from_user_key_ts(b"A", 5).as_key_slice(), b"v5")
        .unwrap();
    builder
        .add_raw(KeyVec::from_user_key_ts(b"B", 10).as_key_slice(), b"v10")
        .unwrap();
    builder
        .add_raw(KeyVec::from_user_key_ts(b"C", 3).as_key_slice(), b"v3")
        .unwrap();
    let sst = builder.build_for_test(&path).unwrap();
    // max_ts should be the highest timestamp seen (10).
    assert_eq!(sst.max_ts(), 10);

    // Reopen from disk and verify max_ts persists.
    let sst2 = SsTable::open_for_test(crate::table::FileObject::open(&path).unwrap()).unwrap();
    assert_eq!(sst2.max_ts(), 10);
}

#[test]
fn test_sst_max_ts_zero_for_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.sst");
    let builder = SsTableBuilder::new(128);
    let sst = builder.build_for_test(&path).unwrap();
    assert_eq!(sst.max_ts(), 0);
}

#[test]
fn test_sst_max_ts_u64_max_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("max_ts_u64.sst");
    let mut builder = SsTableBuilder::new(128);
    builder
        .add_raw(
            KeyVec::from_user_key_ts(b"A", u64::MAX).as_key_slice(),
            b"v",
        )
        .unwrap();
    let sst = builder.build_for_test(&path).unwrap();
    assert_eq!(sst.max_ts(), u64::MAX);

    // Reopen and verify.
    let sst2 = SsTable::open_for_test(crate::table::FileObject::open(&path).unwrap()).unwrap();
    assert_eq!(sst2.max_ts(), u64::MAX);
}

#[test]
fn test_sst_data_readable_after_mvcc_footer() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data_check.sst");
    let mut builder = SsTableBuilder::new(128);
    // Add several key-value pairs with different timestamps.
    for i in 0..5u64 {
        let key = format!("key{:02}", i);
        let val = format!("val{:02}", i);
        builder
            .add_raw(
                KeyVec::from_user_key_ts(key.as_bytes(), i + 1).as_key_slice(),
                val.as_bytes(),
            )
            .unwrap();
    }
    let sst = builder.build_for_test(&path).unwrap();
    assert_eq!(sst.max_ts(), 5);

    // Reopen and verify all data is readable via iterator.
    let sst2 = SsTable::open_for_test(crate::table::FileObject::open(&path).unwrap()).unwrap();
    assert_eq!(sst2.max_ts(), 5);
    let mut iter = SsTableIterator::create_and_seek_to_first(Arc::new(sst2)).unwrap();
    let mut count = 0;
    while iter.is_valid() {
        let key = iter.key();
        let val = iter.value();
        let user_key = crate::key::decode_user_key(key.raw_ref()).unwrap();
        let ts = key.ts();
        assert!((1..=5).contains(&ts), "unexpected ts={}", ts);
        assert!(
            user_key.starts_with(b"key"),
            "unexpected key {:?}",
            user_key
        );
        assert!(val.starts_with(b"val"), "unexpected val {:?}", val);
        count += 1;
        iter.next().unwrap();
    }
    assert_eq!(count, 5);
}

#[test]
fn test_sst_open_empty_file_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.sst");
    // Create a 0-byte file.
    std::fs::File::create(&path).unwrap();
    let file = crate::table::FileObject::open(&path).unwrap();
    let result = SsTable::open_for_test(file);
    assert!(result.is_err(), "expected error for empty SST file");
    let err_msg = format!("{:?}", result.err().unwrap());
    assert!(
        err_msg.contains("too small"),
        "error should mention 'too small', got: {}",
        err_msg
    );
}

#[test]
fn test_sst_legacy_mvcc_sst_without_footer_rejected() {
    // Build an SST with MVCC-encoded keys, then strip the v2 footer to
    // simulate a pre-footer legacy SST. Opening should fail because we
    // cannot safely recover max_ts from block metadata alone.
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy.sst");
    let mut builder = SsTableBuilder::new(128);
    builder
        .add_raw(KeyVec::from_user_key_ts(b"A", 42).as_key_slice(), b"v")
        .unwrap();
    builder
        .add_raw(KeyVec::from_user_key_ts(b"B", 99).as_key_slice(), b"v")
        .unwrap();
    let _sst = builder.build_for_test(&path).unwrap();

    // Read the full file and strip the 17-byte MVCC footer.
    let raw = std::fs::read(&path).unwrap();
    let mvcc_footer_size = 17; // max_ts(8) + magic(4) + version(1) + bloom_offset(4)
    assert!(raw.len() > mvcc_footer_size);
    let bloom_offset_bytes = &raw[raw.len() - mvcc_footer_size..raw.len() - mvcc_footer_size + 4];
    let mut legacy = raw[..raw.len() - mvcc_footer_size].to_vec();
    legacy.extend_from_slice(bloom_offset_bytes);
    std::fs::write(&path, &legacy).unwrap();

    // Open should fail — footer-less MVCC SSTs are rejected.
    let result = SsTable::open_for_test(crate::table::FileObject::open(&path).unwrap());
    assert!(result.is_err(), "expected error for footer-less MVCC SST");
    let err_msg = format!("{:?}", result.err().unwrap());
    assert!(
        err_msg.contains("no v2 footer"),
        "error should mention 'no v2 footer', got: {}",
        err_msg
    );
}
