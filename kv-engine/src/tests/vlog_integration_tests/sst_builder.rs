use super::*;

#[test]
fn test_sst_builder_kind_prefix_inline() {
    // Values added via add() are always stored inline with KvKind::Inline prefix
    let mut builder = SsTableBuilder::new(4096);
    builder
        .add(
            KeySlice::for_testing_from_slice_no_ts(b"key1"),
            b"small", // 5 bytes < 16 byte threshold
        )
        .unwrap();

    // The block should contain the value with KvKind::Inline prefix
    // We verify by building and reading back through the iterator
    let dir = tempfile::tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("test.sst")).unwrap();

    let iter = crate::table::SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key().raw_ref(), b"key1");
    // value() should strip the kind prefix and return the raw value
    assert_eq!(iter.value(), b"small");
}

#[test]
fn test_sst_builder_kind_prefix_empty_value() {
    // Empty values (tombstones) should be stored as [KvKind::Inline] only
    let mut builder = SsTableBuilder::new(4096);
    builder
        .add(
            KeySlice::for_testing_from_slice_no_ts(b"key1"),
            b"", // tombstone
        )
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("test.sst")).unwrap();

    let iter = crate::table::SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key().raw_ref(), b"key1");
    // value() should return empty for tombstones
    assert!(iter.value().is_empty());
}

#[test]
fn test_sst_builder_add_raw_preserves_pointer() {
    use crate::vlog::{KvKind, ValuePointer};

    // Simulate a compaction scenario: add_raw with a ValuePointer
    let mut builder = SsTableBuilder::new(4096);

    // Create a fake ValuePointer
    let ptr = ValuePointer {
        file_id: 42,
        offset: 1234,
        size: 5678,
    };
    let mut raw = vec![KvKind::ValuePointer as u8];
    ptr.encode(&mut raw);

    builder
        .add_raw(KeySlice::for_testing_from_slice_no_ts(b"key1"), &raw)
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("test.sst")).unwrap();

    // raw_value() should return the original bytes with kind prefix
    let iter = crate::table::SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.raw_value(), &raw[..]);
}
