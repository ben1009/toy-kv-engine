use std::time::Duration;

use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions, WriteBatchRecord},
};

// ─── TTL Read Path ──────────────────────────────────────────────────────

#[test]
fn test_ttl_put_and_get_non_expired() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"k1", b"v1", Duration::from_secs(3600))
        .unwrap();
    engine.put(b"k2", b"v2").unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"k2").unwrap(), Some(Bytes::from("v2")));
}

#[test]
fn test_ttl_put_expired_returns_none() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"k1", b"v1", Duration::from_secs(0))
        .unwrap();

    assert_eq!(
        engine.get(b"k1").unwrap(),
        None,
        "expired TTL should return None"
    );
}

#[test]
fn test_ttl_expired_key_is_hidden_with_default_options() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"k1", b"v1", Duration::from_secs(0))
        .unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), None);
}

#[test]
fn test_ttl_get_with_kind_returns_expire_at() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"k1", b"v1", Duration::from_secs(3600))
        .unwrap();
    engine.put(b"k2", b"v2").unwrap();

    let (val, _kind, expire_at) = engine.inner.get_with_kind(b"k1").unwrap();
    assert_eq!(val, Some(Bytes::from("v1")));
    assert!(expire_at.is_some(), "TTL entry should have expire_at");

    let (val, _kind, expire_at) = engine.inner.get_with_kind(b"k2").unwrap();
    assert_eq!(val, Some(Bytes::from("v2")));
    assert!(
        expire_at.is_none(),
        "non-TTL entry should have no expire_at"
    );
}

// ─── TTL Write Batch ────────────────────────────────────────────────────

#[test]
fn test_ttl_write_batch_put_with_ttl() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Put(b"k1", b"v1"),
        WriteBatchRecord::PutWithTtl(b"k2", b"v2", 3600),
    ];
    engine.write_batch(&batch).unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"k2").unwrap(), Some(Bytes::from("v2")));
}

#[test]
fn test_ttl_batch_get_filters_expired() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"k1", b"v1", Duration::from_secs(3600))
        .unwrap();
    engine
        .put_with_ttl(b"k2", b"v2", Duration::from_secs(0))
        .unwrap();
    engine.put(b"k3", b"v3").unwrap();

    let results = engine.batch_get(&[b"k1", b"k2", b"k3"]);
    assert_eq!(results[0].as_ref().unwrap(), &Some(Bytes::from("v1")));
    assert_eq!(results[1].as_ref().unwrap(), &None);
    assert_eq!(results[2].as_ref().unwrap(), &Some(Bytes::from("v3")));
}

// ─── TTL Overwrite Semantics ────────────────────────────────────────────

#[test]
fn test_ttl_delete_removes_ttl_entry() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"k1", b"v1", Duration::from_secs(3600))
        .unwrap();
    engine.delete(b"k1").unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), None);
}

#[test]
fn test_ttl_overwrite_with_put_removes_ttl() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"k1", b"v1", Duration::from_secs(0))
        .unwrap();
    engine.put(b"k1", b"v2").unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v2")));
}

#[test]
fn test_ttl_overwrite_ttl_with_ttl() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"k1", b"v1", Duration::from_secs(3600))
        .unwrap();
    engine
        .put_with_ttl(b"k1", b"v2", Duration::from_secs(0))
        .unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), None);
}

#[test]
fn test_ttl_scan_includes_ttl_entries() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"a", b"va", Duration::from_secs(3600))
        .unwrap();
    engine
        .put_with_ttl(b"b", b"vb", Duration::from_secs(3600))
        .unwrap();
    engine.put(b"c", b"vc").unwrap();

    let mut iter = engine
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .unwrap();
    let mut entries = Vec::new();
    while iter.is_valid() {
        entries.push((
            Bytes::copy_from_slice(iter.key()),
            Bytes::copy_from_slice(iter.value()),
        ));
        iter.next().unwrap();
    }

    assert_eq!(entries.len(), 3);
}

#[test]
fn test_ttl_scan_filters_expired_entries() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine
        .put_with_ttl(b"b", b"vb", Duration::from_secs(0))
        .unwrap();
    engine
        .put_with_ttl(b"c", b"vc", Duration::from_secs(3600))
        .unwrap();

    let mut iter = engine
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .unwrap();
    let mut entries = Vec::new();
    while iter.is_valid() {
        entries.push((
            Bytes::copy_from_slice(iter.key()),
            Bytes::copy_from_slice(iter.value()),
        ));
        iter.next().unwrap();
    }

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], (Bytes::from("a"), Bytes::from("va")));
    assert_eq!(entries[1], (Bytes::from("c"), Bytes::from("vc")));
}

#[test]
fn test_ttl_prefix_scan_filters_expired_entries() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions::default_for_test();
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine
        .put_with_ttl(b"p:alive", b"v1", Duration::from_secs(3600))
        .unwrap();
    engine
        .put_with_ttl(b"p:expired", b"v2", Duration::from_secs(0))
        .unwrap();
    engine.put(b"q:other", b"v3").unwrap();

    let mut iter = engine.prefix_scan(b"p:").unwrap();
    let mut entries = Vec::new();
    while iter.is_valid() {
        entries.push((
            Bytes::copy_from_slice(iter.key()),
            Bytes::copy_from_slice(iter.value()),
        ));
        iter.next().unwrap();
    }

    assert_eq!(entries, vec![(Bytes::from("p:alive"), Bytes::from("v1"))]);
}

// ─── TTL Encoding ───────────────────────────────────────────────────────

#[test]
fn test_ttl_encoding_roundtrip() {
    use crate::vlog::{KvKind, encode_ttl_value};

    let expire_at = crate::vlog::wall_clock_secs() + 3600;
    let value = b"hello world";
    let encoded = encode_ttl_value(KvKind::TtlInline, expire_at, value);

    assert_eq!(encoded[0], 0x03);
    assert_eq!(encoded.len(), 1 + 8 + value.len());

    let (decoded_val, decoded_kind, decoded_expire_at) =
        crate::lsm_storage::LsmStorageInner::parse_value_kind(encoded.into());
    assert_eq!(decoded_val, Some(Bytes::from("hello world")));
    assert_eq!(decoded_kind, KvKind::TtlInline);
    assert_eq!(decoded_expire_at, Some(expire_at));
}

#[test]
fn test_ttl_encoding_malformed_inline() {
    let raw = Bytes::from_static(&[0x03, 0x00, 0x00]);
    let (val, kind, expire_at) = crate::lsm_storage::LsmStorageInner::parse_value_kind(raw);
    // Malformed TtlInline (<9 bytes) returns None value.
    assert_eq!(val, None);
    assert_eq!(kind, crate::vlog::KvKind::TtlInline);
    assert_eq!(expire_at, None);
}

#[test]
fn test_ttl_parse_value_kind_tombstone() {
    let raw = Bytes::from_static(&[0x02]);
    let (val, kind, expire_at) = crate::lsm_storage::LsmStorageInner::parse_value_kind(raw);
    assert_eq!(val, None);
    assert_eq!(kind, crate::vlog::KvKind::Tombstone);
    assert_eq!(expire_at, None);
}

#[test]
fn test_ttl_parse_value_kind_inline() {
    // Inline = 0, so first byte is 0x00 followed by payload.
    let raw = Bytes::from_static(&[0x00, b'a', b'b', b'c']);
    let (val, kind, expire_at) = crate::lsm_storage::LsmStorageInner::parse_value_kind(raw);
    assert_eq!(val, Some(Bytes::from("abc")));
    assert_eq!(kind, crate::vlog::KvKind::Inline);
    assert_eq!(expire_at, None);
}

// ─── TTL vLog Helpers ───────────────────────────────────────────────────

#[test]
fn test_ttl_wall_clock_secs() {
    let now = crate::vlog::wall_clock_secs();
    assert!(now > 0, "wall clock should return a reasonable timestamp");
}

#[test]
fn test_ttl_compute_expire_at() {
    let ttl = Duration::from_secs(60);
    let expire_at = crate::vlog::compute_expire_at(ttl);
    let now = crate::vlog::wall_clock_secs();
    assert!(
        expire_at >= now && expire_at <= now + 61,
        "expire_at={expire_at} should be within [now={now}, now+61]"
    );
}

// ─── TTL Compaction ─────────────────────────────────────────────────────

#[test]
fn test_ttl_compaction_preserves_non_expired() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions {
        target_sst_size: 64,
        ..LsmStorageOptions::default_for_test()
    };
    let engine = KvEngine::open(&dir, opts).unwrap();

    for i in 0..20 {
        let key = format!("k{:03}", i);
        let val = format!("v{:03}", i);
        if i % 3 == 0 {
            engine
                .put_with_ttl(key.as_bytes(), val.as_bytes(), Duration::from_secs(3600))
                .unwrap();
        } else {
            engine.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
    }

    engine.force_flush().unwrap();
    engine.force_full_compaction().unwrap();

    assert_eq!(engine.get(b"k000").unwrap(), Some(Bytes::from("v000")));
    assert_eq!(engine.get(b"k001").unwrap(), Some(Bytes::from("v001")));
}

#[test]
fn test_ttl_expired_entries_dropped_during_compaction() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions {
        target_sst_size: 64,
        ..LsmStorageOptions::default_for_test()
    };
    let engine = KvEngine::open(&dir, opts).unwrap();

    for i in 0..10 {
        let key = format!("k{:03}", i);
        let val = format!("v{:03}", i);
        engine
            .put_with_ttl(key.as_bytes(), val.as_bytes(), Duration::from_secs(0))
            .unwrap();
    }
    engine.put(b"keep", b"vkeep").unwrap();

    engine.force_flush().unwrap();
    engine.force_full_compaction().unwrap();

    assert_eq!(engine.get(b"k000").unwrap(), None);
    assert_eq!(engine.get(b"keep").unwrap(), Some(Bytes::from("vkeep")));
}

#[test]
fn test_ttl_wholesale_drop_pure_ttl_sst() {
    let dir = tempdir().unwrap();
    let opts = LsmStorageOptions {
        target_sst_size: 64,
        ..LsmStorageOptions::default_for_test()
    };
    let engine = KvEngine::open(&dir, opts).unwrap();

    for i in 0..10 {
        let key = format!("k{:03}", i);
        let val = format!("v{:03}", i);
        engine
            .put_with_ttl(key.as_bytes(), val.as_bytes(), Duration::from_secs(0))
            .unwrap();
    }

    engine.force_flush().unwrap();
    engine.force_full_compaction().unwrap();

    for i in 0..10 {
        let key = format!("k{:03}", i);
        assert_eq!(engine.get(key.as_bytes()).unwrap(), None);
    }
}
