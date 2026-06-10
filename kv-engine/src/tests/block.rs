use std::sync::Arc;

use bytes::Bytes;

use crate::{
    block::{Block, BlockBuilder, BlockIterator},
    key::{KeySlice, KeyVec},
};

#[test]
fn test_block_build_single_key() {
    let mut builder = BlockBuilder::new(16);
    assert!(
        builder
            .add(KeySlice::for_testing_from_slice_no_ts(b"233"), b"233333")
            .unwrap()
    );
    builder.build();
}

#[test]
fn test_block_build_full() {
    let mut builder = BlockBuilder::new(16);
    assert!(
        builder
            .add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11")
            .unwrap()
    );
    assert!(
        !builder
            .add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22")
            .unwrap()
    );
    builder.build();
}

#[test]
fn test_block_build_large_1() {
    let mut builder = BlockBuilder::new(16);
    assert!(
        builder
            .add(
                KeySlice::for_testing_from_slice_no_ts(b"11"),
                &b"1".repeat(100)
            )
            .unwrap()
    );
    builder.build();
}

#[test]
fn test_block_build_large_2() {
    let mut builder = BlockBuilder::new(16);
    assert!(
        builder
            .add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"1")
            .unwrap()
    );
    assert!(
        !builder
            .add(
                KeySlice::for_testing_from_slice_no_ts(b"11"),
                &b"1".repeat(100)
            )
            .unwrap()
    );
}

fn key_of(idx: usize) -> KeyVec {
    KeyVec::for_testing_from_vec_no_ts(format!("key_{:03}", idx * 5).into_bytes())
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{idx:010}").into_bytes()
}

fn num_of_keys() -> usize {
    100
}

fn generate_block() -> Block {
    let mut builder = BlockBuilder::new(10000);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        assert!(builder.add(key.as_key_slice(), &value[..]).unwrap());
    }
    builder.build()
}

#[test]
fn test_block_build_all() {
    generate_block();
}

#[test]
fn test_block_encode() {
    let block = generate_block();
    block.encode().unwrap();
}

#[test]
fn test_block_decode() {
    let block = generate_block();
    let expected_offsets = block.offsets.clone();
    let expected_data = block.data.clone();
    let encoded = block.encode().unwrap();
    let decoded_block = Block::decode(&encoded).unwrap();
    assert_eq!(expected_offsets, decoded_block.offsets);
    assert_eq!(expected_data, decoded_block.data);
}

#[test]
fn test_block_decode_rejects_truncated_input() {
    assert!(Block::decode(&[]).is_err());
    assert!(Block::decode_from_vec(vec![]).is_err());
    assert!(Block::decode_from_vec(vec![0]).is_err());
}

#[test]
fn test_block_decode_rejects_invalid_offsets() {
    // Unsorted offsets: [200, 100]
    let mut data = vec![0u8; 10];
    data.extend_from_slice(&200u16.to_le_bytes());
    data.extend_from_slice(&100u16.to_le_bytes());
    data.extend_from_slice(&2u16.to_le_bytes()); // num_elements = 2
    assert!(Block::decode(&data).is_err());
    assert!(Block::decode_from_vec(data.clone()).is_err());

    // Offset past data region
    let mut data2 = vec![0u8; 4];
    data2.extend_from_slice(&9999u16.to_le_bytes());
    data2.extend_from_slice(&1u16.to_le_bytes()); // num_elements = 1
    assert!(Block::decode(&data2).is_err());
    assert!(Block::decode_from_vec(data2).is_err());
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[test]
fn test_block_builder_key_at() {
    let mut builder = BlockBuilder::new(10000);
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key_{:03}", i * 5).into_bytes())
        .collect();
    for key in &keys {
        assert!(
            builder
                .add(
                    KeySlice::for_testing_from_slice_no_ts(key),
                    &format!("value_{:010}", key.len()).into_bytes(),
                )
                .unwrap()
        );
    }

    for (i, expected) in keys.iter().enumerate() {
        let actual = builder.key_at(i);
        assert_eq!(actual.as_ref(), expected.as_slice(), "key_at({i}) mismatch");
    }
}

#[test]
fn test_block_builder_key_at_single_entry() {
    let mut builder = BlockBuilder::new(10000);
    assert!(
        builder
            .add(KeySlice::for_testing_from_slice_no_ts(b"lonely"), b"value")
            .unwrap()
    );
    assert_eq!(builder.key_at(0).as_ref(), b"lonely");
}

#[test]
fn test_block_builder_key_at_empty_first_key() {
    let mut builder = BlockBuilder::new(10000);
    assert!(
        builder
            .add(KeySlice::for_testing_from_slice_no_ts(b""), b"v0")
            .unwrap()
    );
    assert!(
        builder
            .add(KeySlice::for_testing_from_slice_no_ts(b"abc"), b"v1")
            .unwrap()
    );
    assert!(
        builder
            .add(KeySlice::for_testing_from_slice_no_ts(b"axyz"), b"v2")
            .unwrap()
    );

    // key_at should reconstruct the keys correctly, including the empty first key.
    assert_eq!(builder.key_at(0).as_ref(), b"");
    assert_eq!(builder.key_at(1).as_ref(), b"abc");
    assert_eq!(builder.key_at(2).as_ref(), b"axyz");

    // The block must round-trip through build/encode/decode without panicking and
    // preserve the entry data. (BlockIterator treats an empty key as "invalid" by
    // existing design, so we verify the decoded layout directly.)
    let block = builder.build();
    let encoded = block.encode().unwrap();
    let decoded = Block::decode(&encoded).unwrap();
    assert_eq!(decoded.offsets.len(), 3);
    assert!(!decoded.data.is_empty());

    // Verify by re-walking the decoded entries with a fresh iterator.
    let block = Arc::new(decoded);
    let mut iter = BlockIterator::create_and_seek_to_first(block);
    // Empty key makes `is_valid()` false by existing iterator semantics, but the
    // entry is still present at idx 0.
    assert_eq!(iter.key().raw_ref(), b"");
    assert_eq!(iter.value(), b"v0");

    iter.next();
    assert!(iter.is_valid());
    assert_eq!(iter.key().raw_ref(), b"abc");
    assert_eq!(iter.value(), b"v1");

    iter.next();
    assert!(iter.is_valid());
    assert_eq!(iter.key().raw_ref(), b"axyz");
    assert_eq!(iter.value(), b"v2");
}

#[test]
#[should_panic(expected = "key_at index out of bounds")]
fn test_block_builder_key_at_out_of_bounds() {
    let builder = BlockBuilder::new(10000);
    builder.key_at(0);
}

#[test]
fn test_block_builder_key_at_long_keys() {
    // Keys longer than 128 bytes exercise the chunked overlap_len fast path.
    let mut builder = BlockBuilder::new(100_000);
    let prefix = b"common_prefix_";
    let keys: Vec<Vec<u8>> = (0..10)
        .map(|i| {
            let mut k = prefix.to_vec();
            // Pad to ~200 bytes so chunks_exact(128) iterates at least once.
            k.extend(format!("key_{:03}_", i).as_bytes());
            k.resize(200, b'x');
            k
        })
        .collect();
    for key in &keys {
        assert!(
            builder
                .add(KeySlice::for_testing_from_slice_no_ts(key), b"v")
                .unwrap()
        );
    }

    for (i, expected) in keys.iter().enumerate() {
        let actual = builder.key_at(i);
        assert_eq!(actual.as_ref(), expected.as_slice(), "key_at({i}) mismatch");
    }
}

#[test]
fn test_block_iterator() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_first(block);
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
            iter.next();
        }
        iter.seek_to_first();
    }
}

#[test]
fn test_block_seek_key() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_key(block, key_of(0).as_key_slice());
    for offset in 1..=5 {
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
            iter.seek_to_key(KeySlice::for_testing_from_slice_no_ts(
                &format!("key_{:03}", i * 5 + offset).into_bytes(),
            ));
        }
        iter.seek_to_key(KeySlice::for_testing_from_slice_no_ts(b"k"));
    }
}
