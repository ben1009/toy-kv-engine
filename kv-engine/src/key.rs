use std::borrow::Cow;
use std::fmt::Debug;

use bytes::Bytes;

pub const TS_ENABLED: bool = true;

/// Create a `Bytes` with the `SHARED` representation directly, avoiding the
/// `PROMOTABLE` -> `SHARED` transition cost on the first clone.
///
/// `Bytes::copy_from_slice` always creates `PROMOTABLE` (len == cap), which
/// requires an atomic CAS + allocation on the first clone. By allocating one
/// extra byte of capacity, `Bytes::from(vec)` takes the fast path that creates
/// a refcounted `SHARED` buffer immediately.
pub(crate) fn shared_bytes_from_slice(src: &[u8]) -> Bytes {
    if src.is_empty() {
        return Bytes::new();
    }
    let mut vec = Vec::with_capacity(src.len() + 1);
    vec.extend_from_slice(src);

    Bytes::from(vec)
}

// ---------------------------------------------------------------------------
// Internal key encoding helpers
//
// Format: [memcomparable_user_key][!ts: u64 BE]
//
// TiKV-style memcomparable encoding for user keys:
//   - Bytes are split into 8-byte groups
//   - Each group is followed by a marker byte
//   - Last group: padded with 0x00, marker = 0xFF - pad_count
//   - Marker 0xFF means no padding (full 8-byte group)
//
// Timestamp suffix:
//   - `!ts` (bitwise NOT) stored as big-endian u64
//   - Higher ts → lower !ts → sorts first within a user key
//
// This preserves byte-order for memcmp: user keys sort lexicographically,
// and for the same user key newer timestamps sort first.
// ---------------------------------------------------------------------------

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = 0xFF;
const ENC_PAD: u8 = 0x00;

/// Encode the user key portion using TiKV memcomparable encoding.
fn encode_memcomparable_user_key_to(buf: &mut Vec<u8>, user_key: &[u8]) {
    let mut chunks = user_key.chunks_exact(ENC_GROUP_SIZE);
    for chunk in &mut chunks {
        buf.extend_from_slice(chunk);
        buf.push(ENC_MARKER);
    }
    let remainder = chunks.remainder();
    // Always emit a final padded group (terminator). This ensures every encoded
    // key ends with a marker < 0xFF, which is required for correct lexicographic
    // sort order — without it, a key whose length is an exact multiple of 8
    // would end with marker 0xFF and sort incorrectly after longer keys with
    // the same prefix.
    let pad_count = ENC_GROUP_SIZE - remainder.len();
    buf.extend_from_slice(remainder);
    buf.resize(buf.len() + pad_count, ENC_PAD);
    buf.push(ENC_MARKER.wrapping_sub(pad_count as u8));
}

/// Encode a user key + timestamp into an internal key byte vector.
pub fn encode_internal_key(user_key: &[u8], ts: u64) -> Vec<u8> {
    // Always one extra group for the terminator (even for empty key).
    let groups = user_key.len() / ENC_GROUP_SIZE + 1;
    let encoded_len = groups * (ENC_GROUP_SIZE + 1) + 8;
    let mut buf = Vec::with_capacity(encoded_len);
    encode_memcomparable_user_key_to(&mut buf, user_key);
    let inv_ts = u64::MAX - ts;
    buf.extend_from_slice(&inv_ts.to_be_bytes());

    buf
}

/// Append an encoded internal key to an existing buffer.
pub fn encode_internal_key_to_buf(buf: &mut Vec<u8>, user_key: &[u8], ts: u64) {
    encode_memcomparable_user_key_to(buf, user_key);
    let inv_ts = u64::MAX - ts;
    buf.extend_from_slice(&inv_ts.to_be_bytes());
}

/// Returns the encoded internal key length for a given user key length.
/// Used to validate key size before encoding (RFC §6.1).
pub fn encoded_internal_key_len(user_key_len: usize) -> usize {
    let groups = user_key_len / ENC_GROUP_SIZE + 1;
    groups
        .checked_mul(ENC_GROUP_SIZE + 1)
        .and_then(|n| n.checked_add(8)) // +8 for timestamp
        .unwrap_or(usize::MAX)
}

/// Maximum allowed encoded internal key length (u16::MAX).
/// WAL, block builder, and vLog all store key lengths as u16.
pub const MAX_ENCODED_KEY_LEN: usize = u16::MAX as usize;

/// Extract the encoded user-key prefix (memcomparable form, without timestamp).
/// Returns the bytes before the 8-byte suffix timestamp.
pub fn encoded_user_key_prefix(encoded: &[u8]) -> Option<&[u8]> {
    // Minimum: 1 group (9 bytes) + 8-byte ts = 17 bytes
    if encoded.len() < 17 {
        return None;
    }
    Some(&encoded[..encoded.len() - 8])
}

/// Extract the timestamp from an encoded internal key.
/// Returns `None` if the key is too short or malformed.
pub fn extract_ts(encoded: &[u8]) -> Option<u64> {
    if encoded.len() < 17 {
        return None;
    }
    let ts_start = encoded.len() - 8;
    let inv_ts = u64::from_be_bytes(encoded[ts_start..].try_into().ok()?);

    Some(u64::MAX - inv_ts)
}

/// Decode the memcomparable-encoded user key prefix into a destination buffer.
/// Returns `false` if the encoded prefix is malformed.
pub fn decode_user_key_into(encoded_prefix: &[u8], dst: &mut Vec<u8>) -> bool {
    dst.clear();
    // The encoded prefix must be a multiple of (ENC_GROUP_SIZE + 1) bytes
    if encoded_prefix.is_empty() || !encoded_prefix.len().is_multiple_of(ENC_GROUP_SIZE + 1) {
        return false;
    }
    let mut i = 0;
    while i + ENC_GROUP_SIZE <= encoded_prefix.len() {
        let group = &encoded_prefix[i..i + ENC_GROUP_SIZE];
        let marker = encoded_prefix[i + ENC_GROUP_SIZE];
        let pad_count = ENC_MARKER.wrapping_sub(marker) as usize;
        if pad_count > ENC_GROUP_SIZE {
            return false;
        }
        if pad_count > 0 {
            // Padding bytes must be 0x00 to preserve sort order
            if !group[ENC_GROUP_SIZE - pad_count..]
                .iter()
                .all(|&b| b == ENC_PAD)
            {
                return false;
            }
            dst.extend_from_slice(&group[..ENC_GROUP_SIZE - pad_count]);
            i += ENC_GROUP_SIZE + 1;
            // Padded group must be the last group
            return i == encoded_prefix.len();
        }
        dst.extend_from_slice(group);
        i += ENC_GROUP_SIZE + 1;
    }
    i == encoded_prefix.len()
}

/// Decode the user key from an encoded internal key.
/// Returns `None` if the key is malformed.
pub fn decode_user_key(encoded: &[u8]) -> Option<Vec<u8>> {
    let prefix = encoded_user_key_prefix(encoded)?;
    let mut buf = Vec::new();
    if decode_user_key_into(prefix, &mut buf) {
        Some(buf)
    } else {
        None
    }
}

/// Decode the user key into a `Cow`.
/// With memcomparable encoding, decoding always requires processing markers,
/// so this always returns `Cow::Owned`.
pub fn decode_user_key_cow(encoded: &[u8]) -> Option<Cow<'_, [u8]>> {
    decode_user_key(encoded).map(Cow::Owned)
}

pub struct Key<T: AsRef<[u8]>>(T);

pub type KeySlice<'a> = Key<&'a [u8]>;
pub type KeyVec = Key<Vec<u8>>;
pub type KeyBytes = Key<Bytes>;

impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }

    /// Return the timestamp embedded in this encoded internal key.
    pub fn ts(&self) -> u64 {
        extract_ts(self.0.as_ref()).unwrap_or(0)
    }

    /// Return the encoded user-key prefix (memcomparable form, without timestamp suffix).
    pub fn encoded_user_key(&self) -> &[u8] {
        encoded_user_key_prefix(self.0.as_ref()).unwrap_or_else(|| self.0.as_ref())
    }

    /// Decode the user key from this internal key.
    pub fn decode_user_key(&self) -> Vec<u8> {
        decode_user_key(self.0.as_ref()).unwrap_or_else(|| self.0.as_ref().to_vec())
    }

    /// Decode the user key into a `Cow`, avoiding allocation when possible.
    pub fn decode_user_key_cow(&self) -> Cow<'_, [u8]> {
        decode_user_key_cow(self.0.as_ref()).unwrap_or(Cow::Borrowed(self.0.as_ref()))
    }

    /// Decode the user key into a caller-provided buffer.
    pub fn decode_user_key_into(&self, dst: &mut Vec<u8>) {
        if let Some(prefix) = encoded_user_key_prefix(self.0.as_ref()) {
            decode_user_key_into(prefix, dst);
        } else {
            dst.clear();
        }
    }

    /// Return the raw encoded internal key bytes.
    pub fn raw_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// For testing: return the comparable key bytes.
    /// When TS_ENABLED, returns the encoded user-key prefix (memcomparable form,
    /// without timestamp suffix).
    pub fn for_testing_key_ref(&self) -> &[u8] {
        if TS_ENABLED {
            self.encoded_user_key()
        } else {
            self.0.as_ref()
        }
    }

    /// For testing: return the timestamp.
    pub fn for_testing_ts(self) -> u64 {
        self.ts()
    }
}

impl Key<Vec<u8>> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Create a `KeyVec` from pre-encoded internal key bytes.
    pub fn from_vec(key: Vec<u8>) -> Self {
        Self(key)
    }

    /// Create a `KeyVec` from a user key + timestamp, encoding the internal key.
    pub fn from_user_key_ts(user_key: &[u8], ts: u64) -> Self {
        Self(encode_internal_key(user_key, ts))
    }

    /// Set from a user key + timestamp, reusing buffer capacity.
    pub fn set_from_user_key_ts(&mut self, user_key: &[u8], ts: u64) {
        self.0.clear();
        let groups = user_key.len() / ENC_GROUP_SIZE + 1;
        self.0.reserve(groups * (ENC_GROUP_SIZE + 1) + 8);
        encode_internal_key_to_buf(&mut self.0, user_key, ts);
    }

    /// Clears the key.
    pub fn clear(&mut self) {
        self.0.clear()
    }

    /// Append a slice to the end of the key (raw bytes, no encoding).
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    /// Set the key from a `KeySlice` (which holds encoded internal key bytes),
    /// reusing buffer capacity.
    pub fn set_from_slice(&mut self, key_slice: KeySlice) {
        self.0.clear();
        self.0.reserve(key_slice.len() + 1);
        self.0.extend(key_slice.0);
    }

    pub fn as_key_slice(&self) -> KeySlice<'_> {
        Key(self.0.as_slice())
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        Key(self.0.into())
    }

    /// For testing: create from raw bytes (no encoding).
    pub fn for_testing_from_vec_no_ts(key: Vec<u8>) -> Self {
        Self(key)
    }
}

impl Key<Bytes> {
    pub fn as_key_slice(&self) -> KeySlice<'_> {
        Key(&self.0)
    }

    /// Create a `KeyBytes` from pre-encoded internal key bytes.
    pub fn from_bytes(bytes: Bytes) -> KeyBytes {
        Key(bytes)
    }

    /// For testing: create from raw bytes (no encoding).
    pub fn for_testing_from_bytes_no_ts(bytes: Bytes) -> KeyBytes {
        Key(bytes)
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        Key(self.0.to_vec())
    }

    /// Create a `KeySlice` from pre-encoded internal key bytes.
    pub fn from_slice(slice: &'a [u8]) -> Self {
        Self(slice)
    }

    /// For testing: create from raw bytes with no timestamp encoding.
    /// Returns the raw slice as-is regardless of `TS_ENABLED`.
    /// Callers who need a TS-encoded owned key should use
    /// `KeyVec::from_user_key_ts` instead.
    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self(slice)
    }

    /// For testing: encode a user key + timestamp and return a `KeyVec`.
    pub fn for_testing_from_slice_with_ts(user_key: &[u8], ts: u64) -> KeyVec {
        KeyVec::from_user_key_ts(user_key, ts)
    }
}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- encode/decode round-trip ---

    #[test]
    fn test_encode_decode_round_trip() {
        let cases: Vec<(&[u8], u64)> = vec![
            (b"hello", 0),
            (b"hello", 42),
            (b"hello", u64::MAX),
            (b"", 100),
            (&[0x00, 0x01, 0x00, 0xff], 999),
            (b"a", 1),                // exactly 1 group
            (b"abcdefgh", 7),         // exactly 1 full group
            (b"abcdefghi", 8),        // 2 groups (1 full + 1 partial)
            (b"abcdefghijklmnop", 9), // exactly 2 full groups
            (&[0x00; 16], 42),        // all zeros, 2 groups
        ];
        for (uk, ts) in cases {
            let enc = encode_internal_key(uk, ts);
            assert_eq!(decode_user_key(&enc).unwrap(), uk);
            assert_eq!(extract_ts(&enc).unwrap(), ts);
        }
    }

    // --- memcomparable encoding structure ---

    #[test]
    fn test_encode_empty_key() {
        let enc = encode_internal_key(b"", 0);
        // Empty key: 1 group of 8 zero bytes + marker 0xF8 + 8-byte ts = 17 bytes
        assert_eq!(enc.len(), 17);
        assert_eq!(enc[8], 247); // marker = 0xFF - 8 = 247
        // Timestamp for ts=0: !0 = u64::MAX
        assert_eq!(&enc[9..17], &u64::MAX.to_be_bytes());
    }

    #[test]
    fn test_encode_short_key() {
        // "key" = 3 bytes → pad 5 → marker = 0xFF - 5 = 0xFA
        let enc = encode_internal_key(b"key", 100);
        assert_eq!(enc.len(), 17); // 9 (encoded key) + 8 (ts)
        assert_eq!(&enc[0..3], b"key");
        assert_eq!(&enc[3..8], &[0, 0, 0, 0, 0]); // padding
        assert_eq!(enc[8], 0xFA); // marker
    }

    #[test]
    fn test_encode_full_group_key() {
        // "abcdefgh" = 8 bytes → group1 (8 bytes + 0xFF) + terminator (8 zeros + 0xF7)
        let enc = encode_internal_key(b"abcdefgh", 1);
        assert_eq!(enc.len(), 26); // 18 (encoded key) + 8 (ts)
        assert_eq!(&enc[0..8], b"abcdefgh");
        assert_eq!(enc[8], 0xFF); // group1 marker (no padding)
        assert_eq!(&enc[9..17], &[0, 0, 0, 0, 0, 0, 0, 0]); // terminator padding
        assert_eq!(enc[17], 0xF7); // terminator marker = 0xFF - 8
    }

    #[test]
    fn test_encode_two_group_key() {
        // "abcdefghi" = 9 bytes → group1 (8+0xFF) + group2 (1+pad7+0xF8)
        // The group2 with pad_count=7 IS the terminator (marker < 0xFF)
        let enc = encode_internal_key(b"abcdefghi", 0);
        assert_eq!(enc.len(), 26); // 18 (encoded key) + 8 (ts)
        assert_eq!(&enc[0..8], b"abcdefgh");
        assert_eq!(enc[8], 0xFF); // group1 marker
        assert_eq!(enc[9], b'i');
        assert_eq!(&enc[10..17], &[0, 0, 0, 0, 0, 0, 0]); // padding
        assert_eq!(enc[17], 0xF8); // group2 marker = 0xFF - 7
    }

    #[test]
    fn test_encode_all_zeros_key() {
        let key = [0x00u8; 5];
        let enc = encode_internal_key(&key, 42);
        assert_eq!(enc.len(), 17); // 9 + 8
        assert_eq!(&enc[0..5], &[0, 0, 0, 0, 0]);
        assert_eq!(&enc[5..8], &[0, 0, 0]); // pad
        assert_eq!(enc[8], 0xFC); // marker = 0xFF - 3
        assert_eq!(decode_user_key(&enc).unwrap(), key);
    }

    // --- decode_user_key_cow (always Owned with memcomparable) ---

    #[test]
    fn test_decode_user_key_cow_always_owned() {
        // Memcomparable encoding always requires decoding (markers differ from data)
        let enc = encode_internal_key(b"abc", 10);
        let cow = decode_user_key_cow(&enc).unwrap();
        assert_eq!(&*cow, b"abc");
        // Always Owned — memcomparable always needs decode
        match cow {
            std::borrow::Cow::Owned(_) => {}
            _ => panic!("expected Owned variant with memcomparable encoding"),
        }
    }

    #[test]
    fn test_decode_user_key_cow_with_zeros() {
        let enc = encode_internal_key(&[0x00, 0x42, 0x00], 5);
        let cow = decode_user_key_cow(&enc).unwrap();
        assert_eq!(&*cow, &[0x00, 0x42, 0x00]);
    }

    // --- decode_user_key ---

    #[test]
    fn test_decode_user_key_too_short() {
        // Less than 17 bytes → None
        assert!(decode_user_key(&[0u8; 16]).is_none());
    }

    // --- encoded_user_key_prefix ---

    #[test]
    fn test_encoded_user_key_prefix() {
        let enc = encode_internal_key(b"key", 100);
        let prefix = encoded_user_key_prefix(&enc).unwrap();
        // Prefix is the memcomparable-encoded user key (without 8-byte ts suffix)
        assert_eq!(prefix.len(), 9); // 1 group of 8 + marker
        assert_eq!(&prefix[0..3], b"key");
        assert_eq!(prefix[8], 0xFA); // marker = 0xFF - 5
    }

    #[test]
    fn test_encoded_user_key_prefix_multi_group() {
        let enc = encode_internal_key(b"abcdefghi", 1);
        let prefix = encoded_user_key_prefix(&enc).unwrap();
        assert_eq!(prefix.len(), 18); // 2 groups * 9
    }

    // --- Key accessors ---

    #[test]
    fn test_key_ts_and_user_key() {
        let k = KeyVec::from_user_key_ts(b"mykey", 42);
        assert_eq!(k.ts(), 42);
        assert_eq!(k.decode_user_key(), b"mykey");
        // encoded_user_key returns the memcomparable prefix
        assert_eq!(k.encoded_user_key().len(), 9); // 1 group + marker
    }

    #[test]
    fn test_key_ordering_newer_first() {
        // Higher ts → lower inverted_ts → sorts first
        let k_new = KeyVec::from_user_key_ts(b"same", 100);
        let k_old = KeyVec::from_user_key_ts(b"same", 1);
        assert!(k_new.as_key_slice() < k_old.as_key_slice());
    }

    #[test]
    fn test_key_ordering_different_user_keys() {
        let k_a = KeyVec::from_user_key_ts(b"aaa", 10);
        let k_b = KeyVec::from_user_key_ts(b"bbb", 10);
        assert!(k_a.as_key_slice() < k_b.as_key_slice());
    }

    #[test]
    fn test_key_ordering_full_group_prefix() {
        // Keys whose length is an exact multiple of 8 must sort BEFORE longer
        // keys with the same prefix. The terminator group ensures this.
        let k8 = KeyVec::from_user_key_ts(b"abcdefgh", 1); // 8 bytes
        let k9 = KeyVec::from_user_key_ts(b"abcdefghi", 1); // 9 bytes (same prefix)
        assert!(
            k8.as_key_slice() < k9.as_key_slice(),
            "8-byte key must sort before 9-byte key with same prefix"
        );
    }

    // --- encode_internal_key_to_buf ---

    #[test]
    fn test_encode_internal_key_to_buf() {
        let mut buf = Vec::new();
        encode_internal_key_to_buf(&mut buf, b"hello", 42);
        let expected = encode_internal_key(b"hello", 42);
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_encode_internal_key_to_buf_with_zeros() {
        let mut buf = Vec::new();
        encode_internal_key_to_buf(&mut buf, &[0x00, 0x42], 10);
        let expected = encode_internal_key(&[0x00, 0x42], 10);
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_encode_internal_key_to_buf_append() {
        let mut buf = b"prefix".to_vec();
        encode_internal_key_to_buf(&mut buf, b"k", 1);
        assert!(buf.starts_with(b"prefix"));
        assert!(buf.len() > 6);
    }

    // --- KeyVec::set_from_user_key_ts ---

    #[test]
    fn test_keyvec_set_from_user_key_ts() {
        let mut k = KeyVec::new();
        k.set_from_user_key_ts(b"key", 99);
        assert_eq!(k.ts(), 99);
        assert_eq!(k.decode_user_key(), b"key");
    }

    #[test]
    fn test_keyvec_set_from_user_key_ts_reuse() {
        let mut k = KeyVec::from_user_key_ts(b"old", 1);
        k.set_from_user_key_ts(b"new", 2);
        assert_eq!(k.ts(), 2);
        assert_eq!(k.decode_user_key(), b"new");
    }

    // --- decode_user_key_into edge cases ---

    #[test]
    fn test_decode_user_key_into_empty() {
        let mut dst = Vec::new();
        assert!(!decode_user_key_into(&[], &mut dst));
    }

    #[test]
    fn test_decode_user_key_into_not_multiple_of_group() {
        // 5 bytes is not a multiple of 9 (ENC_GROUP_SIZE + 1)
        let mut dst = Vec::new();
        assert!(!decode_user_key_into(&[0u8; 5], &mut dst));
    }

    #[test]
    fn test_decode_user_key_into_bad_marker() {
        // Marker 0x00 would imply pad_count = 0xFF - 0x00 = 255 > 8 → invalid
        let mut data = vec![0u8; 8];
        data.push(0x00); // bad marker
        let mut dst = Vec::new();
        assert!(!decode_user_key_into(&data, &mut dst));
    }

    #[test]
    fn test_decode_user_key_into_non_zero_padding() {
        // pad_count=5, but padding bytes are 0x01 instead of 0x00 → invalid
        let mut data = vec![0x41; 8]; // group: "AAAAAAAA"
        data.push(0xFC); // marker = 0xFF - 3 → pad_count=3
        // Append a second group with non-zero padding
        let mut group2 = vec![0x42, 0x42, 0x42, 0x42, 0x42, 0x01, 0x01, 0x01]; // bad padding
        group2.push(0xFC); // marker = 0xFF - 3 → pad_count=3
        data.extend_from_slice(&group2);
        let mut dst = Vec::new();
        assert!(!decode_user_key_into(&data, &mut dst));
    }

    #[test]
    fn test_decode_user_key_into_empty_terminator_group() {
        // pad_count=8 (empty terminator group) at position > 0 is valid —
        // this is the terminator for keys whose length is an exact multiple of 8.
        let mut data = vec![0x41; 8]; // first group: "AAAAAAAA"
        data.push(0xFF); // marker = 0xFF - 0 → pad_count=0, full group
        // Second group: empty terminator (pad_count=8)
        data.extend_from_slice(&[0x00; 8]);
        data.push(0xF7); // marker = 0xFF - 8 → pad_count=8
        let mut dst = Vec::new();
        assert!(decode_user_key_into(&data, &mut dst));
        assert_eq!(dst, b"AAAAAAAA");
    }

    // --- extract_ts edge cases ---

    #[test]
    fn test_extract_ts_too_short() {
        // Less than 17 bytes → None
        assert!(extract_ts(&[0u8; 16]).is_none());
    }

    #[test]
    fn test_extract_ts_exactly_minimum() {
        // 17 bytes: 9 bytes encoded user key + 8 bytes ts
        // Marker 0xF8 = 0xFF - 7 → 1 user data byte + 7 padding
        let inv_ts = u64::MAX - 42;
        let mut enc = vec![0u8; 9];
        enc[8] = 0xF8;
        enc.extend_from_slice(&inv_ts.to_be_bytes());
        assert_eq!(extract_ts(&enc).unwrap(), 42);
    }

    // --- Key trait methods ---

    #[test]
    fn test_key_vec_new_and_clear() {
        let mut k = KeyVec::new();
        assert!(k.is_empty());
        k.append(b"hello");
        assert_eq!(k.len(), 5);
        k.clear();
        assert!(k.is_empty());
    }

    #[test]
    fn test_key_vec_set_from_slice() {
        let src = KeyVec::from_user_key_ts(b"key", 10);
        let mut dst = KeyVec::new();
        dst.set_from_slice(src.as_key_slice());
        assert_eq!(dst.as_key_slice(), src.as_key_slice());
    }

    #[test]
    fn test_key_vec_into_key_bytes() {
        let k = KeyVec::from_user_key_ts(b"key", 5);
        let kb = k.into_key_bytes();
        assert_eq!(kb.ts(), 5);
        assert_eq!(kb.decode_user_key(), b"key");
    }

    #[test]
    fn test_key_bytes_from_bytes() {
        let enc = encode_internal_key(b"test", 77);
        let kb = KeyBytes::from_bytes(Bytes::from(enc));
        assert_eq!(kb.ts(), 77);
        assert_eq!(kb.decode_user_key(), b"test");
    }

    #[test]
    fn test_key_slice_to_key_vec() {
        let k = KeyVec::from_user_key_ts(b"abc", 3);
        let slice = k.as_key_slice();
        let v = slice.to_key_vec();
        assert_eq!(v.ts(), 3);
        assert_eq!(v.decode_user_key(), b"abc");
    }

    #[test]
    fn test_for_testing_from_slice_no_ts() {
        let k = KeySlice::for_testing_from_slice_no_ts(b"raw");
        assert_eq!(k.raw_ref(), b"raw");
    }

    #[test]
    fn test_for_testing_from_vec_no_ts() {
        let k = KeyVec::for_testing_from_vec_no_ts(b"raw".to_vec());
        assert_eq!(k.raw_ref(), b"raw");
    }

    #[test]
    fn test_for_testing_from_bytes_no_ts() {
        let k = KeyBytes::for_testing_from_bytes_no_ts(Bytes::from_static(b"raw"));
        assert_eq!(k.raw_ref(), b"raw");
    }

    #[test]
    fn test_for_testing_from_slice_with_ts() {
        let k = KeySlice::for_testing_from_slice_with_ts(b"key", 55);
        assert_eq!(k.ts(), 55);
        assert_eq!(k.decode_user_key(), b"key");
    }

    // --- shared_bytes_from_slice ---

    #[test]
    fn test_shared_bytes_from_slice_empty() {
        let b = shared_bytes_from_slice(b"");
        assert!(b.is_empty());
    }

    #[test]
    fn test_shared_bytes_from_slice_nonempty() {
        let b = shared_bytes_from_slice(b"hello");
        assert_eq!(&*b, b"hello");
    }

    // --- Key default ---

    #[test]
    fn test_key_default() {
        let k: KeyVec = Key::default();
        assert!(k.is_empty());
    }

    // --- Key debug ---

    #[test]
    fn test_key_debug() {
        let k = KeyVec::from_user_key_ts(b"key", 1);
        let dbg = format!("{:?}", k);
        assert!(!dbg.is_empty());
    }

    // --- Cover remaining uncovered lines ---

    #[test]
    fn test_key_into_inner() {
        let k = KeyVec::from_user_key_ts(b"abc", 5);
        let inner: Vec<u8> = k.into_inner();
        assert!(!inner.is_empty());
    }

    #[test]
    fn test_key_slice_into_inner() {
        let data = b"hello";
        let k = KeySlice::from_slice(data);
        let inner: &[u8] = k.into_inner();
        assert_eq!(inner, data);
    }

    #[test]
    fn test_key_decode_user_key_into_method() {
        let k = KeyVec::from_user_key_ts(b"test", 42);
        let mut dst = Vec::new();
        k.decode_user_key_into(&mut dst);
        assert_eq!(dst, b"test");
    }

    #[test]
    fn test_key_encoded_user_key_fallback() {
        // When encoded_user_key_prefix returns None (key too short),
        // falls back to raw_ref
        let k = KeySlice::from_slice(b"short"); // 5 bytes < 17
        let uk = k.encoded_user_key();
        assert_eq!(uk, b"short");
    }

    #[test]
    fn test_key_for_testing_ts() {
        let k = KeyVec::from_user_key_ts(b"key", 77);
        let slice = k.as_key_slice();
        assert_eq!(slice.for_testing_ts(), 77);
    }

    #[test]
    fn test_key_vec_for_testing_key_ref() {
        let k = KeyVec::from_user_key_ts(b"key", 1);
        // for_testing_key_ref returns encoded_user_key when TS_ENABLED
        // Memcomparable prefix for "key" = [k, e, y, 0, 0, 0, 0, 0, 0xFA]
        let r = k.for_testing_key_ref();
        assert_eq!(r, &[b'k', b'e', b'y', 0, 0, 0, 0, 0, 0xFA]);
    }

    #[test]
    fn test_decode_user_key_cow_malformed_short() {
        // Too short for any valid encoded key
        assert!(decode_user_key_cow(&[0x41, 0x00]).is_none());
    }

    #[test]
    fn test_extract_ts_malformed_short() {
        assert!(extract_ts(&[0x41, 0x42, 0x43]).is_none());
    }

    #[test]
    fn test_encoded_user_key_prefix_short() {
        assert!(encoded_user_key_prefix(b"no_term").is_none());
    }

    #[test]
    fn test_decode_user_key_short() {
        assert!(decode_user_key(b"no_term").is_none());
    }
}
