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
// Internal key encoding helpers (RFC Section 6.1)
//
// Format: [escaped_user_key][0x00 0x00 terminator][inverted_ts: u64 BE]
//
// Escaping rules:
//   - Non-zero byte `b`      → `b`
//   - Zero byte `0x00`       → `0x00 0xff`
//   - End of user key        → `0x00 0x00`
//   - Inverted timestamp     → `u64::MAX - ts`, big-endian
//
// This preserves byte-order: user keys sort ascending, and for the same user
// key newer timestamps (higher ts → lower inverted_ts) sort first.
// ---------------------------------------------------------------------------

/// Encode a user key + timestamp into an internal key byte vector.
pub fn encode_internal_key(user_key: &[u8], ts: u64) -> Vec<u8> {
    // Worst case: every byte is 0x00 (doubled) + 2-byte terminator + 8-byte ts
    let mut buf = Vec::with_capacity(user_key.len() * 2 + 2 + 8);
    for &b in user_key {
        if b == 0 {
            buf.push(0x00);
            buf.push(0xff);
        } else {
            buf.push(b);
        }
    }
    // Terminator: 0x00 0x00
    buf.push(0x00);
    buf.push(0x00);
    // Inverted timestamp, big-endian
    let inv_ts = u64::MAX - ts;
    buf.extend_from_slice(&inv_ts.to_be_bytes());
    buf
}

/// Append an encoded internal key to an existing buffer.
pub fn encode_internal_key_to_buf(buf: &mut Vec<u8>, user_key: &[u8], ts: u64) {
    for &b in user_key {
        if b == 0 {
            buf.push(0x00);
            buf.push(0xff);
        } else {
            buf.push(b);
        }
    }
    buf.push(0x00);
    buf.push(0x00);
    let inv_ts = u64::MAX - ts;
    buf.extend_from_slice(&inv_ts.to_be_bytes());
}

/// Find the offset of the `0x00 0x00` terminator in an encoded internal key.
/// Returns `None` if the terminator is not found (malformed key).
fn find_terminator_offset(encoded: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i < encoded.len() {
        if encoded[i] == 0x00 {
            if i + 1 >= encoded.len() {
                return None;
            }
            match encoded[i + 1] {
                0x00 => return Some(i), // terminator
                0xff => i += 2,         // escaped zero, skip both
                _ => return None,       // malformed
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Decode the user key from an encoded internal key into a destination buffer.
/// Returns `false` if the encoded key is malformed.
pub fn decode_user_key_into(encoded: &[u8], dst: &mut Vec<u8>) -> bool {
    dst.clear();
    let mut i = 0;
    while i < encoded.len() {
        if encoded[i] == 0x00 {
            if i + 1 >= encoded.len() {
                return false;
            }
            match encoded[i + 1] {
                0x00 => return true, // terminator
                0xff => dst.push(0), // escaped zero
                _ => return false,   // malformed
            }
            i += 2;
        } else {
            dst.push(encoded[i]);
            i += 1;
        }
    }
    false // no terminator found
}

/// Decode the user key from an encoded internal key.
/// Returns `None` if the key is malformed.
pub fn decode_user_key(encoded: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    if decode_user_key_into(encoded, &mut buf) {
        Some(buf)
    } else {
        None
    }
}

/// Extract the encoded user-key prefix (including escaping, excluding terminator).
/// This is useful for comparisons that don't need the decoded key.
pub fn encoded_user_key_prefix(encoded: &[u8]) -> Option<&[u8]> {
    find_terminator_offset(encoded).map(|off| &encoded[..off])
}

/// Extract the timestamp from an encoded internal key.
/// Returns `None` if the key is too short or malformed.
pub fn extract_ts(encoded: &[u8]) -> Option<u64> {
    let term_off = find_terminator_offset(encoded)?;
    let ts_start = term_off + 2; // skip 0x00 0x00
    if ts_start + 8 > encoded.len() {
        return None;
    }
    let inv_ts = u64::from_be_bytes(encoded[ts_start..ts_start + 8].try_into().ok()?);
    Some(u64::MAX - inv_ts)
}

/// Decode the user key into a `Cow`. If the key contains no escaped zeros,
/// the borrowed variant avoids allocation.
pub fn decode_user_key_cow(encoded: &[u8]) -> Option<Cow<'_, [u8]>> {
    let term_off = find_terminator_offset(encoded)?;
    let escaped = &encoded[..term_off];
    // Check if escaping is needed
    if !escaped.contains(&0x00) {
        return Some(Cow::Borrowed(escaped));
    }
    let mut buf = Vec::with_capacity(term_off);
    let mut i = 0;
    while i < term_off {
        if escaped[i] == 0x00 {
            // Must be 0x00 0xff (escaped zero)
            if i + 1 >= term_off || escaped[i + 1] != 0xff {
                return None; // malformed
            }
            buf.push(0);
            i += 2;
        } else {
            buf.push(escaped[i]);
            i += 1;
        }
    }
    Some(Cow::Owned(buf))
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

    /// Return the encoded user-key prefix (with escaping, without terminator).
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
        decode_user_key_into(self.0.as_ref(), dst);
    }

    /// Return the raw encoded internal key bytes.
    pub fn raw_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// For testing: return the comparable key bytes.
    /// When TS_ENABLED, returns the encoded user-key prefix (escaped form,
    /// without terminator or timestamp). For keys without embedded zero bytes
    /// this equals the raw user key.
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
        // Reserve enough for worst-case encoding
        self.0.reserve(user_key.len() * 2 + 10);
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
        ];
        for (uk, ts) in cases {
            let enc = encode_internal_key(uk, ts);
            assert_eq!(decode_user_key(&enc).unwrap(), uk);
            assert_eq!(extract_ts(&enc).unwrap(), ts);
        }
    }

    // --- decode_user_key_cow ---

    #[test]
    fn test_decode_user_key_cow_no_zeros_borrowed() {
        // No 0x00 bytes → Cow::Borrowed (zero-copy)
        let enc = encode_internal_key(b"abc", 10);
        let cow = decode_user_key_cow(&enc).unwrap();
        assert_eq!(&*cow, b"abc");
        // Should be borrowed, not owned
        match cow {
            std::borrow::Cow::Borrowed(_) => {}
            _ => panic!("expected Borrowed variant for key without zeros"),
        }
    }

    #[test]
    fn test_decode_user_key_cow_with_zeros_owned() {
        // Contains 0x00 bytes → Cow::Owned
        let enc = encode_internal_key(&[0x00, 0x42, 0x00], 5);
        let cow = decode_user_key_cow(&enc).unwrap();
        assert_eq!(&*cow, &[0x00, 0x42, 0x00]);
    }

    #[test]
    fn test_decode_user_key_cow_malformed_escape_returns_none() {
        // 0x00 followed by 0x01 (not 0x00 or 0xff) → malformed
        let malformed = [0x41, 0x00, 0x01, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(decode_user_key_cow(&malformed).is_none());
    }

    #[test]
    fn test_decode_user_key_cow_trailing_zero_returns_none() {
        // Ends with bare 0x00 (no second byte) → malformed
        let malformed = [0x41, 0x00];
        assert!(decode_user_key_cow(&malformed).is_none());
    }

    // --- decode_user_key ---

    #[test]
    fn test_decode_user_key_malformed_returns_none() {
        let malformed = [0x41, 0x00, 0x02, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(decode_user_key(&malformed).is_none());
    }

    // --- encoded_user_key_prefix ---

    #[test]
    fn test_encoded_user_key_prefix() {
        let enc = encode_internal_key(b"key", 100);
        let prefix = encoded_user_key_prefix(&enc).unwrap();
        // Prefix is the escaped form (no 0x00 bytes in "key")
        assert_eq!(prefix, b"key");
    }

    // --- Key accessors ---

    #[test]
    fn test_key_ts_and_user_key() {
        let k = KeyVec::from_user_key_ts(b"mykey", 42);
        assert_eq!(k.ts(), 42);
        assert_eq!(k.decode_user_key(), b"mykey");
        assert_eq!(k.encoded_user_key(), b"mykey");
    }

    #[test]
    fn test_key_ordering_newer_first() {
        // Higher ts → lower inverted_ts → sorts first
        let k_new = KeyVec::from_user_key_ts(b"same", 100);
        let k_old = KeyVec::from_user_key_ts(b"same", 1);
        assert!(k_new.as_key_slice() < k_old.as_key_slice());
    }
}
