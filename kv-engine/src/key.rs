use std::fmt::Debug;

use bytes::Bytes;

pub const TS_ENABLED: bool = false;

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

    pub fn for_testing_ts(self) -> u64 {
        0
    }
}

impl Key<Vec<u8>> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Create a `KeyVec` from a `Vec<u8>`. Will be removed in MVCC.
    pub fn from_vec(key: Vec<u8>) -> Self {
        Self(key)
    }

    /// Clears the key and set ts to 0.
    pub fn clear(&mut self) {
        self.0.clear()
    }

    /// Append a slice to the end of the key
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    /// Set the key from a slice, reusing buffer capacity. Always reserves one
    /// extra byte so that subsequent `into_key_bytes()` creates a `SHARED`
    /// `Bytes` instead of a `PROMOTABLE` one (which incurs an atomic CAS on
    /// the first clone).
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

    /// Always use `raw_ref` to access the key before MVCC. This function will be removed in MVCC.
    pub fn raw_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn for_testing_from_vec_no_ts(key: Vec<u8>) -> Self {
        Self(key)
    }
}

impl Key<Bytes> {
    pub fn as_key_slice(&self) -> KeySlice<'_> {
        Key(&self.0)
    }

    /// Create a `KeyBytes` from a `Bytes`. Will be removed in MVCC.
    pub fn from_bytes(bytes: Bytes) -> KeyBytes {
        Key(bytes)
    }

    /// Always use `raw_ref` to access the key before MVCC. This function will be removed in MVCC.
    pub fn raw_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn for_testing_from_bytes_no_ts(bytes: Bytes) -> KeyBytes {
        Key(bytes)
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        Key(self.0.to_vec())
    }

    /// Create a key slice from a slice. Will be removed in MVCC.
    pub fn from_slice(slice: &'a [u8]) -> Self {
        Self(slice)
    }

    /// Always use `raw_ref` to access the key before MVCC. This function will be removed in MVCC.
    pub fn raw_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self(slice)
    }

    pub fn for_testing_from_slice_with_ts(slice: &'a [u8], _ts: u64) -> Self {
        Self(slice)
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
