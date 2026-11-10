//! RFC 026's composable BLAKE3-256 logical prefix digest.

use anyhow::{Result, bail, ensure};
use blake3::Hasher;

#[cfg(all(feature = "bench", target_os = "linux"))]
pub(crate) fn thread_cpu_time_ns() -> u64 {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: `clock_gettime` initializes the supplied timespec on success.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, time.as_mut_ptr()) } != 0 {
        return 0;
    }
    // SAFETY: the successful `clock_gettime` call initialized `time`.
    let time = unsafe { time.assume_init() };

    u64::try_from(time.tv_sec)
        .unwrap_or(u64::MAX)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(time.tv_nsec).unwrap_or(u64::MAX))
}

#[cfg(all(feature = "bench", not(target_os = "linux")))]
pub(crate) fn thread_cpu_time_ns() -> u64 {
    0
}

pub(crate) const WAL_V8_DIGEST_LEN: usize = 32;
pub(crate) const WAL_V8_LOGICAL_BATCH_HEADER_LEN: usize = 48;
pub(crate) const WAL_V8_LINK_INPUT_LEN: usize = 113;
#[cfg(feature = "bench")]
pub(crate) const WAL_V8_HASH_CPU_SAMPLE_RATE: u64 = 64;

#[cfg(feature = "bench")]
pub(crate) fn should_sample_hash_cpu(ticket: u64) -> bool {
    ticket.is_multiple_of(WAL_V8_HASH_CPU_SAMPLE_RATE)
}

pub(crate) type WalV8Digest = [u8; WAL_V8_DIGEST_LEN];

#[cfg(feature = "bench")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalV8DigestMetrics {
    pub(crate) payload_hash_bytes: u64,
    pub(crate) payload_hash_cpu_ns: u64,
    pub(crate) header_batch_hash_cpu_ns: u64,
    pub(crate) ordered_link_hash_cpu_ns: u64,
    pub(crate) post_encode_hash_reread_bytes: u64,
    pub(crate) cpu_timing_sampled_tickets: u64,
}

#[cfg(not(feature = "bench"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalV8DigestMetrics;

#[cfg(feature = "bench")]
impl WalV8DigestMetrics {
    pub(crate) fn add_assign(&mut self, other: Self) {
        self.payload_hash_bytes = self
            .payload_hash_bytes
            .saturating_add(other.payload_hash_bytes);
        self.payload_hash_cpu_ns = self
            .payload_hash_cpu_ns
            .saturating_add(other.payload_hash_cpu_ns);
        self.header_batch_hash_cpu_ns = self
            .header_batch_hash_cpu_ns
            .saturating_add(other.header_batch_hash_cpu_ns);
        self.ordered_link_hash_cpu_ns = self
            .ordered_link_hash_cpu_ns
            .saturating_add(other.ordered_link_hash_cpu_ns);
        self.post_encode_hash_reread_bytes = self
            .post_encode_hash_reread_bytes
            .saturating_add(other.post_encode_hash_reread_bytes);
        self.cpu_timing_sampled_tickets = self
            .cpu_timing_sampled_tickets
            .saturating_add(other.cpu_timing_sampled_tickets);
    }
}

const HEADER_DOMAIN: &[u8] = b"toy-kv-engine/wal-prefix/v8/header\0";
const ENTRIES_DOMAIN: &[u8] = b"toy-kv-engine/wal-prefix/v8/entries\0";
const BATCH_DOMAIN: &[u8] = b"toy-kv-engine/wal-prefix/v8/batch\0";
const INIT_DOMAIN: &[u8] = b"toy-kv-engine/wal-prefix/v8/init\0";
const LINK_DOMAIN: &[u8] = b"toy-kv-engine/wal-prefix/v8/link\0";
const _: [(); WAL_V8_LINK_INPUT_LEN] = [(); LINK_DOMAIN.len() + WAL_V8_DIGEST_LEN * 2 + 16];

/// Incremental hash state for exactly one canonical v8 entry stream.
///
/// Callers feed the same emitted canonical ranges to the entry CRC and this
/// hasher. `finish` rejects a stream whose encoded length differs from the
/// preflighted length. An update error permanently invalidates the hasher;
/// an admitted producer must also propagate that error to the WAL runtime.
pub(crate) struct WalV8EntryHasher {
    hasher: Hasher,
    expected_len: u64,
    encoded_len: u64,
    failed: bool,
    #[cfg(feature = "bench")]
    hash_cpu_ns: u64,
    #[cfg(feature = "bench")]
    measure_cpu: bool,
}

impl WalV8EntryHasher {
    pub(crate) fn new(expected_len: u64) -> Self {
        Self::new_inner(expected_len, false)
    }

    #[cfg(feature = "bench")]
    pub(crate) fn new_for_ticket(expected_len: u64, ticket: u64) -> Self {
        Self::new_inner(expected_len, should_sample_hash_cpu(ticket))
    }

    fn new_inner(expected_len: u64, measure_cpu: bool) -> Self {
        #[cfg(feature = "bench")]
        let started = measure_cpu.then(thread_cpu_time_ns);
        #[cfg(not(feature = "bench"))]
        let _ = measure_cpu;
        let mut hasher = Hasher::new();
        hasher.update(ENTRIES_DOMAIN);
        hasher.update(&expected_len.to_be_bytes());

        Self {
            hasher,
            expected_len,
            encoded_len: 0,
            failed: false,
            #[cfg(feature = "bench")]
            hash_cpu_ns: started
                .map(|started| thread_cpu_time_ns().saturating_sub(started))
                .unwrap_or_default(),
            #[cfg(feature = "bench")]
            measure_cpu,
        }
    }

    pub(crate) fn update(&mut self, canonical_bytes: &[u8]) -> Result<()> {
        ensure!(!self.failed, "v8 entry stream hasher already failed");
        let Some(next_len) = u64::try_from(canonical_bytes.len())
            .ok()
            .and_then(|len| self.encoded_len.checked_add(len))
        else {
            self.failed = true;
            bail!("v8 entry stream length overflows u64");
        };
        if next_len > self.expected_len {
            self.failed = true;
            bail!("v8 entry stream exceeds its preflighted length");
        }

        #[cfg(feature = "bench")]
        let started = self.measure_cpu.then(thread_cpu_time_ns);
        self.hasher.update(canonical_bytes);
        #[cfg(feature = "bench")]
        if let Some(started) = started {
            self.hash_cpu_ns = self
                .hash_cpu_ns
                .saturating_add(thread_cpu_time_ns().saturating_sub(started));
        }
        self.encoded_len = next_len;

        Ok(())
    }

    pub(crate) fn finish(self) -> Result<WalV8Digest> {
        self.validate_complete()?;

        Ok(*self.hasher.finalize().as_bytes())
    }

    pub(crate) fn finish_with_metrics(self) -> Result<(WalV8Digest, WalV8DigestMetrics)> {
        self.validate_complete()?;
        #[cfg(feature = "bench")]
        let started = self.measure_cpu.then(thread_cpu_time_ns);
        let digest = *self.hasher.finalize().as_bytes();
        #[cfg(feature = "bench")]
        let metrics = WalV8DigestMetrics {
            payload_hash_bytes: self.encoded_len,
            payload_hash_cpu_ns: self.hash_cpu_ns.saturating_add(
                started
                    .map(|started| thread_cpu_time_ns().saturating_sub(started))
                    .unwrap_or_default(),
            ),
            cpu_timing_sampled_tickets: u64::from(self.measure_cpu),
            ..WalV8DigestMetrics::default()
        };
        #[cfg(not(feature = "bench"))]
        let metrics = WalV8DigestMetrics;

        Ok((digest, metrics))
    }

    fn validate_complete(&self) -> Result<()> {
        ensure!(!self.failed, "v8 entry stream hasher failed");
        ensure!(
            self.encoded_len == self.expected_len,
            "v8 entry stream length does not match its preflighted length"
        );

        Ok(())
    }
}

/// Hash the full immutable v8 header for the logical digest chain.
///
/// This primitive does not validate the header's format or checksum. A caller
/// must validate the header before using the digest as recovery authority.
pub(crate) fn header_digest(header: &[u8; super::WAL_V8_HEADER_LEN]) -> WalV8Digest {
    hash_parts(HEADER_DOMAIN, &[header])
}

/// Compute the distinct empty-prefix digest P_0 from the logical header seed.
pub(crate) fn initial_prefix_digest(header_seed: &WalV8Digest) -> WalV8Digest {
    hash_parts(INIT_DOMAIN, &[header_seed])
}

/// Bind the exact canonical logical batch header to its canonical entry digest.
///
/// The caller must validate the header, entry stream, CRCs and declared length,
/// and must ensure the entry digest comes from those same canonical bytes.
pub(crate) fn batch_digest(
    logical_header: &[u8; WAL_V8_LOGICAL_BATCH_HEADER_LEN],
    entry_digest: &WalV8Digest,
) -> WalV8Digest {
    hash_parts(BATCH_DOMAIN, &[logical_header, entry_digest])
}

/// Advance the logical prefix using RFC 026's fixed 113-byte ordered link.
///
/// This is a hash primitive, not a validator. Before trusting its output, the
/// caller must verify ticket continuity and the ticket and checked batch length
/// against the validated logical header and DATA fragments.
pub(crate) fn advance_prefix(
    previous: &WalV8Digest,
    ticket: u64,
    logical_batch_len: u64,
    batch: &WalV8Digest,
) -> WalV8Digest {
    let mut input = [0; WAL_V8_LINK_INPUT_LEN];
    let mut offset = 0;

    input[offset..offset + LINK_DOMAIN.len()].copy_from_slice(LINK_DOMAIN);
    offset += LINK_DOMAIN.len();
    input[offset..offset + WAL_V8_DIGEST_LEN].copy_from_slice(previous);
    offset += WAL_V8_DIGEST_LEN;
    input[offset..offset + 8].copy_from_slice(&ticket.to_be_bytes());
    offset += 8;
    input[offset..offset + 8].copy_from_slice(&logical_batch_len.to_be_bytes());
    offset += 8;
    input[offset..offset + WAL_V8_DIGEST_LEN].copy_from_slice(batch);
    offset += WAL_V8_DIGEST_LEN;
    debug_assert_eq!(offset, WAL_V8_LINK_INPUT_LEN);

    *blake3::hash(&input).as_bytes()
}

fn hash_parts(domain: &[u8], parts: &[&[u8]]) -> WalV8Digest {
    let mut hasher = Hasher::new();
    hasher.update(domain);
    for part in parts {
        hasher.update(part);
    }

    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pitr::{LIVE_WAL_V5_LIMITS, RecordedAt, decode_wal_entry_stream},
        wal::v7::codec::WalV7LogicalBatchHeader,
    };

    fn hex<const N: usize>(value: &str) -> [u8; N] {
        assert_eq!(value.len(), N * 2);

        std::array::from_fn(|index| {
            u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap()
        })
    }

    fn entry_digest_in_chunks(data: &[u8], chunk_len: usize) -> Result<WalV8Digest> {
        let mut hasher = WalV8EntryHasher::new(u64::try_from(data.len())?);
        hasher.update(&[])?;
        for chunk in data.chunks(chunk_len) {
            hasher.update(chunk)?;
        }

        hasher.finish()
    }

    #[test]
    fn rfc_026_digest_conformance_vector() -> Result<()> {
        // The RFC vector uses a synthetic header which is not a valid WAL header.
        let header = std::array::from_fn(|index| (index % 256) as u8);
        let logical_header = hex::<48>(concat!(
            "0000000000000000000000000000000100000000000000010000000000000001",
            "00000010c4459325224cded900000000"
        ));
        let entries = hex::<16>("01000000000a000000016b0000000176");

        let h = header_digest(&header);
        assert_eq!(
            h,
            hex("53edd07fe6e3d0dda540770b400ece2538228897a535415cf8958be5a1bb922f")
        );
        let p0 = initial_prefix_digest(&h);
        assert_eq!(
            p0,
            hex("308b9c35ac3fa99809a80e99ad7dde4de8d653dd782ee9adc0dd390ed9bb87e3")
        );
        let e0 = entry_digest_in_chunks(&entries, 7)?;
        assert_eq!(
            e0,
            hex("15c56b2a4fef9ab6690b5f8c68e10b8750ad63a677951adcc5a3aeefa3f45fb4")
        );
        let b0 = batch_digest(&logical_header, &e0);
        assert_eq!(
            b0,
            hex("0bdffcb250aaf751128e25262a7942f608fa99d7857eb43c54c5abf922532c48")
        );
        assert_eq!(
            advance_prefix(&p0, 0, 64, &b0),
            hex("b0d28130323ebce6e10591137ab6623bdb6533e9863b39ef56e9b0d6f59f2d53")
        );

        Ok(())
    }

    fn v8_header_fixture() -> [u8; super::super::WAL_V8_HEADER_LEN] {
        let mut header = [0; super::super::WAL_V8_HEADER_LEN];
        header[..4].copy_from_slice(b"WAL2");
        header[4..6].copy_from_slice(&8_u16.to_be_bytes());
        header[8..10].copy_from_slice(&4096_u16.to_be_bytes());
        for (index, byte) in header[12..44].iter_mut().enumerate() {
            *byte = 0x10 + index as u8;
        }
        header[44..52].copy_from_slice(&0x0102_0304_0506_0708_u64.to_be_bytes());
        for (index, byte) in header[128..144].iter_mut().enumerate() {
            *byte = 0x30 + index as u8;
        }
        header[144..148].copy_from_slice(&[0xb9, 0x7f, 0xe4, 0xf2]);

        header
    }

    fn assert_valid_logical_batch(
        header: &[u8; 48],
        entries: &[u8],
        ticket: u64,
    ) -> Result<WalV7LogicalBatchHeader> {
        let batch_len = 48 + u64::try_from(entries.len())?;
        let decoded = WalV7LogicalBatchHeader::decode(
            header,
            entries,
            ticket,
            batch_len,
            LIVE_WAL_V5_LIMITS,
        )?;
        decode_wal_entry_stream(
            decoded.commit_ts,
            RecordedAt {
                secs: decoded.recorded_at_secs,
                nanos: decoded.recorded_at_nanos,
            },
            decoded.entry_count,
            entries,
            LIVE_WAL_V5_LIMITS,
        )?;

        Ok(decoded)
    }

    #[test]
    fn valid_header_and_two_batches_match_independent_vectors() -> Result<()> {
        // Expected digests were generated independently by the official BLAKE3 C
        // and Rust reference implementations. The second entry crosses a DATA
        // fragment boundary and both batches have negative recorded seconds.
        let header = v8_header_fixture();
        assert_eq!(crc32fast::hash(&header[..144]), 0xb97f_e4f2);
        let h = header_digest(&header);
        assert_eq!(
            h,
            hex("1f2f23b2b7b861b75b908516c230301890e432632652ee1671c9dc697d53708d")
        );
        let p0 = initial_prefix_digest(&h);
        assert_eq!(
            p0,
            hex("56bf0f5971fff54b5f4d11e2cb2b9dd4d51c6ba82ee691966008ab267c7089a6")
        );

        let q0 = hex::<48>(concat!(
            "00000000000000000000000000000001fffffffffffffffd1dcd650000000001",
            "00000010c445932542a3b0b900000000"
        ));
        let s0 = hex::<16>("01000000000a000000016b0000000176");
        let decoded0 = assert_valid_logical_batch(&q0, &s0, 0)?;
        let e0 = entry_digest_in_chunks(&s0, 7)?;
        assert_eq!(
            e0,
            hex("15c56b2a4fef9ab6690b5f8c68e10b8750ad63a677951adcc5a3aeefa3f45fb4")
        );
        let b0 = batch_digest(&q0, &e0);
        assert_eq!(
            b0,
            hex("dca94ad72c4cc0bd3c4e139afc1e4fed6cb3b08b316a29f1e0f5e07b0f123f24")
        );
        let p1 = advance_prefix(&p0, 0, 64, &b0);
        assert_eq!(
            p1,
            hex("9effca1cceea2658a7a1fb4b4a1f2e03ce5a2f97ec1a609b57a2312e4408a79d")
        );

        let q1 = hex::<48>(concat!(
            "00000000000000010000000000000002fffffffffffffffe1dcd650000000001",
            "00000fa99a2887a5566c61ba00000000"
        ));
        let mut s1 = hex::<15>("010000000fa3000000016b00000f9a").to_vec();
        s1.extend((0..3994).map(|index| ((index * 37 + 11) & 255) as u8));
        assert_eq!(s1.len(), 4009);
        assert_valid_logical_batch(&q1, &s1, 1)?;
        let e1 = entry_digest_in_chunks(&s1, 3960)?;
        assert_eq!(
            e1,
            hex("a7644e1d54029781478f6b49d34ed77416d6795196135d223b0ed7ade633a2d3")
        );
        let b1 = batch_digest(&q1, &e1);
        assert_eq!(
            b1,
            hex("f4a9ca98d0a4fbec1bd015a138f7cca22050dd5f7e225206993b9c0e5aa8bf08")
        );
        let p2 = advance_prefix(&p1, 1, 4057, &b1);
        assert_eq!(
            p2,
            hex("463abe7d990aaa4814e13796735767c93509e8c61f0ede850e1c61122d0cd0df")
        );

        let skipped = advance_prefix(&p0, 1, 4057, &b1);
        let duplicated = advance_prefix(&p1, 0, 64, &b0);
        let reordered = advance_prefix(&skipped, 0, 64, &b0);
        assert_ne!(p2, skipped);
        assert_ne!(p2, duplicated);
        assert_ne!(p2, reordered);

        let mut changed_header = header;
        changed_header[12] ^= 1;
        let changed_header_crc = crc32fast::hash(&changed_header[..144]);
        changed_header[144..148].copy_from_slice(&changed_header_crc.to_be_bytes());
        assert_ne!(header_digest(&changed_header), h);

        let mut changed_entries = s0;
        changed_entries[15] ^= 1;
        let changed_q0 = decoded0.encode(&changed_entries, LIVE_WAL_V5_LIMITS)?;
        assert_valid_logical_batch(&changed_q0, &changed_entries, 0)?;
        let changed_e0 = entry_digest_in_chunks(&changed_entries, 7)?;
        assert_ne!(changed_e0, e0);
        let changed_b0 = batch_digest(&changed_q0, &changed_e0);
        assert_ne!(changed_b0, b0);
        assert_ne!(advance_prefix(&p0, 0, 64, &changed_b0), p1);

        Ok(())
    }

    #[test]
    fn streaming_entry_digests_match_independent_boundary_vectors() -> Result<()> {
        // The 979..981 cases straddle BLAKE3's first 1024-byte input chunk
        // including the entry domain and length. The 3959..3961 cases straddle
        // the first DATA payload boundary after the 48-byte logical header.
        let vectors = [
            (
                0,
                "de6b39323573e6cc72c358062fb68b137ad3361235be893c0c77f7e1cef0ae8d",
            ),
            (
                1,
                "e03a1a66911981f687338c31dd30913d70ca0f6ac4c6c9435ee3269a71b10735",
            ),
            (
                979,
                "b7005494dcc8d6a21a43bee07961a4a318506c11480ac43aca05d175161c05ff",
            ),
            (
                980,
                "2e4bcd5269e5230be67976845d3386b7ef54cad51cd585cfc208da0a8ab9e0cc",
            ),
            (
                981,
                "cda1c8e454af32f60257fcd936c7ce0af75b81497b06906c0e41f80ad883d0f7",
            ),
            (
                3959,
                "257750dcd76c5401bea825c9021038feb66c45b3481c528b26776d8feb810054",
            ),
            (
                3960,
                "39c32d877bc2994664b114e8abcce872a06dd38037b85bbb3c942f36574fa40d",
            ),
            (
                3961,
                "352ba764930f69c3190d087d29d3b9075be0be891fbcf7d4d11b555d07f8a25a",
            ),
            (
                4008,
                "cd00ecde57a5e0d6aa8b651b2f8b2d23be94f3f89ed6d8bcf86ab5f94c278e39",
            ),
            (
                4009,
                "2bf4bbd34e5be2eb4cd69be9e5586a6f5d44573600a826f37c07659ea2c21cc4",
            ),
            (
                65536,
                "ecefb6f4312c49b70a7f2055c8ed88611e0e567781900d8cfa110995d99e9cbb",
            ),
        ];
        for (length, expected) in vectors {
            let data: Vec<u8> = (0..length)
                .map(|index| ((index * 37 + 11) & 255) as u8)
                .collect();
            for chunk_len in [1, 7, 64, 1024, 4008, 65536] {
                assert_eq!(
                    entry_digest_in_chunks(&data, chunk_len)?,
                    hex::<32>(expected),
                    "entry length {length}, chunk length {chunk_len}"
                );
            }
        }

        Ok(())
    }

    #[test]
    fn length_errors_permanently_invalidate_the_entry_hasher() -> Result<()> {
        let mut short = WalV8EntryHasher::new(3);
        short.update(&[1, 2])?;
        assert!(short.finish().is_err());

        let mut overlong = WalV8EntryHasher::new(2);
        overlong.update(&[1, 2])?;
        assert!(overlong.update(&[3]).is_err());
        assert!(overlong.update(&[]).is_err());
        assert!(overlong.finish().is_err());

        let mut empty = WalV8EntryHasher::new(0);
        assert!(empty.update(&[1]).is_err());
        assert!(empty.finish().is_err());

        let mut overflow = WalV8EntryHasher::new(u64::MAX);
        overflow.encoded_len = u64::MAX;
        assert!(overflow.update(&[1]).is_err());
        assert!(overflow.finish().is_err());

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn cpu_timing_samples_one_ticket_in_each_fixed_window() {
        assert!(should_sample_hash_cpu(0));
        assert!(!should_sample_hash_cpu(1));
        assert!(!should_sample_hash_cpu(WAL_V8_HASH_CPU_SAMPLE_RATE - 1));
        assert!(should_sample_hash_cpu(WAL_V8_HASH_CPU_SAMPLE_RATE));
        assert!(should_sample_hash_cpu(WAL_V8_HASH_CPU_SAMPLE_RATE * 2));
    }

    #[test]
    fn ordered_link_uses_big_endian_ticket_and_length() {
        let previous = std::array::from_fn(|index| index as u8);
        let batch = std::array::from_fn(|index| 255 - index as u8);
        let ticket = 0x0102_0304_0506_0708;
        let length = 0x1112_1314_1516_1718;
        let result = advance_prefix(&previous, ticket, length, &batch);
        assert_eq!(
            result,
            hex("730e92407fbe778bd394277c687db71806762e850a59d6a134209d043070638d")
        );
        assert_ne!(
            result,
            advance_prefix(&previous, ticket + 1, length, &batch)
        );
        assert_ne!(
            result,
            advance_prefix(&previous, ticket, length + 1, &batch)
        );
        assert_ne!(result, advance_prefix(&[0; 32], ticket, length, &batch));
        assert_ne!(result, advance_prefix(&previous, ticket, length, &[0; 32]));
    }
}
