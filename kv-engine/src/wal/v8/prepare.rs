//! Two-phase canonical batch preparation for the proposed v8 writer.
//!
//! Preflight owns only the canonical operation view and checked sizes. The
//! caller publishes the assigned ticket through `admit` before `encode` can
//! serialize payload bytes or update the payload digest.

use anyhow::{Context, Result, ensure};

use crate::pitr::{CanonicalWalEntryStream, RecordedAt, WalBatch, WalV5Limits};

use super::{WAL_V8_HEADER_LEN, codec, digest};

const WAL_V8_DATA_START: u64 = (WAL_V8_HEADER_LEN + codec::WAL_V8_FRAME_LEN) as u64;

/// Coordinates assigned by the writer after resource reservation and before
/// payload encoding begins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8BatchAssignment {
    pub(crate) segment_ticket: u64,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
    pub(crate) first_data_offset: u64,
    pub(crate) segment_incarnation: [u8; 16],
    pub(crate) physical_header_digest: [u8; 32],
    pub(crate) initial_prefix_digest: digest::WalV8Digest,
}

/// Checked sizes that the runtime must reserve before assigning a ticket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8AdmissionPlan {
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
    pub(crate) logical_batch_bytes: usize,
    pub(crate) fragment_count: u32,
    pub(crate) frame_buffer_bytes: usize,
}

/// Runtime-owned failure guard for an admitted ticket.
///
/// Implementations must remain armed until durable publication. Dropping an
/// armed guard poisons the ticket and wakes group and durability waiters.
pub(crate) trait WalV8AdmissionGuard {
    fn segment_ticket(&self) -> u64;
    fn poison(&mut self);
    fn disarm_after_durable(&mut self);
}

/// Parallel admission boundary used by v8 producers.
///
/// Implementations must reserve all logical/frame/spool capacity before
/// assigning a ticket, publish the `Preparing` ticket to group formation, and
/// return only after that publication is visible. The guard owns the charges
/// and poisons the ticket if dropped before durable publication. If an error
/// occurs after ticket allocation but before the guard is returned, the
/// implementation must poison that ticket before returning the error.
pub(crate) trait WalV8AdmissionPublisher {
    type Guard: WalV8AdmissionGuard;

    fn admit_and_publish(
        &self,
        plan: WalV8AdmissionPlan,
    ) -> Result<(WalV8BatchAssignment, Self::Guard)>;
}

/// Canonical operations and checked encoded sizes, with no serialized payload.
pub(crate) struct PreparedWalV8Batch<'a, R> {
    canonical: CanonicalWalEntryStream<'a>,
    limits: WalV5Limits,
    commit_ts: u64,
    recorded_at: RecordedAt,
    logical_batch_bytes: usize,
    preparation_buffer_bytes: u64,
    fragment_count: u32,
    frame_buffer_bytes: usize,
    preparation_reservation: R,
}

impl<'a, R> PreparedWalV8Batch<'a, R> {
    /// Calculate the canonical point-index charge before allocating it during
    /// preflight.
    pub(crate) fn preparation_buffer_bytes(batch: &WalBatch, limits: WalV5Limits) -> Result<u64> {
        CanonicalWalEntryStream::preparation_buffer_bytes(batch, limits)
    }

    /// Reserve the canonicalization index before allocating it. This only
    /// canonicalizes operations and computes encoded sizes; it does not
    /// serialize or hash payload bytes.
    pub(crate) fn preflight<F>(
        batch: &'a WalBatch,
        limits: WalV5Limits,
        reserve_preparation: F,
    ) -> Result<PreparedWalV8Batch<'a, R>>
    where
        F: FnOnce(u64) -> Result<R>,
    {
        let preparation_bytes = Self::preparation_buffer_bytes(batch, limits)?;
        let preparation_reservation = reserve_preparation(preparation_bytes)?;
        let canonical = CanonicalWalEntryStream::new(batch, limits)?;
        let logical_batch_bytes = codec::WAL_V8_LOGICAL_BATCH_HEADER_LEN
            .checked_add(canonical.data_len())
            .context("v8 logical batch length overflows")?;
        let logical_batch_bytes_u64 =
            u64::try_from(logical_batch_bytes).context("v8 logical batch length exceeds u64")?;
        let fragment_count = codec::data_fragment_count(logical_batch_bytes_u64)
            .context("v8 logical batch length is outside wire bounds")?;
        let frame_buffer_bytes = usize::try_from(fragment_count)?
            .checked_mul(codec::WAL_V8_FRAME_LEN)
            .context("v8 DATA frame buffer length overflows")?;
        Ok(PreparedWalV8Batch {
            canonical,
            limits,
            commit_ts: batch.commit_ts,
            recorded_at: batch.recorded_at,
            logical_batch_bytes,
            preparation_buffer_bytes: preparation_bytes,
            fragment_count,
            frame_buffer_bytes,
            preparation_reservation,
        })
    }

    pub(crate) fn logical_batch_bytes(&self) -> usize {
        self.logical_batch_bytes
    }

    pub(crate) fn preparation_index_bytes(&self) -> u64 {
        self.preparation_buffer_bytes
    }

    pub(crate) fn fragment_count(&self) -> u32 {
        self.fragment_count
    }

    pub(crate) fn frame_buffer_bytes(&self) -> usize {
        self.frame_buffer_bytes
    }

    /// Complete Parallel admission and publication before returning a value
    /// that can encode payload bytes.
    #[cfg(test)]
    pub(crate) fn admit<P>(self, publisher: &P) -> Result<AdmittedWalV8Batch<'a, P::Guard, R>>
    where
        P: WalV8AdmissionPublisher,
    {
        let commit_ts = self.commit_ts;
        let recorded_at = self.recorded_at;
        self.admit_with_metadata(commit_ts, recorded_at, publisher)
    }

    /// Complete admission with MVCC metadata allocated by the sequencer.
    /// Parallel callers can preflight canonical operation sizes before taking
    /// that sequencer, then publish the actual ticket and timestamp before
    /// encoding or hashing payload bytes.
    pub(crate) fn admit_with_metadata<P>(
        self,
        commit_ts: u64,
        recorded_at: RecordedAt,
        publisher: &P,
    ) -> Result<AdmittedWalV8Batch<'a, P::Guard, R>>
    where
        P: WalV8AdmissionPublisher,
    {
        ensure!(commit_ts > 0, "v8 admission commit timestamp is zero");
        ensure!(
            recorded_at.nanos < 1_000_000_000,
            "v8 admission recorded-at timestamp is invalid"
        );
        let plan = WalV8AdmissionPlan {
            commit_ts,
            recorded_at,
            logical_batch_bytes: self.logical_batch_bytes,
            fragment_count: self.fragment_count,
            frame_buffer_bytes: self.frame_buffer_bytes,
        };
        let (assignment, admission_guard) = publisher.admit_and_publish(plan)?;
        validate_assignment(assignment, plan)?;
        ensure!(
            admission_guard.segment_ticket() == assignment.segment_ticket,
            "v8 admission guard ticket does not match its published assignment"
        );

        Ok(AdmittedWalV8Batch {
            canonical: self.canonical,
            limits: self.limits,
            logical_batch_bytes: self.logical_batch_bytes,
            assignment,
            fragment_count: self.fragment_count,
            frame_buffer_bytes: self.frame_buffer_bytes,
            admission_guard,
            preparation_reservation: self.preparation_reservation,
        })
    }
}

fn validate_assignment(assignment: WalV8BatchAssignment, plan: WalV8AdmissionPlan) -> Result<()> {
    ensure!(
        assignment.commit_ts == plan.commit_ts,
        "v8 admission changed the batch commit timestamp"
    );
    ensure!(
        assignment.recorded_at >= plan.recorded_at,
        "v8 admission moved the recorded-at timestamp backwards"
    );
    ensure!(
        assignment.commit_ts > 0,
        "v8 batch commit timestamp is zero"
    );
    ensure!(
        assignment.recorded_at.nanos < 1_000_000_000,
        "v8 recorded-at nanos out of range"
    );
    ensure!(
        assignment.first_data_offset >= WAL_V8_DATA_START
            && assignment
                .first_data_offset
                .is_multiple_of(codec::WAL_V8_FRAME_LEN as u64),
        "v8 DATA allocation starts outside the aligned DATA region"
    );
    assignment
        .first_data_offset
        .checked_add(u64::try_from(plan.frame_buffer_bytes)?)
        .context("v8 DATA allocation end overflows")?;
    Ok(())
}

/// A preflighted batch whose group membership has already been published.
pub(crate) struct AdmittedWalV8Batch<'a, G, R> {
    canonical: CanonicalWalEntryStream<'a>,
    limits: WalV5Limits,
    logical_batch_bytes: usize,
    assignment: WalV8BatchAssignment,
    fragment_count: u32,
    frame_buffer_bytes: usize,
    admission_guard: G,
    preparation_reservation: R,
}

impl<G: WalV8AdmissionGuard, R> AdmittedWalV8Batch<'_, G, R> {
    /// Coordinates published during admission, available before payload
    /// encoding so persistent reservations can be attached to the ticket.
    pub(crate) fn assignment(&self) -> WalV8BatchAssignment {
        self.assignment
    }

    /// Encode canonical entries while updating CRC and BLAKE3 from the same
    /// emitted slices, then finalize the assigned logical header and batch hash.
    pub(crate) fn encode(self) -> Result<EncodedWalV8Batch<G>> {
        let Self {
            preparation_reservation,
            canonical,
            limits,
            logical_batch_bytes,
            assignment,
            fragment_count,
            frame_buffer_bytes,
            admission_guard,
        } = self;
        let entry_stream_len = canonical.data_len();
        let entry_count = canonical.entry_count();
        let mut logical_batch = Vec::new();
        logical_batch
            .try_reserve_exact(logical_batch_bytes)
            .context("reserve admitted v8 logical batch bytes")?;
        logical_batch.resize(logical_batch_bytes, 0);

        let expected_entry_len = u64::try_from(entry_stream_len)?;
        #[cfg(feature = "bench")]
        let mut entry_hasher =
            digest::WalV8EntryHasher::new_for_ticket(expected_entry_len, assignment.segment_ticket);
        #[cfg(not(feature = "bench"))]
        let mut entry_hasher = digest::WalV8EntryHasher::new(expected_entry_len);
        let entry_stream = &mut logical_batch[codec::WAL_V8_LOGICAL_BATCH_HEADER_LEN..];
        let (written, entry_stream_crc) = canonical
            .encode_into_with_observer(entry_stream, |range| entry_hasher.update(range))?;
        ensure!(
            written == entry_stream_len,
            "v8 canonical entry stream length changed while encoding"
        );
        #[cfg(feature = "bench")]
        let (entry_digest, mut digest_metrics) = entry_hasher.finish_with_metrics()?;
        #[cfg(not(feature = "bench"))]
        let entry_digest = entry_hasher.finish()?;

        let header = codec::WalV8LogicalBatchHeader {
            segment_ticket: assignment.segment_ticket,
            commit_ts: assignment.commit_ts,
            recorded_at_secs: assignment.recorded_at.secs,
            recorded_at_nanos: assignment.recorded_at.nanos,
            entry_count,
        }
        .encode_with_data_crc(written, entry_stream_crc, limits)?;
        logical_batch[..codec::WAL_V8_LOGICAL_BATCH_HEADER_LEN].copy_from_slice(&header);
        #[cfg(feature = "bench")]
        let batch_hash_started = digest::should_sample_hash_cpu(assignment.segment_ticket)
            .then(digest::thread_cpu_time_ns);
        let batch_digest = digest::batch_digest(&header, &entry_digest);
        #[cfg(feature = "bench")]
        {
            if let Some(batch_hash_started) = batch_hash_started {
                digest_metrics.header_batch_hash_cpu_ns =
                    digest::thread_cpu_time_ns().saturating_sub(batch_hash_started);
            }
        }

        // Release the shared preparation charge only after its index storage
        // has actually been dropped, on both success and error paths.
        drop(canonical);
        drop(preparation_reservation);

        Ok(EncodedWalV8Batch {
            logical_batch,
            assignment,
            entry_digest,
            batch_digest,
            #[cfg(feature = "bench")]
            digest_metrics,
            fragment_count,
            frame_buffer_bytes,
            admission_guard,
        })
    }
}

/// One completed producer digest, available before physical frame materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8DigestReady {
    pub(crate) segment_incarnation: [u8; 16],
    pub(crate) segment_ticket: u64,
    pub(crate) logical_batch_bytes: u64,
    pub(crate) batch_digest: digest::WalV8Digest,
}

/// Immutable canonical bytes and their digest, ready for independent framing.
pub(crate) struct EncodedWalV8Batch<G> {
    logical_batch: Vec<u8>,
    assignment: WalV8BatchAssignment,
    entry_digest: digest::WalV8Digest,
    batch_digest: digest::WalV8Digest,
    #[cfg(feature = "bench")]
    digest_metrics: digest::WalV8DigestMetrics,
    fragment_count: u32,
    frame_buffer_bytes: usize,
    admission_guard: G,
}

impl<G: WalV8AdmissionGuard> EncodedWalV8Batch<G> {
    pub(crate) fn assignment(&self) -> WalV8BatchAssignment {
        self.assignment
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.logical_batch
    }

    pub(crate) fn entry_digest(&self) -> &digest::WalV8Digest {
        &self.entry_digest
    }

    pub(crate) fn batch_digest(&self) -> &digest::WalV8Digest {
        &self.batch_digest
    }

    #[cfg(feature = "bench")]
    pub(crate) fn digest_metrics(&self) -> digest::WalV8DigestMetrics {
        self.digest_metrics
    }

    /// Publish this fixed-size result independently from DATA framing or I/O.
    pub(crate) fn digest_ready(&self) -> Result<WalV8DigestReady> {
        Ok(WalV8DigestReady {
            segment_incarnation: self.assignment.segment_incarnation,
            segment_ticket: self.assignment.segment_ticket,
            logical_batch_bytes: u64::try_from(self.logical_batch.len())?,
            batch_digest: self.batch_digest,
        })
    }

    pub(crate) fn frame_buffer_bytes(&self) -> usize {
        self.frame_buffer_bytes
    }

    /// Disarm the ticket's poison guard only after durable publication.
    pub(crate) fn disarm_after_durable(&mut self) {
        self.admission_guard.disarm_after_durable();
    }

    /// Materialize the assigned logical bytes into the v8 physical DATA frames.
    /// The output is separately owned so a runtime can publish frame readiness
    /// and digest readiness as independent coordinator events.
    pub(crate) fn encode_data_frames_into(&mut self, output: &mut [u8]) -> Result<usize> {
        let result = self.encode_data_frames(output);
        if result.is_err() {
            self.admission_guard.poison();
        }
        result
    }

    fn encode_data_frames(&self, output: &mut [u8]) -> Result<usize> {
        ensure!(
            output.len() >= self.frame_buffer_bytes,
            "v8 DATA frame output buffer is too small"
        );
        let frame_buffer_bytes_u64 = u64::try_from(self.frame_buffer_bytes)?;
        self.assignment
            .first_data_offset
            .checked_add(frame_buffer_bytes_u64)
            .context("v8 DATA frame range overflows")?;

        let chunk_capacity = codec::WAL_V8_DATA_FRAGMENT_PAYLOAD_CAPACITY;
        let batch_bytes = u64::try_from(self.logical_batch.len())?;
        let actual_fragment_count = codec::data_fragment_count(batch_bytes)
            .context("v8 logical batch length is outside wire bounds")?;
        ensure!(
            actual_fragment_count == self.fragment_count,
            "v8 DATA fragment count changed after admission"
        );

        for (index, payload) in self.logical_batch.chunks(chunk_capacity).enumerate() {
            let fragment_index = u32::try_from(index)?;
            let fragment = codec::WalV8DataFragmentHeader {
                segment_ticket: self.assignment.segment_ticket,
                fragment_index,
                fragment_count: self.fragment_count,
                batch_bytes,
            };
            let frame_offset_delta = u64::try_from(index)?
                .checked_mul(codec::WAL_V8_FRAME_LEN as u64)
                .context("v8 DATA frame offset overflows")?;
            let frame_offset = self
                .assignment
                .first_data_offset
                .checked_add(frame_offset_delta)
                .context("v8 DATA frame offset overflows")?;
            let output_start = index
                .checked_mul(codec::WAL_V8_FRAME_LEN)
                .context("v8 DATA frame output offset overflows")?;
            let output_end = output_start
                .checked_add(codec::WAL_V8_FRAME_LEN)
                .context("v8 DATA frame output end overflows")?;
            codec::encode_data_fragment_frame_into(
                self.assignment.physical_header_digest,
                self.assignment.initial_prefix_digest,
                frame_offset,
                fragment,
                payload,
                &mut output[output_start..output_end],
            )?;
        }

        Ok(self.frame_buffer_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use crate::{
        pitr::{WalEntry, decode_wal_entry_stream},
        wal::{
            v7::codec::prepare_logical_batch as prepare_v7_logical_batch,
            v8::codec::{
                DecodedWalV8Frame, WalV8FrameKind, WalV8LogicalBatchHeader, decode_frame_structural,
            },
        },
    };

    struct TestPreparationReservation {
        _bytes: u64,
    }

    struct TestAdmissionGuard {
        ticket: u64,
        poisoned: Arc<AtomicBool>,
        armed: bool,
    }

    impl WalV8AdmissionGuard for TestAdmissionGuard {
        fn segment_ticket(&self) -> u64 {
            self.ticket
        }

        fn poison(&mut self) {
            if self.armed {
                self.poisoned.store(true, Ordering::Release);
                self.armed = false;
            }
        }

        fn disarm_after_durable(&mut self) {
            self.armed = false;
        }
    }

    impl Drop for TestAdmissionGuard {
        fn drop(&mut self) {
            if self.armed {
                self.poisoned.store(true, Ordering::Release);
            }
        }
    }

    struct TestPublisher {
        assignment: WalV8BatchAssignment,
        admitted_tickets: parking_lot::Mutex<Vec<u64>>,
        plans: parking_lot::Mutex<Vec<WalV8AdmissionPlan>>,
        poisoned: Arc<AtomicBool>,
    }

    impl TestPublisher {
        fn new(first_data_offset: u64) -> Self {
            Self {
                assignment: assignment(first_data_offset),
                admitted_tickets: parking_lot::Mutex::new(Vec::new()),
                plans: parking_lot::Mutex::new(Vec::new()),
                poisoned: Arc::new(AtomicBool::new(false)),
            }
        }

        fn group_window(&self) -> Vec<u64> {
            self.admitted_tickets.lock().clone()
        }

        fn is_poisoned(&self) -> bool {
            self.poisoned.load(Ordering::Acquire)
        }
    }

    impl WalV8AdmissionPublisher for TestPublisher {
        type Guard = TestAdmissionGuard;

        fn admit_and_publish(
            &self,
            plan: WalV8AdmissionPlan,
        ) -> Result<(WalV8BatchAssignment, Self::Guard)> {
            self.plans.lock().push(plan);
            // This is the group-formation visibility boundary in the test
            // publisher. `admit` cannot return an encoder before this push.
            self.admitted_tickets
                .lock()
                .push(self.assignment.segment_ticket);
            Ok((
                self.assignment,
                TestAdmissionGuard {
                    ticket: self.assignment.segment_ticket,
                    poisoned: Arc::clone(&self.poisoned),
                    armed: true,
                },
            ))
        }
    }

    fn batch(value_len: usize) -> WalBatch {
        WalBatch {
            commit_ts: 17,
            recorded_at: RecordedAt { secs: -2, nanos: 7 },
            entries: vec![
                WalEntry::Put {
                    key: b"duplicate".to_vec(),
                    value: b"superseded".to_vec(),
                },
                WalEntry::PointDelete {
                    key: b"duplicate".to_vec(),
                },
                WalEntry::Put {
                    key: b"k".to_vec(),
                    value: vec![0x5a; value_len],
                },
                WalEntry::RangeDelete {
                    start: b"a".to_vec(),
                    end: b"z".to_vec(),
                },
            ],
        }
    }

    fn assignment(first_data_offset: u64) -> WalV8BatchAssignment {
        WalV8BatchAssignment {
            segment_ticket: 3,
            commit_ts: 17,
            recorded_at: RecordedAt { secs: -2, nanos: 7 },
            first_data_offset,
            segment_incarnation: [0x33; 16],
            physical_header_digest: [0x11; 32],
            initial_prefix_digest: [0x22; 32],
        }
    }

    fn preflight(batch: &WalBatch) -> Result<PreparedWalV8Batch<'_, TestPreparationReservation>> {
        PreparedWalV8Batch::<TestPreparationReservation>::preflight(
            batch,
            crate::pitr::LIVE_WAL_V5_LIMITS,
            |bytes| Ok(TestPreparationReservation { _bytes: bytes }),
        )
    }

    fn prepare_and_encode(batch: &WalBatch) -> Result<EncodedWalV8Batch<TestAdmissionGuard>> {
        let publisher = TestPublisher::new(WAL_V8_DATA_START);
        preflight(batch)?.admit(&publisher)?.encode()
    }

    #[test]
    fn admission_uses_sequencer_metadata_from_after_preflight() -> Result<()> {
        let batch = batch(32);
        let prepared = preflight(&batch)?;
        let mut publisher = TestPublisher::new(WAL_V8_DATA_START);
        let commit_ts = 42;
        let recorded_at = RecordedAt {
            secs: 123,
            nanos: 456,
        };
        publisher.assignment.commit_ts = commit_ts;
        publisher.assignment.recorded_at = recorded_at;

        let mut encoded = prepared
            .admit_with_metadata(commit_ts, recorded_at, &publisher)?
            .encode()?;
        assert_eq!(publisher.group_window(), vec![3]);
        assert_eq!(
            publisher.plans.lock().as_slice(),
            &[WalV8AdmissionPlan {
                commit_ts,
                recorded_at,
                logical_batch_bytes: encoded.as_bytes().len(),
                fragment_count: encoded.fragment_count,
                frame_buffer_bytes: encoded.frame_buffer_bytes,
            }]
        );

        let logical_bytes = encoded.as_bytes();
        let header = codec::WalV8LogicalBatchHeader::decode(
            &logical_bytes[..codec::WAL_V8_LOGICAL_BATCH_HEADER_LEN],
            &logical_bytes[codec::WAL_V8_LOGICAL_BATCH_HEADER_LEN..],
            3,
            u64::try_from(logical_bytes.len())?,
            crate::pitr::LIVE_WAL_V5_LIMITS,
        )?;
        assert_eq!(header.commit_ts, commit_ts);
        assert_eq!(header.recorded_at_secs, recorded_at.secs);
        assert_eq!(header.recorded_at_nanos, recorded_at.nanos);
        encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn invalid_sequencer_metadata_is_rejected_before_publication() -> Result<()> {
        let batch = batch(16);
        let publisher = TestPublisher::new(WAL_V8_DATA_START);
        assert!(
            preflight(&batch)?
                .admit_with_metadata(0, batch.recorded_at, &publisher)
                .is_err()
        );
        assert!(
            preflight(&batch)?
                .admit_with_metadata(
                    batch.commit_ts,
                    RecordedAt {
                        secs: batch.recorded_at.secs,
                        nanos: 1_000_000_000,
                    },
                    &publisher,
                )
                .is_err()
        );
        assert!(publisher.group_window().is_empty());
        assert!(publisher.plans.lock().is_empty());

        Ok(())
    }

    #[test]
    fn canonical_bytes_crc_and_entry_digest_share_one_encoding_pass() -> Result<()> {
        let batch = batch(32);
        let preparation_bytes =
            PreparedWalV8Batch::<TestPreparationReservation>::preparation_buffer_bytes(
                &batch,
                crate::pitr::LIVE_WAL_V5_LIMITS,
            )?;
        let mut reserved_bytes = None;
        let prepared = PreparedWalV8Batch::<TestPreparationReservation>::preflight(
            &batch,
            crate::pitr::LIVE_WAL_V5_LIMITS,
            |bytes| {
                reserved_bytes = Some(bytes);
                Ok(TestPreparationReservation { _bytes: bytes })
            },
        )?;
        assert_eq!(reserved_bytes, Some(preparation_bytes));
        let publisher = TestPublisher::new(WAL_V8_DATA_START);
        let plan_expected = WalV8AdmissionPlan {
            commit_ts: batch.commit_ts,
            recorded_at: batch.recorded_at,
            logical_batch_bytes: prepared.logical_batch_bytes(),
            fragment_count: prepared.fragment_count(),
            frame_buffer_bytes: prepared.frame_buffer_bytes(),
        };
        let admitted = prepared.admit(&publisher)?;
        assert_eq!(publisher.group_window(), vec![3]);
        assert_eq!(publisher.plans.lock().as_slice(), &[plan_expected]);
        let mut encoded = admitted.encode()?;

        let v7 = prepare_v7_logical_batch(&batch, crate::pitr::LIVE_WAL_V5_LIMITS)?
            .encode_entry_stream()?
            .into_logical_batch(3, batch.commit_ts, batch.recorded_at)?;
        let entry_stream = &encoded.as_bytes()[codec::WAL_V8_LOGICAL_BATCH_HEADER_LEN..];
        assert_eq!(entry_stream, &v7.as_bytes()[48..]);

        let header: [u8; codec::WAL_V8_LOGICAL_BATCH_HEADER_LEN] =
            encoded.as_bytes()[..codec::WAL_V8_LOGICAL_BATCH_HEADER_LEN].try_into()?;
        let decoded = WalV8LogicalBatchHeader::decode(
            &header,
            entry_stream,
            3,
            u64::try_from(encoded.as_bytes().len())?,
            crate::pitr::LIVE_WAL_V5_LIMITS,
        )?;
        assert_eq!(
            u32::from_be_bytes(header[36..40].try_into()?),
            crc32fast::hash(entry_stream)
        );
        let mut expected_entry_hasher =
            digest::WalV8EntryHasher::new(u64::try_from(entry_stream.len())?);
        expected_entry_hasher.update(entry_stream)?;
        assert_eq!(encoded.entry_digest(), &expected_entry_hasher.finish()?);
        assert_eq!(
            decode_wal_entry_stream(
                decoded.commit_ts,
                RecordedAt {
                    secs: decoded.recorded_at_secs,
                    nanos: decoded.recorded_at_nanos,
                },
                decoded.entry_count,
                entry_stream,
                crate::pitr::LIVE_WAL_V5_LIMITS,
            )?,
            WalBatch {
                commit_ts: batch.commit_ts,
                recorded_at: batch.recorded_at,
                entries: vec![
                    WalEntry::PointDelete {
                        key: b"duplicate".to_vec(),
                    },
                    WalEntry::Put {
                        key: b"k".to_vec(),
                        value: vec![0x5a; 32],
                    },
                    WalEntry::RangeDelete {
                        start: b"a".to_vec(),
                        end: b"z".to_vec(),
                    },
                ],
            }
        );
        assert_eq!(
            encoded.batch_digest(),
            &digest::batch_digest(&header, encoded.entry_digest())
        );
        encoded.disarm_after_durable();
        assert!(!publisher.is_poisoned());
        Ok(())
    }

    #[test]
    fn digest_is_ready_before_frames_and_frames_round_trip_at_fragment_boundaries() -> Result<()> {
        for value_len in [3945, 3946, 7953, 7954] {
            let mut encoded = prepare_and_encode(&batch(value_len))?;
            let digest_ready = encoded.digest_ready()?;
            assert_eq!(digest_ready.segment_incarnation, [0x33; 16]);
            assert_eq!(digest_ready.segment_ticket, 3);
            assert_eq!(
                digest_ready.logical_batch_bytes,
                u64::try_from(encoded.as_bytes().len())?
            );
            assert_eq!(digest_ready.batch_digest, *encoded.batch_digest());

            let mut frame_output = vec![0xa5; encoded.frame_buffer_bytes() + 19];
            assert_eq!(
                encoded.encode_data_frames_into(&mut frame_output)?,
                encoded.frame_buffer_bytes()
            );
            assert!(
                frame_output[encoded.frame_buffer_bytes()..]
                    .iter()
                    .all(|byte| *byte == 0xa5)
            );
            let (frames, remainder) = frame_output[..encoded.frame_buffer_bytes()]
                .as_chunks::<{ codec::WAL_V8_FRAME_LEN }>();
            assert!(remainder.is_empty());

            let mut logical = Vec::new();
            for (index, frame) in frames.iter().enumerate() {
                let offset =
                    WAL_V8_DATA_START + u64::try_from(index)? * codec::WAL_V8_FRAME_LEN as u64;
                let payload_start = index
                    .checked_mul(codec::WAL_V8_DATA_FRAGMENT_PAYLOAD_CAPACITY)
                    .context("test DATA payload offset overflows")?;
                let payload_end = payload_start
                    .checked_add(codec::WAL_V8_DATA_FRAGMENT_PAYLOAD_CAPACITY)
                    .context("test DATA payload end overflows")?
                    .min(encoded.as_bytes().len());
                let payload = &encoded.as_bytes()[payload_start..payload_end];
                let expected_fragment = codec::WalV8DataFragmentHeader {
                    segment_ticket: encoded.assignment.segment_ticket,
                    fragment_index: u32::try_from(index)?,
                    fragment_count: encoded.fragment_count,
                    batch_bytes: u64::try_from(encoded.as_bytes().len())?,
                };
                let expected_body = expected_fragment.encode_body(payload)?;
                let expected_frame = codec::encode_data_frame(
                    encoded.assignment.physical_header_digest,
                    encoded.assignment.initial_prefix_digest,
                    offset,
                    &expected_body,
                )?;
                assert_eq!(frame, &expected_frame);

                let decoded: DecodedWalV8Frame =
                    decode_frame_structural(frame, offset, &[0x11; 32], &[0x22; 32])?;
                assert_eq!(decoded.kind, WalV8FrameKind::Data);
                let (fragment, payload) =
                    codec::WalV8DataFragmentHeader::decode_body(&decoded.body)?;
                assert_eq!(fragment.segment_ticket, 3);
                assert_eq!(usize::try_from(fragment.fragment_index)?, index);
                assert_eq!(usize::try_from(fragment.fragment_count)?, frames.len());
                assert_eq!(
                    fragment.batch_bytes,
                    u64::try_from(encoded.as_bytes().len())?
                );
                logical.extend_from_slice(payload);
            }
            assert_eq!(logical, encoded.as_bytes());
            encoded.disarm_after_durable();
        }
        Ok(())
    }

    #[test]
    fn invalid_published_assignment_poisons_its_ticket() -> Result<()> {
        let batch = batch(1);
        for first_data_offset in [
            0,
            WAL_V8_HEADER_LEN as u64,
            WAL_V8_DATA_START + 1,
            u64::MAX - 4095,
        ] {
            let publisher = TestPublisher::new(first_data_offset);
            assert!(preflight(&batch)?.admit(&publisher).is_err());
            assert_eq!(publisher.group_window(), vec![3]);
            assert!(publisher.is_poisoned());
        }
        Ok(())
    }

    #[test]
    fn dropping_a_preparing_ticket_trips_its_poison_guard() -> Result<()> {
        let publisher = TestPublisher::new(WAL_V8_DATA_START);
        let batch = batch(1);
        let admitted = preflight(&batch)?.admit(&publisher)?;
        assert_eq!(publisher.group_window(), vec![3]);
        assert!(!publisher.is_poisoned());

        drop(admitted);
        assert!(publisher.is_poisoned());
        Ok(())
    }

    #[test]
    fn frame_preparation_failure_poisons_the_admitted_ticket() -> Result<()> {
        let batch = batch(1);
        let publisher = TestPublisher::new(WAL_V8_DATA_START);
        let mut encoded = preflight(&batch)?.admit(&publisher)?.encode()?;

        assert!(encoded.encode_data_frames_into(&mut []).is_err());
        assert!(publisher.is_poisoned());
        Ok(())
    }
}
