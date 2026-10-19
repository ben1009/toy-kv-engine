//! Capacity decisions for migrating a legacy PITR epoch to v7.
//!
//! This module estimates the transition's source-spool requirement, including
//! the next v7 seal/rotation headroom. The caller must provide the reconstructed
//! allocation charge for every existing obligation, pin, orphan, and staging
//! file, plus the recovered legacy Active WAL image length. The capacity result
//! can be atomically acquired from the live v7 resource ledger as an owned
//! reservation before publishing durable intent.

use anyhow::{Result, ensure};
use std::{error::Error, fmt};

use crate::{
    pitr::{WAL_V5_HEADER_LEN, manifest::PersistedPitrConfig},
    wal::v7::{
        admission::{
            WalV7Reservation, WalV7ReservationError, WalV7ResourceCharge, WalV7ResourceLedger,
        },
        codec::{WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN},
    },
};

// The image needs the header, generation-zero frontier, one DATA frame, and a
// terminal frontier before it can acknowledge its first batch.
const MINIMUM_NONEMPTY_V7_SEGMENT_BYTES: u64 = (WAL_V7_HEADER_LEN + 3 * WAL_V7_FRAME_LEN) as u64;
const MINIMUM_EMPTY_V7_IMAGE_BYTES: u64 = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64;
// Bound the complete SealStarted, SegmentSealed, and NewPitrMemtable JSON
// sequence independently of filesystem allocation size. The encoding-bound
// regression below exercises all fields at their maximum serialized widths.
const V7_TRANSITION_MANIFEST_BYTES: u64 = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum V7MigrationDeferralReason {
    SegmentLimitTooSmall,
    SegmentLimitTooLarge,
    InsufficientSourceSpool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum V7MigrationCapacityDecision {
    Eligible,
    Deferred(V7MigrationDeferralReason),
}

/// Checked source-spool estimate for one legacy-to-v7 transition.
///
/// `required_transition_bytes` includes a maximum-size v7 active image, its
/// fresh-inode recovery workspace, worst-case legacy and v7 seal sidecars, an
/// empty v7 successor image and its atomic-install staging extent, and terminal
/// manifest headroom. File charges are rounded to the filesystem allocation
/// unit. `available_capacity_bytes` is the configured source-spool limit after
/// subtracting all currently retained source allocations supplied by the caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct V7MigrationCapacity {
    pub(crate) decision: V7MigrationCapacityDecision,
    pub(crate) source_spool_limit_bytes: u64,
    pub(crate) retained_source_spool_bytes: u64,
    pub(crate) available_capacity_bytes: u64,
    pub(crate) required_transition_bytes: u64,
    pub(crate) required_total_bytes: u64,
    pub(crate) configured_segment_limit_bytes: u64,
    pub(crate) legacy_active_image_bytes: u64,
    pub(crate) v7_active_image_bytes: u64,
    pub(crate) recovery_workspace_bytes: u64,
    pub(crate) legacy_seal_sidecar_bytes: u64,
    pub(crate) v7_seal_sidecar_bytes: u64,
    pub(crate) successor_image_bytes: u64,
    pub(crate) successor_staging_bytes: u64,
    pub(crate) terminal_manifest_headroom_bytes: u64,
}

/// Failure to acquire the whole migration charge from the live resource ledger.
#[derive(Debug)]
pub(crate) enum V7MigrationReservationError {
    Deferred(V7MigrationDeferralReason),
    InvalidAssessment,
    Ledger(WalV7ReservationError),
}

impl fmt::Display for V7MigrationReservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deferred(reason) => {
                write!(formatter, "PITR v7 migration is deferred: {reason:?}")
            }
            Self::InvalidAssessment => {
                formatter.write_str("invalid PITR v7 migration capacity assessment")
            }
            Self::Ledger(error) => write!(
                formatter,
                "could not reserve PITR v7 migration capacity: {error}"
            ),
        }
    }
}

impl Error for V7MigrationReservationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Ledger(error) => Some(error),
            Self::Deferred(_) | Self::InvalidAssessment => None,
        }
    }
}

/// Owns the complete migration headroom charge until the transition is safe to
/// release. Keep this guard with the migration lifecycle after durable intent;
/// dropping it releases both source-spool and recovery-workspace capacity.
#[must_use = "retain the migration reservation through successor installation and cleanup"]
#[derive(Debug)]
pub(crate) struct V7MigrationTransitionReservation {
    _reservation: WalV7Reservation,
}

/// Atomically reserve the capacity represented by a successful preflight.
///
/// The assessment is a snapshot and may become stale while other work reserves
/// resources. Charging its complete source-spool and recovery-workspace amounts
/// in one ledger operation closes that race before a caller publishes durable
/// migration intent. The returned owned guard must remain alive until the
/// transition has installed its successor, published the manifest handoff, and
/// completed required durable cleanup.
pub(crate) fn reserve_v7_migration_transition(
    capacity: V7MigrationCapacity,
    ledger: &WalV7ResourceLedger,
) -> std::result::Result<V7MigrationTransitionReservation, V7MigrationReservationError> {
    if let V7MigrationCapacityDecision::Deferred(reason) = capacity.decision {
        return Err(V7MigrationReservationError::Deferred(reason));
    }

    let Some(source_spool_bytes) = capacity
        .required_transition_bytes
        .checked_sub(capacity.recovery_workspace_bytes)
    else {
        return Err(V7MigrationReservationError::InvalidAssessment);
    };
    let Some(required_total_bytes) = capacity
        .retained_source_spool_bytes
        .checked_add(capacity.required_transition_bytes)
    else {
        return Err(V7MigrationReservationError::InvalidAssessment);
    };
    if source_spool_bytes == 0
        || required_total_bytes != capacity.required_total_bytes
        || capacity.available_capacity_bytes
            != capacity
                .source_spool_limit_bytes
                .saturating_sub(capacity.retained_source_spool_bytes)
        || capacity.required_transition_bytes > capacity.available_capacity_bytes
    {
        return Err(V7MigrationReservationError::InvalidAssessment);
    }

    let reservation = ledger
        .reserve(WalV7ResourceCharge {
            source_spool_bytes,
            recovery_workspace_bytes: capacity.recovery_workspace_bytes,
            ..WalV7ResourceCharge::default()
        })
        .map_err(V7MigrationReservationError::Ledger)?;

    Ok(V7MigrationTransitionReservation {
        _reservation: reservation,
    })
}

/// Decide whether a migration can reserve every source-spool resource it needs.
///
/// The calculation deliberately retains the current files in the total: the
/// old active WAL and its existing obligations remain live while the v7 image
/// and recovery workspace are prepared. A later atomic ledger reservation must
/// consume this exact transition charge through [`reserve_v7_migration_transition`]
/// before a Sealing intent is durable.
///
/// `legacy_active_image_bytes` is the recovered v5/v6 WAL's validated image
/// length, independently of its filesystem allocation charge. Legacy admission
/// can exceed the configured segment limit after reopen, so that configuration
/// cannot bound the predecessor's seal index. The caller must keep admission
/// stopped while fixing this image length and acquiring the reservation.
pub(crate) fn assess_v7_migration_capacity(
    config: &PersistedPitrConfig,
    legacy_active_image_bytes: u64,
    retained_source_spool_bytes: u64,
    filesystem_allocation_unit: u64,
) -> Result<V7MigrationCapacity> {
    ensure!(
        filesystem_allocation_unit > 0,
        "PITR filesystem allocation unit is zero"
    );
    ensure!(
        config.max_source_spool_bytes >= config.max_segment_bytes,
        "PITR source-spool limit is below the segment limit"
    );
    ensure!(
        (WAL_V5_HEADER_LEN as u64..=crate::wal::MAX_WAL_FILE_SIZE)
            .contains(&legacy_active_image_bytes),
        "PITR legacy Active WAL image is outside its format bounds"
    );

    // The configured v5 limit may exceed the global WAL file cap. Preserve
    // that as a migration deferral, while still reporting the source-spool
    // requirement at the largest image this binary can represent.
    let image_limit_bytes = config.max_segment_bytes.min(crate::wal::MAX_WAL_FILE_SIZE);
    let v7_active_image_bytes =
        round_up_to_allocation_unit(image_limit_bytes, filesystem_allocation_unit)?;
    let recovery_workspace_bytes =
        round_up_to_allocation_unit(image_limit_bytes, filesystem_allocation_unit)?;
    let legacy_seal_logical_bytes =
        crate::pitr::seal::V5Seal::max_encoded_len_for_segment(legacy_active_image_bytes)?;
    let legacy_seal_sidecar_bytes =
        round_up_to_allocation_unit(legacy_seal_logical_bytes, filesystem_allocation_unit)?;
    let v7_seal_logical_bytes = u64::try_from(crate::pitr::seal::V7Seal::max_encoded_len(
        image_limit_bytes.max(MINIMUM_EMPTY_V7_IMAGE_BYTES),
    )?)?;
    let v7_seal_sidecar_bytes =
        round_up_to_allocation_unit(v7_seal_logical_bytes, filesystem_allocation_unit)?;
    let successor_image_bytes =
        round_up_to_allocation_unit(MINIMUM_EMPTY_V7_IMAGE_BYTES, filesystem_allocation_unit)?;
    // The live successor installer owns persistent and staging reservations
    // concurrently; the shared recovery workspace cannot replace either one.
    let successor_staging_bytes = successor_image_bytes;
    let terminal_manifest_headroom_bytes =
        round_up_to_allocation_unit(V7_TRANSITION_MANIFEST_BYTES, filesystem_allocation_unit)?;
    let required_transition_bytes = v7_active_image_bytes
        .checked_add(recovery_workspace_bytes)
        .and_then(|bytes| bytes.checked_add(legacy_seal_sidecar_bytes))
        .and_then(|bytes| bytes.checked_add(v7_seal_sidecar_bytes))
        .and_then(|bytes| bytes.checked_add(successor_image_bytes))
        .and_then(|bytes| bytes.checked_add(successor_staging_bytes))
        .and_then(|bytes| bytes.checked_add(terminal_manifest_headroom_bytes))
        .ok_or_else(|| anyhow::anyhow!("PITR v7 migration capacity requirement overflows"))?;
    let required_total_bytes = retained_source_spool_bytes
        .checked_add(required_transition_bytes)
        .ok_or_else(|| anyhow::anyhow!("PITR v7 migration total capacity overflows"))?;
    let available_capacity_bytes = config
        .max_source_spool_bytes
        .saturating_sub(retained_source_spool_bytes);

    let decision = if config.max_segment_bytes < MINIMUM_NONEMPTY_V7_SEGMENT_BYTES {
        V7MigrationCapacityDecision::Deferred(V7MigrationDeferralReason::SegmentLimitTooSmall)
    } else if config.max_segment_bytes > crate::wal::MAX_WAL_FILE_SIZE {
        V7MigrationCapacityDecision::Deferred(V7MigrationDeferralReason::SegmentLimitTooLarge)
    } else if required_transition_bytes > available_capacity_bytes {
        V7MigrationCapacityDecision::Deferred(V7MigrationDeferralReason::InsufficientSourceSpool)
    } else {
        V7MigrationCapacityDecision::Eligible
    };

    Ok(V7MigrationCapacity {
        decision,
        source_spool_limit_bytes: config.max_source_spool_bytes,
        retained_source_spool_bytes,
        available_capacity_bytes,
        required_transition_bytes,
        required_total_bytes,
        configured_segment_limit_bytes: config.max_segment_bytes,
        legacy_active_image_bytes,
        v7_active_image_bytes,
        recovery_workspace_bytes,
        legacy_seal_sidecar_bytes,
        v7_seal_sidecar_bytes,
        successor_image_bytes,
        successor_staging_bytes,
        terminal_manifest_headroom_bytes,
    })
}

fn round_up_to_allocation_unit(bytes: u64, allocation_unit: u64) -> Result<u64> {
    let remainder = bytes % allocation_unit;
    if remainder == 0 {
        return Ok(bytes);
    }

    bytes
        .checked_add(allocation_unit - remainder)
        .ok_or_else(|| anyhow::anyhow!("PITR sidecar allocation reservation overflows"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(max_segment_bytes: u64, max_source_spool_bytes: u64) -> PersistedPitrConfig {
        PersistedPitrConfig {
            archive_interval_ms: 1,
            max_segment_bytes,
            max_unarchived_bytes: max_segment_bytes,
            max_source_spool_bytes,
        }
    }

    #[test]
    fn migration_capacity_defers_when_transition_does_not_fit() {
        let segment_bytes = 1_u64 << 30;
        let spool_limit = segment_bytes + segment_bytes * 3 / 10;
        let capacity = assess_v7_migration_capacity(
            &config(segment_bytes, spool_limit),
            segment_bytes,
            0,
            4096,
        )
        .unwrap();

        assert_eq!(
            capacity.decision,
            V7MigrationCapacityDecision::Deferred(
                V7MigrationDeferralReason::InsufficientSourceSpool
            )
        );
        assert!(capacity.required_transition_bytes > capacity.available_capacity_bytes);
        assert_eq!(
            capacity.required_total_bytes,
            capacity.required_transition_bytes
        );
        assert_eq!(capacity.legacy_seal_sidecar_bytes % 4096, 0);
    }

    #[test]
    fn migration_capacity_accounts_for_retained_files_before_transition() {
        let segment_bytes = (1 << 20) + 1;
        let without_retained =
            assess_v7_migration_capacity(&config(segment_bytes, 4 << 20), 1 << 20, 0, 4096)
                .unwrap();
        let with_retained =
            assess_v7_migration_capacity(&config(segment_bytes, 4 << 20), 1 << 20, 2 << 20, 4096)
                .unwrap();

        assert_eq!(
            without_retained.decision,
            V7MigrationCapacityDecision::Eligible
        );
        assert!(without_retained.v7_active_image_bytes > segment_bytes);
        assert_eq!(without_retained.v7_active_image_bytes % 4096, 0);
        assert_eq!(without_retained.recovery_workspace_bytes % 4096, 0);
        assert_eq!(
            with_retained.decision,
            V7MigrationCapacityDecision::Deferred(
                V7MigrationDeferralReason::InsufficientSourceSpool
            )
        );
        assert_eq!(
            with_retained.required_total_bytes,
            (2 << 20) + with_retained.required_transition_bytes
        );
        assert_eq!(
            with_retained.available_capacity_bytes,
            (4 << 20) - (2 << 20)
        );
    }

    #[test]
    fn migration_capacity_reports_unusable_segment_limits_as_deferrals() {
        let too_small_limits = [2 * WAL_V7_FRAME_LEN, 3 * WAL_V7_FRAME_LEN];
        for segment_bytes in too_small_limits {
            let too_small = assess_v7_migration_capacity(
                &config(segment_bytes as u64, 64 * 1024),
                WAL_V5_HEADER_LEN as u64,
                0,
                4096,
            )
            .unwrap();
            assert_eq!(
                too_small.decision,
                V7MigrationCapacityDecision::Deferred(
                    V7MigrationDeferralReason::SegmentLimitTooSmall
                ),
                "segment limit {segment_bytes} should defer migration"
            );
        }

        let minimum_nonempty = assess_v7_migration_capacity(
            &config(MINIMUM_NONEMPTY_V7_SEGMENT_BYTES, 64 * 1024),
            WAL_V5_HEADER_LEN as u64,
            0,
            4096,
        )
        .unwrap();
        let too_large = assess_v7_migration_capacity(
            &config(crate::wal::MAX_WAL_FILE_SIZE + 1, u64::MAX),
            WAL_V5_HEADER_LEN as u64,
            0,
            4096,
        )
        .unwrap();

        assert_eq!(
            minimum_nonempty.decision,
            V7MigrationCapacityDecision::Eligible
        );
        assert_eq!(
            too_large.decision,
            V7MigrationCapacityDecision::Deferred(V7MigrationDeferralReason::SegmentLimitTooLarge)
        );
    }

    #[test]
    fn migration_capacity_reserves_v7_rotation_and_publication_headroom() {
        let segment_bytes = MINIMUM_NONEMPTY_V7_SEGMENT_BYTES;
        let retained_bytes = segment_bytes;
        let insufficient = assess_v7_migration_capacity(
            &config(segment_bytes, 68 * 1024),
            segment_bytes,
            retained_bytes,
            4096,
        )
        .unwrap();

        assert_eq!(
            insufficient.decision,
            V7MigrationCapacityDecision::Deferred(
                V7MigrationDeferralReason::InsufficientSourceSpool
            )
        );
        assert_eq!(insufficient.v7_seal_sidecar_bytes, 4096);
        assert_eq!(insufficient.successor_image_bytes, 8192);
        assert_eq!(insufficient.successor_staging_bytes, 8192);
        assert_eq!(insufficient.terminal_manifest_headroom_bytes, 4096);
        assert_eq!(insufficient.required_total_bytes, 76 * 1024);

        let exactly_sufficient = assess_v7_migration_capacity(
            &config(segment_bytes, insufficient.required_total_bytes),
            segment_bytes,
            retained_bytes,
            4096,
        )
        .unwrap();
        assert_eq!(
            exactly_sufficient.decision,
            V7MigrationCapacityDecision::Eligible
        );
    }

    #[test]
    fn migration_capacity_covers_legacy_seal_past_configured_segment_limit() {
        // 161 one-frame batches fit the configured legacy segment. After
        // reopen, the legacy first-batch exception can admit batch 162 before
        // rotation, crossing the seal's 4 KiB allocation boundary.
        let segment_bytes = 162 * 4096;
        let legacy_image_bytes = 163 * 4096;
        let mut insufficient_config = config(segment_bytes, 495 * 4096);
        insufficient_config.max_unarchived_bytes = legacy_image_bytes;
        let insufficient = assess_v7_migration_capacity(
            &insufficient_config,
            legacy_image_bytes,
            legacy_image_bytes,
            4096,
        )
        .unwrap();

        assert_eq!(insufficient.legacy_active_image_bytes, legacy_image_bytes);
        assert_eq!(insufficient.legacy_seal_sidecar_bytes, 8192);
        assert_eq!(insufficient.v7_active_image_bytes, segment_bytes);
        assert_eq!(insufficient.recovery_workspace_bytes, segment_bytes);
        assert_eq!(insufficient.required_total_bytes, 496 * 4096);
        assert_eq!(
            insufficient.decision,
            V7MigrationCapacityDecision::Deferred(
                V7MigrationDeferralReason::InsufficientSourceSpool
            )
        );

        insufficient_config.max_source_spool_bytes = insufficient.required_total_bytes;
        let exactly_sufficient = assess_v7_migration_capacity(
            &insufficient_config,
            legacy_image_bytes,
            legacy_image_bytes,
            4096,
        )
        .unwrap();
        assert_eq!(
            exactly_sufficient.decision,
            V7MigrationCapacityDecision::Eligible
        );
    }

    #[test]
    fn migration_capacity_manifest_headroom_covers_encoded_transition_on_small_units() {
        use crate::{
            manifest::ManifestRecord,
            pitr::manifest::{
                ActiveBoundary, ImmutableBoundary, PersistedChainAnchor, PersistedCommitAnchor,
                PersistedRecordedAt, PitrManifestRecord,
            },
        };

        // These need only serialize: maximal numeric widths and byte arrays
        // conservatively cover every valid transition's smaller coordinates.
        let active = ActiveBoundary {
            timeline_id: [u8::MAX; 16],
            archive_epoch_id: [u8::MAX; 16],
            segment_id: u64::MAX,
            incarnation: [u8::MAX; 16],
            ticket_end: u64::MAX,
            durable_end: u64::MAX,
            last_commit_ts: u64::MAX,
            prefix_digest: [u8::MAX; 32],
        };
        let recorded_at = PersistedRecordedAt {
            secs: i64::MIN,
            nanos: u32::MAX,
        };
        let records = [
            ManifestRecord::Pitr(PitrManifestRecord::SealStarted {
                segment_id: u64::MAX,
                successor_segment_id: u64::MAX,
                logical_length: u64::MAX,
                immutable_boundary: Some(ImmutableBoundary {
                    active,
                    sealed_end: u64::MAX,
                    terminal_frontier_digest: [u8::MAX; 32],
                    wal_digest: [u8::MAX; 32],
                }),
            }),
            ManifestRecord::Pitr(PitrManifestRecord::SegmentSealed {
                segment_id: u64::MAX,
                segment_anchor: PersistedChainAnchor::Segment {
                    segment_id: u64::MAX,
                    wal_digest: [u8::MAX; 32],
                    seal_digest: [u8::MAX; 32],
                },
                last_recorded_at: Some(recorded_at),
                last_commit_anchor: Some(PersistedCommitAnchor {
                    segment_id: u64::MAX,
                    commit_ts: u64::MAX,
                    recorded_at,
                    entry_digest: [u8::MAX; 32],
                }),
                successor_active_boundary: Some(active),
            }),
            ManifestRecord::NewPitrMemtable {
                id: usize::MAX,
                segment_id: u64::MAX,
            },
        ];
        let mut encoded = Vec::new();
        for record in &records {
            serde_json::to_writer(&mut encoded, record).unwrap();
        }
        let encoded_bytes = u64::try_from(encoded.len()).unwrap();
        assert!(encoded_bytes > 1024);
        assert!(encoded_bytes <= V7_TRANSITION_MANIFEST_BYTES);

        for allocation_unit in [512, 1024, 4096, 8192] {
            let capacity = assess_v7_migration_capacity(
                &config(MINIMUM_NONEMPTY_V7_SEGMENT_BYTES, 128 * 1024),
                WAL_V5_HEADER_LEN as u64,
                0,
                allocation_unit,
            )
            .unwrap();
            assert!(capacity.terminal_manifest_headroom_bytes >= encoded_bytes);
            assert!(
                capacity
                    .terminal_manifest_headroom_bytes
                    .is_multiple_of(allocation_unit)
            );
            assert_eq!(capacity.decision, V7MigrationCapacityDecision::Eligible);
        }
    }

    #[test]
    fn migration_capacity_rejects_invalid_inputs_and_overflow() {
        assert!(
            assess_v7_migration_capacity(
                &config(1 << 20, u64::MAX),
                WAL_V5_HEADER_LEN as u64,
                u64::MAX,
                4096,
            )
            .is_err()
        );
        assert!(
            assess_v7_migration_capacity(
                &config(1 << 20, 4 << 20),
                WAL_V5_HEADER_LEN as u64,
                0,
                0,
            )
            .is_err()
        );
        for invalid_legacy_bytes in [
            WAL_V5_HEADER_LEN as u64 - 1,
            crate::wal::MAX_WAL_FILE_SIZE + 1,
        ] {
            assert!(
                assess_v7_migration_capacity(
                    &config(1 << 20, 4 << 20),
                    invalid_legacy_bytes,
                    0,
                    4096,
                )
                .is_err()
            );
        }
    }
}
