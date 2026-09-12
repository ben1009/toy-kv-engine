//! Dormant PITR enable-transition harness over manifest v7 state.
#![allow(dead_code)]

use anyhow::{Result, ensure};

use crate::pitr_manifest::{
    PersistedPitrConfig, PitrManifestRecord, PitrMode, PitrState, replay_pitr_records,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PitrEnableRequest {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: [u8; 16],
    pub(crate) archive_epoch_id: [u8; 16],
    pub(crate) config: PersistedPitrConfig,
}

#[derive(Debug, Default)]
pub(crate) struct PitrEnableCoordinator {
    state: PitrState,
    records: Vec<PitrManifestRecord>,
}

impl PitrEnableCoordinator {
    pub(crate) fn recover(records: Vec<PitrManifestRecord>) -> Result<Self> {
        let state = replay_pitr_records(records.clone())?;
        Ok(Self { state, records })
    }

    pub(crate) fn begin_enable(&mut self, request: PitrEnableRequest) -> Result<()> {
        ensure!(
            self.state.mode == PitrMode::Disabled,
            "PITR enable overlaps existing state"
        );
        ensure!(
            request.repository_id != [0; 16],
            "PITR repository identity is empty"
        );
        ensure!(
            request.timeline_id != [0; 16],
            "PITR timeline identity is empty"
        );
        ensure!(
            request.archive_epoch_id != [0; 16],
            "PITR archive epoch identity is empty"
        );
        request.config.validate_for_enable()?;
        let record = PitrManifestRecord::EnableIntent {
            repository_id: request.repository_id,
            timeline_id: request.timeline_id,
            archive_epoch_id: request.archive_epoch_id,
            config: request.config,
        };
        self.state = replay_pitr_records([record.clone()])?;
        self.records.push(record);
        Ok(())
    }

    pub(crate) fn complete_enable(&mut self, active_segment_id: u64) -> Result<()> {
        ensure!(
            self.state.mode == PitrMode::Enabling,
            "PITR enable completion has no intent"
        );
        let record = PitrManifestRecord::EnableComplete { active_segment_id };
        let mut records = self.records.clone();
        records.push(record.clone());
        self.state = replay_pitr_records(records.clone())?;
        self.records = records;
        Ok(())
    }

    pub(crate) fn state(&self) -> &PitrState {
        &self.state
    }

    pub(crate) fn records(&self) -> &[PitrManifestRecord] {
        &self.records
    }
}

trait EnableConfigValidation {
    fn validate_for_enable(&self) -> Result<()>;
}

impl EnableConfigValidation for PersistedPitrConfig {
    fn validate_for_enable(&self) -> Result<()> {
        ensure!(
            self.archive_interval_ms > 0,
            "PITR archive interval must be nonzero"
        );
        ensure!(
            self.max_segment_bytes > 0,
            "PITR segment limit must be nonzero"
        );
        ensure!(
            self.max_unarchived_bytes >= self.max_segment_bytes,
            "PITR unarchived limit is below segment limit"
        );
        ensure!(
            self.max_source_spool_bytes >= self.max_unarchived_bytes,
            "PITR source spool limit is below unarchived limit"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> PitrEnableRequest {
        PitrEnableRequest {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            config: PersistedPitrConfig {
                archive_interval_ms: 1000,
                max_segment_bytes: 8192,
                max_unarchived_bytes: 16384,
                max_source_spool_bytes: 32768,
            },
        }
    }

    #[test]
    fn enable_intent_and_completion_recover_deterministically() {
        let mut coordinator = PitrEnableCoordinator::default();
        coordinator.begin_enable(request()).unwrap();
        assert_eq!(coordinator.state().mode, PitrMode::Enabling);
        coordinator.complete_enable(7).unwrap();
        assert_eq!(coordinator.state().mode, PitrMode::Enabled);
        let recovered = PitrEnableCoordinator::recover(coordinator.records().to_vec()).unwrap();
        assert_eq!(recovered.state(), coordinator.state());
    }

    #[test]
    fn enable_rejects_invalid_or_overlapping_requests() {
        let mut coordinator = PitrEnableCoordinator::default();
        let mut invalid = request();
        invalid.config.max_unarchived_bytes = 4096;
        assert!(coordinator.begin_enable(invalid).is_err());
        coordinator.begin_enable(request()).unwrap();
        assert!(coordinator.begin_enable(request()).is_err());
        coordinator.complete_enable(8).unwrap();
        assert!(coordinator.complete_enable(7).is_err());
    }
}
