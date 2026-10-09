//! WAL format descriptors shared by creation and recovery.
//!
//! This registry records the on-disk and I/O contracts for released formats
//! plus the planned v7 contract. Registering a format does not make its reader
//! or writer available: those capabilities are explicit descriptor fields.
#![allow(dead_code)]

use super::WalIoMode;

pub(crate) const WAL_MVCC_MAGIC: u32 = 0x5741_4C32;
pub(crate) const WAL_FORMAT_VERSION_V2: u16 = 2;
pub(crate) const WAL_FORMAT_VERSION_V3: u16 = 3;
pub(crate) const WAL_FORMAT_VERSION_V4: u16 = 4;
pub(crate) const WAL_FORMAT_VERSION_V7: u16 = 7;
pub(crate) const WAL_HEADER_SIZE: usize = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalFraming {
    LegacyUnframed,
    MvccV2,
    MvccV3,
    MvccV4,
    PitrBatch,
    EmbeddedFrontier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalDigestPolicy {
    None,
    WholeAlignedPrefix,
    LogicalBatches,
    LogicalTicketPrefix,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalRecoveryPolicy {
    LegacyRecordPrefix,
    FirstInvalidBatch,
    PitrStrictPrefix,
    ActiveFrontierFallback,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalIoPolicy {
    Buffered,
    Leader,
    Selectable { default: WalIoMode },
}

impl WalIoPolicy {
    pub(crate) fn resolve(self, requested: WalIoMode) -> WalIoPath {
        match self {
            Self::Buffered => WalIoPath::Buffered,
            Self::Leader => WalIoPath::Leader,
            Self::Selectable { .. } => match requested {
                WalIoMode::Leader => WalIoPath::Leader,
                WalIoMode::Parallel => WalIoPath::Parallel,
            },
        }
    }

    pub(crate) const fn default_path(self) -> WalIoPath {
        match self {
            Self::Buffered => WalIoPath::Buffered,
            Self::Leader => WalIoPath::Leader,
            Self::Selectable {
                default: WalIoMode::Leader,
            } => WalIoPath::Leader,
            Self::Selectable {
                default: WalIoMode::Parallel,
            } => WalIoPath::Parallel,
        }
    }

    pub(crate) const fn supports(self, requested: WalIoMode) -> bool {
        match self {
            Self::Buffered => false,
            Self::Leader => matches!(requested, WalIoMode::Leader),
            Self::Selectable { .. } => {
                matches!(requested, WalIoMode::Leader | WalIoMode::Parallel)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalIoPath {
    Buffered,
    Leader,
    Parallel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalParallelRecovery {
    NotAWriter,
    Covered,
    LeaderOnlyException {
        rfc: &'static str,
        alternative_recovery_covered: bool,
    },
    PendingImplementation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalFormatDescriptor {
    pub(crate) version: Option<u16>,
    pub(crate) framing: WalFraming,
    pub(crate) alignment: usize,
    pub(crate) data_start: usize,
    pub(crate) digest: WalDigestPolicy,
    pub(crate) recovery: WalRecoveryPolicy,
    pub(crate) io: WalIoPolicy,
    pub(crate) parallel_recovery: WalParallelRecovery,
    pub(crate) reader_implemented: bool,
    pub(crate) writer_implemented: bool,
}

const LEGACY: WalFormatDescriptor = WalFormatDescriptor {
    version: None,
    framing: WalFraming::LegacyUnframed,
    alignment: 1,
    data_start: 0,
    digest: WalDigestPolicy::None,
    recovery: WalRecoveryPolicy::LegacyRecordPrefix,
    io: WalIoPolicy::Buffered,
    parallel_recovery: WalParallelRecovery::NotAWriter,
    reader_implemented: true,
    writer_implemented: false,
};

const V2: WalFormatDescriptor = WalFormatDescriptor {
    version: Some(WAL_FORMAT_VERSION_V2),
    framing: WalFraming::MvccV2,
    alignment: 1,
    data_start: WAL_HEADER_SIZE,
    digest: WalDigestPolicy::None,
    recovery: WalRecoveryPolicy::FirstInvalidBatch,
    io: WalIoPolicy::Leader,
    parallel_recovery: WalParallelRecovery::NotAWriter,
    reader_implemented: true,
    writer_implemented: false,
};

const V3: WalFormatDescriptor = WalFormatDescriptor {
    version: Some(WAL_FORMAT_VERSION_V3),
    framing: WalFraming::MvccV3,
    alignment: 1,
    data_start: WAL_HEADER_SIZE,
    digest: WalDigestPolicy::None,
    recovery: WalRecoveryPolicy::FirstInvalidBatch,
    io: WalIoPolicy::Leader,
    parallel_recovery: WalParallelRecovery::NotAWriter,
    reader_implemented: true,
    writer_implemented: false,
};

const V4: WalFormatDescriptor = WalFormatDescriptor {
    version: Some(WAL_FORMAT_VERSION_V4),
    framing: WalFraming::MvccV4,
    alignment: 4096,
    data_start: 4096,
    digest: WalDigestPolicy::None,
    recovery: WalRecoveryPolicy::FirstInvalidBatch,
    io: WalIoPolicy::Selectable {
        default: WalIoMode::Parallel,
    },
    parallel_recovery: WalParallelRecovery::Covered,
    reader_implemented: true,
    writer_implemented: true,
};

const V5: WalFormatDescriptor = WalFormatDescriptor {
    version: Some(crate::pitr::WAL_V5_VERSION_LEGACY),
    framing: WalFraming::PitrBatch,
    alignment: crate::pitr::WAL_V5_ALIGNMENT,
    data_start: crate::pitr::WAL_V5_HEADER_LEN,
    digest: WalDigestPolicy::WholeAlignedPrefix,
    recovery: WalRecoveryPolicy::PitrStrictPrefix,
    io: WalIoPolicy::Leader,
    parallel_recovery: WalParallelRecovery::LeaderOnlyException {
        rfc: "RFC 025 §3: existing PITR v5 keeps Leader I/O",
        alternative_recovery_covered: true,
    },
    reader_implemented: true,
    writer_implemented: true,
};

const V6: WalFormatDescriptor = WalFormatDescriptor {
    version: Some(crate::pitr::WAL_V5_VERSION),
    framing: WalFraming::PitrBatch,
    alignment: crate::pitr::WAL_V5_ALIGNMENT,
    data_start: crate::pitr::WAL_V5_HEADER_LEN,
    digest: WalDigestPolicy::LogicalBatches,
    recovery: WalRecoveryPolicy::PitrStrictPrefix,
    io: WalIoPolicy::Leader,
    parallel_recovery: WalParallelRecovery::LeaderOnlyException {
        rfc: "RFC 025 §3: existing PITR v6 keeps Leader I/O",
        alternative_recovery_covered: true,
    },
    reader_implemented: true,
    writer_implemented: true,
};

const V7: WalFormatDescriptor = WalFormatDescriptor {
    version: Some(WAL_FORMAT_VERSION_V7),
    framing: WalFraming::EmbeddedFrontier,
    alignment: 4096,
    data_start: 8192,
    digest: WalDigestPolicy::LogicalTicketPrefix,
    recovery: WalRecoveryPolicy::ActiveFrontierFallback,
    io: WalIoPolicy::Selectable {
        default: WalIoMode::Parallel,
    },
    parallel_recovery: WalParallelRecovery::PendingImplementation,
    reader_implemented: false,
    writer_implemented: false,
};

impl WalFormatDescriptor {
    pub(crate) const fn legacy() -> &'static Self {
        &LEGACY
    }

    /// Return the registered descriptor for a version, including planned
    /// formats whose reader and writer are not implemented yet.
    pub(crate) const fn for_version(version: u16) -> Option<&'static Self> {
        match version {
            WAL_FORMAT_VERSION_V2 => Some(&V2),
            WAL_FORMAT_VERSION_V3 => Some(&V3),
            WAL_FORMAT_VERSION_V4 => Some(&V4),
            crate::pitr::WAL_V5_VERSION_LEGACY => Some(&V5),
            crate::pitr::WAL_V5_VERSION => Some(&V6),
            WAL_FORMAT_VERSION_V7 => Some(&V7),
            _ => None,
        }
    }

    pub(crate) fn reader(version: u16) -> anyhow::Result<&'static Self> {
        let descriptor = Self::for_version(version)
            .ok_or_else(|| anyhow::anyhow!("unsupported WAL version: got {version}"))?;
        descriptor.validate_version(version)?;
        anyhow::ensure!(
            descriptor.reader_implemented,
            "unsupported WAL version: got {version} (format is registered but recovery is not implemented)"
        );
        Ok(descriptor)
    }

    pub(crate) fn writer(version: u16) -> anyhow::Result<&'static Self> {
        let descriptor = Self::for_version(version)
            .ok_or_else(|| anyhow::anyhow!("unsupported WAL version: got {version}"))?;
        descriptor.validate_version(version)?;
        descriptor.validate_writer_contract(version)?;
        Ok(descriptor)
    }

    fn validate_version(self, version: u16) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.version == Some(version),
            "WAL format registry mismatch for version {version}: descriptor declares {:?}",
            self.version
        );
        Ok(())
    }

    fn validate_writer_contract(self, version: u16) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.reader_implemented,
            "WAL writer version {version} cannot be enabled without an implemented reader"
        );
        anyhow::ensure!(
            self.writer_implemented,
            "unsupported WAL version: got {version} (format is registered but writing is not implemented)"
        );
        match self.parallel_recovery {
            WalParallelRecovery::Covered => anyhow::ensure!(
                matches!(
                    self.io,
                    WalIoPolicy::Selectable {
                        default: WalIoMode::Parallel
                    }
                ),
                "WAL writer version {version} claims Parallel recovery coverage without defaulting to Parallel I/O"
            ),
            WalParallelRecovery::LeaderOnlyException {
                rfc,
                alternative_recovery_covered: true,
            } => {
                anyhow::ensure!(
                    !rfc.trim().is_empty(),
                    "WAL version {version} Leader exception has no RFC reference"
                );
                anyhow::ensure!(
                    matches!(self.io, WalIoPolicy::Leader),
                    "WAL version {version} exception in {rfc} does not select Leader I/O"
                );
            }
            WalParallelRecovery::NotAWriter
            | WalParallelRecovery::LeaderOnlyException { .. }
            | WalParallelRecovery::PendingImplementation => {
                anyhow::bail!(
                    "WAL writer version {version} lacks Parallel recovery coverage or a covered Leader exception"
                );
            }
        }
        Ok(())
    }

    pub(crate) fn is_v4(self) -> bool {
        self.framing == WalFraming::MvccV4
    }

    pub(crate) fn is_pitr(self) -> bool {
        matches!(
            self.framing,
            WalFraming::PitrBatch | WalFraming::EmbeddedFrontier
        )
    }

    pub(crate) fn is_v5_family(self) -> bool {
        self.framing == WalFraming::PitrBatch
    }

    pub(crate) fn pitr_digest_rule(self) -> Option<crate::pitr::WalDigestRule> {
        match self.digest {
            WalDigestPolicy::WholeAlignedPrefix => {
                Some(crate::pitr::WalDigestRule::WholeAlignedPrefix)
            }
            WalDigestPolicy::LogicalBatches => Some(crate::pitr::WalDigestRule::LogicalBatches),
            WalDigestPolicy::None | WalDigestPolicy::LogicalTicketPrefix => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptors_preserve_existing_format_contracts() {
        let expected = [
            (2, WalFraming::MvccV2, 1, WAL_HEADER_SIZE, WalIoPath::Leader),
            (3, WalFraming::MvccV3, 1, WAL_HEADER_SIZE, WalIoPath::Leader),
            (4, WalFraming::MvccV4, 4096, 4096, WalIoPath::Parallel),
            (5, WalFraming::PitrBatch, 4096, 4096, WalIoPath::Leader),
            (6, WalFraming::PitrBatch, 4096, 4096, WalIoPath::Leader),
        ];

        for (version, framing, alignment, data_start, default_path) in expected {
            let descriptor = WalFormatDescriptor::reader(version).unwrap();
            assert_eq!(descriptor.version, Some(version));
            assert_eq!(descriptor.framing, framing);
            assert_eq!(descriptor.alignment, alignment);
            assert_eq!(descriptor.data_start, data_start);
            assert_eq!(descriptor.io.default_path(), default_path);
        }

        assert_eq!(WalFormatDescriptor::legacy().io, WalIoPolicy::Buffered);
        assert_eq!(
            WalFormatDescriptor::writer(4).unwrap().parallel_recovery,
            WalParallelRecovery::Covered
        );
        for version in [5, 6] {
            assert!(matches!(
                WalFormatDescriptor::writer(version)
                    .unwrap()
                    .parallel_recovery,
                WalParallelRecovery::LeaderOnlyException {
                    alternative_recovery_covered: true,
                    ..
                }
            ));
        }
        assert_eq!(
            WalFormatDescriptor::reader(4)
                .unwrap()
                .io
                .resolve(WalIoMode::Leader),
            WalIoPath::Leader
        );
        assert!(WalFormatDescriptor::reader(7).is_err());
        assert!(WalFormatDescriptor::for_version(99).is_none());

        let leader_default = WalFormatDescriptor {
            version: Some(8),
            io: WalIoPolicy::Selectable {
                default: WalIoMode::Leader,
            },
            ..V4
        };
        assert!(leader_default.validate_writer_contract(8).is_err());

        let unreadable_writer = WalFormatDescriptor {
            version: Some(8),
            reader_implemented: false,
            writer_implemented: true,
            ..V4
        };
        assert!(unreadable_writer.validate_writer_contract(8).is_err());
        assert!(leader_default.validate_version(8).is_ok());
        assert!(leader_default.validate_version(4).is_err());
    }

    #[test]
    fn v7_contract_is_registered_without_enabling_production_io() {
        let descriptor = WalFormatDescriptor::for_version(WAL_FORMAT_VERSION_V7).unwrap();
        assert_eq!(descriptor.framing, WalFraming::EmbeddedFrontier);
        assert_eq!(descriptor.data_start, 8192);
        assert_eq!(descriptor.digest, WalDigestPolicy::LogicalTicketPrefix);
        assert_eq!(
            descriptor.recovery,
            WalRecoveryPolicy::ActiveFrontierFallback
        );
        assert_eq!(descriptor.io.default_path(), WalIoPath::Parallel);
        assert!(!descriptor.reader_implemented);
        assert!(!descriptor.writer_implemented);
        assert!(descriptor.io.supports(WalIoMode::Leader));
        assert!(descriptor.io.supports(WalIoMode::Parallel));
        assert!(WalFormatDescriptor::writer(WAL_FORMAT_VERSION_V7).is_err());
    }
}
