    #[test]
    #[ignore = "manual CPU attribution probe"]
    fn benchmark_borrowed_input_cpu() -> Result<()> {
        use std::{hint::black_box, time::Instant};
        let entries: Vec<_> = (0..64).map(|i| (
            Bytes::from(format!("key{i:08}")),
            Bytes::from(vec![1; 1025]),
            BatchEntryKind::PutPrefixed,
        )).collect();
        for round in 0..6 {
            for borrowed in if round % 2 == 0 { [false, true] } else { [true, false] } {
                let start = Instant::now();
                for _ in 0..3000 {
                    let publisher = TestPublisher::new(WAL_V8_DATA_START);
                    let mut encoded = if borrowed {
                        PreparedWalV8Batch::preflight_points(&entries, crate::pitr::LIVE_WAL_V5_LIMITS, |_| Ok(()))?
                            .admit_with_metadata(17, RecordedAt { secs: -2, nanos: 7 }, &publisher)?.encode()?
                    } else {
                        let batch = WalBatch {
                            commit_ts: 17, recorded_at: RecordedAt { secs: -2, nanos: 7 },
                            entries: entries.iter().map(|(key,value,_)| WalEntry::Put {
                                key: key.to_vec(), value: value.to_vec(),
                            }).collect(),
                        };
                        PreparedWalV8Batch::preflight(&batch, crate::pitr::LIVE_WAL_V5_LIMITS, |_| Ok(()))?
                            .admit(&publisher)?.encode()?
                    };
                    let mut frames = vec![0; encoded.frame_buffer_bytes];
                    encoded.encode_data_frames_into(&mut frames)?;
                    black_box((frames, encoded));
                }
                println!("round={round} borrowed={borrowed} us_per_batch={:.3}", start.elapsed().as_secs_f64()*1e6/3000.0);
            }
        }
        Ok(())
    }
