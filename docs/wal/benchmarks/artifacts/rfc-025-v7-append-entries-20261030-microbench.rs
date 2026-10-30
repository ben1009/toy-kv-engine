use super::{CanonicalWalEntryStream, LIVE_WAL_V5_LIMITS, RecordedAt, WalBatch, WalEntry};
use std::{hint::black_box, time::Instant};

#[test]
#[ignore = "manual CPU measurement"]
fn logical_append_cpu_screen() -> anyhow::Result<()> {
    const HEADER: usize = 48;
    for (num_entries, iterations) in [(2, 24_000), (8, 8_000), (64, 2_000)] {
        let batch = WalBatch {
            commit_ts: 23,
            recorded_at: RecordedAt { secs: 1, nanos: 0 },
            entries: (0..num_entries)
                .map(|i| WalEntry::Put {
                    key: format!("key-{i:011}").into_bytes(),
                    value: vec![0x5a; 1024],
                })
                .collect(),
        };
        let stream = CanonicalWalEntryStream::new(&batch, LIVE_WAL_V5_LIMITS)?;
        let len = HEADER + stream.data_len();
        let mut expected = vec![0; len];
        let (old_len, old_crc) = stream.encode_into(&mut expected[HEADER..])?;
        let mut actual = Vec::with_capacity(len);
        actual.resize(HEADER, 0);
        let (new_len, new_crc) = stream.append_into(&mut actual)?;
        assert_eq!((new_len, new_crc), (old_len, old_crc));
        assert_eq!(actual, expected);
        for round in 0..7 {
            for append in [round % 2 == 1, round % 2 == 0] {
                let start = Instant::now();
                for _ in 0..iterations {
                    if append {
                        let mut dst = Vec::with_capacity(len);
                        dst.resize(HEADER, 0);
                        black_box(stream.append_into(&mut dst)?);
                        black_box(&dst);
                    } else {
                        let mut dst = vec![0; len];
                        black_box(stream.encode_into(&mut dst[HEADER..])?);
                        black_box(&dst);
                    }
                }
                println!("entries={num_entries} round={round} mode={} iterations={iterations} ns={}", if append { "append" } else { "zero" }, start.elapsed().as_nanos());
            }
        }
    }
    Ok(())
}
