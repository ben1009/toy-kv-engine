# RFC 025: direct-to-frame CPU screen

**Not retained.** The direct-to-frame prototype improves the isolated
64-entry median by 3.8%, below the requested 10–15% threshold for an SSD
experiment. Production WAL code remains unchanged.

The standalone screen compares three paths using identical unique PUT entries,
1-KiB values, aligned 4-KiB frames, `crc32fast`, and SHA-256:

1. Encode an intermediate logical batch, hash it, and copy it into DATA frames.
2. Encode directly into frame payloads, fill the logical header after the entry
   CRC is known, then hash the payload slices in order.
3. Compute the entry CRC in a preliminary pass, then encode directly into
   frames while updating SHA.

The preliminary pass is necessary for the third path because the logical header
contains the entry-stream CRC and must be hashed before the entry bytes. Each
case uses seven alternating rounds and includes buffer allocation, framing,
frame CRC, and SHA. It excludes canonical-index construction, producer
scheduling, and I/O. The baseline is a standalone reproduction of the relevant
encoding operations, not the production runtime.

| Entries | Intermediate best / median (µs) | Direct, then SHA best / median (µs) | CRC prepass best / median (µs) |
| ---: | ---: | ---: | ---: |
| 2 | 2.901 / 2.904 | 2.849 / 2.857 | 2.902 / 2.907 |
| 8 | 6.220 / 6.230 | 6.119 / 6.132 | 6.331 / 6.343 |
| 64 | 37.861 / 37.873 | 36.297 / 36.437 | 38.380 / 38.414 |

The direct-then-SHA median improves 1.6%, 1.6%, and 3.8% respectively; the
CRC-prepass median is 0.1%, 1.8%, and 1.4% slower. These CPU-only results do
not establish end-to-end behavior or account for the retained producer framing
and SHA/I/O overlap.

An independent test temporarily included the prototype in the v7 codec test
module and compared all three paths with the repository's actual logical batch
and DATA frame encoders. It verified exact frame bytes and rolling SHA for 18
cases: 2, 8, and 64 entries crossed with six value sizes, including fragment
boundaries. The test passed outside the sandbox with nextest run ID
`78437617-925d-436e-9054-9aa33a7dcea6`. It uses unique PUT keys; it does not
qualify duplicate canonicalization, runtime ownership, or crash recovery.

The [prototype](artifacts/rfc-025-v7-direct-frame-cpu-20261030-prototype.rs),
[manifest](artifacts/rfc-025-v7-direct-frame-cpu-20261030-Cargo.toml),
[lockfile](artifacts/rfc-025-v7-direct-frame-cpu-20261030-Cargo.lock),
[results](artifacts/rfc-025-v7-direct-frame-cpu-20261030-results.txt), and
[validation runner](artifacts/rfc-025-v7-direct-frame-cpu-20261030-validate.py)
are preserved. The runner refers to the repository-local scratch path; copy
the prototype there before rerunning it. No SSD or full-suite tests were run.
