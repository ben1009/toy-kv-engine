# RFC 025: fused scalar copy and CRC screen

**Not retained.** A scalar prototype loads each payload byte once, copies it,
and updates a table-driven IEEE CRC in the same loop. It is much slower than
copying the payload and using the existing hardware-accelerated `crc32fast`
implementation. No production WAL code was changed, and the result does not
rule out a hardware-accelerated fused implementation.

The CPU-only screen uses seven alternating rounds per payload length. Each
iteration copies and checksums one payload following an 84-byte header. The
4,008-byte payload represents a full DATA fragment; the 1,024-byte case does
not include the zero padding of a full physical frame. This is a component
screen, not an end-to-end WAL measurement.

| Payload | Existing best / median (µs) | Scalar fused best / median (µs) | Median ratio |
| ---: | ---: | ---: | ---: |
| 1,024 bytes | 0.065 / 0.066 | 1.650 / 1.652 | 25.1× |
| 4,008 bytes | 0.158 / 0.158 | 6.187 / 6.190 | 39.1× |

The verified run compares the output bytes and CRCs for every payload length
from zero through 4,008 bytes, using varied input and header lengths. It does
not replace the WAL's golden vectors or recovery tests. SSD benchmarks were not
run because this scalar CPU result cannot qualify the change.

The [prototype](artifacts/rfc-025-v7-fused-scalar-crc-20261030-prototype.rs),
[manifest](artifacts/rfc-025-v7-fused-scalar-crc-20261030-Cargo.toml),
[lockfile](artifacts/rfc-025-v7-fused-scalar-crc-20261030-Cargo.lock),
[initial results](artifacts/rfc-025-v7-fused-scalar-crc-20261030-initial-results.txt),
and [verified results](artifacts/rfc-025-v7-fused-scalar-crc-20261030-verified-results.txt)
preserve the screen. Run it in a standalone Cargo project with the prototype as
`src/main.rs` using `cargo run --release --offline`.
