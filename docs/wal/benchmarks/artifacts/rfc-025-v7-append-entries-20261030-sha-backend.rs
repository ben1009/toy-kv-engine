use std::{hint::black_box, time::Instant};
use sha2::{Digest, Sha256};

fn run_sha2(data: &[u8], iters: usize) -> ([u8; 32], u128) {
    let mut state = Sha256::new();
    state.update([0x31; 4096]);
    let start = Instant::now();
    for _ in 0..iters {
        state.update(black_box(data));
        black_box(state.clone());
    }
    let elapsed = start.elapsed().as_nanos();
    (state.finalize().into(), elapsed)
}
fn run_ring(data: &[u8], iters: usize) -> ([u8; 32], u128) {
    let mut state = ring::digest::Context::new(&ring::digest::SHA256);
    state.update(&[0x31; 4096]);
    let start = Instant::now();
    for _ in 0..iters {
        state.update(black_box(data));
        black_box(state.clone());
    }
    let elapsed = start.elapsed().as_nanos();
    (state.finish().as_ref().try_into().unwrap(), elapsed)
}
fn main() {
    for size in [1040_usize, 65_600] {
        let data: Vec<u8> = (0..size).map(|i| (i.wrapping_mul(31) % 251) as u8).collect();
        let iterations = (256 * 1024 * 1024 / size).max(1000);
        let mut reference = None;
        for round in 0..6 {
            for ring_first in [round % 2 == 1, round % 2 == 0] {
                let (name, (digest, elapsed)) = if ring_first { ("ring", run_ring(&data, iterations)) } else { ("sha2", run_sha2(&data, iterations)) };
                if let Some(r) = reference { assert_eq!(r, digest); } else { reference = Some(digest); }
                println!("size={size} round={round} algorithm={name} iterations={iterations} ns={elapsed}");
            }
        }
    }
}
