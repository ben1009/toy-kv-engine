use std::{hint::black_box, time::Instant};
const fn table() -> [u32; 256] {
    let mut t = [0; 256]; let mut n = 0;
    while n < 256 { let mut c = n as u32; let mut b = 0; while b < 8 { c = (c >> 1) ^ (0xedb88320 & (0u32.wrapping_sub(c & 1))); b += 1; } t[n] = c; n += 1; } t
}
const TABLE: [u32; 256] = table();
fn reference(src: &[u8], dst: &mut [u8], header: &[u8]) -> u32 {
    dst.copy_from_slice(src); let mut crc = crc32fast::Hasher::new(); crc.update(header); crc.update(dst); crc.finalize()
}
fn fused(src: &[u8], dst: &mut [u8], header: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in header { crc = TABLE[((crc ^ u32::from(b)) & 255) as usize] ^ (crc >> 8); }
    for (d, &b) in dst.iter_mut().zip(src) { *d = b; crc = TABLE[((crc ^ u32::from(b)) & 255) as usize] ^ (crc >> 8); }
    !crc
}
fn main() {
 for size in 0usize..=4008 {
  let src: Vec<u8> = (0..size).map(|n| (n.wrapping_mul(31).wrapping_add(size) % 251) as u8).collect();
  let header: Vec<u8> = (0..(size % 85)).map(|n| (n * 7) as u8).collect();
  let mut a = vec![0xa5; size]; let mut b = vec![0x5a; size];
  assert_eq!(reference(&src, &mut a, &header), fused(&src, &mut b, &header));
  assert_eq!(a, b);
 }
 for size in [1024usize,4008] {
  let src:Vec<u8>=(0..size).map(|n| (n.wrapping_mul(31)%251) as u8).collect(); let header=[0x45;84]; let mut dst=vec![0;size]; let expected=reference(&src,&mut dst,&header); let mut other=vec![0;size]; assert_eq!(fused(&src,&mut other,&header),expected); assert_eq!(dst,other);
  let iterations=26214400/size;
  for round in 0..7 { for new in [round%2==1,round%2==0] { let start=Instant::now(); for _ in 0..iterations { let s=black_box(&src); let d=black_box(&mut dst); black_box(if new {fused(s,d,&header)} else {reference(s,d,&header)}); } println!("size={size} round={round} method={} iterations={iterations} ns={}",if new {"fused"}else{"reference"},start.elapsed().as_nanos()); } }
 }
}
