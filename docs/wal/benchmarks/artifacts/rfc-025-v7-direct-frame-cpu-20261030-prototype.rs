use sha2::{Digest, Sha256};
use std::{hint::black_box,time::Instant};
const F: usize=4096; const H:usize=88; const P:usize=F-H;
#[repr(align(4096))]
#[derive(Clone)]
struct Frame([u8;F]);
struct Entry { key:Vec<u8>, value:Vec<u8> }
fn emit(entries:&[Entry], mut sink:impl FnMut(&[u8])) {
 for e in entries {
  let len=(8+e.key.len()+e.value.len()) as u32;
  let mut h=[0u8;6];h[0]=1;h[2..6].copy_from_slice(&len.to_be_bytes());sink(&h);
  sink(&(e.key.len() as u32).to_be_bytes());sink(&e.key);
  sink(&(e.value.len() as u32).to_be_bytes());sink(&e.value);
 }
}
fn logical_header(n:usize, len:usize, crc:u32)->[u8;48] {
 let mut h=[0u8;48];h[..8].copy_from_slice(&41u64.to_be_bytes());h[8..16].copy_from_slice(&23u64.to_be_bytes());h[16..24].copy_from_slice(&1i64.to_be_bytes());h[28..32].copy_from_slice(&(n as u32).to_be_bytes());h[32..36].copy_from_slice(&(len as u32).to_be_bytes());h[36..40].copy_from_slice(&crc.to_be_bytes());let c=crc32fast::hash(&h[..40]);h[40..44].copy_from_slice(&c.to_be_bytes());h
}
fn frames(logical_len:usize)->Vec<Frame> {
 let count=logical_len.div_ceil(P); let mut v=vec![Frame([0;F]);count];
 for (i,frame) in v.iter_mut().enumerate(){let b=&mut frame.0;let plen=(logical_len-i*P).min(P);b[..8].copy_from_slice(b"TKVW7FR1");b[8..10].copy_from_slice(&1u16.to_be_bytes());b[10..12].copy_from_slice(&1u16.to_be_bytes());b[12..14].copy_from_slice(&64u16.to_be_bytes());b[16..48].fill(0x31);b[48..56].copy_from_slice(&((8192+i*F) as u64).to_be_bytes());b[56..60].copy_from_slice(&((24+plen) as u32).to_be_bytes());b[64..72].copy_from_slice(&41u64.to_be_bytes());b[72..76].copy_from_slice(&(i as u32).to_be_bytes());b[76..80].copy_from_slice(&(count as u32).to_be_bytes());b[80..88].copy_from_slice(&(logical_len as u64).to_be_bytes()); }
 v
}
fn put(out:&mut [Frame], mut at:usize, mut bytes:&[u8]) {while !bytes.is_empty(){let i=at/P;let k=at%P;let n=(P-k).min(bytes.len());out[i].0[H+k..H+k+n].copy_from_slice(&bytes[..n]);at+=n;bytes=&bytes[n..];}}
fn finish(out:&mut [Frame]) {for f in out {let mut h=crc32fast::Hasher::new();h.update(&f.0[..60]);h.update(&f.0[64..]);f.0[60..64].copy_from_slice(&h.finalize().to_be_bytes());}}
fn run(entries:&[Entry], mode:u8)->(Vec<Frame>,[u8;32]) {
 let data_len:usize=entries.iter().map(|e|14+e.key.len()+e.value.len()).sum();let len=48+data_len;let mut crc=crc32fast::Hasher::new();let mut sha=Sha256::new();sha.update([0x31;4096]);
 if mode==0 {let mut logical=vec![0;len];let mut at=48;emit(entries,|x|{logical[at..at+x.len()].copy_from_slice(x);at+=x.len();crc.update(x)});let header=logical_header(entries.len(),data_len,crc.finalize());logical[..48].copy_from_slice(&header);sha.update(&logical);let mut out=frames(len);put(&mut out,0,&logical);finish(&mut out);return (out,sha.finalize().into());}
 let mut out=frames(len);
 if mode==1 {let mut at=48;emit(entries,|x|{put(&mut out,at,x);at+=x.len();crc.update(x)});let header=logical_header(entries.len(),data_len,crc.finalize());put(&mut out,0,&header);for (i,f) in out.iter().enumerate(){let n=(len-i*P).min(P);sha.update(&f.0[H..H+n]);}}
 else {emit(entries,|x|crc.update(x));let header=logical_header(entries.len(),data_len,crc.finalize());put(&mut out,0,&header);sha.update(header);let mut at=48;emit(entries,|x|{put(&mut out,at,x);at+=x.len();sha.update(x)});}
 finish(&mut out);(out,sha.finalize().into())
}
fn main(){for n in [2usize,8,64] {let entries:Vec<_>=(0..n).map(|i|Entry{key:format!("key-{i:011}").into_bytes(),value:vec![0x5a;1024]}).collect();let (reference,hash)=run(&entries,0);for mode in [1,2]{let (other,h)=run(&entries,mode);assert_eq!(hash,h);assert_eq!(reference.len(),other.len());for (a,b) in reference.iter().zip(other.iter()){assert_eq!(a.0,b.0);}}
 let iterations= (8_000/n).max(200);for round in 0..7 {for step in 0..3 {let mode=(step+round)%3;let start=Instant::now();for _ in 0..iterations{black_box(run(black_box(&entries),mode));}println!("entries={n} round={round} mode={mode} iterations={iterations} ns={}",start.elapsed().as_nanos());}}}}

#[cfg(test)]
mod engine_validation {
 use super::*;
 use crate::pitr::{LIVE_WAL_V5_LIMITS,RecordedAt,WalBatch,WalEntry};
 use crate::wal::v7::codec::{encode_logical_batch,encode_data_frames_into};
 #[test]
 fn matches_real_v7_codec_and_rolling_sha() -> anyhow::Result<()> {
  for count in [2usize,8,64] {for value_len in [0usize,1,1024,3944,4008,8001] {
   let entries:Vec<_>=(0..count).map(|i|Entry{key:format!("key-{i:011}").into_bytes(),value:vec![0x5a;value_len]}).collect();
   let batch=WalBatch{commit_ts:23,recorded_at:RecordedAt{secs:1,nanos:0},entries:entries.iter().map(|e|WalEntry::Put{key:e.key.clone(),value:e.value.clone()}).collect()};
   let logical=encode_logical_batch(41,&batch,LIVE_WAL_V5_LIMITS)?;
   let mut actual=vec![0u8;logical.len().div_ceil(P)*F];
   let written=encode_data_frames_into([0x31;32],8192,41,&logical,LIVE_WAL_V5_LIMITS,&mut actual)?;
   let mut state=Sha256::new();state.update([0x31;4096]);state.update(&logical);let hash:[u8;32]=state.finalize().into();
   for mode in [0,1,2] {let (frames,digest)=run(&entries,mode);assert_eq!(digest,hash);assert_eq!(frames.len()*F,written);for (i,frame) in frames.iter().enumerate(){assert_eq!(&frame.0[..],&actual[i*F..(i+1)*F]);}}
  }}
  Ok(())
 }
}
