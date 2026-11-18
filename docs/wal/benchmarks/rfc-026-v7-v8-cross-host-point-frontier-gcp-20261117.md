# V7/V8 Cross-Host Point Qualification

**Outcome:** the large V7 baseline drop from the previous capture did not reproduce on either fresh host. V7 medians were within 0.75% across the two hosts. Direct FRONTIER beat Buffered on both hosts, while V8 Direct remained about 14–17% below V7.

## Experiment

- Two independently created `c4-standard-16-lssd` VMs in `us-east1-b`, run sequentially to stay within the 24-vCPU project quota.
- Each host: 5 counterbalanced repetitions of V7 Parallel, V8 Point-only 1 MiB Lookahead + Buffered/Dynamic, and V8 Point-only 1 MiB Lookahead + Direct/Dynamic; 15 runs per host, 30 total.
- Every run: 16 writers, 100,000 Point puts, 1 KiB values, compaction disabled, built-in latency sample every 100 operations; no perf/eBPF/WAL detailed diagnostics.
- The same low-overhead `iostat`, `mpstat`, `vmstat`, and 1-second CPU-frequency samplers ran on both hosts during score runs.
- Source commit: `dc067676fa4205e93ae98ea9e74cecb876e7506d`; source archive SHA-256: `32e136d9b83d1e65dee1bd0ba88711569f582f64c51d9e2cd20e64734c1e860b`; runner SHA-256: `9a7b4fd4fb3223ffff12b28e1426fde666ce29b473b21bab3f55700f08d2570a`.
- Both hosts reported Linux 7.0.0-1017-gcp, the same Intel Xeon 6985P-C CPU model, 16 logical CPUs/8 cores, ext4 over RAID0 of two NVMe Local SSDs, and Rust `rustc 1.100.0-nightly (6bb1652a0 2026-09-22)`.

## Results

Host medians over five runs; latency is p50/p95/p99 in ms, with each run sampling every 100th Put.

| Host | Configuration | Throughput | CPU μs/Put | Latency p50/p95/p99 | Sync count | Puts/Sync |
|---|---|---:|---:|---:|---:|---:|
| host-a | V7 Parallel | 48.02k/s | 60.24 | 0.302/0.570/0.912 | 8306 | 12.04 |
| host-a | V8 Lookahead + Buffered | 37.86k/s | 81.21 | 0.369/0.857/2.370 | 9764 | 10.24 |
| host-a | V8 Lookahead + Direct | 40.07k/s | 80.05 | 0.359/0.787/1.435 | 9985 | 10.02 |
| host-b | V7 Parallel | 48.37k/s | 60.16 | 0.297/0.563/0.862 | 8293 | 12.06 |
| host-b | V8 Lookahead + Buffered | 37.84k/s | 81.49 | 0.371/0.917/3.172 | 9763 | 10.24 |
| host-b | V8 Lookahead + Direct | 40.86k/s | 80.89 | 0.348/0.746/1.806 | 10134 | 9.87 |

### Paired comparisons

- Direct vs Buffered: Host A median **+5.75%** (4/5 wins); Host B **+7.21%** (4/5 wins). Across both hosts: **+7.02%**, 8/10 wins.
- Direct vs paired V7: Host A **-16.55%** (0/5); Host B **-14.54%** (0/5).
- V7 Host B vs Host A median changed **+0.75%**. V8 Buffered changed **-0.04%**; V8 Direct changed **+1.96%**.

## Host health during the score window

- host-a: `mpstat` 38 one-second all-CPU samples; mean/max CPU steal **0.00% / 0.00%**, mean idle **80.6%**, mean iowait **1.42%**.
  CPU-frequency sysfs samples: 640; median 3722 MHz (p10–p90 3521–3882 MHz). This is sampled `scaling_cur_freq`, not an independent hardware clock counter.
  `iostat` nvme0n1: 38 one-second write-active intervals; median/p95 `w_await` 30/50 μs. This is host/block-layer aggregate, not WAL-inode or per-fdatasync attribution.
  `iostat` nvme1n1: 38 one-second write-active intervals; median/p95 `w_await` 30/50 μs. This is host/block-layer aggregate, not WAL-inode or per-fdatasync attribution.
  `iostat` md127: 38 one-second write-active intervals; median/p95 `w_await` 30/50 μs. This is host/block-layer aggregate, not WAL-inode or per-fdatasync attribution.
- host-b: `mpstat` 38 one-second all-CPU samples; mean/max CPU steal **0.00% / 0.00%**, mean idle **80.6%**, mean iowait **1.36%**.
  CPU-frequency sysfs samples: 624; median 3663 MHz (p10–p90 3471–3857 MHz). This is sampled `scaling_cur_freq`, not an independent hardware clock counter.
  `iostat` nvme0n1: 38 one-second write-active intervals; median/p95 `w_await` 30/90 μs. This is host/block-layer aggregate, not WAL-inode or per-fdatasync attribution.
  `iostat` nvme1n1: 38 one-second write-active intervals; median/p95 `w_await` 30/80 μs. This is host/block-layer aggregate, not WAL-inode or per-fdatasync attribution.
  `iostat` md127: 38 one-second write-active intervals; median/p95 `w_await` 30/100 μs. This is host/block-layer aggregate, not WAL-inode or per-fdatasync attribution.

## Interpretation and limits

- The previously observed ~30k/s V7 result was not reproduced: both new hosts returned ~48k/s with near-identical V7 CPU/Put and throughput. This supports a capture/host-state difference as a plausible explanation, but does not identify why the earlier run was slow.
- Direct FRONTIER improved paired throughput over Buffered on both new hosts, with no reduction in Sync count (it was slightly higher). This independently supports a real Direct candidate benefit of roughly 6–7% in this configuration; it does not qualify a production default.
- Direct remained materially slower than V7: about 14–17% in the paired comparisons. The original performance gap is reduced but not closed.
- Source archive SHA, runner SHA, source commit, build command, and Rust compiler version match. The compiled `write-perf` SHA-256 differs (`bc8b0b095b804cc8c95a3719d2e4a18b8c99ba5de5dc27f7312785a7e99dc50f` vs `c5890117ee151d7ffbd78b703be18b2b759acd5842ed395f3c5b8bd1ed4dfbdd`). The report therefore does not claim byte-identical executable artifacts; a stricter follow-up can distribute one prebuilt binary to both hosts.
- p95/p99 use 1,000 latency samples per run and are summarized as the median of five per-run quantiles; they are indicative rather than a high-confidence tail qualification.
- These tests compare only 16-writer Point writes. They do not qualify mixed Point/Batch, other writer counts, quota/ENOSPC, crash recovery, or PITR replay.

## Result integrity and cleanup

- Host A and Host B each reported `complete: true`, 15 planned/completed runs, `remote-exit.json failed: false`, and 15/15 matching result + run-metadata pairs.
- Both result tarballs and their SHA-256 sidecars are retained locally and verified. No raw result tarball was uploaded to GitHub.
- Host A’s runner preflight command contained a mistyped commit argument; the benchmark itself used the verified `dc067676fa4205e93ae98ea9e74cecb876e7506d` source archive, which both remote plans record. Host B’s first read-only preflight selected the runner’s default Batch64 suite; it created no resources and was followed by a Point-lookahead preflight before VM creation. The runner’s Point preflight models its 750k/40-run suite, while the actual custom qualification schedule was 100k/15 runs per host. Machine, network, zone, VM lifetime and cost arguments matched.
- One Host A progress-check attempt used a mistyped project ID for the firewall audit; `set -e` was added before subsequent remote operations and the audit passed. The intervening operation was read-only, and the network’s `/32` SSH rule was unchanged.
- The workflow’s background monitor shutdown waited indefinitely on SIGINT-ignoring `vmstat` and CPU-frequency samplers after scoring. They were stopped with SIGTERM after the runner completed; both post-run metadata checks and all score results then completed.
- Both VMs, boot disks, temporary SSH firewalls, subnets, and isolated VPCs were destroyed after local verification; cleanup inventory checks confirm the named resources are absent.
- Local result directory: `bench-results/gcp-v7-v8-cross-host-20261117/`.
