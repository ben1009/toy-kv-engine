# RFC 026 V8 positional DATA-write timing screen (2026-11-01)

## Purpose

This screen validates the new bench-only `v8_data_write_call_ns` counter and
separates V8 producer DATA-write call latency from FRONTIER writes and
`fdatasync`. It is diagnostic evidence, not a performance qualification or a
retention decision.

## Method

The release `write-perf` binary with `--features bench` ran the same 16-writer,
750,000-operation, batch-64, 1 KiB-value workload on `/dev/nvme0n1p3` (ext4),
first with PITR V8 Parallel and then with PITR V7 Parallel. Latency sampling was
every 10 operations. The command shape was:

```bash
write-perf --bench wal_batch_concurrent --wal-batch-size 64 \
  --pitr --pitr-format v8 --wal-io-mode parallel --num 750000 \
  --threads 16 --value-size 1024 --target-sst-size 2147483648 \
  --compaction none --output json --latency-sample-every 10
```

The V7 control changed only `--pitr-format v7`.

## Results

| Format | Run ID | Throughput (ops/s) | p99 (ms) | Syncs | `fdatasync` (s) | Frontier write (s) | DATA write-call sum (s) |
|---|---:|---:|---:|---:|---:|---:|---:|
| V8 Parallel | `1793501834672` | 81,617 | 33.071 | 1,915 | 8.129 | 0.072 | 1.904 |
| V7 Parallel | `1793501853025` | 99,943 | 31.830 | 1,423 | 1.880 | 2.685 | not measured |

V8 committed 11,728 ticket buffers (816,054,272 aligned DATA bytes). Its
`v8_data_write_call_ns` value is the sum across concurrent producer calls, not
elapsed wall time. The digest profile sampled 184 tickets: 787,500,000 payload
bytes in the workload, 16.1 ms sampled payload-hash CPU, 0.28 ms sampled
header/batch-hash CPU, 0.23 ms ordered-link CPU, and zero post-encode hash
reread bytes.

Both runs were stall-dominated: V8 spent 8.13 seconds in `fdatasync` during a
9.19-second measurement and reached a 103.9 ms maximum operation latency. The
V7 control also had a 31.8 ms p99 and a 77.5 ms maximum; it spent 2.71 seconds
in preallocation and 2.68 seconds in the measured frontier-write phase. These
paired results are excluded from performance comparison. They validate that
the new V8 counter is populated, but do not isolate V8 DATA writes from device
stalls or justify a write-path change.
