# RFC 028: V8 Point Critical-Path GCP Diagnostic (2026-11-22)

## Decision

This diagnostic does not identify one stage as the cause of V8 Point's
throughput gap. It does show that FRONTIER mutex acquisition wait was small in
these runs, while the durability cycle contains substantial coalescing,
FRONTIER-write, and retirement time. `fdatasync` has a very short median and a
long tail. The measurements are useful for choosing the next diagnostic, not
for selecting a runtime optimization.

Do not treat the apparent Relay-versus-Direct throughput differences below as
a performance result: each arm has one instrumented run at each writer count.
The sync coverage is nearly unchanged between completion modes. No WAL runtime
code or persistence protocol was changed for this experiment.

## Workload and setup

One GCP `c4-standard-16-lssd` host in `us-central1-b` ran four diagnostic
configurations: Cursor Relay and Cursor Direct Completion at 8 and 16 writers.
Each run issued 100,000 Point puts with 1 KiB values to a fresh database.
Results are diagnostic values with sampling enabled, not uninstrumented
throughput scores.

The shared configuration was:

- Release `write-perf` binary built with the `bench` feature.
- Cursor progress mode 2; singleton submission disabled.
- Direct FRONTIER, O_DIRECT DATA, and 1 MiB Point-only sliding lookahead.
- Dynamic coalescing; point direct-frame and point frontier-credit batching
  disabled.
- Latency sampled every 10 puts; durability, CPU attribution, and mutex
  sampling every 128 events.
- Full `--wal-sync-diagnostics` and `--wal-completion-timeline` were disabled.

The checked-in bench build rejected `--wal-v8-durability-sample-every` with
Cursor mode, so a diagnostic-only patch removed the sampler-compatibility
guard in `WriteProfile::validate_v8_progress_ring_compatibility()` and
`MemTable::set_wal_v8_durability_sample_every()`. It did not change WAL runtime
behavior, synchronization, or the Cursor fast path. Patch SHA-256:

`c55c8e0ef1e7490e7892ba05f137b7fa8456b97c56935199a15d6a8a289f7f37`

The patched binary SHA-256 is
`c54349f0c4ccfb0a0dfa9bc22f9431ad31bb1c3fff24f6c444992ed0093e4744`.
The runtime source revision was `118067e805e12e597b3ad46199b256a7f5f57c80`.

Host I/O details: 16 vCPUs, Intel Xeon 6985P-C @ 2.30 GHz, two 375 GiB Local
SSD devices in RAID0, ext4 mounted with `rw,noatime,stripe=256`, Linux
`7.0.0-1017-gcp`; Rust `1.100.0` and Cargo `1.100.0`.

## Run verification

All four planned runs completed successfully. The first Relay8 attempt was
rejected by the checked-in sampler-compatibility guard before the workload
started; it is recorded as a failed setup attempt and is not counted as a
benchmark run. After applying the diagnostic-only patch, the four planned run
IDs each had a successful `result.json` and matching `run-metadata.json`.

- `summary.json`: `complete: true`, 4 planned and 4 completed.
- `remote-exit.json`: `failed: false`, 4 runs started.
- The downloaded archive matched its recorded SHA-256:
  `ae3feddf00f9bc9b5ff4ce4b24efa232e7ab9e7032769500ffe2a9ee944f0528`.
- Local archive and extracted run data:
  `bench-results/rfc-028-v8-point16-critical-path-118067e8-us-central1-b/collection-attempt-01/`.
- Cleanup verification confirmed the VM, boot disk, temporary SSH firewall
  rule, isolated VPC, subnet, and egress route were absent after collection.

## Run-level results

Each row is one run, not a repeated-run median. CPU/Put is process CPU divided
by completed puts. Latencies are sampled put latencies.

| Run | Throughput (puts/s) | CPU/Put (μs) | WAL syncs | Puts/sync | Put latency p50 / p95 / p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Relay8 | 31,075.4 | 55.72 | 12,989 | 7.70 | 0.234 / 0.416 / 0.759 |
| Direct8 | 32,393.5 | 53.94 | 12,954 | 7.72 | 0.228 / 0.387 / 0.723 |
| Relay16 | 41,538.2 | 80.90 | 10,130 | 9.87 | 0.342 / 0.683 / 0.991 |
| Direct16 | 43,337.1 | 74.86 | 10,060 | 9.94 | 0.326 / 0.666 / 0.937 |

Direct's single-run throughput was 4.2% higher at 8 writers and 4.3% higher at
16 writers. With no repeats, these differences do not establish a stable gain.
At 16 writers, estimated mean process CPU occupancy was about 3.36 cores for
Relay and 3.24 cores for Direct (`CPU/Put × throughput`); this is far below
the host's 16 vCPUs, but does not by itself identify the limiting resource.

## Durability-cycle phases

Values are `p50 / p95 / p99` in microseconds. `Wall` is elapsed time and `CPU`
is thread CPU time. Phase distributions were sampled once every 128 advances;
sample counts were 101 (Relay8), 101 (Direct8), 79 (Relay16), and 78
(Direct16). The table reports each stage independently. Do not add stage
percentiles to estimate an end-to-end cycle.

| Stage | Relay8 (Wall; CPU) | Direct8 (Wall; CPU) | Relay16 (Wall; CPU) | Direct16 (Wall; CPU) |
| --- | --- | --- | --- | --- |
| Driver cycle | 167.63/404.59/445.74; 81.37/120.58/123.50 | 168.54/331.25/474.19; 88.39/116.08/126.78 | 244.69/464.99/590.14; 123.87/187.51/221.22 | 226.45/417.44/554.16; 120.08/172.42/196.51 |
| Coalescing | 65.70/142.13/173.43; 22.86/62.12/67.08 | 74.31/118.95/174.90; 26.33/53.74/72.85 | 110.96/178.37/241.74; 41.75/85.33/120.78 | 108.88/142.47/207.30; 38.31/76.31/83.49 |
| FRONTIER preparation | 4.94/20.62/31.01; 4.94/14.91/28.64 | 5.10/12.71/21.08; 5.09/12.71/20.90 | 5.57/17.44/34.03; 5.53/14.82/20.61 | 5.46/24.26/173.87; 5.47/19.92/29.38 |
| FRONTIER write | 34.28/46.79/152.40; 6.17/11.58/17.12 | 33.55/45.24/63.14; 6.05/14.65/21.10 | 35.74/47.67/119.83; 6.86/17.78/28.58 | 35.19/45.73/69.67; 6.89/17.03/32.02 |
| `fdatasync` | 1.17/119.54/140.12; 1.16/9.30/10.18 | 1.18/111.15/146.34; 1.17/10.40/13.95 | 1.77/115.08/136.99; 1.79/10.74/18.10 | 1.58/120.01/132.35; 1.60/10.72/18.48 |
| Pre-publish retirement | 18.71/19.62/19.93; 18.71/19.53/19.94 | 18.92/20.06/25.05; 18.92/19.99/25.05 | 27.18/41.02/64.85; 27.18/40.94/64.80 | 25.60/36.51/40.81; 25.60/36.51/40.15 |
| Post-publish retirement | 13.93/65.22/71.72; 13.52/19.75/30.05 | 13.79/65.36/74.55; 13.61/19.77/28.96 | 23.87/77.55/142.00; 20.79/29.17/31.65 | 20.25/85.06/202.12; 18.75/30.30/33.34 |
| Durable publish | 3.81/4.53/4.81; 3.82/4.55/4.83 | 4.02/4.67/4.72; 4.03/4.67/4.73 | 4.89/6.07/7.91; 4.90/6.07/7.92 | 5.04/5.91/8.19; 5.08/5.97/8.24 |
| Publish notifications | 1.16/1.31/1.42 (Wall) | 1.17/1.28/1.31 (Wall) | 1.99/2.27/3.65 (Wall) | 2.03/2.26/2.60 (Wall) |

## Sync coverage

The sampled ticket count covered by each durability advance was:

| Run | p50 tickets | p95 tickets | p99 tickets | Mean tickets |
| --- | ---: | ---: | ---: | ---: |
| Relay8 | 8 | 8 | 8 | 7.70 |
| Direct8 | 8 | 8 | 8 | 7.68 |
| Relay16 | 12 | 16 | 16 | 10.68 |
| Direct16 | 11 | 16 | 16 | 9.77 |

Relay and Direct had similar sync counts and mean coverage in this single-run
comparison. It does not show that Direct Completion changed group formation.

## Lock waits

The FRONTIER mutex acquisition wait was small in these samples. Values below
are wall-time `p50 / p95 / p99` in microseconds:

| Run | FRONTIER mutex wait | Coordinator publish lock wait |
| --- | --- | --- |
| Relay8 | 1.08 / 1.40 / 3.31 | 0.24 / 0.26 / 0.34 |
| Direct8 | 1.04 / 1.31 / 2.25 | 0.24 / 0.26 / 0.32 |
| Relay16 | 0.57 / 3.10 / 4.57 | 0.29 / 0.37 / 1.25 |
| Direct16 | 0.62 / 3.02 / 5.04 | 0.32 / 0.38 / 0.98 |

The mutex sampler also reported quantized wait and hold distributions for the
resource ledger, reservation owner, extent state, extent preparation, and MVCC
publication locks. At 16 writers, estimated contended-sample counts were
385/0 (Relay/Direct) for the resource ledger, 13,612/12,324 for the
reservation owner, 13,845/13,855 for extent state, and 84,773/88,069 for MVCC
publication. These are sampled/extrapolated counts, not exact acquisition
totals. The report does not contain an independent `frontier_lock_hold_ns`
measurement, so low acquisition wait cannot rule out a long serialized driver
cycle.

## ACK and publication latency

Each run contributed 781 sampled ACK records. Values are `p50 / p95 / p99`
microseconds; these are observed stage latencies, not additive components.

| Stage | Relay8 | Direct8 | Relay16 | Direct16 |
| --- | --- | --- | --- | --- |
| Durable publish → WAL return | 25.42 / 52.78 / 66.43 | 26.18 / 53.79 / 70.25 | 33.44 / 68.45 / 93.39 | 33.81 / 69.45 / 85.29 |
| WAL return → memtable start | 0.08 / 0.24 / 0.40 | 0.08 / 0.20 / 0.37 | 0.09 / 0.21 / 0.76 | 0.08 / 0.24 / 0.75 |
| Memtable publication wall | 2.26 / 3.08 / 9.47 | 2.28 / 3.09 / 8.41 | 2.56 / 5.41 / 13.81 | 2.52 / 5.36 / 12.46 |
| MVCC publication | 1.60 / 21.72 / 38.60 | 1.77 / 23.20 / 32.69 | 10.04 / 81.00 / 113.85 | 9.16 / 68.20 / 136.64 |
| WAL return → Put return | 5.63 / 28.76 / 44.26 | 5.94 / 29.14 / 39.21 | 16.47 / 87.91 / 122.27 | 15.04 / 75.34 / 141.39 |

## Interpretation and limits

- FRONTIER mutex acquisition contention was not prominent: its sampled p50
  wait stayed below 1.1 μs and p95 near 3.1 μs or less. This does not rule out
  the time spent executing the serialized durability cycle; lock hold time was
  not measured independently.
- Coalescing consumed substantial wall time at 16 writers (p50 about 109–111
  μs), with thread CPU p50 about 38–42 μs. The sampler did not split condition
  variable sleep, active work, and reacquisition; detailed sync diagnostics
  were intentionally disabled to preserve the Cursor fast path.
- FRONTIER write p50 was about 35 μs wall and 7 μs thread CPU. `fdatasync`
  p50 was about 1.6–1.8 μs, while p95 was about 115–120 μs. The broad
  distribution deserves follow-up, but these single-run samples do not prove
  that sync I/O is the throughput limiter.
- Direct Completion did not materially change puts per sync here. Its raw
  throughput was higher in both writer counts, but one instrumented run per
  arm is insufficient to claim a repeatable improvement.
- Phase p99s use only 78–101 samples and are noisy. ACK p99s use 781 samples.
  Instrumentation can perturb scheduling and latency; these are not promotion
  scores.
- The report does not establish causality between any phase and throughput.

The next diagnostic should separate Cursor coalescing into actual wait,
active work, and mutex reacquisition without enabling detailed snapshots that
change the fast path. If the durability cycle remains the dominant wall-time
stage, compare its sync-I/O behavior with a matched V7 path before changing
runtime code. Keep the current WAL protocol and production defaults unchanged
until a repeated, uninstrumented A/B demonstrates a benefit.
