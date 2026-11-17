# RFC 026: V7/V8 Point thread-sample reanalysis, 2026-11-17

This is an offline reanalysis of the eight `perf.data` captures already
described in the [V7/V8 Point lifecycle CPU profile](v7-v8-point-lifecycle-cpu-profile-gcp-20261117.md).
It adds no benchmark run and changes no runtime code.

## Method and limits

The source is the checksum-verified archive
`bench-results/gcp-point-lifecycle-profile-20261117T154123Z/gcp-point-lifecycle-profile-retry-20261117.tar.gz`
(SHA-256 `54460f3c50caf93f9870401fbd46545930fcd8f80521a39e94895f4adac11c6d`).
The archive contains all eight planned profile captures, each with zero lost
samples. The event is `task-clock` sampled at 199 Hz. I grouped
`perf script -F comm,pid,tid,time,event,period` samples by Linux thread
command name and summed sample periods. The values below are **sample-weighted
CPU estimates**, not exact per-thread `CLOCK_THREAD_CPUTIME_ID` totals.

Linux truncates thread command names to 15 bytes. The `toy-kv-v8-exten` prefix
identifies the V8 extent-lookahead worker; `wal-v8-completi` identifies the
completion reaper. `write-perf` includes the main and writer threads. V8's
durability driver runs on whichever writer wins election, so its CPU is also
inside the `write-perf` group.

## V8 sampled thread CPU

Each row is one profiled 100,000-put run. Sample-weighted time is approximate;
the low lookahead sample counts in particular do not support a precise CPU
total.

| Writers | Lookahead worker samples / estimated CPU | Completion reaper samples / estimated CPU | I/O worker samples / estimated CPU | `write-perf` share of sampled task-clock |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 24 / 120.6 ms | 154 / 773.9 ms | 305 / 1,532.7 ms | 54.1% |
| 4 | 11 / 55.3 ms | 62 / 311.6 ms | 152 / 763.8 ms | 73.6% |
| 8 | 10 / 50.3 ms | 67 / 336.7 ms | 163 / 819.1 ms | 76.5% |
| 16 | 14 / 70.4 ms | 71 / 356.8 ms | 182 / 914.6 ms | 80.4% |

At 16 writers, the lookahead worker accounts for about 1.0% of the sampled
task-clock, or roughly 0.7 sample-weighted CPU microseconds per Put. The
completion reaper accounts for about 5.2%, and the shared I/O worker about
13.3%. These estimates make lookahead initialization CPU an unlikely
explanation for the full 25 μs/Put process-CPU difference, while leaving the
I/O worker, writer-side coordination, and reaper as measurable contributors.
They do not establish which contributor is causally responsible for the
throughput gap.

For context, the 16-writer V7 capture attributed sampled task-clock to
`wal-ordered-pac` (112 samples, 562.8 ms), `wal-sync-coordi` (80 samples,
402.0 ms), `wal-io-worker` (107 samples, 537.7 ms), and
`wal-v7-extent-i` (4 samples, 20.1 ms). These are different thread roles and
should not be treated as one-to-one equivalents of V8's thread groups.

## Lock attribution remains unresolved

The archived `perf-report.txt` identifies the generic
`parking_lot::RawMutex::lock_slow` symbol but does not identify the owning
mutex. The raw profile records the exact binary's build ID, but that matching
binary/debug information is not in the local archive, so this reanalysis
cannot symbolize the Rust caller frames. The `RawMutex` percentage must not be
assigned to the V8 coordinator, extent state, resource maps, or MVCC mutex.

Exact worker CPU still requires a bench-only `CLOCK_THREAD_CPUTIME_ID`
measurement (with counters for the lookahead worker, completion reaper, and
I/O worker). Lock attribution needs caller-resolved stacks or sampled
wait/hold counters around the named locks. The driver must be attributed to
its elected writer, rather than described as a dedicated thread.

## GCP status

The profile archive is locally verified and complete. A read-only inventory
check found the retained VM still running at 2026-11-17 16:49 UTC, about 66
minutes after its recorded creation and beyond the configured 60-minute
maximum runtime. No additional benchmark was started, and no cloud resource
was deleted. Preserve the retained VM and network as previously instructed;
do not treat this note as evidence that cloud cleanup completed.
