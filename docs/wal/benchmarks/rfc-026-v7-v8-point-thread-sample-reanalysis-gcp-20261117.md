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

## GCP status at initial reanalysis, 2026-11-17

The profile archive is locally verified and complete. A read-only inventory
check found the retained VM still running at 2026-11-17 16:49 UTC, about 66
minutes after its recorded creation and beyond the configured 60-minute
maximum runtime. No additional benchmark was started, and no cloud resource
had been deleted at the time of that check. The later inventory is recorded
below.

## Follow-up symbol-recovery check, 2026-11-18

The archived V8 `perf.data` records the benchmark ELF build ID
`f6620398d1cac57aadda63aed872418311d95cb7` at
`/mnt/gcp-wal-bench/work/toy-kv-engine/target/release/write-perf`. The archive
contains the raw profiles and text `perf-report.txt` files, but no matching ELF
or separate debug file. Re-running `perf report` locally without that ELF does
not resolve the benchmark's caller frames. The existing text report therefore
shows the generic `RawMutex::lock_slow` symbol and futex-side frames, but not
which Rust call sites acquired the mutex.

The matching build ID was absent from the local perf build-ID/debug caches and
from 149 `write-perf*` executables in the repository's target tree. The original
profile VM is now absent; a read-only Compute inventory found no matching
standalone disk or snapshot from which to recover its binary. The
profile-specific SSH firewall rule was removed after confirming that no VM
used its unique target tag. The shared network and subnet were left intact.

Consequently, caller-level attribution cannot be recovered from the retained
local artifacts. The lock percentages remain unattributed and must not be
assigned to a particular V8 mutex. A future profile run must retain the exact
Build-ID-matched ELF or detached debug symbols alongside `perf.data` and verify
both artifacts before releasing the VM. No new benchmark or runtime change was
made for this follow-up.

## GCP symbol-recovery retest, 2026-11-18

The retained GCP VM was reused; no VM was created. The first runner attempt
exited during SSH startup, so I resumed the runner on that same VM. The main
`v8-small-ticket-profile` suite then completed and was verified: 8/8 planned
runs, 8/8 matching result/metadata pairs, 2/2 requested perf captures, and
`remote-exit.json` reported `failed: false` with eight runs started. The
collected archive and checksum are retained under
`bench-results/gcp-rawmutex-stack-retest-20261118/collection-attempt-03/`.
The two failed collection attempts remain recorded in
`artifact-collection.json`; the successful recovery is appended as attempt 3.

The profile archive contained the ELF matching the older profile's recorded
Build ID, `f6620398d1cac57aadda63aed872418311d95cb7`. Its SHA-256 is
`9cf7d9bf1f6482cf2929ba5775a1e78ea8512632366fce0821303b50a65f324a`, but it
has no `.debug_info`. It confirms the executable identity, but cannot provide
the inline caller information needed to name the owning mutex.

I then built a separate release ELF with `CARGO_PROFILE_RELEASE_DEBUG=2` for
new captures. The V7 and V8 captures use the same binary (Build ID
`7436d4c34d768b01e432139b62bc1bc77eff4637`, SHA-256
`d61120c896e3a6e7365decbba72cdeeeb3ce50db31e1af9f45cf649776ae7481`) and
detached debug file (SHA-256
`4ef0c63fcf1ec7a79289eaa35b6a66babf5b9699fa153b617af95a0c5e6db9cb`). The
V8 run used Direct FRONTIER, 1 MiB Point Lookahead, and dynamic coalescing; the
V7 run had the V8-only environment overrides removed. Both were 16-writer,
100,000-put Point profiles, sampled at 199 Hz with `dwarf,4096` call graphs.
The exact debug ELF differs from the older ELF, so I did not use it to
re-symbolize the old `perf.data`.

The regenerated low-threshold reports confirm a larger generic
`RawMutex::lock_slow` share in the new diagnostic pair:

| Profile | Samples | `lock_slow` children | `lock_slow` self |
| --- | ---: | ---: | ---: |
| V7 | 959 | 8.34% | 1.98% |
| V8 Direct + Point Lookahead | 1,297 | 13.26% | 4.78% |

These percentages are sample shares from one diagnostic run per arm, not lock
wait duration or a throughput comparison. The internal call graph shows
parking/futex and spin/yield paths, but the raw mutex caller frames still stop
at `RawMutex::lock_slow` or contain unmapped addresses. `perf script` found 24
V7 and 63 V8 samples containing that symbol; none exposed a resolved
`kv_engine` or `write_perf` caller frame. The retest therefore confirms more
generic slow-path CPU samples in V8, but does **not** identify the coordinator,
resource ledger, extent, or MVCC mutex as the owner. The apparent
`acquire_durability_driver`/Condvar stacks elsewhere in the report are not
enough to assign the `lock_slow` samples to that mutex.

The new pair's profiled result rates (46.25k/s V7, 39.80k/s V8) are diagnostic
outputs only. `perf`, DWARF unwinding, and Hotpath sampling were enabled, so do
not compare these rates with unprofiled benchmark scores. The main eight-run
profile suite used runner-default WAL settings; it is also not a score for the
Point Lookahead + Direct FRONTIER candidate.

I attempted one final V8 capture with a 16 KiB DWARF stack buffer. The VM's
`perf` rejected the output-file override before launching the benchmark, and
no result was produced; that attempt is not part of the run counts above. No
runtime source was changed. The successful primary and supplemental archives,
their SHA-256 sidecars, the matching ELF/debug file, and verification metadata
are retained under `bench-results/gcp-rawmutex-stack-retest-20261118/`.

No cloud resource was manually deleted. A read-only inventory at 2026-11-18
00:52 UTC found the VM absent after its configured 60-minute server-side
lifetime. No delete command was issued for the temporary firewall or isolated
network. The local lifecycle record distinguishes server-side expiry from
manual cleanup and records the completed, checksum-verified result collection.

**Conclusion:** the exact old ELF is recovered, but it lacks DWARF, and fresh
DWARF-enabled captures still did not unwind the application caller above
`RawMutex::lock_slow`. The current evidence narrows the signal to more V8
generic lock slow-path samples, but mutex ownership remains unresolved. A future
capture needs a callchain method that successfully preserves caller frames or
targeted per-lock wait/hold counters before selecting an optimization.
