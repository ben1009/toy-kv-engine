# RFC 026: V8 background-role affinity, 2026-11-02

Status: diagnostic only; role attribution remains inconclusive. No affinity
policy or benchmark hook is retained in production. Runtime still matches the
notification/cursor implementation measured at `b8da402a`.

The requested unchanged-binary rerun is complete: all 12 additional captures
are storage-flagged, even with 60s idle before every pair. It adds no eligible
score. The original cohort, rerun and registration calibration are reported
separately below.

A later [thirty-minute quiet-recovery test](#thirty-minute-quiet-recovery-test-november-3)
also fails to restore the fast same-file durability behavior. OS background
writes and flushes continue during that pause; it does not establish an
uninterrupted thirty-minute device idle or identify garbage collection.

**Putting all userspace background roles and io_uring workers on P-cores can
reach 667,497 puts/s while main remains unrestricted.** However, 22 of 24 full
captures fail the predeclared storage filter, leaving no eligible narrow-role
or whole-process controls in this cohort. These data cannot identify the
responsible background role or qualify a production affinity change.

## Best eligible results

All six modes use one executable, 750,000 puts, 16 writers, batch64, 1KiB values,
V8 Parallel, no compaction, and the same ext4 SSD. A score selects the highest
eligible throughput within its mode, with p99 and CPU from the same capture.
Storage-flagged records never contribute a score.

| Mode, cumulative from top | Eligible / runs | Best puts/s | p99 (ms) | CPU (s) | Syncs |
|---|---:|---:|---:|---:|---:|
| Writers P-core, all background unrestricted | 0/4 | — | — | — | — |
| Add kernel io_uring workers P-core | 0/4 | — | — | — | — |
| Add WAL I/O worker P-core | 0/4 | — | — | — | — |
| Add V8 completion reaper P-core | 0/4 | — | — | — | — |
| Add remaining userspace background P-core; main unrestricted | 2/4 | 667,497 | 3.394 | 5.873 | 942 |
| Whole process P-core, including main | 0/4 | — | — | — | — |

The two eligible records for the shared-background mode are:

| Record | Puts/s | p99 (ms) | CPU (s) | Syncs | Mean fdatasync (ms) |
|---|---:|---:|---:|---:|---:|
| `07-v8-userspace` | 667,497 | 3.394 | 5.873 | 942 | 0.408 |
| `17-v8-userspace` | 628,803 | 4.208 | 6.700 | 896 | 0.405 |

The eligible median is 648,150 puts/s, p99 3.801ms and CPU 6.287s. The selected
best has 92,226 voluntary and 710 involuntary context switches. Existing 1/64
ticket sampling estimates payload BLAKE3 CPU at 0.560s and ordered-link CPU at
0.008s. Those are extrapolated samples, not exhaustive phase measurements.

For context, the [previous writer-affinity cohort](rfc-026-v8-writer-affinity-20261102.md)
reported 655,686 puts/s / 3.357ms / 5.853s for whole-process affinity. The new
667,497 result is in that performance range, but it uses a different executable
and cohort. This is not a matched improvement or a V4/V7 parity result.

## Why kernel placement comes first

The requested initial order was writers, then the I/O worker, completion
reaper, remaining userspace WAL roles, and finally kernel workers. Two smoke
preflights caught a confound before full measurements started:

1. With the I/O worker pinned after engine setup, newly created `iou-wrk-*`
   tasks also had mask `0-15` instead of the intended `0-31`.
2. Registering an explicit `0-31` io-wq mask at ring startup returned success
   but did not prevent that observed narrowing after the creator was pinned.

Both failed audits, including their successful command results and actual
thread masks, are preserved in the preflight artifact. They are unscored.
The exact kernel reason for this interaction is not established by this test.
A successful affinity-registration call alone was insufficient to verify the
requested intervention on this host.

The revised design tests kernel-only affinity before cumulative userspace
affinity. It holds actual kernel masks at `0-15` while adding userspace roles,
so the I/O worker's effect can be compared against the same kernel placement.
All six modes perform the same startup registration call, using `0-31` for the
writer-only control and `0-15` for the remaining modes. Every recorded kernel
worker mask matches the revised expectation.

| Mode | Writers | WAL I/O | Reaper | Shared background | Kernel workers | Main |
|---|---|---|---|---|---|---|
| `writers` | P | P+E | P+E | P+E | P+E | P+E |
| `kernel_only` | P | P+E | P+E | P+E | P | P+E |
| `io` | P | P | P+E | P+E | P | P+E |
| `reaper` | P | P | P | P+E | P | P+E |
| `userspace` | P | P | P | P | P | P+E |
| `whole` | P | P | P | P | P | P |

Here P means allowed CPUs `0-15`; P+E means `0-31` on the i9-13900T. Allowed
masks do not establish actual execution residency or independently control
frequency, SMT contention, cache sharing, or pipeline timing.

## Role boundaries and measurement

V8 has two dedicated userspace WAL threads: `wal-io-worker` and
`wal-v8-completion-reaper` (Linux comm `wal-v8-completi`). There is no additional
standalone V8 packer/coordinator thread to test. The next userspace arm therefore
adds the engine background runtime (`kv-background-r`) and Tokio worker threads;
it is explicitly broader than a dedicated-WAL-thread intervention. The engine
runtime hosts background work, rather than being a dedicated reclamation thread.

Foreground writers already execute canonical encoding, CRC, BLAKE3, memtable
and MVCC publication, and the submission/prefix/frontier work that runs inline.
They are pinned once at thread entry in every arm except whole-process, where
they inherit the same P-core mask. Background threads are pinned after engine
setup and before the timed interval. Main's mask remains `0-31` except in the
whole-process arm, launched with `taskset -c 0-15`.

The temporary bench-feature hook calls io-uring 0.6.4's `register_iowq_aff` at
ring startup and logs successful registrations. It changes no ring flags,
SQE policy, digest, DATA grouping, wire format, or durability-coalescing policy.
Both modified Rust files were restored from exact backups before the full
measurements. The reproduction patch and preserved executable describe the
diagnostic intervention; neither is an adopted runtime change.

Normal measurements use existing counters without `--profile`, perf sampling,
or new per-batch clocks. Thread masks are audited before/after the timed
interval and once at each writer's entry. Context switches are measured with
`getrusage`; `/proc` traversal, background pinning and audit serialization are
outside the throughput/process-CPU interval. One-time writer affinity calls
remain within the original writer-launch interval.

## Qualification and reproduction

The fixed schedule has four rounds of six modes: two order/reverse pairs,
45s idle before each round and 3s between captures. Every arm uses the unchanged
filter: `p99 > 10ms OR mean fdatasync > 1ms`. All 24 commands succeed and pass
their role, operation-count and digest assertions, but only two captures pass
the storage filter. No adjacent narrow-role comparison is jointly eligible.

Eight additional 4,096-put smokes cover all six V8 modes and V7 writer/kernel
controls. They validate affinity and operations only; their timings are not
performance scores. Every V8 capture has the expected payload-hash byte count
and zero post-encode hash reread bytes. In V8, `commit_groups` counts durability
groups, not worker DATA submission groups.

The local executable is
`target/rfc-026-v8-background-affinity-20261102/write-perf-background-affinity`,
SHA256 `2ee0a92c06ff4d1f69937fb1bfe0130582b3a679c1b5730fa0c4110bb22578f1`.
Its source base is `604b870e`; production Rust sources remain unchanged.

## Unchanged-binary rerun

The maintainer requested another run after the initial cohort. The rerun uses
the exact same executable and six masks, with a fresh 12-capture forward/reverse
schedule. It increases idle time to 60s before every pair, rather than 45s
before each six-capture round. Workload, counters and exclusion thresholds are
unchanged.

All 12 commands succeed and pass their role, 750,000-operation and V8 digest
assertions. Every capture exceeds both storage thresholds, leaving 0/2 eligible
for every mode and no new best score. The longer-idle rerun therefore cannot
confirm or reject a role-specific performance benefit. The earlier 667,497
puts/s record remains context from the original cohort, rather than being
presented as a successful rerun.

The [rerun plan](artifacts/rfc-026-v8-background-affinity-20261102-rerun-01-plan.json),
[runner](artifacts/rfc-026-v8-background-affinity-20261102-rerun-01-rerun.py),
[all 12 records](artifacts/rfc-026-v8-background-affinity-20261102-rerun-01-measurements.jsonl),
[summary](artifacts/rfc-026-v8-background-affinity-20261102-rerun-01-summary.json),
and [validation](artifacts/rfc-026-v8-background-affinity-20261102-rerun-01-checks.json)
preserve the separate cohort. Its local directory is
`target/rfc-026-v8-background-affinity-20261102/rerun-01`.

### Additional control-gated continuation

A further continuation uses the same executable, workload, masks and exclusion
thresholds. It first runs writer-only and whole-process endpoint controls in
forward/reverse pairs, with 15s idle before the first pair and 45s before the
second. Its predeclared plan expands into twelve further role captures only
if at least one endpoint pair is jointly eligible.

All four controls complete and pass operation, digest and affinity checks, but
all four are storage-flagged. Writer-only p99 is 31.224 / 27.172ms and whole-process
p99 is 25.651 / 29.052ms. Mean fdatasync ranges from 1.192 to 2.388ms. Neither
pair is jointly eligible, so the twelve conditional role captures are not run.
There is no new eligible score or role-specific performance conclusion.

The [continuation plan](artifacts/rfc-026-v8-background-affinity-20261102-rerun-02-plan.json),
[runner](artifacts/rfc-026-v8-background-affinity-20261102-rerun-02-rerun.py),
[all four controls](artifacts/rfc-026-v8-background-affinity-20261102-rerun-02-measurements.jsonl),
[gate decision](artifacts/rfc-026-v8-background-affinity-20261102-rerun-02-gate.json),
and [independent validation](artifacts/rfc-026-v8-background-affinity-20261102-rerun-02-checks.json)
preserve the conditional design and outcome. No SMART query, tracing, device
setting change or production code change occurs during this continuation.

## Registration-hook calibration

A separate four-capture control checks whole-process P-core placement with and
without the registration hook, in order/reverse pairs with 60s idle before each
pair. The earlier preflight executable supplies the unregistered arm; it uses
the same foreground/background affinity harness. Actual masks match in both
arms. Both registered and unregistered captures are storage-flagged (0/2
eligible per arm), so the stalls are not confined to the new hook. This does
not establish performance neutrality: no clean calibration pair exists.
These four captures are kept separate from the 24-run role cohort.

The [calibration plan](artifacts/rfc-026-v8-background-affinity-20261102-calibration-plan.json),
[runner](artifacts/rfc-026-v8-background-affinity-20261102-calibrate.py), and
[all four records](artifacts/rfc-026-v8-background-affinity-20261102-calibration-measurements.json)
preserve this check. The earlier executable's
[patch](artifacts/rfc-026-v8-background-affinity-20261102-preflight-first-background-affinity.patch)
and [build identity](artifacts/rfc-026-v8-background-affinity-20261102-preflight-first-build.json)
are included for reproduction.

## Why the rerun was storage-flagged

The timing filter alone does not prove an SSD-internal cause. Source inspection
confirms that V8 records wall time around `fdatasync_once`, separately from
coalescing and FRONTIER writes; that syscall can include filesystem, pending
I/O, device, and completion-scheduling time. Further diagnostics reproduce a
delay below the WAL runtime.

A single-thread standalone probe reuses an initialized 8MiB file on the same
ext4 SSD, with aligned synchronous 4KiB `O_DIRECT` writes. It has no engine,
io_uring, WAL worker, MVCC, memtable or affinity-registration hook. Two passes
with `fdatasync` every four writes reach only 3,124 and 3,323 writes/s. The first
pass's slowest `pwrite` is 41.630ms. A phase without periodic syncs reaches
30,530 writes/s, although rare long writes remain. That phase makes no
durability claim and is an isolation control, not a proposed write policy.

A second standalone run records NVMe setup/completion tracepoints. Trace data
and diagnostic stdout remain in RAM during the workload, then are copied to
the local repository. All 36,946 device commands pair by controller, queue and
command ID, with no unmatched events, live-key collisions or nonzero statuses.
The 32,784 probe-issued commands include its initialization, writes and reads.
Kernel threads submit flushes, so flush statistics include the whole device.

| NVMe setup → driver completion | Commands | Median (µs) | p99 (µs) | Maximum (ms) |
|---|---:|---:|---:|---:|
| Probe write following a device flush completion | 4,094 | 629.851 | 8,707.560 | 20.394 |
| Other probe writes | 20,498 | 7.549 | 14.486 | 9.819 |
| Probe reads | 8,192 | 17.071 | 58.662 | 0.337 |
| Device flushes | 4,100 | 212.596 | 532.349 | 0.934 |

"Following" means the last observed device completion at write setup was a
flush; it does not assert the controller's internal ordering. This category
accounts for 91.4% of the probe's summed NVMe write-command durations. Its long
tail already exists between NVMe setup and driver completion, before caller
wakeup or WAL coordination. This is consistent with the
[earlier post-flush storage investigation](../environment/rfc-024-wal-stall-diagnosis.md).

The strongest supported explanation is a shared sync-sensitive storage-path
delay that all benchmark modes keep exercising. Longer idle did not reliably
restore comparable storage latency. A syscall or driver trace still cannot
separate controller firmware, NAND work, transport and interrupt processing,
or quantify how much of each V8 capture's latency came from this mechanism.
It does not establish garbage collection, SLC-cache exhaustion, APST, or a
thermal-throttling cause.

A read-only health query before the standalone probes reports firmware `004C`,
35°C, critical warning 0, media errors 0, error-log entries 0, and no recorded
warning/critical temperature time. These counters do not exclude a firmware
performance state, nor measure temperature during the earlier cohort. SMART
queries remain outside measurement intervals; the earlier investigation found
that admin queries can perturb timing. No device setting or firmware is changed.

The perf text renderer falls back to raw fields with a `FAILED TO PARSE` marker
for opcode-specific formatting. Pairing uses only the emitted disk, controller,
queue, command ID, opcode and status fields; it does not use that pretty-printer
or discard those events. The complete compressed event stream is preserved.

Diagnostic artifacts:

- [Standalone probe](artifacts/rfc-026-v8-background-affinity-20261102-storage-probe.py),
  [untraced results](artifacts/rfc-026-v8-background-affinity-20261102-storage-probe.json),
  [health counters](artifacts/rfc-026-v8-background-affinity-20261102-storage-health.json).
- [RAM-backed capture script](artifacts/rfc-026-v8-background-affinity-20261102-trace-storage.py),
  [traced probe results](artifacts/rfc-026-v8-background-affinity-20261102-storage-trace-probe.json),
  [pairing analyzer](artifacts/rfc-026-v8-background-affinity-20261102-analyze-storage-trace.py),
  [NVMe latency summary](artifacts/rfc-026-v8-background-affinity-20261102-storage-trace-summary.json).
- [Complete decoded trace, gzip](artifacts/rfc-026-v8-background-affinity-20261102-storage-trace-events.txt.gz),
  [trace identities](artifacts/rfc-026-v8-background-affinity-20261102-storage-trace-hashes.json).
  The 11.2MB raw perf recording remains under the local `storage-trace` directory.

## Mitigation audit and application batching

No software cure for this device state has been established. The
[earlier mitigation work](../environment/rfc-024-environment-mitigations-20261004.md)
and [storage diagnosis](../environment/rfc-024-wal-stall-diagnosis.md) already
tested APST, volatile cache, HMB, CPU policy, VMD bypass, buffered I/O and FUA.
None reliably eliminated the stalls. Disabling volatile cache or using FUA
made some tests slower. TRIM followed by settling sometimes preceded recovery,
but neither idle nor TRIM provided a reliable reset.

Fresh native inspection finds 544GiB available on `/home`, 36% filesystem use,
and an active weekly `fstrim.timer`. Its latest run succeeded on November 2,
00:17:30–00:23:15 CST. Thus this is not an obviously full filesystem or disabled
TRIM schedule. No extra discard is issued for this audit.

The installed `004C` firmware remains the P41 Plus's latest generic release in
[Solidigm's firmware table](https://www.solidigm.com/support-page/drivers-downloads/ka-00099.html).
[Solidigm support](https://community.solidigm.com/t5/solid-state-drives-nand/p41-plus/td-p/24663)
also states that no further firmware releases are planned. There is no newer
published firmware fix to apply based on those sources.

A separate application-level screen tests whether larger write batches can
amortize the storage cost without changing engine durability. The same
executable runs 750,000 puts, 16 writers, whole-process P-core affinity, and
unchanged V8 Parallel settings in order `64 → 256 → 256 → 64`.

| Order | Entries per application batch | Puts/s | Batch p99 (ms) | Syncs | Mean fdatasync (ms) |
|---|---:|---:|---:|---:|---:|
| 0 | 64 | 106,090 | 26.269 | 1,524 | 2.480 |
| 1 | 256 | 138,311 | 119.624 | 473 | 6.060 |
| 2 | 256 | 286,044 | 50.775 | 423 | 2.770 |
| 3 | 64 | 324,468 | 17.553 | 1,039 | 1.118 |

All four captures are storage-flagged and preserved. They are diagnostic
observations of degraded storage, not qualified RFC scores. The larger batch
cuts sync count by 59–69%, but throughput changes by +30.4% in the first adjacent
comparison and -11.8% in the reversed comparison. Per-batch p99 increases in
both. Each batch now contains four times the application work; these are not
same-request-size latency comparisons or a V8 runtime optimization claim.
Changing device state remains a confound.

Acknowledgement still follows durable FRONTIER sync. No engine setting, format,
source code or default batch size is changed. This screen does not establish a
reliable batching mitigation to recommend. A comparison on another physical
SSD would be an optional isolation test, not a prerequisite for continuing work.
The current environment has only this SSD. Correctness and CPU investigations
can continue here; physical-SSD performance qualification still requires clean
matched controls on this device. Neither a shorter workload nor RAM-backed I/O
replaces that evidence.

The [screen plan](artifacts/rfc-026-v8-background-affinity-20261102-batch-mitigation-plan.json),
[runner](artifacts/rfc-026-v8-background-affinity-20261102-batch-mitigation-run.py),
[all four captures](artifacts/rfc-026-v8-background-affinity-20261102-batch-mitigation-measurements.jsonl),
[pair deltas](artifacts/rfc-026-v8-background-affinity-20261102-batch-mitigation-summary.json),
[native environment checks](artifacts/rfc-026-v8-background-affinity-20261102-batch-mitigation-environment.json),
and [validation](artifacts/rfc-026-v8-background-affinity-20261102-batch-mitigation-checks.json)
preserve the mitigation assessment.

### Single-SSD follow-up, November 3

Native device enumeration confirms one physical NVMe SSD. Both ext4 partitions
(`/` and `/home`) share it; `/tmp` is tmpfs. Diagnostic databases remain on the
local repository's physical ext4 filesystem.

A four-capture writer-only/whole-process forward/reverse screen reduces the
workload from 750,000 to 150,000 puts, keeping batch64, 16 writers, value size,
binary, masks, durable-sync semantics and storage thresholds unchanged. There
is 15s idle before each pair. The 80% reduction in total writes does not restore
usable controls: all four captures are storage-flagged and neither pair is
jointly eligible. All operation, digest, registration and affinity assertions
pass, with zero post-encode hash reread bytes.

These shorter runs have fewer latency samples and do not qualify sustained
performance or V4/V7 parity. No device setting or runtime source is changed.
The [plan](artifacts/rfc-026-v8-background-affinity-20261102-low-pressure-20261103-plan.json),
[runner](artifacts/rfc-026-v8-background-affinity-20261102-low-pressure-20261103-run.py),
[all four records](artifacts/rfc-026-v8-background-affinity-20261102-low-pressure-20261103-measurements.jsonl),
[summary](artifacts/rfc-026-v8-background-affinity-20261102-low-pressure-20261103-summary.json),
and [validation](artifacts/rfc-026-v8-background-affinity-20261102-low-pressure-20261103-checks.json)
preserve this single-device continuation.

### Thirty-minute quiet-recovery test, November 3

A fixed paired experiment tests a substantially longer powered-on interval
without benchmark writes. The same previously initialized 8 MiB scratch inode
is held open throughout. Each endpoint overwrites 1,024 aligned 4 KiB blocks
(4 MiB total), with `fdatasync` after every four writes, on the same ext4
filesystem. Both endpoints run on CPU 8 and use identical data, offsets and a
matching durable prelude. No file allocation, initialization, TRIM, admin query,
profiling or device-setting change occurs between endpoints.

The measured interval is **1,800.020 seconds**. A RAM-held collector reads
whole-device counters every five seconds and checks benchmark/maintenance
process names every minute. No matching job is observed. The reference file
is untouched during the pause, and reports are written only after the second
probe. Ordinary OS work continues: the SSD records **77.027 MiB of writes,
361 flushes, zero read sectors and zero discard sectors** during the pause.
The longest observed counter interval without completed host I/O is about
15 seconds. This is a low-activity operating system, not thirty minutes of
uninterrupted device idleness; host counters do not expose internal NAND work.

| Same-file durability probe | Before quiet interval | After quiet interval |
|---|---:|---:|
| Writes/s, including periodic syncs | 3,407 | 3,317 |
| Mean direct-write time, µs | 230.860 | 237.774 |
| First write after probe fdatasync, median µs | 640.057 | 640.137 |
| Other writes, median µs | 13.539 | 16.250 |
| Mean fdatasync, µs | 248.105 | 251.909 |
| Writes over 5 ms / 1,024 | 8 | 8 |
| Maximum write, ms | 9.156 | 9.486 |

**The longer pause does not restore the fast state in this experiment.**
Throughput changes by −2.7%; the routine post-sync delay is essentially
unchanged. Every write exceeding 5 ms is the first write after the preceding
probe fdatasync in both endpoints. These are syscall sequence observations,
not another NVMe trace or proof of the controller's internal ordering.

The overall write p99 is only 0.644/0.643 ms because eight long writes among
1,024 fall below one percent. That percentile must not be substituted for the
slow-write count or interpreted as recovery. This small standalone diagnostic
does not use the engine's storage filter and does not enter best-score tables.

This result does not establish garbage collection, cache exhaustion or their
absence. It rejects assuming that a thirty-minute benchmark pause alone resets
this state on the running system. OS flushes remain a confound for completely
idle cleanup; a single before/after sequence is not a randomized causal test.
The [raw result](artifacts/rfc-026-v8-background-affinity-20261102-quiet-recovery-20261103-result.json),
[runner](artifacts/rfc-026-v8-background-affinity-20261102-quiet-recovery-20261103-run.py),
[analyzer](artifacts/rfc-026-v8-background-affinity-20261102-quiet-recovery-20261103-analyze.py),
[summary](artifacts/rfc-026-v8-background-affinity-20261102-quiet-recovery-20261103-summary.json)
and [checks](artifacts/rfc-026-v8-background-affinity-20261102-quiet-recovery-20261103-checks.json)
preserve all write/sync latencies and 360 quiet-interval counter snapshots.

## Artifacts

Artifacts use prefix
`artifacts/rfc-026-v8-background-affinity-20261102-`:

- [Plan and fixed schedule](artifacts/rfc-026-v8-background-affinity-20261102-plan.json),
  [runner](artifacts/rfc-026-v8-background-affinity-20261102-run.py),
  [analyzer](artifacts/rfc-026-v8-background-affinity-20261102-analyze.py).
- [Complete 24 records with audits/status](artifacts/rfc-026-v8-background-affinity-20261102-measurements.jsonl),
  [summary](artifacts/rfc-026-v8-background-affinity-20261102-summary.json),
  [run log](artifacts/rfc-026-v8-background-affinity-20261102-run.log).
- [Eight smoke records](artifacts/rfc-026-v8-background-affinity-20261102-smoke-measurements.json),
  [failed preflight audits](artifacts/rfc-026-v8-background-affinity-20261102-preflight-audits.json).
- [Temporary diagnostic patch](artifacts/rfc-026-v8-background-affinity-20261102-background-affinity.patch),
  [build identity](artifacts/rfc-026-v8-background-affinity-20261102-build.json),
  [source restoration](artifacts/rfc-026-v8-background-affinity-20261102-restoration.json),
  [validation](artifacts/rfc-026-v8-background-affinity-20261102-checks.json).

The useful next comparison is a clean same-binary narrow-role pair, preserving
the kernel-mask control above. These data do not justify a prep worker pool,
production pinning, or another digest/coalescing change.
