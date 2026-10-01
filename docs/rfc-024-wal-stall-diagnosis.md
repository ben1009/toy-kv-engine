# RFC 024: investigation of unstable ext4 measurements

The low-concurrency rerun reproduced large throughput swings with an unchanged
binary. The strongest current finding is a persistence-sensitive delay before
NVMe CQE consumption. On the same initialized file, the first write after each
four-write flush rises from 26 µs median in the fast state to 610 µs in the slow
state; the other three writes stay around 11–12 µs. The added mean latency of
that first command accounts for approximately 96% of this probe's throughput gap.
Longer stalls also follow a repeatable write/sync boundary. They persist with
the engine paused, on one repeatedly overwritten block, and with only one
host-visible NVMe command outstanding.

Delay insertion now identifies a routine post-flush service window: for short
inserted gaps, the next write CQE remains approximately 641 µs after the preceding
flush CQE. Reads of untouched blocks on another queue also absorb the wait.
Passive completion-queue sampling at approximately 100 µs intervals finds that
the delayed commands' CQEs become visible only near driver consumption; observed
ready CQEs wait at most 7.1 µs in the routine delayed-write sample.

The narrower reproducer now needs no data writes during a phase: sequential empty
NVMe flush/read pairs delay the next read by approximately 644 µs. An additional
flush restarts the window in these sequences, including with APST or volatile write cache disabled.
The driver publishes the corresponding submission tails in about 5 µs; most of
the delay follows that publication. This identifies the flush-command path as
the strongest lead without naming its internal firmware operation.

Parallel reads on four or eight NVMe queues also absorb the window. Controlled
read-before-flush timing does not establish a reliable bypass: an earlier read
can complete quickly while the following read still pays the remaining delay.
Read-only admin queries also change these timings and must stay outside measured
windows. Neither finding establishes the controller's internal execution order.

An explicitly approved firmware update from `002C` to `004C` does not prevent
the reproducer. The empty-flush/read median starts at 66 µs after the update,
then reaches approximately 675 µs after 11.7 seconds of the unchanged WAL
pressure sequence. The firmware test below records activation, pressure,
and subsequent unchanged-binary controls separately.

These observations favor an internal SSD flush/persistence mechanism over WAL
coordination or host queue saturation. They do not identify the specific
firmware/NAND operation or what switches the SSD between its fast and slow
states; exact transport attribution is also not closed. APST, CPU activity,
volatile cache, SMART interference, allocation, write pressure, and TRIM controls
are recorded below. Small optimization gains remain unqualified until fresh
controls show comparable storage latency. Neither idle nor TRIM has supplied
a reliable way to maintain that condition.

## What has been measured

Diagnostic artifacts are in `target/rfc024-stall-diagnosis/`. These are diagnostic
runs, not adoption scores: monitoring and detailed profiling have overhead.
The original comparison and rerun remain in
[rfc-024-parallel-wal-benchmark.md](rfc-024-parallel-wal-benchmark.md).

The host uses Linux 6.18.9, ext4 on `/dev/nvme0n1p3`, and a Solidigm
`SSDPFKNU010TZ` SSD behind Intel VMD. The same device holds `/` and `/home`.
Its scheduler is `none`. The CPU has 32 logical CPUs with performance and
efficiency cores. No global scheduler, affinity, mount, durability, or production WAL setting
has been changed. Explicitly approved APST experiments temporarily change its
volatile enable bit and restore the exact original bit and transition table
with verified readback after every experiment.

Eight monitored current-parallel runs record device and partition counters,
I/O pressure, dirty/writeback pages, accessible process I/O, thread scheduling
and wait channels, CPU frequencies, and temperatures at 200 ms intervals.
Accessible per-process I/O is sampled every second. The analysis uses the inner
write window to avoid opening and post-measurement flush/cleanup as far as
possible; it does not have an exact kernel timestamp for benchmark start.

A four-writer slow run reaches 3,398 puts/s versus 8,517 in a fast monitored run.
Median per-window whole-device write time rises from 0.146 to 1.570 ms. CPU
run-queue delays remain small. Accessible competing processes contribute only
a few MB of writes over the run; no device reads or discards are recorded.
Dirty-page counts remain low. The SSD composite temperature during the slow
run is 48.85–57.85 Celsius, below its exposed 76.85 Celsius maximum and
79.85 Celsius critical thresholds, with no temperature alarm. These observations
do not prove that every inaccessible process or firmware throttle is excluded.
Whole-device counters are not individual request attribution.

## Reproducer below the WAL pipeline

`direct_probe.py` performs synchronous, aligned 4 KiB `pwrite` calls with
`O_DIRECT`, and `fdatasync` after every four writes. It compares fallocated
unwritten files with files initialized by direct writes and synchronized before
timing. Each file is 128 MiB. There is no MVCC, memtable, WAL worker, or io_uring.

Three initialized-file runs deliver 11.0–11.3k writes/s with write p99
0.046–0.053 ms. Another initialized-file run drops to 2,931 writes/s with
write p99 7.919 ms. Mean sync time stays close: 0.246–0.248 ms in the fast
runs and 0.266 ms in the slow one. The slow run therefore includes delayed
direct writes, not just slow syncs or group coordination.

`reuse_probe.py` repeats measurements on the same initialized file, avoiding
new allocation between passes. Four write-plus-sync passes reach 2.8–3.1k
writes/s and write p99 5.4–7.5 ms. They repeatedly show roughly 8 ms stalls,
commonly separated by 96 writes. Direct-write passes without per-group syncs
also have occasional millisecond stalls, but fewer than 1% of writes, so p99
alone hides them. Direct-read passes are fast, with p99 0.075–0.079 ms.
Preserve the slow-operation records, not only p99.

This establishes that the WAL pipeline is not required to trigger the delay.
It does not absolve an implementation that changes I/O cadence or amplifies
storage stalls, and it does not identify a specific SSD firmware mechanism.

## Explanations tested but not established

- A fixed initialized file stays fast through 12 consecutive write/sync passes
  and six more after a 60-second cooldown. This experiment does not demonstrate
  simple cache exhaustion followed by idle recovery.
- Twelve concurrently retained initialized files all run fast despite differing
  extent layouts. This experiment does not establish address-dependent stalls.
- The same initialized file runs fast when pinned separately to each of the 32
  CPUs, tested in forward and reverse order. Efficiency cores are somewhat
  slower, but do not reproduce the millisecond latency mode.
- Pinned, unbound, and deliberately migrating submissions all run fast in a
  separate six-pass experiment. CPU migration alone is not established as the
  cause.
- Wait-channel sampling during one direct-write experiment finds
  `__iomap_dio_rw`, but that run is mostly fast. It cannot locate the recurring
  stall relative to device issue/completion. In-flight counters sampled from
  sysfs are insufficient for that distinction.

Do not label the cause as thermal throttling, SLC-cache exhaustion, garbage
collection, ext4 journaling, or VMD interrupt handling based on these results.
A direct-write syscall still includes filesystem, block-layer, driver, device,
and completion processing. Its duration alone cannot distinguish them.

## Completed privileged capture

The initial UID 1000 tracepoint and device-access restrictions were resolved
with user-authorized sudo authentication. Outside-sandbox execution alone did
not confer root. The capture changed no global permissions or runtime policies.

`capture.sh` records block issue/completion, NVMe command setup/completion,
direct-write and sync syscalls, and ext4 sync events. Six identical current
parallel-WAL four-writer rotation runs each execute 50,000 puts. Recording goes
to RAM under `/dev/shm/rfc024-kernel-trace.qLf3E9/`, avoiding trace-writer SSD
traffic. Decoded events, benchmark JSON, analysis, and trace metadata are copied
to `target/rfc024-stall-diagnosis/kernel-capture/`.

The first three runs deliver about 9,380–9,406 puts/s with commit p99
1.29–1.45 ms. The next three deliver 2,395–3,948 puts/s with commit p99
9.39–12.15 ms. These are diagnostic rates, not adoption scores.

NVMe commands are paired by controller, queue, and command identifier. The
trace renderer cannot format some kernel helper expressions, but exposes the
primitive fields used for matching; root-readable event format files validate
those fields. Eight repeated setup identifiers are ambiguous: their previous
setup records and replacement completion pairs are excluded. No lost-event
records or unmatched completions are found, and no commands remain pending at
the end. All retained completion statuses are successful. The capture contains
2,018,585 events; its decoded time span is approximately 72 seconds.

For write commands submitted in the `wal-io-worker` context:

| Diagnostic interval, seconds from first event | Matched commands | Median setup-to-completion | p99 | Commands over 5 ms |
| --- | ---: | ---: | ---: | ---: |
| Fast, [1, 15) | 120,923 | 0.020 ms | 0.282 ms | 8 |
| Slow, [20, 60) | 86,340 | 0.546 ms | 8.670 ms | 4,411 |

These intervals select visible fast and slow operating states for diagnosis;
they are not filtered optimization comparisons. `evidence.json` preserves the
window definitions and summary. Extent-initializer and journal writes also show
millisecond command lifetimes. Delayed writes occur across all 15 I/O queues,
so the observation is not limited to one worker CPU or NVMe queue.

Setup-to-completion is a driver-side lifetime, including submission and device
or interrupt/completion work. It is not a measurement of NAND service time or
hardware queue depth. Nevertheless, the long interval already exists before
userspace consumes the WAL CQE: a delayed writer wakeup alone cannot explain it.
Most delayed completions are recorded in interrupt context. This trace does not
separate controller/firmware stalls from delayed interrupt delivery through VMD.

## Health, power, and host checks

Read-only SMART and feature queries find:

- Firmware `002C`, healthy SMART status, no media/data integrity errors, no error
  log entries, and no warning or critical temperature time.
- IRQ coalescing disabled, volatile write cache enabled, and configured power
  state zero. Configured power state is not a sample of actual autonomous state.
- APST enabled with a 100 ms idle threshold for power state four. Every retained
  WAL write taking over 5 ms starts within 0.707 ms of the preceding command
  completion, with a median gap of 0.068 ms. This does not fit a simple 100 ms
  device-idle APST-entry explanation.
- Thermal management thresholds approximately 74 and 77 Celsius, above the
  observed slow-run temperatures. No exposed temperature alarm occurs.
- PCIe link at 16 GT/s, four lanes, with zero exposed endpoint Advanced Error Reporting correctable,
  nonfatal, or fatal errors.

A separate 184-second run samples the hardware SMI counter once per second
while six unchanged-binary 200,000-put runs alternate four and eight writers.
Four-writer rates are 9,855, 2,582, and 3,879 puts/s; the slow runs have commit
p99 of 29.33 and 10.62 ms. Eight-writer rates remain approximately 18.1–18.3k.
All 184 SMI samples are counted successfully and zero, including the reproduced
slowdowns. SMI handling therefore does not explain those intervals. Artifacts
are `smi-runs.jsonl` and `smi-long.log`.

These checks narrow the hypotheses; healthy SMART does not prove consistent
latency. Cache exhaustion, garbage collection, and a firmware fault remain
unproven. The burst, layout, CPU-placement, and migration experiments above did
not establish them. Firmware updates or changes to APST, IRQ affinity, mount
options, and durability are outside this investigation's changes.

An additional 100-second capture includes IRQ and softirq entry/exit events.
Its three 200,000-put four-writer samples stay fast at 9,425–9,447 puts/s,
with commit p99 1.28–1.39 ms and zero SMI counts. It does not capture a slow
interval and therefore cannot settle the remaining interrupt-delivery question.
Trace data is retained in `/dev/shm/rfc024-long-host-trace.mLBKfy/`; small
metadata and benchmark results are copied to `long-host-capture/` under the
diagnostic artifact directory. The earlier 75-second IRQ capture also stayed
fast. Neither is evidence that interrupt behavior during the slow state is
normal.

## eBPF capture of the slow state

The two earlier IRQ captures stayed fast. A subsequent eBPF investigation
actually captures the recurring slow state with IRQ probes active.
Official standalone bpftrace v0.27.0 is unpacked under the ignored diagnostic
directory; no system package or global policy is changed. Programs attach
NVMe setup/completion tracepoints and typed driver function entry/exit probes.
Histograms remain in kernel maps; only commands taking at least 5 ms and
exceptional timing observations are printed. These are diagnostic workloads,
not throughput qualifications. The WAL binary remains unchanged.

The first capture alternates four-writer rotation and eight-writer large-WAL
runs, each with 200,000 puts, and stops after two four-writer runs below
6,000 puts/s. All seven executed runs are retained:

| Run | Writers | Puts/s | Commit p99, ms |
| --- | ---: | ---: | ---: |
| 0 | 4 | 3,942 | 10.158 |
| 1 | 8 | 18,802 | 0.974 |
| 2 | 4 | 10,185 | 1.348 |
| 3 | 8 | 18,674 | 0.998 |
| 4 | 4 | 10,206 | 1.382 |
| 5 | 8 | 18,794 | 0.963 |
| 6 | 4 | 3,962 | 10.322 |

Among 20,648 recorded slow commands, completion follows the most recent
`nvme_irq` entry for that queue by a median 3 microseconds, p99 11 microseconds,
and maximum 63 microseconds. No measured NVMe IRQ handler lasts 1 ms; its
largest histogram bucket is [128, 256) microseconds. The slow-run commands
alone have median setup-to-completion times of 8.438 and 8.481 ms. These are
slow-command subset medians, not whole-run request percentiles.

The second capture adds entry/exit probes for `nvme_queue_rq`,
`nvme_queue_rqs`, and `nvme_commit_rqs`, plus sampling-cadence measurements.
It runs four-writer rotation samples until a slow run occurs, with a maximum
of six samples. Three samples deliver 10,064–10,197 puts/s with commit p99
1.156–1.244 ms. The fourth delivers 5,611 puts/s with commit p99 9.774 ms.
Its 5,499 recorded slow commands have a median lifetime of 8.513 ms.
Across all four samples:

- Neither observed submission function takes 1 ms. Both `nvme_queue_rq` and
  `nvme_queue_rqs` have their largest histogram bucket at [128, 256)
  microseconds; `nvme_commit_rqs` is attached but receives no calls.
- For 5,604 recorded slow commands, IRQ-entry-to-completion has median
  3 microseconds, p99 11 microseconds, and maximum 20 microseconds.
- No measured IRQ handler takes 1 ms.

Both programs also passively sample the current completion-queue head for
queues 1–15 from CPU 8 at 997 Hz, after learning queue pointers from typed
`nvme_irq` arguments. They compare the CQ entry status phase bit with the
queue's expected phase. This only reads coherent completion memory: it does
not consume entries, advance heads, ring doorbells, or change interrupt policy.
The status offset, 14 bytes in a 16-byte entry, is checked against the
[kernel completion layout](https://github.com/torvalds/linux/blob/v6.18/include/linux/nvme.h).
The [driver](https://github.com/torvalds/linux/blob/v6.18/drivers/nvme/host/pci.c)
uses the phase bit to recognize pending completions in its IRQ path.

In the second capture, 95,810 sampling callbacks observe 2,984 ready-head
samples. No unchanged ready head persists for the 2 ms reporting threshold.
Fifty-five callback gaps exceed 2 ms; the largest is 2.007 ms. This provides
coverage during a real slowdown, rather than assuming the nominal sample rate.
Sampling is still discrete and can miss short ready intervals; queue pointers
are unavailable before their first observed IRQ. Absence of a persistent ready
head is evidence against an 8 ms IRQ-service backlog, not proof of a specific
controller fault or of every individual CQ entry's DMA time.

Repeated setup identifiers are marked ambiguous and excluded from latency
histograms and slow-command records. The first capture counts 542 replacements
and 523 unmatched/ambiguous completions; the second counts 972 and 863.
Replacement counts need not equal completions because one identifier can be
replaced repeatedly or remain without a terminal completion at capture end.
No printed slow command has a nonzero completion status, and no event-loss
warning is emitted. Pointer-read failures are not independently counted;
nonzero ready samples validate activity but do not establish perfect sampling.

The new attribution is stronger: the observed millisecond delay is not inside
the measured driver submission calls or NVMe IRQ handler, and completed CQ
heads are not observed waiting through it. The evidence points toward delayed
completion visibility from the device/platform. It does not distinguish SSD
controller/firmware/media work from PCIe/DMA visibility effects. Claiming SLC
cache exhaustion or garbage collection would still exceed the evidence.

Reproducers, decoded slow records, full maps/histograms, stderr, benchmark JSON,
and analyses are retained under `target/rfc024-stall-diagnosis/`:
`nvme-stall.bt`, `nvme-stall-submit.bt`, `capture_bpf.sh`,
`capture_bpf_submit.sh`, `bpf_runs.py`, `bpf_submit_runs.py`, `analyze_bpf.py`,
`bpf-capture/`, and `bpf-submit-capture/`. These captures preserve all executed
samples; stopping on an observed slow state is for diagnosis, not an adoption
sampling rule.

## Command flags and flush-cadence counterexamples

A follow-up eBPF capture records NVMe read/write control bits and issuing PIDs.
The direct-write workload overwrites one fully initialized 128 MiB file, using
4 KiB synchronous `O_DIRECT` writes. It varies only the number of writes between
`fdatasync` calls; zero means no intermediate sync, with a final sync outside
the timed window. All eight executed samples are retained:

| Run | Writes per sync | Writes/s | Write p99, ms | Mean sync, ms |
| --- | ---: | ---: | ---: | ---: |
| 0 | 4 | 3,008 | 5.385 | 0.271 |
| 1 | 0 | 20,708 | 0.037 | — |
| 2 | 16 | 9,509 | 0.642 | 0.621 |
| 3 | 1 | 1,688 | 7.523 | 0.259 |
| 4 | 64 | 40,500 | 0.035 | 0.249 |
| 5 | 4 | 10,729 | 0.052 | 0.250 |
| 6 | 0 | 82,744 | 0.016 | — |
| 7 | 4 | 10,615 | 0.051 | 0.250 |

The traced workload writes have NVMe control value zero in both fast and slow
states; the FUA bit does not change. This rejects a change in write durability
flags as the explanation for these samples. Slow commands also occur without
intermediate syncs: run 1 has 88 commands taking at least 5 ms, despite write
p99 below 0.04 ms. Again, fewer than 1% of slow operations can evade p99.

The exact sync-every-four workload is slow in run 0 and fast in runs 5 and 7,
on the same file and device. Flush frequency alone is insufficient to explain
the operating-state change. Since the cadence sequence is not randomized or
repeated in reverse order, this experiment does not establish that changing
cadence causes recovery. No WAL rotation, fresh extent allocation, MVCC, or
io_uring is required for the reproduced slow state.

A read-only PCIe capability inspection finds the NVMe endpoint in D0 with
ASPM disabled and all L1 substates disabled. Its link remains 16 GT/s, x4,
with no exposed error-status bits. A vendor-specific additional SMART Get Log
request, page `0xca`, returns status `0x109` (unsupported log page), so it
provides no NAND/cache/performance-state telemetry. That diagnostic rejection
may appear in the error information log and should not be classified as a new
media or workload I/O failure.

Artifacts are `flush_probe.py`, `nvme-flush.bt`, `capture_flush.sh`, and
`flush-capture/` under the diagnostic directory.

## APST intervention and restoration

After explicit approval, each root guard snapshots the APST enable bit and all
32 transition entries, uses volatile Set Features only, runs workloads as the
ordinary user, and restores the original configuration. Enable-bit readback and
byte-for-byte table comparison confirm exact restoration in every experiment.
Signal handlers also preserve restoration on interruption. No firmware or
persistent power configuration changes are made.

The first direct-write comparison alternates on/off/off/on/on/off. All six
samples are fast, at 11,011–11,717 writes/s, so it is inconclusive. A six-sample
four-writer engine comparison produces five fast samples at 9,866–9,945 puts/s
and one APST-on sample at 3,706 puts/s with p99 9.978 ms. That correlation alone
cannot establish causation.

A further experiment disables APST continuously without reprogramming it between
samples. The first engine sample reaches 9,950 puts/s; the next drops to 3,839
with p99 10.099 ms. The guard then stops the diagnostic on the reproduced slow
state and restores the original settings. APST is therefore not necessary for
the recurring stall. Artifacts are the `apst*_guard.c` and `apst*_probe.py`
scripts, `apst-capture/`, `apst-engine-capture/`, `apst-off-capture/`, and
`apst-summary.json`.

## Controlled write pressure and same-file recovery

`pressure_probe.py` initializes a 64 MiB reference file once and reuses it for
all probes: 16,384 aligned 4 KiB direct writes, syncing every four writes. A
separate 96 GiB fallocated scratch file receives sequential 1 MiB direct writes
in 8 GiB chunks, with a sync after each chunk. The reference probe runs between
chunks. This is ordinary file I/O, with no device setting changes, explicit
TRIM, cache clearing, or writes to existing user files.

| Completed pressure, GiB | Chunk throughput, MiB/s | Reference writes/s | Reference write p99, ms |
| --- | ---: | ---: | ---: |
| 0 | — | 11,867; repeat 10,900 | 0.044; 0.046 |
| 8 | 2,413 | 11,616 | 0.044 |
| 16 | 2,376 | 11,366 | 0.044 |
| 24 | 2,410 | 11,381 | 0.050 |
| 32 | 168 | 11,436 | 0.044 |
| 40 | 185 | 3,022 | 8.200 |
| 48 | 181 | 3,135 | 4.842 |
| 56 | 151 | 3,082 | 7.713 |
| 64 | 145 | 8,528 | 0.642 |

The large-write throughput cliff begins between 24 and 32 GiB. The small-write
reference subsequently reproduces the recurring millisecond stall without the
WAL. The 64 GiB reference sample partly recovers; the behavior is not a monotonic
function of total bytes written, and this sample is retained rather than filtered.

After the 64 GiB chunk and a partial next chunk, the diagnostic is deliberately
interrupted to avoid further pressure and measure recovery. Both scratch inodes
are retained through hard links before the script removes its original names.
The completed chunks are known; the exact partial-chunk byte count is not.
The experiment does not claim that all 96 GiB were written. All executed samples
and the intentional stop are recorded in `pressure-capture/runs.jsonl` and
`pressure-stop.json`.

With both allocations retained, the immediate reference probe reaches 3,056
writes/s, p99 7.775 ms, at 51.85 Celsius. After 60 seconds idle it remains at
3,069, p99 7.676 ms, at 49.85 Celsius; a repeat reaches 2,879. A second recovery
experiment holds APST disabled throughout 120 seconds idle. Reference throughput
stays at 3,020–3,027 with p99 8.119–8.198 ms and temperature 47.85–48.85 Celsius.
The APST guard restores and verifies the original configuration afterward.

Temperature alone does not explain these samples: the reference is fast at
63.85 Celsius after 32 GiB, yet slow after cooling to 47.85–51.85 Celsius.
Short idle, including idle without autonomous power saving, does not reset the
observed state. The pressure sequence reproduces the original stall, but does not establish
write volume as its unique cause. It does not separate internal media management
from other firmware or platform effects, and is not a randomized repeated
intervention trial.

## Vendor cache telemetry leaves cache policy unresolved

Solidigm documents a read-only `show --performancebooster` query and a separate
command to start cache flushing. See the
[maintenance documentation](https://www.solidigm.com/support-page/maintenance-tools/ka-00057.html)
and [SST CLI guide](https://sdmsdfwdriver.blob.core.windows.net/files/kba-gcc/drivers-downloads/ka-00085/sst--3-1/solidigm-cli-storage-tool-user-guide-727329-019us.pdf).

The official SST 3.1.346 Debian package is extracted into the ignored diagnostic
directory, without installation. Its absolute library path is supplied through
a private mount namespace using the extracted libraries; host mounts are
unchanged. A library-path shim attempt fails before the working namespace query.
The initial read-only query uses:

```text
sst show --output json --ssd /dev/nvme0n1 --performancebooster
```

The drive reports **89% SLC buffer available**, eviction completion 100%, flush
elapsed time zero, and zero host initialize/cancel operations. The last three
same-file probes immediately afterward remain slow at 2,004, 2,043, and 1,961
writes/s, with p99 8.195, 8.029, and 8.660 ms. Temperature is 50.85–53.85 Celsius.

Consequently, the reported percentage does not support saying the SLC buffer is
simply full during this slow state. It also does not establish whether these
writes actually use SLC, how available space is eligible for writes, or how fresh
the metric is. The preceding sequential-write cliff and slow reference are
compatible with internal cache/media-management effects, but that mechanism
remains an inference. After separate explicit approval for this device-wide intervention, the vendor
`start --performancebooster` command reports success and the host-initialize
counter increments to one. This acknowledges initiation, not completion. All
24 read-only polls at five-second intervals show eviction completion zero,
available buffer 89%, and flush elapsed time zero. After the bounded two-minute
poll, `stop --performancebooster` reports success. The host-cancel counter
increments to one and eviction completion returns to 100%, with elapsed time
still zero. The operation is cancelled rather than established as a successful
cache eviction; the final 100% status must not be used as proof of a reset.
Consequently, this intervention cannot validate or reject an effect of a
successfully completed cache flush. Three same-inode probes after cancellation
reach 1,629, 1,522, and 1,444 writes/s, with write p99 14.570, 15.033, and
15.477 ms. They do not recover. The before/after comparison is confounded by
the uncompleted intervention and changing device state; it does not establish
that the vendor command causes this further deterioration.

After the comparison, the diagnostic's own 96 GiB pressure file is removed;
the same 64 MiB reference remains. The first cleanup probe is still slow at
2,509 writes/s, p99 12.128 ms. The next two recover to 11,334 and 11,658,
both with p99 0.043 ms, at 47.85–48.85 Celsius. The host ext4 mount uses
`rw,relatime`, without the `discard` option, and no explicit discard is issued.
A further vendor query reports 90% buffer available, elapsed flush time zero,
and unchanged initialize/cancel counters. Thus the slow-to-fast reversal occurs
without a reported successful vendor flush and without allocating a new
reference inode. A single recovery following deletion cannot attribute it to
deleting the file rather than elapsed time or changing controller state.

An allocation-only control then repeats the same reference probe around
fallocating a separate unwritten 96 GiB file. There are no large data writes:
before allocation it reaches 12,162 writes/s; with the allocation retained,
11,647 and 10,410, all with p99 0.040–0.043 ms. After removing that file, the
next probe drops to 3,036 with p99 5.772 ms. This rejects a deterministic
explanation based solely on filesystem free space and shows that large data
write volume is not required to observe a transition. It does not prove that
unlink causes the stall: metadata I/O, timing, and device state are not separated
in this sequence. Three immediate follow-up probes stay slow at 3,014–3,054 writes/s with p99
7.775–7.819 ms. All seven samples are retained in the same JSONL log;
`allocation_control_probe.py` cleans up its own large file in a `finally` block.
`allocation_followup_probe.py` records the subsequent samples. Both 96 GiB
scratch allocations are removed; only the 64 MiB reference remains.

Artifacts are `sst-tools/cache-status.json`, `cache-flush-start.log`,
`cache-flush-poll.log`, `cache-flush-stop.log`, the extracted official package,
`show-cache.sh`, `start-cache.sh`, `poll-cache.sh`, `stop-cache.sh`,
`vendor_cache_probe.py`, `vendor_cache_cancel_probe.py`,
`pressure_cleanup_probe.py`, `pressure-capture/cleanup.json`, and
`sst-tools/cache-after-cleanup.json`. No vendor package
is installed globally and no firmware update is performed.

## Remaining intervention: filesystem discard

Read-only systemd inspection shows `fstrim.timer` enabled, with the last
successful run on September 28, 2026, from 00:43:02 to 00:48:15 CST; the next
run is October 5. The SSD supports discard, but ext4 has no continuous discard
mount option. Thus removing scratch files frees filesystem space without
necessarily informing the SSD immediately. Benchmark churn since the last
scheduled trim could leave stale mappings and internal reclamation pressure.
This is a hypothesis, not a measured stale-mapping count.

After separate explicit approval, `fstrim -v /home` completes successfully
from 02:10:14 to 02:12:09 CST on October 1. It reports 137,764,712,448 bytes
(128.3 GiB) trimmed. Whole-device counters independently increase by 56,507
discard commands and exactly the reported byte count. Allocated reference
file data is preserved. No continuous-discard setting or timer changes are made.

| Probe phase | Samples | Writes/s | Write p99, ms |
| --- | ---: | --- | --- |
| Immediately before TRIM | 3 | 2,954; 3,037; 3,076 | 5.795; 7.856; 7.758 |
| Immediately after TRIM | 6 | 2,894; 3,051; 3,360; 3,375; 3,330; 3,364 | 7.096; 2.885; 0.649; 0.649; 0.654; 0.654 |

TRIM does not immediately restore the 11–12k writes/s fast state. The apparent
p99 improvement does not mean all stalls disappear: the last four probes each
have 134 of 16,384 writes taking at least 1 ms, below the 1% percentile threshold.
Mean direct-write time remains 0.232–0.235 ms versus roughly 0.024 ms in earlier
fast probes; mean sync time remains 0.256–0.258 ms. Post-TRIM vendor telemetry
still reports 89% SLC buffer available and unchanged cache-flush counters.

After another 120 seconds idle, the first probe remains at 3,362 writes/s,
p99 0.664 ms. The next two recover to 11,389 and 11,540, with p99 0.044 and
0.043 ms, all at 47.85 Celsius. This contrasts with the earlier idle control
that stayed slow, but recovery also occurred before TRIM during scratch cleanup.
Therefore TRIM plus settling is a possible mitigation, not proof that stale
mappings uniquely cause the instability. There is no randomized TRIM/sham
comparison, and the intervention cannot restore the previous mapping state.

Repeating the allocation-only control after TRIM produces four fast samples:
11,552 before allocation; 11,371 and 11,350 with the unwritten 96 GiB file
retained; 11,370 after removing it. Write p99 is 0.045–0.050 ms throughout.
This contrasts with the pre-TRIM allocation control, but one before/after
sequence does not prove a general cure or isolate the firmware mechanism.
`trim_allocation_control_probe.py` removes its own scratch file afterward.

Three unchanged parallel WAL engine samples then use four writers, 200,000
puts, 1,024-byte values, 1 MiB target SST size, and latency sampling every ten
puts, matching the earlier APST engine diagnostic. All three stay fast:
9,917, 9,851, and 9,890 puts/s, with commit p99 1.255, 1.417, and 1.364 ms.
The runtime binary is unchanged at `925ed07d`, with the previously recorded
SHA-256. APST uses its restored original configuration. These are three
post-intervention diagnostic samples, not an interleaved baseline/candidate
comparison or an adoption score. Artifacts are `trim_engine_probe.py` and
`trim-engine-capture/runs.jsonl`.

Artifacts are `trim_compare.sh`, `trim_before_probe.py`, `trim_after_probe.py`,
`trim_idle_probe.py`, `trim-result.txt`, start/end timestamps, before/after device
counters, `trim-summary.json`, `sst-tools/cache-after-trim.json`, and the same
`pressure-capture/runs.jsonl` containing every probe.

## Post-TRIM recovery does not persist in the comparison rerun

The following 4/8/16-writer comparison reuses the unchanged binaries and retains
all completed samples. Four-writer same-binary throughput ratios are 2.540,
0.409, and 0.999; sixteen-writer ratios are 2.249, 0.993, and 0.997. Thus the
previous recovery after TRIM does not establish a stable environment. At the
user's direction, the comparison stops after 68 of 90 completed samples.
Its results cannot qualify optimization gains. See the
[benchmark report](rfc-024-parallel-wal-benchmark.md) and
`target/rfc024-post-trim-comparison/variability-summary.json`.

## SMART polling introduces a separate, smaller stall

A temperature read through NVMe hwmon is an active SMART Get Log Page command,
as shown by [the Linux hwmon implementation](https://github.com/torvalds/linux/blob/v6.18/drivers/nvme/host/hwmon.c).
The new admin-queue trace includes queue zero, which the earlier eBPF probes
filtered out. Six ten-second phases alternate temperature polling off/on/on/off/off/on
while the same initialized 64 MiB scratch file receives 4 KiB direct writes and
one `fdatasync` per four writes.

| Phase | Temperature polling | Median writes/s | Writes taking at least 1 ms | Overlapping an admin command |
| --- | --- | ---: | ---: | ---: |
| 0 | Off | 10,847 | 2 | 0 |
| 1 | On | 9,018 | 90 | 89 |
| 2 | On | 8,996 | 85 | 83 |
| 3 | Off | 10,590 | 1 | 0 |
| 4 | Off | 10,687 | 2 | 0 |
| 5 | On | 8,919 | 86 | 85 |

Admin commands have median latency 23.901 ms and p99 24.169 ms. Polling causes
repeatable interference, roughly a 16–18% throughput reduction in this experiment.
The trace identifies no external admin-command issuer. There is one ambiguous
request replacement. This is a monitoring confounder, not the explanation for
the original multi-fold throughput swings: the earlier full `perf` capture
includes all NVMe queues and contains no queue-zero commands, and the subsequent
slow-state capture below also contains none. Avoid active SMART polling during
performance qualification. Artifacts are `admin_probe.py`, `nvme-admin.bt`,
`admin-capture/`, and `analyze_admin.py`.

## Reads also stall during the actual WAL slow state

Eight unchanged parallel-WAL samples use four writers, 50,000 puts, 1,024-byte
values, and 1 MiB rotation. Alternate samples add an independent 4 KiB direct-read
probe of the retained initialized file, without temperature polling. The full
admin-queue trace records zero admin commands in every sample.

| Phase | Read probe | Engine puts/s | Commit p99, ms | Read command p99, ms | Write commands taking at least 5 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 0 | Off | 9,568 | 1.262 | — | 4 |
| 1 | On | 9,298 | 1.442 | 0.795 | 14 |
| 2 | On | 9,447 | 1.163 | 0.721 | 6 |
| 3 | Off | 9,533 | 1.279 | — | 7 |
| 4 | Off | 2,632 | 13.597 | — | 3,186 |
| 5 | On | 2,246 | 12.432 | 18.334 | 3,606 |
| 6 | On | 4,019 | 9.629 | 11.129 | 1,525 |
| 7 | Off | 9,476 | 1.451 | — | 7 |

This reproduces the original slow mode without SMART polling. Independent reads
also slow during it; the earlier separately timed fast read tests did not
establish that reads remain fast during stalled WAL writes. Adding reads does
not reliably prevent the transition. All traced command statuses are successful.
Artifacts are `read_intervention_probe.py`, `nvme-read-write.bt`,
`read-intervention-capture/`, and `analyze_read_intervention.py`.

## Keeping a spare CPU core busy does not prevent the stall

A single 700,000-put parallel-WAL run has an independent direct-read probe and
alternates five-second intervals with and without a busy loop pinned to CPU 0.
The engine and reader use CPUs 2–31 throughout, avoiding that core's sibling.
These are temporary per-process affinity settings. Trace output goes to `/tmp`
(tmpfs) and is copied to the artifact directory after tracing ends.

Intervals 0–12 have read command p99 0.660–0.976 ms. The slow state starts
in interval 13 while the spare core is busy, with read p99 42.654 ms. It persists
across subsequent on/off intervals, with p99 10.344–25.053 ms through interval 18,
and recovers around interval 20. All intervals have zero admin commands.
The busy-loop intervention does not prevent the slow state. The package MSR
sampler only overlaps the earlier fast intervals and cannot supply residency
measurements for the slow intervals.

The controller-reported SQ head sometimes passes a submitted read's position
well before that request completes. This is evidence against missed submission
notification for those examples, but SQ progress alone cannot distinguish
completion of that particular command from another command. The command-specific
completion split below addresses that distinction. The first queue-progress
capture missed submission positions because of its thread-name filter; it
cannot support this position-based conclusion. Artifacts are
`cpu_awake_probe.py`, `nvme-progress.bt`, `cpu-awake-capture/`, and
`analyze_cpu_awake.py`.

## Matching CQE consumption to request completion

A fresh 700,000-put run records the command-specific boundary between CQE
consumption and Linux request completion. The probe observes
`blk_mq_complete_request_remote()` for the matching NVMe request and associates
the immediately preceding `nvme_sq` event on the same CPU. In
[the Linux NVMe completion path](https://github.com/torvalds/linux/blob/v6.18/drivers/nvme/host/pci.c),
the CQE is consumed and `nvme_sq` fires before
[`nvme_try_complete_req()` invokes this function](https://github.com/torvalds/linux/blob/v6.18/drivers/nvme/host/nvme.h).
The subsequent `nvme_complete_rq` event supplies the final timestamp. Matching
uses queue and command IDs; it excludes ambiguous reused IDs.

| Span, for commands taking at least 5 ms overall | Median | p99 | Maximum |
| --- | ---: | ---: | ---: |
| Command setup to CQE consumption | 7.891 ms | 56.824 ms | 260.454 ms |
| CQE consumption to request completion | 14 µs | 32 µs | 277 µs |

There are 13,504 such stalled commands, and none spends 1 ms after CQE consumption.
Thus deferred kernel completion or a remote CPU completing the request does not
explain this reproduced slow mode. The long wait occurs before Linux consumes
that command's CQE. Combined with the earlier passive-CQ and short-submit
observations, this narrows attribution toward the device/platform completion
path. It still does not identify a specific firmware operation, NAND maintenance
step, or DMA mechanism.

The capture matches 985,017 commands. There are 759 missing/ambiguous completions
and 825 replacement events; every matched CQE is correlated with `nvme_sq`.
No traced command reports an error. An earlier instrumentation attempt produced
no matching timestamps and is excluded. Artifacts are
`nvme-completion-split.bt`, `completion-split-v2-capture/`, and
`analyze_completion_split.py`.

## Write protocol and cache controls

`RWF_DSYNC` on the initialized scratch file produces NVMe writes with the FUA bit
(`0x4000`); ordinary writes have control zero. All four ordinary-write phases
remain fast, 10.5–11.2k writes/s, while FUA phases reach about 4.36k writes/s.
The original slow mode is absent, so this verifies the protocol intervention
but cannot establish FUA as a cure. A separate attempt to trigger the slow
reference-file mode before interleaving write patterns stays fast through ten
engine/probe attempts; that experiment is likewise inconclusive. Artifacts are
`fua-capture/` and `slow-protocol-capture/`.

Read-only feature inspection confirms that volatile write cache and HMB are
enabled. HMB reports 19,968 pages and 20 descriptors. The Solidigm tool's
`show --backgroundprocessing` command returns `Feature is not implemented` on
this device, so it supplies no internal maintenance-state evidence.

## Approved volatile-cache intervention and its limit

The user approves a temporary device-wide volatile-cache test. The guard flushes
the device before changing FID `0x06`, never sets the persistent Save bit, verifies
each value, runs the benchmark as the ordinary user, bounds child lifetime, and
restores the exact original value on exit. The first sequence completes all six
50,000-put phases:

| Phase | Volatile cache | Puts/s | Commit p99, ms |
| --- | --- | ---: | ---: |
| 0 | Enabled | 9,080 | 1.446 |
| 1 | Disabled | 3,542 | 3.514 |
| 2 | Disabled | 3,522 | 4.169 |
| 3 | Enabled | 9,109 | 1.357 |
| 4 | Enabled | 9,099 | 1.363 |
| 5 | Disabled | 3,522 | 3.918 |

Disabling the cache reduces throughput; it is not a proposed mitigation. The
cache-enabled controls stay fast, so these short phases cannot decide the
original slow-state trigger. They do establish a repeatable cache-setting effect.
The guard reports successful final device flush and exact restoration to enabled.
Artifacts are `vwc_guard.c`, `vwc_probe.py`, and `vwc-capture/`.

A second sequence extends each phase to 200,000 puts within the same 90-second
child bound. The enabled phase completes at 9,344 puts/s, p99 1.252 ms. The first
disabled phase times out at 85 seconds, so no throughput result is fabricated for
it and the remaining phases do not run. The guard restores enabled cache and
verifies it. The timed-out phase's command trace remains diagnostic evidence:

| Issuing thread / request size | Commands taking at least 5 ms | Median setup-to-CQE | p99 setup-to-CQE | Maximum after CQE |
| --- | ---: | ---: | ---: | ---: |
| WAL worker / 4 KiB | 2,075 | 8.787 ms | 47.526 ms | 21 µs |
| Extent initializer / 128 KiB | 219 | 8.609 ms | 26.202 ms | 21 µs |

These delays affect small WAL writes as well as initialization, with cache
verified disabled. Larger `tokio-rt-worker` writes in this trace belong to the
same benchmark process; they are not an unidentified external workload.
The incomplete run does not supply a paired cache-on/cache-off comparison of
the original slow mode. It does show that cache disabling does not eliminate
millisecond waits before hardware completion. Artifacts are `vwc-long-capture/`
and `incomplete-phase-summary.json` within it.

The Solidigm `dump --nlog` command subsequently returns `Device does not support
this command set`. Together with unavailable background-processing telemetry,
this prevents using these vendor commands to name an internal firmware or NAND
maintenance operation. No controller reset, firmware update, or production
configuration change is performed. A final reference-file check after cache
restoration reaches 10,523 writes/s with write p99 0.048 ms and no 1 ms write
stalls; it does not reproduce the slow mode and therefore does not proceed to
a write-pattern comparison. Artifacts are `post-cache-protocol-capture/`.

## Stalls with the engine paused and unchanged LBAs

A new trace records command LBAs, sizes, issuing threads, setup timestamps, and
CQE consumption. During a 700,000-put run, a low-rate direct-read probe detects
the slow state. The controller and filesystem settings remain unchanged; there
is no SMART polling. The experiment pauses the benchmark process three times,
quiesces the companion reader, waits 300 ms, and interleaves scratch-file write
patterns with sequential-write controls on the retained, initialized 64 MiB file.
It resumes the benchmark after each sequence.

| Operation while the engine is paused | Operations/s across three sequences | Write stalls of at least 5 ms |
| --- | ---: | ---: |
| Sequential 4 KiB writes, sync every four | 2,386–3,124 | 10–12 per 1,024 writes |
| Repeatedly overwrite the same 4 KiB block, sync every four | 2,594–2,985 | 11–12 per 1,024 writes |
| Random 4 KiB writes, sync every four | 2,601–2,982 | 10–11 per 1,024 writes |
| Sequential 4 KiB writes, no intermediate sync | 16,896–19,528 | 10–11 per 4,096 writes |
| 4 KiB FUA writes | 1,363–1,801 | 42–45 per 1,024 writes |
| Sequential direct reads | 19,587–26,606 | None in 4,096 reads |

Every read-only phase has a slow sequential-write control immediately before
and after it. Thus concurrent engine traffic is not necessary to sustain the
write stall, and read latency on this file is not intrinsically slow. This is
consistent with the earlier simultaneous-read test: reads can stall when
submitted while the device is processing writes. Repeated overwrites of one
block also reject an explanation requiring a large active LBA working set or
repeated extent allocation. They do not distinguish data programming from FTL
metadata work.

The trace matches 1,053,804 commands, excluding 1,152 missing/ambiguous completions
and recording 1,246 replacement events. There are no status errors, admin
commands, or discards in the completed capture. No matched command issued by
the paused benchmark falls inside any scratch-probe phase. Slow writes span
many LBA regions and include WAL, extent initialization, SST, and journal I/O;
the companion read uses exactly the same LBA in fast and slow intervals.

The capture contains about 24.9 GiB of device writes. Approximately 17.62 GiB
comes from the benchmark's SST worker, 3.41 GiB from its extent initializer,
and 2.66 GiB from its WAL worker. Ownership is checked by issuing process ID,
not just thread name. This makes the workload's substantial storage history
visible; it does not prove that any one of these sources uniquely triggers
the state transition. The deliberately paused benchmark's throughput is not
a valid optimization comparison.

The first attempt stops when the user quota on `/tmp` interrupts capture after
two completed probe phases; its partial artifacts remain preserved. The
completed retry compresses the trace as it streams to RAM: 261.5 MB raw becomes
45.4 MB. Diagnostic output is not written to the tested SSD during the run.
Artifacts are `lba-pause-partial-capture/`, `lba-pause-capture/`,
`nvme-completion-lba.bt`, `lba_pause_probe.py`, and `analyze_lba_pause.py`.

## Persistence cadence explains the throughput gap

A subsequent standalone sweep varies only write size and synchronization
cadence on the same initialized file. It pins the issuing thread to CPU 20,
uses synchronous `O_DIRECT` writes, and runs no engine. Each treatment is
followed by a 4 KiB, sync-every-four control. All controls through the first
64 KiB write treatment remain slow, about 2.55–3.00k writes/s. The sweep writes
848 MiB in total and matches all 139,305 traced commands, with no reported
missing/replaced matches, errors, admin commands, or discards.

| Write pattern in the confirmed slow state | Median spacing between write stalls of at least 5 ms |
| --- | ---: |
| 4 KiB, sync every write | 24 writes |
| 4 KiB, sync every two | 48 writes |
| 4 KiB, sync every four | 96 writes |
| 4 KiB, sync every eight | 192 writes |
| 4 KiB, no intermediate sync, sequential or fixed block | 378 writes |
| 16 KiB, no intermediate sync | 94.5 writes |
| 64 KiB, no intermediate sync | 24 writes |
| 16 KiB, sync every write | 24 writes |

Without intermediate sync, the spacing follows approximately 1.5 MiB of
submitted data, including when repeatedly overwriting one block. With small
synchronized writes, it follows approximately 24 persistence cycles. The
separate paused-engine test likewise sees 24-write spacing with FUA. This
supports a persistence-granularity explanation rather than a fixed wall-clock
timer or a particular bad LBA. It is not a measurement of physical NAND page
size, internal write amplification, or a named garbage-collection operation.

The delay can move between command types. At sync-every-16, no write reaches
5 ms, but 12 NVMe flushes do and flush p99 reaches 8.437 ms. At sync-every-32
and sync-every-64, both writes and flushes stall. Looking only at write p99
would therefore misclassify some slow treatments as recovered. The 64 KiB,
sync-every-write phase contains 27 slow flushes and then transitions to fast
controls; the second sweep cycle stays fast. This single transition does not
establish that the larger synchronized writes cause recovery.

For the two cycle-opening, sync-every-four controls, matched command positions
show where the throughput gap comes from:

| Setup-to-CQE latency, median | Slow control | Fast control |
| --- | ---: | ---: |
| First write in each four-write block, immediately after the previous sync | 610 µs | 26 µs |
| Second write | 11 µs | 12.5 µs |
| Third write | 11 µs | 12 µs |
| Fourth write | 10 µs | 12 µs |

These controls reach 2,957 and 9,182 writes/s. Their difference is about 917 µs
per four-write block. The first command's mean setup-to-CQE latency, including
the periodic long stalls, adds about 883 µs per block, accounting for approximately
96% of that difference. This is an attribution for these diagnostic controls,
not a claim that the same percentage applies to every WAL workload. The longer
stalls and the routine roughly 610 µs post-flush delay both matter.

Reconstructing outstanding commands from every matched setup/CQE pair finds
that 550 of 553 long writes spend their entire wait with exactly one host-visible
NVMe command outstanding and no other completion. The remaining three overlap
other I/O. Thus the reproduced stall does not require host queue saturation.
These are outstanding NVMe commands, not a measurement of the controller's
internal queue depth. The long writes' CQE-to-request-completion interval
remains only a few microseconds.

Artifacts are `persistence-stride-capture/`, `persistence_stride_probe.py`,
`analyze_persistence_stride.py`, and its `modulo-summary.json`,
`decomposition.json`, and `depth-summary.json`. No production code or
durability setting changes result from the sweep.

## A fixed post-flush service window

`post_flush_gap_probe.py` overwrites only the first 16 KiB of the existing
64 MiB reference, with four synchronous 4 KiB direct writes and then `fdatasync`.
It inserts a controlled gap or direct read after each sync. The main writer is
pinned to CPU 20. The first four-cycle sweep remains fast; all 65,122 traced
commands match, and its controls have first-write setup-to-CQE medians of
21–28 µs. That capture cannot establish a slow-state treatment effect.

A second capture runs the unchanged four-writer parallel-WAL rotation binary
until the reference read detector observes stalls. At 66.5 seconds it pauses
only that engine process, waits 300 ms, and confirms a slow reference control.
The subsequent four randomized treatment cycles have slow controls before and
after every treatment. No engine command starts inside these scratch-probe
phases. The benchmark is intentionally terminated afterward; its incomplete
output is not a throughput result.

The medians below aggregate the per-phase medians across these four cycles.
All gaps are extra work between a completed sync and the next write.

| Work inserted after sync | Next write setup-to-CQE median | Inserted read setup-to-CQE median |
| --- | ---: | ---: |
| None, controls | 612 µs | — |
| Busy wait, 50 µs | 561 µs | — |
| Busy wait, 100 µs | 510 µs | — |
| Busy wait, 250 µs | 359 µs | — |
| Busy wait, 500 µs | 110 µs | — |
| Busy wait, 1,000 µs | 26 µs | — |
| One direct read of the recently written block | 14 µs | 651 µs |

For 7,568 routine first writes with setup-to-CQE time below 2 ms and inserted
gaps of at most 500 µs, the preceding flush CQE to write CQE interval has a
641.061 µs median. Regressing write latency against the actual flush-CQE to
write-setup gap gives a slope of -0.996 and intercept of 640.777 µs, with residual
standard deviation 11.629 µs. This describes the routine window; periodic longer
stalls are excluded from that regression and remain in all saved samples.

The nearly one-for-one subtraction shows that the routine wait elapses while
the host delays submission. The next write does not need to initiate all of
that work. Inserting a read similarly moves the wait into the read. Neither
treatment supplies a reliable end-to-end speedup, and an apparently recovered
write percentile alone would be misleading. This is a latency observation,
not evidence that the flush durability contract is violated.

An additional two-cycle capture reads the untouched last 4 KiB of the reference,
including submissions from CPU 10 on a different NVMe queue. During slow
controls, these far reads have setup-to-CQE medians of 651 µs on CPU 20 and
711–714 µs on CPU 10; the following writes are fast. The delay therefore also
affects reads outside the just-written range and across queues. The second
cycle transitions to fast during its 500 µs treatment; preserve that transition
and do not attribute recovery to the treatment. Later treatments without slow
controls cannot be scored as cures.

The triggered capture has 882,170 matched commands, 1,043 missing/ambiguous
completions, and 1,154 replaced identifiers. The far-read capture has 29,741
matched commands without matching exclusions. Every scratch write, read, and
sync used in the phase analysis has a matching command; matched statuses are
successful, with no admin commands, discards, or reported event loss. All trace
output is compressed in RAM and copied after the measurements. Artifacts are
`post-flush-gap-capture/`, `post-flush-gap-triggered-capture/`,
`post-flush-far-read-capture/`, `analyze_post_flush_gap.py`, and
`routine-deadline.json`.

## Completion visibility during the service window

`nvme-post-flush-ready.bt` combines command matching with passive reads of the
host completion-queue head. A profiler callback on CPU 8 samples at 9,997 Hz;
769,744 of 770,714 observed callback gaps fall in the 64–128 µs histogram bin.
The sampler reads queue head, phase, CQE status, and command identifier, then
checks that the head and phase stayed unchanged. It never advances a head or
rings a doorbell. The dedicated probe submits one synchronous request at a time.

With the same bounded trigger and engine pause, the routine post-flush window
remains reproducible. The eight no-gap controls have first-write setup-to-CQE
medians of 608–614 µs and routine flush-to-next-write-CQE medians of 640.8–641.2 µs.
Far reads and the gap treatments reproduce the previous pattern. These heavily
instrumented runs measure attribution, not an optimization score.

Of 1,104 routine delayed writes with setup-to-CQE durations of 500–1,000 µs,
1,099 have at least three stable samples and no unknown queue, head/phase race,
or unrelated ready entry. They have six samples per command at the median,
including 6,615 not-ready samples. The last not-ready sample precedes consumed
CQE time by 50.6 µs at the median and at most 107.1 µs. Ninety-five ready samples
precede consumption by at most 7.073 µs. No matching CQE is observed ready more
than 100 µs before consumption.

All 96 sampled write stalls of at least 5 ms meet the same eligibility checks,
with 78.5 samples per command at the median. Their four observed ready entries
precede consumption by at most 3.780 µs. Another 801 eligible delayed reads show
the same behavior. These samples exclude a long wait for interrupt handling
after a visible CQE in the reproduced routine and long stalls. They locate the
wait before host-visible completion, while controller/media work versus an
upstream PCIe/DMA visibility delay still requires additional attribution.

The complete capture has 904,020 matched commands, 1,618 missing/ambiguous
completions, and 2,388 replaced identifiers, with successful matched statuses and
no admin commands, discards, or reported event loss. All scratch operations used
in the phase analysis match. The sampling matcher pairs 13,116 of 13,117 command
records; the unpaired record is excluded. Runtime error reporting records 52
map-lookup warnings around sampling/completion races and no probe-read warnings;
eligibility excludes the recorded race categories. The warnings are retained
alongside the capture.

The first invocation used an unsupported `-kk` flag and started no workload.
A subsequent partial invocation filled the diagnostic RAM log quota with
expected absent-map warnings; it was stopped and retained separately, with no
CQ timing conclusion. The successful invocation initializes sampling state,
uses supported `-k` reporting, and compresses stdout and stderr independently.
Verified duplicate RAM artifacts are removed after archival. Artifacts are
`post-flush-cq-unsupported-kk-capture/`, `post-flush-cq-quota-partial-capture/`,
`post-flush-cq-sample-capture/`, `cq-window-samples.bt`,
`analyze_cq_window.py`, and `cq-summary.json`.

## Empty flushes reproduce and restart the window

`flush_policy_probe.py` starts with an already slow control, so no engine is
launched. It uses the same initialized reference and CPU 20, overwrites only
the first 16 KiB, and reads the untouched last 4 KiB. Namespace passthrough
commands are exclusively opcode-zero `FLUSH` for namespace 1; there are no raw
device writes. Three randomized treatment cycles have 33 four-write/sync
controls, whose first-write setup-to-CQE medians remain 605–609 µs throughout.
The complete sweep writes about 163 MiB through the scratch file.

The table aggregates per-phase medians across those three cycles. Times refer
to individual command setup to consumed CQE, rather than syscall time.

| Command sequence in the slow state | Relevant flush median | Following read median |
| --- | ---: | ---: |
| Four ordinary writes, read, no intervening sync | — | 25 µs |
| Empty namespace flush, read; no writes in the phase | 22 µs | 645 µs |
| Empty namespace flush, second empty flush, read | Second flush: 621 µs | 642 µs |
| Four ordinary writes, `fdatasync`, read | 260 µs | 648 µs |
| Four ordinary writes, namespace flush, read | 259 µs | 647 µs |
| Four ordinary writes, 750 µs gap, `fdatasync`, read | 258 µs | 643 µs |

All six empty-flush phases contain only the helper's flush/read commands: zero
data writes from any host process, zero other command issuers, and no admin
commands or discards. Repeated empty flushes reproduce the routine delay without
new host data to persist. This is stronger than correlating the stall with a
dirty WAL or a full userspace queue. It still allows controller-internal metadata
or other maintenance work; host traces cannot establish that the controller
performs no internal writes.

The second empty flush finishes about 648 µs after the first flush's CQE.
The subsequent read finishes another approximately 674 µs after the second
flush's CQE. Adding two namespace flushes after a dirty `fdatasync` likewise
makes each additional flush cost about 619–620 µs, followed by a delayed read.
Thus another flush does not merely consume the original window: it starts
another one. Delaying before the dirty sync also fails to remove the window.
Direct namespace flushes reproduce it without ext4's `fdatasync` implementation.

No command in the empty-flush phases reaches 5 ms. Data-writing phases retain
the periodic longer stalls. The routine post-flush wait and the recurring
approximately 8 ms data/persistence stalls therefore need separate attribution;
the empty-flush result does not identify the latter's internal cause.

All-four-write FUA phases verify control `0x4000` on every write. Their following
reads have medians of 219–253 µs, but they retain 31–45 write stalls of at least
5 ms per 1,024 writes. This is a different persistence path, not an established
cure. A last-write-only FUA treatment is diagnostic only: FUA makes that
command's range durable and supplies no implied ordering with other commands,
so it cannot replace a group barrier for the preceding ordinary writes.
[NVM command-set specification, Write FUA definition](https://nvmexpress.org/wp-content/uploads/NVM-Express-NVM-Command-Set-Specification-1.0d-2023.12.28-Ratified.pdf#page=39).

The capture matches 62,260 commands without reported missing/replaced matches,
status errors, admin commands, discards, or event loss. Two control sync
intervals contain both the helper's flush and a journal flush; their ambiguous
sync associations are excluded. Every targeted write, read, and raw flush
matches. Trace output is compressed in RAM and archived after measurement.
Artifacts are `flush-policy-capture/`, `flush_policy_probe.py`,
`nvme-flush-policy.bt`, and `analyze_flush_policy.py`. An earlier authentication
failure starts no workload and remains in `flush-policy-auth-failed-capture/`.

## The empty-flush window survives both approved feature controls

A guarded seven-phase experiment repeats the same small-file treatments under
original settings, APST disabled twice, original settings, volatile cache
disabled twice, and original settings again. Each change is verified by readback,
never sets the persistent Save bit, and happens outside the measured probe
phases. Child lifetime is bounded; final device flush, exact original cache bit,
and exact original APST bit and transition table are successfully restored.

| Feature configuration | Following read after an empty flush, per-phase medians |
| --- | ---: |
| APST enabled, volatile cache enabled; original/restored phases | 643–646 µs |
| APST disabled, volatile cache enabled | 644–645 µs |
| APST enabled, volatile cache disabled | 643–644 µs |

The second empty flush also remains at 619–622 µs in every configuration.
Cache-disabled ordinary writes become slower, as in the earlier engine test;
neither setting supplies a mitigation. Every empty-flush phase during the two
disabled-feature comparisons has zero global data writes and no other issuers.
One final restored empty phase overlaps ten background writes; it remains
recorded and is not needed for the no-competing-write conclusion.

The trace matches 64,753 I/O commands with successful statuses and no discards
or reported event loss. It records 34 expected feature-query/set admin setups
outside the measured phases; those admin commands account for 34 completions
without an I/O CQE match. Three control sync intervals have ambiguous associations
with concurrent journal flushes and are excluded from that association analysis.
All targeted reads, writes, and namespace flushes match. Artifacts are
`flush-policy-features-capture/`, `flush_policy_feature_guard.c`, and the guard's
verified restoration log.

## Submission publication precedes the empty-flush wait

A further capture records command ID and submission-queue slot at the NVMe
driver's dispatch return. For all 1,280 commands in its two pure empty phases,
the command is present in the submission queue, `sq_tail == last_sq_tail`, and
the doorbell-buffer pointer is null. The
[Linux driver](https://github.com/torvalds/linux/blob/v6.18/drivers/nvme/host/pci.c)
sets that last-tail marker after its MMIO doorbell write. This locates driver
publication without assuming that a software submission count measures device
queue depth or proves when a PCIe transaction reaches the endpoint.

For the 256 reads following one empty flush, command setup to driver return has
a 5.24 µs median and 7.30 µs maximum. Driver return to consumed CQE has a 637.51 µs
median and 640.50 µs maximum. The second-flush/read sequence shows the same
short submission interval and long following wait. The capture matches 9,237
commands; 9,230 of 9,233 driver records pair with commands, excluding three
unmatched driver records. All targeted empty-phase commands pair successfully.
Artifacts are `flush-policy-submit-capture/`, `driver-publication.bt`, and
`analyze_driver_publication.py`.

A combined submission/CQ-head capture then samples the same empty sequences at
9,997 Hz on CPU 8, without consuming CQ entries or ringing doorbells. All 512
delayed reads and 255 of 256 delayed second flushes meet the requirement for
at least three stable samples, with six per command at the median. They have
4,787 not-ready samples. Observed matching ready CQEs precede driver consumption
by at most 4.848 µs for reads and 3.954 µs for flushes; none is observed ready
more than 100 µs before consumption. The last not-ready observation precedes
consumption by at most 104.868 µs. One flush with a recorded sampling race is
excluded. Thus the empty-flush wait also precedes host-visible completion,
rather than being a long interrupt wait after a ready CQE.

All 9,230 traced commands and their driver records match in this combined
capture. Its empty phases have no competing commands or data writes, successful
statuses, no admin commands or discards, and no reported event loss. Adding
sampling increases the measured setup-to-driver-return interval to about
10 µs; the following read still waits about 627 µs after publication. The sampler
records 47 map-lookup warnings and no probe-read warnings; warnings remain with
the raw artifacts and recorded races are excluded by the eligibility checks.
An earlier combined capture generated 36,963 warnings from uninitialized map
lookups. It is retained separately; initializing matcher state removes that
logging noise, and the repeated capture preserves the result. These captures
are timing diagnostics, not benchmark scores. Artifacts are
`flush-policy-cq-capture/`, `flush-policy-cq-clean-capture/`,
`nvme-flush-cq-clean.bt`, `analyze_empty_cq_clean.py`, and
`analyze_empty_driver_clean.py`.

## Parallel read queues share the empty-flush delay

A state-gated capture compares one, four, and eight read threads on the existing
reference file, opened with `O_RDONLY | O_DIRECT`. It uses namespace `FLUSH`
commands and reads of separate 4 KiB blocks, with no scratch writes. The helper
first checks the scalar empty-flush/read reproducer. When initially fast, it
runs the unchanged parallel-WAL binary under a bounded pressure sequence,
pauses that process, and checks again. The slow signature appears after about
74 seconds. The engine remains paused throughout the queue comparison and is
terminated afterward; its incomplete run is not a throughput score.

Every treatment has a scalar control before and after it. Their 36 medians for
flush CQE to following read CQE remain within 675.5–677.0 µs. In the two cycles,
post-flush read command latencies are:

| Read threads / distinct NVMe queues | Per-phase read medians | Median peak outstanding reads |
| --- | ---: | ---: |
| 1 | 642–646 µs | 1 |
| 4 | 617–664 µs | 4 |
| 8 | 653–679 µs | 8 |

All six post-flush phases have zero global data writes and no other command
issuers. The reads really overlap on different queues, but increasing their
count does not multiply the delay. This supports a shared NVM service window
rather than a stall confined to one submission queue. Host outstanding counts
still do not measure controller execution or physical device queue depth.
A repeat adds a final read after each group settles and preserves the same
initial delay; its controls remain at 675.8–677.3 µs. One repeat phase has
background writes, which remain recorded separately.

## Read/flush overlap does not establish a workaround

A first concurrent-read capture appears promising when classified only by
command setup and CQE times: its 208 groups with a read spanning the flush CQE
have a 41.6 µs median final read, while 23 groups whose read completes earlier
have a 595.1 µs final read. The submission-publication repeat shows why that
classification is insufficient. Of its 384 concurrent groups, 374 publish the
read after the flush publication, and 371 of those reads themselves take at
least 500 µs. A fast final read can simply mean the earlier read already paid
the delay. Software overlap alone is not evidence that the window disappeared.

A native helper then signals immediately before the first synchronous `pread`.
The flush thread waits a requested 0, 2, 5, 10, 20, 30, 50, or 100 µs before
issuing `FLUSH`; this signaling and delay run outside Python's interpreter lock.
Sixteen shuffled phases compare a reader sharing queue 10 with the flush thread
against a reader on queue 5. Each has 64 groups plus scalar controls before and
after. The 32 control medians remain at 675.9–677.4 µs. Every directed phase has
zero global data writes and no other command issuers.

Five groups have inconsistent submission-tail snapshots under concurrent queue
updates. They are excluded from publication-order analysis; their raw CQE
records remain retained. Among the 1,019 eligible groups:

| Timing class | Groups | First-read median | Final-read median |
| --- | ---: | ---: | ---: |
| Read CQE before flush CQE | 656 | 46.2 µs | 613.2 µs |
| Read published before flush CQE, still pending at that CQE | 360 | 129.3 µs | 510.3 µs |
| Read published after flush CQE | 3 | 764.3 µs | 46.8 µs |

More specifically, 215 eligible reads have a matching publication marker at
driver return before flush command setup and are still pending at its CQE.
Their final-read median is 549.6 µs; 180 final
reads take at least 500 µs. Thus even this earlier host publication plus a
request spanning the flush CQE does not guarantee avoidance. Some short-delay
queue-5 phases have fast final reads, but other delays retain the window; this
is not a monotonic timing rule or a tested WAL mitigation. CQE timestamps and
driver publication do not reveal when the controller starts processing a read.

## Read-only admin queries perturb the measurement

A separate controlled capture queries volatile-cache Feature `0x06` with the
read-only `Get Features` opcode `0x0a`. It never changes that feature, and every
query returns the expected enabled value. The 16 bracketing scalar controls
remain at 675.9–676.2 µs from flush CQE to read CQE.

| Concurrent reads | Read median without admin query | Read median with query while reads are outstanding | Admin setup-to-completion median |
| --- | ---: | ---: | ---: |
| 4 | 618.0 µs | 1,188.3 µs | 815.2 µs |
| 8 | 652.8 µs | 1,089.5 µs | 842.9 µs |

The admin command completes with at least one NVM read still pending in
127/128 four-reader groups and 122/128 eight-reader groups. In 114 and 84 groups,
respectively, all reads remain pending at that instant. The four-reader
treatment has no other commands; the eight-reader treatment has three
background commands. Admin completion during delayed data reads argues against
a complete freeze of the controller or its PCIe path. Querying the feature
also clearly perturbs the observation, so it must not serve as passive
telemetry inside a performance comparison. These measurements use the admin
completion tracepoint, not an admin hardware-CQE visibility sampler.

Seven new captures and their helpers remain archived under
`target/rfc024-stall-diagnosis/`: `flush-scope-capture/`,
`flush-scope-gated-capture/`, `flush-scope-controlled-capture/`,
`flush-scope-tail-capture/`, `flush-scope-admin-controlled-capture/`,
`flush-scope-overlap-driver-capture/`, and `flush-scope-directed-capture/`.
The first two are exploratory: the intended gate in the second did not run
because `sudo` removed its parent environment configuration. That mistake is
recorded in `capture-notes.json`; later wrappers set configuration inside the
root helper. Neither exploratory capture supplies a state-controlled claim.

The successful controlled pressure capture has ambiguous/unmatched records
during warmup, including one target flush in a fast-state check; those are
excluded. All subsequent treatment operations match. The tail, admin,
publication-repeat, and native captures match every target operation with no
status errors, discards, reported event loss, or runtime tracer warnings. The
admin capture's 512 expected completions without an I/O-CQE match are separately
paired with `ADMIN_DONE`. The native capture matches all 7,430 commands and
driver records, while excluding the five tail-snapshot groups above. Whole-run
background writes are recorded and are not described as scratch writes. Source,
analysis, raw logs, summaries, and native-helper hashes are recorded in
`flush-scope-artifacts.json`; `flush-scope-findings.json` collects the cited
comparisons. No controller settings change in these seven captures.

## Online evidence for this SSD and firmware

An independent test reports the exact `SSDPFKNU010TZ` model and `002C` firmware.
At 10% versus 50% fill, its sequential writes fall from 1,862 to 219 MB/s and
512-byte QD1 write p99.9 rises from 3.844 to 9.718 ms. It observes no SLC reclaim
after up to four minutes idle following half-drive fill. These measurements
corroborate strong occupancy/history sensitivity for this device; they do not
report our 641 µs post-flush window or establish its cause.
[PyNVMe P41 Plus 1TB test report](https://pynv.me/ssd/solidigm-p41-plus-1tb/).
The platform's published design uses a userspace SPDK NVMe driver. That supplies
a useful comparison beyond the Linux filesystem path, although the report does
not publish enough raw traces to equate its stalls with ours.
[PyNVMe3 design](https://pynv.me/ssd/pynvme3-design/).

Solidigm's June 2026 firmware history lists a fix for premature DSLC disablement
in `002C`, which is already installed here. Its `004C` entry concerns postponing
info-block refresh on host shutdown to avoid data loss in an edge case; it does
not claim a flush-latency or sustained-write performance fix.
[SST release notes, Table 14](https://sdmsdfwdriver.blob.core.windows.net/files/kba-gcc/drivers-downloads/ka-00085/sst--3-1/solidigm-storage-tool-release-notes-727314-027us.pdf#page=13).
Solidigm currently lists `004C` as the latest P41 Plus firmware.
[Official firmware table](https://www.solidigm.com/support-page/drivers-downloads/ka-00099.html).
The captures above use `002C`; no firmware update had been performed at that
stage. The subsequent approved firmware intervention is recorded below.

There is also a first-hand Ubuntu report of burst-then-collapse behavior on
2 TB P41 Plus drives already running `004C`. Its copy workload and capacity
differ from ours, so it supplies context rather than a matching reproducer or
proof that a particular workaround succeeds.
[Solidigm support discussion](https://community.solidigm.com/t5/solid-state-drives-nand/poor-performance-p41-plus/m-p/24606).

The earlier reported 89% SLC-buffer availability cannot establish the placement
of these writes or exclude a cache-policy issue. Filesystem free space also
does not measure the controller's valid mappings after benchmark churn without
continuous discard. Neither the firmware history nor the independent report
lets us label our exact mechanism as the old DSLC bug, QLC programming, garbage
collection, or a confirmed Linux defect. What is established locally is a
state-dependent post-flush service window before host-visible completion.

The broader measurement literature documents that SSD buffer policies can
produce periodic latency spikes, that idle gaps change those spikes, and that
some devices serialize concurrently submitted writes. It provides experimental
methods and competing mechanisms, rather than a matching P41 Plus firmware fix.
The studied drives predate this model, and the full probing suite requires
destructive preparation, so that suite is not run on this mounted system drive.
[Fantastic SSD Internals and How to Learn and Use Them](https://people.cs.vt.edu/huaicheng/p/systor22-queenie.pdf).
No primary report found in the searched Linux/vendor material establishes a
known fix for the reproduced empty-flush/post-CQE timing signature. This is a
search result limitation, not proof that no relevant defect exists.

The Linux 6.18 NVMe PCI driver has a deepest-power-state workaround for Solidigm
P44 Pro, PCI `025e:f1ac`. This host's P41 Plus is `025e:f1ab`; that entry is not
a matching-device fix, and disabling APST here already failed to remove the
window. [Linux NVMe PCI device table](https://github.com/torvalds/linux/blob/v6.18/drivers/nvme/host/pci.c).

The 2026 SIndex study also observes read-latency spikes after host writers stop,
attributing them to internal buffer flushing on its tested devices. Its
P4510/P4610/ZNS examples concern internal maintenance after data writes, not our
empty namespace-FLUSH reproducer on P41 Plus. This supports an experimental
hypothesis, without identifying our firmware task or a transferable fix.
[SIndex, Section 3.3](https://doi.org/10.1145/3789205).

## Approved 004C firmware intervention (2026-10-01)

After explicit approval, Solidigm Storage Tool 3.1 applies its bundled firmware
to `/dev/nvme0n1`, the `SOLIDIGM SSDPFKNU010TZ` 1 TB system SSD. Preflight verifies
the exact model, healthy status, running `002C`, and available `004C`. The tool
runs in a private mount namespace with its extracted firmware modules; nothing
is installed globally. No external firmware image or erase command is used.

The update finishes with exit status zero and reports successful installation,
with a recommendation to reboot. No host reboot is performed. Subsequent NVMe
Identify Controller and Firmware Slot Information queries independently report
running `004C`, active slot 1 containing `004C`, and no slot pending activation
on reset. Slot 2 retains `002C`. Linux sysfs and the vendor postflight also
report `004C`; the controller is live and vendor status remains healthy. This
establishes the active revision for the experiment despite the tool's reboot
recommendation. It does not determine whether the tool internally reset the
controller during the update.

An initially faster device after firmware activation would not establish a
fix: activation itself may change device state. The diagnostic therefore
reuses the same initialized 64 MiB reference file and the previous pressure
procedure. It starts the unchanged `925ed07d` parallel-WAL binary with four
writers, 700,000 puts, 1 KiB values, and 1 MiB SST rotation. Every two seconds
it pauses that process, waits 300 ms, and checks 128 sequential empty-FLUSH/read
pairs. The pressure run is bounded and intentionally terminated after the
scratch tests; its partial benchmark output is not a throughput score.

The initial empty-flush/read median is 66.359 µs. Four pressure checks remain
at 72.5–77.0 µs, but the next reaches 674.708 µs, 11.675 seconds after pressure
starts. The process remains paused for the following two cycles, each with
128 groups per treatment. All phase records are complete and error-free.

| Concurrent readers | Direct-read median without FLUSH, µs | Direct-read median after empty FLUSH, µs | Reads per treatment |
| --- | ---: | ---: | ---: |
| 1 | 108.0 | 698.0 | 256 |
| 4 | 127.9 | 660.4 | 1,024 |
| 8 | 125.7 | 681.0 | 2,048 |

The scalar controls before and after each cycle remain at 674.7–675.2 µs.
No controller admin queries or profiling run inside these measurement phases.
The scratch operations only issue namespace FLUSH and read-only `O_DIRECT`
reads; the preceding WAL pressure does write data. This experiment has no
eBPF command/CQE capture, so these numbers are syscall timings, not a new
command-publication or hardware-completion attribution. Other host activity is
not excluded by a whole-command trace in this run.

The update does not prevent this pressure sequence from reproducing the long
post-flush read latency. It does not show that `004C` has no other benefits,
establish a particular internal firmware mechanism, or support a production
durability workaround. The firmware intervention is separate from code
optimization and cannot qualify a WAL adoption gate.

The subsequent end-to-end check runs the same saved parallel-WAL binary against
itself at 4, 8, and 16 writers. Its SHA-256 is unchanged before and after all
18 successful runs. Each count has three adjacent A/B pairs with alternating
arm order; writer-count order rotates between rounds. All runs use fresh paths,
50,000 puts, 1 KiB values, and latency sampling every ten operations. Four writers
use 1 MiB rotation; eight and sixteen use a 1 GiB SST target. No build, profiler,
or controller admin polling overlaps timing. All samples are retained.

| Writers | Median puts/s | Minimum–maximum puts/s | Minimum–maximum commit p99, ms | Same-binary B/A throughput ratios |
| --- | ---: | ---: | ---: | --- |
| 4 | 9,695 | 2,398–9,811 | 1.190–11.926 | 3.239, 0.995, 0.987 |
| 8 | 18,041 | 17,775–18,311 | 1.095–1.166 | 1.016, 1.012, 0.971 |
| 16 | 28,247 | 27,632–29,211 | 1.237–1.300 | 0.968, 0.991, 0.982 |

The first four-writer pair changes from 2,398 to 7,765 puts/s without a code
change; later samples reach about 9.8k. This reproduces substantial variability
on active `004C`. The eight- and sixteen-writer samples are closer in this short
session, which does not establish sustained stability at those counts. This
is a 50k-put diagnostic, not a rerun of the prior 200k-put adoption matrix,
a matched old/new firmware throughput comparison, or an optimization score.

Artifacts are `sst-tools/firmware-004c-update-20261001/` for preflight, update,
postflight, and activation verification; `firmware-004c-probe/` for pressure
checks and direct-read phases; and `target/rfc024-firmware-004c-comparison/`
for the unchanged-binary 4/8/16-writer checks. `firmware-004c-artifacts.json`
records their source and result hashes. The firmware package remains the same
vendor package used in the earlier read-only eligibility check.

## Consequence for optimization decisions

The benchmark mixes substantially different storage-completion latency states.
That can produce apparent gains or regressions even for the identical binary,
as the same-binary controls already demonstrated. The recurring stall is not
created exclusively by the parallel WAL state machine, although an optimization
can still change the I/O cadence and its sensitivity to the stall.

Previous comparison artifacts and scores remain preserved, with the
low-concurrency conclusions provisional. Do not silently discard slow runs or
qualify adoption from a favorable subset. A fresh comparison should interleave
short baseline/candidate blocks and same-binary controls, record write and sync
latency plus software pipeline depth, and retain every result. Report storage
states separately as well as the full run distribution; a code benefit requires
comparable controls and repeatable results. Outstanding write SQEs remain a
software metric, not device queue depth.

No Rust implementation or production default changes result from this
investigation. The slow-state eBPF captures narrow attribution toward device/platform
completion visibility, with short observed submission and IRQ-service times.
The pressure sequence reproduces the stall, but the allocation-only control
also observes a transition without large data writes. APST is not required,
and the slow state persists after cooling and short idle. Read-only cache telemetry
reports available SLC space but leaves actual write placement and cache policy
unresolved. TRIM followed by idle coincides with recovery,
and the subsequent allocation control and three engine samples remain fast.
The subsequent rerun again shows large identical-binary swings, so this
recovery does not supply a reliable measurement mitigation or establish a unique
cause. Exact controller, media,
firmware, or PCIe/DMA attribution remains open. The matched completion split
now excludes millisecond delays after CQE consumption in the reproduced slow
mode. SMART polling is a measurable confounder but is absent in slow captures.
The CPU busy-loop control and cache-disabled trace fail to prevent the long
wait. Vendor background-processing and NLog commands are unsupported, so these
results do not establish a specific internal maintenance mechanism.

The delay-insertion and completion-head samples further identify a routine
approximately 641 µs post-flush service window. The sampled commands do not
spend that interval waiting for interrupt handling after a visible CQE. Known
measurements on the same model/firmware support investigating internal cache and
persistence policy, without establishing which policy switches the observed
state. Inserting delays or dummy reads merely shifts the cost and is not a
production mitigation.

Empty-flush tests narrow the routine mechanism further: the namespace `FLUSH`
path itself can reproduce and restart the window without new host data writes,
including with APST or volatile cache disabled. Command-specific submission-tail
timestamps put the large wait after driver publication. Controller flush handling
is consequently the leading explanation; physical transport timing and the
operation that switches the original fast/slow mode remain unproven. The separate
data-dependent long stalls also remain unexplained. No production durability or
WAL changes follow from these diagnostics.

The queue and native-overlap captures narrow that lead to shared NVM service
timing without proving an unconditional blackout after every flush. Requests
published early can finish while later requests still absorb the remaining
window. Admin commands can complete during delayed NVM reads and can extend the
delay themselves. Neither higher outstanding depth nor read/flush overlap is
a demonstrated cure. The exact fast/slow transition and the separate periodic
approximately 8 ms data-writing stalls still require attribution; no safe
production optimization or end-to-end WAL gain is established by these probes.

The approved update activates `004C`, but the same pressure procedure still
reproduces the post-flush read delay and an unchanged four-writer binary still
has a 3.24x paired throughput swing. The firmware intervention consequently
does not resolve the measurement problem. Its short comparison cannot establish
an old/new firmware regression or qualify code gains.
