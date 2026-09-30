# RFC 024: investigation of unstable ext4 measurements

The low-concurrency rerun reproduced large throughput swings with an unchanged
binary. Privileged tracing now locates a large part of the recurring delay in
the NVMe command setup-to-completion path, before userspace WAL completion
handling. The same stalls reproduce without the engine. This explains a major
measurement confounder, but does not identify the exact device or interrupt
mechanism. Subsequent eBPF captures reproduce the slow state, find short
driver submission and interrupt handling, and observe no persistent ready CQ
backlog at millisecond sampling resolution. Small optimization gains remain unqualified until fresh controls
show comparable storage latency.

## What has been measured

Diagnostic artifacts are in `target/rfc024-stall-diagnosis/`. These are diagnostic
runs, not adoption scores: monitoring and detailed profiling have overhead.
The original comparison and rerun remain in
[rfc-024-parallel-wal-benchmark.md](rfc-024-parallel-wal-benchmark.md).

The host uses Linux 6.18.9, ext4 on `/dev/nvme0n1p3`, and a Solidigm
`SSDPFKNU010TZ` SSD behind Intel VMD. The same device holds `/` and `/home`.
Its scheduler is `none`. The CPU has 32 logical CPUs with performance and
efficiency cores. No global scheduler, affinity, mount, power, durability, or
production WAL setting has been changed.

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
Exact controller, media, firmware, or PCIe/DMA attribution remains open.
