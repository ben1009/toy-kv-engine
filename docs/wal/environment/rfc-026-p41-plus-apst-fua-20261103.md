# P41 Plus firmware 004C: APST and file-based FUA controls

The fresh tests reproduce the slow state while APST is disabled. Real NVMe
FUA writes also retain millisecond stalls, including a confirmation phase
with zero namespace FLUSH commands and zero other host data writes. This does
not support a stable FUA bypass. It does not identify the internal NAND,
FTL or firmware operation responsible for the change of state.

These tests run on the existing 1 TB `SOLIDIGM SSDPFKNU010TZ`, firmware
`004C`, Linux 6.12.71-1-lts, and the repository's ext4 filesystem. They follow
the [thirty-minute quiet test](../benchmarks/rfc-026-v8-background-affinity-20261102.md#thirty-minute-quiet-recovery-test-november-3)
and supplement the [historical APST/FUA investigation](rfc-024-wal-stall-diagnosis.md),
whose earlier feature controls predate the firmware update. No production
WAL source, format, durability policy or default affinity changes.

## Method and scope

Neither fio nor nvme-cli is installed on this host. Read-only Identify, SMART
and current-feature queries use the existing NVMe admin ioctl. These are the
same classes of device information requested by modern `nvme id ctrl` and
`nvme log smart`; command grouping is version-dependent. See the
[nvme-cli release notes](https://github.com/linux-nvme/nvme-cli/blob/master/NEWS.md).

Fio's NVMe `io_uring_cmd` uses the namespace character device, as shown in its
[official example](https://github.com/axboe/fio/blob/master/examples/uring-cmd-ng.fio).
Its `writefua=1` is not a way to turn an ordinary filesystem filename into
NVMe passthrough. This experiment issues **no raw-device data writes**.
Every data write uses the same existing, initialized 8 MiB scratch inode
through ordinary filesystem APIs. Each probe overwrites only its first 4 MiB.
No allocation, discard, reformat, firmware update or controller reset occurs.

The three protocols are:

- `flush4`: aligned 4 KiB O_DIRECT writes, fdatasync after four writes.
- `flush1`: aligned 4 KiB O_DIRECT write plus fdatasync for each write.
- `dsync1`: aligned 4 KiB filesystem `pwritev` with `RWF_DSYNC` for each write.

The last protocol is called FUA only after tracing verifies `control=0x4000`
on every targeted NVMe write. Ordinary writes have control zero. Each request
is eight 512-byte logical blocks. Both `flush1` and `dsync1` give each write
its own durability boundary. `flush4` amortizes a boundary over four writes;
its throughput is not a like-for-like per-write durability comparison.

Probe code runs as UID 1000 on CPU 8. A privileged parent records NVMe command
setup events into a memfd; stdout and trace data stay in RAM until probes and
feature restoration finish. Timestamps explicitly use CLOCK_MONOTONIC. The
kernel tracepoint format and compressed native perf streams are archived.
Raw decoding is used because this host's perf pretty-printer cannot render
the NVMe command-specific fields correctly. The decoder first reproduces the
earlier trace's 36,946 setup events and 24,592 ordinary probe write flags.

The first smoke has sixteen writes per protocol, validates contents and FUA
flags, and changes no feature. Each full probe has 1,024 writes. The fixed
original/APST-disabled sequence is `flush4, flush1, dsync1, dsync1, flush1,
flush4`, followed by two restored `flush4` controls. A separate six-probe
confirmation runs that same sequence with restored APST unchanged. All twenty
full probes are retained: 80 MiB of scratch writes, plus the unscored smoke.

## Capabilities and health

The preflight Identify/SMART report contains:

| Field | Observation |
|---|---|
| Firmware | `004C` |
| NVMe version | `0x10400` (1.4) |
| APST capability | `APSTA=1`, five advertised power states |
| Current APST enable bit | 1 |
| Volatile-cache capability/current value | `VWC=5`, enabled/current value 1 |
| Optional NVM command support | `ONCS=0xdf` |
| Critical warning / media errors / error-log entries | 0 / 0 / 0 |
| Temperature at preflight | 304 K, approximately 31°C |
| Spare / endurance used | 100% / 6% |

These are preflight health observations, not temperature sampling during I/O.
SMART and feature queries stay outside timed probe phases. Capability bits
do not diagnose performance or identify background NAND maintenance.

## APST result

The original enable bit and all 256 bytes of the transition table are saved.
Only volatile FID `0x0c` is changed, with the persistent Save bit clear.
Readback verifies disabled value 0 and the unchanged table before its cohort.
The original value 1 and identical table are then restored and verified.
The volatile-cache setting remains enabled throughout. The module parameter
`nvme_core.default_ps_max_latency_us` is recorded but never modified.

The state changes within the sequence, so the following observations must
not be interpreted as average effects of changing APST:

| Phase | `flush4` writes/s | First post-sync write, median µs | Writes >5 ms / 1,024 |
|---|---:|---:|---:|
| Original APST, first/last controls | 12,206–12,411 | 29.832–30.058 | 0 / 0 |
| APST disabled, initial control | 12,391 | 29.802 | 0 |
| APST disabled, final control | 2,053 | 639.567 | 32 |
| Restored APST, two controls | 1,947–2,067 | 639.667–639.811 | 31 / 31 |

The first disabled FUA phase contains a transition to longer latencies.
The routine approximately 640 µs post-sync delay is present afterward while
APST is disabled, and persists after re-enabling it. APST therefore need not
be enabled for this slowdown. This ordered, single-device sequence does not
establish that toggling APST, or FUA itself, causes the transition.

## FUA result and confirmation

Original-state FUA phases are initially fast: 4,470–4,485 writes/s, no writes
over 5 ms, and per-request p99 0.523–0.525 ms. The corresponding `flush1`
controls reach 3,654–3,662 writes/s with p99 approximately 0.574 ms. This is a
fast-state protocol observation, not a remedy for a reproduced slow state.

With APST disabled, the two FUA phases have 108 and 129 writes over 5 ms.
After exact restoration, the confirmation is already slow in its first
`flush4` control and stays slow through its final control:

| Confirmation, APST enabled | Writes/s | Durable request p99, ms | Writes >5 ms | Global FLUSH commands | Other host data writes |
|---|---:|---:|---:|---:|---:|
| `flush1`, before FUA | 525 | 9.406 | 128 | 1,025 | 28 |
| FUA, first phase | 629 | 15.806 | 122 | **0** | **0** |
| FUA, second phase | 799 | 9.235 | 127 | 1 | 19 |
| `flush1`, after FUA | 523 | 9.498 | 129 | 1,025 | 9 |

Every row contains 1,024 targeted writes. Every FUA write has the verified bit;
the first FUA row also has no other namespace read commands. The second FUA
phase overlaps one background flush; it remains preserved and is not needed
for the zero-FLUSH observation. Slow-state `flush4` controls enclosing this
confirmation reach 2,005/2,033 writes/s and approximately 640 µs post-sync
medians, with 32 long writes in each control.

FUA lowers the routine slow-state request median to approximately 244 µs
versus approximately 904 µs for write-plus-fdatasync, but retains a severe
tail. The fresh evidence rejects treating FUA as a stable escape from all
observed stalls. It does not prove that ordinary NAND programming latency
is intrinsically that large: the same FUA protocol is much faster in the
initial phases. FTL policy, media work and controller firmware remain possible
explanations; these captures have command setup events rather than completion
events, so they do not independently locate each FUA syscall stall within
the device versus the remaining filesystem/host completion path.

The routine post-FLUSH window and the longer persistence-related stalls must
remain separate findings. Nor may a WAL replace its group barrier with FUA
on only the FRONTIER write: FUA covers the command's own data/metadata and
does not order other writes. The preceding DATA still needs an independently
correct persistence protocol. See the [NVM Write FUA definition, Figure 63](https://nvmexpress.org/wp-content/uploads/NVM-Express-NVM-Command-Set-Specification-1.0d-2023.12.28-Ratified.pdf#page=39).
No WAL change follows from this diagnostic.

## Artifacts and checks

The [measurement records](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-measurements.json)
preserve every probe and feature readback. The
[analyzer](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-analyze.py)
re-decodes the five compressed native perf streams entirely in RAM and
recomputes the [summary](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-summary.json),
including competing I/O and FUA bits. All **20,528** targeted writes, including
the smoke, match the trace; no sample loss is reported. This is not a normal
engine performance score or a V4/V7/V8 comparison.

The [guard](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-guard.py),
[probe](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-probe.py),
[raw decoder](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-decode.py),
and [read-only ioctl helper](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-nvme_readonly.py)
are reproduction artifacts. They were executed under local
`target/rfc-026-v8-flush-fua-20261103/` with their original short filenames;
archived filenames do not change the expected layout. The
[check record](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-checks.json)
records data/trace validation and exact restoration. The
[environment record](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-environment.json)
includes the kernel/tool versions and independent native perf loss checks;
the [hash manifest](../benchmarks/artifacts/rfc-026-v8-flush-fua-20261103-hashes.json)
covers the report and its artifacts. Retained Rust sources
remain unchanged; no new runtime suite is required for these diagnostics.
