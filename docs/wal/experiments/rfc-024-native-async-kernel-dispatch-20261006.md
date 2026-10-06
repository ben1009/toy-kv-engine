# RFC 024: native forced-async kernel dispatch, 2026-10-06

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

`IOSQE_ASYNC` makes the submission syscall shorter, but it moves filesystem
write issuance to io-wq helpers. Ordinary SQEs already issue asynchronous
direct I/O successfully without helpers for most writes. In the two healthy
sixteen-writer pairs, the flag lowers median submission-call time from about
10.5 microseconds to 2.0–2.4, while median submission-to-kernel-CQE-fill time
changes from 223–230 to 227–238 microseconds. Sync latency remains similar.

This explains the limited headroom in the
[preceding forced-async experiment](rfc-024-native-submission-and-key-preparation-20261006.md).
That uninstrumented screen reports +1.5% target throughput, below its frozen
+2% threshold, with failed repeat and null controls. These new traces are
descriptive; they establish no new qualified speedup. The flag remains
unretained. No production Rust code changes in this diagnostic.

## Protocol and definitions

The baseline and forced-async executables are copied exactly from the preceding
screen. Each primary trace uses 524,288 puts as 8,192 batches of 64, 1 KiB
values, eight Tokio workers, explicit native parallel WAL, and a 1 GiB memtable
target. PITR and serializable transactions are off; no timed rotation occurs.
Fresh databases use ext4 `/dev/nvme0n1p3` on Linux `6.12.71-1-lts`.

Two 32,768-put pilots validate extraction. The diagnostic phase then runs four
warmups and three AB/BA/AB paired blocks per eight/sixteen-writer case, rotating
case order. Every run has a five-second idle gap. A primary p99 above 6 ms
triggers an extra one-second pause and one separately retained retry. There
are twelve original primary traces and five retries; every original remains
reported. Traced throughput is excluded from the performance gate.

The profiler records selected io_uring, syscall, and scheduling events in RAM,
with temporary entry/return kprobes on `io_write`. It runs as root; the benchmark
runs as UID 1000. Request matching uses the benchmark's WAL-worker TID,
ring, request pointer, and `user_data`. Only the owned probes and test databases
are removed afterward. No sysctl or global tracer settings are changed.

Submission-call time is the elapsed `io_uring_enter` syscall for calls submitting
writes, measured per call. Helper handoff is `queue_async_work` to helper
`io_write` entry; it includes io-wq queueing and scheduling, rather than isolating
CPU scheduling. Write-issuance time measures entry to return of `io_write`,
which may return while device I/O remains pending.

The CQE interval starts at `io_uring_submit_req` and ends at
`io_uring_complete`, which fires while filling the CQE before its publication
to userspace. It includes kernel issuance, filesystem/device work and kernel
completion handling. It does not measure SSD service time alone or userspace
CQE consumption. No `io_uring_task_add` events are observed, so these traces
cannot separately time that part of completion delivery.

## Measurements

All values below are within-run medians in microseconds. The table includes
all original primary pairs, including stalled observations; retries are kept
separately in the archive. The helper percentage counts ordinary requests
that reach io-wq. Forced-async requests reach io-wq in every observation.

| Writers | Block | Ordinary helper % | Ordinary submit | Forced submit | Forced helper handoff | Ordinary CQE interval | Forced CQE interval |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 1 | 13.8% | 16.5 | 3.9 | 21.4 | 195.6 | 393.4 |
| 8 | 2 | 11.9% | 18.0 | 3.1 | 9.2 | 117.2 | 121.4 |
| 8 | 3 | 14.8% | 18.1 | 3.7 | 15.5 | 276.9 | 298.9 |
| 16 | 1 | 24.0% | 10.4 | 2.0 | 5.9 | 222.8 | 227.2 |
| 16 | 2 | 23.2% | 10.7 | 3.7 | 79.6 | 225.2 | 8125.2 |
| 16 | 3 | 23.4% | 10.5 | 2.4 | 6.3 | 230.1 | 237.7 |

At sixteen writers, ordinary submission tries every write inline and offloads
23.2–24.0% after an `EAGAIN` return. The other 76.0–76.8% issue successfully
without the helper hop. Forced async offloads 100% and avoids that initial
attempt, but the helper still performs the write-issuance work. In healthy
pairs its median `io_write` execution is 10.5–10.8 microseconds, after a
5.9–6.3 microsecond median handoff. These interval medians cannot be added
to construct a median end-to-end request latency.

Healthy sixteen-writer paired blocks have median `fdatasync` times of
438/437 and 457/460 microseconds for ordinary/forced submission. Summed sync
syscall elapsed time occupies 63–65% of their measurement durations. Syncs
are serialized in the coordinator, while submission and write execution
can overlap them; sums of concurrent timers are not an end-to-end latency
breakdown. Sync counts change from 1,072 to 1,052 and 1,087 to 1,065.
Both arms reach the same 15–16 in-flight-group peaks. The configured 32-group
cap is not reached, and these software counts are not device queue depth.

The stalled sixteen-writer forced run has 31.149 ms batch p99, a median helper
handoff of 79.6 microseconds, an 8.125 ms median CQE interval, and a 3.878 ms
mean sync syscall. Its retry also stalls. The longer handoff is measurable,
but accounts for a small portion of the complete write wait; the trace does
not attribute the whole stall to scheduling. Earlier identical-baseline
controls already show stalls without this flag. Neither this trace nor
those controls isolate the storage-stall cause to candidate code.

The measured mechanism is narrower than saying helper scheduling alone
caused the failed screen: the flag saves submitter time, preserves the
filesystem write and durability work, and adds a helper hop to most writes
that previously avoided it. The healthy traces show no corresponding CQE
latency reduction. A useful subsequent experiment must address the durable
sync cadence or another measured commit-path delay, with independent
uninstrumented controls before retention.

## Kernel source and audit

The matching upstream Linux source confirms the ordinary nonblocking issue
attempt and `EAGAIN` fallback, while `REQ_F_FORCE_ASYNC` takes the io-wq path.
See [io_uring.c](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/io_uring.c)
and [rw.c](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/rw.c).
All successful issue attempts return internal status `-529`,
`IOU_ISSUE_SKIP_COMPLETE = -EIOCBQUEUED`, indicating pending completion rather
than a failed WAL write. See
[io_uring.h](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/io_uring.h)
and [errno.h](https://github.com/gregkh/linux/blob/v6.12.71/include/linux/errno.h).
Every terminal WAL CQE returns the full 69,632-byte write length.

An independent audit reconciles all 23 accepted observations and 142,336
terminal WAL CQEs with batch/buffer counters, contiguous request identifiers,
submission syscall results, and sync counts/durations. Diagnostic probe hit
deltas match trace entry/return counts, with zero missed probes and no lost
perf events. All 115 recorded production inputs and both executable hashes
remain unchanged; owned RAM stages, probes, and databases are removed.

The raw captures contain sixteen exact duplicate records: fifteen scheduler
records and one WAL-submit record. Standard replay and raw perf dump confirm
the duplicated submit record at distinct offsets, with identical timestamp
and payload. Extraction normalizes only identical event identities and logs
each duplicate; there is one write entry/return and one CQE for that request.
One other trace reports a cross-CPU ordering inversion of 1.471 microseconds.
It retains the sample and matches calls by TID and requests by identity;
all resulting durations remain nonnegative. Raw captures and the initial
parser are preserved, with parser revisions recorded separately.

Two earlier preflight attempts are excluded: a tracefs append-mode error
before benchmarking, and rejection of the recorder's intentional SIGINT
shutdown status after a completed pilot. Accepted pilots use the corrected
driver. The full diagnostic driver and protocol are frozen before timing.
These tooling corrections do not change the benchmark executables.

Archive: `target/rfc024-native-async-kernel-dispatch-20261006/`. It includes
protocol and source hashes, recorder commands, raw perf captures and replay,
benchmark outputs, per-request evidence, parser revisions, independent audit,
and cleanup records. The exact timing binaries retain these SHA-256 hashes:

- Ordinary: `7f113ba4b8c116ea7adfb3bc058b983f7b10ad64db4023a5e93ecc62f8ecef76`.
- Forced async: `d981a0c5299869f27939fc4806e4149e161c8a513034e79b43f7b194414e62a6`.
