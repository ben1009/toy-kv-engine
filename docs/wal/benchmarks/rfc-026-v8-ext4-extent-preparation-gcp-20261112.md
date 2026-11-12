# RFC 026 V8 ext4 extent preparation on GCP — 2026-11-12

## Conclusion

The V8 extent-preparation hypothesis is strongly supported. In the unprepared
V8 arm's one-run trace, every DATA write was observed entering io-wq, and the
ext4 write path returned `-EAGAIN` 100,000 times. Preallocating the same WAL
suffix reduced both counts to about 1,300 and raised timed throughput by 56%.
V7 had 311 such events in its diagnostic run and remained the fastest arm.

Zero-initializing the allocated suffix raised V8's timed throughput further,
but the 512 MiB initialization took about 759 ms outside the timed benchmark.
Charging that one-time work to this 100,000-put run reduces its effective rate
to 32.50k puts/s, slightly below `fallocate` alone at 33.22k puts/s. The next
bounded-preparation follow-up is recorded in the
[sliding extent preparation report](rfc-026-v8-bounded-extent-gcp-20261112.md):
1 MiB and 4 MiB windows both reached about 33.8–33.9k puts/s with six runs per
arm and allocation work included in the measured workload.

No production WAL behavior changed. The preparation control and crash-recovery
coverage are available only with the `bench` feature.

## Experiment

The host was a 16-vCPU C4 with two Local SSDs in RAID0, ext4, and kernel
`7.0.0-1017-gcp`. io-wq was unpinned. Each arm ran five times with 16 writers,
100,000 point puts, and 1 KiB values. The 20-run cohort used counterbalanced
arm order and disabled profiling and per-sync/HOL diagnostics; aggregate WAL
counters remained enabled.

All V8 arms used Parallel I/O, Direct FRONTIER, and a fixed 400 μs coalescing
budget. The DATA, FRONTIER, and sync descriptors were confirmed to remain on
the same WAL inode, with `O_DIRECT=true`, `true`, and `false` respectively.
Start/end metadata was stable for every V8 run.

| Arm | WAL mode | Extra preparation before timing | Timed throughput, median (range) | Preparation-inclusive equivalent | Process CPU | Syncs, median | Mean `fdatasync` |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| A | V8 | None | 21.25k/s (20.68–21.35k) | 21.25k/s | 11.35 s | 7,381 | 104.9 μs |
| B | V8 | `fallocate` 512 MiB | 33.23k/s (33.01–33.47k) | 33.22k/s | 7.08 s | 6,347 | 97.8 μs |
| C | V8 | `fallocate` + 512 MiB zero initialization | 43.14k/s (42.14–43.29k) | 32.50k/s | 6.88 s | 6,358 | 1.4 μs |
| D | Native V7 | V7 runtime preparation overlaps the run | 49.39k/s (45.94–49.55k) | — | 5.91 s | 8,194 | 15.1 μs |

The preparation-inclusive V8 rate is derived from the 100,000-put timed
duration plus the recorded preparation duration; it is not a separate
end-to-end benchmark. B's preparation took a median 0.25 ms. C's 512 MiB of
aligned zero writes took a median 759 ms. In C, `fallocate` allocated
536,875,008 bytes and the direct zero writes initialized 536,870,912 bytes.
The measured active WAL began at offset 8,192 bytes. These V8 values were
exported in benchmark JSON and verified against the actual file metadata.

The timed medians show B +56.4% and C +103.0% over A. C was +29.8% over B
while preparation was excluded, but its preparation-inclusive equivalent was
2.2% below B. C remained 12.6% below native V7 on timed throughput. Mean
`fdatasync` is the per-run aggregate `fdatasync` time divided by sync count,
then summarized by the median. Sync groups covered a median 13.55, 15.76,
15.73, and 12.20 tickets for A–D respectively.

## Kernel diagnostics

Separate one-run diagnostics used bpftrace on the
`io_uring_queue_async_work` tracepoint and an `ext4_file_write_iter` return
probe filtered for `-EAGAIN`. Both probes were filtered to the WAL I/O worker;
the io-uring count below is for write opcode 23. Counts matched between probes
in each run:

| Arm | io-wq async-work events | ext4 `-EAGAIN` returns |
| --- | ---: | ---: |
| A: V8, no extra preparation | 100,000 | 100,000 |
| B: V8, `fallocate` | 1,253 | 1,253 |
| C: V8, `fallocate` + initialize | 1,280 | 1,280 |
| D: native V7 | 311 | 311 |

These are one-run diagnostic counts, not the five-run throughput sample. The
matching counts support ext4 NOWAIT fallback as a major part of the unprepared
V8 path, while the residual events in B/C show that preallocation does not
eliminate all fallback. The trace does not establish that every async-work
event has only one cause.

A separate completion-timeline run sampled 782 requests per V8 arm, every 128th
request, with zero dropped samples. Submit to userspace CQE observation was:

| Arm | p50 | p95 |
| --- | ---: | ---: |
| A: no extra preparation | 254.4 μs | 373.8 μs |
| B: `fallocate` | 43.1 μs | 72.9 μs |
| C: `fallocate` + initialize | 37.3 μs | 62.4 μs |

The result-send to reaper p95 stayed around 18–22 μs across these arms. The
reaper-to-prefix-ready p95 fell from 232.1 μs in A to 4.1 μs in B and 4.3 μs
in C. This points to the DATA completion path and its effect on prefix
formation, rather than a long reaper handoff. Submit-to-CQE observation still
combines io-wq scheduling, filesystem work, device service, and userspace
observation; it does not isolate NVMe service time. Traced timings are
diagnostic and are not reported as unprofiled scores.

## Interpretation and follow-up

The evidence moves the leading explanation from generic completion
coordination toward ext4 work caused by extending or converting the active WAL
file's extents. `fallocate` alone removes most `-EAGAIN`/io-wq events and gives
a large timed gain with negligible setup cost. Initializing the extents gives
another timed gain and sharply reduces aggregate sync time, but the full
512 MiB zero pass is expensive when charged to one run.

This does not qualify a production change. The benchmark preallocates one
512 MiB suffix before timing, while V7 prepares bounded regions concurrently
with writes. A next experiment should implement a benchmark-only bounded
preparation window with a `prepared_file_end` guard. That follow-up has now
compared no extra preparation with sliding `fallocate` at 1 MiB and 4 MiB
windows, charging allocation calls during the workload. The measured gain is
large, but the matrix still needs writer-count and batch-workload coverage
before any production adoption.

## Implementation, validation, and provenance

- The benchmark-only controls are `TOYKV_BENCH_V8_EXTENT_PREP=fallocate|initialize`
  and `TOYKV_BENCH_V8_EXTENT_PREP_BYTES`; DATA must be O_DIRECT, the preparation
  range must be within the configured segment, and the benchmark records
  preparation mode, range, allocated/initialized bytes, and elapsed time.
- Crash recovery passed for 3 extent modes × 2 FRONTIER modes × 4 crash points.
  The test verifies that recovery normalizes the physical file length to the
  recovered logical image length.
- `cargo check -p kv-engine --features bench --all-targets`,
  `cargo clippy -p kv-engine --features bench --all-targets -- -D warnings`,
  the extent-mode parser unit test, the crash-recovery test,
  `cargo fmt --all -- --check`, and `git diff --check` passed.
- Source base: `30ebec419f635110541d18e9815643db8b9227f5`; benchmark-only Rust
  patch SHA-256: `84247c0c0a9fcf23d4bb1d020969375d7aa4780c7d5b8de5158a8d2ac752c37e`;
  release benchmark binary SHA-256:
  `f684227706aadf6dd53d89d54b21d900a26f04283d85f3477133fecc969d26e7`.
- Raw benchmark JSON and diagnostic traces were copied to a local temporary
  result directory and are not included in the PR. No tar archive is added to
  GitHub.
- Terraform removed the benchmark VM, its auto-delete boot disk, SSH firewall,
  subnet, VPC, and lifetime marker. Independent project-scoped queries found
  zero matching VMs, disks, firewalls, subnets, VPCs, or routes; Terraform state
  is empty.

No production default is changed by these results.
