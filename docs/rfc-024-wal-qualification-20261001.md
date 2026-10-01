# RFC 024: WAL qualification matrix, 2026-10-01

Parallel WAL remains opt-in. The original ext4 workload improved by **171.0%**
in this session's paired blocks, with **48.9% lower p99**, but the full RFC
performance gate is not met: tmpfs single-writer throughput regressed, ext4
batch tails regressed, and slow intervals still affected all three historical
paths. These are current parallel-versus-leader comparisons, rather than
current parallel versus an older parallel implementation.

## Protocol and scope

- Current runtime: `925ed07d`; checkout `08460c63` adds documentation only.
- Current release binary: `--locked --offline --features bench`, pinned
  `nightly-2026-09-23` (`rustc 1.100.0-nightly`, LLVM 23.1.1).
- Pre-PITR `2f556ccb` and earlier candidate `067d4b09` were built in this session
  with the same compiler, unchanged source and identical Cargo.lock. Each
  executable was snapshotted and hashed before timing.
- Filesystems: the host's NVMe-backed ext4 and `/dev/shm` tmpfs. SSD firmware
  remained `004C`, on kernel `6.18.9-arch1-2`. No SSD admin queries, tracing,
  compilation, or storage-setting changes occurred during adoption timing.
- Original case: 200,000 single puts, four writers, 1 KiB values, WAL on, PITR
  off, 1 MiB SST target. Isolated cases: 50,000 single puts or 65,536 puts in
  64-entry batches, at 1/4/8/16 writers, with a 1 GiB SST target.
- Single-put latency samples every tenth commit; batch latency samples every
  commit. Throughput counts puts in both cases; batch latency counts an entire
  64-entry commit, not one key.
- Three ABBA or BAAB blocks per case, with rotated case order and one fixed
  warmup per case. Both arms run twice within each block. The frozen repeat
  screen requires throughput max/min <= 1.05 and p99 max/min <= 1.10 within
  each arm. All blocks, including failures of that screen, remain in the score.
- 312 main comparison runs, 18 historical-triplet runs, and 26 warmups
  completed. The scored window lasted 2,393 seconds. Separate diagnostics
  comprise 14 matrix profiles and two later lifecycle checks.
- The CLI rejects parallel mode with PITR enabled. PITR controls therefore
  compare current leader against the earlier `067d4b09` leader with PITR on;
  they are not parallel-PITR adoption scores.

A block ratio is the geometric mean of the two B results divided by the
geometric mean of the two A results. Tables report the median of all three
block ratios. Intervals use 20,000 seed-24 percentile bootstrap resamples of
the three blocks. With so few blocks they are exploratory and conditional
on the observed data; they are not strong evidence of 95% coverage under
the host's intermittent stalls. No passing-control subset replaces the full
matrix. Short batch runs have only 1,024 completed commits and need longer
repeats before making fine-grained tuning claims.

## Current parallel / current leader

Ratios above one favor parallel for throughput and worsen its p99. The final
column counts blocks passing both repeat-spread checks.

| Filesystem | Case | Writers | Throughput change | Exploratory throughput interval | p99 change | Controls |
| --- | --- | ---: | ---: | --- | ---: | ---: |
| ext4 | Isolated, single puts | 1 | +66.1% | 1.644–2.358 | -36.2% | 2/3 |
| tmpfs | Isolated, single puts | 1 | -37.7% | 0.596–0.629 | +63.4% | 0/3 |
| ext4 | Isolated, batch64 | 1 | +0.8% | 0.999–1.010 | +42.1% | 2/3 |
| tmpfs | Isolated, batch64 | 1 | -4.0% | 0.811–1.004 | -4.1% | 0/3 |
| ext4 | Isolated, single puts | 4 | +177.4% | 2.730–2.835 | -59.3% | 3/3 |
| tmpfs | Isolated, single puts | 4 | +9.1% | 0.950–1.161 | +10.1% | 0/3 |
| ext4 | Isolated, batch64 | 4 | +15.9% | 1.154–1.364 | -7.3% | 0/3 |
| tmpfs | Isolated, batch64 | 4 | +4.5% | 0.959–1.051 | +2.9% | 0/3 |
| ext4 | Original, rotation | 4 | +171.0% | 2.687–2.730 | -48.9% | 3/3 |
| tmpfs | Original, rotation | 4 | -25.3% | 0.728–0.782 | +47.0% | 0/3 |
| ext4 | Isolated, single puts | 8 | +160.3% | 2.584–2.642 | -54.4% | 2/3 |
| tmpfs | Isolated, single puts | 8 | +5.4% | 1.022–1.062 | +6.7% | 2/3 |
| ext4 | Isolated, batch64 | 8 | -3.1% | 0.903–0.983 | +16.7% | 1/3 |
| tmpfs | Isolated, batch64 | 8 | +21.1% | 1.062–1.244 | -8.1% | 0/3 |
| ext4 | Isolated, single puts | 16 | +111.5% | 2.111–2.199 | -48.7% | 3/3 |
| tmpfs | Isolated, single puts | 16 | +40.3% | 0.934–1.502 | -29.1% | 1/3 |
| ext4 | Isolated, batch64 | 16 | -2.1% | 0.800–1.000 | -60.8% | 0/3 |
| tmpfs | Isolated, batch64 | 16 | -34.7% | 0.634–0.790 | +72.8% | 0/3 |

The original ext4 blocks produced throughput ratios `2.710, 2.687, 2.730`
and p99 ratios `0.494, 0.511, 0.522`, with 3/3 repeat controls passing.
Across those six runs per arm, median throughput was 3,638 leader versus
9,854 parallel puts/s; median p99 was 2.600 versus 1.324 ms. Those absolute
medians are not the definition of the paired estimate.

The ext4 single-writer batch p99 ratios were `1.421, 1.471, 1.371`; two
blocks passed the controls. The eight-writer batch p99 ratios were
`1.436, 1.167, 1.134`; its third block passed. These exceed the RFC's 10%
tail-regression guard despite the large single-put gains. At 16 writers,
batch tails swung badly in both paths: one leader pair went from 35.2 to
3.0 ms p99, and a parallel pair went from 3.8 to 30.9 ms. Its favorable
median p99 ratio is not a clean latency improvement.

## PITR controls: current / earlier leader

| Filesystem | Writers | Throughput change | p99 change | Controls |
| --- | ---: | ---: | ---: | ---: |
| ext4 | 1 | +0.0% | +0.3% | 3/3 |
| tmpfs | 1 | -3.1% | -8.5% | 0/3 |
| ext4 | 4 | -0.2% | +0.0% | 3/3 |
| tmpfs | 4 | +3.3% | -4.3% | 0/3 |
| ext4 | 8 | -0.2% | +0.1% | 3/3 |
| tmpfs | 8 | +6.0% | -2.0% | 0/3 |
| ext4 | 16 | +0.5% | -0.2% | 3/3 |
| tmpfs | 16 | -5.3% | -3.2% | 1/3 |

All 12 ext4 PITR blocks passed the repeat checks, with median throughput
changes between -0.2% and +0.5%. The tmpfs controls were mostly too variable
for a no-regression claim. PITR profiles showed zero dedicated I/O/sync
workers, one in-flight group, and no later write progress during sync;
they stayed on the legacy path.

## Same-session pre-PITR reference

The ext4 historical triplets rotated the order among all three binaries:

| Order | Pre-PITR puts/s | Current leader puts/s | Current parallel puts/s | Parallel / pre-PITR |
| --- | ---: | ---: | ---: | ---: |
| pre-PITR → leader → parallel | 3,811 | 1,855 | 9,890 | 2.595 |
| leader → parallel → pre-PITR | 3,812 | 3,647 | 4,021 | 1.055 |
| parallel → pre-PITR → leader | 2,356 | 3,618 | 9,867 | 4.187 |

The slow result appeared in the second position of each ext4 triplet,
affecting a different path each time. That positional pattern is an
observation, not proof of a specific device or harness mechanism. The
parallel/pre-PITR ratios span 1.055–4.187; do not claim reliably established
gap closure from their 2.595 median. The historical harness has no commit
latency or measurement CPU fields, so neither was invented or backported.
On tmpfs, all three parallel/pre-PITR ratios were below one (0.550–0.665).
The separate unchanged ext4 single-writer leader dip from about 1,770 to
869 puts/s, with 16.4 ms p99, is also retained. See the
[stall diagnosis](rfc-024-wal-stall-diagnosis.md) for the earlier evidence
and remaining uncertainty.

## Pipeline and resource diagnostics

These runs enable sync observations and are excluded from adoption timing.
Software depth reached 1/4/8/16 outstanding groups and write SQEs in the
isolated ext4 single-put cases. It is not a measurement of NVMe queue depth.

| Ext4 case | Sync calls | Groups/sync | SQEs submitted during sync | CQEs consumed during sync | Groups completed during sync |
| --- | ---: | ---: | ---: | ---: | ---: |
| Single, 1 writer | 50,000 | 1.00 | 0 | 0 | 0 |
| Single, 4 writers | 12,695 | 3.94 | 628 | 71 | 71 |
| Single, 8 writers | 6,410 | 7.80 | 1,152 | 136 | 136 |
| Single, 16 writers | 3,456 | 14.47 | 4,651 | 681 | 681 |
| Batch64, 16 writers | 144 | 7.11 | 460 | 306 | 306 |
| Original, 4 writers | 51,256 | 3.90 | 3,060 | 639 | 639 |

The original ext4 diagnostic advanced its written frontier during 366 of
51,256 sync observations. The sixteen-writer isolated case did so during
92 of 3,456. Thus overlap occurs, but completed groups during sync are a
small part of these single-put workloads; most groups are already written
when each sync starts. This does not prove sustained device overlap or SSD
saturation. At one writer no later operation can be issued before its
synchronous commit returns.

Whole-process procfs sampling initially found up to 163 I/O workers and
163 sync workers in the tmpfs rotation diagnostic. Two separate profiles
checked the stage before the post-timing `write profile` stderr marker:
leader had up to 174 ring descriptors and no dedicated WAL workers;
parallel had up to 164 rings, 164 I/O workers, and 164 sync workers.
The marker follows profile construction and precedes `drain_flush()`, so
there is a small post-timing observation gap. The buildup precedes the final
drain; it is not established as the cause of the measured regression.

Inspection of `force_freeze_with_new_memtable_locked()` confirms that freeze
moves the old memtable to `imm_memtables` without closing its parallel WAL
runtime. Each retained WAL carries its own threads and ring. Worker ownership
is per WAL, not a single pool shared by the engine. Investigate retiring
quiescent runtimes at rotation while retaining WAL data and honoring the
admission cutoff, final sync, file lifetime, and join guarantees. This is a
concrete resource issue; it does not explain the raw NVMe stall previously
reproduced without engine workers.

Raw JSON retains p50/p99, process CPU, solo groups, buffers/group, sync/CQE
counts, notification requests and eventfd writes, preallocation and sync time,
and present WAL file length/allocation. File usage is a snapshot, not total
physical device traffic. Profile JSON retains every captured sync frontier
and overlap observation. Publication-wait time is not exposed separately
by this binary; memtable-insertion time must not be relabeled as that wait.
Actual device queue depth was not measured.

## Gate decision and preserved evidence

| RFC requirement | Outcome in this session |
| --- | --- |
| Original four-writer improvement beyond within-block repeat spread | Observed on ext4, 3/3 control blocks; tmpfs regressed |
| Representative device workload >=10% gain, 95% interval excluding parity | Large ext4 single-put gain; interval remains exploratory with only three blocks and intermittent stalls |
| Single-writer throughput regression <=5% | Failed on tmpfs: -37.7% for single puts |
| p99 regression <=10% across tested matrix | Failed: ext4 batch64 +42.1% at one writer and +16.7% at eight |
| Same-session pre-PITR gap closure | Not reliably established; historical ratio spans 1.055–4.187 |
| PITR stays legacy and has no material regression | Legacy confirmed; ext4 controls near parity; tmpfs confidence limited |
| Crash/prefix validation | Not rerun for this documentation-only session; no runtime changes were made |

Keep leader as default and parallel as opt-in. The next optimization work
should examine retired-WAL resource lifetime and batch tail latency. Longer
batch tests and more independently repeated blocks are needed for tighter
estimates; they must retain the original workload as a separate case.

Local artifacts are under `target/rfc024-qualification-20261001/`:
`protocol.json`, `source-provenance.json`, `build-provenance.json`,
`runs.jsonl`, `blocks.jsonl`, `historical.jsonl`, `summary.json`,
`diagnostic-resume/`, `rotation-lifecycle/`, and the raw stdout/stderr files.
The frozen protocol hash is
`d7b991e560b8b0060375723e9d20b03b18996cd711d625076183aa0a8dad6e87`.
All current runs use binary SHA-256
`5a24f243770d3f0f3071bc50e80d95ee7a83f0ae55a191e7817273f817a71514`.

After all adoption runs completed, the fourth profile hit `/tmp`'s quota
while storing diagnostic output. All 359 completed records were archived
and byte-verified, including 330 scored runs and three profiles. The partial
fourth stdout and failed empty completion file are preserved, and the
remaining eleven profiles completed on `/dev/shm`. This capture failure
is not an engine failure or an omitted scored run. `capture-recovery.json`
records it; `artifact-manifest.json` hashes the archived evidence. A shared
Cargo target-directory reuse initially returned the wrong earlier executable;
it was rejected before timing and rebuilt in a separate target directory.
