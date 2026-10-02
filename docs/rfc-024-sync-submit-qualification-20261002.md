# RFC 024: Synchronous `pwritev` group submission — rejected

**Decision:** Reject. The synchronous submit path is reverted from the tree; it
wins at low concurrency and loses at high concurrency and in the batch tails,
so it does not qualify as a replacement for the ring on the leader path. The
prototype is preserved on the local branch `perf/wal-sync-submit` (commit
`70882df3`, never pushed) for future work; this record is the outcome.

**Run date:** 2026-10-02. Host: Linux 6.18.9-arch1-2, existing ext4 mount and
SSD, `/dev/shm` for tmpfs. Release binary built with `--features bench --bin
write-perf`, snapshotted before timing:

```text
sha256 96692db4c8d49939f263eeacb04fb2c57774ce968864aec5f499315fd43e7883
protocol.json sha256 4144a9aa133b26d72eba3ec8847274a942204a8b25637a2c05d013b70621ec01
```

## Hypothesis

The leader path has one submitter and waits for its group's writes before
`fdatasync`, so the ring's per-write SQE/CQE round trip is pure overhead unless
the filesystem overlaps the group's writes. On tmpfs an `O_DIRECT` write
completes inline in the submitting task.

Raw primitives behind that claim (single thread, tmpfs, preallocated file,
4 KiB-aligned `O_DIRECT`):

| Shape | µs per call | µs per 4 KiB | GiB/s |
| --- | ---: | ---: | ---: |
| `pwrite` 4 KiB | 0.512 | 0.512 | 7.46 |
| `pwrite` 8 KiB | 0.858 | 0.429 | 8.89 |
| `pwrite` 16 KiB | 1.500 | 0.375 | 10.17 |
| `pwritev` 2 iovecs | 0.873 | 0.437 | 8.73 |
| `pwritev` 8 iovecs | 2.819 | 0.352 | 10.83 |
| `fallocate`, 1 MiB increments | 183.8 / MiB | 0.72 per page | — |

Against those, the leader path's profile put 3.16 µs per 4 KiB buffer inside
`io_uring_enter` on the four-writer tmpfs case - about six times the `pwrite`
syscall for the same bytes. That gap motivated the prototype.

## Change under test

`Wal::sync_submit_for` selected, per WAL file, whether a group's writes were
submitted as one `pwritev(2)` over the group's aligned buffers (tmpfs, or
`KV_WAL_SYNC_SUBMIT=1`) or as one io_uring SQE per buffer (`=0`). Offsets,
bytes, and the write -> `fdatasync` -> publish protocol were unchanged, so both
arms ran on one binary.

An earlier exploratory screen on the canonical tmpfs four-writer case measured
a 2.021 median block ratio (three ABBA blocks, 148k/161k vs 350k/324k and
similar pairs, null spread 1.106). That screen had two control arms outside
the repeat screen; this matrix was run to qualify or reject it.

## Fixed comparison

Leader-versus-leader, both arms on one binary, 96 scored runs and
16 warmups over 8 cases. Three ABBA blocks per case, two
scored runs per arm per block, fresh paths, one warmup per arm per case, case
order rotated per block (listed, reversed, rotated by one). Control screen
within each block and arm: throughput max/min <= 1.05 and p99 max/min <= 1.10.
Block ratio is the geometric mean of the two B results over the geometric mean
of the two A results; the case score is the median of the three block ratios,
with a 2.5/97.5 interval from 20,000 seed-24 bootstrap resamples. The decision
rule was frozen in `protocol.json` before timing: adopt only with a median
ratio >= 1.05, every block ratio above 1.0, and at least two passing control
blocks; no case may lose more than 5% median throughput or gain more than 10%
median p99; all blocks stay in the score.

| Case | A (io_uring) | B (sync) | Median ratio | Block ratios | 95% interval | Controls | p99 ratio | Verdict |
| --- | ---: | ---: | ---: | --- | --- | ---: | ---: | --- |
| tmpfs-4w-rot | 166,863 | 351,148 | **2.092** | 2.256, 2.051, 2.092 | [2.051, 2.256] | 1/3 | 0.862 | no decision |
| tmpfs-1w-iso | 97,245 | 322,480 | **3.200** | 3.542, 3.200, 2.922 | [2.922, 3.542] | 0/3 | 0.313 | no decision |
| tmpfs-16w-iso | 159,614 | 120,046 | **0.752** | 0.647, 0.936, 0.752 | [0.647, 0.936] | 0/3 | 1.305 | **regression** |
| ext4-4w-rot | 2,822 | 3,753 | **1.292** | 1.345, 1.060, 1.292 | [1.060, 1.345] | 0/3 | 0.444 | no decision |
| ext4-16w-iso | 13,202 | 14,221 | **1.068** | 1.068, 1.060, 1.590 | [1.060, 1.590] | 1/3 | 0.995 | no decision |
| ext4-1w-iso | 1,777 | 1,794 | **1.018** | 1.018, 1.001, 1.021 | [1.001, 1.021] | 1/3 | 0.955 | no decision |
| ext4-batch64-8w | 150,008 | 150,463 | **0.988** | 1.082, 0.988, 0.975 | [0.975, 1.082] | 1/3 | 1.349 | **regression** |
| tmpfs-batch64-8w-red | 333,615 | 340,342 | **1.056** | 0.967, 1.057, 1.056 | [0.967, 1.057] | 0/3 | 1.301 | **regression** |

Median absolute values are medians of the three block geometric means, in
puts/s. `tmpfs-batch64-8w-red` is the 65,536-put substitute described below.

### Per-case findings

- **tmpfs, one and four writers: large wins.** 3.200 and 2.092 median ratios
  with better tails (p99 ratios 0.308 and 0.875). These are the shapes the
  original regression gate names.
- **tmpfs, 16 writers: regression.** 0.752 median ratio with all three block
  ratios below 1, and a worse tail (1.318). The io_uring arm's six runs ranged
  124,860-200,189 ops/s against the sync arm's flat 117,121-123,237, so the
  ring is recovering real write overlap once many writes are outstanding.
- **ext4: mixed, inside noise at the extremes.** 4 writers 1.292 with a better
  tail; 16 writers 1.068; one writer 1.018.
- **Batch tails: p99 regressions in both filesystems.** ext4 batch64 0.988
  throughput with a 1.304 p99 ratio; tmpfs batch64-substitute 1.056 with a
  1.359 p99 ratio. One large vectored write per group delays the whole group's
  acknowledgement, which shows up in the tail.

### Mechanism

io_uring pays off when many writes are outstanding, where the kernel's async
path overlaps them; it costs a round trip when a group holds one or two
buffers. The leader-path group size is set by concurrency: about 2.1 buffers
per group at four writers against 4.5 (max 16) at sixteen. That makes the
tradeoff a function of group shape, not of the filesystem, so neither a
blanket replacement nor a filesystem-keyed selector is the right axis.

## Environment notes

- `tmpfs-batch64-8w` (524,288 puts) could not run: 14 attempts inside the
  matrix and 4 more in a dedicated re-run failed at io_uring ring creation
  with `ENOMEM`, on both arms and before any write, so the submit mechanism is
  not implicated. The host's 8 MiB `RLIMIT_MEMLOCK` and that case's rotation
  rate are the plausible cause; the same class of failure is already on record
  in the stall diagnosis. The case stays in `runs.jsonl` as failed and was
  replaced by `tmpfs-batch64-8w-red` (65,536 puts, the same count the ext4
  batch case uses), marked as a post-hoc addition in `protocol.json`.
- Control screens passed in only 0-1 of 3 blocks for every case: the host's
  within-arm spread exceeded 5% nearly everywhere. No case therefore reached
  the frozen adopt rule, and the wins above are exploratory despite their size.
- Artifacts: `target/rfc024-sync-submit-qualification-20261002/` -
  `protocol.json` (frozen rule, case list, deviations), `runs.jsonl`,
  `runs-retry.jsonl`, `blocks.jsonl`, `summary.json`, `runner.py`,
  `analyze.py`, and both logs. Unversioned, retained on this host.

## Next steps

The measured shape dependence suggests one follow-up if this is revisited: use
the ring when a group holds many buffers and synchronous submission when it
holds one or two, with the threshold measured rather than assumed, then re-run
the sixteen-writer and batch cases. That is a new candidate with its own
hypothesis, not a retry of this one - and adaptive selectors have been rejected
twice in this record (rotation-bound selection, submission staging), so it
should be qualified against an unchanged control before adoption.
