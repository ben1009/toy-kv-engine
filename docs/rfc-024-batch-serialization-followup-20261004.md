# RFC 024: batch serialization follow-up, 2026-10-04

No additional production change is retained. Three isolated experiments
targeted batch preparation, ordered packing, and skip-list publication. None
produced a qualified throughput gain. The previously validated
[large-batch extent preparation](rfc-024-large-batch-preallocation-20261004.md)
remains intact, including its +18.3% paired batch64/16-writer result against
the baseline used in that confirmation. This round does not establish the
RFC's leader-relative adoption gate.

## Protocol and results

Native io_uring outside the sandbox, fresh databases on NVMe-backed ext4,
PITR off, release builds with `bench`, 16 writers, batch64, 1 KiB values,
262,144 logical puts, and a 1 GiB SST target. Each candidate had a separate
fixed screen: three warmups, three ABBA/BAAB/ABBA blocks with two observations
per arm per block, and two unchanged leader reference runs. Executables and
outputs were staged in RAM, with five seconds idle after each run. Builds,
tests, profiling, and device administration did not overlap scored timing.

| Candidate | Paired median throughput | Paired median p99 | Passing repeat controls | Decision |
| --- | ---: | ---: | ---: | --- |
| Prepare keys and values before the MVCC write mutex | +1.6% | +2.7% | 0/3 | Revert |
| Dedicated ordered packer for large batches | +3.2% | -2.0% | 0/3 | Revert |
| Reuse an epoch pin during bounded batch publication | -1.7% | +1.1% | 0/3 | Revert |

Changes are medians of three block-level geometric-mean candidate/baseline
ratios. A repeat control requires both arms' within-block throughput spread
at most 5% and p99 spread at most 10%. All observations, including latency
cliffs, are included. The failed controls prevent interpreting small positive
screening differences as established improvements. None advanced to a guard
confirmation or an adoption decision.

The preparation screen encountered stalls in all three warmups, including
the unchanged baseline and leader. Its final scored block also contained a
cliff in both parallel arms. The other screens varied without those large
cliffs but still failed the fixed repeat controls. These observations do not
identify the cause of the existing storage instability.

## MVCC preparation

Temporary instrumentation separated acquisition of the MVCC write mutex,
preparation under that mutex, and the remaining WAL/deferred-write phase.
These are aggregate per-thread wall times, not CPU time or elapsed time saved.
The diagnostic executable was separate from every scored executable.

| Diagnostic run | Write mutex acquisition | Preparation under mutex | Remaining WAL phase |
| --- | ---: | ---: | ---: |
| Parallel, 16 writers | 954 ms | 73 ms | 162 ms |
| Parallel, 8 writers | 302 ms | 69 ms | 150 ms |
| Leader, 16 writers | 793 ms | 92 ms | 146 ms |
| Parallel, 16 writers, repeat | 980 ms | 72 ms | 162 ms |

The candidate prepared publication keys and values before acquiring the
mutex for parallel batches with at least 64 entries. It then reserved the
commit timestamp and stamped every internal key under the mutex before WAL
admission. Allocation failure could not consume a timestamp or WAL ticket.
Its screen did not demonstrate that reducing the preparation portion removed
the observed mutex wait.

## Ordered packing

The baseline packed from the client thread after admission. Its scored runs
formed 4,096 one-buffer groups. The experiment handed batches of at least
64 KiB to a dedicated ordered packer, allowing the client to release the MVCC
write mutex before packing. With multiple unacknowledged tickets, the packer
used a fixed 64 microsecond gather deadline and the existing eight-ticket
group limit. A single synchronous writer did not receive the gather delay.

The existing synchronization cutoff and deadline were unchanged. Packing
continued to use admitted lengths and ticket-ordered physical offsets;
poison, completion, durability, and buffer ownership used the existing paths.
The prototype added park/wake and join handling for close and failures.

| Six-run median | Baseline | Packer candidate |
| --- | ---: | ---: |
| Logical puts/s | 541,054 | 557,791 |
| I/O groups | 4,096 | 1,989 |
| Buffers per group | 1.00 | 2.06 |
| Sync calls | 657 | 599 |

Packing reduced the median sync count by 8.8%, but the paired throughput
blocks were -3.2%, +4.8%, and +3.2%, with no passing repeat controls.
The first scored candidate run had six groups and 13 write SQEs at peak;
the corresponding baseline had 13 groups and 13 write SQEs. Combining
batches reduced the group count without increasing outstanding writes.
Neither counter measures device queue depth. The extra thread and gather
delay are not justified by this screen.

## Epoch pinning

The locked dependency versions are `crossbeam-skiplist` 0.1.3 and
`crossbeam-epoch` 0.9.18. `SkipMap::insert` pins an epoch guard, and entry
release can pin again. An outer pin lets nested pins reuse the thread's
existing pinned state instead of repeating its full pinning fence.

The candidate scoped that outer guard to memtable publication for parallel
WAL batches of 2–128 entries. WAL durability waits occurred before the guard
was acquired. Single-entry writes, larger batches, and other WAL modes used
the existing behavior. The throughput blocks were -1.7%, -4.6%, and +1.5%;
this screen did not establish an end-to-end benefit.

## Validation and restoration

The preparation candidate passed `cargo make check` outside the sandbox:
formatting, default and all-feature Clippy, dependencies, typos, and 1,422
tests with no skips. One compaction test required a retry. Its temporary
regression test covered mixed entry kinds, timestamp stamping, delayed
publication, and WAL recovery. The packer candidate passed all 104 selected
parallel tests; the epoch candidate passed all 196 selected parallel and
memtable tests. These selected runs are not full-suite validation of the
rejected prototypes.

All 51 raw screen records were checked for operation count, mode, reported
rate, completed write CQEs, and buffer count. Disposable databases and RAM
staging directories were removed. Rejected patches, source snapshots,
executable hashes, protocols, logs, and raw records remain under:

- `target/rfc024-batch-preparation-20261004/`
- `target/rfc024-batch-preparation-screen-20261004/`
- `target/rfc024-batch-packer-20261004/`
- `target/rfc024-batch-epoch-20261004/`

All tracked Rust sources were restored and compared against `HEAD`.
Formatting passed, and the normal release executable was rebuilt and
verified against the saved baseline SHA-256:
`9d9091390f8ba7fdff6f2aaf5c0846a6701b647c6e94c5c571e50d4594690031`.
