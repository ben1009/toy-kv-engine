# RFC 024: retire frozen parallel WAL runtimes

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

The parallel WAL now closes its dedicated runtime when its memtable freezes,
and after recovery classifies a memtable as immutable. This fixes the worker
and ring buildup found in the [qualification run](benchmarks/rfc-024-wal-qualification-20261001.md).
The before/after measurements below do not establish a throughput improvement
or satisfy the full RFC adoption gate. Leader remains the default.

## Change and safety boundary

Freeze holds the active-memtable write guard. Engine writers hold the matching
read guard through WAL durability and MVCC publication, so freeze first waits
for those writers to finish. It then calls the existing parallel runtime close
protocol before publishing the successor:

```text
wait for current writers
    → close admission and capture cutoff
    → drain packer and extent initializer
    → drain/join I/O worker
    → finish final sync and join coordinator
    → publish successor and retain immutable memtable
```

WAL bytes and memtable contents remain available for reads and recovery.
Snapshots may retain old memtables without retaining their running workers.
If close reports an error, freeze returns it and leaves the original memtable
active; it does not publish the newly created successor. Cleanup still follows
the existing runtime close/error ownership contract.

Recovery closes each recovered immutable parallel WAL runtime before retaining
its memtable. It still starts the runtime during WAL recovery; avoiding that
startup is outside this change. PITR and leader WALs use their existing paths.
Frozen WAL handles and direct-buffer pools remain owned by their memtables.

## Measurements

Both arms use parallel mode: A is the prior `925ed07d` runtime, as preserved
at documentation commit `7566a4b1`; B adds only runtime retirement. Both use
the same locked release build with `bench` and nightly `2026-09-23`.

Each filesystem runs the original 200,000 single-put workload with four
writers, 1 KiB values and 1 MiB SST target, plus a control with 50,000 puts
and a 1 GiB SST target that does not rotate during measurement. Each case has
three alternating ABBA/BAAB blocks, for 48 scored runs. Exactly one 50,000-put
warmup per arm per case precedes its first block. All 60 runs completed:
48 scored, eight warmups, and four separate profiles. No results were
discarded or rerun to obtain a faster result.

No builds, tests, tracing, or device administration polling ran during timing.
Latency sampling is every tenth commit. The host uses ext4 on the P41 Plus,
firmware `004C`, and Linux `6.18.9-arch1-2`; tmpfs uses `/dev/shm`.

The table reports the median of three block ratios. Each block compares the
geometric mean of its two candidate runs with its two baseline runs. Lower
p99 and CPU time are better. Within-arm repeat controls require throughput
max/min <=1.05 and p99 max/min <=1.10.

| Case | Throughput change | p99 change | CPU/put change | Blocks passing both repeat controls |
| --- | ---: | ---: | ---: | ---: |
| Tmpfs, original rotation | +1.43% | -12.90% | -2.92% | 0/3 |
| Ext4, original rotation | -0.07% | +2.68% | +1.16% | 1/3 |
| Tmpfs, no rotation | +1.31% | -5.25% | -3.30% | 0/3 |
| Ext4, no rotation | +0.61% | -1.99% | -1.00% | 2/3 |

Tmpfs rotation block throughput changes range from +0.81% to +9.09%; its
p99 improves in every block, but no block passes both repeat controls. Ext4
rotation block ratios are 0.603, 0.999 and 1.000. The first block includes a
candidate run at 3,608 puts/s and 9.90 ms p99, compared with its other
candidate run at 9,861 puts/s and 1.34 ms p99. That large timing swing remains
in the report. The one passing ext4 rotation block is essentially flat.
These three-block estimates are descriptive; there is no confidence interval
or promotion of a selected passing subset.

## Resource profiles

After all scored runs, one separate original-workload profile per arm and
filesystem sampled procfs every 20 ms. These profiles are excluded from
throughput and latency estimates above. Counts below are sampled peaks
before the post-measurement `write profile` stderr marker. The marker follows
profile construction and precedes final drain, with a small parent receive
delay; these are not atomic counts or exact transition maxima.

| Filesystem / arm | I/O workers | Sync workers | Extent workers | Ring descriptors | Peak RSS (MiB) | Peak virtual size (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Tmpfs / prior parallel | 161 | 161 | 0 | 162 | 364.2 | 19,698.0 |
| Tmpfs / retirement | 2 | 2 | 0 | 2 | 357.6 | 3,498.5 |
| Ext4 / prior parallel | 5 | 5 | 5 | 5 | 222.2 | 1,733.8 |
| Ext4 / retirement | 2 | 2 | 1 | 1 | 247.2 | 1,002.0 |

The successor runtime is created before the old runtime closes, so two
runtimes can briefly coexist during rotation. The sampled tmpfs virtual size
falls substantially, while RSS does not. This change retires threads and
rings; it does not release the buffer pools retained by immutable memtables
or promise lower resident memory. Both arms still reach four outstanding
groups and four outstanding write SQEs. Those are software depths, not NVMe
queue depth. This resource issue does not explain the raw-device stalls
documented in the [stall diagnosis](environment/rfc-024-wal-stall-diagnosis.md).

## Validation and evidence

`cargo make check` passed formatting, dependency sorting, default and
all-feature Clippy, unused-dependency and spelling checks, and 1,413 tests
outside the sandbox. Three unrelated test names required nextest retries;
all parallel WAL lifecycle tests passed on their first attempt. The focused
parallel WAL run also passed all 15 tests, including:

- Eight repeated freezes with pinned state snapshots, durable WAL files and
  successful recovery; immutable runtimes remain closed after reopening.
- A writer paused during sync: freeze waits for durability and publication
  before joining its runtime, and writes continue on the successor.
- An injected sync failure: freeze reports close failure and retains the
  original active memtable without publishing a successor.
- Existing rotation, transaction, TTL, delete, range-tombstone, poison-prefix
  and process-level crash coverage.

Local evidence is under `target/rfc024-frozen-wal-retirement-20261001/`:
`run.py`, `protocol.json`, source/build provenance, binary snapshots,
raw stdout/stderr, `runs.jsonl`, `blocks.jsonl`, `summary.json`, and the
SHA-256 artifact manifest. The frozen protocol hash is
`866abfed07807b9cc4c3f14d9c78aacae75c4ba1a30576edf953b6a491616494`.
Baseline binary SHA-256 is
`5a24f243770d3f0f3071bc50e80d95ee7a83f0ae55a191e7817273f817a71514`;
candidate SHA-256 is
`212824f527a01d5a5f51572b9717faa4f3c0ec9a4ac6196d1e4f94f168dbae9a`.

Keep runtime retirement for bounded worker/ring lifetime. The full RFC
performance gate still requires work on single-writer and batch workloads,
and qualified measurements that account for the remaining timing swings.
