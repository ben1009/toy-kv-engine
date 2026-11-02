# RFC 026 producer preparation: shared encoding work and hybrid-core hashing

## Result

The producer-preparation investigation changes the interpretation of the
earlier 2.09 s V8 encoding figure. That scope includes logical allocation,
canonical encoding, entry CRC, payload BLAKE3 and header/batch finalization.
V7 also performs allocation, canonical encoding and entry CRC, before its
separately measured ordered SHA. Comparing all of V8's encoding with only
V7's 0.84 s ordered hash overstates the additional V8 work.

The clearest new finding is **core-dependent hash cost** on this Intel Core
i9-13900T. In the isolated batch64 probe, BLAKE3 is faster than the SHA-256
comparison on a P-core, but slower on an E-core. Both digest implementations
are unchanged. Live CPU samples confirm that producer BLAKE3 and the ordered
V7 packer run on different mixtures of core classes.

No new optimization is retained. All temporary instrumentation is removed,
and the runtime is restored byte-for-byte to `b8da402a`, including the kept
notification streams, single durability driver and published cursor. No
digest, wire format, grouping, sync policy or production affinity changes.

## CPU-only decomposition

The probe uses the real canonical encoder and CRC/BLAKE3 implementations,
12-byte prefixed keys and 1 KiB values. It validates that the copy-only
diagnostic clone emits identical canonical bytes; the normal and observer
paths emit the same bytes/CRC and the expected entry digest. V7/V8 logical
batch bytes match, and generated V8 frames pass structural/CRC validation.
No live writer calls the copy-only clone or bypasses its checksum.

CPU 8 belongs to the kernel's `cpu_core` mask `0-15`; CPU 31 belongs to
`cpu_atom`, `16-31`. The
[topology](artifacts/rfc-026-v8-producer-attribution-20261102-cpu-topology.json)
and [masks](artifacts/rfc-026-v8-producer-attribution-20261102-core-masks.json)
are archived. The initial probe ran on CPU 31. A separate fixed follow-up
runs CPU 8, CPU 31, CPU 31, CPU 8. Each invocation has one warmup and seven
rounds with reversed stage order. Each stage repeats 2,000 times with two
clock reads around the entire repeated stage. Buffers and canonical indexes
are reused; native frame buffers retain 4 KiB alignment.

The table shows the median of 14 scored rounds per core for a batch of 64:

| Stage | P-core CPU 8, µs/batch | E-core CPU 31, µs/batch |
|---|---:|---:|
| Canonical copy/validation, diagnostic CRC omission | 2.423 | 5.945 |
| Canonical encode + entry CRC | 5.871 | 10.489 |
| Canonical encode + CRC + inline BLAKE3 | 21.054 | 43.326 |
| BLAKE3 entry-stream digest alone | 15.154 | 32.659 |
| Standalone SHA-256 of logical batch | 26.060 | 26.489 |
| Physical frame CRC alone, all 17 frames | 2.978 | 3.982 |
| V7 physical frame codec | 4.283 | 8.056 |
| V8 physical frame codec | 4.271 | 8.551 |

The standalone SHA probe resets/finalizes a hash per batch; V7's live rolling
hash is measured separately below. The BLAKE3 probe computes the RFC entry
digest, including its domain and length. These are isolated primitive costs,
not interchangeable protocol digests or end-to-end estimates.

For this fixture, the CRC-enabled/copy-only difference is approximately
3.45 µs on CPU 8 and 4.54 µs on CPU 31. Inline BLAKE3 adds approximately
15.18 and 32.84 µs respectively. This identifies hash work as a larger
increment than entry CRC in the isolated encoder. Microbench timings cannot
be subtracted from live scope estimates or converted directly into SSD gains.
The frame-codec rows include their CRC and must not be added to frame CRC again.

All **840 CPU-only samples**, including the initial CPU-31 screen, are in the
[raw archive](artifacts/rfc-026-v8-producer-attribution-20261102-micro-raw.json).
The [summary](artifacts/rfc-026-v8-producer-attribution-20261102-micro-summary.json)
retains sample counts, ranges and medians for 2-, 8- and 64-entry batches.

## Live WAL attribution

The live workload is Parallel WAL, 750,000 puts, batch64, 1 KiB values,
no compaction, a 2 GiB SST target and latency sampling every ten batches.
All databases and captures stay in this repository on ext4/Solidigm P41 Plus.
Each cohort starts with a fixed 45-second cooldown and uses three-second gaps.
No compiler, tests or other probe overlaps a measured invocation.

Temporary whole-batch scopes sample one call per 64 per thread, using thread
CPU and monotonic wall clocks. Estimates scale each thread's sampled phase
separately by its actual call/sample counts. Scope timings are inclusive and
include probe overhead. Allocation/entry rows below are children of encoding,
and BLAKE3 is a child of entry encoding. **Do not add these nested rows.**
No allocation wrapper or per-field CRC timer is installed. Allocation counters
in the reused sampler are disabled/unmeasured. Detailed WAL `--profile`
diagnostics remain off. Existing ticket-sampled digest metrics remain enabled.

The initial cohort has eight diagnostic invocations at one/sixteen writers
in reversed format/workload order, followed by four normal-counter controls
at sixteen writers. `perf cpu-clock:u` samples at 499 Hz, only during timed
writes. Recordings remain in a Linux memfd until measurement finishes.
Initial recordings omit CPU IDs; stack export was retried without that field.
One fixed diagnostic follow-up adds `--sample-cpu` for V8 then V7 at sixteen
writers, using the same executable and parameters. It observes placement
without pinning or changing live writers. All **14 live measurements**, of
which **10 are stall-flagged**, remain in the
[measurement archive](artifacts/rfc-026-v8-producer-attribution-20261102-measurements.jsonl).

### One writer: encoding is shared work

These are the initial cohort's eligible V7/V8 samples, `00-diagnostic-v7-1`
and `06-diagnostic-v8-1`. Each format has only one eligible sample out of two.
They are diagnostics, excluded from performance rankings.

| Measured CPU component, seconds | V7 | V8 |
|---|---:|---:|
| Canonical preflight | 0.131 | 0.109 |
| Encoding parent | 0.514 | 1.473 |
| Logical allocation/zeroing, inside encoding | 0.194 | 0.514 |
| Entry encoding + CRC, including V8 observer hash | 0.284 | 0.870 |
| V8 payload BLAKE3, inside entry encoding | — | 0.626 |
| V7 ordered logical SHA, separate from encoding | 1.147 | — |
| Physical frame materialization + CRC | 0.722 | 0.277 |
| Native aligned allocation, all buffer types | 0.086 | 0.156 |

V8 has no post-encode full payload reread. Its payload hash covers exactly
787,500,000 bytes. The V8 hash observation is a child of the encoding row;
V7's separately observed ordered SHA must also be considered before calling
the larger V8 encoding parent an equivalent-sized CPU regression.

### Sixteen writers: producer cost does not explain the whole process gap

The two CPU-aware follow-up captures have the following estimates. **Both
are stall-flagged**: p99 is 29.763/37.632 ms for V8/V7 respectively, with
mean sync 1.931/1.620 ms. These figures locate work under those conditions;
they are not a clean throughput or causal format comparison.

| Measured CPU component, seconds | V7 | V8 |
|---|---:|---:|
| Encoding parent | 0.988 | 2.249 |
| Logical allocation/zeroing, inside encoding | 0.505 | 0.788 |
| Entry encoding + CRC, including V8 observer hash | 0.425 | 1.319 |
| V8 payload BLAKE3, inside entry encoding | — | 0.956 |
| V7 ordered logical SHA, separate from encoding | 1.132 | — |
| Physical frame materialization + CRC | 0.972 | 0.454 |
| V8 ordered digest link | — | 0.015 |
| Memtable publication | 2.279 | 2.777 |
| MVCC publication | 0.706 | 1.150 |
| Total measured process CPU | 10.287 | 11.812 |

Encoding, physical framing and V7's ordered SHA are disjoint CPU scopes.
Their approximate sum is 3.09 s for V7 and 2.70 s for V8 in this diagnostic
pair; V8 encoding already includes its payload hash. This is incomplete
pipeline attribution and excludes other work, including native allocation,
admission and publication. Wall times overlap and must not be summed this way.
It does not establish a source speedup, but it does reject treating V8's entire
encoding parent as additional work absent from V7.

The initial sixteen-writer diagnostic pairs show V8 encoding at 2.05–2.30 s,
of which payload BLAKE3 estimates are 0.95–0.97 s and logical allocation/zeroing
0.73–0.85 s. Both V8 samples are flagged. Allocation/zeroing is a concrete
producer-path difference worth isolating; its measured cost does not imply
that changing allocator or buffer initialization will pass an SSD gate.

## Core placement and update sizes

The CPU-aware recordings observe 186 E-core and 164 P-core samples in V8's
bulk BLAKE3 leaf, across sixteen threads. **53.1% of these CPU samples are on
E-cores.** V7's ordered-packer SHA subset, matched by the thread ID with observed
logical-hash calls, has 99 E-core and 463 P-core samples: **17.6% E-core**.
The [placement summary](artifacts/rfc-026-v8-producer-attribution-20261102-cpu-sample-summary.json)
and [analyzer](artifacts/rfc-026-v8-producer-attribution-20261102-analyze-cpus.py)
preserve the counts. These are sample shares, not the fraction of jobs/bytes
assigned to each core type. They do not prove that placement accounts for the
entire process-CPU difference. The fixed-core microprobe supplies the separate
controlled observation that hash cost differs between these two cores.

Bulk hashing executes `_blake3_hash_many_avx2`; the SSE4.1 compression routine
also appears. The data does not support a portable-only BLAKE3 fallback.
CRC samples include both the baseline short-update routine and accelerated
PCLMUL processing. The investigation does not replace either primitive.

The entry observer is already batched. A full batch emits 63 entries before
crossing the 64 KiB threshold: one 66,150-byte update followed by 1,050 bytes.
The 16-writer histogram contains 11,712 updates above 64 KiB, 11,712 updates
at most 4 KiB and sixteen bounded final-batch updates, totaling 787,500,000
bytes. There is no per-field BLAKE3 update stream to eliminate. A threshold
checked after a complete entry can produce a chunk slightly larger than 64 KiB.

## Performance status and next decision

The normal-counter control has one eligible V8 run at 477,784 puts/s,
3.788 ms p99 and 8.947 s process CPU; neither V7 control is eligible. It does
not supply a new V7/V8 performance comparison. Diagnostic measurements do not
replace the existing best-score tables or close the unresolved eight-writer
guard from the notification rerun.

Keep the notification optimization and stop notification-policy tuning as
requested. This attribution does not justify a digest/RFC change, checksum
removal or production CPU affinity. The useful next producer experiment is
to isolate logical-buffer allocation/zeroing while preserving fallible
post-admission error handling and all emitted bytes. Any retained change
still needs normal-binary throughput/p99 checks; the CPU-only results alone
do not authorize a performance claim.

## Verification and reproduction

The temporary build passes **112 focused tests**, covering V8 writers,
concurrency and crash boundaries, V7 codec conformance and canonical entry
streams. All microprobe byte/CRC/digest assertions pass. The retained runtime
is then restored from checked source snapshots, with matching SHA256 values;
its earlier full `./dev check` passed 2,151 tests with 32 skipped. This
investigation introduces no runtime change that requires repeating that suite.
The [check record](artifacts/rfc-026-v8-producer-attribution-20261102-checks.json)
also records passing formatting, typo, artifact-integrity and reproduction-patch
checks after restoration.

The [temporary attribution patch](artifacts/rfc-026-v8-producer-attribution-20261102-producer-attribution.patch)
applies to `b8da402a`. Build with
`cargo build --release --package kv-engine --features bench --bin write-perf`.
The diagnostic executable SHA256 is
`fbb90ea8a6f20bf476760e79ae60e04cda3edae04138d45ae5a9e6346da299f1`.
The unchanged normal-counter executable is the notification/cursor build
with SHA256 `937a3843a044e107e15db756674be151875e388ff3a0a392cbd4e97463bbd2aa`.

The [live plan](artifacts/rfc-026-v8-producer-attribution-20261102-plan.json),
[CPU-aware follow-up plan](artifacts/rfc-026-v8-producer-attribution-20261102-cpu-followup-plan.json),
[micro plan](artifacts/rfc-026-v8-producer-attribution-20261102-micro-cores-plan.json),
[live runner](artifacts/rfc-026-v8-producer-attribution-20261102-run.py),
[CPU-aware runner](artifacts/rfc-026-v8-producer-attribution-20261102-cpu-followup.py),
[micro runner](artifacts/rfc-026-v8-producer-attribution-20261102-micro-cores.py)
and [archive script](artifacts/rfc-026-v8-producer-attribution-20261102-archive.py)
are preserved. Paths refer to the local
`target/rfc-026-v8-producer-attribution-20261102/` directory and need adjustment
elsewhere. Complete original JSON, CPU scopes, perf recordings, stacks, build
and test logs remain there. This documentation update adds the findings to
PR #532's existing branch; all temporary runtime probes have been removed.
