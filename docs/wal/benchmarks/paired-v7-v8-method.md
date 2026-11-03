# Paired V7/V8 diagnostics with explicit storage state

The P41 Plus can change between a roughly 30 µs and roughly 640 µs
post-flush first-write state. CPU attribution and real-device qualification
therefore need separate conclusions. This method supplements the existing
RFC gates; it does not replace them or promote slow-state captures into
normal best-score tables.

## Three questions

| Level | Question | Evidence |
|---|---|---|
| Algorithm | Does preparation do less CPU work? | CPU-only codec measurements, matching core placement and canonical bytes |
| Runtime | Does coordination/publication cost less under the same completion pattern? | Controlled completion replay or an isolated runtime benchmark; ordinary SSD runs cannot establish this control |
| Device | Does real durability improve throughput and p99? | Same executable, boot, CPU mask and storage-probed reversed comparisons |

Phase clocks from a device run locate work, but storage stalls can change
publication contention, CPU work and context switches. Those clocks remain
diagnostic when device matching fails. They do not by themselves prove a
runtime regression or saving. A tmpfs throughput comparison also does not
provide a matched completion pattern or SSD durability evidence.

## Runner

[paired_v7_v8.py](paired_v7_v8.py) reuses the preserved producer-attribution
executable and its actual V7/V8 codecs/runtime. Both formats run with sixteen
writers, batch64, 1 KiB values, no compaction and a 2 GiB SST target. The same
whole-process CPU mask, `0-15`, applies to every workload child. This is a
placement control, not a production affinity policy. The probe uses CPU 8.

The runner executes two fixed blocks:

```text
probe → V7 → probe → V8 → probe → V8 → probe → V7 → probe
probe → V8 → probe → V7 → probe → V7 → probe → V8 → probe
```

Each probe overwrites the first 4 MiB of the existing initialized disposable
8 MiB file using 1,024 aligned 4 KiB O_DIRECT writes and fdatasync every four
writes. It records raw write/sync latencies, post-flush first-write median
and p99, writes over 5 ms, sync median/p99/count and physical diskstats. It
never writes to a raw namespace or changes device features. Probes themselves
perform durable I/O and may affect the device; they are identical across
positions, rather than assumed to be passive observations.

Binary SHA256, boot ID, masks and the policy are saved before measurement.
Benchmark JSON, stderr and sampled attribution JSON stay in RAM until a
whole block finishes; attribution writes into an inherited memfd. Database
setup/close remains outside the benchmark's timed interval. No build, test,
admin query or separate profiling job runs alongside the captures.

An example using the current preserved executable is:

```bash
python3 -B docs/wal/benchmarks/paired_v7_v8.py \
  --binary target/rfc-026-v8-producer-attribution-20261102/write-perf-producer-attribution \
  --scratch-file target/rfc-026-v8-background-affinity-20261102/standalone-direct-probe.data \
  --output-dir target/rfc-026-v8-state-paired-20261103
```

The scratch file must already exist, belong to the current user, and contain
the known repeating byte pattern. Output must be a new directory under the
local repository's `target/`. The runner uses no elevated device operations.
An instrumented executable with the archived attribution hook is required;
an ordinary `write-perf` executable cannot supply the phase record. Binary
identity/build instructions are in the
[producer attribution report](rfc-026-v8-producer-attribution-20261102.md).

## Classification and rejection

These empirical thresholds describe this P41 Plus, not other SSDs:

| Post-flush first-write median | Label |
|---|---|
| Below 100 µs | `fast` |
| 100 µs to below 500 µs | `intermediate` |
| At least 500 µs | `slow` |

A probe is `transitioning` if the four consecutive quarters disagree on
state, or their medians differ by more than both 50 µs and 25% of the lower
median. A block is rejected if any probe transitions, if states differ, or
if any later probe drifts from the first beyond these predeclared limits:

| Metric | Absolute allowance | Relative allowance |
|---|---:|---:|
| Post-flush median | 50 µs | 25% |
| Post-flush p99 | 150 µs | 50% |
| Sync median | 50 µs | 35% |
| Sync p99 | 250 µs | 50% |
| Writes over 5 ms | 8 writes | 50% |

Reject only when the difference exceeds **both** allowances; relative
differences use the lower endpoint. An incomplete block, a binary/boot/mask
mismatch or an incorrect format order also rejects its comparisons. Raw
measurements remain available for every rejected block.

All probes in a compatible block have the same observed state. This is
weaker than proving identical internal controller state: a transition and
recovery entirely inside one workload can escape endpoint probes. Whole-run
sync/frontier timings and the existing storage flags remain visible. Neither
a compatible slow block nor this instrumented runner establishes RFC
performance qualification. The existing p99 >10 ms or mean fdatasync >1 ms
filter remains an independent field on every run.

## Reading results

Only a compatible block gets V8/V7 ratios. Compare adjacent pairs in both
directions, retaining their individual ratios and their geometric mean.
Never select an unrelated V7 winner and V8 winner to infer a code effect.
Historical best scores remain context and are kept outside this comparison.
Do not merge different storage states. Agreement across the two reversed
blocks matters more than one attractive run.

Reports include process CPU, context switches, reported commit/solo groups,
buffers/reported group, worker notification requests, eventfd writes, syncs
and sampled phase CPU/wall estimates. Counter meanings must be checked:
V8 currently records commit groups at FRONTIER publication, whereas the
ordinary parallel V7 path records DATA groups. Those counts and their buffer
ratios cannot directly compare DATA batching across formats. Worker
notification requests are reported separately; they are not actual eventfd
writes. A phase with zero observed calls is uninstrumented or unobserved,
rather than evidence of zero CPU cost.
Scopes are inclusive and are scaled separately per thread; do not sum a
parent with its children. This runner does not collect hardware counters, so
cycles/instructions are explicitly absent and it makes no IPC claim. CPU
and runtime work still need further isolation before changing production code.

Recompute saved summaries without any benchmark or device probe:

```bash
python3 -B docs/wal/benchmarks/paired_v7_v8.py \
  --analyze-dir target/rfc-026-v8-state-paired-20261103
python3 -B -m unittest discover -s docs/wal/benchmarks -p test_paired_v7_v8.py
```
