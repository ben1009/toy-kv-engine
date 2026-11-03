# Held-out, path-conditioned WAL latency calibration

This follows [extent attribution](wal-extent-attribution-method.md). It changes
only offline measurement analysis. The capture runner, extent helper, executable,
WAL behavior, digest, grouping, sync policy, and affinity remain unchanged.

The question is whether repeatable latency modes **within the same I/O path
class** permit a conditional V7/V8 comparison. A low/high label is local to a
class. It is not proof of a hidden SSD firmware state, and labels from different
classes are not interchangeable. Conditioning on the resulting I/O path can
also exclude real effects of the writer implementation. These diagnostics do
not qualify RFC 026 or establish an unconditional causal performance ratio.

## Declare, train, validate, freeze, evaluate

`wal_state_calibration.py` declares the complete policy and hashes all four
measurement/analysis sources before any study capture. Three separate cohorts
then run once, without state-dependent retries or waiting for a favorable disk:

1. Training: V7 → V8 → V8 → V7, point then 16-writer batch64.
2. Validation: the same schedule, on later data, with no model refitting.
3. Evaluation: the same schedule, starting **after** model freeze.

Point is 20,000 puts, one writer, batch1. Batch64 is 750,000 puts, 16 writers.
Both use 1,024-byte values, Parallel PITR, 100 ms counters, no compaction, a
2 GiB SST target, and CPUs 0–15. Boot, executable hash, kernel/perf, CPU mask,
device layout, workload, capture policy, and instrumentation hashes must match.
No additional probe or device-setting change is made. Each cohort retains all
eight native traces, request timelines, snapshots, and original decisions.
The exploratory `1951dc73` captures cannot enter this study. A trace with lost
or ambiguous events is retained and hashed, but its **entire run** is quarantined
as unknown and contributes no training, validation or performance evidence.
Other traces still face the full shared-format/mode validation requirements.
There are no state- or outcome-dependent capture retries.

A new study directory, plan, and model use exclusive creation. Source or policy
changes require a new study. The frozen model binds the training and validation
file hashes, exact coefficients, policy and source hashes. A fresh evaluation
must follow that freeze on the same boot. `verify` checks the calibration input
hashes and recomputes the coefficients without replacing the frozen model.

```bash
python3 -B docs/wal/benchmarks/wal_state_calibration.py declare \
  --study-dir target/wal-calibrated-states --binary target/release/write-perf
# Repeat the existing capture command with output-dir training, then validation:
sudo env PYTHONDONTWRITEBYTECODE=1 python3 -B \
  docs/wal/benchmarks/capture_wal_trace.py \
  --binary target/release/write-perf --output-dir target/wal-calibrated-states/training \
  --wal-extents --counter-interval-ms 100 --cpus 0-15 --uid 1000 --gid 1000
python3 -B docs/wal/benchmarks/wal_extent_trace.py \
  --capture-dir target/wal-calibrated-states/training
# Capture and analyze validation identically, then freeze:
python3 -B docs/wal/benchmarks/wal_state_calibration.py fit \
  --study-dir target/wal-calibrated-states \
  --training target/wal-calibrated-states/training \
  --validation target/wal-calibrated-states/validation
# Only now capture/analyze evaluation with the same executable and policy.
python3 -B docs/wal/benchmarks/wal_state_calibration.py evaluate \
  --study-dir target/wal-calibrated-states \
  --evaluation target/wal-calibrated-states/evaluation
```

Use the same already-built executable in every command. Native capture and
analysis must run on the actual host; the declaration's monotonic clock and boot
identity must also be native. No builds/tests/analysis run concurrently with the
measurement workload. `PYTHONDONTWRITEBYTECODE` prevents incidental helper cache
files without modifying the frozen capture instrumentation.

## Routing and observations

The routing key contains workload, first namespace-write role/control and size
bucket, FUA/ordinary writes before DATA, DATA size bucket, queue depth at DATA
submission, peak other-command overlap, queue depth at FLUSH completion, and
presence of intervening reads. The supported first-write roles are WAL ordinary,
non-WAL ordinary and non-WAL FUA. Unresolved ownership or other control flags
remain unsupported. Non-WAL does not establish journal ownership.

**Latency is absent from routing.** In particular, the old FLUSH→DATA gap bucket
is not a routing key: otherwise a latency change could move the sample to another
class and hide the transition. Instead, the feature vector is the log1p of the
window medians of first namespace-write latency, WAL DATA latency, and
FLUSH→DATA submission gap. Neither throughput, application latency nor CPU is
an input to fitting or validation. No namespace threshold is inherited from the
older isolated-4KiB classifier.

Features are available only after all required completions. The existing record
lacks the first namespace write's start, so its completion is conservatively
bounded by `DATA submission + first namespace-write duration`. The observation
time is the maximum of that upper bound and DATA completion. Labels cannot
consume a future completion to score an earlier interval.

## Fixed model and rejection rules

Training uses non-overlapping 16-observation windows within each run and class,
with a maximum 250 ms span from earliest FLUSH start to feature availability.
A deterministic weighted L1 two-median fit uses namespace/DATA/gap weights 2/1/1,
quartile initialization and at most 50 iterations. Failure to converge rejects
the class. Sparse or unimodal classes are not forced into two accepted modes.

A class needs at least 12 training windows, at least four per mode and at least
two per format per mode. The high namespace median must be at least four times
the low median, and low p90 times two must be below high p10. Assignment uses a
frozen distance radius and per-feature envelope, plus a minimum nearest-mode
margin. The full numeric policy is in the predeclared JSON and source.

The independent validation cohort must repeat both modes in **both formats**:
at least two windows per format/mode, at least 80% classified coverage, and a
continuous sequence lasting at least 100 ms per format/mode. Otherwise the class
is rejected with its reason. This prevents accepting clusters that merely
identify V7 versus V8, while remaining a repeatability check rather than an
independent observation of controller state.

Evaluation uses trailing 16-observation windows and five agreeing confirmations,
without backdating. A 20 ms guard follows confirmation; long observation gaps
reset it. Unknown classes and out-of-distribution observations stay unknown.
Whole 90–110 ms application counter intervals require bracketing observations
and a 20 ms guard on both sides, at least 16 I/O observations, at least 95%
calibrated coverage, one common mode, and at least 80% from the same calibrated
dominant class. Any unconfirmed calibrated observation rejects the interval.
The coverage allowance is for a small amount of unroutable/unmodeled context,
not permission to cross a detected mode transition.

Every completed application request is retained. Its **entire** latency and the
counter-read envelope must fit the classified interval envelope. A request
starting earlier extends the required evidence back to its start; crossing a
transition rejects the whole bucket. Percentiles are recomputed from original
requests, never averaged or trimmed. Short final counter intervals stay unknown.

Consecutive qualified buckets with the same dominant class/mode form episodes
of at least 500 ms. Only the predeclared adjacent pairs (0,1) and (2,3) can be
compared. Each arm contributes its highest-throughput eligible episode; CPU and
p99 come from that same episode. Class-histogram total variation must be ≤0.10.
An incompatible best pair is rejected rather than replaced by a convenient
alternate. The two opposite orders remain separate results; a repeatable claim
requires support in both. Zero accepted classes or zero matched pairs is a
valid outcome and produces no V8/V7 performance ratio.
