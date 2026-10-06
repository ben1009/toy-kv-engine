# Async Scan Findings

Date: 2026-07-05

## Scope

This note records the findings and benchmark conclusions from the RFC 014 async
scan exploration in the current checkout.

## Review Findings

### 1. `AsyncScan::try_next()` cancellation regression

When `AsyncScan::try_next()` moved iterator state out before awaiting a
blocking task, dropping the future could leave the cursor permanently
corrupted. The next poll could panic because the iterator state had already
been taken, and the in-flight blocking task could consume rows that were never
surfaced to the caller.

Status: fixed in the explored branch before the final revert-to-wrapper
decision.

### 2. `AsyncTxnScan::try_next()` row-loss on cancellation

When `AsyncTxnScan::try_next()` advanced the shared transaction iterator inside
`spawn_blocking` and only later copied the buffered rows back into the async
cursor, cancelling the future could skip unseen rows permanently.

Status: fixed in the explored branch before the final revert-to-wrapper
decision.

### 3. `measure-scan` benchmark measured the wrong path

At one point the benchmark timed `scan.try_next().await`, but the public async
cursor internally advanced the underlying iterator synchronously inside the
blocking executor. That meant the benchmark no longer measured the Phase 4
`next_async()` iterator stack it was supposed to evaluate.

Status: fixed by separating the benchmarked path from the public wrapper while
reviewing the results.

### 4. `measure-scan --mode both` order bias

Running one full mode before the other made the second mode inherit a warmer
cache state. Flipping the order randomly was not enough for a single run.

Status: fixed during the benchmark-correction pass by interleaving the two
modes when comparing them.

### 5. Background compaction confounded scan comparisons

Running `measure-scan` with compaction enabled could change SST layout and
cache locality between repetitions without changing row count, making the
results physically incomparable.

Status: fixed by requiring `--compaction none` for scan comparisons.

## Benchmark Conclusions

### Corrected internal async iterator benchmark

On the stable dataset `.tmp/async-phase4-scan` with compaction disabled, the
corrected benchmark that exercised `ScanIterator::next_async()` directly showed
that the internal async iterator path is significantly slower than sync full
scan:

- sync median: about 490 ms
- async median: about 1775 ms

Conclusion: the current Phase 4 per-step async iterator path is slower than the
sync iterator for full scans.

### Public wrapper-based async scan

The public `scan_async()` API performs much better when treated as a batched
wrapper around the sync scan path instead of trying to expose one true async
iterator step per row.

Best measured result from the exploration:

- sync median: about 505 ms
- async median: about 647 ms

Conclusion: if the goal is throughput rather than “true async iteration,” the
best result in this exploration came from a batched wrapper over the sync scan
path.

## Experiments Tried

These changes were tested and then rejected because they did not beat the best
batched-wrapper result:

- larger fixed batch size (`4096`)
- reusable batch buffers
- adaptive batch sizing with a time budget
- lower-copy payload path
- explicit chunked async scan API
- long-lived blocking producer task feeding the cursor

## Final Recommendation

If the goal is scan performance, keep `scan_async()` as a compatibility wrapper
around the sync scan path and batch work behind `try_next().await`.

If the goal is a “true” async iterator surface, that likely needs a more
fundamental redesign, because the current per-step async iterator execution
model is slower on this workload.
