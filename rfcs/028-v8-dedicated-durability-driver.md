# RFC 028: V8 Dedicated Durability Driver

Status: D3a implemented as a benchmark-only, default-off experiment; performance
qualification pending.

## Context

V8 Parallel currently lets a writer that needs a durable prefix compete for the
Coordinator's durability-driver election. The elected caller runs the existing
coalescing, completion drain, digest fold, FRONTIER write, sync, retirement, and
durable-publication path before returning its ACK. This couples writer progress
to driver election and makes a writer thread perform both producer and
durability-coordination work.

RFC 027 retained this behavior for its progress-ring qualification. D3 tests a
separate execution owner without changing the V8 on-disk format or durability
protocol.

## Goals

- Move durability-driver election and execution off writer threads in D3a.
- Reuse the existing `advance_frontier_through` implementation and all existing
  ordering, coalescing, sync, retirement, poison, and publication rules.
- Coalesce requests by the maximum requested ticket end, using a capacity-one
  wake channel rather than allocating one request object per Put.
- Keep the caller-driven path as the default and as the comparison baseline.
- Make the benchmark result prove which mode ran and whether any writer elected
  a driver.

## Non-goals

- No WAL frame, digest, FRONTIER, or recovery format changes.
- No io_uring submission/completion changes, coalescing-policy changes, or MVCC
  publication changes.
- No independent ACK waiter or atomic ACK watermark in D3a.
- No driver-owned outer cursor publication in D3a.
- No production-default change. The new mode is compiled only with `bench` and
  requires `TOYKV_BENCH_V8_DEDICATED_DURABILITY_DRIVER=1`.

## D3a design

The dedicated request state contains a monotonic `requested_ticket_end`, a
capacity-one wake channel, a shutdown flag, and a terminal failure slot. Each
writer that needs a durability advance publishes its target with `fetch_max`,
sends a wake token if the channel is empty, and then waits on the Coordinator's
existing durable-prefix condition variable. A full channel means that a wake is
already pending; the atomic target retains the newest request.

The dedicated thread reads the target and current durable end. If work remains,
it calls `advance_frontier_through(requested_ticket_end - 1)`, then rechecks the
atomic target before sleeping. The recheck plus bounded channel closes the
request-versus-sleep lost-wakeup window. The thread is the only caller that
elects and owns `WalV8DurabilityDriver` in this mode. Writer waiters use a
wait-only Coordinator predicate and never enter driver election.

If the thread returns an error or panics, it poisons the Coordinator first,
waking durable waiters through the existing fail-stop path. It records the
detailed terminal error afterward on a best-effort basis; a waiter may therefore
observe the generic poison error instead of the underlying cause. The normal
I/O worker and completion reaper remain independently alive while the driver
performs FRONTIER sync.

The driver join handle is owned by the outer `Wal`. Its field is dropped before
the `Arc<WalV8ParallelWriter>` so teardown signals and joins the driver before
releasing writer-owned segment resources. `Wal::close()` remains a durability
barrier; it does not stop the thread. Per-ticket ACKs return after durable
publication, while whole-WAL sync/close additionally waits for the driver's
post-publication resource retirement to finish. Rotation drains the old writer
through that barrier, and dropping that WAL owner stops and joins its driver.

## D3a benchmark diagnostics

Detailed driver counters are disabled for scoring runs and enabled only with
`TOYKV_BENCH_V8_DURABILITY_DRIVER_DIAGNOSTICS=1`. The benchmark output records
when enabled:

- Whether the dedicated driver owner was actually installed.
- Number of durability requests, wake tokens, coalesced wake attempts, and
  driver advance attempts. The latter includes failed calls and is not a count
  of successful FRONTIER syncs.
- Caller-driven writer election attempts, which must be zero in Dedicated mode.

The mode parser accepts only `1` as enabled; absent and `0` are disabled, and
other values fail explicitly. This avoids silently running a mislabeled arm.

## Correctness and lifecycle checks

The D3a test suite must cover:

- Wait-only Coordinator calls do not elect a driver.
- Idle worker shutdown wakes and joins cleanly.
- Concurrent ticket requests are merged without losing the highest target.
- Later DATA readiness cannot advance the durable prefix over an earlier ticket.
- Sync failure and worker panic poison the Coordinator and wake waiters.
- Close is still a barrier, and recovery replays exactly the durable prefix.
- Rotation drops and joins the old segment's driver before its resources are
  released.

The background driver must not be made responsible for its own JoinHandle
lifetime. The writer may be held by an `Arc` in the worker closure only because
the independent outer WAL owner signals shutdown and joins before dropping the
writer. Do not call `Coordinator::close()` as a substitute for shutdown: close
is not a guarantee that an incomplete admitted prefix can drain.

## Qualification plan

First run correctness tests in bench and non-bench builds. Then compare
CallerDriven and Dedicated with the same binary and configuration: Stage A on,
C1/C2 and D1/D2 off, and no CPU-attribution or detailed sync instrumentation in
scoring runs. Use Point1, Point8, Point16, and Batch64 with five interleaved
pairs (40 runs). Record throughput, CPU/Put, p99, Puts/Sync, driver requests,
wake coalescing, advances, and caller election attempts. A diagnostic run may
enable lightweight counters but must not be presented as an unprofiled score.

The mode remains experimental unless Point8 and Point16 show repeatable gains
without a material Point1 or Batch64 regression, and the lifecycle suite passes
without skipped required Linux io_uring cases. D3b (independent ACK waiter) and
D3c (driver-owned outer cursor publication) require separate review and
qualification; neither is part of this RFC's D3a implementation slice.
