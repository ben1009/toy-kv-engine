# V8 Point Progress Ring implementation plan

Implement F0 from RFC 027, which builds on RFC 026 and was introduced in draft form by PR #559. Point16 currently runs at
about 41–42k puts/s on V8 versus 48–49k on V7 in paired GCP runs. F0 tests
whether publishing readiness to individual ticket cells and applying completed
DATA groups in batches can reduce shared Coordinator work. The Point16 target
is +10% over V8. The promotion gates below also cover Point8, Point1 and Batch64.
[RFC 027: V8 Point Progress Ring](../../rfcs/027-v8-point-progress-ring.md)

Use one mode for all Point and Batch tickets in a V8 Parallel writer. The
experiment is off by default and selected at writer construction in bench
builds. Reuse MVCC sequencing, ordered admission, DATA packing, io_uring,
extent preparation and dynamic FRONTIER coalescing. Reuse the V8 disk format,
digest chain, resource accounting, ACK path and recovery rules. F0 uses the
existing worker, Completion Reaper and writer-elected driver. V8 Leader uses
the eager Coordinator backend.

## Patch sequence

| Patch | Code entry points | Ready to advance when |
| --- | --- | --- |
| F0.1 Ticket cells | `parallel.rs` admission and cell helpers | Isolated tests pass for digest validation, capacity and reuse. |
| F0.2 Completion queue | `leader.rs::WalV8ParallelIo::spawn`, `reap_v8_group_result`, `parallel.rs::mark_data_group_complete_inner` | Whole group validation, queue capacity and reopen isolation pass in tests. |
| F0.3 Producer publication and advance | `leader.rs::WalV8FrameJob`, `parallel.rs::fold_ready_digests`, readiness and FRONTIER accessors | An elected owner builds the same ordered digest and durable prefix as the baseline. |
| F0.4 Driver waits and enablement | `leader.rs::drive_frontier_through`, `parallel.rs` driver acquisition and readiness waits, `mem_table.rs` Active writer open | Both waits use the token protocol. The entire mode passes the correctness matrix before scoring. |
| F0.5 Qualification | `write-perf`, `scripts/gcp_wal_matrix.py`, the GCP launcher and archive verifier | The user-waived local performance screen is recorded as skipped; five valid GCP pairs per workload produce a keep or stop decision. |

Both the producer and Coordinator code live in the
[V8 writer](../../kv-engine/src/wal/v8/leader.rs) and
[Coordinator](../../kv-engine/src/wal/v8/parallel.rs).

### F0.1 Preallocate and validate the ticket cells

1. Build a ring sized to `max_inflight_tickets` once per writer. Index a cell
   by absolute ticket modulo that capacity. Put its identity, expected logical
   length, digest and frame readiness under one mutex. Each cell owns its
   payload for the ring's lifetime. Reuse the current ticket and byte limits
   and keep producer progress publication free of per-ticket allocations.
2. Under the Coordinator lock, check that the slot is free before the
   fallible `register(assignment)` call. Initialize the cell after registration
   succeeds and before the Release store to `admitted_ticket_end` exposes
   the ticket. A registration error leaves the slot free and the admission
   cursor at the same ticket.
3. Keep the absolute assignment with the producer and revalidate incarnation
   and ticket under the cell mutex on every access. Validate the admitted byte
   length on digest publication. The first accepted digest is immutable.
   An identical retry succeeds without a new write or wake. A conflict, length
   mismatch or stale identity leaves the cell untouched and routes through
   the existing poison transition after unlocking.
4. Let admission and retirement take the Coordinator lock before a cell lock.
   Producers release the cell lock before any call into the Coordinator.
   Inside `publish_durable_frontier`, validate the complete covered prefix
   before clearing its cells and removing its deque entries. Expose the freed
   slots with the new durable boundary. Old guard cleanup continues to check
   its absolute ticket against the published watermark after cell reuse.
5. Deliver isolated tests first using a constructor or fixture that takes the
   mode and capacity explicitly. Exercise capacities one and two, a failed
   registration followed by retry of the same ticket, nonzero recovered
   tickets, bad length and incarnation, identical and conflicting digests,
   and a stale producer event after reuse.

### F0.2 Route successful DATA groups to the elected owner

1. In Parallel construction, before spawning the worker and Reaper, create a
   fresh completion queue using the existing `crossbeam_channel::bounded`.
   Give the Reaper the only sender and reserve receive access for the elected
   driver of that writer. The queue identity remains private to the writer
   even if reopening a segment reuses its persisted incarnation and absolute
   ticket range.
2. Map a successful `GroupWriteResult` to one event with the absolute ticket
   range, `group_id`, `write_count` and `write_bytes`. Keep
   `wal_v8.parallel.after_data_write` before publishing the successful group
   in either mode. Enqueue with `try_send` and signal progress after enqueue.
   Capacity is at least `max_inflight_tickets`. Worker groups have disjoint
   nonempty ranges, so each queued or in-delivery event covers at least one
   distinct live ticket. A ticket can retire only after its completion is applied.
3. Extract range validation and application from
   `mark_data_group_complete_inner` into a helper that uses the already held
   state lock. Keep the old notifications in the eager wrapper. Check the
   entire range and each live cell's identity and frame readiness before
   marking any DATA completion. An entirely completed live range is
   idempotent. Reject and poison on empty, unknown, retired or partially
   completed ranges and on invalid frame readiness.
4. Immediately poison on failed worker results, overflow or unexpected
   disconnection. Install an exit guard at F0 Reaper entry that poisons and
   signals all waiters on unexpected return or unwind, including after enqueue
   and before notification. Run the guard outside Coordinator and cell locks.
   In the owner drain, handle `TryRecvError::Empty` as the end of the current
   batch and `Disconnected` as a failure outside final teardown. Keep the
   existing worker close and Reaper join order. Only final teardown makes a
   disconnect expected. Public `Wal::close()` remains a sync barrier.

### F0.3 Put producer readiness in the ring

1. In `WalV8FrameJob::finish_with_point_direct_frame_and_phases`, publish the
   digest to its cell after logical encoding, then mark frames ready after
   materialization. Keep guard registration before `publish_data`. Single
   frame Point DATA can still submit immediately. Keep the first accepted
   digest in the cell until retirement so later duplicates can be validated
   after folding.
2. Add an owner-only `advance_and_snapshot(&WalV8DurabilityDriver)` operation.
   Require the active driver guard for the same writer. Under one Coordinator
   lock, drain up to the bounded queue capacity, apply whole groups, then fold
   contiguous ready digests from `folded_ticket_end` in ticket order. Fold
   eligible digests before their DATA completes when the owner can do so.
   Snapshot the prefix eligible for FRONTIER using frame readiness, DATA
   completion and the folded digest. In bench builds, record the existing
   sampled ordered-link hash CPU delta once per advance from the driver.
   Keep producer digest metrics for the encoding work they perform.
3. Treat the cell as the only producer readiness source in F0. Keep
   `data_complete`, `prefix_digest_after`, the ordered fold cursor, reservation
   and published durable boundaries in the Coordinator. Use the same validated
   cell accessor for completion checks, readiness views, snapshots and FRONTIER
   reservation and publication. A read-only snapshot or `prefix_digest_at`
   reads established progress without draining or folding. Exercise a missing
   digest after DATA completion and out-of-order completion behind an older
   gap. Each ticket must contribute to the prefix digest exactly once.
4. Keep the durability order: reserve FRONTIER, write it, successfully
   `fdatasync`, complete resource retirement, publish the durable boundary and
   release admission capacity, then retire producer guards. Recheck terminal
   poison at the existing publication gate. Already admitted DATA can race
   poison. Preserve ACKs already returned and the existing fast path for a
   ticket covered by the WAL facade's durable cursor. A pending waiter can
   still observe poison after Coordinator publication and before that cursor
   is refreshed. Recovery may include such a durable, unacknowledged ticket.

### F0.4 Integrate both readiness waits before exposing the mode

1. Use a capacity-one channel as a persistent wake token. Signal after the
   first accepted digest, each queued successful completion, and poison or
   close. In F0, frame readiness updates only the slot. Actual DATA buffer
   insertion still wakes the packer. Reserve the admission Condvar for
   admission state and capacity changes. Publish state and release its lock
   before producer signaling. `try_send` on a full token channel leaves the
   pending wake in place. Use the cell mutex and completion queue for
   publication ordering and the token for wakeups, without a per-publication
   increment of a shared progress counter.
2. Carry the existing driver guard through `drive_frontier_through`. Start
   every F0 driver iteration, including the first scan after election or
   handoff, by consuming any pending token **before**
   `advance_and_snapshot`. Use the resulting snapshot for coalescing and
   FRONTIER selection. Reuse this step in `wait_for_durability_change` and
   `coalesce_frontier_ready` after each receive or timeout. If more progress
   is needed, release the Coordinator and cell locks before receiving with
   the time remaining on the original absolute deadline. The empty-prefix
   wait returns when a prefix becomes ready. The coalescer stops at its
   deadline, when the prefix becomes empty or covers the admitted window,
   or when existing terminal handling takes over.
3. Route sampled coalescing through the same F0 advance and wait protocol.
   Keep `durable_published` for follower ACK and driver handoff, and `changed`
   for admission backpressure. Close and poison wake the token and those
   waiters through their existing state transitions. Use `poison_locked` for
   terminal failure. Preserve the public sync and close barriers, externally
   fenced PITR seal, and final owner teardown as distinct operations. Teardown
   joins run outside Coordinator, submission and physical FRONTIER locks.
4. Wire the immutable mode selection through
   `into_parallel_with_worker_file` before spawning the pipeline. Use the
   common Parallel attachment in `MemTable::recover_from_v8_active_inner` for
   fresh Active WALs, reopens and rotation successors. Activate the ring with
   empty cells at an empty admission window. Keep the recovered absolute
   ticket cursors and verified digest boundary in the Coordinator. The bench
   switch `TOYKV_BENCH_V8_POINT_PROGRESS_RING` selects Eager with `0` and
   Cursor with `2`; retired mode `1` is rejected. Non-bench builds keep Eager
   by default and allow explicit Cursor opt-in through
   `TOYKV_WAL_V8_PROGRESS_MODE=cursor` (`eager` selects the fallback). Use
   explicit mode injection in parallel tests. Export the actual mode from the opened writer in both
   `wal_io_start` and `wal_io_end`, including on a reopened writer. Keep
   F0.2 and F0.3 behind test construction until both runtime waits are wired.
5. Adapt sync diagnostics, completion timeline and HOL sampling to F0 or
   reject unsupported enablement in the setters before mutating
   `WriteProfile`. For supported sync diagnostics, validate
   `mark_data_write_started` through the ring's Frame Ready accessor and
   distinguish Reaper receive time from owner completion publication time.
   `write-perf` calls the setters after opening the writer and successors
   inherit the profile. Keep disabled settings valid and check inherited
   settings on rotation. For role CPU diagnostics, account for `write-perf`
   enabling durability phase sampling by default when CPU attribution is
   requested.

## Correctness before scoring

Use barriers, controlled completions and bounded waits to force interleavings.
Run the common behavior in eager and F0 modes. Check that the real io_uring
tests execute on the qualification host.

| Area | Required cases |
| --- | --- |
| Single readiness authority | Snapshot and FRONTIER validation agree after each event. Include wrong digest length, duplicate and conflicting digests before and after folding, and DATA completion before digest. |
| Reuse and backpressure | Capacity one and two, recovered ticket base, delayed guard disarm after publication, byte and ticket limits, FRONTIER credits and source-spool quota. A stale producer or retired completion poisons after reuse without changing the successor cell. |
| Queue and reopen | Multi-ticket atomic application, duplicate live range, partial duplicate and invalid range rejection, full valid queue, overflow, failed worker result, and old versus new queue isolation even when incarnation and tickets match. Inject a Reaper unwind after enqueue and before wake with an owner parked and another token sender alive. Repeat without an active owner and check that subsequent admission and waits observe poison. |
| Wakeup and handoff | Concurrent publishers with a full token, publication between scan and park, close, poison and coalescing deadline under continued arrivals. Hand off with a queued event and pending token, then require the new owner's initial scan to advance it without a later publication. For each wait site, consume the only DATA token while the digest is missing and require the late digest alone to wake the driver. |
| Poison and ACK | Poison before and after local readiness, guard registration, DATA submission and durable publication. Verify that new admission and later durable publication fail. Check the fast path when the facade cursor already covers the ticket. Inject poison between Coordinator publication and facade refresh and compare the wait and recovery outcomes against eager mode. Preserve resource charges on uncertain I/O. |
| Crash and PITR | Reuse the four durability crash cuts in both modes. Pass the mode to the crash child and assert the opened writer uses it before issuing the writes. In a multi-group case, ACK one group, then crash during the next and verify the acknowledged prefix survives. A fully persisted but unacknowledged FRONTIER may be recovered under the existing rules. Cover ENOSPC, Active recovery, rotation, seal, archive and PITR replay. |
| Mixed writes and compatibility | Exercise concurrent Point Put/Delete/TTL and Batch writes on the same WAL, including one-entry batches, transactions and cross-window writes. Compare recovered logical records and digests. Run the existing V8 Leader and legacy reader regression coverage. |
| Diagnostics | Request sync diagnostics with F0 active. Verify that supported settings complete writes using ring readiness, and unsupported settings fail during configuration without changing the profile. Exercise the successor's inherited profile after rotation. |

Extend `failpoint_parallel_writer_crashes_at_durability_boundaries` in
`leader.rs`. Add F0-enabled lifecycle cases using the Active recovery and
rotation fixtures in `lsm_storage.rs` and the replay fixtures in
`pitr/archive.rs`. Assert F0 on the source and successor Parallel writers.
Through the engine's normal open path, append in eager mode, reopen the
nonempty V8 WAL in F0 and append, then reopen in eager mode and append again.
Verify recovered records, the nonzero ticket base and the digest boundary at
each reopen.
Filter these cases with `v8_progress_ring_` and retain the `failpoint_` prefix
for tests that arm failpoints so the sanitizer filters isolate them. Check
bench and non-bench builds before scoring. Keep the historical guard and
interrupted migration cases. Add a completed migration case built with
`bench,chaos-testing` that appends and syncs using a verified Parallel F0
writer before the historical readers attempt to open the image. Reuse the
pinned readers and mutation checks. Use the `v8_progress_ring_pre_v8_` prefix
for the ignored F0 compatibility cases.

```sh
cargo nextest run --package kv-engine --features bench,chaos-testing --lib \
  -E 'test(wal::v8::) | test(v8_progress_ring_)'
```

With the pinned reader environment variables set, run the ignored cases
separately and confirm the F0 completed migration case ran.
[Historical reader compatibility suite](rfc-026-implementation-plan.md#old-reader-compatibility-gate)

```sh
cargo test --package kv-engine --lib --features bench,chaos-testing \
  v8_progress_ring_pre_v8_ -- --ignored --test-threads=1
```

## F0.5 Local screen and GCP A/B

1. Skip local Point8 and Point16 performance screening under the user's waiver;
   do not use local throughput or p99 as an advance gate. Use the GCP matrix
   below as the performance evidence. Keep F0-enabled correctness checks as a
   separate requirement. Use separate diagnostic runs for role CPU, owner lock
   hold time, digest and group counts per wake, and peak ring and queue occupancy.
2. Schedule five counterbalanced pairs per workload on one
   `c4-standard-16-lssd` with two Local SSDs in ext4 RAID0. Run Point at 1, 8
   and 16 writers and Batch64 at 16 writers, for 40 scored runs. Use 100,000
   logical puts, 1 KiB values, separate database directories, Direct FRONTIER,
   1 MiB Point Lookahead and dynamic coalescing. Exclude warmup from scores and
   use `--latency-sample-every 10` for both Point arms. Keep ordered DATA
   submission, direct-frame encoding off and Point credit batching off in
   both arms. Leave `TOYKV_WAL_V8_COALESCE_BUDGET_US` unset to select dynamic
   coalescing. Vary only the F0 switch.
3. Explicitly set `TOYKV_BENCH_V8_EXTENT_POLICY_SCAN_DIAGNOSTICS=0` in both
   arms. Require `sliding_extent_preparation.extent_policy_scan_diagnostics_enabled`
   to be `false` in both `result.wal_pipeline.wal_io_start` and `wal_io_end`.
   Keep CPU attribution, sync and durability diagnostics, HOL and completion
   timelines, and `perf` off in score runs. Clear `TOYKV_WAL_TIMELINE` in the
   launch environment to keep Batch64's periodic sampler off. Keep the
   existing one in 64 hash CPU sampling rate in both arms and verify that
   F0 reports the driver's fold CPU in `result.wal_v8_digest.ordered_link_hash_cpu_ns`.
   Validate the opened writer mode at both snapshots, actual I/O settings,
   workload, run IDs and binary SHA-256 against the plan. Require
   `result.ops` to be 100,000 and `result.ops_per_sec` to be finite and
   positive in every run. Each scored Point result must have
   `latency.sample_every` of 10, exactly 10,000 samples, and sample and
   unsampled operation counts that add up to `result.ops`. Require a
   finite positive `latency.p99_ms` for Point.
4. Use the Point direct-frame suite as a template for plan and pairing format.
   Register the F0 suite in the GCP launcher, preflight plan, remote upload
   and dispatch, and local `_verify_result_set` path. Add the F0 schedule
   filename to the allowlist in `REMOTE_ARCHIVE_SCRIPT`. Share the schedule
   and match result and metadata directories by exact planned run IDs. Pack,
   unpack and verify a complete 40-run fixture, including its schedule.
   Check that missing, extra, failed and mismatched runs are rejected.
   Include fixtures with an incorrect raw Point sample count and with
   matching metadata and summary scores that disagree with the raw result.
   Write `summary.json` and `remote-exit.json` on failure as well as success.
   [Point A/B runner](../../scripts/gcp_v8_point_direct_frame_runner.py)
5. Follow the repo's GCP workflow using a temporary Terraform isolated VPC and
   subnet. Audit its effective ingress. Require `--preflight-only` to pass
   before VM creation, with the same machine, network, workload and cost
   settings as launch. Pass `--max-runtime-minutes 60` and
   `--max-estimated-spend-usd 2` with the same hourly cost estimate to both.
   Budget setup, the 40 runs and collection against the server-side deadline.
   [GCP test environment](../../infra/gcp/test-environment/README.md)
6. Keep the source revision, binary hash, per-run metadata, score JSON,
   schedule and archive checksum together. Require all 40 planned runs to
   succeed with matching metadata and a verified archive before scoring.
   If remote testing started or its outcome is uncertain, retain the test
   resources for collection within the existing deadline. After verification,
   tear down the VM and isolated network through the runner workflow and
   confirm removal.
   [GCP scoring runbook](https://github.com/ben1009/toy-kv-engine/blob/13fa67640889d35e56c9476191d8276ee0e185cc/docs/wal/benchmarks/gcp-free-trial-wal-bench.md#scoring-v8-point-candidates)

Report throughput, process CPU/Put, Puts/Sync and Point p50/p95/p99. Keep the
worker-group histogram disabled during scored runs so it cannot add arm-dependent
measurement overhead. If group-size distribution is needed for interpretation,
collect it in a separate, explicitly diagnostic run and do not mix it into the
qualification score.
V8 `commit_groups` counts FRONTIER/sync cycles. Compute promotion scores
from archived `result.json` values and require the run metadata and summary
to agree. Match each pair by workload, writer count and repetition. Retain
the individual changes `(F0 / eager - 1) × 100` and take their median.
Use throughput for Batch64's promotion check.

| Promotion gate | Required result |
| --- | --- |
| Point8 and Point16 throughput | At least +5% paired median and at least four wins out of five for each workload |
| Point8 and Point16 p99 | At most +5% paired median regression for each workload |
| Point1 and Batch64 throughput | At least −2% paired median for each workload |
| Correctness | All RFC 027 Section 7 checks pass with F0 enabled |

The user explicitly waived local performance screening because the local SSD is
unstable; record that screen as skipped and use the cloud matrix as the performance
evidence. Stop F0 if any promotion gate fails. Passing the gates qualifies the
opt-in implementation for review. Changing the
production default requires a separate decision.

## F0.5 qualification outcome (2026-11-21)

The local SSD screen was skipped at the user's direction. The GCP matrix
completed and verified 40/40 planned runs. It failed the Point throughput
promotion gate: Point8 was −5.05% (0/5 wins) and Point16 was +1.92% (4/5
wins), versus the required +5% paired median and at least 4/5 wins for both.
Point8/16 p99 and the Point1/Batch64 throughput guardrails passed, but every
gate is required. Per the stop rule, F0 is stopped and is not qualified for
promotion or production enablement. The mode remains off by default.

See the [GCP qualification report](benchmarks/rfc-027-v8-point-progress-ring-gcp-20261121.md)
for the complete score, archive verification and run provenance.

## Cursor production opt-in stage

The first production-selection slice retires the intermediate Ring mode while
keeping Eager as the default. Cursor can be selected in a non-bench build with
`TOYKV_WAL_V8_PROGRESS_MODE=cursor`; unset or `eager` keeps the previous Eager
behavior. The benchmark runner now compares only Eager and Cursor. The old
two-arm F0.5 Ring plan and result verifier remain for reading archived results,
but the runner no longer launches mode 1.

This does not promote Cursor to the production default. Before considering that
change, qualify the non-bench Cursor path with production I/O configuration,
including buffered FRONTIER and production extent preparation, across PITR,
crash recovery, rotation and seal tests. Keep Eager available as the rollback
mode until that qualification and a separate default-change review are complete.
