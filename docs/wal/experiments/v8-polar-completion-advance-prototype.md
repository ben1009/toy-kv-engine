# V8 Polar-style Completion Advance — benchmark prototype

This first iteration adapts the **Advance** principle in [PolarDB PostgreSQL WAL
Pipeline](https://docpolardbpg.openpolardb.com/PolarDB-for-PostgreSQL/WAL-Pipeline.html)
to V8 DATA completions. It is a benchmark-only experiment, **not** a full
lock-free `recent written` implementation or a production setting.

## Pipeline

```text
parallel V8 DATA SQE/CQE
    -> IoWorker GroupWriteResult (unordered)
    -> Completion Reaper: drain up to 32 ready results without waiting
    -> Coordinator: mark all tickets with one lock, advance contiguous
       DATA-complete cursor, notify durability driver only if it advances
    -> Existing ordered digest / FRONTIER / fdatasync / ACK
```

The DATA completion cursor is **not** a durable watermark. Non-contiguous
completions remain in the bounded ticket queue until earlier gaps close.
Any worker error poisons the WAL. The original per-group publication path
remains the default, including in `bench` builds.

## Compare

Build with `--features bench` and run the same V8 `wal_concurrent` Point
configuration in each arm:

- `TOYKV_BENCH_V8_POLAR_ADVANCE=0`: legacy completion publication
- `TOYKV_BENCH_V8_POLAR_ADVANCE=1`: bounded burst + prefix advance

The benchmark JSON's `wal_pipeline.v8_polar_advance` records the arm.
Use counterbalanced paired runs with **identical** io-wq affinity, writer
count, and coalescing controls. Track puts/s, CPU/put, eventfd, wakes,
Syncs, tickets/Sync, DATA Completion HOL, and completed groups per
advance. The last counter is a follow-up instrumentation gap.

## Limits

This version still acquires the Coordinator mutex to publish completion
bits. It removes redundant per-group acquisitions/notifications when
multiple results are queued but does **not** yet implement PolarDB's
lock-free hole detection, an independent Flush/Notify stage, or eliminate
kernel io-wq execution. It makes no performance claim without GCP A/B and
does not relax ordered digest, committed FRONTIER or crash recovery.
