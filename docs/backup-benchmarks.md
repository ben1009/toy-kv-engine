# RFC 022 Backup Benchmarks

The benchmark fixture compares backup latency and byte accounting for inline
and vLog-sized values across three repository states:

- `full_first`: first backup into a new repository;
- `incremental_unchanged`: backup after an identical generation;
- `incremental_changed`: backup after updating a subset of keys.

Run it with:

```bash
cargo bench -p kv-engine --bench backup_benchmarks
```

The measured operation is the synchronous `create_backup` call. Setup work
(database population and the first generation for incremental scenarios) is
outside the measured routine. The benchmark retains `logical_bytes` and
`new_object_bytes` from the committed `BackupInfo` as black-boxed accounting
signals; use the Criterion output for latency comparisons and inspect those
fields when extending the fixture to emit a machine-readable report.

This is an operational measurement fixture, not a correctness gate. Run it on
a stable Linux host and compare repeated runs when evaluating changes.
