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

To emit one JSON accounting record per scenario:

```bash
TOYKV_BACKUP_BENCH_REPORT=/tmp/rfc022-backup-accounting.json \
  cargo bench -p kv-engine --bench backup_benchmarks
```

Override the default 500-entry workload for larger runs with
`TOYKV_BACKUP_BENCH_ENTRIES`; override the changed-key count with
`TOYKV_BACKUP_BENCH_CHANGED_KEYS`.

The measured operation is the synchronous `create_backup` call. Setup work
(database population and the first generation for incremental scenarios) is
outside the measured routine. The benchmark retains `logical_bytes` and
`new_object_bytes` from the committed `BackupInfo`; when the report environment
variable is set, it writes those fields to JSON alongside the scenario name.
It also records `repository_bytes`, the physical size of published repository
objects. Criterion output contains the latency and throughput comparisons.

## Observed comparison

One full run with 500 entries, 4 KiB inline values, 16 KiB vLog values, and 50
changed keys produced these medians:

| Scenario | Median latency | New object bytes | Repository bytes |
| --- | ---: | ---: | ---: |
| Inline full | 5.44 ms | 2,099,151 | 2,099,151 |
| Inline unchanged incremental | 3.39 ms | 0 | 2,099,151 |
| Inline changed incremental | 4.04 ms | 209,939 | 2,309,090 |
| vLog full | 14.51 ms | 8,242,715 | 8,242,715 |
| vLog unchanged incremental | 13.04 ms | 0 | 8,242,715 |
| vLog changed incremental | 14.80 ms | 824,271 | 9,066,986 |

The same-run full backup is the baseline. Unchanged incremental backups are
faster and publish no new objects; changed incremental backups publish about
10% of the full object bytes. vLog changed backups can be slightly slower than
the first full backup because incremental bookkeeping is still required. This
is a ToyKV mode comparison, not a comparison against another database or a
previous commit.

This is an operational measurement fixture, not a correctness gate. Run it on
a stable Linux host and compare repeated runs when evaluating changes.
