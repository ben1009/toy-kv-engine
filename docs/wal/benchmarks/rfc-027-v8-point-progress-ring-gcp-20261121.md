# RFC 027 V8 Point Progress Ring GCP qualification (2026-11-21)

## Decision

F0 did not pass its performance promotion gates. Stop the candidate and do not
promote the progress-ring mode or enable it for production. The mode remains
opt-in in bench builds; the default remains the eager Coordinator path.

The local SSD performance screen was skipped at the user's direction because
the local SSD is unstable. The planned same-host GCP matrix completed and is
the qualification evidence for this decision.

## Results

One `c4-standard-16-lssd` host ran a counterbalanced, five-pair comparison of
the eager and progress-ring arms: Point at 1, 8 and 16 writers, and Batch64 at
16 writers. Each run wrote 100,000 operations with 1 KiB values. Both arms
used the same release binary, two Local SSDs in ext4 RAID0, Direct FRONTIER,
1 MiB Point Lookahead, dynamic coalescing and the same digest hash sampling.
Only `TOYKV_BENCH_V8_POINT_PROGRESS_RING` varied. Detailed profiling,
completion timelines, sync diagnostics and worker-group histograms were off.
Point latency was sampled every 10 operations (10,000 samples per run).

| Workload | Writers | Eager kputs/s | Ring kputs/s | Paired throughput change | Ring wins | CPU/Put eager → ring (μs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 7.644 | 7.678 | +1.77% | 4/5 | 60.79 → 62.40 |
| Point | 8 | 31.974 | 30.358 | −5.05% | 0/5 | 51.55 → 61.18 |
| Point | 16 | 42.310 | 43.100 | +1.92% | 4/5 | 79.09 → 83.33 |
| Batch64 | 16 | 629.609 | 623.578 | −0.75% | 1/5 | 4.94 → 5.07 |

Throughput is the median of the five runs in each arm. The paired change is
the median of the five within-pair percentage changes, `(ring / eager - 1) ×
100`.

| Workload | Writers | Syncs eager → ring | Puts/sync eager → ring | p50 ms eager → ring | p95 ms eager → ring | p99 ms eager → ring | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 100,000 → 100,000 | 1.000 → 1.000 | 0.1230 → 0.1217 | 0.2156 → 0.1957 | 0.4935 → 0.4204 | −5.17% |
| Point | 8 | 13,018 → 13,062 | 7.682 → 7.656 | 0.2259 → 0.2402 | 0.4064 → 0.4211 | 0.7133 → 0.7235 | +1.43% |
| Point | 16 | 9,995 → 10,300 | 10.005 → 9.709 | 0.3360 → 0.3278 | 0.6610 → 0.6683 | 0.9321 → 0.9354 | +0.19% |
| Batch64 | 16 | 194 → 194 | 515.464 → 515.464 | — | — | — | — |

Latency percentiles are medians of per-run sampled percentiles. Batch64 was a
throughput-only guard. At Point8 and Point16, the p99 gate passed (no more than
5% paired-median regression), but the throughput gate failed: Point8 regressed
5.05% and Point16 gained only 1.92%, below the required 5% and 4/5-wins gate.
Point1 and Batch64 stayed within their −2% throughput guards. Since all gates
must pass, F0 is stopped despite the p99 and guardrail results. The Point16
gain also falls short of RFC 027's +10% target.

The ring arm used more process CPU per Put in each workload. It also had fewer
Puts per Sync at Point16. These measurements do not identify the cause of the
Point8 regression or prove that any one ring operation caused the result.

## Run verification and provenance

- Planned and completed: 40/40 runs; five counterbalanced pairs per workload.
- `summary.json`: `complete: true`, `runs_planned: 40`, `runs_completed: 40`.
- `remote-exit.json`: `failed: false`, `runs_started: 40`.
- Every planned run ID has a successful `result.json` and matching
  `run-metadata.json`; local strict verification reported 40/40 valid runs.
- Each Point result has 100,000 operations, 10,000 latency samples and 90,000
  unsampled operations. The start/end snapshots report the requested arm and
  show extent-policy scan diagnostics disabled.
- The transferred tar archive checksum passed verification. Archive SHA-256:
  `7f85fa784114dfbc5dfbc6a42c9bcf1626d9193f97f311ea9768fd8e66633ebd`.
- Runtime source commit: `d04a0bba0ae864dd85f9b53fc84d00da472517bd`.
- GCP runner/configuration fix commit: `2796f97c3d523826fb61c0ad6039af0df1ba1338`.
- `write-perf` SHA-256:
  `534362cfd48580a63f2da7f1291a9d9834c2a487902479dfc537c807ff446b36`.
- Host: 16-vCPU C4, two 375 GiB Local SSD devices in RAID0, ext4
  (`rw,noatime,stripe=256`), Linux `7.0.0-1017-gcp`, pinned nightly
  `nightly-2026-09-23`.
- The first remote attempt stopped before scoring because it supplied an
  unsupported DATA submission mode. The corrected runner used
  `TOYKV_BENCH_V8_DATA_SUBMIT_ORDER=ordered`; all 40 per-run environment
  captures confirm this. The archived plan's descriptive
  `data_submit_order` label still said `ticket_prefix`; the checked-in plan
  generator now records the actual supported value, `ordered`. The verifier
  accepts that historical plan label only while requiring every per-run
  environment capture to match the exact `ordered` configuration. This
  metadata label mismatch did not change the measured setting.
- The raw archive and checksum sidecar are retained locally at
  `/tmp/rfc027-f05-point-progress-ring-qualification-20261121-b/recovered/`;
  they are not committed because the archive includes per-run environment
  metadata. The extracted verified result set is in the same directory under
  `rfc027-f05-progress-ring-retry-2796f97c/`.
- After checksum and result verification, teardown completed. The VM, boot
  disk, temporary SSH firewall rule, isolated VPC, subnet and egress route are
  absent; Terraform state has no remaining resources.

This qualification covers only the specified host and workload matrix. It is
not evidence for other filesystems, machines, mixed Point/Batch behavior or
production deployment. See the [RFC 027 implementation plan](../rfc-027-v8-point-progress-ring-implementation-plan.md)
for the stopped-candidate status.
