# RFC 026 V8 low-overhead coalescing confirmation on GCP — 2026-11-11

## Result

The fixed 400 μs coalescing override retained a throughput advantage with
profiling and per-sync observations disabled. Across five counterbalanced
blocks, Direct FRONTIER with a fixed 400 μs budget reached a median 21.53k
puts/s, compared with 20.67k for the current Buffered/Dynamic path (+4.15%) and
20.07k for Direct/Dynamic (+7.25%). The five Direct/400 runs were all faster
than the five Buffered/Dynamic runs in this experiment.

Direct/400 also had a median 7,199 syncs per 100,000 puts, nearly the same as
Buffered/Dynamic's 7,288 and 25.1% fewer than Direct/Dynamic's 9,608. This
reproduces the throughput and sync-count direction from the diagnostic run
without capturing an observation for every sync.

This is a promising 16-writer Point result, not production qualification. The
experiment sampled operation latency and retained aggregate bench counters;
it did not run crash-recovery qualification or test other writer counts and
workloads. Runtime defaults remain unchanged.

## Setup and method

- Source commit: `0c5efdd6aad8f88d9807b65c1f33f06e3a009d06`.
- GCP `c4-standard-16-lssd`, 16 vCPUs, Intel Xeon 6985P-C, two NVMe Local SSDs
  in RAID0, mounted as ext4 at `/mnt/test-ssd`.
- Each run performed 100,000 concurrent Point puts with 1 KiB values, V8 PITR
  Parallel WAL, a 2 GiB target SST size, and compaction disabled.
- Broad profiling was off (`--profile` omitted) and per-sync capture was off
  (`--wal-sync-diagnostics` omitted). Operation latency was sampled once per
  100 operations; each run contributed 1,008 samples. The `bench` feature still
  recorded aggregate runtime counters, including sync count and total
  FRONTIER-write/`fdatasync` time.
- All 15 runs reported DATA `O_DIRECT=true`, a buffered sync descriptor, and
  the requested FRONTIER mode. The three handles identified the same device
  and inode, and start/end I/O metadata was stable. No `sync_observations` or
  per-sync group-count records were present.
- `TOYKV_WAL_V8_COALESCE_BUDGET_US=400` was set only for the Direct/400 arm and
  was unset for dynamic arms. The override is benchmark-only and sets the
  budget used by the existing coalescing branch when that branch is eligible;
  it does not make every durability advance wait for 400 μs. No runtime
  default or durability ordering changed.

Each block ran all three arms once, with order varied to reduce position bias:

| Block | Run order |
| --- | --- |
| 1 | Buffered/Dynamic, Direct/400, Direct/Dynamic |
| 2 | Direct/Dynamic, Direct/400, Buffered/Dynamic |
| 3 | Direct/400, Buffered/Dynamic, Direct/Dynamic |
| 4 | Buffered/Dynamic, Direct/Dynamic, Direct/400 |
| 5 | Direct/Dynamic, Buffered/Dynamic, Direct/400 |

## Results

Values are medians across five runs; ranges show the five individual runs.
The last column is the median per-run sum of aggregate FRONTIER-write and
`fdatasync` timings.

| FRONTIER / budget | Throughput, k puts/s | Sampled p50, ms | Sampled p99, ms | Syncs per 100k puts | FRONTIER write + sync per run |
| --- | ---: | ---: | ---: | ---: | ---: |
| Buffered / Dynamic | 20.67 (20.22–20.69) | 0.764 (0.758–0.770) | 2.87 (1.40–3.95) | 7,288 (7,255–7,418) | 1.259 s |
| Direct / 400 μs | 21.53 (21.25–21.65) | 0.735 (0.728–0.749) | 2.00 (1.63–4.66) | 7,199 (7,181–7,409) | 1.047 s |
| Direct / Dynamic | 20.07 (19.98–20.18) | 0.770 (0.763–0.781) | 2.87 (1.72–3.91) | 9,608 (9,371–9,638) | 1.657 s |

The Direct/Dynamic arm had 33.5% more syncs than Direct/400. Its median
operations covered per sync, estimated as 100,000 divided by the median sync
count, were 10.41 versus 13.89 for Direct/400. The lower sync count and higher
throughput are consistent with the adaptive budget affecting group formation.
Because per-decision budget observations were disabled, this run does not
measure how often the dynamic rule selected 100 versus 400 μs.

The aggregate FRONTIER-write-plus-sync counter sums were lower for Direct/400
than Buffered/Dynamic (1.047 s versus 1.259 s, −16.8%) and Direct/Dynamic
(1.657 s, −36.8%). These are cumulative measured call times over each run, not
wall-clock time saved; the aggregate timing counters remain enabled in the
bench build.

Sampled p99 medians are lower for Direct/400 than for the dynamic arms, but
the individual ranges overlap and include substantial run-to-run variation.
Do not treat this set as evidence of a stable tail-latency improvement.

## Interpretation and limits

The lower-overhead rerun supports the fixed-budget candidate: Direct/400
outperformed both the current Buffered/Dynamic path and Direct/Dynamic in all
five measured blocks. Direct/400 also reduced aggregate FRONTIER-write plus
sync time in this workload. The Direct/Dynamic sync increase and throughput
loss are consistent with the scheduling-feedback explanation from the
per-sync study, although this run did not capture the budget decision counts.

There was no Buffered/Fixed-400 arm, so this three-arm design cannot isolate
the effect of Direct FRONTIER from the effect of fixing the coalescing budget
at 400 μs. It also covers only one VM and one 16-writer Point workload. Latency
percentiles are sampled estimates, and p99 varied widely across repetitions.

Keep the experiment controls bench-only. Before considering a runtime policy,
qualify recovery with Direct FRONTIER and measure lower writer counts and
batch workloads. The measured result does not justify making 400 μs or Direct
FRONTIER the universal default.

## Artifacts and cleanup

The compressed artifact contains all 15 raw benchmark JSON records, normalized
run summaries, the run schedule, binary/source metadata, and `lscpu` output:
[download raw results](artifacts/rfc-026-v8-low-overhead-budget-gcp-20261111.tar.gz).
SHA-256:
`5c55a4b8b439c7fc227d7f5143cf44aa03e9fe936bcd685062b7af021ad075e6`.
The artifact was checked for project IDs, IP addresses, and email addresses.

After collecting the artifact, Terraform destroyed the VM, boot disk,
firewall, subnet, and VPC. Independent GCP inventory confirmed those resources
were absent, and Terraform state contained no managed GCP resources.
