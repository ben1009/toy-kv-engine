# RFC 028 V8 Point Resource Preclaim final GCP A/B

November 24, 2026. Tested source commit
`3e54bd15327218cc94ada9358d6ea16fb6d8862d` on the PR #562 branch.

**Stage A+B3 did not qualify. Keep Stage A (Point buffer preallocation only)
and stop the Resource Preclaim optimization.** The candidate missed the
predeclared throughput gate at Point8 and Point16. Sampled p99 and Batch64
throughput stayed within their guardrails.

## Method

One `c4-standard-16-lssd` host in `us-central1-b` ran 30 tests: five
counterbalanced pairs for each of Point8, Point16, and Batch64. Each run
processed 100,000 1 KiB Put records with a fresh database and the same
bench-enabled `write-perf` binary.

Stage A enabled Point buffer preallocation and disabled Resource Preclaim.
Stage A+B3 kept Stage A and enabled Resource Preclaim, the cached speculative
total, and preclaim-after-buffer ordering. The remaining V8 settings were fixed
to Cursor mode, Direct Completion, Direct FRONTIER, 1 MiB Point Lookahead, and
Dynamic coalescing. Batch64 did not enter the Point-only preallocation or
preclaim paths.

CPU attribution, mutex profiling, and detailed WAL diagnostics were disabled.
Point latency was sampled once per 10 operations (10,000 samples per run), so
throughput includes that same sampling cost in both arms. Both DATA and
FRONTIER used O_DIRECT. The host had two 375 GiB NVMe Local SSDs striped as
ext4 with `noatime,stripe=256`; it ran Ubuntu 24.04 on 16 Intel Xeon 6985P-C
vCPUs.

## Results

Throughput and CPU columns show medians of five runs per arm. Paired throughput
change is the median of the five within-round B3 versus Stage A changes. CPU is
process user+system time per Put record.

| Workload | Stage A puts/s | Stage A+B3 puts/s | Paired change | B3 wins | CPU μs/put A → B3 | Corrected puts/sync A → B3 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point8 | 33,729 | 32,957 | −2.32% | 1/5 | 48.75 → 50.03 | 7.72 → 7.70 |
| Point16 | 44,215 | 44,011 | −0.46% | 1/5 | 71.06 → 71.11 | 10.96 → 10.83 |
| Batch64 | 626,249 | 629,423 | +0.51% | 4/5 | 4.96 → 5.02 | 512.82 → 515.46 |

Arm latency columns are medians of per-run sampled percentiles. The paired p99
column is the median of the five per-round p99 changes.

| Workload | p50 ms A → B3 | p95 ms A → B3 | p99 ms A → B3 | Paired p99 change |
| --- | ---: | ---: | ---: | ---: |
| Point8 | 0.209 → 0.214 | 0.417 → 0.409 | 0.740 → 0.741 | −0.17% |
| Point16 | 0.317 → 0.319 | 0.653 → 0.656 | 0.962 → 0.934 | −1.12% |

The predeclared throughput gate required at least +5% and at least four wins
out of five for both Point writer counts. It failed at both counts. The p99
regression limit of +5% and Batch64 limit of −2% passed. The earlier
two-repetition Point8 diagnostic improvement did not persist in this
five-round qualification.

Across the five B3 runs, Point8 recorded 500,000 preclaim attempts, 355,406
grants/promotions (71.1%), and 144,594 fallbacks. Point16 recorded 500,000
attempts, 365,694 grants/promotions (73.1%), and 134,306 fallbacks. Almost all
fallbacks were caused by a busy Owner try-lock; Point16 had seven Ledger
try-lock failures. There were no quota rejections, revocations, or abandoned
claims. Batch64 preclaim counters were all zero.

## Data correction

The temporary runner incorrectly derived Batch64 `puts_per_sync` by multiplying
the 100,000 Put records by batch size 64. `write-perf` reports `ops=--num` Put
records for this workload, so the correct formula is `ops / sync_count`. Raw
results and sync counts are intact; this did not affect throughput validation
or the qualification gates. The original results archive is unchanged, and
`summary-corrected.json` contains the corrected derived metric.

## Verification and cleanup

The remote summary reports `complete=true`, 30/30 planned runs, and
`remote-exit.json` reports `failed=false`. All 30 planned runs have successful
`result.json` files and matching metadata. The transferred archive SHA-256 was
verified locally. Terraform destroyed the five managed resources; GCP
inventory checks confirmed that the VM, boot disk, restricted SSH firewall,
subnet, and isolated VPC are absent. The VM used the Free Trial attestation,
60-minute lifetime, $1.25/hour estimate, and $2 ceiling. No paid billing,
API-enablement, or quota changes were made.

- Local artifact directory: `bench-results/stage-b-gcp-final-qualification-20261124/`
- Results archive SHA-256: `2facd7549b4e3851667637dfbf632ae0b10b45268a2fefa04240cec63b916983`
- Corrected summary SHA-256: `091fdc38d3f0829b904d25c9ee62565be7260c84de974501fced316bf09d00c1`
- Bench binary SHA-256: `a3e805b8296b5ed43b4f6c139eff145f46fc877bdb936a0dcd24d4dbd7775cd9`
- Runner SHA-256: `2938528f443ecfe88cfddd11bf49d661245e7114c566ea1487891436a847df67`
- Source archive SHA-256: `4a31b273b75f6d2d5f830953eb3666f2cd9c781fe76f4bccb22bb633a16bfa96`
