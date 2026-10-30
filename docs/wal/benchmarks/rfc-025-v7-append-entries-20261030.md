# RFC 025: avoiding logical-batch zero initialization

## Result

The candidate was **not retained**. It makes the isolated multi-entry encoder
faster, but the SSD screen did not establish an end-to-end throughput and p99
improvement. At eight writers, the best eligible candidate was slightly slower
than the best eligible baseline. At sixteen writers, all samples crossed the
predeclared timing filter. The WAL source and release benchmark executable
have been restored to the dynamic-grouping baseline.

## Candidate and correctness

The v7 logical encoder normally allocates and zeroes the entire logical batch,
then writes canonical entry bytes over the zeroes while calculating the entry
CRC. The candidate instead reserves the same capacity and appends multi-entry
streams after initializing the 48-byte header. It checks that all appended
bytes fit the preflight length and capacity. Single-entry encoding uses the
existing path. The logical bytes, entry and frame CRC domains, logical SHA-256,
resource reservation, v4 path, DATA grouping, and sync policy are unchanged.
Avoiding an explicit zero initialization does not prove that the allocator or
operating system performs fewer physical memory writes.

The [candidate patch](artifacts/rfc-025-v7-append-entries-20261030-candidate.patch.gz)
contains the prototype and its tests. Forty-nine canonical encoding, v7 codec,
and v7 runtime tests passed outside the sandbox without retries. The expanded
mixed-entry differential test compares exact canonical bytes and CRC against the
existing encoder across 9,330 sequences, including duplicate points and ranges.
Existing byte-exact vectors, DATA fragment boundaries, and v7 recovery tests
also passed. A release benchmark build succeeded.

## CPU-only screens

The old and new canonical-entry encoders were measured in the **same release
test binary**, with one prebuilt canonical index and preallocated source keys
and values. Each case uses seven alternating rounds, allocates an output
buffer each iteration, and checks byte and CRC equality. Times below are
microseconds per encoded batch; the elapsed-time screen does not include
canonical-index construction, SHA-256, DATA framing, I/O, or concurrency.

| Entries, 1-KiB values | Old best / median (µs) | Append best / median (µs) | Median change |
| ---: | ---: | ---: | ---: |
| 2 | 0.175 / 0.188 | 0.161 / 0.168 | −10.3% |
| 8 | 0.691 / 0.698 | 0.640 / 0.649 | −7.0% |
| 64 | 6.794 / 6.860 | 6.268 / 6.328 | −7.8% |

For 64 entries the median saving is about 0.532 µs per batch. Multiplying by
11,719 batches gives about 6.2 ms of isolated encoder time per 750,000-put run;
this is an estimate, not a measurement of end-to-end improvement.

A separate CPU-only screen compared the existing `sha2` 0.10.9 backend with
`ring` 0.17.14 SHA-256. Both processed the same bytes, cloned the rolling state
on every update, and produced identical final digests. Their best times for
roughly 256 MiB were 104.97 / 105.05 ms for 1,040-byte updates and 104.08 /
104.07 ms for 65,600-byte updates (`sha2` / `ring`). This small host-specific
screen does not justify changing the production hash backend or adding a
dependency; it does not measure the full WAL path.

## SSD measurements

The initial screen uses three scored alternating rounds per arm at 1, 8, and
16 writers, after one warmup per arm. A separate follow-up uses two scored
rounds per arm after warmup. Every ext4 run writes 750,000 1-KiB puts in
batch64 with Parallel v7 WAL and no compaction. The candidate and baseline
binaries were frozen; 42 unique invocations and their hashes were checked.

A sample is flagged when its mean FRONTIER-write time is at least 0.12 ms or
mean `fdatasync` time is at least 0.6 ms. Those averages are not proof of an
individual stall and can miss short stalls. The table takes the highest eligible
throughput per exact binary across the initial and follow-up screens, with p99
and counters from that same invocation. The single-writer winners come from
different cohorts; the eight-writer winners are from the follow-up. These are
not controlled paired effect estimates.

| Writers | Arm | Best eligible puts/s | p99 (ms) | Process CPU (s) | Syncs | DATA groups | Eligible scored runs |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Dynamic baseline | 84,032 | 1.220 | 5.782 | 11,719 | 11,719 | 2/5 |
| 1 | Append | 84,628 | 1.213 | 5.723 | 11,719 | 11,719 | 3/5 |
| 8 | Dynamic baseline | 351,248 | 2.636 | 5.378 | 2,273 | 6,016 | 3/5 |
| 8 | Append | 348,773 | 2.608 | 5.711 | 2,361 | 5,541 | 2/5 |
| 16 | Dynamic baseline | — | — | — | — | — | 0/5 |
| 16 | Append | — | — | — | — | — | 0/5 |

The 8-writer follow-up has two eligible samples per arm. Its highest candidate
throughput is 0.7% lower and its p99 is 1.1% lower; therefore it fails the
requirement that both improve. The selected candidate also records 88 more
syncs and different DATA group formation. The CPU totals are from those same
full runs and cannot isolate the encoder cost.

Neither 16-writer arm has an eligible sample in either cohort. The best raw
initial runs show 14.9 / 16.7 ms p99 for baseline / candidate, and the best
raw follow-up runs show 15.9 / 17.5 ms. They are excluded from the table.
The follow-up's single-writer baseline samples were both flagged, while both
candidate samples were eligible. One initial candidate sample passed the
average filter despite 3.392 ms p99, demonstrating the limit of that filter.

For context, the earlier best eligible scores of the **same baseline binary**
were 84,491 / 1.203 ms, 387,287 / 2.672 ms, and 567,320 / 4.006 ms at 1, 8,
and 16 writers. The best historical v4 Parallel scores were 85,618 / 1.195 ms,
388,372 / 2.511 ms, and 595,941 / 2.967 ms. These historical runs use different
cohorts; they are context for the user's highest-score comparison, not paired
measurements of this candidate.

## Artifacts and disposition

The [initial plan](artifacts/rfc-025-v7-append-entries-20261030-plan.json) and
[summary](artifacts/rfc-025-v7-append-entries-20261030-summary.json),
[follow-up plan](artifacts/rfc-025-v7-append-entries-followup-20261030-plan.json)
and [summary](artifacts/rfc-025-v7-append-entries-followup-20261030-summary.json),
plus [environment](artifacts/rfc-025-v7-append-entries-20261030-environment.json),
record the runs, filters, and binary identities. Raw JSONL, host audits, run
manifests, capture logs, and runner scripts share those prefixes. Conditional
point-workload guards were not run because neither batch screen qualified.

The [encoder diagnostic](artifacts/rfc-025-v7-append-entries-20261030-microbench.rs)
and [output](artifacts/rfc-025-v7-append-entries-20261030-microbench.log.gz)
record the isolated CPU comparison. It was temporarily included as a
`#[cfg(test)]` module in `pitr`, run with `cargo test --release --all-features
--lib logical_append_cpu_screen -- --ignored --nocapture --test-threads=1`,
and removed before restoring the candidate patch. The
[diagnostic runner](artifacts/rfc-025-v7-append-entries-20261030-microbench-run.py)
records the exact local steps. The SHA backend [source](artifacts/rfc-025-v7-append-entries-20261030-sha-backend.rs),
[manifest](artifacts/rfc-025-v7-append-entries-20261030-sha-backend-Cargo.toml),
[lockfile](artifacts/rfc-025-v7-append-entries-20261030-sha-backend-Cargo.lock),
and [results](artifacts/rfc-025-v7-append-entries-20261030-sha-backend-results.txt)
are preserved separately; no backend dependency was added to the project.
