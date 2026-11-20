# V7/V8 Point Sync-Decision Snapshots

Date: 2026-11-19

## Scope and method

This diagnostic follows the [V8 Point Admission and Sequencer breakdown](rfc-026-v8-point-admission-sequencer-gcp-20261119.md). It records the state visible when each V7/V8 Parallel durability group is selected: admitted tickets, contiguous producer-frame-ready and DATA-complete prefixes, plus the first ticket beyond the selected group. It does not change WAL runtime behavior.

The run used source commit `406230c69831d4fcd170139d4d2ec835c3bba862` and release binary SHA-256 `ead6ca03e865de65a985fc5bca3a036ed8f1a326bdd772f4398f5c3e5c12015a`. The host was one `c4-standard-16-lssd` with two NVMe Local SSDs in RAID0, ext4, and kernel `7.0.0-1017-gcp`. Rust was `nightly-2026-09-23`.

Each cell ran 100,000 1 KiB Point puts. The two arms were V7 Parallel and V8 Parallel with 1 MiB Point-only Lookahead, Direct FRONTIER, and Dynamic Coalescing. There was one instrumented run per writer/version cell (eight runs total). `--wal-sync-diagnostics` was enabled, and every sync produced an observation; these are diagnostic data, not uninstrumented throughput scores. The measured throughput and latency fields in the run outputs are therefore contextual only.

The initial VM bootstrap was given a mistyped source SHA and stopped at `git fetch`, before any benchmark started. The corrected run fetched and verified the full source SHA above on that same retained VM. The archived `host.txt` records the initial bootstrap argument; the completed run's `commit.txt`, plan, summary, per-run metadata, and build log record the verified source SHA.

## Sync-decision observations

Prefix values are ticket counts at the sync-selection snapshot. `Admitted` is the unresolved ticket count; `Frames` is the contiguous producer-frame-ready prefix; `DATA` is the contiguous prefix whose DATA I/O had completed. Mean and p50 are shown as `mean / p50`. `Next present / frames-ready / DATA-complete` counts sync decisions with the first admitted ticket beyond the selected group in each state; these are event counts, not unique tickets.

| Writers | WAL | Syncs | Puts/sync | Admitted mean / p50 | Frames mean / p50 | DATA mean / p50 | Next present / ready / complete |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | V7 | 100,000 | 1.00 | 1.00 / 1 | 1.00 / 1 | 1.00 / 1 | 0 / 0 / 0 |
| 1 | V8 | 100,000 | 1.00 | 1.00 / 1 | 1.00 / 1 | 1.00 / 1 | 0 / 0 / 0 |
| 4 | V7 | 25,044 | 3.99 | 4.00 / 4 | 4.00 / 4 | 3.99 / 4 | 36 / 36 / 0 |
| 4 | V8 | 25,224 | 3.96 | 3.99 / 4 | 3.99 / 4 | 3.96 / 4 | 225 / 225 / 0 |
| 8 | V7 | 13,049 | 7.66 | 7.71 / 8 | 7.71 / 8 | 7.66 / 8 | 231 / 231 / 0 |
| 8 | V8 | 13,095 | 7.64 | 7.79 / 8 | 7.79 / 8 | 7.64 / 8 | 670 / 669 / 0 |
| 16 | V7 | 8,301 | 12.05 | 13.33 / 16 | 13.33 / 16 | 12.05 / 15 | 2,538 / 2,538 / 0 |
| 16 | V8 | 10,179 | 9.82 | 12.14 / 15 | 11.98 / 14 | 9.82 / 10 | 5,815 / 5,764 / 0 |

At one writer, both implementations sync each Put. At four and eight writers, their median DATA-complete prefixes are nearly the full admitted groups. The clearest divergence is at sixteen writers: V8's mean admitted count is 12.14 versus V7's 13.33, while the mean DATA-complete prefix is 9.82 versus 12.05. V8's median DATA-complete prefix is 10 tickets, compared with 15 for V7.

At sixteen writers, a ticket beyond the selected group was present for 57.1% of V8 sync decisions and 30.6% of V7 decisions. It was frame-ready in 56.6% of all V8 decisions and 30.6% of V7 decisions, but the next ticket was DATA-complete in none of the observations in either arm. These observations show that V8 more often has a following, already frame-ready ticket while the DATA-complete prefix remains shorter. They do not establish why that ticket is incomplete.

## Interpretation and next diagnostic

The 16-writer snapshot locates the visible readiness gap between the producer-frame-ready prefix and DATA-complete prefix. Admission alone does not explain the whole difference: V8's mean DATA-complete prefix trails its own mean frame-ready prefix by 2.16 tickets, versus 1.28 tickets for V7. This is consistent with DATA I/O completion or completion-state publication arriving later relative to V8's sync decision, but the snapshot cannot separate device latency, CQE processing, completion-reaper scheduling, or other timing effects. It also does not prove that this gap causes the throughput difference.

The next diagnostic should correlate the same ticket through DATA submission, write CQE completion, completion-reaper processing, publication of `data_complete`, and the frontier decision. Focus on sixteen writers, with eight writers as a control. Do not change coalescing or group-formation policy based on these snapshots alone.

## Validation, artifacts, and cleanup

The runner reported `complete: true` with 8/8 planned cells completed; `remote-exit.json` reported `failed: false` and 8 runs started. The schedule validator found no missing, duplicate, unexpected, or invalid cells. Every run has a successful `result.json`, matching `run-metadata.json`, and raw `sync-observations.json` whose record count matches that run's sync count.

The verified 24 MiB archive, normalized checksum sidecar, original remote checksum sidecar, and extracted results are retained locally under `/tmp/gcp-v8-sync-decision-406230c6/recovered/`. Archive SHA-256: `2e0251df022585326999564dc6269a7e4275a1ff9777e00114f288b97956720e`. Raw results are not checked into the repository.

After local verification, the VM and boot disk, temporary SSH firewall rule, isolated VPC, subnet, and egress route were deleted. Read-only inventory checks found none of those resources, and Terraform state is empty. Billing, APIs, and quota settings were not changed.
