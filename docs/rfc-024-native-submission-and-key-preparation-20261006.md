# RFC 024: native submission and key preparation, 2026-10-06

Neither candidate is retained. The baseline includes the retained
[coalescing cutoff refresh](rfc-024-native-cutoff-refresh-20261006.md) and
the current working-tree default selection. Both scored arms explicitly
select native async parallel WAL. These experiments do not change the
leader implementation or establish a new leader-relative result.

Forced-asynchronous submission reports **+1.5% paired throughput at sixteen
writers**, below the frozen +2% incremental threshold. Early key preparation
reports **-6.1%**, with higher process CPU per put. Both sessions fail their
identical-baseline controls, so neither result qualifies as a measured
production improvement. The full RFC adoption gate remains unchanged.

## Candidates

The preceding diagnostics report roughly 15–16 microseconds per submission
call and 308–333 microseconds from submission return to CQE consumption at
sixteen writers. Candidate A adds `IOSQE_ASYNC` to the dedicated worker's
write SQEs. It requests immediate helper-thread execution, which can add
scheduling overhead. See the
[liburing SQE flags documentation](https://www.man7.org/linux/man-pages/man3/io_uring_sqe_set_flags.3.html).
No other runtime code changes in this candidate.

Candidate B starts independently from the baseline, excluding A's flag.
It prepares validated mutable internal keys and prefixed values before the
fair native preparation permit. The existing sequencer still assigns the
timestamp, stamps every key, and admits the WAL batch together. Failed
try-lock and WAL-full attempts reuse the templates and acquire no WAL ticket;
a retry stamps its fresh timestamp. Conversion to immutable publication
entries happens after installing the admitted-commit cancellation guard.
The existing PITR fallback retains its validation and I/O path.

B moves allocation and encoding work between stages; it does not eliminate
that work. Its mutable-entry vector also requires conversion to the final
publication-entry vector. Its admission timer excludes moved preparation
and publication construction, so a shorter timer cannot establish a speedup.
End-to-end throughput, latency, and process CPU remain the scored measures.

Neither candidate changes physical ordering, captured fdatasync prefixes,
poison boundaries, rotation cutoffs, buffer ownership, publication ordering,
the 32-group limit, or the 256-SQE ring capacity.

## Frozen comparison

Each primary observation uses 524,288 puts as 8,192 batches of 64, 1 KiB
values, eight Tokio workers, and a 1 GiB memtable target. PITR and serializable
transactions are off; no rotation occurs during timing. Fresh databases are
on ext4 `/dev/nvme0n1p3`, with Linux `6.12.71-1-lts`.

Each session has two warmups per arm/case, three fixed ABBA/BAAB/ABBA blocks
with two observations per arm, rotating eight/sixteen-writer case order,
and an identical-baseline pair per case after block one. Binaries are staged
in RAM. Runs have five-second idle gaps, and no builds, tests, traces, or
device administration overlap timing.

The predeclared incremental rule requires at least +2% sixteen-writer paired
throughput, improvement in every target block, at least two passing repeat
blocks per case, every null control passing, and both case medians within
-5% throughput and +10% p99. The small-gain threshold follows the maintainer's
retention preference; it does not revise earlier experiments or the RFC gate.
Single-writer and one-put guards would be required after a qualifying screen.
Neither screen qualifies, so those additional guard runs are not performed.

Repeat and null controls allow at most 5% throughput spread and 10% p99
spread. An anomalous primary run triggers an extra one-second pause and one
excluded retry. A block below 0.90 throughput ratio or above 1.20 p99 ratio
triggers an excluded reversed-block retry after that pause. Every original
remains scored.

## Results

Paired changes are medians of the three block geometric-mean ratios.
Absolute throughput medians use all six primary observations per arm;
their quotient is not the paired estimate. Positive p99 or CPU change is
worse. Failed controls are preserved.

| Candidate | Writers | Baseline puts/s | Candidate puts/s | Paired throughput | Paired p99 | Paired CPU/put | Repeat blocks | Null |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| A: forced async SQEs | 8 | 340,960 | 366,454 | +7.3% | -5.7% | -7.4% | 2/3 | Fail |
| A: forced async SQEs | 16 | 637,523 | 649,419 | +1.5% | -2.2% | +0.1% | 1/3 | Fail |
| B: early key preparation | 8 | 294,738 | 218,137 | -26.8% | +81.0% | -3.2% | 0/3 | Fail |
| B: early key preparation | 16 | 599,225 | 538,310 | -6.1% | +4.1% | +17.6% | 1/3 | Fail |

A's target block throughput changes are +2.3%, +1.5%, and +0.8%.
B's are -6.1%, -63.3%, and +0.2%. B's final target block passes its repeat
control, but still has +12.6% CPU/put and only +0.2% throughput. Original
storage stalls dominate several other blocks; these observations cannot
isolate their cause to the candidate code.

The baseline/baseline controls expose the environment instability directly:

| Session | Writers | First puts/s | Second puts/s | First p99 ms | Second p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | 8 | 352,164 | 80,317 | 2.469 | 48.509 |
| A | 16 | 90,768 | 107,210 | 28.145 | 47.473 |
| B | 8 | 368,551 | 74,556 | 2.433 | 24.337 |
| B | 16 | 85,722 | 462,584 | 32.205 | 3.688 |

Both arms in both sessions reach eight in-flight groups at eight writers
and 15–16 at sixteen writers. Outstanding write-SQE peaks match those
counts for this workload. The configured 32-group cap is not reached.
These are software pipeline counts, not device queue depth.

A retains 42 observations; B retains 57. Together they include 16 warmups,
48 primary observations, eight null observations, seven excluded single
retries, and twenty excluded block-retry observations. No retry replaces
an original.

## Validation and artifacts

Initial workspace checks using the shared target directory reused an older
test binary. Cargo metadata identified the private package, but test
discovery omitted B's two newly added tests. Those initial checks are
excluded from accepted validation; their logs remain archived.

Both prototypes are checked again using separate fresh target directories.
Formatting and default/all-feature all-target Clippy pass with `-D warnings`.
A passes all 1,292 library tests and B all 1,294, without skips or retries.
B's new tests verify binary/boundary keys, timestamp restamping, unchanged
values and encoded lengths, and oversized-input rejection. Both prototypes
also pass three public-API probe tests covering publication and recovery.
A fresh isolated release build reproduces B's timing executable hash.

Independent audits reconcile all 99 observations, frozen orders, retries,
block/null estimates, raw CQE/buffer/sample counts, and cleanup. All 115
recorded production inputs and each candidate's 150 frozen inputs remain
unchanged. Owned databases and RAM staging directories are removed.

Archives contain protocols, source snapshots, candidate patches, drivers,
raw outputs, summaries, hashes, validation provenance, and independent audits:

- `target/rfc024-native-async-submission-20261006/`: A.
- `target/rfc024-native-key-preparation-20261006/`: B.

Baseline executable SHA-256:
`7f113ba4b8c116ea7adfb3bc058b983f7b10ad64db4023a5e93ecc62f8ecef76`.
A executable:
`d981a0c5299869f27939fc4806e4149e161c8a513034e79b43f7b194414e62a6`.
B executable:
`f0cdfc7c3fb507c479b1ab229f6e4c418d620f6c539e97f4b272f058eb4263a9`.
