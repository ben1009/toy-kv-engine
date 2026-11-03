from pathlib import Path
import json


p = Path(__file__).resolve().parent
repo = p.parent.parent
rows = [json.loads(line) for line in (p / "measurements.jsonl").read_text().splitlines()]
summary = json.loads((p / "summary.json").read_text())
assert len(rows) == 40
normal = [r for r in rows if r["mode"] == "normal"]
diagnostic = [r for r in rows if r["mode"] == "diagnostic"]
groups = {(r["format"], r["affinity"]): r for r in summary["groups"] if r["mode"] == "normal"}
prefix = p.name
link = lambda suffix: f"artifacts/{prefix}-{suffix}"
best_rows = []
for fmt, affinity in [("v7", "hybrid"), ("v7", "pcore"), ("v8", "hybrid"), ("v8", "pcore")]:
    group = groups[fmt, affinity]
    best = group["best_eligible"]
    if best:
        score = f"{best['puts_s']:,.0f} | {best['p99_ms']:.3f} | {best['cpu_s']:.3f} | `{best['name']}`"
    else:
        score = "— | — | — | no eligible score"
    best_rows.append(f"| {fmt.upper()} | {'default 0–31' if affinity == 'hybrid' else 'P cores 0–15'} | {group['eligible']}/{group['total']} | {score} |")
a = groups["v8", "hybrid"]["best_eligible"]
b = groups["v8", "pcore"]["best_eligible"]
assert a and b
by_name = {r["name"]: r for r in rows}
change = lambda before, after: 100 * (after / before - 1)
metric_rows = []
for label, before, after, spec in [
    ("Throughput (puts/s)", a["puts_s"], b["puts_s"], ",.0f"),
    ("p99 (ms)", a["p99_ms"], b["p99_ms"], ".3f"),
    ("Process CPU (s)", a["cpu_s"], b["cpu_s"], ".3f"),
    ("Estimated payload BLAKE3 CPU (s)", a["digest_cpu_estimates"]["payload_hash_cpu_estimated_s"], b["digest_cpu_estimates"]["payload_hash_cpu_estimated_s"], ".3f"),
    ("Voluntary context switches", a["context"]["voluntary"], b["context"]["voluntary"], ",.0f"),
    ("Involuntary context switches", a["context"]["involuntary"], b["context"]["involuntary"], ",.0f"),
    ("Syncs", by_name[a["name"]]["record"]["result"]["wal_pipeline"]["sync_count"], by_name[b["name"]]["record"]["result"]["wal_pipeline"]["sync_count"], ",.0f"),
    ("Mean fdatasync (ms)", a["mean_sync_ms"], b["mean_sync_ms"], ".3f"),
]:
    metric_rows.append(f"| {label} | {format(before, spec)} | {format(after, spec)} | {change(before, after):+.1f}% |")
raw_rows = [f"| `{r['name']}` | {r['puts_s']:,.0f} | {r['p99_ms']:.3f} | {r['cpu_s']:.3f} | {r['mean_sync_ms']:.3f} | {'flagged' if r['stall_flag'] else 'eligible'} |" for r in normal]
phase_rows = []
for r in diagnostic:
    phases = r["phase_estimates_inclusive"]
    phase = lambda label: phases.get(label, {}).get("cpu_s", 0.)
    digest = r["digest_cpu_estimates"]
    hash_cpu = digest["payload_hash_cpu_estimated_s"] if digest else phase("v7_logical_hash")
    phase_rows.append(f"| `{r['name']}` | {'flagged' if r['stall_flag'] else 'eligible'} | {phase('encode'):.3f} | {phase('frame'):.3f} | {hash_cpu:.3f} | {phase('memtable_publish'):.3f} | {phase('mvcc_publish'):.3f} | {r['cpu_s']:.3f} |")
placement = json.loads((p / "cpu-sample-summary.json").read_text())["samples"]
placement_rows = []
for name, values in placement.items():
    key = "blake3_hash_many" if "v8" in name else "sha256_on_ordered_packer"
    sample = values[key]
    placement_rows.append(f"| `{name}` | {'BLAKE3 bulk leaf' if 'v8' in name else 'ordered-packer SHA256 leaf'} | {sample['P_samples']} | {sample['E_samples']} | {sample['E_pct']:.1f}% |")
v7_missing = any(groups["v7", mask]["best_eligible"] is None for mask in ["hybrid", "pcore"])
conclusion = ("The V8 affinity result is promising, but the four-way V7/V8 comparison remains inconclusive: at least one V7 control cell has no eligible run. This experiment does not establish V8/V7 parity or a format-specific affinity advantage." if v7_missing else "All four normal control cells have eligible scores. The table uses the highest eligible score in each cell, including the later user-requested V7 cohort; it is not a fully matched four-way cohort or a production affinity qualification.")
rerun = [r for r in normal if r["cohort"] == "v7-user-rerun"]
assert len(rerun) == 8
rerun_counts = "; ".join(f"{mask}: {sum(not r['stall_flag'] for r in rerun if r['affinity'] == mask)}/4 eligible" for mask in ["hybrid", "pcore"])
v7p = groups["v7", "pcore"]["best_eligible"]
p_comparison = (
    f"With P-core affinity, the best V8 score is {change(v7p['puts_s'], b['puts_s']):+.1f}% versus V7 throughput, "
    f"{change(v7p['p99_ms'], b['p99_ms']):+.1f}% in p99, and {change(v7p['cpu_s'], b['cpu_s']):+.1f}% in process CPU. "
    "The remaining CPU gap is material even when throughput is close. These best scores come from different cohorts; they are not a matched pair."
    if v7p else "There is no eligible P-core V7 score for a same-mask format comparison."
)
document = f"""# RFC 026: V7/V8 P-core affinity comparison, 2026-11-02

Status: diagnostic experiment; production runtime and affinity defaults unchanged.
The retained notification separation, single durability driver and published
cursor remain in place. The source still matches `b8da402a`; `efa90f11` added
the preceding attribution report.

Best eligible V8 batch64 throughput rises from **{a['puts_s']:,.0f}** to
**{b['puts_s']:,.0f} puts/s** with whole-process P-core affinity
({change(a['puts_s'], b['puts_s']):+.1f}%), while same-run p99 and process CPU improve.
{conclusion}

## Normal-counter comparison

Each row selects the highest eligible throughput for that format/mask using the
same retained normal-counter executable. p99, CPU and supporting counters come
from that same run. Diagnostic scores never enter this ranking.

| Format | Affinity | Eligible / runs | Best puts/s | Same-run p99 (ms) | Same-run CPU (s) | Selected run |
|---|---|---:|---:|---:|---:|---|
{chr(10).join(best_rows)}

{p_comparison}

V8 eligible throughput medians are
{groups['v8', 'hybrid']['median_eligible']['puts_s']:,.0f} puts/s (default) and
{groups['v8', 'pcore']['median_eligible']['puts_s']:,.0f} puts/s (P cores).
There are only two eligible observations per V8 cell. Both clean P-core runs
are in the initial cohort; the longer-cooldown confirmation has no clean P-core
V8 run. This limits repeatability claims.

The best V8 default/P-core scores are adjacent initial runs `01` and `02`.
Their supporting counters are:

| Metric | Default | P cores | Change |
|---|---:|---:|---:|
{chr(10).join(metric_rows)}

The second clean P-core V8 run reaches 638,421 puts/s, p99 3.156ms and CPU
6.203s. The second clean default V8 run reaches 472,040 puts/s, p99 4.830ms and
CPU 9.543s. Every flagged record remains in the raw table and artifact.

## What affinity changes

The i9-13900T kernel exposes P-core logical CPUs `0-15` and E-core CPUs
`16-31`. Default runs inherit `0-31`. P-core runs start the benchmark with
`taskset -c 0-15`; its userspace threads inherit that mask. Kernel I/O work is
not separately pinned. CPU-aware perf samples confirm zero E-core userspace
samples in all four P-core diagnostic recordings.

Whole-process affinity also reduces the available physical cores from 24 to
8 with SMT and changes placement for encoding, publication, grouping and I/O
coordination together. This is not an isolated producer-hash intervention.
The unchanged coalescing policy produces fewer syncs in the best P-core V8 run;
affinity therefore changes pipeline timing as well as hash execution cost.

In the selected V8 pair, process CPU falls by 2.310s while sampled payload
BLAKE3 CPU falls by about 0.263s, roughly 11.4% of that difference. The remaining
saving cannot be attributed to BLAKE3 alone. A bounded preparation stage might
recover only part of the whole-process result and would introduce its own
coordination costs. No such production stage is introduced here.

The previous report's 53.1% E-core BLAKE3 figure is a time-weighted CPU sample
share, not a fraction of batches or bytes. It cannot be used directly as a
job-placement probability to weight P/E microbench times or predict a 38%
hash saving. The live CPU measurements above provide the observed comparison.

## Separate phase diagnostics

Eight separate captures use the preserved attribution executable, whole-batch
phase clocks sampled once per 64 calls per thread, and timed-only perf recording.
Only {sum(not r['stall_flag'] for r in diagnostic)}/8 passes the storage filter;
all P-core diagnostics are flagged. These figures are diagnostic observations,
not qualified performance comparisons or proof of a publication speedup.

All times below are estimated thread CPU seconds except process CPU, which is
the measured timed process CPU. Encoding contains allocation and entry encoding;
for V8 it also contains payload hashing. **Do not add hash to V8 encoding.** V7
ordered SHA is outside its encoding/frame scopes. Do not add nested child scopes
or extrapolate stalled publication costs to clean runs.

| Capture | Storage filter | Encode parent | Frames + CRC | Payload hash (V7 SHA / V8 BLAKE3) | Memtable publication | MVCC publication | Process CPU |
|---|---|---:|---:|---:|---:|---:|---:|
{chr(10).join(phase_rows)}

All V8 runs report exactly 787,500,000 payload hash bytes and zero
`post_encode_hash_reread_bytes`. Hash CPU estimates scale the existing 184
sampled ticket timings to the exact 11,728 logical batches. No digest formula,
CRC implementation, allocator, DATA grouping, sync policy or wire bytes changed.

The CPU-aware leaf samples show the placement intervention directly:

| Capture | Hash leaf category | P samples | E samples | E share |
|---|---|---:|---:|---:|
{chr(10).join(placement_rows)}

V7's SHA subset is restricted to thread IDs that actually execute the measured
ordered logical-prefix SHA scope; it does not combine the physical WAL digest
thread. Leaf shares describe sampled CPU time, not operations or elapsed time.

## Method and storage qualification

All captures run natively outside the sandbox on the same ext4 filesystem
(`/dev/nvme0n1p3`). Workload: 750,000 puts, 16 writers, batch64, 1KiB values,
Parallel WAL, no compaction, 2GiB SST target, latency sampling every 10 batches.
PITR maintenance remains outside the timed interval.

The initial plan fixes four balanced normal rounds, one capture per cell per
round, then two reversed diagnostic rounds. Each round starts after 45s idle;
captures are separated by 3s. Its 16 normal runs yielded no eligible V7 control.
A separate fixed confirmation adds two reversed normal rounds with 120s idle
per round and 10s gaps, retaining separate cohort labels.

The maintainer then explicitly requested a V7 rerun. A new fixed plan adds
eight V7-only normal captures, four per mask, with reversed orders, 120s idle
per round and 10s gaps. Rerun outcome: **{rerun_counts}**.
Earlier captures and V8 scores remain unchanged.

The predeclared storage flag is `p99 > 10ms OR mean fdatasync > 1ms`. It is
identical for both formats and affinities. This produces
{sum(r['stall_flag'] for r in normal)}/{len(normal)} flagged normal captures and
{sum(r['stall_flag'] for r in diagnostic)}/{len(diagnostic)} flagged diagnostics,
{summary['stall_flagged']}/{summary['samples']} overall. No flagged score is
promoted because it is fast, and no record is silently replaced. Missing
controls do not establish that the missing format is slower.

No build, test or other benchmark from this task runs concurrently with a
capture. Normal captures use the existing context-switch probe and normal
benchmark counters, without `--profile`, new phase scopes or perf. Diagnostic
captures use `cpu-clock:u` at 499Hz, DWARF4096 and `--sample-cpu`, enabled only
inside the timed interval through the existing perf control FIFO. Perf writes
to a memory file until each timed interval has ended. Offline stack export
finishes before the confirmation cooldown begins.

## All normal captures

| Capture | Puts/s | p99 (ms) | Process CPU (s) | Mean sync (ms) | Storage filter |
|---|---:|---:|---:|---:|---|
{chr(10).join(raw_rows)}

## Reproduction and checks

The normal executable SHA256 is
`937a3843a044e107e15db756674be151875e388ff3a0a392cbd4e97463bbd2aa`.
The separate diagnostic executable SHA256 is
`fbb90ea8a6f20bf476760e79ae60e04cda3edae04138d45ae5a9e6346da299f1`.
Both are preserved from the preceding experiments; no recompilation was needed.
The preceding [producer attribution report](rfc-026-v8-producer-attribution-20261102.md)
contains the diagnostic source patch and instrumentation limitations.

The [initial plan]({link('plan.json')}),
[confirmation plan]({link('confirmation-plan.json')}),
[user-requested V7 rerun plan]({link('v7-rerun-plan.json')}),
[summary]({link('summary.json')}),
[complete measurements including raw JSON and phase records]({link('measurements.jsonl')}),
[CPU sample summary]({link('cpu-sample-summary.json')}),
and [checks]({link('checks.json')}) preserve the full result.
The [initial runner]({link('run.py')}),
[confirmation runner]({link('confirm.py')}),
[V7 rerun runner]({link('v7-rerun.py')}),
[analysis]({link('analyze.py')}), [report generator]({link('write-report.py')})
and [archive script]({link('archive.py')})
use paths under `target/{prefix}/`, which need adjustment on another machine.
Original perf recordings, stacks, normal JSON, phase files and logs remain there.

All 40 commands exit successfully. Every raw-record checksum, operation count,
V8 hash-byte count and zero-reread assertion passes. All 16 perf exports succeed.
The original source checksums and empty runtime Git diff confirm no Rust change;
only this report, its artifacts and the documentation index are added. No
production affinity default or performance qualification gate changes.
"""
target = repo / f"docs/wal/benchmarks/{prefix}.md"
target.write_text(document)
print(target, len(document.splitlines()), "lines")
