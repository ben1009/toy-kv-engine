#!/usr/bin/env python3
"""Verify fixed SSD captures and rank best eligible runs with their own p99."""
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-dynamic-window-20261030'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def analyze(cell):
    cfg, plan, identity = read('-matrix.json')['cells'][cell], read('-plan.json'), read('-binaries.json')
    prefix = STEM + '-' + cell
    lines = (ART / (prefix + '.jsonl')).read_bytes().splitlines()
    rows = list(csv.DictReader((ART / (prefix + '-runs.tsv')).open(), delimiter='\t'))
    audits = (ART / (prefix + '-host.jsonl')).read_bytes().splitlines()
    raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest()) for line in lines for r in [json.loads(line)]}
    host = {r['run_id']: (r, hashlib.sha256(line).hexdigest()) for line in audits for r in [json.loads(line)]}
    schedule = [('warmup', 0, arm) for arm in cfg['arms']]
    for round_id in range(1, cfg['pairs'] + 1):
        order = cfg['arms'][(round_id - 1 + cfg['schedule_offset']) % len(cfg['arms']):] + cfg['arms'][:(round_id - 1 + cfg['schedule_offset']) % len(cfg['arms'])]
        schedule += [('scored', round_id, arm) for arm in order]
    assert len(raw) == len(lines) == len(rows) == len(host) == len(audits) == len(schedule)
    assert [(r['phase'], int(r['pair']), r['arm']) for r in rows] == schedule
    per_arm = {arm: [] for arm in cfg['arms']}
    all_masks = []
    for row in rows:
        r, digest = raw[row['run_id']]
        audit, audit_digest = host[row['run_id']]
        assert digest == row['raw_sha256'] and audit_digest == row['audit_sha256']
        arm = row['arm']
        assert row['binary_sha256'] == audit['binary_sha256'] == identity[arm]['sha256']
        assert (audit['phase'], audit['pair'], audit['arm']) == (row['phase'], int(row['pair']), arm)
        assert audit['clean_sampled'] and audit['returncode'] == 0 and not audit['sampling_errors']
        assert audit['benchmark_pid'] == audit['child_rusage']['pid'] != audit['launcher_pid']
        assert hashlib.sha256((json.dumps(audit['child_rusage'], sort_keys=True) + '\n').encode()).hexdigest() == audit['child_rusage_sha256']
        assert all(not s['competing'] for s in [audit['before'], *audit['during'], audit['after']])
        p, result = r['params'], r['result']
        assert r['workload'] == cfg['bench'] and p['wal_format_version'] == (7 if arm.endswith('v7') else 4)
        assert p['wal_io_mode'] == 'parallel' and p['value_size'] == 1024 and p['threads'] == cfg['writers']
        assert p['batch_size'] == (cfg['batch'] or None) and result['ops'] == cfg['operations']
        assert r['engine_options']['target_sst_size'] == 2147483648 and r['engine_options']['compaction'] == 'none'
        assert r['latency']['sample_every'] == cfg['sample_every']
        counts = [cfg['operations'] // cfg['writers'] + (t < cfg['operations'] % cfg['writers']) for t in range(cfg['writers'])]
        calls = [(n + cfg['batch'] - 1) // cfg['batch'] for n in counts] if cfg['batch'] else counts
        assert r['latency']['samples'] == sum((n + cfg['sample_every'] - 1) // cfg['sample_every'] for n in calls)
        pipeline = result['wal_pipeline']
        marker = pipeline['frontier_write_ns'] / max(pipeline['frontier_writes'], 1) / 1e6
        sync = pipeline['fdatasync_ns'] / max(pipeline['sync_count'], 1) / 1e6
        flagged = marker >= plan['stall_filter']['frontier_mean_ms_at_least'] or sync >= plan['stall_filter']['fdatasync_mean_ms_at_least']
        item = dict(run_id=r['run_id'], arm=arm, phase=row['phase'], round=int(row['pair']),
                    puts_s=result['ops_per_sec'], p99_ms=r['latency']['p99_ms'],
                    elapsed_ms=result['measure_elapsed_ms'],
                    cpu_ms=result['process_cpu_user_ms'] + result['process_cpu_system_ms'],
                    flagged=flagged, frontier_mean_ms=marker, fdatasync_mean_ms=sync,
                    raw_sha256=digest, binary_sha256=identity[arm]['sha256'], pipeline=pipeline)
        all_masks.append({k: item[k] for k in ['run_id', 'arm', 'phase', 'round', 'flagged', 'frontier_mean_ms', 'fdatasync_mean_ms']})
        if row['phase'] == 'scored':
            per_arm[arm].append(item)
    peaks = {}
    for arm, items in per_arm.items():
        eligible = [item for item in items if not item['flagged']]
        rank = lambda r: (r['puts_s'], r['run_id'])
        peaks[arm] = dict(scored=len(items), eligible=len(eligible),
                          best=max(eligible, key=rank) if eligible else None,
                          raw_best=max(items, key=rank),
                          median_all_rate=statistics.median(r['puts_s'] for r in items))
    old, new = peaks['retained-v7']['best'], peaks['candidate-v7']['best']
    ratios = dict(throughput=new['puts_s'] / old['puts_s'], p99=new['p99_ms'] / old['p99_ms'],
                  cpu=new['cpu_ms'] / old['cpu_ms']) if old and new else None
    bounds = plan['screen_gate' if cell in plan['screen_cells'] else 'guard_gate']
    checks = dict(enough_eligible=all(peaks[a]['eligible'] >= bounds['minimum_eligible_runs_per_arm'] for a in (['retained-v4', 'retained-v7', 'candidate-v7'] if 'retained-v4' in peaks else ['retained-v7', 'candidate-v7'])),
                  throughput=bool(ratios and ratios['throughput'] >= bounds['minimum_throughput_ratio']),
                  p99=bool(ratios and ratios['p99'] <= bounds['maximum_p99_ratio']))
    v4 = peaks.get('retained-v4', {}).get('best')
    vs_v4 = dict(throughput=new['puts_s']/v4['puts_s'], p99=new['p99_ms']/v4['p99_ms']) if new and v4 else None
    return dict(vs_v4=vs_v4, invocations=len(raw), scored_records=sum(len(x) for x in per_arm.values()),
                run_ids=list(raw), peaks=peaks, ratios=ratios, checks=checks, passed=all(checks.values()),
                stall_masks=all_masks,
                artifact_sha256={suffix: sha(ART / (prefix + suffix)) for suffix in ['.jsonl', '-runs.tsv', '-host.jsonl']})


def main():
    matrix, plan = read('-matrix.json'), read('-plan.json')
    cells = sys.argv[1:] or [c for c in matrix['cells'] if (ART / (STEM + '-' + c + '.jsonl')).exists()]
    results = {cell: analyze(cell) for cell in cells}
    ids = [run for result in results.values() for run in result['run_ids']]
    assert len(ids) == len(set(ids))
    screen_done = all(cell in results for cell in plan['screen_cells'])
    screen_passed = screen_done and all(results[cell]['passed'] for cell in plan['screen_cells'])
    guards_done = all(cell in results for cell in plan['guard_cells'])
    output = dict(method=plan['ranking'], cells=results, invocations=len(ids),
                  scored_records=sum(r['scored_records'] for r in results.values()),
                  screens_complete=screen_done, screen_passed=screen_passed,
                  guards_complete=guards_done,
                  retention_passed=screen_passed and guards_done and all(r['passed'] for r in results.values()),
                  missing_cells=[c for c in matrix['cells'] if c not in results],
                  plan_sha256=sha(ART / (STEM + '-plan.json')),
                  analyzer_sha256=sha(Path(__file__)))
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
