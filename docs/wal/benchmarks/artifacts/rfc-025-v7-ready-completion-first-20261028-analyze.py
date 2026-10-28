#!/usr/bin/env python3
"""Validate retained/candidate SSD captures without dropping stalls."""
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-ready-completion-first-20261028'
MATRIX = json.loads((ART / (STEM + '-matrix.json')).read_text())
IDENTITY = json.loads((ART / (STEM + '-binaries.json')).read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def analyze(cell):
    cfg = MATRIX['cells'][cell]
    stem = STEM + '-' + cell
    lines = (ART / (stem + '.jsonl')).read_bytes().splitlines()
    raw = {(r['run_id'], r['measurement']): (r, hashlib.sha256(line).hexdigest())
           for line in lines for r in [json.loads(line)]}
    rows = list(csv.DictReader((ART / (stem + '-runs.tsv')).open(), delimiter='\t'))
    audit_lines = (ART / (stem + '-host.jsonl')).read_bytes().splitlines()
    audit = {r['run_id']: (r, hashlib.sha256(line).hexdigest())
             for line in audit_lines for r in [json.loads(line)]}
    invocations = len(cfg['arms']) * (cfg['pairs'] + 1)
    parts = list(cfg['parts']) if cfg['kind'] == 'cpu' else [rows[0]['part']]
    assert len(raw) == len(lines) == len(rows) == invocations * len(parts)
    assert len(audit) == len(audit_lines) == invocations
    schedule = [('warmup', 0, arm) for arm in cfg['arms']]
    for pair in range(1, cfg['pairs'] + 1):
        schedule += [('scored', pair, arm) for arm in (cfg['arms'] if pair % 2 else list(reversed(cfg['arms'])))]
    assert [(r['phase'], int(r['pair']), r['arm'], r['part']) for r in rows] == [
        (*item, part) for item in schedule for part in parts]
    host_samples = 0
    for item, _ in audit.values():
        assert item['clean_sampled'] and item['returncode'] == 0 and not item['sampling_errors']
        assert item['during']
        assert item['benchmark_pid'] == item['child_rusage']['pid'] != item['launcher_pid']
        encoded = (json.dumps(item['child_rusage'], sort_keys=True) + '\n').encode()
        assert hashlib.sha256(encoded).hexdigest() == item['child_rusage_sha256']
        for sample in [item['before'], *item['during'], item['after']]:
            assert not sample['competing']
            host_samples += 1
    indexed = {}
    for row in rows:
        key = (row['run_id'], row['part'])
        r, digest = raw[key]
        a, audit_digest = audit[row['run_id']]
        arm = row['arm']
        binary = IDENTITY['baseline' if arm.startswith('old') else 'candidate']
        assert row['raw_sha256'] == digest and row['audit_sha256'] == audit_digest
        assert row['binary_sha256'] == a['binary_sha256'] == binary['sha256']
        assert a['phase'] == row['phase'] and a['pair'] == int(row['pair']) and a['arm'] == arm
        assert r['workload'] == cfg['bench'] and r['params']['value_size'] == 1024
        assert r['engine_options']['target_sst_size'] == 2147483648
        if cfg['kind'] == 'ssd':
            assert r['params']['wal_format_version'] == (7 if arm.endswith('v7') else 4)
            assert r['params']['wal_io_mode'] == 'parallel' and r['params']['threads'] == cfg['writers']
            assert r['params']['batch_size'] == cfg['batch'] and r['result']['ops'] == cfg['operations']
            assert r['latency']['sample_every'] == cfg['sample_every']
            counts = [cfg['operations'] // cfg['writers'] + (t < cfg['operations'] % cfg['writers']) for t in range(cfg['writers'])]
            operations = [(n + cfg['batch'] - 1) // cfg['batch'] for n in counts] if cfg['batch'] else counts
            assert r['latency']['samples'] == sum((n + cfg['sample_every'] - 1) // cfg['sample_every'] for n in operations)
        else:
            assert not r['engine_options']['wal']
            assert (r['result'].get('ops') or r['result'].get('entries')) == cfg['parts'][row['part']]
            if cfg['bench'] == 'readrandom':
                assert r['result']['found'] == cfg['reads']
        indexed[row['phase'], int(row['pair']), arm, row['part']] = r

    def rate(r):
        return r['result'].get('ops_per_sec') or r['result']['entries_per_sec']

    def usage(r):
        return audit[r['run_id']][0]['child_rusage']

    def cpu(r):
        if cfg['kind'] == 'ssd':
            return r['result']['process_cpu_user_ms'] + r['result']['process_cpu_system_ms']
        return 1000 * (usage(r)['whole_process_user_seconds'] + usage(r)['whole_process_system_seconds'])

    comparisons = {}
    groups = [('v7', 'old_v7', 'new_v7')]
    for part in parts:
        for fmt, old_arm, new_arm in groups:
            pairs = []
            for pair in range(1, cfg['pairs'] + 1):
                old, new = [indexed['scored', pair, arm, part] for arm in (old_arm, new_arm)]
                delta = dict(pair=pair, throughput=rate(new) / rate(old), cpu=cpu(new) / cpu(old),
                             rss=usage(new)['max_rss_kib'] / usage(old)['max_rss_kib'])
                if cfg['kind'] == 'ssd':
                    delta.update(p99=new['latency']['p99_ms'] / old['latency']['p99_ms'],
                                 sync_count=new['result']['wal_pipeline']['sync_count'] / old['result']['wal_pipeline']['sync_count'],
                                 data_bytes=new['result']['wal_pipeline']['commit_bytes'] / old['result']['wal_pipeline']['commit_bytes'])
                pairs.append(delta)
            median = {k: statistics.median(p[k] for p in pairs) for k in pairs[0] if k != 'pair'}
            per_arm = {}
            for arm in (old_arm, new_arm):
                records = [indexed['scored', pair, arm, part] for pair in range(1, cfg['pairs'] + 1)]
                peak = max(records, key=rate)
                per_arm[arm] = dict(median_rate=statistics.median(rate(r) for r in records),
                                    median_cpu_ms=statistics.median(cpu(r) for r in records),
                                    median_peak_rss_kib=statistics.median(usage(r)['max_rss_kib'] for r in records),
                                    peak_rate=rate(peak), peak_run_id=peak['run_id'],
                                    peak_raw_sha256=raw[peak['run_id'], part][1],
                                    binary_sha256=IDENTITY['baseline' if arm.startswith('old') else 'candidate']['sha256'])
                if cfg['kind'] == 'ssd':
                    per_arm[arm]['median_p99_ms'] = statistics.median(r['latency']['p99_ms'] for r in records)
                    metrics = ('commit_groups','commit_buffers','commit_bytes','sync_count','fdatasync_ns','frontier_writes','frontier_bytes','frontier_write_ns','preallocation_ns','wal_sync_wait_ns','memtable_insert_ns')
                    per_arm[arm]['pipeline_medians'] = {metric:statistics.median(r['result']['wal_pipeline'][metric] for r in records) for metric in metrics}
            floor = 0.90 if cfg['kind'] == 'cpu' else 0.95
            guards = dict(paired_floor=median['throughput'] >= floor,
                          arm_median_floor=per_arm[new_arm]['median_rate'] / per_arm[old_arm]['median_rate'] >= floor)
            if cfg['kind'] == 'ssd':
                guards.update(p99_ceiling=median['p99'] <= 1.10,
                              identical_data_bytes=all(p['data_bytes'] == 1 for p in pairs))
            comparisons[part + '/' + fmt] = dict(pairs=pairs, median=median, per_arm=per_arm, guards=guards,
                                                passed=all(guards.values()), memory_review=median['rss'] > 1.20)
    return dict(invocations=invocations, records=len(raw), warmup_records=len(cfg['arms']) * len(parts),
                host_samples=host_samples, run_ids=list(audit), comparisons=comparisons,
                passed=all(c['passed'] for c in comparisons.values()),
                artifact_sha256={suffix: sha(ART / (stem + suffix)) for suffix in ('.jsonl', '-runs.tsv', '-host.jsonl')})


def main():
    cells = sys.argv[1:] or [c for c in MATRIX['cells'] if (ART/(STEM+'-'+c+'.jsonl')).exists()]
    output = {cell:analyze(cell) for cell in cells}
    for cell, data in output.items():
        raw = {r['run_id']:r for line in (ART/(STEM+'-'+cell+'.jsonl')).read_text().splitlines() for r in [json.loads(line)]}
        manifest=list(csv.DictReader((ART/(STEM+'-'+cell+'-runs.tsv')).open(),delimiter='\t'))
        indexed={(int(r['pair']),r['arm']):raw[r['run_id']] for r in manifest if r['phase']=='scored'}
        masks=[]
        for pair in range(1,MATRIX['cells'][cell]['pairs']+1):
            flags={}
            for arm in ['old_v7','new_v7']:
                r=indexed[pair,arm];c=r['result']['wal_pipeline']
                frontier_ms=c['frontier_write_ns']/max(c['frontier_writes'],1)/1e6
                sync_ms=c['fdatasync_ns']/max(c['sync_count'],1)/1e6
                flags[arm]=dict(run_id=r['run_id'],frontier_mean_ms=frontier_ms,fdatasync_mean_ms=sync_ms,
                                obvious_stall=frontier_ms>=1.0 or sync_ms>=1.5,
                                puts_s=r['result']['ops_per_sec'],p99_ms=r['latency']['p99_ms'])
            masks.append(dict(pair=pair,**flags,excluded=any(f['obvious_stall']for f in flags.values())))
        kept=[m['pair'] for m in masks if not m['excluded']]
        raw_comparison=next(iter(data['comparisons'].values()))
        pairs=[p for p in raw_comparison['pairs'] if p['pair'] in kept]
        median={k:statistics.median(p[k]for p in pairs)for k in pairs[0]if k!='pair'}if pairs else None
        rates={arm:[indexed[p,arm]['result']['ops_per_sec'] for p in kept]for arm in ['old_v7','new_v7']}
        arm_ratio=statistics.median(rates['new_v7'])/statistics.median(rates['old_v7']) if pairs else None
        screen=cell in MATRIX['screen']
        enough=len(pairs)>=(3 if screen else 2)
        guards=dict(enough_matched_pairs=enough,
                    paired_throughput=median is not None and (median['throughput']>1 if screen else median['throughput']>=.95),
                    arm_median_throughput=arm_ratio is not None and (arm_ratio>1 if screen else arm_ratio>=.95),
                    paired_p99=median is not None and (median['p99']<1 if screen else median['p99']<=1.10),
                    identical_data_bytes=all(p['data_bytes']==1 for p in raw_comparison['pairs']))
        data.update(raw_v7_passed=raw_comparison['passed'],v7_passed=all(guards.values()),
                    stall_filter=dict(rule='Either arm mean FRONTIER write>=1ms or mean fdatasync>=1.5ms',
                                      pair_masks=masks,matched_pairs=kept,
                                      excluded_by_arm={arm:sum(m[arm]['obvious_stall']for m in masks)for arm in rates},
                                      median=median,arm_median_rate_ratio=arm_ratio,guards=guards,
                                      limits='Aggregate mask excludes obvious long-I/O runs but misses short outliers and does not establish stall-free conditions or prove source/environment attribution. Every raw record is preserved.'))
    ids=[run for data in output.values()for run in data['run_ids']]
    assert len(ids)==len(set(ids))
    output['status']=dict(all_planned_cells_complete=cells==list(MATRIX['cells']),
                          captured_cells=cells,cancelled_cells=[c for c in MATRIX['cells']if c not in cells],
                          captured_cells_pass=all(data['v7_passed']for data in output.values()),
                          records=len(ids),scored_records=sum(d['records']-d['warmup_records']for d in output.values()),
                          warmup_records=sum(d['warmup_records']for d in output.values()),
                          cpu_is_diagnostic=True,source_candidates=1)
    output['analysis_sha256']=sha(Path(__file__))
    output['driver_sha256']=sha(ART/(STEM+'-run.py'))
    output['matrix_sha256']=sha(ART/(STEM+'-matrix.json'))
    print(json.dumps(output,indent=2))


if __name__=='__main__':
    main()
