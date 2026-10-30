#!/usr/bin/env python3
"""Verify fresh process-audited captures; exclude earlier interference matrices."""
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-buffered-isolated-20261026'
SSD = ('ext4-point8', 'ext4-batch64')
RAM = ('tmpfs-batch64', 'tmpfs-point8', 'tmpfs-point1')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def template(variant, tag):
    path = ART / ('rfc-025-v7-' + variant + '-20261026-analyze.py')
    source = path.read_text()
    old = "stem='rfc-025-v7-" + variant + "-'+cell+'-20261026'"
    new = "stem='rfc-025-v7-" + variant + '-' + tag + "-'+cell+'-20261026'"
    assert source.count(old) == 1
    namespace = {'__name__': 'isolated_template', '__file__': str(path)}
    exec(compile(source.replace(old, new), str(path), 'exec'), namespace)
    return namespace, path


def audit(variant, tag, cell):
    stem = 'rfc-025-v7-' + variant + '-' + tag + '-' + cell + '-20261026'
    rows = list(csv.DictReader((ART / (stem + '-runs.tsv')).open(), delimiter='\t'))
    audits = {}
    for line in (ART / (stem + '-host.jsonl')).read_bytes().splitlines():
        item = json.loads(line)
        assert item['run_id'] not in audits
        audits[item['run_id']] = (item, hashlib.sha256(line).hexdigest())
    count = 5 if cell.startswith('ext4') else 3
    expected = [('warmup', 0, arm) for arm in ('old_v4', 'old_v7', 'new_v4', 'new_v7')]
    for pair in range(1, count + 1):
        arms = ('old_v4', 'new_v4', 'old_v7', 'new_v7') if pair % 2 else ('new_v7', 'old_v7', 'new_v4', 'old_v4')
        expected += [('scored', pair, arm) for arm in arms]
    assert [(r['phase'], int(r['pair']), r['arm']) for r in rows] == expected
    assert len(rows) == len(audits) == 4 * (count + 1)
    assert set(audits) == {r['run_id'] for r in rows}
    samples = 0
    for row in rows:
        item, digest = audits[row['run_id']]
        assert digest == row['audit_sha256']
        assert row['isolation_status'] == 'clean_sampled' and item['clean_sampled']
        assert item['run_id'] == row['run_id'] and item['arm'] == row['arm']
        assert item['phase'] == row['phase'] and item['pair'] == int(row['pair'])
        assert item['binary_sha256'] == row['binary_sha256'] and item['returncode'] == 0
        assert item['during'] and not item['sampling_errors']
        for sample in [item['before'], *item['during'], item['after']]:
            assert not sample['competing']
            samples += 1
    return dict(run_count=len(rows), host_samples=samples,
                host_audit_sha256=sha(ART / (stem + '-host.jsonl')))


def ssd_guards(cells):
    guards = {}
    for cell in SSD:
        x = cells[cell]['median_changes']
        arms = cells[cell]['per_arm']
        guards[cell + '_paired_raw_floor'] = x['v7_throughput_change'] >= 0.95
        guards[cell + '_paired_normalized_floor'] = x['normalized_v7_v4_change'] >= 0.95
        guards[cell + '_arm_median_floor'] = arms['new_v7']['median_puts_per_sec'] / arms['old_v7']['median_puts_per_sec'] >= 0.95
        guards[cell + '_p99_ceiling'] = x['v7_p99_change'] <= 1.10
    guards['ssd_material_gain'] = any(cells[cell]['median_changes']['v7_throughput_change'] >= 1.02
                                      and cells[cell]['median_changes']['normalized_v7_v4_change'] >= 1.0
                                      for cell in SSD)
    guards['ssd_phase_pass'] = all(guards.values())
    return guards


def analyze(variant, tag, cells):
    module, path = template(variant, tag)
    output = {'variant': variant, 'capture_tag': tag, 'cells': {}, 'run_ids': [],
              'template_sha256': sha(path), 'isolation': {},
              'excluded': 'Earlier potentially contaminated matrices are not pooled or used in these guards.'}
    for cell in cells:
        result = module['compare'](cell)
        output['run_ids'] += result['run_ids']
        output['isolation'][cell] = audit(variant, tag, cell)
        pairs, _, _ = module['load'](cell)
        result['per_run_io_means_us'] = {
            arm: [dict(run_id=p[arm]['run_id'], syncs=(c := p[arm]['result']['wal_pipeline'])['sync_count'],
                       frontier_write=c['frontier_write_ns'] / c['sync_count'] / 1000,
                       fdatasync=c['fdatasync_ns'] / c['sync_count'] / 1000,
                       combined=(c['frontier_write_ns'] + c['fdatasync_ns']) / c['sync_count'] / 1000)
                  for p in pairs] for arm in ('old_v7', 'new_v7')
        }
        output['cells'][cell] = result
    assert len(output['run_ids']) == len(set(output['run_ids']))
    if set(SSD) <= set(cells):
        output['ssd_guards'] = ssd_guards(output['cells'])
        output['ram_eligible'] = output['ssd_guards']['ssd_phase_pass']
    if set(RAM) <= set(cells):
        guards = {}
        for cell in RAM:
            x = output['cells'][cell]['median_changes']
            arms = output['cells'][cell]['per_arm']
            guards[cell + '_raw_floor'] = x['v7_throughput_change'] >= 0.95
            guards[cell + '_normalized_floor'] = x['normalized_v7_v4_change'] >= 0.95
            guards[cell + '_arm_floor'] = arms['new_v7']['median_puts_per_sec'] / arms['old_v7']['median_puts_per_sec'] >= 0.95
            guards[cell + '_cpu_ceiling'] = x['v7_cpu_change'] <= 1.05
        guards['solo_p99_ceiling'] = output['cells']['tmpfs-point1']['median_changes']['v7_p99_change'] <= 1.10
        guards['ram_phase_pass'] = all(guards.values())
        output['ram_guards'] = guards
    output['adopted'] = False
    output['plan_sha256'] = sha(ART / (STEM + '-plan.json'))
    output['driver_sha256'] = sha(ART / (STEM + '-run.py'))
    output['analysis_sha256'] = sha(Path(__file__))
    return output


if __name__ == '__main__':
    variant = sys.argv[1]
    tag = 'isolated-confirmation' if '--confirmation' in sys.argv else 'isolated'
    cells = [arg for arg in sys.argv[2:] if not arg.startswith('--')] or list(SSD)
    print(json.dumps(analyze(variant, tag, cells), indent=2))
