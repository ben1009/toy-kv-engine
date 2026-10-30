#!/usr/bin/env python3
"""Reconstruct and verify the minimum-write batch-membership SSD experiment."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-batch-membership-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-batch-membership'
CHANGED = ['kv-engine/src/wal/parallel/runtime.rs', 'kv-engine/src/wal/parallel/worker.rs']


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def main():
    plan = read('-plan.json')
    matrix = read('-matrix.json')
    manifest = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({r['path'] for r in manifest}) == 128
    scratch = WORK / 'verification-source'
    assert not scratch.exists()
    scratch.mkdir()
    try:
        for row in manifest:
            p = scratch / row['path']
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT))
        subprocess.run(['patch', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))], cwd=scratch,
                       check=True, capture_output=True)
        for row in manifest:
            assert sha(scratch / row['path']) == row['sha256'], row['path']
    finally:
        shutil.rmtree(scratch)
    identity = read('-binaries.json')
    assert identity['baseline'] == plan['baseline']
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    env = read('-environment.json')
    for key, suffix in [('source_manifest_sha256','-source-sha256.tsv'),('patch_sha256','-candidate.patch'),
                        ('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),('driver_sha256','-run.py')]:
        assert env[key] == sha(ART / (STEM + suffix))
    assert not env['allocator_environment_overrides']
    regenerated = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))])
    assert regenerated == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    assert len(matrix['cells']) == 5 and summary['status']['all_planned_cells_complete']
    assert all(c['pairs'] == 3 and len(c['arms']) == 4 for c in matrix['cells'].values())
    assert summary['status']['records'] == 80 and summary['status']['scored_records'] == 60
    ids = {run for cell in matrix['cells'] for run in summary[cell]['run_ids']}
    assert len(ids) == 80
    peaks = read('-peaks.json')
    assert peaks['cells'] == {cell + '/' + part: c['per_arm'] for cell in matrix['cells'] for part, c in summary[cell]['comparisons'].items()}
    diagnostic_plan = read('-diagnostic-plan.json')
    profiles = read('-profiles.json')
    diagnosis = read('-diagnosis.json')
    lines = (ART / (STEM + '-profiles.jsonl')).read_bytes().splitlines()
    assert len(profiles) == len(lines) == len(diagnostic_plan['captures']) == len(diagnosis) == 3
    for meta, line, cfg, d in zip(profiles, lines, diagnostic_plan['captures'], diagnosis):
        r = json.loads(line)
        assert r['run_id'] == meta['run_id'] == d['run_id'] and r['run_id'] not in ids
        ids.add(r['run_id'])
        assert meta['excluded_from_scoring'] and meta['binary'] == identity['baseline']
        assert meta['samples'] > 0 and meta['lost_samples'] == 0
        assert hashlib.sha256(line).hexdigest() == meta['raw_sha256']
        assert r['params']['threads'] == cfg['writers'] and r['params']['wal_format_version'] == 7
        assert r['params']['batch_size'] == 64 and r['result']['ops'] == 750000
        assert r['engine_options']['target_sst_size'] == 2147483648
        assert meta['audit']['returncode'] == 0 and not meta['audit']['sampling_errors']
        assert not any(s['competing'] for s in [meta['audit']['before'], *meta['audit']['during'], meta['audit']['after']])
        for path, digest in meta['files'].items():
            assert sha(ROOT / path) == digest
        c = r['result']['wal_pipeline']
        assert d['groups'] == c['commit_groups'] and d['solo_groups'] == c['commit_solo_groups']
        assert d['logical_batches'] == c['commit_buffers']
        assert d['multi_ticket_groups'] == c['commit_groups'] - c['commit_solo_groups']
        assert d['fraction_batches_in_multi_ticket_groups'] == (c['commit_buffers'] - c['commit_solo_groups'])/c['commit_buffers']
        assert d['batches_per_sync'] == c['commit_buffers']/c['sync_count']
        for metric in ['frontier_write_ns','fdatasync_ns','preallocation_ns']:
            assert d[metric] == c[metric]
    assert len(ids) == 83
    old_ids = set()
    prior_stems = ['rfc-025-v7-ssd-sync-preparation-20261027','rfc-025-v7-ssd-two-chunk-lookahead-20261027',
                   'rfc-025-v7-ssd-batch-window-20261027','rfc-025-v7-ssd-batch-window-followup-20261027']
    for prior in prior_stems:
        old = json.loads((ART / (prior + '-summary.json')).read_text())
        for c in old.values():
            if isinstance(c, dict) and 'run_ids' in c:
                assert old_ids.isdisjoint(c['run_ids'])
                old_ids.update(c['run_ids'])
    old_solo = json.loads((ART / 'rfc-025-v7-ssd-batch-solo-control-20261027-summary.json').read_text())['cell']
    assert old_ids.isdisjoint(old_solo['run_ids'])
    old_ids.update(old_solo['run_ids'])
    for line in (ART / 'rfc-025-v7-ssd-sync-preparation-20261027-profiles.jsonl').read_bytes().splitlines():
        r = json.loads(line)
        assert r['run_id'] not in old_ids
        old_ids.add(r['run_id'])
    assert len(old_ids) == 237 and ids.isdisjoint(old_ids)
    checks = read('-checks.json')
    assert checks['outside_sandbox'] and checks['tests'] == 1973 and checks['focused_tests'] == 8
    assert any(c['phase'] == 'direct spelling' for c in checks['checks'])
    assert any(c['phase'] == 'final spelling' for c in checks['checks'])
    for c in checks['checks']:
        assert c['exit_code'] == 0 and sha(ROOT / c['log']) == c['log_sha256']
    check = next(c for c in checks['checks'] if c['phase'] == 'check')
    assert '1973 tests run: 1973 passed' in (ROOT / check['log']).read_text()
    assert checks['embedded_spelling_issue']
    decision = read('-decision.json')
    expected_pass = summary['status']['captured_v7_cells_pass'] and summary['status']['confirmed_batch_benefit']
    assert decision['assessment_passed'] == expected_pass and decision['cpu_is_diagnostic']
    assert decision['all_requested_cells_run'] and decision['cancelled_cells'] == []
    if decision['source_retained']:
        assert expected_pass
        for row in manifest:
            assert sha(ROOT / row['path']) == row['sha256']
        for path in CHANGED:
            committed = subprocess.check_output(['git','show',decision['source_commit']+':'+path],cwd=ROOT)
            assert hashlib.sha256(committed).hexdigest() == sha(ROOT / path)
        assert sha(ROOT / 'target/release/write-perf') == identity['candidate']['sha256']
    else:
        restoration = read('-restoration.json')
        assert restoration['scope'].startswith('Historical restoration')
        for path in CHANGED:
            original = subprocess.check_output(['git','show',plan['source_base']+':'+path],cwd=ROOT)
            assert hashlib.sha256(original).hexdigest() == restoration['source_hashes'][path]
        assert restoration['canonical_sha256'] == identity['baseline']['sha256']
        assert decision['superseded_by'] == 'rfc-025-v7-batch-wait-budget-20261027'
        later = json.loads((ART / (decision['superseded_by'] + '-decision.json')).read_text())
        assert later['source_base'] == plan['source_base']
    for cell in matrix['cells']:
        assert not (WORK / (cell + '-scratch')).exists()
        for c in summary[cell]['comparisons'].values():
            assert 'cpu_ceiling' not in c['guards'] and all(p['data_bytes'] == 1 for p in c['pairs'])
    print(json.dumps(dict(source_base=plan['source_base'],reconstructed_inputs=128,verified_binaries=2,
                          source_reconstructs=True,summary_regenerates=True,records=80,scored_records=60,warmup_records=20,
                          excluded_profiles=3,unique_invocations=83,global_unique_invocations=len(ids|old_ids),
                          recognized_process_samples=sum(summary[c]['host_samples'] for c in matrix['cells']),
                          source_retained=decision['source_retained'],assessment_passed=expected_pass,cpu_is_diagnostic=True,
                          tests=1973,focused_tests=8,checks_sha256=sha(ART/(STEM+'-checks.json')),
                          decision_sha256=sha(ART/(STEM+'-decision.json')),verifier_sha256=sha(Path(__file__))),indent=2))


if __name__ == '__main__':
    main()
