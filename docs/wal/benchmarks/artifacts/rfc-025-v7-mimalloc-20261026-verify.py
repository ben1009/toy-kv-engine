#!/usr/bin/env python3
"""Verify both allocator snapshots, score sets, memory diagnostics and restoration."""
import csv
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[4]
ART=Path(__file__).resolve().parent
STEM='rfc-025-v7-mimalloc-20261026'
CELLS=('tmpfs-point8','tmpfs-batch64','tmpfs-point1','ext4-point8','ext4-batch64')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    all_ids=set()
    samples=0
    for variant in ('mimalloc','mimalloc-aligned'):
        prefix='rfc-025-v7-'+variant+'-20261026'
        env=json.loads((ART/(prefix+'-environment.json')).read_text())
        manifest=ART/(prefix+'-source-sha256.tsv')
        assert sha(manifest)==env['manifest_sha256']
        rows=list(csv.DictReader(manifest.open(),delimiter='\t'))
        assert len(rows)==128
        with tempfile.TemporaryDirectory(prefix='verify-allocator-',dir=ROOT/'target/wal-opt') as directory:
            restored=Path(directory)
            for row in rows:
                relative=Path(row['path'])
                assert not relative.is_absolute() and '..' not in relative.parts
                data=subprocess.check_output(['git','show',env['source_base']+':'+row['path']],cwd=ROOT)
                assert (ROOT/relative).read_bytes()==data,row['path']
                path=restored/relative
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(data)
            subprocess.run(['patch','--batch','--fuzz=0','-p1','-i',str(ART/(prefix+'-candidate.patch'))],cwd=restored,check=True,capture_output=True)
            for row in rows:
                assert sha(restored/row['path'])==row['sha256'],row['path']
        identities=json.loads((ART/(prefix+'-binaries.json')).read_text())
        for identity in identities.values():
            assert sha(ROOT/identity['path'])==identity['sha256']
        regenerated=subprocess.check_output(['python3',str(ART/(prefix+'-analyze.py'))],cwd=ROOT)
        assert regenerated==(ART/(prefix+'-summary.json')).read_bytes()
        summary=json.loads(regenerated)
        assert summary['initial_phase_complete'] and not summary['initial_phase_pass']
        assert summary['unique_run_count']==96
        assert not json.loads((ART/(prefix+'-assessment.json')).read_text())['adopted']
        for cell in CELLS:
            for run_id in summary[cell]['run_ids']:
                assert run_id not in all_ids
                all_ids.add(run_id)
            samples+=summary[cell]['host_samples']
            stem='rfc-025-v7-'+variant+'-initial-'+cell+'-20261026'
            rows=list(csv.DictReader((ART/(stem+'-runs.tsv')).open(),delimiter='\t'))
            audits={r['run_id']:r for r in map(json.loads,(ART/(stem+'-host.jsonl')).read_text().splitlines())}
            work=ROOT/('target/wal-opt/20261026-v7-'+variant+'-initial')
            scratch=(work if cell.startswith('ext4') else Path('/dev/shm'))/(stem+'-scratch')
            assert not scratch.exists()
            for row in rows:
                item=audits[row['run_id']]
                usage=work/(cell+'-'+row['source_record'].removesuffix('.json')+'.rusage.json')
                assert sha(usage)==item['child_rusage_sha256']
                assert json.loads(usage.read_text())==item['child_rusage']
                assert item['child_rusage']['max_rss_kib']>0
                assert item['child_rusage']['pid']!=item['benchmark_pid']
        print(variant,'exact sources, frozen binaries, all score/audit hashes and summaries verified')
    assert len(all_ids)==192 and samples==1398
    for tag in ('memory-probe','memory-probe-engine'):
        stem=STEM+'-'+tag
        rows=list(csv.DictReader((ART/(stem+'-runs.tsv')).open(),delimiter='\t'))
        records={json.loads(line)['run_id']:(json.loads(line),hashlib.sha256(line).hexdigest()) for line in (ART/(stem+'.jsonl')).read_bytes().splitlines()}
        audits={json.loads(line)['run_id']:(json.loads(line),hashlib.sha256(line).hexdigest()) for line in (ART/(stem+'-host.jsonl')).read_bytes().splitlines()}
        assert len(rows)==len(records)==len(audits)==6
        for row in rows:
            run_id=row['run_id']
            assert run_id not in all_ids
            all_ids.add(run_id)
            record,digest=records[run_id]
            audit,audit_digest=audits[run_id]
            assert digest==row['raw_sha256'] and audit_digest==row['audit_sha256']
            assert audit['excluded_from_scoring'] and audit['clean_sampled']
            assert not audit['sampling_errors'] and audit['returncode']==0
            assert int(row['max_rss_kib'])==audit['child_rusage']['max_rss_kib']
            for sample in [audit['before'],*audit['during'],audit['after']]:
                assert not sample['competing']
            stats=ART/(stem+'-'+row['cell']+'-'+row['policy']+'-stats.txt')
            assert sha(stats)==row['stats_sha256']
            if tag=='memory-probe-engine':
                assert audit['MIMALLOC_SHOW_STATS']=='engine-only:1'
                assert 'heap stats:' not in stats.read_text()
                if row['policy']=='system':
                    assert not stats.read_bytes()
                else:
                    assert stats.read_text().count('subproc 0')==1
        scratch=Path('/dev/shm')/('toy-kv-mimalloc-'+tag+'-20261026')
        assert not scratch.exists()
    assert len(all_ids)==204
    peaks=json.loads((ART/(STEM+'-peaks.json')).read_text())
    for cells in peaks.values():
        for arms in cells.values():
            for winner in arms.values():
                matches=[line for line in (ART/winner['raw']).read_bytes().splitlines() if json.loads(line)['run_id']==winner['run_id']]
                assert len(matches)==1 and hashlib.sha256(matches[0]).hexdigest()==winner['raw_record_sha256']
                assert json.loads(matches[0])['result']['ops_per_sec']==winner['puts_per_sec']
                row=next(r for r in csv.DictReader((ART/winner['manifest']).open(),delimiter='\t') if r['run_id']==winner['run_id'])
                assert row['phase']=='scored' and row['binary_sha256']==winner['binary_sha256']
    for check in json.loads((ART/(STEM+'-checks.json')).read_text()).values():
        log=ROOT/check['log']
        assert sha(log)==check['sha256'] and check['retries']==0
        assert check['summary'] in log.read_text() and check['nextest_run_id'] in log.read_text()
    assert sha(ROOT/'target/release/write-perf')=='614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9'
    print('Verified 204 unique IDs, 1398 clear benchmark-matrix host samples, native-stat isolation, checks, peaks and restored canonical baseline.')


if __name__=='__main__':
    main()
