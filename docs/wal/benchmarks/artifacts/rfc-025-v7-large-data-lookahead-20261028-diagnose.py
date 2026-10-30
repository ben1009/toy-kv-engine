#!/usr/bin/env python3
"""Recompute same-inode background zeroing overlap from excluded native traces."""
import bisect
import csv
import hashlib
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-large-data-lookahead-20261028'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def trace_stats(capture):
    path = ROOT / next(p for p in capture['files'] if p.endswith('-io.csv'))
    assert sha(path) == capture['files'][str(path.relative_to(ROOT))]
    rows = list(csv.DictReader(path.open()))
    frontier = [r for r in rows if r['kind'] == '2' and int(r['ticket_end']) > 0]
    inode = max(frontier, key=lambda r: int(r['ticket_end']))['inode']
    rows = [r for r in rows if r['inode'] == inode]
    frontier = [r for r in frontier if r['inode'] == inode]
    sync = sorted((int(r['start_ns']), int(r['end_ns'])) for r in rows if r['op'] == '2')
    starts = [s for s, e in sync]
    cycles = []
    for r in frontier:
        index = bisect.bisect_left(starts, int(r['end_ns']))
        assert index < len(sync)
        cycles.append((int(r['start_ns']), sync[index][1]))
    background = [(int(r['start_ns']), int(r['end_ns']), int(r['length'])) for r in rows
                  if r['op'] == '1' and r['kind'] == '0' and r['name'].startswith('wal-v7-extent')]
    assert all(int(r['error']) == 0 for r in rows)
    overlaps = lambda a, b, spans: any(s < b and a < e for s, e in spans)
    begins = lambda a, spans: any(s <= a < e for s, e in spans)
    return dict(run_id=capture['run_id'], writers=capture['writers'], source='excluded native diagnostic',
                trace_path=str(path.relative_to(ROOT)), trace_sha256=sha(path),
                frontiers=len(frontier), syncs=len(sync), background_writes=len(background),
                background_bytes=sum(n for a, b, n in background),
                background_starts_during_sync=sum(begins(a, sync) for a, b, n in background),
                background_overlaps_sync=sum(overlaps(a, b, sync) for a, b, n in background),
                background_starts_during_marker_and_sync=sum(begins(a, cycles) for a, b, n in background),
                background_overlaps_marker_and_sync=sum(overlaps(a, b, cycles) for a, b, n in background),
                already_counted_in_lineage=False,
                foreground_zero_bytes=sum(int(r['length']) for r in rows if r['op']=='1' and r['kind']=='0' and not r['name'].startswith('wal-v7-extent')),
                total_zero_bytes=sum(int(r['length']) for r in rows if r['op']=='1' and r['kind']=='0'),
                frontier_mean_us=statistics.mean((int(r['end_ns'])-int(r['start_ns']))/1000 for r in frontier),
                sync_mean_us=statistics.mean((e-s)/1000 for s,e in sync))


def main():
    path = ART / (STEM + '-probe-captures.json')
    captures = json.loads(path.read_text())
    assert [c['arm'] for c in captures] == ['baseline', 'candidate']
    results = [dict(arm=c['arm'], **trace_stats(c)) for c in captures]
    print(json.dumps(dict(captures=results, metadata_sha256=sha(path),
                          added_invocations=2, reused_invocations=0,
                          limits='Two excluded native traces are mechanism diagnostics, not scored throughput evidence or proof that conversion costs explain every tail.',
                          analyzer_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
