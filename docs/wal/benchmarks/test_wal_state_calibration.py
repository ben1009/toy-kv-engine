"""Independent calibration, causal labels, and uncensored metric joins."""

import copy
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import wal_state_calibration as cal


MS = 1_000_000


def sample(at, latency=30., eligible=True):
    return {"time_ns": at, "flush_start_ns": at - MS, "write_start_ns": at - 500_000,
            "eligible": eligible, "first_namespace_write_control": 0,
            "first_namespace_write_role": "DATA", "first_namespace_write_bytes": 4096,
            "intervening_fua": 0, "intervening_other_writes": 0,
            "first_namespace_write_us": latency, "post_flush_write_us": latency,
            "flush_to_write_gap_us": latency,
            "shape": {"size": "le_4096", "queue": "0", "peak": "0", "flush_queue": "0",
                      "intervening_reads": False, "gap": "le_100us"}}


def training_rows():
    output = []
    for fmt in ("v7", "v8"):
        for latency in (30., 640.):
            for i in range(12):
                output.append({"vector": [math.log1p(latency + i % 3)] * 3,
                               "format": fmt, "name": fmt,
                               "start_ns": (i * 20 + (1000 if latency > 100 else 0)) * MS,
                               "end_ns": (i * 20 + 19 + (1000 if latency > 100 else 0)) * MS})
    return output


def model():
    rows = training_rows()
    spec = cal.validate_class(cal.fit_class(rows), rows)
    assert spec['accepted'], spec
    return spec


def timeline():
    points = []
    for i in range(11):
        at = (i + 1) * 100 * MS
        points.append({'sample_begin_ns': at - 1000, 'sample_end_ns': at,
                       'user_cpu_ms': i * 10, 'system_cpu_ms': i,
                       **{key: i for key in cal.COUNTERS}})
    requests = [{'start_ns': i * MS - MS // 2, 'end_ns': i * MS, 'puts': 1} for i in range(101, 1100)]
    return {'clock': 'CLOCK_MONOTONIC', 'schema': 'kv-engine.wal-benchmark-timeline.v1',
            'counter_points': points, 'requests': requests}


def run():
    tl = timeline()
    return {'name': 'point-0-v7', 'format': 'v7', 'workload': 'point', 'position': 0,
            'timeline': tl, 'samples': [sample(i * MS) for i in range(101, 1100)]}


class RoutingTests(unittest.TestCase):
    def test_gap_latency_and_format_do_not_change_routing(self):
        a = sample(10 * MS)
        b = sample(10 * MS, 640.)
        b['shape']['gap'] = 'gt_500us'
        self.assertEqual(cal.route(a, 'point'), cal.route(b, 'point'))
        self.assertNotEqual(cal.vector([a]), cal.vector([b]))

    def test_first_role_fua_size_depth_and_workload_remain_distinct(self):
        a = sample(10 * MS)
        for field, value in [('first_namespace_write_role', 'NON_WAL'),
                             ('first_namespace_write_bytes', 65536), ('intervening_fua', 1)]:
            b = {**a, field: value}
            self.assertNotEqual(cal.route(a, 'point'), cal.route(b, 'point'))
        b = copy.deepcopy(a)
        b['shape']['peak'] = '8+'
        self.assertNotEqual(cal.route(a, 'point'), cal.route(b, 'point'))
        self.assertNotEqual(cal.route(a, 'point'), cal.route(a, 'batch64'))
        b.update(first_namespace_write_control=0x4000, first_namespace_write_role='NON_WAL')
        self.assertIn('non_WAL_FUA', cal.route(b, 'point'))

    def test_unproven_role_or_control_is_unroutable(self):
        for role in ('FRONTIER', 'MAPPING_CONFLICT', 'UNOBSERVED_ALLOCATION'):
            self.assertIsNone(cal.route({**sample(10 * MS), 'first_namespace_write_role': role}, 'point'))
        self.assertIsNone(cal.route({**sample(10 * MS), 'first_namespace_write_control': 0x8000}, 'point'))
        self.assertIsNone(cal.route(sample(10 * MS, eligible=False), 'point'))

    def test_first_namespace_completion_cannot_leak_from_future(self):
        s = sample(10 * MS, 5000.)
        self.assertEqual(cal.observed_ns(s), s['write_start_ns'] + 5 * MS)

    def test_window_spanning_gap_or_run_boundary_is_not_training_data(self):
        a = run()
        a['samples'] = [sample(i * 100 * MS) for i in range(1, 33)]
        self.assertEqual(dict(cal.windows(a)), {})
        a['samples'] = [sample(i * MS) for i in range(1, 16)]
        self.assertEqual(dict(cal.grouped_windows([a, a])), {})

    def test_nonfinite_or_negative_latency_is_rejected(self):
        for invalid in (-1., math.inf, math.nan):
            with self.assertRaises(ValueError):
                cal.vector([sample(10 * MS, invalid)])


class FitTests(unittest.TestCase):
    def test_repeatable_shared_modes_pass_and_classify_holdout(self):
        spec = model()
        self.assertEqual(cal.assign(spec, cal.vector([sample(10 * MS)])), 'low')
        self.assertEqual(cal.assign(spec, cal.vector([sample(10 * MS, 640.)])), 'high')
        self.assertGreaterEqual(spec['validation_persistence_ns']['low']['v7'], 100 * MS)

    def test_sparse_or_unimodal_class_is_not_forced_into_two_modes(self):
        self.assertFalse(cal.fit_class(training_rows()[:3])['accepted'])
        rows = training_rows()
        for row in rows:
            row['vector'] = [math.log1p(30.)] * 3
        self.assertNotIn('low', cal.fit_class(rows))

    def test_modes_that_only_identify_format_are_rejected(self):
        rows = [r for r in training_rows() if (r['format'] == 'v7') == (r['vector'][0] < 5)]
        self.assertEqual(cal.fit_class(rows)['reason'], 'mode_is_format_specific_or_sparse')

    def test_validation_must_repeat_both_modes_in_both_formats(self):
        spec = cal.fit_class(training_rows())
        holdout = [r for r in training_rows() if r['vector'][0] < 5]
        result = cal.validate_class(spec, holdout)
        self.assertFalse(result['accepted'])
        self.assertEqual(result['reason'], 'validation_missing_shared_modes')
        self.assertEqual(spec['reason'], 'pending_validation')

    def test_out_of_distribution_does_not_become_a_convenient_mode(self):
        spec = model()
        self.assertEqual(cal.assign(spec, [math.log1p(20000.)] * 3), 'unknown')
        self.assertEqual(cal.assign(spec, [math.log1p(30.), math.log1p(20000.), math.log1p(30.)]), 'unknown')

    def test_short_oscillating_validation_modes_are_not_persistent(self):
        rows = training_rows()
        for i, row in enumerate(rows):
            row['name'] = str(i)
        result = cal.validate_class(cal.fit_class(rows), rows)
        self.assertEqual(result['reason'], 'validation_modes_not_persistent')

    def test_fit_is_deterministic(self):
        self.assertEqual(cal.canonical(model()), cal.canonical(model()))


class EvaluationTests(unittest.TestCase):
    def labels(self, a=None):
        a = a or run()
        key = cal.route(a['samples'][0], a['workload'])
        return cal.classify_run(a, {'classes': {key: model()}})

    def test_confirmation_is_not_backdated_and_gap_resets_it(self):
        a = run()
        labels = self.labels(a)
        self.assertTrue(all(r['state'] == 'unknown' for r in labels[:40]))
        self.assertEqual(labels[-1]['state'], 'low')
        a['samples'] = a['samples'][:80] + [sample(900 * MS + i * MS) for i in range(40)]
        labels = self.labels(a)
        self.assertTrue(all(r['state'] == 'unknown' for r in labels[80:]))

    def test_unsupported_class_never_inherits_another_class_threshold(self):
        a = run()
        labels = cal.classify_run(a, {'classes': {}})
        self.assertEqual({r['state'] for r in labels}, {'unknown'})
        rows = cal.interval_diagnostic(a, labels)
        self.assertTrue(all(r['state'] == 'unknown' for r in rows))

    def test_metrics_keep_every_request_and_compute_episode_p99_from_raw(self):
        a = run()
        rows = cal.interval_diagnostic(a, self.labels(a))
        self.assertEqual(sum(r['puts'] for r in rows), len(a['timeline']['requests']))
        self.assertGreater(sum(r['state'] == 'low' for r in rows), 5)
        episodes = cal.episodes(a, rows)
        self.assertEqual(len(episodes), 1)
        e = episodes[0]
        self.assertEqual(e['p99_ms'], .5)
        self.assertEqual(e['puts'], sum(r['puts'] for r in a['timeline']['requests'] if e['start_ns'] <= r['end_ns'] < e['end_ns']))

    def test_uncertain_observation_rejects_bucket_without_removing_latency(self):
        a = run()
        labels = self.labels(a)
        victim = next(r for r in labels if r['time_ns'] == 550 * MS)
        victim.update(state='unknown', reason='unconfirmed_or_guarded')
        rows = cal.interval_diagnostic(a, labels)
        row = next(r for r in rows if r['start_ns'] == 500 * MS)
        self.assertEqual(row['state'], 'unknown')
        self.assertEqual(row['puts'], 100)

    def test_long_request_crossing_transition_rejects_whole_bucket(self):
        a = run()
        a['timeline']['requests'][500]['start_ns'] = 150 * MS
        labels = self.labels(a)
        for row in labels:
            if row['time_ns'] < 300 * MS:
                row.update(state='unknown', reason='unconfirmed_or_guarded')
        rows = cal.interval_diagnostic(a, labels)
        selected = next(r for r in rows if r['start_ns'] == 600 * MS)
        self.assertEqual(selected['state'], 'unknown')
        self.assertEqual(selected['puts'], 100)

    def test_mixed_states_and_unbracketed_endpoints_are_unknown(self):
        a = run()
        labels = self.labels(a)
        for row in labels:
            if row['time_ns'] >= 550 * MS:
                row['state'] = 'high'
        rows = cal.interval_diagnostic(a, labels)
        self.assertEqual(rows[-1]['state'], 'unknown')
        self.assertEqual(next(r for r in rows if r['start_ns'] == 500 * MS)['rejection_reason'], 'mixed_latency_modes')

    def test_best_episode_does_not_cherry_pick_p99_from_another_episode(self):
        a = run()
        runs = [{**a, 'name': f'{w}-{p}-{fmt}', 'position': p, 'workload': w, 'format': fmt}
                for w in ('point', 'batch64') for p, fmt in enumerate(('v7', 'v8', 'v8', 'v7'))]
        results = {}
        for r in runs:
            episode = {'format': r['format'], 'state': 'low', 'dominant_class': 'x',
                       'puts_s': 200 if r['format'] == 'v8' else 100,
                       'p99_ms': 4 if r['format'] == 'v8' else 2, 'class_histogram': {'x': 100}}
            results[r['name']] = {'episodes': [episode, {**episode, 'puts_s': 50, 'p99_ms': .1}]}
        comparison = cal.compare(runs, results)[0]
        self.assertEqual(comparison['v8_over_v7_puts'], 2)
        self.assertEqual(comparison['v8_over_v7_p99'], 2)
        results[runs[1]['name']]['episodes'][0]['class_histogram'] = {'x': 80, 'y': 20}
        self.assertFalse(cal.compare(runs, results)[0]['qualified'])


class ProvenanceTests(unittest.TestCase):
    def make_cohort(self, directory, bad=False):
        study = {'binary_sha256': 'binary', 'boot_id': 'boot', 'cpu_mask': list(range(16)),
                 'capture_order': ['v7', 'v8', 'v8', 'v7'], 'source_sha256': cal.source_hashes(),
                 'counter_interval_ms': 100, 'declared_monotonic_ns': 0,
                 'workloads': [{'name': 'point', 'num': 20000, 'writers': 1, 'batch': 1},
                               {'name': 'batch64', 'num': 750000, 'writers': 16, 'batch': 64}]}
        plan = {**{k: study[k] for k in ('binary_sha256', 'boot_id', 'cpu_mask', 'workloads')},
                'protocol': cal.ext.EXTENT_PROTOCOL, 'order': study['capture_order'],
                'policy': cal.ext.EXTENT_POLICY, 'clock': 'CLOCK_MONOTONIC', 'events': [], 'uid': 1000,
                'kernel': 'test', 'perf': 'test', 'device': {},
                'runner_sha256': study['source_sha256']['capture_wal_trace.py'],
                'analyzer_sha256': study['source_sha256']['wal_trace_regimes.py'],
                'extent_helper_sha256': study['source_sha256']['wal_extent_trace.py']}
        cal.save_new(directory / 'plan.json', plan)
        cal.save_new(directory / 'formats.json', {})
        for wi, w in enumerate(study['workloads']):
            for pos, fmt in enumerate(study['capture_order']):
                start = (wi * 4 + pos + 1) * 1000 * MS
                name = f"{w['name']}-{pos}-{fmt}"
                tl = {'clock': 'CLOCK_MONOTONIC', 'schema': 'kv-engine.wal-benchmark-timeline.v1',
                      'interval_ms': 100, 'counter_points': [
                          {'sample_begin_ns': start, 'sample_end_ns': start + 1000},
                          {'sample_begin_ns': start + 100 * MS, 'sample_end_ns': start + 100 * MS + 1000}],
                      'requests': [{'start_ns': start + MS, 'end_ns': start + 50 * MS, 'puts': w['num']}]}
                cal.save_new(directory / (name + '-timeline.json'), tl)
                cal.save_new(directory / (name + '-capture.json'), {'returncode': 0})
                cal.save_new(directory / (name + '-benchmark.json'), {'params': {
                    'num': w['num'], 'threads': w['writers'], 'batch_size': w['batch'],
                    'wal_format_version': int(fmt[1:]), 'wal_io_mode': 'parallel', 'value_size': 1024}})
                cal.save_new(directory / (name + '-extent-analysis.json'), {
                    'summary': {'metadata': {'lost': 0}, 'trace_quality_ok': not bad},
                    'samples': [{**sample(start + 50 * MS), 'reasons': []}]})
                (directory / (name + '.perf')).write_bytes(b'fixture only')
        return study

    def test_bad_trace_is_quarantined_in_full_without_reclassifying_its_samples(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            study = self.make_cohort(directory, bad=True)
            runs, proof = cal.load_cohort(directory, study)
            self.assertEqual(len(proof['sha256']), 42)
            self.assertFalse(any(s['eligible'] for r in runs for s in r['samples']))
            self.assertEqual(dict(cal.grouped_windows(runs)), {})
            original = cal.read(directory / 'point-0-v7-extent-analysis.json')
            self.assertTrue(original['samples'][0]['eligible'])

    def test_predeclaration_capture_and_changed_workload_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            study = self.make_cohort(directory)
            with self.assertRaisesRegex(ValueError, 'predeclaration'):
                cal.load_cohort(directory, {**study, 'declared_monotonic_ns': 2000 * MS})
            changed = copy.deepcopy(study)
            changed['workloads'][0]['num'] = 1
            with self.assertRaisesRegex(ValueError, 'protocol'):
                cal.load_cohort(directory, changed)

    def test_replay_detects_changed_inputs_and_changed_coefficients(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            study = {'ok': True}
            cal.save_new(directory / 'study-plan.json', study)
            proof = {'path': temp, 'sha256': {'raw.perf': 'original'}, 'identity': {'binary': 'same'}}
            frozen = {'schema': cal.SCHEMA, 'policy': cal.POLICY, 'features': cal.FEATURES,
                      'source_sha256': cal.source_hashes(), 'study_sha256': cal.sha(directory / 'study-plan.json'),
                      'training': proof, 'validation': proof, 'frozen_monotonic_ns': 1000,
                      'classes': {'x': {'accepted': False}}}
            cal.save_new(directory / 'frozen-model.json', frozen)
            runs = [[{'start_ns': 100, 'end_ns': 200}], [{'start_ns': 300, 'end_ns': 400}]]
            changed = {**proof, 'sha256': {'raw.perf': 'changed'}}
            with patch.object(cal, 'check_study', return_value=study), patch.object(cal, 'load_cohort',
                    return_value=(runs[0], changed)):
                with self.assertRaisesRegex(ValueError, 'input changed'):
                    cal.verify(directory)
            with patch.object(cal, 'check_study', return_value=study), patch.object(cal, 'load_cohort',
                    side_effect=[(r, proof) for r in runs]), patch.object(cal, 'derive_classes', return_value={}):
                with self.assertRaisesRegex(ValueError, 'coefficients'):
                    cal.verify(directory)

    def test_plan_and_model_outputs_are_immutable(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'model.json'
            cal.save_new(path, {'value': 1})
            with self.assertRaises(FileExistsError):
                cal.save_new(path, {'value': 2})
            self.assertEqual(cal.read(path), {'value': 1})

    def test_source_or_policy_change_requires_new_study(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            plan = {'schema': cal.SCHEMA, 'policy': cal.POLICY, 'source_sha256': {'old': 'hash'}}
            cal.save_new(directory / 'study-plan.json', plan)
            with self.assertRaisesRegex(ValueError, 'changed'):
                cal.check_study(directory)

    def test_evaluation_before_freeze_or_different_identity_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            study = {'ok': True}
            cal.save_new(directory / 'study-plan.json', study)
            frozen = {'schema': cal.SCHEMA, 'policy': cal.POLICY, 'source_sha256': cal.source_hashes(),
                      'study_sha256': cal.sha(directory / 'study-plan.json'), 'frozen_monotonic_ns': 200,
                      'training': {'identity': {'binary': 'original'}}}
            cal.save_new(directory / 'frozen-model.json', frozen)
            for start, identity in ((100, {'binary': 'original'}), (300, {'binary': 'other'})):
                with patch.object(cal, 'check_study', return_value=study), patch.object(cal, 'load_cohort', return_value=(
                        [{'start_ns': start}], {'identity': identity})):
                    with self.assertRaisesRegex(ValueError, 'fresh'):
                        cal.evaluate(directory, directory)


if __name__ == '__main__':
    unittest.main()
