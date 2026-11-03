"""Scientific validity and boundary tests; no tracing privilege or SSD I/O."""

import struct
import tempfile
from pathlib import Path
import unittest

import wal_trace_regimes as trace


def command(start, end, opcode=1, size=4096, pid=100, cid=1):
    return {"key": [0, 1, cid], "start_ns": start, "end_ns": end, "opcode": opcode,
            "nsid": 1, "bytes": size, "pid": pid, "tid": pid, "status": 0,
            "retries": 0, "control": 0, "overlap": False}


def sample(index, latency=30, eligible=True):
    end = (index + 1) * 1_000_000
    return {"flush_start_ns": end - 900000, "flush_end_ns": end - 800000,
            "write_start_ns": end - int(latency * 1000), "time_ns": end,
            "post_flush_write_us": latency, "eligible": eligible}


def point(index):
    return {"sample_begin_ns": index * 1_000_000_000,
            "sample_end_ns": index * 1_000_000_000 + 100,
            "user_cpu_ms": index * 100, "system_cpu_ms": index * 10,
            "sync_count": index * 1000, "reported_commit_groups": index * 1000,
            "worker_notification_requests": index * 1000, "worker_eventfd_writes": index * 500}



class PerfDecoderTests(unittest.TestCase):
    def fixture(self, clock=1, lost=0, truncate=False):
        # Minimal native perf file with one real tracepoint-shaped sample.
        fmt = """name: nvme_setup_cmd
ID: 42
format:
field:unsigned short common_type; offset:0; size:2; signed:0;
field:int common_pid; offset:4; size:4; signed:1;
field:char disk[32]; offset:8; size:32; signed:0;
field:int ctrl_id; offset:40; size:4; signed:1;
field:int qid; offset:44; size:4; signed:1;
field:u8 opcode; offset:48; size:1; signed:0;
field:u16 cid; offset:52; size:2; signed:0;
field:u32 nsid; offset:56; size:4; signed:0;
field:u8 cdw10[24]; offset:61; size:24; signed:0;
"""
        payload = bytearray(85)
        struct.pack_into('<H', payload, 0, 42)
        payload[8:16] = b'nvme0n1\0'
        struct.pack_into('<iiB', payload, 40, 0, 2, 1)
        struct.pack_into('<H', payload, 52, 7)
        struct.pack_into('<I', payload, 56, 1)
        struct.pack_into('<QHH', payload, 61, 12345, 7, 0x4000)
        mask = (1 << 16) | 2 | 4 | 128 | 256 | 1024
        body = struct.pack('<QIIQIIQI', 77, 100, 101, 123456789, 3, 0, 1, len(payload)) + payload
        if truncate:
            body = body[:-1]
        record = struct.pack('<IHH', 9, 0, 8 + len(body)) + body
        if lost:
            record += struct.pack('<IHHQQ', 2, 0, 24, 77, lost)
        data_offset = 104 + 144 + 8
        header = struct.pack('<13Q', int.from_bytes(b'PERFILE2', 'little'), 104, 144, 104, 144,
                             data_offset, len(record), 0, 0, 0, 0, 0, 0)
        attr = bytearray(144)
        struct.pack_into('<IIQQQ', attr, 0, 2, 128, 42, 1, mask)
        struct.pack_into('<Q', attr, 40, 1 << 25)
        struct.pack_into('<i', attr, 92, clock)
        struct.pack_into('<QQ', attr, 128, 248, 8)
        return header + attr + struct.pack('<Q', 77) + record, {'nvme/nvme_setup_cmd': fmt}

    def decode(self, **kwargs):
        data, formats = self.fixture(**kwargs)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'perf.data'
            path.write_bytes(data)
            return trace.decode_perf(path, formats)

    def test_exact_sample_identity_clock_and_wire_fields(self):
        events, meta = self.decode()
        self.assertEqual(meta['clock'], 'CLOCK_MONOTONIC')
        event = events[0]
        self.assertEqual((event['pid'], event['tid'], event['cpu'], event['time_ns']), (100, 101, 3, 123456789))
        self.assertEqual((event['qid'], event['cid'], event['lba'], event['blocks'], event['control']),
                         (2, 7, 12345, 8, 0x4000))

    def test_nonmonotonic_clock_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'CLOCK_MONOTONIC'):
            self.decode(clock=7)

    def test_lost_record_is_preserved(self):
        _, metadata = self.decode(lost=17)
        self.assertEqual(metadata['lost'], 17)

    def test_truncated_raw_payload_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Truncated'):
            self.decode(truncate=True)


class CommandTests(unittest.TestCase):
    def test_queue_identity_and_reuse_pair_correctly(self):
        events = []
        for begin, finish, qid in [(1, 3, 1), (2, 4, 2), (5, 7, 1)]:
            events.append({"event": "nvme/nvme_setup_cmd", "time_ns": begin, "disk": "nvme0n1",
                           "ctrl_id": 0, "qid": qid, "cid": 1, "opcode": 1, "nsid": 1,
                           "pid": 100, "tid": 101, "blocks": 8, "control": 0})
            events.append({"event": "nvme/nvme_complete_rq", "time_ns": finish, "disk": "nvme0n1",
                           "ctrl_id": 0, "qid": qid, "cid": 1, "status": 0, "retries": 0})
        commands, anomalies = trace.match_commands(sorted(events, key=lambda e: e['time_ns']))
        self.assertFalse(any(anomalies.values()))
        self.assertEqual(len(commands), 3)
        self.assertEqual([c['end_ns'] - c['start_ns'] for c in commands], [2, 2, 2])

    def test_live_cid_reuse_is_an_anomaly(self):
        event = {"event": "nvme/nvme_setup_cmd", "time_ns": 1, "disk": "nvme0n1",
                 "ctrl_id": 0, "qid": 1, "cid": 1}
        _, anomalies = trace.match_commands([event, {**event, "time_ns": 2}])
        self.assertEqual(anomalies['duplicate_live_key'], 1)
        self.assertEqual(anomalies['setup_without_completion'], 1)

    def test_next_setup_after_flush_not_next_completion(self):
        commands = [command(0, 40, size=69632), command(10, 20, opcode=0, size=0), command(50, 80)]
        samples = trace.post_flush_samples(commands, 100)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]['write_start_ns'], 50)
        self.assertIn('overlapping_namespace_io', samples[0]['reasons'])

    def test_isolated_post_flush_write_is_eligible(self):
        samples = trace.post_flush_samples([command(0, 20, opcode=0, size=0), command(30, 60)], 100)
        self.assertTrue(samples[0]['eligible'])
        self.assertEqual(samples[0]['post_flush_write_us'], .03)

    def test_later_overlap_invalidates_initially_idle_write(self):
        commands = [command(0, 20, opcode=0, size=0), command(30, 80), command(40, 90, opcode=2)]
        self.assertIn('overlapping_namespace_io', trace.post_flush_samples(commands, 100)[0]['reasons'])

    def test_unknown_size_does_not_skip_to_later_4k_write(self):
        commands = [command(0, 20, opcode=0, size=0), command(30, 80, size=69632), command(90, 120)]
        samples = trace.post_flush_samples(commands, 100)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]['write_bytes'], 69632)
        self.assertIn('uncalibrated_write_size', samples[0]['reasons'])

    def test_other_process_cannot_be_called_wal_data(self):
        samples = trace.post_flush_samples([command(0, 20, opcode=0), command(30, 60, pid=200)], 100)
        self.assertIn('other_process', samples[0]['reasons'])

    def test_two_flushes_do_not_duplicate_one_write(self):
        samples = trace.post_flush_samples([command(0, 10, opcode=0), command(11, 20, opcode=0), command(30, 60)], 100)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]['flush_end_ns'], 20)

    def test_fua_and_failed_write_are_not_the_calibrated_signal(self):
        write = command(30, 60)
        write.update({'control': 0x4000, 'status': 1})
        samples = trace.post_flush_samples([command(0, 20, opcode=0), write], 100)
        self.assertIn('nonordinary_write_control', samples[0]['reasons'])
        self.assertIn('error_or_retry', samples[0]['reasons'])


class RegimeTests(unittest.TestCase):
    def test_one_tail_outlier_does_not_create_slow_regime(self):
        samples = [sample(i, 10000 if i == 100 else 30) for i in range(300)]
        regions = trace.detect_regimes(samples, 0, 301000000)
        self.assertEqual({r['state'] for r in regions}, {'fast', 'unknown'})

    def test_sustained_change_has_unknown_guard_and_no_backdating(self):
        samples = [sample(i, 30 if i < 150 else 640) for i in range(400)]
        regions = trace.detect_regimes(samples, 0, 401000000)
        fast = next(r for r in regions if r['state'] == 'fast')
        slow = next(r for r in regions if r['state'] == 'slow')
        self.assertLess(fast['end_ns'], 150000000)
        self.assertGreater(slow['start_ns'], 150000000)
        self.assertTrue(any(r['state'] == 'unknown' and fast['end_ns'] <= r['start_ns'] < slow['start_ns'] for r in regions))

    def test_insufficient_samples_stay_unknown(self):
        for count in [0, 1, 16, 19]:
            regions = trace.detect_regimes([sample(i) for i in range(count)], 0, 100000000)
            self.assertEqual(regions, [{'start_ns': 0, 'end_ns': 100000000, 'state': 'unknown'}])

    def test_trace_loss_cannot_produce_classified_intervals(self):
        regions = trace.detect_regimes([sample(i) for i in range(300)], 0, 301000000, quality_ok=False)
        self.assertTrue(all(r['state'] == 'unknown' for r in regions))

    def test_sampling_gap_is_not_forward_filled(self):
        samples = [sample(i) for i in range(100)] + [sample(i) for i in range(600, 800)]
        regions = trace.detect_regimes(samples, 0, 801000000)
        self.assertIsNone(trace.containing_regime(regions, 150000000, 500000000))

    def test_uncalibrated_sample_resets_evidence(self):
        samples = [sample(i) for i in range(100)] + [sample(100, eligible=False)]
        samples += [sample(i) for i in range(101, 110)]
        regions = trace.detect_regimes(samples, 0, 111000000)
        self.assertIsNone(trace.containing_regime(regions, 101000000, 110000000))

    def test_median_middle_band_stays_unknown(self):
        regions = trace.detect_regimes([sample(i, 250) for i in range(300)], 0, 301000000)
        self.assertTrue(all(r['state'] == 'unknown' for r in regions))

    def test_regimes_cover_capture_without_overlap_or_zero_width(self):
        samples = [sample(i, 30 if i % 200 < 100 else 640) for i in range(600)]
        regions = trace.detect_regimes(samples, 0, 601000000)
        self.assertEqual(regions[0]['start_ns'], 0)
        self.assertEqual(regions[-1]['end_ns'], 601000000)
        self.assertTrue(all(a['end_ns'] == b['start_ns'] for a, b in zip(regions, regions[1:])))
        self.assertTrue(all(r['start_ns'] < r['end_ns'] for r in regions))


class IntervalTests(unittest.TestCase):
    def timeline(self):
        return {'clock': 'CLOCK_MONOTONIC', 'schema': 'kv-engine.wal-benchmark-timeline.v1',
                'counter_points': [point(0), point(1), point(2)],
                'requests': [{'start_ns': 1000, 'end_ns': 1000000, 'puts': 64},
                             {'start_ns': 1000001000, 'end_ns': 1001000000, 'puts': 64}]}

    def test_counts_and_cpu_conserved_in_real_interval_lengths(self):
        timeline = self.timeline()
        regions = [{'start_ns': 0, 'end_ns': 3000000000, 'state': 'fast'}]
        rows = trace.metric_intervals(timeline, regions)
        self.assertEqual(sum(r['puts'] for r in rows), 128)
        self.assertAlmostEqual(sum(r['process_cpu_s'] for r in rows), .22)
        self.assertTrue(all(r['state'] == 'fast' for r in rows))

    def test_crossing_request_rejects_whole_interval_not_just_request(self):
        timeline = self.timeline()
        timeline['requests'][1]['start_ns'] = 900000000
        regions = [{'start_ns': 0, 'end_ns': 950000000, 'state': 'fast'},
                   {'start_ns': 950000000, 'end_ns': 3000000000, 'state': 'slow'}]
        rows = trace.metric_intervals(timeline, regions)
        self.assertEqual(rows[1]['state'], 'unknown')
        self.assertEqual(rows[1]['rejection_reason'], 'request_crosses_regime')
        self.assertEqual(rows[1]['puts'], 64)

    def test_crossing_counter_bracket_rejects_interval(self):
        timeline = self.timeline()
        regions = [{'start_ns': point(0)['sample_end_ns'], 'end_ns': 3000000000, 'state': 'fast'}]
        rows = trace.metric_intervals(timeline, regions)
        self.assertEqual(rows[0]['state'], 'unknown')

    def test_too_few_latencies_do_not_get_p99(self):
        rows = trace.metric_intervals(self.timeline(), [{'start_ns': 0, 'end_ns': 3000000000, 'state': 'fast'}])
        self.assertIsNone(rows[0]['p99_ms'])

    def test_clock_mismatch_is_rejected(self):
        timeline = self.timeline()
        timeline['clock'] = 'CLOCK_REALTIME'
        with self.assertRaises(ValueError):
            trace.metric_intervals(timeline, [])

    def test_counter_reset_is_rejected(self):
        timeline = self.timeline()
        timeline['counter_points'][1]['sync_count'] = -1
        with self.assertRaises(ValueError):
            trace.metric_intervals(timeline, [])

    def test_short_last_interval_keeps_raw_data_without_state_score(self):
        timeline = self.timeline()
        timeline['counter_points'].pop()
        timeline['counter_points'][1]['sample_begin_ns'] = 100000000
        timeline['counter_points'][1]['sample_end_ns'] = 100000100
        timeline['requests'].pop()
        rows = trace.metric_intervals(timeline, [{'start_ns': 0, 'end_ns': 3000000000, 'state': 'fast'}])
        self.assertEqual(rows[0]['rejection_reason'], 'short_interval')
        self.assertEqual(rows[0]['puts'], 64)


if __name__ == '__main__':
    unittest.main()
