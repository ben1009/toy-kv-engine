"""Check storage rejection and paired-ratio behavior without device access."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

import paired_v7_v8 as paired


def make_probe(median_us=640, sync_us=240):
    writes = [int((median_us if i % 4 == 0 else 30) * 1000) for i in range(1024)]
    syncs = [sync_us * 1000] * 256
    probe = {"write_ns": writes, "sync_ns": syncs,
             "write": paired.stats(writes), "first_after_sync": paired.stats(writes[::4]),
             "sync": paired.stats(syncs)}
    probe["storage_state"] = paired.probe_state(probe)
    return probe


def make_block(order=paired.ORDERS[0], median_us=640):
    runs = []
    for fmt in order:
        runs.append({"format": fmt, "binary_sha256": "same", "boot_id": "same",
                     "record": {"params": {"num": 750000, "value_size": 1024, "batch_size": 64,
                                           "threads": 16, "wal_io_mode": "parallel"}},
                     "cpu_mask": list(range(16)), "metrics": {
                         "puts_s": 400000 if fmt == "v7" else 420000,
                         "p99_ms": 20, "cpu_s": 5 if fmt == "v7" else 6,
                         "syncs": 1000, "voluntary": 10000, "existing_storage_flag": True}})
    return {"runs": runs, "probes": [make_probe(median_us) for _ in range(5)]}


class StorageStateTests(unittest.TestCase):
    def test_thresholds_have_no_unclassified_gap(self):
        for value, expected in [(30, "fast"), (99.9, "fast"), (100, "intermediate"),
                                (300, "intermediate"), (499.9, "intermediate"), (500, "slow")]:
            self.assertEqual(paired.state_for_median(value), expected)

    def test_constant_slow_probe_is_not_a_transition(self):
        self.assertEqual(make_probe()["storage_state"], "slow")

    def test_fast_to_slow_inside_probe_is_detected(self):
        probe = make_probe(30)
        for i in range(512, 1024, 4):
            probe["write_ns"][i] = 640000
        self.assertEqual(paired.probe_state(probe), "transitioning")

    def test_within_slow_state_drift_is_detected(self):
        probe = make_probe(640)
        for i in range(512, 1024, 4):
            probe["write_ns"][i] = 1000000
        self.assertEqual(paired.probe_state(probe), "transitioning")

    def test_endpoint_tail_change_rejects_same_median(self):
        a, b = make_probe(), make_probe()
        b["first_after_sync"]["p99_us"] = 15000
        self.assertIn("post_flush_p99_drift", paired.compare_probes(a, b))

    def test_sync_tail_change_rejects_same_post_flush(self):
        a, b = make_probe(), make_probe()
        b["sync"]["p99_us"] = 9000
        self.assertIn("sync_p99_drift", paired.compare_probes(a, b))

    def test_long_write_count_change_rejects_same_median(self):
        a, b = make_probe(), make_probe()
        b["write"]["over_5ms"] = 32
        self.assertIn("long_write_count_drift", paired.compare_probes(a, b))

    def test_drift_is_symmetric(self):
        a, b = make_probe(), make_probe(900)
        self.assertEqual(paired.compare_probes(a, b), paired.compare_probes(b, a))

    def test_small_slow_state_variation_is_accepted(self):
        self.assertEqual(paired.compare_probes(make_probe(640), make_probe(650)), [])


class PairingTests(unittest.TestCase):
    def test_stable_slow_block_has_diagnostic_ratios_but_no_qualification(self):
        result = paired.summarize_block(make_block())
        self.assertTrue(result["probe_compatible_diagnostic_block"])
        self.assertEqual(result["storage_state"], "slow")
        self.assertFalse(result["all_runs_pass_existing_storage_filter"])
        self.assertFalse(result["RFC_performance_qualified"])
        self.assertAlmostEqual(result["paired_geomean_v8_over_v7"]["puts_s"], 1.05)

    def test_order_reversal_keeps_ratio_direction(self):
        a = paired.summarize_block(make_block(paired.ORDERS[0]))
        b = paired.summarize_block(make_block(paired.ORDERS[1]))
        self.assertEqual(a["paired_geomean_v8_over_v7"], b["paired_geomean_v8_over_v7"])

    def test_changed_state_discards_entire_block(self):
        block = make_block()
        block["probes"][2] = make_probe(30)
        result = paired.summarize_block(block)
        self.assertFalse(result["probe_compatible_diagnostic_block"])
        self.assertEqual(result["adjacent_pairs"], [])
        self.assertIsNone(result["paired_geomean_v8_over_v7"])

    def test_same_state_tail_drift_discards_entire_block(self):
        block = make_block()
        block["probes"][4]["sync"]["p99_us"] = 10000
        self.assertFalse(paired.summarize_block(block)["probe_compatible_diagnostic_block"])

    def test_identity_mismatches_reject_ratios(self):
        for field, value, reason in [("binary_sha256", "different", "binary_mismatch"),
                                     ("boot_id", "different", "boot_mismatch"),
                                     ("cpu_mask", [0, 1], "cpu_mask_mismatch")]:
            block = make_block()
            block["runs"][1][field] = value
            result = paired.summarize_block(block)
            self.assertIn(reason, result["rejection_reasons"])
            self.assertEqual(result["adjacent_pairs"], [])
            self.assertEqual(result["storage_state"], "slow")

    def test_missing_probe_or_run_rejects_ratios(self):
        for field in ("probes", "runs"):
            block = make_block()
            block[field].pop()
            self.assertIn("incomplete_block", paired.summarize_block(block)["rejection_reasons"])

    def test_wrong_order_rejects_ratios(self):
        block = make_block(("v7", "v7", "v8", "v8"))
        self.assertIn("order_mismatch", paired.summarize_block(block)["rejection_reasons"])

    def test_different_workload_rejects_ratios(self):
        block = make_block()
        block["runs"][1]["record"]["params"]["threads"] = 8
        self.assertIn("workload_mismatch", paired.summarize_block(block)["rejection_reasons"])

    def test_missing_block_remains_incomplete_and_raw_record_unchanged(self):
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory(dir=root / "target") as directory:
            p = Path(directory)
            (p / "plan.json").write_text(json.dumps({"policy": paired.POLICY}))
            raw = json.dumps(make_block())
            (p / "block0.json").write_text(raw)
            summary = paired.analyze_directory(p)
            self.assertFalse(summary["all_raw_blocks_preserved"])
            self.assertFalse(summary["blocks"][1]["probe_compatible_diagnostic_block"])
            self.assertIn("incomplete_block", summary["blocks"][1]["rejection_reasons"])
            self.assertEqual((p / "block0.json").read_text(), raw)

    def test_ratios_use_adjacent_pairs_not_unmatched_best_scores(self):
        block = make_block()
        for run, throughput in zip(block["runs"], (100, 120, 200, 250), strict=True):
            run["metrics"]["puts_s"] = throughput
        result = paired.summarize_block(block)
        self.assertAlmostEqual(result["paired_geomean_v8_over_v7"]["puts_s"], (1.2 * 0.8) ** 0.5)
        self.assertNotEqual(result["paired_geomean_v8_over_v7"]["puts_s"], 200 / 250)

    def test_phase_scaling_is_per_thread_and_inclusive(self):
        phases = {"phase_names": ["encode"], "threads": [
            {"buckets": [{"calls": 100, "samples": 2, "cpu_ns": 200, "wall_ns": 400}]},
            {"buckets": [{"calls": 10, "samples": 1, "cpu_ns": 200, "wall_ns": 400}]},
        ]}
        estimate = paired.phase_estimates(copy.deepcopy(phases))["encode"]
        self.assertAlmostEqual(estimate["cpu_s"], 12000 / 1e9)
        self.assertEqual(estimate["calls"], 110)


if __name__ == "__main__":
    unittest.main()
