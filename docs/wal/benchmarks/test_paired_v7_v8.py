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


def make_pair_plan():
    return {"protocol": paired.PAIR_PROTOCOL, "policy": paired.POLICY,
            "binary_sha256": "same", "boot_id": "same", "cpu_mask": list(range(16)),
            "num": 750000, "writers": 16, "batch_entries": 64, "value_bytes": 1024,
            "admission_max_probes": 6, "pair_schedule": paired.PAIR_SCHEDULE}


def make_pair(spec=paired.PAIR_SCHEDULE[0]):
    median = 30 if spec["target_state"] == "fast" else 640
    block = make_block(spec["order"], median)
    for run in block["runs"]:
        record = run["record"]
        record["params"].update({"seed": 42, "latency_sample_every": 10,
                                  "wal_format_version": int(run["format"][1:])})
        record["engine_options"] = {"wal": True, "compaction": "none", "target_sst_size": 2147483648}
        m = run["metrics"]
        record["latency"] = {"p99_ms": m["p99_ms"]}
        record["result"] = {"ops": 750000, "ops_per_sec": m["puts_s"],
                            "process_cpu_user_ms": m["cpu_s"] * 1000, "process_cpu_system_ms": 0,
                            "wal_pipeline": {"sync_count": m["syncs"]},
                            "wal_v8_digest": {"payload_hash_bytes": 750000 * 1050,
                                              "post_encode_hash_reread_bytes": 0}}
        run["phases"] = {"process_before": {"voluntary": 20},
                         "process_after": {"voluntary": 20 + m["voluntary"]}}
    return {"target_state": spec["target_state"], "order": spec["order"], "admitted": True,
            "admission_probes": block["probes"][:2], "runs": block["runs"], "probes": block["probes"][1:4]}


class IndependentPairTests(unittest.TestCase):
    def test_fast_and_slow_pairs_can_be_diagnostic(self):
        for spec in paired.PAIR_SCHEDULE:
            result = paired.summarize_pair(make_pair(spec), make_pair_plan(), spec)
            self.assertTrue(result["probe_compatible_diagnostic_pair"], result)
            self.assertAlmostEqual(result["v8_over_v7"]["puts_s"], 1.05)
            self.assertFalse(result["RFC_performance_qualified"])

    def test_reversed_pair_keeps_v8_over_v7_direction(self):
        a = paired.summarize_pair(make_pair(), make_pair_plan(), paired.PAIR_SCHEDULE[0])
        spec = paired.PAIR_SCHEDULE[1]
        b = paired.summarize_pair(make_pair(spec), make_pair_plan(), spec)
        self.assertEqual(a["v8_over_v7"], b["v8_over_v7"])

    def test_rejected_neighbor_does_not_discard_valid_pair(self):
        bad = make_pair()
        bad["probes"][1] = make_probe(640)
        bad_summary = paired.summarize_pair(bad, make_pair_plan(), paired.PAIR_SCHEDULE[0])
        spec = paired.PAIR_SCHEDULE[1]
        good_summary = paired.summarize_pair(make_pair(spec), make_pair_plan(), spec)
        self.assertIsNone(bad_summary["v8_over_v7"])
        self.assertTrue(good_summary["probe_compatible_diagnostic_pair"])
        self.assertEqual(paired.summarize_pairs([bad_summary, good_summary])["fast"]["accepted_pairs"], 1)

    def test_middle_probe_transition_rejects_pair(self):
        pair = make_pair()
        pair["probes"][1] = make_probe(640)
        result = paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])
        self.assertFalse(result["probe_compatible_diagnostic_pair"])
        self.assertIsNone(result["v8_over_v7"])

    def test_all_three_probes_must_agree_pairwise(self):
        spec = paired.PAIR_SCHEDULE[2]
        pair = make_pair(spec)
        pair["probes"][1:] = [make_probe(770), make_probe(540)]
        self.assertEqual(paired.compare_probes(pair["probes"][0], pair["probes"][1]), [])
        self.assertEqual(paired.compare_probes(pair["probes"][0], pair["probes"][2]), [])
        result = paired.summarize_pair(pair, make_pair_plan(), spec)
        self.assertIn("probes_1_2:post_flush_p50_drift", result["rejection_reasons"])

    def test_saved_probe_summary_cannot_override_raw_latency(self):
        pair = make_pair()
        pair["probes"][1] = copy.deepcopy(pair["probes"][1])
        pair["probes"][1]["storage_state"] = "slow"
        result = paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])
        self.assertIn("probe_1:invalid_raw_probe", result["rejection_reasons"])

    def test_nan_or_wrong_sample_count_is_invalid(self):
        for value in [float('nan'), 0, -1]:
            probe = make_probe()
            probe["write_ns"][0] = value
            with self.assertRaises(ValueError):
                paired.checked_probe(probe)
        probe = make_probe()
        probe["sync_ns"].pop()
        with self.assertRaises(ValueError):
            paired.checked_probe(probe)

    def test_admission_requires_two_probes_in_target_state(self):
        self.assertFalse(paired.admission_ready([make_probe(30)], "fast"))
        self.assertFalse(paired.admission_ready([make_probe(), make_probe()], "fast"))
        self.assertTrue(paired.admission_ready([make_probe(), make_probe()], "slow"))

    def test_admission_stops_on_first_stable_window(self):
        calls = []
        values = iter([30, 640, 640, 640])

        def probe():
            calls.append(1)
            return make_probe(next(values))

        probes, admitted = paired.acquire_stable_probes(probe, "slow")
        self.assertTrue(admitted)
        self.assertEqual(len(probes), 3)
        self.assertEqual(len(calls), 3)

    def test_unavailable_state_has_bounded_probes_and_no_captures(self):
        calls = []

        def capture(*_args):
            self.fail("No benchmark may run without admission")

        def probe():
            calls.append(1)
            return make_probe(640)

        result = paired.run_pair(paired.PAIR_SCHEDULE[0], probe, capture)
        self.assertFalse(result["admitted"])
        self.assertEqual(len(calls), 6)
        self.assertEqual(result["runs"], [])

    def test_each_admitted_pair_gets_exactly_one_attempt(self):
        calls = []
        spec = paired.PAIR_SCHEDULE[1]
        fixture = make_pair(spec)

        def capture(position, fmt):
            calls.append(fmt)
            return fixture["runs"][position]

        pair = paired.run_pair(spec, lambda: make_probe(30), capture)
        self.assertEqual(calls, ["v8", "v7"])
        self.assertEqual(len(pair["admission_probes"]), 2)
        self.assertEqual(len(pair["probes"]), 3)
        self.assertTrue(paired.summarize_pair(pair, make_pair_plan(), spec)["probe_compatible_diagnostic_pair"])

    def test_failed_admission_probe_preserves_earlier_samples(self):
        calls = []
        partial = {}

        def probe():
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("probe failed")
            return make_probe(30)

        with self.assertRaises(RuntimeError):
            paired.run_pair(paired.PAIR_SCHEDULE[0], probe, lambda *_: None, result=partial)
        self.assertEqual(len(partial["admission_probes"]), 1)

    def test_admission_cannot_continue_past_success(self):
        pair = make_pair()
        pair["admission_probes"].append(make_probe(30))
        result = paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])
        self.assertIn("admission_continued_after_success", result["rejection_reasons"])

    def test_two_matching_wrong_identities_do_not_match_plan(self):
        for field, value in [("binary_sha256", "wrong"), ("boot_id", "wrong"), ("cpu_mask", [0, 1])]:
            pair = make_pair()
            for run in pair["runs"]:
                run[field] = value
            result = paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])
            self.assertIn(field + "_mismatch", result["rejection_reasons"])

    def test_two_matching_wrong_workloads_do_not_match_plan(self):
        pair = make_pair()
        for run in pair["runs"]:
            run["record"]["params"]["threads"] = 8
        result = paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])
        self.assertIn("run_0:workload_mismatch", result["rejection_reasons"])

    def test_derived_metrics_cannot_override_raw_capture(self):
        pair = make_pair()
        pair["runs"][1]["metrics"]["puts_s"] *= 2
        result = paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])
        self.assertIn("run_1:invalid_metrics", result["rejection_reasons"])

    def test_wrong_digest_accounting_rejects_pair(self):
        pair = make_pair()
        pair["runs"][1]["record"]["result"]["wal_v8_digest"]["post_encode_hash_reread_bytes"] = 1
        result = paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])
        self.assertIn("run_1:digest_accounting_mismatch", result["rejection_reasons"])

    def test_incomplete_pair_and_capture_error_reject_ratios(self):
        for field in ("probes", "runs"):
            pair = make_pair()
            pair[field].pop()
            result = paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])
            self.assertIn("incomplete_pair", result["rejection_reasons"])
            self.assertIsNone(result["v8_over_v7"])
        pair = make_pair()
        pair["error"] = "interrupted"
        self.assertIn("capture_error", paired.summarize_pair(pair, make_pair_plan(), paired.PAIR_SCHEDULE[0])["rejection_reasons"])

    def test_combined_ratio_requires_both_orders_in_same_state(self):
        plan = make_pair_plan()
        fast = paired.summarize_pair(make_pair(), plan, paired.PAIR_SCHEDULE[0])
        slow_spec = paired.PAIR_SCHEDULE[3]
        slow = paired.summarize_pair(make_pair(slow_spec), plan, slow_spec)
        summary = paired.summarize_pairs([fast, slow])
        self.assertIsNone(summary["fast"]["geomean_v8_over_v7"])
        self.assertIsNone(summary["slow"]["geomean_v8_over_v7"])
        reverse = paired.summarize_pair(make_pair(paired.PAIR_SCHEDULE[1]), plan, paired.PAIR_SCHEDULE[1])
        summary = paired.summarize_pairs([fast, reverse, slow])
        self.assertAlmostEqual(summary["fast"]["geomean_v8_over_v7"]["puts_s"], 1.05)
        self.assertIsNone(summary["slow"]["geomean_v8_over_v7"])

    def test_legacy_archived_blocks_keep_their_original_rejections(self):
        root = Path(__file__).resolve().parent / "artifacts"
        prefix = "rfc-026-v8-state-paired-20261103"
        original = json.loads((root / f"{prefix}-summary.json").read_text())
        for index in range(2):
            raw = json.loads((root / f"{prefix}-block{index}.json").read_text())
            self.assertEqual(paired.summarize_block(raw), original["blocks"][index])


if __name__ == "__main__":
    unittest.main()
