"""Verify interrupted confirmation runs preserve their frozen evidence."""

import hashlib
import json
from pathlib import Path
import runpy
import tempfile
import types
import unittest
from unittest import mock


BENCHMARKS = Path(__file__).resolve().parent
RUNNER = runpy.run_path(
    str(BENCHMARKS / "artifacts/rfc-026-v8-notification-rerun-20261102-confirmation.py")
)
LOCAL_TARGET = BENCHMARKS.parents[2] / "target"


class ConfirmationResumeTests(unittest.TestCase):
    def setUp(self):
        LOCAL_TARGET.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=LOCAL_TARGET)
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        binaries = {}
        hashes = {}
        for arm in ("baseline", "kept"):
            path = self.directory / f"write-perf-{arm}"
            path.write_bytes(arm.encode())
            binaries[arm] = str(path)
            hashes[arm] = hashlib.sha256(path.read_bytes()).hexdigest()
        (self.directory / "run.log").write_text("DONE\n")
        (self.directory / "plan.json").write_text(json.dumps({
            "binary_paths": binaries,
            "binary_sha256": hashes,
            "selection": "fixed",
            "stall_filter": "fixed",
            "operations": {"batch": 750000, "point": 20000},
        }))

    @staticmethod
    def write_result(output):
        output.write(json.dumps({
            "result": {
                "ops": 100,
                "ops_per_sec": 1000,
                "process_cpu_user_ms": 1,
                "process_cpu_system_ms": 1,
                "wal_pipeline": {
                    "sync_count": 1,
                    "fdatasync_ns": 100,
                    "worker_eventfd_writes": 0,
                },
            },
            "latency": {"p99_ms": 1},
        }))
        output.flush()

    def test_interruption_resumes_without_overwriting_successes(self):
        calls = []

        def interrupted(command, *, stdout, **_kwargs):
            calls.append(command)
            if len(calls) == 3:
                stdout.write('{"partial":')
                stdout.flush()
                raise RuntimeError("interrupted")
            self.write_result(stdout)
            return types.SimpleNamespace(returncode=0)

        with mock.patch.object(RUNNER["time"], "sleep"), mock.patch.object(
            RUNNER["subprocess"], "run", side_effect=interrupted
        ):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                RUNNER["run_cohort"](self.directory)

        completed_name = "confirmation-00-batch-baseline-8"
        completed = {
            path.name: path.read_bytes()
            for path in RUNNER["sample_paths"](self.directory, completed_name)
            if path.exists()
        }
        second_calls = []

        def succeed(command, *, stdout, **_kwargs):
            second_calls.append(command)
            self.write_result(stdout)
            return types.SimpleNamespace(returncode=0)

        with mock.patch.object(RUNNER["time"], "sleep"), mock.patch.object(
            RUNNER["subprocess"], "run", side_effect=succeed
        ):
            RUNNER["run_cohort"](self.directory)
        self.assertEqual(len(second_calls), 10)
        for name, contents in completed.items():
            self.assertEqual((self.directory / name).read_bytes(), contents)
        archived = self.directory / "confirmation-interrupted/confirmation-02-point-baseline-8/attempt-000"
        self.assertEqual((archived / "confirmation-02-point-baseline-8.json").read_text(), '{"partial":')
        self.assertEqual(len(list(self.directory.glob("confirmation-*-status.json"))), 12)
        self.assertTrue((self.directory / "confirmation-resume-log.jsonl").is_file())

    def test_completed_sample_with_another_command_is_not_replaced(self):
        name = "confirmation-00-batch-baseline-8"
        result_path, _, _, status_path = RUNNER["sample_paths"](self.directory, name)
        result_path.write_text('{"result": "preserve"}')
        status_path.write_text(json.dumps({"name": name, "returncode": 0, "command": ["other"]}))
        with self.assertRaisesRegex(ValueError, "frozen command"):
            RUNNER["sample_completed"](self.directory, name, ["expected"])
        self.assertEqual(result_path.read_text(), '{"result": "preserve"}')
        self.assertFalse((self.directory / "confirmation-interrupted").exists())

    def test_existing_plan_must_match(self):
        path = self.directory / "confirmation-plan.json"
        self.assertTrue(RUNNER["ensure_frozen_plan"](path, {"schedule": [1]}))
        original = path.read_bytes()
        self.assertFalse(RUNNER["ensure_frozen_plan"](path, {"schedule": [1]}))
        with self.assertRaisesRegex(ValueError, "differs"):
            RUNNER["ensure_frozen_plan"](path, {"schedule": [2]})
        self.assertEqual(path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
