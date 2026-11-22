"""Integrity checks for the RFC 028 Cursor durability-sampler patch artifact."""

import hashlib
import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = (
    REPO_ROOT
    / "docs/wal/benchmarks/artifacts/rfc-028-v8-point16-critical-path-gcp-20261122"
)
PATCH_PATH = ARTIFACT_DIR / "diagnostic-cursor-sampler.patch"
PATCH_SHA256 = "c55c8e0ef1e7490e7892ba05f137b7fa8456b97c56935199a15d6a8a289f7f37"
REPORT_PATH = REPO_ROOT / "docs/wal/benchmarks/rfc-028-v8-point16-critical-path-gcp-20261122.md"


class Rfc028SamplerPatchTests(unittest.TestCase):
    def test_patch_digest_matches_reported_experiment(self):
        digest = hashlib.sha256(PATCH_PATH.read_bytes()).hexdigest()
        self.assertEqual(digest, PATCH_SHA256)
        self.assertIn(PATCH_SHA256, REPORT_PATH.read_text(encoding="utf-8"))

    def test_patch_only_removes_the_two_bench_sampler_guards(self):
        patch = PATCH_PATH.read_text(encoding="utf-8")
        changed_paths = re.findall(r"^\+\+\+ b/(.+)$", patch, re.MULTILINE)
        added_lines = [
            line[1:]
            for line in patch.splitlines()
            if line.startswith("+") and not line.startswith("+++")
        ]
        removed_lines = [
            line[1:].strip()
            for line in patch.splitlines()
            if line.startswith("-") and not line.startswith("---")
        ]

        self.assertEqual(changed_paths, ["kv-engine/src/mem_table.rs"])
        self.assertEqual(added_lines, [])
        self.assertIn("self.wal_v8_durability_sample_every() == 0,", removed_lines)
        self.assertIn("if every > 0", removed_lines)
        self.assertIn(
            '"V8 durability phase diagnostics are not supported with the progress ring"',
            removed_lines,
        )


if __name__ == "__main__":
    unittest.main()
