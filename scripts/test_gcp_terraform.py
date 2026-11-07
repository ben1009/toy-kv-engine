#!/usr/bin/env python3
"""Render the real Terraform template and parse it with Bash; no provider calls."""

import base64
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class GcpTerraformTemplateTests(unittest.TestCase):
    def test_real_template_renders_valid_bash_for_each_supported_shape_and_rust_setting(self):
        terraform = os.environ.get("TERRAFORM_BIN", "terraform")
        template = ROOT / "infra/gcp/test-environment/startup-script.sh.tftpl"
        for count in (1, 2):
            for rust in (False, True):
                with self.subTest(count=count, rust=rust), tempfile.TemporaryDirectory() as temp:
                    variables = {"local_ssd_count": count, "install_rust_toolchain": rust,
                                 "ssh_username": "tester", "local_ssd_mount": "/mnt/test-ssd"}
                    expression = f'base64encode(templatefile({json.dumps(str(template))}, {json.dumps(variables)}))\n'
                    result = subprocess.run(
                        [terraform, "console", "-no-color"], input=expression, text=True,
                        capture_output=True, check=False, cwd=temp,
                        env={**os.environ, "CHECKPOINT_DISABLE": "1"},
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    rendered = base64.b64decode(json.loads(result.stdout))
                    parsed = subprocess.run(["bash", "-n"], input=rendered, capture_output=True, check=False)
                    self.assertEqual(parsed.returncode, 0, parsed.stderr.decode())


if __name__ == "__main__":
    unittest.main()
