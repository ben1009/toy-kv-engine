#!/usr/bin/env python3
"""Verify the rejected whole-group retirement tmpfs comparison."""
import json
import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parent
analysis = runpy.run_path(str(ROOT / "rfc-025-v7-mvcc-preparation-20261025-analyze.py"), run_name="analysis")
summary = analysis["compare"]("rfc-025-v7-frontier-retirement-tmpfs-8writers-20261025")
assert set(summary["binary_sha256"]) == {
    "06efc8d7dc473ddfd770832c9075c6cfffdc9ab05df13460481d4f2302f2625b",
    "0eb717d809a8baef5397398b086d5f5243c83b1167f3261378682d9d750005f9",
}
print(json.dumps(summary, indent=2))
