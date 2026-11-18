#!/usr/bin/env python3
"""Analyze a saved commit-order trace or write-perf result JSON offline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from gcp_commit_order_trace_runner import analyze_trace


def extract_trace(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the trace payload from either a trace object or write-perf JSON."""
    if isinstance(payload.get("records"), list):
        return payload

    result = payload.get("result")
    if not isinstance(result, dict):
        result = payload
    pipeline = result.get("wal_pipeline")
    if isinstance(pipeline, dict):
        trace = pipeline.get("commit_order_trace")
        if isinstance(trace, dict) and isinstance(trace.get("records"), list):
            return trace

    raise ValueError("input JSON does not contain a commit-order trace")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_json", type=Path, help="saved write-perf result.json or extracted trace JSON")
    args = parser.parse_args(argv)

    try:
        payload = json.loads(args.result_json.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("input JSON must contain an object")
        result = analyze_trace(extract_trace(payload))
    except (OSError, json.JSONDecodeError, ValueError) as error:
        parser.error(str(error))

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
