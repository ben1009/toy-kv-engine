#!/usr/bin/env bash
set -euo pipefail
cd /home/liu/proj/toy-kv-engine
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-large-group-hint-cache-20261026-run.py tmpfs-point1
