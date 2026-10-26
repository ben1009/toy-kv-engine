#!/usr/bin/env bash
set -euo pipefail
cd /home/liu/proj/toy-kv-engine
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-mimalloc-aligned-20261026-run.py ext4-point8
