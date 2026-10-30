#!/bin/sh
set -eu
cd /home/liu/proj/toy-kv-engine
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-async-extent-20261026-run.py ext4-batch64
