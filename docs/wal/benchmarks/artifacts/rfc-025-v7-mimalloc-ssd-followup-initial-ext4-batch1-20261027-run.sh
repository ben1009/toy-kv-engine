#!/bin/sh
set -eu
cd /home/liu/proj/toy-kv-engine
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-run.py ext4-batch1
