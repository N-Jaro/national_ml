#!/bin/bash

# Script to easily run the rerun_pipeline.py with various options
# Usage examples:
#   ./run_rerun_pipeline.sh --hucs 19010207 --dry-run
#   ./run_rerun_pipeline.sh --hucs 19010207 10020007 --force 3
#   ./run_rerun_pipeline.sh --hucs 19010207 --force 1 2 3 4

cd "$(dirname "$0")"

echo "=== Running Pipeline Rerun Tool ==="
echo "Working directory: $(pwd)"
echo "Command: python rerun_pipeline.py $@"
echo ""

python rerun_pipeline.py "$@"