#!/bin/bash
#
# Test script for Clay Foundation Model fine-tuning
# Tests the model with a minimal configuration following Prithvi pattern
#

set -e

echo "=== Clay Foundation Model Fine-tuning Test ==="
echo "Testing Clay model with 2 epochs and 2 HUCs..."

# Navigate to the Clay experiment directory
cd "$(dirname "$0")/.."

# Activate terratorch environment
echo "Activating terratorch environment..."
source activate terratorch_env

# Run training with test configuration
echo "Starting Clay fine-tuning test..."
python training/train_clay.py \
    --config configs/clay_config.yaml \
    --test \
    --gpus 1

echo "=== Clay Fine-tuning Test Completed ==="
