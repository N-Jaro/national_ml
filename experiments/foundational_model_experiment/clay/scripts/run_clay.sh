#!/bin/bash
#
# Clay Foundation Model Fine-tuning Training Script
# Following the same pattern as Prithvi fine-tuning
#

set -e

# Set up environment
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD:$PYTHONPATH"

# Default settings
GPUS=1
CONFIG="configs/clay_config.yaml"
TEST_MODE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --gpus)
            GPUS="$2"
            shift 2
            ;;
        --config)
            CONFIG="$2" 
            shift 2
            ;;
        --test)
            TEST_MODE="--test"
            shift
            ;;
        *)
            echo "Unknown option $1"
            exit 1
            ;;
    esac
done

echo "🚀 Starting Clay Foundation Model Fine-tuning"
echo "=================================================="
echo "Model: Clay v1 Base (timm_clay_v1_base)"
echo "Task: 4-modal water segmentation fine-tuning"
echo "Config: $CONFIG"
echo "GPUs: $GPUS"
if [[ -n "$TEST_MODE" ]]; then
    echo "Mode: TEST (2 epochs, 2 HUCs)"
else
    echo "Mode: FULL FINE-TUNING (500 epochs, 50 HUCs)"
fi
echo ""

# Activate terratorch environment
echo "Activating terratorch environment..."
source activate terratorch_env

# Run training
echo "Starting Clay fine-tuning..."
python training/train_clay.py \
    --config "$CONFIG" \
    --gpus $GPUS \
    $TEST_MODE

echo ""
echo "✅ Clay fine-tuning completed!"
