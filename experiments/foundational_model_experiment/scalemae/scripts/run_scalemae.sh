#!/bin/bash
# Scalemae Foundation Model Training Script

# Set up environment
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD:$PYTHONPATH"

# Default settings
GPUS=1
CONFIG="configs/scalemae_config.yaml"
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

echo "🚀 Starting Scalemae Foundation Model Training"
echo "=================================================="
echo "Config: $CONFIG"
echo "GPUs: $GPUS"
if [[ -n "$TEST_MODE" ]]; then
    echo "Mode: TEST (2 epochs, 2 HUCs)"
else
    echo "Mode: FULL TRAINING"
fi
echo ""

# Run training
python training/train_scalemae.py \
    --config "$CONFIG" \
    --gpus $GPUS \
    $TEST_MODE

echo ""
echo "✅ Scalemae training completed!"
