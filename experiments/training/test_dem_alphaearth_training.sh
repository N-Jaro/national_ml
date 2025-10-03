#!/bin/bash

#-----------------------------------------------------------------------------
# Quick test script for DEM + AlphaEarth model training
# Tests with 1 HUC code and 1 epoch to verify everything works
#-----------------------------------------------------------------------------

echo "======================================================"
echo "Testing DEM + AlphaEarth Model Training"
echo "Quick test: 1 HUC, 1 epoch, offline W&B"
echo "======================================================"

# Create log directory
mkdir -p slurm_logs

# Get timestamp for unique naming
DATE_TIME=$(date +%Y%m%d_%H%M%S)
WANDB_NAME="test_dem_alphaearth_${DATE_TIME}"

echo "Configuration:"
echo "  HUC Code: 03030005 (single test HUC with data)"
echo "  Epochs: 1"
echo "  Batch Size: 4 (small for quick test)"
echo "  W&B Mode: offline"
echo "  Run Name: $WANDB_NAME"
echo "======================================================"

# Activate Python environment
echo "Activating Python environment..."
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Navigate to training directory
cd /u/nathanj/national_ml/experiments/training

# Run quick training test
echo "Starting training test..."
python run_lightning_train_dem_alphaearth.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset/ \
    --hucs "03030005" \
    --batch_size 4 \
    --epochs 1 \
    --lr 1e-3 \
    --precision 16 \
    --alphaearth_channels 64 \
    --wandb_mode offline \
    --wandb_run "$WANDB_NAME" \
    --patience 5 \
    --num_workers 2

# Check exit code
if [ $? -eq 0 ]; then
    echo "======================================================"
    echo "✅ SUCCESS: DEM + AlphaEarth training test completed!"
    echo "The model training pipeline works correctly."
    echo "Ready for full training runs."
    echo "======================================================"
else
    echo "======================================================"
    echo "❌ ERROR: Training test failed!"
    echo "Please check the error messages above."
    echo "======================================================"
    exit 1
fi