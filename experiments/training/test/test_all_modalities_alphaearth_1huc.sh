#!/bin/bash
#SBATCH --job-name=test_all_modalities_alphaearth_1huc
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --time=2:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nj7@illinois.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_1huc_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_1huc_%j.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

echo "=== SLURM Job Info ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Job name: $SLURM_JOB_NAME"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "GPU: $CUDA_VISIBLE_DEVICES"
echo ""

echo "=== Environment ==="
echo "Python: $(which python)"
echo "Conda env: $CONDA_DEFAULT_ENV"
echo "PyTorch version: $(python -c 'import torch; print(torch.__version__)')"
echo "CUDA available: $(python -c 'import torch; print(torch.cuda.is_available())')"
echo "GPU count: $(python -c 'import torch; print(torch.cuda.device_count())')"
echo ""

# Single HUC test - first HUC from optimal lists
TRAIN_HUC="03160113"
VAL_HUC="16040204"

echo "=== Single HUC Test Configuration ==="
echo "Train HUC: $TRAIN_HUC"
echo "Val HUC: $VAL_HUC"
echo "Purpose: Quick validation of All Modalities + AlphaEarth architecture"
echo "Batch size: 32 (optimized)"
echo "Epochs: 5 (quick test)"
echo ""

# Change to training directory
cd /u/nathanj/national_ml/experiments/training

# Run training with single HUCs for quick validation
python run_lightning_train_all_modalities_alphaearth.py \
    --train_hucs "$TRAIN_HUC" \
    --val_hucs "$VAL_HUC" \
    --epochs 5 \
    --batch_size 32 \
    --lr 1e-3 \
    --base_channels 64 \
    --water_loss_scale 1.0 \
    --d8_loss_scale 0.5 \
    --d8_label_smoothing 0.05 \
    --num_workers 8 \
    --precision "32" \
    --wandb_mode "online" \
    --early_stopping_patience 15 \
    --seed 42 \
    --experiment_name "all_modalities_alphaearth_1huc_test_$(date +%Y%m%d_%H%M%S)"

echo "=== Single HUC test completed at $(date) ==="