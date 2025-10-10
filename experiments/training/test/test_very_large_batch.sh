#!/bin/bash
#SBATCH --job-name=test_very_large_batch
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --time=01:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nathanj@colorado.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_very_large_batch_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_very_large_batch_%j.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

echo "=== Testing Very Large Batch Size Based on W&B Memory Analysis ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Previous batch size 16: ~30% GPU memory usage"
echo "Testing batch size 32: Should use ~50-60% GPU memory"
echo ""

# Print GPU info
nvidia-smi

cd /u/nathanj/national_ml/experiments/training

# Test with even larger batch size
echo ""
echo "=== Starting training with batch size 32 ==="
echo "Expected improvements:"
echo "- Better GPU utilization (currently ~40% peak)"
echo "- Fewer iterations per epoch (11-12 vs 23 batches)"
echo "- More stable gradients with larger batch statistics"
echo ""

python run_lightning_train_all_modalities_alphaearth.py \
    --hucs "07040006,05040003" \
    --epochs 1 \
    --batch_size 32 \
    --lr 0.001 \
    --val_split 0.5 \
    --wandb_mode offline \
    --experiment_name "very_large_batch_test_$(date +%Y%m%d_%H%M%S)" \
    --early_stopping_patience 10 \
    --num_workers 4

echo ""
echo "=== Very Large Batch Size Test Completed ==="
echo "Batch size progression: 4 → 16 → 32 (8x increase from original)"
echo "Check memory usage and training efficiency"