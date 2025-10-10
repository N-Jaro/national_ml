#!/bin/bash
#SBATCH --job-name=test_batch32_wandb_memory
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --time=01:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nathanj@colorado.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_batch32_wandb_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_batch32_wandb_%j.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

echo "=== Batch Size 32 + W&B Memory Analysis ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Purpose: Compare memory usage between batch sizes"
echo ""
echo "Previous Results:"
echo "  Batch 4:  ~90 batches/epoch, small memory usage"
echo "  Batch 16: ~23 batches/epoch, ~30% GPU memory"
echo "  Batch 32: ~12 batches/epoch, memory usage = ?"
echo ""

# Print initial GPU info
nvidia-smi

# Setup W&B online connectivity
echo ""
echo "=== Setting up W&B Online Memory Monitoring ==="
wandb login --relogin
wandb online

cd /u/nathanj/national_ml/experiments/training

# Run with batch size 32 and W&B online for detailed memory monitoring
echo ""
echo "=== Starting Batch 32 Training with W&B Memory Monitoring ==="
echo "Configuration:"
echo "  Batch size: 32 (8x larger than original 4)"
echo "  Epochs: 2 (for sustained memory pattern analysis)"
echo "  Expected batches: ~12 per epoch"
echo "  Goal: Monitor peak GPU memory usage and system metrics"
echo ""

python run_lightning_train_all_modalities_alphaearth.py \
    --hucs "07040006,05040003" \
    --epochs 2 \
    --batch_size 32 \
    --lr 0.001 \
    --val_split 0.5 \
    --wandb_mode online \
    --wandb_project "national_ml_all_modalities_alphaearth" \
    --experiment_name "batch32_memory_analysis_$(date +%Y%m%d_%H%M%S)" \
    --early_stopping_patience 10 \
    --num_workers 4

echo ""
echo "=== Batch 32 + W&B Memory Analysis Completed ==="
echo ""
echo "Key Metrics to Check in W&B Dashboard:"
echo "  1. GPU Memory Allocated (Bytes) - peak usage"
echo "  2. GPU Memory Allocated (%) - percentage of 80GB H100"
echo "  3. GPU Utilization (%) - compute efficiency"
echo "  4. GPU Time Spent Accessing Memory (%) - memory bandwidth"
echo "  5. GPU Temperature (°C) - thermal efficiency"
echo ""
echo "Compare with previous batch 16 run to see scaling impact!"