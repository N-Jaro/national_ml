#!/bin/bash
#SBATCH --job-name=test_large_batch_wandb_memory
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --time=01:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nathanj@colorado.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_large_batch_wandb_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_large_batch_wandb_%j.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

echo "=== Large Batch + W&B Memory Monitoring Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Testing batch size: 16 with W&B online logging"
echo "Goal: Monitor GPU memory usage and system metrics"
echo ""

# Print initial GPU info
nvidia-smi

# Test W&B online connectivity
echo ""
echo "=== Setting up W&B Online Logging ==="
wandb login --relogin
wandb online

cd /u/nathanj/national_ml/experiments/training

# Test with larger batch size and W&B online for detailed monitoring
echo ""
echo "=== Starting Large Batch Training with W&B Memory Monitoring ==="
echo "Batch size: 16 (4x larger than original 4)"
echo "Epochs: 2 (to see sustained memory usage)"
echo "W&B project: national_ml_all_modalities_alphaearth"
echo ""

python run_lightning_train_all_modalities_alphaearth.py \
    --hucs "07040006,05040003" \
    --epochs 2 \
    --batch_size 16 \
    --lr 0.001 \
    --val_split 0.5 \
    --wandb_mode online \
    --wandb_project "national_ml_all_modalities_alphaearth" \
    --experiment_name "large_batch_memory_test_$(date +%Y%m%d_%H%M%S)" \
    --early_stopping_patience 10 \
    --num_workers 4

echo ""
echo "=== Large Batch + W&B Memory Test Completed ==="
echo "Check W&B dashboard for detailed GPU memory usage charts"
echo "Look for: System metrics, GPU memory allocation, GPU utilization"