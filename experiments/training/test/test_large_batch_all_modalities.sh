#!/bin/bash
#SBATCH --job-name=test_large_batch_all_modalities
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --time=01:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nathanj@colorado.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_large_batch_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_large_batch_%j.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

echo "=== Testing Large Batch Size for All Modalities + AlphaEarth ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Testing batch size: 16 (up from 4)"
echo ""

# Print GPU info
nvidia-smi

cd /u/nathanj/national_ml/experiments/training

# Test with larger batch size
echo ""
echo "=== Starting training with batch size 16 ==="
python run_lightning_train_all_modalities_alphaearth.py \
    --hucs "07040006,05040003" \
    --epochs 1 \
    --batch_size 16 \
    --lr 0.001 \
    --val_split 0.5 \
    --wandb_mode offline \
    --experiment_name "large_batch_test_$(date +%Y%m%d_%H%M%S)" \
    --early_stopping_patience 10 \
    --num_workers 4

echo ""
echo "=== Large batch size test completed ==="
echo "Check GPU utilization and memory usage in the logs above"