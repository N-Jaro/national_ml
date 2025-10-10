#!/bin/bash
#SBATCH --job-name=satlas-fixed-train
#SBATCH --output=slurm_logs/fixed_satlas_train_%j.out
#SBATCH --error=slurm_logs/fixed_satlas_train_%j.err
#SBATCH --time=48:00:00
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G

echo "=============================================="
echo "Fixed SATLAS Pretrained Foundation Model Training"
echo "=============================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start time: $(date)"
echo "=============================================="

# Set up environment
export CUDA_VISIBLE_DEVICES=0
export WANDB_MODE=online
export WANDB_PROJECT=satlas_water_segmentation

# Create a unique run name with timestamp and "fixed" tag
export WANDB_NAME="satlas_pretrained_fixed_$(date +%Y%m%d_%H%M%S)_run1"

echo "Environment Configuration:"
echo "  CUDA_VISIBLE_DEVICES: $CUDA_VISIBLE_DEVICES"
echo "  WANDB_MODE: $WANDB_MODE"
echo "  WANDB_PROJECT: $WANDB_PROJECT"
echo "  WANDB_NAME: $WANDB_NAME"
echo "=============================================="

# Activate environment
source ~/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Navigate to experiment directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/satlas

# Create logs directory
mkdir -p slurm_logs

# Run fixed training
echo "Starting fixed SATLAS training..."
python training/train_satlas_satlaspretrain.py \
    --config configs/satlas_pretrained_config.yaml \
    --gpus 1

echo "=============================================="
echo "Training completed at: $(date)"
echo "=============================================="