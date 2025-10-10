#!/bin/bash

#SBATCH --job-name=satlas_foundation_training
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=48:00:00
#SBATCH --output=slurm_logs/satlas_foundation_%j.out
#SBATCH --error=slurm_logs/satlas_foundation_%j.err

# Create log directory
mkdir -p slurm_logs

echo "======================================================"
echo "Starting SATLAS Foundation Model Training"
echo "======================================================"
echo "Job ID: $SLURM_JOB_ID"
echo "Date: $(date)"
echo "Host: $(hostname)"
echo "======================================================"

# Activate environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Navigate to SATLAS directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/satlas

# Set unique experiment name with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
export WANDB_RUN_ID="satlas_foundation_${TIMESTAMP}_${SLURM_JOB_ID}"
export WANDB_NAME="satlas_foundation_${TIMESTAMP}"

echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"

# Run production SATLAS foundation model training
python train_satlas_foundation.py \
    --config configs/satlas_config.yaml \
    --gpus 1 \
    --synthetic

EXIT_CODE=$?

echo "======================================================"
echo "SATLAS Foundation Model Training Completed"
echo "======================================================"
echo "Exit Code: $EXIT_CODE"
echo "Date: $(date)"
echo "======================================================"

exit $EXIT_CODE