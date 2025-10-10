#!/bin/bash
#SBATCH --job-name=test_all_modalities_alphaearth
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --time=01:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nathanj@colorado.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_slurm_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/test_slurm_%j.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

# Print system info
echo "=== SLURM Job Info ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Job name: $SLURM_JOB_NAME"
echo "Node: $(hostname)"
echo "GPU: $CUDA_VISIBLE_DEVICES"
echo "Conda env: $CONDA_DEFAULT_ENV"
echo "Python path: $PYTHONPATH"

# Print GPU info
nvidia-smi

# Test the training pipeline
echo ""
echo "=== Starting All Modalities + AlphaEarth Training Test ==="
echo "Testing HUCs: 07040006 (358 patches), 05040003 (641 patches)"
echo "Mode: Quick test with 1 epoch, small batch size"
echo ""

cd /u/nathanj/national_ml/experiments/training

# Run training with minimal settings for testing
python run_lightning_train_all_modalities_alphaearth.py \
    --hucs "07040006,05040003" \
    --epochs 1 \
    --batch_size 4 \
    --lr 0.001 \
    --val_split 0.5 \
    --wandb_mode offline \
    --experiment_name "test_all_modalities_alphaearth_$(date +%Y%m%d_%H%M%S)" \
    --early_stopping_patience 10 \
    --num_workers 4

echo ""
echo "=== Training test completed ==="
echo "Check the logs above for any issues"