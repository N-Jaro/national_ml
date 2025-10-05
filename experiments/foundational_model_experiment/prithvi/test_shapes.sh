#!/bin/bash

#SBATCH --job-name=prithvi-test
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=1:00:00
#SBATCH --output=slurm_logs/prithvi-test_%j.out
#SBATCH --error=slurm_logs/prithvi-test_%j.err

echo "🧪 Quick Prithvi Test with Shape Debugging"
echo "=========================================="

# Activate environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Navigate to directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/prithvi

# Run quick test with debug config
python training/train_prithvi.py --config configs/prithvi_test_config.yaml --test

echo "Test completed!"