#!/bin/bash
# Quick test script for single HUC evaluation on GPU node
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=100G
#SBATCH --time=1:00:00
#SBATCH --job-name=test_foundation_eval
#SBATCH --output=slurm_logs/test_eval_%j.out
#SBATCH --error=slurm_logs/test_eval_%j.err

echo "Starting foundation model evaluation test at $(date)"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Node: $SLURMD_NODENAME"
echo "GPU: $CUDA_VISIBLE_DEVICES"

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Change to evaluation directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation

# Create log directory
mkdir -p slurm_logs

# Verify environment
echo "Python: $(which python)"
echo "CUDA available: $(python -c 'import torch; print(torch.cuda.is_available())')"

# Test Clay evaluator with GPU
echo "Testing Clay evaluator on GPU..."
python ultra_fast_clay_evaluator.py \
  --checkpoint "/u/nathanj/national_ml/experiments/foundational_model_experiment/clay/outputs/models/clay_9ch_20251005_100452_run1/checkpoints/epoch=58-val_loss=0.2915.ckpt" \
  --data_path "/projects/bcrm/nathanj/data/processed/test/patch_dataset" \
  --output_dir "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results" \
  --batch_size 32 \
  --device cuda \
  --huc_file "test_huc.txt" \
  --run_name "clay_gpu_test" \
  --wandb_mode disabled

CLAY_EXIT_CODE=$?

# Test Prithvi evaluator with GPU
echo "Testing Prithvi evaluator on GPU..."
python ultra_fast_prithvi_evaluator.py \
  --checkpoint "/u/nathanj/national_ml/experiments/foundational_model_experiment/prithvi/outputs/models/prithvi_9ch_20251004_223048_run7/checkpoints/epoch=28-val_loss=0.2528.ckpt" \
  --data_path "/projects/bcrm/nathanj/data/processed/test/patch_dataset" \
  --output_dir "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results" \
  --batch_size 32 \
  --device cuda \
  --huc_file "test_huc.txt" \
  --run_name "prithvi_gpu_test" \
  --wandb_mode disabled

PRITHVI_EXIT_CODE=$?

# Report results
echo "Test completed at $(date)"
echo "Clay evaluator exit code: $CLAY_EXIT_CODE"
echo "Prithvi evaluator exit code: $PRITHVI_EXIT_CODE"

if [ $CLAY_EXIT_CODE -eq 0 ] && [ $PRITHVI_EXIT_CODE -eq 0 ]; then
    echo "SUCCESS: Both evaluators passed GPU testing!"
else
    echo "FAILURE: One or both evaluators failed"
fi