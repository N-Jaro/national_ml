#!/bin/bash
# SLURM Job Array for Prithvi Foundation Model Evaluation
# Runs 5 different Prithvi checkpoints concurrently as separate array tasks
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu

# Job array: 5 tasks (run1, run2, run3, run4, run5) with max 2 concurrent
#SBATCH --array=1

# Each task gets its own GPU
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=200G

# Set the maximum time each array task will run
#SBATCH --time=48:00:00

# Set the name of the job array
#SBATCH --job-name=prithvi_array_eval

# Specify the output and error files for each array task
#SBATCH --output=slurm_logs/prithvi_array_%A_%a.out
#SBATCH --error=slurm_logs/prithvi_array_%A_%a.err

# Prithvi Foundation Model Array Evaluation
# Each array task evaluates one run (checkpoint) on all 67 HUCs

echo "Starting Prithvi Array Task ${SLURM_ARRAY_TASK_ID}/5 at $(date)"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Array Job ID: ${SLURM_ARRAY_JOB_ID}"
echo "Array Task ID: ${SLURM_ARRAY_TASK_ID}"
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

# Map array task ID to run name
# RUN_NAME="run${SLURM_ARRAY_TASK_ID}"
RUN_NAME="run3"  # Temporary fix to always use run5 for testing
echo "Processing: $RUN_NAME"

# Get checkpoint path from config file
CONFIG_FILE="/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config/prithvi_runs.txt"
CHECKPOINT_PATH=$(grep "^${RUN_NAME}|" "$CONFIG_FILE" | cut -d'|' -f2)

if [ -z "$CHECKPOINT_PATH" ] || [ ! -f "$CHECKPOINT_PATH" ]; then
    echo "ERROR: Checkpoint not found for $RUN_NAME"
    echo "Expected path: $CHECKPOINT_PATH"
    echo "Available runs in config:"
    grep -v "^#" "$CONFIG_FILE"
    exit 1
fi

echo "Checkpoint: $CHECKPOINT_PATH"
echo "Starting evaluation of 67 HUCs for $RUN_NAME..."

# Run direct evaluation (no subprocess overhead)
python ultra_fast_prithvi_evaluator.py \
    --checkpoint "$CHECKPOINT_PATH" \
    --data_path "/projects/bcrm/nathanj/data/processed/test/patch_dataset" \
    --output_dir "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results" \
    --batch_size 32 \
    --device cuda \
    --huc_file "/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt" \
    --run_name "$RUN_NAME" \
    --wandb_mode online

EVAL_EXIT_CODE=$?

if [ $EVAL_EXIT_CODE -eq 0 ]; then
    echo "SUCCESS: $RUN_NAME evaluation completed successfully!"
    echo "Results saved to: ultra_fast_results/prithvi_${RUN_NAME}_*.csv"
else
    echo "ERROR: $RUN_NAME evaluation failed with exit code $EVAL_EXIT_CODE"
fi

echo "Array task ${SLURM_ARRAY_TASK_ID} completed at $(date)"