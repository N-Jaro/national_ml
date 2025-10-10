#!/bin/bash
#SBATCH --job-name=prithvi_eval
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --gres=gpu:1
#SBATCH --time=12:00:00
#SBATCH --output=slurm_logs/prithvi_eval_%j.out
#SBATCH --error=slurm_logs/prithvi_eval_%j.err

# Foundation Model Evaluation - Prithvi
# Individual SLURM job for evaluating Prithvi foundation model

echo "Starting Prithvi Foundation Model Evaluation"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Date: $(date)"

# Create log directory
mkdir -p slurm_logs

# Change to evaluation directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation

# Activate conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Verify environment
echo "Python: $(which python)"
echo "CUDA available: $(python -c 'import torch; print(torch.cuda.is_available())')"
echo "GPU devices: $(python -c 'import torch; print(torch.cuda.device_count())')"

# Get checkpoint path from config file (using run5)
CONFIG_FILE="/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config/prithvi_runs.txt"
CHECKPOINT_PATH=$(grep "^run5|" "$CONFIG_FILE" | cut -d'|' -f2)
RUN_NAME="run5"

# Check if checkpoint exists
if [ ! -f "$CHECKPOINT_PATH" ]; then
    echo "Checkpoint not found: $CHECKPOINT_PATH"
    echo "Available runs in config:"
    grep -v "^#" "$CONFIG_FILE"
    exit 1
fi

echo "Using checkpoint: $CHECKPOINT_PATH"
echo "Run name: $RUN_NAME"

# Run evaluation
python ultra_fast_prithvi_evaluator.py \
    --checkpoint "$CHECKPOINT_PATH" \
    --data_path "/u/nathanj/national_ml/data/processed/patch_dataset" \
    --output_dir "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results" \
    --batch_size 32 \
    --device cuda \
    --huc_file "/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt" \
    --run_name "$RUN_NAME" \
    --wandb_mode online

echo "Prithvi evaluation completed at $(date)"