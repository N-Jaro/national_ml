#!/bin/bash

#-----------------------------------------------------------------------------
# Slurm job array for SATLAS pretrained foundation model training with 9-channel input.
# Uses official SATLAS pretrained weights via satlaspretrain-models package.
#-----------------------------------------------------------------------------

# Set the name of the job
#SBATCH --job-name=satlas-pretrained-array

# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify a single GPU for each array job
#SBATCH --gpus=1

# Request a single task (process) per array job
#SBATCH --ntasks=1

# Set the number of CPU cores for each task
#SBATCH --cpus-per-task=16

# Set the memory for each job (optimized for 90M parameter SATLAS model)
#SBATCH --mem=128G

# Set the maximum time each job will run (foundation models need more time)
#SBATCH --time=72:00:00

# Job array with 5 runs, but only 3 concurrent at a time (foundation models are memory intensive)
#SBATCH --array=1-5%3

# Specify the output and error files for each job
#SBATCH --output=slurm_logs/satlas-pretrained-array_%A_%a.out
#SBATCH --error=slurm_logs/satlas-pretrained-array_%A_%a.err

#-----------------------------------------------------------------------------
# Script body.
#-----------------------------------------------------------------------------

# Get the current date and time in a concise format (e.g., YYYYMMDD_HHMMSS)
# This will be the same for all array tasks submitted at the same time.
DATE_TIME=$(date +%Y%m%d_%H%M%S)

# Use the DATE_TIME and SLURM_ARRAY_TASK_ID variables for unique naming
WANDB_NAME="satlas_pretrained_${DATE_TIME}_run${SLURM_ARRAY_TASK_ID}"

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

# Set 9-channel configuration (DEM + 6×Optical + Thermal + SAR)
INPUT_CHANNELS=9
MODEL_TYPE="satlas_sentinel2_swinb_mi_ms"

echo "======================================================"
echo "Starting SATLAS Pretrained Foundation Model Training"
echo "======================================================"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Input Channels: $INPUT_CHANNELS (DEM + 6×Optical + Thermal + SAR)"
echo "Foundation Model: $MODEL_TYPE"
echo "Pretrained Weights: Official SATLAS via satlaspretrain-models"
echo "Model Parameters: 90M (89.6M backbone + 412K segmentation head)"
echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"

# Activate terratorch environment (has satlaspretrain-models installed)
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Set up WandB with unique run naming
export WANDB_PROJECT="satlas_water_segmentation"
export WANDB_RUN_ID="${WANDB_NAME}"
export WANDB_NAME="${WANDB_NAME}"

echo "WandB Configuration:"
echo "  Project: $WANDB_PROJECT"
echo "  Run Name: $WANDB_NAME"

# Navigate to SATLAS experiment directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/satlas

# Execute the SATLAS pretrained training command
python training/train_satlas_satlaspretrain.py \
    --config configs/satlas_pretrained_config.yaml \
    --gpus 1

# Store exit code
EXIT_CODE=$?

echo "======================================================"
echo "Finished SATLAS Pretrained Foundation Model Training"
echo "======================================================"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Model Type: $MODEL_TYPE"
echo "Input Channels: $INPUT_CHANNELS"
echo "WandB Run Name: $WANDB_NAME"
echo "Exit Code: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ SATLAS pretrained training completed successfully"
    echo "✓ Checkpoints saved to: outputs/models/$WANDB_NAME/checkpoints/"
    echo "✓ WandB logs: https://wandb.ai/9bombs/$WANDB_PROJECT/runs/$WANDB_RUN_ID"
else
    echo "✗ SATLAS pretrained training failed with exit code: $EXIT_CODE"
    echo "Check logs: slurm_logs/satlas-pretrained-array_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out"
fi

echo "======================================================"

# Exit with the same code as the training script
exit $EXIT_CODE