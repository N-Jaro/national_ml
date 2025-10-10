#!/bin/bash

#-----------------------------------------------------------------------------
# Slurm job array for SatLas foundation model training with 9-channel input.
# Adapts pre-trained SatLas weights to 9-channel multi-modal data.
#-----------------------------------------------------------------------------

# Set the name of the job
#SBATCH --job-name=satlas-9ch-array

# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify a single GPU for each array job
#SBATCH --gpus=1

# Request a single task (process) per array job
#SBATCH --ntasks=1

# Set the number of CPU cores for each task
#SBATCH --cpus-per-task=16

# Set the memory for each job (optimized for 9-channel SatLas + foundation model)
#SBATCH --mem=128G

# Set the maximum time each job will run (foundation models need more time)
#SBATCH --time=72:00:00

# Job array with 5 runs, but only 3 concurrent at a time (foundation models are memory intensive)
#SBATCH --array=1-5%3

# Specify the output and error files for each job
#SBATCH --output=slurm_logs/satlas-9ch-array_%A_%a.out
#SBATCH --error=slurm_logs/satlas-9ch-array_%A_%a.err

#-----------------------------------------------------------------------------
# Script body.
#-----------------------------------------------------------------------------

# Get the current date and time in a concise format (e.g., YYYYMMDD_HHMMSS)
# This will be the same for all array tasks submitted at the same time.
DATE_TIME=$(date +%Y%m%d_%H%M%S)

# Use the DATE_TIME and SLURM_ARRAY_TASK_ID variables for unique naming
WANDB_NAME="satlas_9ch_${DATE_TIME}_run${SLURM_ARRAY_TASK_ID}"

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

# Set 9-channel configuration (DEM + 6×Optical + Thermal + SAR)
INPUT_CHANNELS=9
MODEL_TYPE="satlas_swin_b_foundation_model"

echo "======================================================"
echo "Starting SatLas Foundation Model 9-Channel Training"
echo "======================================================"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Input Channels: $INPUT_CHANNELS (DEM + 6×Optical + Thermal + SAR)"
echo "Foundation Model: $MODEL_TYPE"
echo "Pre-trained Weights: Enabled with intelligent adaptation"
echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"

# Activate your Python environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Set batch size for 9-channel foundation model (conservative due to model size)
BATCH_SIZE=4

echo "Using batch size: $BATCH_SIZE for foundation model with $INPUT_CHANNELS channels"
echo "Memory optimization: Mixed precision (bf16) enabled"

# Navigate to SatLas experiment directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/satlas

# Set WandB environment variable for unique run naming
export WANDB_RUN_ID="${WANDB_NAME}"
export WANDB_NAME="${WANDB_NAME}"

# Execute the SatLas training command
python training/train_satlas_structured.py \
    --config configs/satlas_config.yaml \
    --gpus 1

# Store exit code
EXIT_CODE=$?

echo "======================================================"
echo "Finished SatLas 9-channel foundation model training"
echo "======================================================"
echo "Input Channels: $INPUT_CHANNELS"
echo "Foundation Model: $MODEL_TYPE"
echo "WandB Run Name: $WANDB_NAME"
echo "Exit Code: $EXIT_CODE"
echo "======================================================"

# Exit with the same code as the training script
exit $EXIT_CODE