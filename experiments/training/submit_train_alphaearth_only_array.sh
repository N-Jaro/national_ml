#!/bin/bash

#-----------------------------------------------------------------------------
# Slurm job array for AlphaEarth-only model training with different configurations.
#-----------------------------------------------------------------------------

# Set the name of the job
#SBATCH --job-name=alphaearth-mdmt-only-array

# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify a single GPU for each array job
#SBATCH --gpus=1

# Request a single task (process) per array job
#SBATCH --ntasks=1

# Set the number of CPU cores for each task
#SBATCH --cpus-per-task=16

# Set the memory for each job (increased for 64-channel AlphaEarth inputs)
#SBATCH --mem=200G

# Set the maximum time each job will run
#SBATCH --time=48:00:00

# Job array with 10 runs, but only 4 concurrent at a time
#SBATCH --array=1-10%4

# Specify the output and error files for each job
#SBATCH --output=slurm_logs/mdmt-alphaearth-only-array_%A_%a.out
#SBATCH --error=slurm_logs/mdmt-alphaearth-only-array_%A_%a.err

#-----------------------------------------------------------------------------
# Script body.
#-----------------------------------------------------------------------------

# Get the current date and time in a concise format (e.g., YYYYMMDD_HHMMSS)
# This will be the same for all array tasks submitted at the same time.
DATE_TIME=$(date +%Y%m%d_%H%M%S)

# Use the DATE_TIME and SLURM_ARRAY_TASK_ID variables for unique naming
WANDB_NAME="alphaearth_only_${DATE_TIME}_run${SLURM_ARRAY_TASK_ID}"

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

# Set AlphaEarth channels (fixed at 64 for actual data)
ALPHAEARTH_CHANNELS=64

echo "======================================================"
echo "Starting AlphaEarth-only Training Array Job"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "AlphaEarth Channels: $ALPHAEARTH_CHANNELS"
echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"

# Activate your Python environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Set batch size for 64-channel AlphaEarth data
BATCH_SIZE=16

echo "Using batch size: $BATCH_SIZE for $ALPHAEARTH_CHANNELS channels"

# Execute the training command
python run_lightning_train_alphaearth_only.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset/ \
    --hucs "03160113,19090102,17090011,03040206,18070103,12090302,14010005,10260010,19050105,05040003,17100206,11020004,19020504,19050401,03050108,10170204,07140202,12070101,10120203,18020151,12090202,07040006,05080002,18020126,08020205,18020111,13060003,18070107,07130003,17110012,03030005,04060102,17010203,14060004,19080302,07130004,19080204,10270104,12090104,13070007,03160106,07030005,05090104,13040209,17060109,16040204,08020301,10130306,18080003,07120005" \
    --batch_size $BATCH_SIZE \
    --patience 20 \
    --epochs 500 \
    --lr 1e-5 \
    --precision 16 \
    --alphaearth_channels $ALPHAEARTH_CHANNELS \
    --wandb_run "${WANDB_NAME}" 

echo "======================================================"
echo "Finished AlphaEarth-only training array job."
echo "AlphaEarth Channels: $ALPHAEARTH_CHANNELS"
echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"