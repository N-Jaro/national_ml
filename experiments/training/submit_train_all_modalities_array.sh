#!/bin/bash

#-----------------------------------------------------------------------------
# Slurm job array for All Modalities (no AlphaEarth) model training with different configurations.
#-----------------------------------------------------------------------------

# Set the name of the job
#SBATCH --job-name=all-modalities-mdmt-array

# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify a single GPU for each array job
#SBATCH --gpus=1

# Request a single task (process) per array job
#SBATCH --ntasks=1

# Set the number of CPU cores for each task
#SBATCH --cpus-per-task=16

# Set the memory for each job
#SBATCH --mem=100G

# Set the maximum time each job will run
#SBATCH --time=48:00:00

# Job array with 10 runs, but only 4 concurrent at a time
#SBATCH --array=1-5%3

# Specify the output and error files for each job
#SBATCH --output=slurm_logs/mdmt-all-modalities-array_%A_%a.out
#SBATCH --error=slurm_logs/mdmt-all-modalities-array_%A_%a.err

#-----------------------------------------------------------------------------
# Script body.
#-----------------------------------------------------------------------------

# Get the current date and time in a concise format (e.g., YYYYMMDD_HHMMSS)
# This will be the same for all array tasks submitted at the same time.
DATE_TIME=$(date +%Y%m%d_%H%M%S)

# Use the DATE_TIME and SLURM_ARRAY_TASK_ID variables for unique naming
WANDB_NAME="all_modalities_${DATE_TIME}_run${SLURM_ARRAY_TASK_ID}"

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

echo "======================================================"
echo "Starting All Modalities (no AlphaEarth) Training Array Job"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Architecture: DEM(1) + Optical(6) + Thermal(1) + SAR(1) = 9 channels"
echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"

# Activate your Python environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Set batch size to 32 (optimized based on H100 memory analysis)
BATCH_SIZE=32

echo "Using batch size: $BATCH_SIZE for 9-channel All Modalities model"

# HUC split for consistent train/validation across all variants
TRAIN_HUCS="03160113,19090102,17090011,03040206,18070103,12090302,14010005,10260010,19050105,05040003,17100206,11020004,19020504,19050401,03050108,10170204,07140202,12070101,10120203,18020151,12090202,07040006,05080002,18020126,08020205,18020111,13060003,18070107,07130003,17110012,03030005,04060102,17010203,14060004,19080302,07130004,19080204,10270104,12090104,13070007,03160106,07030005,05090104,13040209,17060109"
VAL_HUCS="16040204,08020301,10130306,18080003,07120005"

echo "=== HUC Split (HUC-level train/val) ==="
echo "Train HUCs: 45 HUCs"
echo "Val HUCs: 5 HUCs"
echo "Total: 50 HUCs"
echo ""

# Execute the training command with explicit HUC split
python run_lightning_train_all_modalities.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset/ \
    --train_hucs "$TRAIN_HUCS" \
    --val_hucs "$VAL_HUCS" \
    --batch_size $BATCH_SIZE \
    --early_stopping_patience 20 \
    --epochs 500 \
    --lr 1e-4 \
    --precision 16 \
    --experiment_name "${WANDB_NAME}_batch32_huc_split" 

echo "======================================================"
echo "Finished All Modalities (no AlphaEarth) training array job."
echo "Architecture: DEM(1) + Optical(6) + Thermal(1) + SAR(1) = 9 channels"
echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"