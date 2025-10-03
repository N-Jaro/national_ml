#!/bin/bash

#-----------------------------------------------------------------------------
# Slurm job directives for DEM + Optical model training (single job).
#-----------------------------------------------------------------------------

# Set the name of the job
#SBATCH --job-name=mdmt-dem-optical-single

# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify a single GPU for this job
#SBATCH --gpus=1

# Request a single task (process)
#SBATCH --ntasks=1

# Set the number of CPU cores for the single task
#SBATCH --cpus-per-task=16

# Set the memory for the job
#SBATCH --mem=100G

# Set the maximum time the job will run
#SBATCH --time=48:00:00

# Specify the output and error files for the job
#SBATCH --output=slurm_logs/mdmt-dem-optical-single_%j.out
#SBATCH --error=slurm_logs/mdmt-dem-optical-single_%j.err

#-----------------------------------------------------------------------------
# Script body.
#-----------------------------------------------------------------------------

# Get the current date and time in a concise format (e.g., YYYYMMDD_HHMMSS)
DATE_TIME=$(date +%Y%m%d_%H%M%S)

# Use the DATE_TIME for unique naming
WANDB_NAME="dem_optical_single_${DATE_TIME}"

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

echo "======================================================"
echo "Starting single DEM + Optical Training job"
echo "Job ID: $SLURM_JOB_ID"
echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"

# Activate your Python environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Execute the training command
python run_lightning_train_dem_optical.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset/ \
    --hucs "03160113,19090102,17090011,03040206,18070103,12090302,14010005,10260010,19050105,05040003" \
    --batch_size 32 \
    --patience 20 \
    --epochs 100 \
    --lr 1e-4 \
    --precision 16 \
    --optical_channels 6 \
    --wandb_run "${WANDB_NAME}" 

echo "======================================================"
echo "Finished DEM + Optical training job."
echo "======================================================"