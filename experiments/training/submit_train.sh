#!/bin/bash

#-----------------------------------------------------------------------------
# Slurm job directives.
# Run the entire training process as a single, large job.
#-----------------------------------------------------------------------------

# Set the name of the job
#SBATCH --job-name=mdmt-train

# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify a single GPU for this job
#SBATCH --gpus=1

# Request a single task (process)
#SBATCH --ntasks=1

# Set the number of CPU cores for the single task
# Corresponds to num_workers in your Python script
#SBATCH --cpus-per-task=16

# Set the memory for the job
#SBATCH --mem=50G

# Set the maximum time the job will run
#SBATCH --time=48:00:00

# Specify the output and error files for the job
# %j will be replaced with the job ID
#SBATCH --output=slurm_logs/mdmt-train_%j.out
#SBATCH --error=slurm_logs/mdmt-train_%j.err

#-----------------------------------------------------------------------------
# Script body.
# This part of the script is executed on the compute node.
#-----------------------------------------------------------------------------

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

echo "======================================================"
echo "Starting Slurm job for MDMT Training"
echo "Job ID: $SLURM_JOB_ID"
echo "======================================================"

# Activate your Python environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Execute the training command with all arguments
# The backslashes allow the command to be split across multiple lines for readability
python run_lightning_train.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset/ \
    --hucs "03160113,19090102,17090011,03040206,18070103,12090302,14010005,10260010,19050105,05040003,17100206,11020004,19020504,19050401,03050108,10170204,07140202,12070101,10120203,18020151,12090202,07040006,05080002,18020126,08020205,18020111,13060003,18070107,07130003,17110012,03030005,04060102,17010203,14060004,19080302,07130004,19080204,10270104,12090104,13070007,03160106,07030005,05090104,13040209,17060109,16040204,08020301,10130306,18080003,07120005" \
    --batch_size 32 \
    --patience 20 \
    --epochs 500 \
    --lr 1e-5 \
    --precision 16 \
    --wandb_run "combineFL_ls6b_09222025_2"

echo "======================================================"
echo "Finished training job."
echo "======================================================"