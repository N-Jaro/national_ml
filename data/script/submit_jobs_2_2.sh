#!/bin/bash

# This script reads a list of HUC IDs from a file and submits a separate
# Slurm job for each one to run the GEE data processing pipeline.
#
# --- HOW TO RUN ---
# 1. Get the total number of HUCs from the file:
#    NUM_HUCS=$(wc -l < test_huc_list.txt)
#
# 2. To run all jobs in parallel (up to your cluster's limits):
#    sbatch --array=1-$NUM_HUCS submit_jobs.sh
#
# 3. To run a maximum of 3 jobs concurrently:
#    sbatch --array=1-$NUM_HUCS%3 submit_jobs.sh
#    (The %3 at the end tells Slurm to only run 3 tasks from this array at a time)
#

# --- Slurm Parameters (customized for your cluster) ---
#SBATCH --job-name=GEE_Pipeline    # Job name
#SBATCH --partition=cpu            # Partition (queue) name
#SBATCH --account=bcrm-tgirails    # Your specific account/project
#SBATCH --output=slurm_logs/gee_pipeline_%A_%a.out # Standard output and error log
#SBATCH --nodes=1                  # Run all processes on a single node
#SBATCH --ntasks=1                 # Run a single task
#SBATCH --cpus-per-task=16         # Number of CPU cores per task
#SBATCH --mem=100G                  # Job memory request (e.g., 32GB)
#SBATCH --time=12:00:00            # Time limit hrs:min:sec

# --- Script Logic ---

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

# Define the file containing the list of HUC IDs
HUC_LIST_FILE="test_huc_list_2_padded_2.txt"

# Get the specific HUC ID for this job from the Slurm array index
HUC_ID=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$HUC_LIST_FILE")

echo "======================================================"
echo "Starting Slurm job for HUC ID: $HUC_ID"
echo "Job ID: $SLURM_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "======================================================"

# Activate your Python environment (IMPORTANT!)
# THE FIX: Using the correct path for your conda installation.
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Run the pipeline script, passing the specific HUC ID for this job
# The --hucs argument tells run_pipeline.py which HUC to process
python rerun_pipeline.py --hucs "$HUC_ID"

echo "======================================================"
echo "Finished Slurm job for HUC ID: $HUC_ID"
echo "======================================================"
