#!/bin/bash

# Set the name of the job
#SBATCH --job-name=eval_mdmt_batch_runs

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

# Specify the output and error files for each job
#SBATCH --output=/u/nathanj/national_ml/experiments/evaluation/logs/mdmt_batch_eval_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/evaluation/logs/mdmt_batch_eval_%j.err


# Create logs directory if it doesn't exist
mkdir -p /u/nathanj/national_ml/experiments/evaluation/logs

# Activate your Python environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Change to project directory
cd /u/nathanj/national_ml/experiments/evaluation/

# Print job info
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Start time: $(date)"
echo "Working directory: $(pwd)"

# Run batch evaluation
python batch_mdmt_evaluator.py \
    --variants alphaearth dem_thermal dem_sar dem_optical landsat6b \
    --output-dir /u/nathanj/national_ml/experiments/evaluation/batch_results \
    --max-runs 10

echo "End time: $(date)"
echo "Job completed successfully!"