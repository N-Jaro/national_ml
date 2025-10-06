#!/bin/bash
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

# Set the name of the job
#SBATCH --job-name=eval_mdmt_ds_st_allmodal

# Specify the output and error files for each job
#SBATCH --output=logs/eval_alphaearth_%j.out
#SBATCH --error=logs/eval_alphaearth_%j.err

# MDMT Automated Evaluation - alphaearth variant
# This script runs ultra-fast evaluators for the alphaearth variant automatically

echo "Starting MDMT Automated Evaluation at $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "GPU: $CUDA_VISIBLE_DEVICES"

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Change to evaluation directory
cd /u/nathanj/national_ml/experiments/evaluation

# Create logs directory if it doesn't exist
mkdir -p logs

# Run automated evaluation
echo "========================================"
echo "Starting automated evaluation for all variants"
echo "Using full test HUC list (67 HUCs)"
echo "========================================"

python run_ultrafast_evaluations.py --huc-list test_huc_list.txt --variant alphaearth

# the varinants options are: 
# dem_only, landsat6b, dem_optical, dem_sar, dem_thermal, dem_alphaearth, alphaearth

echo "========================================"
echo "Automated evaluation completed at $(date)"
echo "========================================"
