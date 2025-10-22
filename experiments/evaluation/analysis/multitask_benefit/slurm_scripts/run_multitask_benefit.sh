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
#SBATCH --time=2:00:00

# Set the name of the job
#SBATCH --job-name=multitask_benefit

# Specify the output and error files for each job
#SBATCH --output=/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs/multitask_benefit_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs/multitask_benefit_%j.err

# Multitask Benefit Analysis - Representative HUCs
# This analysis compares multitask (seg+flow) vs segmentation-only training
# Purpose: Show multitask learning improves connectivity and hydrological coherence

# Create logs directory if it doesn't exist
mkdir -p /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Change to script directory
cd /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit

echo "Starting Multitask Benefit Analysis..."
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "GPU: $CUDA_VISIBLE_DEVICES"
echo "Date: $(date)"

# Set checkpoint paths - UPDATE WITH YOUR MULTITASK MODEL CHECKPOINT
MULTITASK_CHECKPOINT="/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251004_112528_run2/checkpoints/mdmt-dem-alphaearth-epoch=37-val_loss=0.4154.ckpt"

# Check if checkpoint exists
if [ ! -f "$MULTITASK_CHECKPOINT" ]; then
    echo "ERROR: Multitask checkpoint not found: $MULTITASK_CHECKPOINT"
    echo "Please update the checkpoint path in this script"
    exit 1
fi

# Run the analysis with representative HUCs
python multitask_benefit_analysis.py \
    --multitask-checkpoint "$MULTITASK_CHECKPOINT" \
    --huc-list "../dem_sanity/test_huc_representative.txt" \
    --test-data-path "/projects/bcrm/nathanj/data/processed/test/patch_dataset" \
    --output-dir "./results" \
    --device cuda

echo "Multitask Benefit Analysis completed at $(date)"