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
#SBATCH --time=4:00:00

# Set the name of the job
#SBATCH --job-name=multitask_benefit_reversed

# Specify the output and error files for each job
#SBATCH --output=/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs_multitask_benefit/multitask_benefit_reversed_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs_multitask_benefit/multitask_benefit_reversed_%j.err

# Multitask Benefit Analysis - REVERSED CHECKPOINTS
# This analysis tests what happens when we reverse the checkpoint assignments
# Purpose: Understand if performance difference is due to training approach or checkpoint quality

# Create logs directory if it doesn't exist
mkdir -p /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs_multitask_benefit

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Change to script directory
cd /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit

echo "Starting Multitask Benefit Analysis - REVERSED CHECKPOINTS..."
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "GPU: $CUDA_VISIBLE_DEVICES"
echo "Date: $(date)"

# REVERSED: Set checkpoint paths - swapping the assignments
# What was "multitask" checkpoint now used for "segmentation-only" evaluation
# What was "segmentation-only" checkpoint now used for "multitask" evaluation
MULTITASK_CHECKPOINT="wandb_logs/dem_alphaearth_segonly_seed_222324_production/checkpoints/mdmt-dem-alphaearth-segonly-epoch=18-val_loss=0.2384.ckpt"
SEGONLY_CHECKPOINT="/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251004_112528_run3/checkpoints/mdmt-dem-alphaearth-epoch=39-val_loss=0.3846.ckpt"

# Check if checkpoints exist
if [ ! -f "$MULTITASK_CHECKPOINT" ]; then
    echo "ERROR: Multitask checkpoint not found: $MULTITASK_CHECKPOINT"
    exit 1
fi

if [ ! -f "$SEGONLY_CHECKPOINT" ]; then
    echo "ERROR: Segmentation-only checkpoint not found: $SEGONLY_CHECKPOINT"
    exit 1
fi

echo "REVERSED CHECKPOINT ASSIGNMENT:"
echo "  Multitask evaluation uses: $MULTITASK_CHECKPOINT (originally segmentation-only trained)"
echo "  Segmentation-only evaluation uses: $SEGONLY_CHECKPOINT (originally multitask trained)"

# Run the analysis with full dataset (no batch limit)
python multitask_benefit_analysis.py \
    --multitask-checkpoint "$MULTITASK_CHECKPOINT" \
    --segmentation-only-checkpoint "$SEGONLY_CHECKPOINT" \
    --huc-list "/u/nathanj/national_ml/experiments/evaluation/test_huc_small.txt" \
    --test-data-path "/projects/bcrm/nathanj/data/processed/test/patch_dataset" \
    --output-dir "./results_reversed" \
    --device cuda

echo "Multitask Benefit Analysis (REVERSED) completed at $(date)"