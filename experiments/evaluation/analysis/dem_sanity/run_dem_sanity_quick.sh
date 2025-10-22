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

# Set the maximum time each job will run (500 epochs with early stopping)
#SBATCH --time=24:00:00

# Set the name of the job
#SBATCH --job-name=dem_sanity_quick

# Specify the output and error files for each job
#SBATCH --output=/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/slurm_logs/dem_sanity_quick_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/slurm_logs/dem_sanity_quick_%j.err

# DEM Sanity Analysis - Quick Version (Representative 10 HUCs)
# Runtime: ~30-45 minutes
# Purpose: Prove topographic signal with statistical significance

# Create logs directory if it doesn't exist
mkdir -p /u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/slurm_logs

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Change to script directory
cd /u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity

echo "Starting DEM Sanity Analysis (Quick - Representative HUCs)..."
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "GPU: $CUDA_VISIBLE_DEVICES"
echo "Date: $(date)"

# Set checkpoint paths - ACTUAL TRAINED MODEL CHECKPOINTS
DEM_AE_CHECKPOINT="/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251004_112528_run2/checkpoints/mdmt-dem-alphaearth-epoch=37-val_loss=0.4154.ckpt"
AE_ONLY_CHECKPOINT="/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251003_162616_run10/checkpoints/mdmt-alphaearth-only-epoch=29-val_loss=0.8005.ckpt"

# Check if checkpoints exist
if [ ! -f "$DEM_AE_CHECKPOINT" ]; then
    echo "ERROR: DEM+AlphaEarth checkpoint not found: $DEM_AE_CHECKPOINT"
    exit 1
fi

if [ ! -f "$AE_ONLY_CHECKPOINT" ]; then
    echo "ERROR: AlphaEarth-only checkpoint not found: $AE_ONLY_CHECKPOINT"
    exit 1
fi

# Run the analysis with representative HUCs
python dem_sanity_analysis.py \
    --dem-alphaearth-checkpoint "$DEM_AE_CHECKPOINT" \
    --alphaearth-only-checkpoint "$AE_ONLY_CHECKPOINT" \
    --huc-list "./test_huc_representative.txt" \
    --test-data-path "/projects/bcrm/nathanj/data/processed/test/patch_dataset" \
    --output-dir "./dem_sanity_results" \
    --gaussian-sigma 3.0 \
    --device cuda

echo "DEM Sanity Analysis (Quick) completed at $(date)"