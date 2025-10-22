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
#SBATCH --job-name=dem_sanity_full

# Specify the output and error files for each job
#SBATCH --output=/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/slurm_logs/dem_sanity_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/slurm_logs/dem_sanity_%j.err

# Create logs directory if it doesn't exist
mkdir -p /u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/slurm_logs

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Change to script directory
cd /u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity

echo "Starting DEM Sanity Analysis..."
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
    echo "Please update the checkpoint path in this script"
    exit 1
fi

if [ ! -f "$AE_ONLY_CHECKPOINT" ]; then
    echo "ERROR: AlphaEarth-only checkpoint not found: $AE_ONLY_CHECKPOINT"
    echo "Please update the checkpoint path in this script"
    exit 1
fi

# Run the analysis
python dem_sanity_analysis.py \
    --dem-alphaearth-checkpoint "$DEM_AE_CHECKPOINT" \
    --alphaearth-only-checkpoint "$AE_ONLY_CHECKPOINT" \
    --huc-list "../../test_huc_list.txt" \
    --test-data-path "/projects/bcrm/nathanj/data/processed/test/patch_dataset" \
    --output-dir "./dem_sanity_results_67huc" \
    --gaussian-sigma 3.0 \
    --device cuda

echo "DEM Sanity Analysis completed at $(date)"