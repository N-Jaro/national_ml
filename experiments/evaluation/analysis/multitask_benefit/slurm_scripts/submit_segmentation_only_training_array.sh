#!/bin/bash

#-----------------------------------------------------------------------------
# Slurm job array directives for Segmentation-Only DEM + AlphaEarth model training.
# Following the exact same pattern as submit_train_dem_alphaearth_array.sh
#-----------------------------------------------------------------------------

# Set the name of the job array
#SBATCH --job-name=mdmt-segonly-dem-alphaearth-array

# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify job array (e.g., 10 jobs for statistical analysis)
#SBATCH --array=1-10

# Specify a single GPU for each job
#SBATCH --gpus=1

# Request a single task (process) per job
#SBATCH --ntasks=1

# Set the number of CPU cores for each task
#SBATCH --cpus-per-task=16

# Set the memory for each job (production config: batch_size=32, full precision)
#SBATCH --mem=180G

# Set the maximum time each job will run (500 epochs with early stopping)
#SBATCH --time=72:00:00

# Specify the output and error files for each job in the array
#SBATCH --output=slurm_logs_segmentation_training/mdmt-segonly-dem-alphaearth-array_%A_%a.out
#SBATCH --error=slurm_logs_segmentation_training/mdmt-segonly-dem-alphaearth-array_%A_%a.err

#-----------------------------------------------------------------------------
# Script body.
#-----------------------------------------------------------------------------

# Get the current date and time in a concise format (e.g., YYYYMMDD_HHMMSS)
DATE_TIME=$(date +%Y%m%d_%H%M%S)

# Use the SLURM_ARRAY_TASK_ID and DATE_TIME for unique naming
WANDB_NAME="segonly_dem_alphaearth_${DATE_TIME}_run${SLURM_ARRAY_TASK_ID}"

# Create the log directory if it doesn't exist
mkdir -p slurm_logs_segmentation_training

echo "======================================================"
echo "Starting Array Segmentation-Only DEM + AlphaEarth Training job"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"  
echo "WandB Run Name: $WANDB_NAME"
echo "Purpose: Train single-task baseline for multitask benefit analysis"
echo "======================================================"

# Activate your Python environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Production HUC-level train/val split configuration (matching All Modalities setup)
TRAIN_HUCS="03160113,19090102,17090011,03040206,18070103,12090302,14010005,10260010,19050105,05040003,17100206,11020004,19020504,19050401,03050108,10170204,07140202,12070101,10120203,18020151,12090202,07040006,05080002,18020126,08020205,18020111,13060003,18070107,07130003,17110012,03030005,04060102,17010203,14060004,19080302,07130004,19080204,10270104,12090104,13070007,03160106,07030005,05090104,13040209,17060109"
VAL_HUCS="16040204,08020301,10130306,18080003,07120005"

echo "=== Production HUC-level Train/Val Split ==="
echo "Train HUCs: 45 HUCs (matches All Modalities training)"
echo "Val HUCs: 5 HUCs (held-out regions)"
echo "=== Production Training Configuration ==="
echo "Model: Segmentation-Only DEM + AlphaEarth (64 channels)"
echo "Task: Water segmentation ONLY (no flow direction)"
echo "Batch Size: 32 (production optimized)"
echo "Epochs: 500 (with early stopping patience 15)"
echo "Learning Rate: 1e-4 (foundation model compatible)"
echo "Optimizer: AdamW"
echo "Precision: 32 (full precision for scientific accuracy)"
echo "Array Task: $SLURM_ARRAY_TASK_ID/10"
echo ""

# Navigate to experiments root directory (where imports work correctly)  
cd /u/nathanj/national_ml/experiments

# Run the simplified training script from the training directory
python training/run_segmentation_only_training_simple.py \
    --base_path "/u/nathanj/national_ml/data/processed/patch_dataset" \
    --train_hucs "$TRAIN_HUCS" \
    --val_hucs "$VAL_HUCS" \
    --batch_size 32 \
    --epochs 500 \
    --lr 1e-4 \
    --patience 15 \
    --num_workers 8 \
    --precision "32" \
    --alphaearth_channels 64 \
    --optimizer "AdamW" \
    --seed 222324 \
    --wandb_project "national_ml_segmentation_only_dem_alphaearth" \
    --wandb_run "$WANDB_NAME" \
    --wandb_mode "online" \
    --wandb_dir "./lightning_logs"

echo "======================================================"
echo "Array Segmentation-Only DEM + AlphaEarth Training job completed"
echo "Array Task: $SLURM_ARRAY_TASK_ID"
echo "Completed at: $(date)"
echo "======================================================"