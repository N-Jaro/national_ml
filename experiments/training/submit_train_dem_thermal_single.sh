#!/bin/bash
#SBATCH --job-name=train_dem_thermal_single
#SBATCH --output=slurm_logs_dem_thermal/slurm_single_%j.out
#SBATCH --error=slurm_logs_dem_thermal/slurm_single_%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100G
#SBATCH --gres=gpu:1
#SBATCH --time=12:00:00
#SBATCH --partition=gpu

# Create log directory
mkdir -p slurm_logs_dem_thermal

# Print job information
echo "Job ID: $SLURM_JOB_ID"
echo "Running on node: $(hostname)"
echo "Current directory: $(pwd)"
echo "Date: $(date)"

# Load modules (adjust based on your system)
module load cuda/11.8
module load anaconda3

# Activate conda environment
source activate national_ml

# Print GPU information
echo "GPU information:"
nvidia-smi

# Set W&B settings
export WANDB_PROJECT="national_ml_dem_thermal"
export WANDB_JOB_TYPE="single_train"

# HUC codes for training (smaller subset for single job testing)
HUC_CODES="03030005,03040206,03050101"

# Training parameters (shorter training for single job)
BATCH_SIZE=32
EPOCHS=100
LEARNING_RATE=1e-4
PATIENCE=20
NUM_WORKERS=8

# Model parameters
BASE_CHANNELS=64
N_CLASSES_TASK1=2
N_CLASSES_TASK2=10

# Loss scaling
WATER_LOSS_SCALE=1.0
D8_LOSS_SCALE=0.5

# Training configuration
PRECISION="32"
OPTIMIZER="Adam"
VAL_SPLIT=0.2

# Create unique run name
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RUN_NAME="dem_thermal_single_${SLURM_JOB_ID}_${TIMESTAMP}"

echo "Starting training with the following parameters:"
echo "  HUC codes: $HUC_CODES"
echo "  Batch size: $BATCH_SIZE"
echo "  Epochs: $EPOCHS"
echo "  Learning rate: $LEARNING_RATE"
echo "  Run name: $RUN_NAME"
echo "  Base channels: $BASE_CHANNELS"
echo "  Water loss scale: $WATER_LOSS_SCALE"
echo "  D8 loss scale: $D8_LOSS_SCALE"

# Run training
python run_lightning_train_dem_thermal.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset \
    --hucs "$HUC_CODES" \
    --batch_size $BATCH_SIZE \
    --epochs $EPOCHS \
    --lr $LEARNING_RATE \
    --patience $PATIENCE \
    --num_workers $NUM_WORKERS \
    --val_split $VAL_SPLIT \
    --base_channels $BASE_CHANNELS \
    --n_classes_task1 $N_CLASSES_TASK1 \
    --n_classes_task2 $N_CLASSES_TASK2 \
    --precision $PRECISION \
    --optimizer $OPTIMIZER \
    --water_loss_scale $WATER_LOSS_SCALE \
    --d8_loss_scale $D8_LOSS_SCALE \
    --wandb_project "national_ml_dem_thermal" \
    --wandb_run "$RUN_NAME" \
    --wandb_mode online \
    --gpus 1 \
    --seed 42

echo "Training completed at: $(date)"

# Print some system stats
echo "Final GPU memory usage:"
nvidia-smi

echo "Job completed successfully!"