#!/bin/bash
#SBATCH --job-name=train_all_modalities_alphaearth_array
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=200GB
#SBATCH --time=48:00:00
#SBATCH --array=1-5%2
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nj7@illinois.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/slurm_%A_%a.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth/slurm_%A_%a.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs_all_modalities_alphaearth

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

# Print system info
echo "=== SLURM Job Info ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Job name: $SLURM_JOB_NAME"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "GPU: $CUDA_VISIBLE_DEVICES"
echo ""

echo "=== Environment ==="
echo "Python: $(which python)"
echo "Conda env: $CONDA_DEFAULT_ENV"
echo "PyTorch version: $(python -c 'import torch; print(torch.__version__)')"
echo "CUDA available: $(python -c 'import torch; print(torch.cuda.is_available())')"
echo "GPU count: $(python -c 'import torch; print(torch.cuda.device_count())')"
echo ""

# Array of seeds for reproducible experiments
SEEDS=(42 123 456 789 101112 131415 161718 192021 222324 252627)
SEED=${SEEDS[$((SLURM_ARRAY_TASK_ID-1))]}

# Array of learning rates to explore
LRS=(1e-3 5e-4 2e-4 1e-4 5e-5 1e-3 5e-4 2e-4 1e-4 5e-5)
LR=${LRS[$((SLURM_ARRAY_TASK_ID-1))]}

# Fixed batch size 32 (optimized based on H100 memory analysis)
BATCH_SIZE=32

echo "=== Array Configuration ==="
echo "Task ID: $SLURM_ARRAY_TASK_ID"
echo "Seed: $SEED"
echo "Learning rate: $LR"
echo "Batch size: $BATCH_SIZE (fixed at 32)"
echo "AlphaEarth channels: 64 (fixed)"
echo "Total input channels: 73"
echo ""

# Optimal HUC split based on best performance from All Modalities + AlphaEarth training
TRAIN_HUCS="03160113,19090102,17090011,03040206,18070103,12090302,14010005,10260010,19050105,05040003,17100206,11020004,19020504,19050401,03050108,10170204,07140202,12070101,10120203,18020151,12090202,07040006,05080002,18020126,08020205,18020111,13060003,18070107,07130003,17110012,03030005,04060102,17010203,14060004,19080302,07130004,19080204,10270104,12090104,13070007,03160106,07030005,05090104,13040209,17060109"
VAL_HUCS="16040204,08020301,10130306,18080003,07120005"

echo "=== HUC Split (HUC-level train/val) ==="
echo "Train HUCs: 45 HUCs"
echo "Val HUCs: 5 HUCs"
echo "Total: 50 HUCs"
echo ""

# Change to training directory
cd /u/nathanj/national_ml/experiments/training

# Run training with array-specific parameters and explicit HUC split
python run_lightning_train_all_modalities_alphaearth.py \
    --train_hucs "$TRAIN_HUCS" \
    --val_hucs "$VAL_HUCS" \
    --epochs 500 \
    --batch_size $BATCH_SIZE \
    --lr $LR \
    --base_channels 64 \
    --water_loss_scale 1.0 \
    --d8_loss_scale 0.5 \
    --d8_label_smoothing 0.05 \
    --num_workers 8 \
    --precision "32" \
    --wandb_mode "online" \
    --early_stopping_patience 15 \
    --seed $SEED \
    --experiment_name "all_modalities_alphaearth_task${SLURM_ARRAY_TASK_ID}_seed${SEED}_batch32_huc_split"

echo "=== Array job $SLURM_ARRAY_TASK_ID completed at $(date) ==="