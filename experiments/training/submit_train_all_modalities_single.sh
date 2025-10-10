#!/bin/bash
#SBATCH --job-name=train_all_modalities_single
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --time=24:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nathanj@colorado.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs/slurm_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs/slurm_%j.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs

# Get the current date and time
DATE_TIME=$(date +%Y%m%d_%H%M%S)

# Use the DATE_TIME for unique naming
WANDB_NAME="all_modalities_single_${DATE_TIME}"

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

echo "======================================================"
echo "Starting single All Modalities (no AlphaEarth) Training job"
echo "Job ID: $SLURM_JOB_ID"
echo "Architecture: DEM(1) + Optical(6) + Thermal(1) + SAR(1) = 9 channels"
echo "WandB Run Name: $WANDB_NAME"
echo "======================================================"

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

# Print system info
echo "=== SLURM Job Info ==="
echo "Job ID: $SLURM_JOB_ID"
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

# Change to training directory
cd /u/nathanj/national_ml/experiments/training

# Modern HUC-level train/val split configuration
TRAIN_HUCS="03160113,19090102,17090011,03040206,18070103,12090302,14010005,10260010,19050105,05040003,17100206,11020004,19020504,19050401,03050108,10170204,07140202,12070101,10120203,18020151,12090202,07040006,05080002,18020126,08020205,18020111,13060003,18070107,07130003,17110012,03030005,04060102,17010203,14060004,19080302,07130004,19080204,10270104,12090104,13070007,03160106,07030005,05090104,13040209,17060109"
VAL_HUCS="16040204,08020301,10130306,18080003,07120005"

echo "=== HUC-level Train/Val Split ==="
echo "Train HUCs: 45 HUCs"
echo "Val HUCs: 5 HUCs"
echo "Total: 50 HUCs"
echo ""

# Execute the training command with HUC-level splitting
python run_lightning_train_all_modalities.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset/ \
    --train_hucs "$TRAIN_HUCS" \
    --val_hucs "$VAL_HUCS" \
    --batch_size 32 \
    --epochs 100 \
    --lr 1e-4 \
    --base_channels 64 \
    --water_loss_scale 1.0 \
    --d8_loss_scale 0.5 \
    --num_workers 8 \
    --precision "16" \
    --wandb_mode "online" \
    --early_stopping_patience 20 \
    --experiment_name "${WANDB_NAME}_huc_split" \
    --seed 42

echo "======================================================"
echo "Finished All Modalities (no AlphaEarth) training job."
echo "======================================================"