#!/bin/bash
#SBATCH --job-name=test_dem_alphaearth_1huc
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100GB
#SBATCH --time=2:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=nj7@illinois.edu
#SBATCH --output=/u/nathanj/national_ml/experiments/training/slurm_logs/test_dem_alphaearth_1huc_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/training/slurm_logs/test_dem_alphaearth_1huc_%j.err

# Create logs directory
mkdir -p /u/nathanj/national_ml/experiments/training/slurm_logs

# Load environment
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Set environment variables
export PYTHONPATH="/u/nathanj/national_ml/experiments:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0

echo "=== Single HUC Test: DEM + AlphaEarth ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo ""

# Single HUC test
TRAIN_HUC="03160113"
VAL_HUC="16040204"

echo "Train HUC: $TRAIN_HUC"
echo "Val HUC: $VAL_HUC"
echo "Architecture: DEM (1) + AlphaEarth (64) = 65 channels"
echo ""

cd /u/nathanj/national_ml/experiments/training

python run_lightning_train_dem_alphaearth.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset/ \
    --train_hucs "$TRAIN_HUC" \
    --val_hucs "$VAL_HUC" \
    --batch_size 32 \
    --patience 20 \
    --epochs 5 \
    --lr 1e-5 \
    --precision 16 \
    --alphaearth_channels 64 \
    --wandb_run "dem_alphaearth_1huc_test_$(date +%Y%m%d_%H%M%S)"

echo "=== DEM + AlphaEarth 1HUC test completed ==="