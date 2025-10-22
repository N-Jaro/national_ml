#!/bin/bash
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=180G
#SBATCH --job-name=seg_only_dem_alphaearth
#SBATCH --time=72:00:00
#SBATCH --output=/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs/slurm_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs/slurm_%j.err

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Create log directory
mkdir -p /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs

# Navigate to experiments root directory (where imports work correctly)
cd /u/nathanj/national_ml/experiments

TRAIN_HUCS="03160113,19090102,17090011,03040206,18070103,12090302,14010005,10260010,19050105,05040003,17100206,11020004,19020504,19050401,03050108,10170204,07140202,12070101,10120203,18020151,12090202,07040006,05080002,18020126,08020205,18020111,13060003,18070107,07130003,17110012,03030005,04060102,17010203,14060004,19080302,07130004,19080204,10270104,12090104,13070007,03160106,07030005,05090104,13040209,17060109"
VAL_HUCS="16040204,08020301,10130306,18080003,07120005"

# Production training configuration matching All Modalities setup exactly
python training/run_lightning_train_dem_alphaearth_segonly.py \
    --train_hucs "$TRAIN_HUCS" \
    --val_hucs "$VAL_HUCS" \
    --batch_size 32 \
    --epochs 500 \
    --lr 1e-4 \
    --patience 50 \
    --num_workers 16 \
    --precision bf16 \
    --alphaearth_channels 64 \
    --water_loss_scale 1.0 \
    --d8_loss_scale 0.0 \
    --no_dynamic_weighter \
    --optimizer AdamW \
    --wandb_project "national_ml_segmentation_only_baseline" \
    --wandb_run "dem_alphaearth_segonly_seed_222324_production" \
    --wandb_mode online \
    --wandb_dir "/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/wandb_logs"

echo "Segmentation-only training job completed"