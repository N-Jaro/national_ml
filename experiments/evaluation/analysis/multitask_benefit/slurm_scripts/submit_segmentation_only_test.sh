#!/bin/bash
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --job-name=seg_only_test
#SBATCH --time=4:00:00
#SBATCH --output=/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs/test_slurm_%j.out
#SBATCH --error=/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs/test_slurm_%j.err

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Create log directory
mkdir -p /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/slurm_logs

# Navigate to experiments root directory (where imports work correctly)
cd /u/nathanj/national_ml/experiments

# Test run with single HUC and few epochs
python training/run_lightning_train_dem_alphaearth_segonly.py \
    --train_hucs "03030005" \
    --val_hucs "03030005" \
    --batch_size 16 \
    --epochs 5 \
    --lr 1e-4 \
    --patience 10 \
    --num_workers 8 \
    --precision bf16 \
    --alphaearth_channels 64 \
    --water_loss_scale 1.0 \
    --d8_loss_scale 0.0 \
    --no_dynamic_weighter \
    --optimizer AdamW \
    --wandb_project "national_ml_segmentation_only_test" \
    --wandb_run "dem_alphaearth_segonly_test" \
    --wandb_mode online \
    --wandb_dir "/u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit/wandb_logs"

echo "Segmentation-only test job completed"