#!/bin/bash

#SBATCH --job-name=test_all_modalities_1huc
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --gpus=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100G
#SBATCH --time=01:00:00
#SBATCH --output=slurm_logs/test_all_modalities_1huc_%j.out
#SBATCH --error=slurm_logs/test_all_modalities_1huc_%j.err

# Create the log directory if it doesn't exist
mkdir -p slurm_logs

echo "=== Single HUC Test: All Modalities (no AlphaEarth) ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo ""
echo "Train HUC: 03160113"
echo "Val HUC: 16040204"
echo "Architecture: DEM(1) + Optical(6) + Thermal(1) + SAR(1) = 9 channels"
echo ""

# Activate environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

# Run single HUC test with 5 epochs for quick validation
python run_lightning_train_all_modalities.py \
    --base_path /u/nathanj/national_ml/data/processed/patch_dataset/ \
    --train_hucs "03160113" \
    --val_hucs "16040204" \
    --batch_size 32 \
    --epochs 5 \
    --lr 1e-4 \
    --precision 16 \
    --wandb_project "national_ml_all_modalities" \
    --wandb_mode online \
    --experiment_name "all_modalities_1huc_test_$(date +%Y%m%d_%H%M%S)"

echo ""
echo "=== All Modalities (no AlphaEarth) 1HUC test completed ==="