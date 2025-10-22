#!/bin/bash
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=200G
#SBATCH --gres=gpu:1
#SBATCH --time=12:00:00
#SBATCH --job-name=segonly_dem_alphaearth_dev
#SBATCH --output=slurm_logs_segmentation_training/slurm_dev_%j.out
#SBATCH --error=slurm_logs_segmentation_training/slurm_dev_%j.err

echo "🔬 SLURM Job: Segmentation-Only DEM+AlphaEarth Training (DEV)"
echo "========================================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Started at: $(date)"
echo "Purpose: Quick development run for segmentation-only training"
echo ""

# Environment setup
echo "🔧 Setting up environment..."
source ~/.bashrc
conda activate pytorch_gpu_cu118

# Verify CUDA
echo "🚀 GPU Information:"
nvidia-smi --query-gpu=name,memory.total,memory.used --format=csv
echo ""

# Navigate to script directory
cd /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit

# Development configuration (smaller, faster)
TRAIN_DATA_PATH="/projects/bcrm/nathanj/data/processed/train/patch_dataset"
VAL_DATA_PATH="/projects/bcrm/nathanj/data/processed/test/patch_dataset"

# Limited HUCs for quick development
TRAIN_HUCS=("10020007" "03030005")
VAL_HUCS=("10020007")

echo "📊 Development Training Configuration:"
echo "  Architecture: Segmentation-Only DEM+AlphaEarth"
echo "  Task: Water segmentation ONLY (no flow direction)"
echo "  Train Data: $TRAIN_DATA_PATH"
echo "  Val Data: $VAL_DATA_PATH"
echo "  Train HUCs: ${TRAIN_HUCS[@]} (LIMITED FOR DEV)"
echo "  Val HUCs: ${VAL_HUCS[@]} (LIMITED FOR DEV)"
echo "  AlphaEarth Channels: 64"
echo "  Loss: combined_focal_dice"
echo "  Epochs: 20 (REDUCED FOR DEV)"
echo "  Batch Size: 8 (REDUCED FOR DEV)"
echo "  Limited Batches: 0.3 (30% of data for speed)"
echo ""

# Run development training
echo "🚀 Starting segmentation-only development training..."
python training/run_segmentation_only_training.py \
    --train-data-path "$TRAIN_DATA_PATH" \
    --val-data-path "$VAL_DATA_PATH" \
    --train-hucs "${TRAIN_HUCS[@]}" \
    --val-hucs "${VAL_HUCS[@]}" \
    --alphaearth-channels 64 \
    --batch-size 8 \
    --num-workers 8 \
    --epochs 20 \
    --learning-rate 1e-4 \
    --weight-decay 1e-4 \
    --loss-type combined_focal_dice \
    --optimizer adamw \
    --scheduler cosine \
    --gpus 1 \
    --precision 16-mixed \
    --project-name national_ml_segmentation_only_dem_alphaearth_dev \
    --wandb-mode online \
    --save-top-k 2 \
    --early-stopping-patience 8 \
    --limit-batches 0.3

TRAIN_EXIT_CODE=$?

echo ""
echo "========================================================="
echo "Development training completed at: $(date)"
echo "Exit code: $TRAIN_EXIT_CODE"

if [ $TRAIN_EXIT_CODE -eq 0 ]; then
    echo "✅ Segmentation-only development training completed successfully!"
    echo ""
    echo "📂 Results location:"
    echo "  Checkpoints: lightning_logs/segonly_dem_alphaearth_*/checkpoints/"
    echo "  Logs: lightning_logs/segonly_dem_alphaearth_*/"
    echo ""
    echo "🔬 Development Success - Ready for:"
    echo "  1. Full production training (submit_segmentation_only_training.sh)"
    echo "  2. Update multitask analysis with checkpoint path"
    echo "  3. Run connectivity comparison analysis"
else
    echo "❌ Development training failed with exit code: $TRAIN_EXIT_CODE"
    echo "📋 Check the logs above for error details"
    echo "🔧 Debug and fix issues before running full training"
fi

echo "🏁 SLURM development job completed"