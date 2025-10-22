#!/bin/bash

# Production Launch Script for Segmentation-Only DEM + AlphaEarth Training
# Matches All Modalities + AlphaEarth production configuration

echo "🚀 Production Segmentation-Only Training Launch"
echo "================================================="
echo "Purpose: Train single-task baseline for multitask benefit analysis"
echo "Configuration: 45 train HUCs, 5 val HUCs, 500 epochs, batch_size=32"
echo ""

# Ensure we're in the correct directory
cd /u/nathanj/national_ml/experiments/evaluation/analysis/multitask_benefit

# Display configuration
echo "📊 Production Training Configuration:"
echo "  Model: Segmentation-Only DEM + AlphaEarth (96M parameters)"
echo "  Task: Water segmentation ONLY (no flow direction)"
echo "  Train HUCs: 45 (matches All Modalities training)"
echo "  Val HUCs: 5 (held-out regions)"
echo "  Epochs: 500 (early stopping patience: 15)"
echo "  Batch Size: 32 (H100 optimized)"
echo "  Learning Rate: 1e-4 (foundation model compatible)"
echo "  Precision: 32 (full precision for scientific accuracy)"
echo "  Memory: 180GB (batch_size=32 requirement)"
echo "  Time Limit: 72 hours"
echo "  GPU: H100 80GB HBM3"
echo ""

# Check if log directory exists
if [ ! -d "slurm_logs_segmentation_training" ]; then
    echo "📁 Creating log directory..."
    mkdir -p slurm_logs_segmentation_training
fi

# Check files exist
echo "🔍 Validating training infrastructure..."

if [ ! -f "training/run_segmentation_only_training.py" ]; then
    echo "❌ Training script not found!"
    exit 1
fi

if [ ! -f "slurm_scripts/submit_segmentation_only_training.sh" ]; then
    echo "❌ SLURM script not found!"
    exit 1
fi

echo "✅ All files validated"
echo ""

# Show available options
echo "🎯 Launch Options:"
echo "  1. Single production run:  sbatch slurm_scripts/submit_segmentation_only_training.sh"
echo "  2. Array job (10 runs):   sbatch slurm_scripts/submit_segmentation_only_training_array.sh"
echo ""

# Prompt for choice
echo "Choose launch option (1 for single, 2 for array, or any other key to exit):"
read -n 1 choice
echo ""

case $choice in
    1)
        echo "🚀 Launching single production run..."
        sbatch slurm_scripts/submit_segmentation_only_training.sh
        LAUNCH_SUCCESS=$?
        ;;
    2)
        echo "🚀 Launching array job (10 statistical runs)..."
        sbatch slurm_scripts/submit_segmentation_only_training_array.sh
        LAUNCH_SUCCESS=$?
        ;;
    *)
        echo "👋 Exiting without launching jobs"
        exit 0
        ;;
esac

# Check launch status
if [ $LAUNCH_SUCCESS -eq 0 ]; then
    echo ""
    echo "✅ Job(s) submitted successfully!"
    echo ""
    echo "📊 Monitor progress:"
    echo "  Check queue:     squeue -u $USER"
    echo "  Watch logs:      tail -f slurm_logs_segmentation_training/mdmt-segonly-*.out"
    echo "  W&B dashboard:   https://wandb.ai/your-username/national_ml_segmentation_only_dem_alphaearth"
    echo ""
    echo "⏱️  Expected completion: ~48-72 hours (depends on early stopping)"
    echo ""
    echo "🔬 After training completes:"
    echo "  1. Update analysis/multitask_benefit_analysis.py with checkpoint path"
    echo "  2. Run connectivity comparison analysis"
    echo "  3. Generate publication figures and statistics"
    echo "  4. Prove multitask learning benefits! 🎯"
else
    echo ""
    echo "❌ Job submission failed! Check SLURM configuration."
    exit 1
fi