#!/bin/bash

#-----------------------------------------------------------------------------
# Master script to submit all foundation model array jobs
# Submits Prithvi, Clay, DOFA, and SatLas experiments
#-----------------------------------------------------------------------------

echo "🚀 Foundation Model Array Job Master Submission"
echo "================================================"

# Default values
NUM_RUNS=5
CONCURRENT=3
MODELS="prithvi,clay,dofa,satlas"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --runs)
            NUM_RUNS="$2"
            shift 2
            ;;
        --concurrent)
            CONCURRENT="$2"
            shift 2
            ;;
        --models)
            MODELS="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --runs NUM        Number of array jobs to submit per model (default: 5)"
            echo "  --concurrent NUM  Number of concurrent jobs per model (default: 3)"
            echo "  --models LIST     Comma-separated list of models to submit (default: prithvi,clay,dofa,satlas)"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Available models: prithvi, clay, dofa, satlas"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Submit all models, 5 runs each, 3 concurrent"
            echo "  $0 --runs 10                         # Submit all models, 10 runs each"
            echo "  $0 --models prithvi,clay             # Submit only Prithvi and Clay"
            echo "  $0 --runs 3 --concurrent 1          # Submit all models, 3 runs each, 1 at a time"
            exit 0
            ;;
        *)
            echo "❌ Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Convert comma-separated models to array
IFS=',' read -ra MODEL_ARRAY <<< "$MODELS"

echo "📊 Master Submission Configuration:"
echo "   Models to submit: ${MODELS}"
echo "   Number of runs per model: $NUM_RUNS"
echo "   Concurrent jobs per model: $CONCURRENT"
echo ""

# Check if we're in the right directory
EXPERIMENT_DIR="/u/nathanj/national_ml/experiments/foundational_model_experiment"
if [[ ! -d "$EXPERIMENT_DIR" ]]; then
    echo "❌ Error: Foundation model experiment directory not found: $EXPERIMENT_DIR"
    exit 1
fi

# Change to experiment directory
cd "$EXPERIMENT_DIR"

# Counters for tracking
TOTAL_SUBMITTED=0
FAILED_SUBMISSIONS=0
SUBMITTED_JOBS=()

# Submit each model
for model in "${MODEL_ARRAY[@]}"; do
    model=$(echo "$model" | xargs)  # Trim whitespace
    
    echo "🎯 Submitting $model foundation model..."
    
    if [[ ! -d "$model" ]]; then
        echo "⚠️  Warning: Model directory not found: $model (skipping)"
        continue
    fi
    
    cd "$model"
    
    # Check if submission script exists
    if [[ ! -f "submit_${model}_jobs.sh" ]]; then
        echo "⚠️  Warning: Submission script not found: submit_${model}_jobs.sh (skipping)"
        cd ..
        continue
    fi
    
    # Submit the model
    if ./submit_${model}_jobs.sh --runs $NUM_RUNS --concurrent $CONCURRENT; then
        echo "✅ Successfully submitted $model jobs"
        TOTAL_SUBMITTED=$((TOTAL_SUBMITTED + 1))
        
        # Get the job ID from the output (if available)
        # This is a simple approach - could be improved to capture actual job IDs
        echo "   → $model: $NUM_RUNS runs, $CONCURRENT concurrent"
        SUBMITTED_JOBS+=("$model")
    else
        echo "❌ Failed to submit $model jobs"
        FAILED_SUBMISSIONS=$((FAILED_SUBMISSIONS + 1))
    fi
    
    cd ..
    echo ""
done

# Summary
echo "======================================================"
echo "Foundation Model Submission Summary"
echo "======================================================"
echo "Total models requested: ${#MODEL_ARRAY[@]}"
echo "Successfully submitted: $TOTAL_SUBMITTED"
echo "Failed submissions: $FAILED_SUBMISSIONS"
echo ""

if [[ $TOTAL_SUBMITTED -gt 0 ]]; then
    echo "✅ Successfully submitted models:"
    for model in "${SUBMITTED_JOBS[@]}"; do
        echo "   - $model ($NUM_RUNS runs, $CONCURRENT concurrent)"
    done
    echo ""
    
    echo "📋 Monitor all jobs:"
    echo "   squeue -u \$USER --array"
    echo "   squeue -u \$USER | grep -E '(prithvi|clay|dofa|satlas)'"
    echo ""
    
    echo "📁 Check logs in each model directory:"
    for model in "${SUBMITTED_JOBS[@]}"; do
        echo "   ls $EXPERIMENT_DIR/$model/slurm_logs/"
    done
    echo ""
    
    echo "📊 Monitor on WandB:"
    echo "   Prithvi: https://wandb.ai/[your-entity]/prithvi_water_segmentation"
    echo "   Clay: https://wandb.ai/[your-entity]/clay_water_segmentation"
    echo "   DOFA: https://wandb.ai/[your-entity]/dofa_water_segmentation"
    echo "   SatLas: https://wandb.ai/[your-entity]/satlas_water_segmentation"
fi

if [[ $FAILED_SUBMISSIONS -gt 0 ]]; then
    echo "❌ Some submissions failed. Check the error messages above."
    exit 1
else
    echo "🎉 All requested models submitted successfully!"
fi