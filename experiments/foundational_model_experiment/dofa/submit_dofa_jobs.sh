#!/bin/bash

#-----------------------------------------------------------------------------
# Helper script to submit DOFA foundation model array jobs
#-----------------------------------------------------------------------------

echo "🚀 DOFA Foundation Model Array Submission"
echo "=========================================="

# Check if we're in the right directory
if [[ ! -f "configs/dofa_config.yaml" ]]; then
    echo "❌ Error: Please run this script from the dofa experiment directory"
    echo "Expected to find: configs/dofa_config.yaml"
    exit 1
fi

# Check if slurm is available
if ! command -v sbatch &> /dev/null; then
    echo "❌ Error: SLURM (sbatch) not found. This script requires a SLURM cluster."
    exit 1
fi

# Default values
NUM_RUNS=5
CONCURRENT=3

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
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --runs NUM        Number of array jobs to submit (default: 5)"
            echo "  --concurrent NUM  Number of concurrent jobs (default: 3)"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Submit 5 runs, 3 concurrent"
            echo "  $0 --runs 10         # Submit 10 runs, 3 concurrent" 
            echo "  $0 --runs 3 --concurrent 1  # Submit 3 runs, 1 at a time"
            exit 0
            ;;
        *)
            echo "❌ Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "📊 Submission Configuration:"
echo "   Number of runs: $NUM_RUNS"
echo "   Concurrent jobs: $CONCURRENT"
echo "   Array specification: 1-${NUM_RUNS}%${CONCURRENT}"
echo ""

# Create logs directory
mkdir -p slurm_logs

# Update the array specification in the script
sed -i "s/#SBATCH --array=.*/#SBATCH --array=1-${NUM_RUNS}%${CONCURRENT}/" submit_train_dofa_array.sh

echo "🎯 Submitting DOFA array job..."

# Submit the job
JOB_ID=$(sbatch --parsable submit_train_dofa_array.sh)

if [[ $? -eq 0 ]]; then
    echo "✅ Successfully submitted job array!"
    echo "   Job ID: $JOB_ID"
    echo "   Array: 1-${NUM_RUNS}%${CONCURRENT}"
    echo ""
    echo "📋 Monitor your jobs:"
    echo "   squeue -u \$USER --array"
    echo "   squeue -j $JOB_ID"
    echo ""
    echo "📁 Check logs:"
    echo "   ls slurm_logs/dofa-9ch-array_${JOB_ID}_*.{out,err}"
    echo ""
    echo "📊 Monitor on WandB:"
    echo "   https://wandb.ai/[your-entity]/dofa_water_segmentation"
else
    echo "❌ Failed to submit job array"
    exit 1
fi