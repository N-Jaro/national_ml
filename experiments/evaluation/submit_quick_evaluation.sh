#!/bin/bash
#SBATCH --job-name=mdmt_eval_quick
#SBATCH --account=bcrm
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --gres=gpu:1
#SBATCH --time=2:00:00
#SBATCH --output=logs/mdmt_eval_quick_%j.out
#SBATCH --error=logs/mdmt_eval_quick_%j.err

# MDMT Quick Evaluation Test - All Variants with Small HUC List
# This script tests all ultra-fast evaluators with 3 HUCs for quick validation

echo "Starting MDMT Quick Evaluation Test at $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "GPU: $CUDA_VISIBLE_DEVICES"

# Load conda environment
source /home/nathanj/.bashrc
conda activate national_ml

# Change to evaluation directory
cd /u/nathanj/national_ml/experiments/evaluation

# Create logs directory if it doesn't exist
mkdir -p logs

# Run quick automated evaluation
echo "========================================"
echo "Starting QUICK evaluation for all variants"
echo "Using small test HUC list (3 HUCs)"
echo "========================================"

python run_all_evaluations.py --quick

echo "========================================"
echo "Quick evaluation completed at $(date)"
echo "========================================"

# Show results
echo "Quick test results:"
ls -la *_quick_* 2>/dev/null || echo "No quick test files found"
ls -la *.csv | tail -5