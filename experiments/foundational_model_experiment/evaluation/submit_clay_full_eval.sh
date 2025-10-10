#!/bin/bash
# Your specific account/project
#SBATCH --account=bcrm-tgirails
#SBATCH --partition=gpu    # For GPU workloads

# Specify a single GPU for each array job
#SBATCH --gpus=1

# Request a single task (process) per array job
#SBATCH --ntasks=1

# Set the number of CPU cores for each task
#SBATCH --cpus-per-task=16

# Set the memory for each job
#SBATCH --mem=200G

# Set the maximum time each job will run
#SBATCH --time=48:00:00

# Set the name of the job
#SBATCH --job-name=clay_full_eval

# Specify the output and error files for each job
#SBATCH --output=slurm_logs/clay_full_eval_%j.out
#SBATCH --error=slurm_logs/clay_full_eval_%j.err

# Clay Foundation Model Full Evaluation - All 67 Test HUCs

echo "Starting Clay Foundation Model Full Evaluation (67 HUCs) at $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "GPU: $CUDA_VISIBLE_DEVICES"

# Load conda environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Change to evaluation directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation

# Create log directory
mkdir -p slurm_logs

# Verify environment
echo "Python: $(which python)"
echo "CUDA available: $(python -c 'import torch; print(torch.cuda.is_available())')"

# Run batch evaluation for Clay only
echo "Starting Clay evaluation on 67 HUCs..."

python batch_foundation_evaluator.py \
    --models clay \
    --config_dir "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config" \
    --data_path "/projects/bcrm/nathanj/data/processed/test/patch_dataset" \
    --output_dir "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results" \
    --batch_size 32 \
    --device cuda \
    --huc_file "/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt" \
    --wandb_mode online

echo "Clay full evaluation completed at $(date)"