#!/bin/bash
#SBATCH --job-name=satlas-satlaspretrain-9ch
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128GB
#SBATCH --gres=gpu:1
#SBATCH --time=72:00:00
#SBATCH --output=slurm_logs/slurm_satlas_satlaspretrain_%j.out
#SBATCH --error=slurm_logs/slurm_satlas_satlaspretrain_%j.err

# Create log directory
mkdir -p slurm_logs

echo "=========================================="
echo "SATLAS Pretrained Foundation Model Training (satlaspretrain-models)"
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start time: $(date)"
echo "Working directory: $(pwd)"

# Load required modules
module load cuda/12.1

# Activate terratorch environment (has satlaspretrain-models installed)
echo "Activating terratorch_env environment..."
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Verify environment
echo "Python version: $(python --version)"
echo "PyTorch version: $(python -c 'import torch; print(torch.__version__)')"
echo "CUDA available: $(python -c 'import torch; print(torch.cuda.is_available())')"

# Check satlaspretrain-models availability
echo "Checking satlaspretrain-models availability..."
python -c "
import satlaspretrain_models
print(f'✓ satlaspretrain-models version: {satlaspretrain_models.__version__ if hasattr(satlaspretrain_models, \"__version__\") else \"installed\"}')
weights_manager = satlaspretrain_models.Weights()
print('✓ Weights manager initialized')
print('Available models:', ['Sentinel2_SwinB_MI_MS', 'Sentinel2_SwinB_MI_RGB', 'etc...'])
"

# Set environment variables for reproducibility
export PYTHONHASHSEED=42
export CUBLAS_WORKSPACE_CONFIG=:16:8

# Set up WandB
export WANDB_PROJECT="national_ml_satlas_satlaspretrain"
export WANDB_RUN_ID="satlas_satlaspretrain_9ch_$(date +%Y%m%d_%H%M%S)_run${SLURM_JOB_ID}"
export WANDB_NAME="$WANDB_RUN_ID"
echo "WandB run name: $WANDB_NAME"

# Run training with SATLAS pretrained weights (satlaspretrain-models approach)
echo "Starting SATLAS pretrained model training..."
echo "Config: configs/satlas_pretrained_config.yaml"
echo "Using satlaspretrain-models package: Sentinel2_SwinB_MI_MS"

cd /u/nathanj/national_ml/experiments/foundational_model_experiment/satlas

python training/train_satlas_satlaspretrain.py \
    --config configs/satlas_pretrained_config.yaml \
    --gpus 1

exit_code=$?

echo "=========================================="
echo "Training completed with exit code: $exit_code"
echo "End time: $(date)"
echo "Job ID: $SLURM_JOB_ID"

if [ $exit_code -eq 0 ]; then
    echo "✓ SATLAS satlaspretrain training completed successfully"
    echo "Checkpoints saved to: outputs/models/$WANDB_NAME/checkpoints/"
else
    echo "✗ SATLAS satlaspretrain training failed"
fi

echo "=========================================="

exit $exit_code