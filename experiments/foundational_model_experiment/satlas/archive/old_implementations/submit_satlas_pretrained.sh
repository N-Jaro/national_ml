#!/bin/bash
#SBATCH --job-name=satlas-pretrained-9ch
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128GB
#SBATCH --gres=gpu:1
#SBATCH --time=72:00:00
#SBATCH --output=slurm_logs/slurm_satlas_pretrained_%j.out
#SBATCH --error=slurm_logs/slurm_satlas_pretrained_%j.err

# Create log directory
mkdir -p slurm_logs

echo "=========================================="
echo "SATLAS Pretrained Foundation Model Training"
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start time: $(date)"
echo "Working directory: $(pwd)"

# Load required modules
module load cuda/12.1

# Activate terratorch environment (CRITICAL for SATLAS pretrained weights)
echo "Activating terratorch_env environment..."
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Verify environment
echo "Python version: $(python --version)"
echo "PyTorch version: $(python -c 'import torch; print(torch.__version__)')"
echo "CUDA available: $(python -c 'import torch; print(torch.cuda.is_available())')"

# Check TerraTorch and SATLAS availability
echo "Checking TerraTorch and SATLAS availability..."
python -c "
from terratorch import BACKBONE_REGISTRY
satlas_models = [m for m in BACKBONE_REGISTRY if 'satlas' in m.lower()]
print(f'Found {len(satlas_models)} SATLAS models in TerraTorch')
target_model = 'terratorch_satlas_swin_b_sentinel2_mi_ms'
if target_model in BACKBONE_REGISTRY:
    print(f'✓ Target SATLAS model {target_model} is available')
else:
    print(f'✗ Target SATLAS model {target_model} NOT found')
    print('Available SATLAS models:', satlas_models[:5])
"

# Set environment variables for reproducibility
export PYTHONHASHSEED=42
export CUBLAS_WORKSPACE_CONFIG=:16:8

# Set up WandB
export WANDB_PROJECT="national_ml_satlas_pretrained"
export WANDB_RUN_ID="satlas_pretrained_9ch_$(date +%Y%m%d_%H%M%S)_run${SLURM_JOB_ID}"
export WANDB_NAME="$WANDB_RUN_ID"
echo "WandB run name: $WANDB_NAME"

# Run training with pretrained SATLAS weights
echo "Starting SATLAS pretrained model training..."
echo "Config: configs/satlas_pretrained_config.yaml"
echo "Using TerraTorch backbone: terratorch_satlas_swin_b_sentinel2_mi_ms"

cd /u/nathanj/national_ml/experiments/foundational_model_experiment/satlas

python training/run_satlas_pretrained.py \
    --config configs/satlas_pretrained_config.yaml \
    --gpus 1

exit_code=$?

echo "=========================================="
echo "Training completed with exit code: $exit_code"
echo "End time: $(date)"
echo "Job ID: $SLURM_JOB_ID"

if [ $exit_code -eq 0 ]; then
    echo "✓ SATLAS pretrained training completed successfully"
    echo "Checkpoints saved to: outputs/models/$WANDB_NAME/checkpoints/"
else
    echo "✗ SATLAS pretrained training failed"
fi

echo "=========================================="

exit $exit_code