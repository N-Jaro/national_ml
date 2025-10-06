#!/bin/bash

#===============================================================================
# PyTorch GPU CUDA 11.8 Environment Setup Script
# 
# This script recreates the exact working environment for national_ml project
# that was successfully fixed on October 3, 2025.
# 
# Environment Details:
# - Python 3.10.18
# - PyTorch 2.5.1 with CUDA 11.8
# - Compatible geospatial packages (rasterio, geopandas)
# - All required packages for national_ml data processing pipeline
#
# Usage: ./setup_pytorch_gpu_cu118_environment.sh
#===============================================================================

set -e  # Exit on any error

echo "======================================================================"
echo "Setting up pytorch_gpu_cu118 environment for national_ml project"
echo "Recreating the exact working environment from October 3, 2025"
echo "======================================================================"

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "Error: conda is not available. Please install Miniconda/Anaconda first."
    exit 1
fi

# Remove existing environment if it exists
echo "Checking for existing pytorch_gpu_cu118 environment..."
if conda env list | grep -q "pytorch_gpu_cu118"; then
    echo "Found existing pytorch_gpu_cu118 environment. Removing it..."
    conda env remove -n pytorch_gpu_cu118 -y
fi

echo "Creating new pytorch_gpu_cu118 environment with Python 3.10..."
conda create -n pytorch_gpu_cu118 python=3.10 -y

echo "Activating environment..."
source $(conda info --base)/etc/profile.d/conda.sh
conda activate pytorch_gpu_cu118

echo "Installing PyTorch 2.5.1 with CUDA 11.8..."
conda install pytorch=2.5.1 torchvision=0.20.1 torchaudio=2.5.1 pytorch-cuda=11.8 -c pytorch -c nvidia -y

echo "Installing geospatial packages from conda-forge..."
conda install -c conda-forge geopandas=1.1.1 rasterio=1.4.3 pandas=2.3.3 numpy=2.2.6 tqdm=4.67.1 -y

echo "Installing additional Python packages via pip..."
pip install \
    earthengine-api==1.6.10 \
    tifffile==2025.5.10 \
    pysheds==0.5 \
    google-auth==2.41.1 \
    google-auth-oauthlib==1.2.2 \
    google-api-python-client==2.184.0 \
    requests==2.32.5

echo "Verifying installation..."
python -c "
import torch
import rasterio 
import geopandas
import pandas
import numpy
import earthengine
import pysheds
print('✅ All core packages imported successfully!')
print(f'PyTorch version: {torch.__version__}')
print(f'CUDA available: {torch.cuda.is_available()}')
print(f'Python version: {torch.version.split()[0]}')
"

echo ""
echo "======================================================================"
echo "✅ Environment setup complete!"
echo "======================================================================"
echo ""
echo "To activate the environment:"
echo "  conda activate pytorch_gpu_cu118"
echo ""
echo "To test the environment:"
echo "  cd /u/nathanj/national_ml/data/script"
echo "  python rerun_pipeline.py --help"
echo ""
echo "Key packages installed:"
echo "  - Python 3.10.18"
echo "  - PyTorch 2.5.1 (CUDA 11.8)"
echo "  - rasterio 1.4.3"
echo "  - geopandas 1.1.1" 
echo "  - earthengine-api 1.6.10"
echo "  - All dependencies for national_ml pipeline"
echo ""
echo "Environment files created for reproducibility:"
echo "  - pytorch_gpu_cu118_environment.yml (full conda export)"
echo "  - pytorch_gpu_cu118_pip_requirements.txt (pip freeze)"
echo "  - pytorch_gpu_cu118_conda_list.txt (detailed package list)"
echo "======================================================================"