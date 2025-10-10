#!/bin/bash

#-----------------------------------------------------------------------------
# TerraTorch Environment Setup for Foundational Model Experiments
#-----------------------------------------------------------------------------

echo "======================================================"
echo "Setting up TerraTorch Environment"
echo "Foundational Model Fine-tuning for Water Segmentation"
echo "======================================================"

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "Error: Please run this script from the foundational_model_experiment directory"
    exit 1
fi

# Create a conda environment for TerraTorch experiments
echo "Creating conda environment: terratorch_env"
conda create -n terratorch_env python=3.10 -y

# Activate the environment
echo "Activating environment..."
conda activate terratorch_env

# Install core dependencies
echo "Installing PyTorch and basic dependencies..."
conda install pytorch torchvision pytorch-cuda=11.8 -c pytorch -c nvidia -y

# Install geospatial dependencies
echo "Installing geospatial packages..."
conda install -c conda-forge rasterio geopandas xarray rioxarray gdal -y

# Install remaining dependencies via pip
echo "Installing remaining dependencies via pip..."
pip install -r requirements.txt

# Verify TerraTorch installation
echo "Verifying TerraTorch installation..."
python -c "import terratorch; print('TerraTorch version:', terratorch.__version__)"

# Create initial directory structure if not exists
# echo "Creating directory structure..."
# mkdir -p configs/models
# mkdir -p configs/data
# mkdir -p configs/training
# mkdir -p results/checkpoints
# mkdir -p results/logs
# mkdir -p results/visualizations

echo "======================================================"
echo "TerraTorch environment setup complete!"
echo "======================================================"
echo ""
echo "To activate the environment:"
echo "  conda activate terratorch_env"
echo ""
echo "Next steps:"
echo "1. Create data adapter for your patch dataset"
echo "2. Configure foundation models for water segmentation"
echo "3. Set up training pipeline"
echo "4. Run initial experiments"