# DEM-Only Baseline Model

## Overview

This directory contains the DEM-only baseline model implementation for multitask learning on water segmentation and D8 flow direction prediction. This model serves as a baseline to compare against the multimodal variants (DEM+Optical, DEM+SAR, DEM+Thermal).

## Architecture

The DEM-only model uses a simplified U-Net architecture:

1. **Single Encoder**: Processes only Digital Elevation Model (DEM) data
2. **Shared Processing**: Optional shared layer for representation learning
3. **Dual Decoders**: Separate decoders for water segmentation and D8 flow direction tasks
4. **Skip Connections**: Uses skip connections from the DEM encoder for both tasks

### Key Features

- **Baseline Architecture**: Simplest model using only elevation data
- **Multitask Learning**: Simultaneously predicts water segmentation and flow direction
- **Dynamic Loss Weighting**: Uses uncertainty-based weighting between tasks
- **Class Balancing**: Handles imbalanced water/non-water and D8 direction classes

## Files

### Model Architecture
- `mdmt_dem_only.py`: Core model architecture (MultitaskModel_DEM_Only)

### Training Infrastructure
- `train_dem_only_lightning.py`: PyTorch Lightning training module
- `data_module_dem_only.py`: Data module for loading and preprocessing
- `patchDataLoader_dem_only.py`: Dataset class for DEM-only patches
- `run_lightning_train_dem_only.py`: Training script

### SLURM Scripts
- `submit_train_dem_only_single.sh`: Single training job
- `submit_train_dem_only_array.sh`: Array of training jobs for multiple runs

## Usage

### Quick Test
```bash
# Test training with small dataset
python run_lightning_train_dem_only.py \
    --hucs "03030005" \
    --batch_size 4 \
    --epochs 5 \
    --wandb_mode offline
```

### Full Training
```bash
# Train on full dataset
python run_lightning_train_dem_only.py \
    --hucs "03160113,19090102,17090011,03040206,18070103" \
    --batch_size 32 \
    --epochs 100 \
    --lr 1e-4 \
    --precision 16
```

### SLURM Submission
```bash
# Submit single training job
sbatch submit_train_dem_only_single.sh

# Submit array of 5 training runs
sbatch submit_train_dem_only_array.sh
```

## Model Comparison

This DEM-only baseline can be compared against:

1. **DEM+Optical**: `run_lightning_train_dem_optical.py`
2. **DEM+SAR**: `run_lightning_train_dem_sar.py` 
3. **DEM+Thermal**: `run_lightning_train_dem_thermal.py`
4. **Full Multimodal**: `run_lightning_train.py`

## Expected Performance

The DEM-only model should:
- Perform reasonably well on D8 flow direction (elevation is strongly correlated)
- Show limitations on water segmentation compared to multimodal variants
- Serve as a lower bound for multimodal model performance
- Have fewer parameters and faster training than multimodal models

## Parameters

The model accepts the same training parameters as other variants:
- `--lr`: Learning rate (default: 1e-3)
- `--batch_size`: Batch size (default: 4)
- `--epochs`: Number of epochs (default: 20)
- `--precision`: Training precision (default: "16")
- `--optimizer`: Adam or AdamW (default: "AdamW")
- `--water_loss_scale`: Water segmentation loss scaling (default: 1.0)
- `--d8_loss_scale`: D8 flow direction loss scaling (default: 0.5)
- `--no_dynamic_weighter`: Disable uncertainty-based loss weighting

## Logging

All runs are logged to Weights & Biases with project name `mdmt-hydro-dem-only` for easy comparison with other model variants.