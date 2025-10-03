# DEM + Thermal Multimodal Multitask Model

This directory contains a simplified version of the multimodal multitask model that uses only **2 modalities**: DEM and Thermal data.

## Overview

The original model (`mdmt_v1.py`) used 4 modalities:
- DEM (1 channel) - Priority modality
- Optical (6 channels) - Landsat bands B2,B3,B4,B5,B6,B7
- Thermal (1 channel) - Landsat thermal band
- SAR (1 channel) - Sentinel-1 SAR data

This simplified model (`mdmt_dem_thermal.py`) uses only:
- **DEM (1 channel)** - Priority modality with skip connections
- **Thermal (1 channel)** - Secondary modality

## Files

- `mdmt_dem_thermal.py` - The DEM + Thermal model architecture
- `patchDataLoader_dem_thermal.py` - Data loader for DEM + Thermal data
- `train_dem_thermal.py` - Training script template
- `example_dem_thermal.py` - Example usage and compatibility guide

## Model Architecture

```
Input: DEM (1,H,W) + Thermal (1,H,W)
  ↓
┌─DEM Encoder────────┐  ┌─Thermal Encoder──┐
│ (Priority modality)│  │ (Secondary)      │
│ Returns features + │  │ Returns features │
│ skip connections   │  │ (skips ignored)  │
└────────────────────┘  └──────────────────┘
  ↓                        ↓
┌─────Hierarchical Attention Fusion─────┐
│ DEM features guide thermal attention  │
└───────────────────────────────────────┘
  ↓
┌────────Shared Encoder─────────┐
│ Additional processing         │
└───────────────────────────────┘
  ↓
┌─Task 1 Decoder─┐  ┌─Task 2 Decoder─┐
│ Uses DEM skips │  │ Uses DEM skips │
└────────────────┘  └────────────────┘
  ↓                  ↓
Output 1 (H,W)    Output 2 (H,W)
```

## Key Features

1. **Hierarchical Attention Fusion**: DEM (priority modality) guides attention for thermal data
2. **Skip Connections**: Only DEM encoder provides skip connections to decoders
3. **Reduced Complexity**: ~98M parameters (compared to original 4-modality model)
4. **Same Output Format**: Two task outputs with same spatial dimensions as input

## Data Requirements

The dataset expects patches with these keys:
- `'dem'`: Elevation data (2D array)
- `'thermal'`: Thermal data (2D array) - **now required** (was optional in 4-modality version)
- `'hydro_mask'`: Ground truth for task 1 (2D array)
- `'flow_dir'`: Ground truth for task 2 (2D array)

All spatial arrays must have the same H×W dimensions.

## Usage

### 1. Basic Model Usage

```python
from models.mdmt_dem_thermal import MultimodalMultitaskModel_DEM_Thermal
import torch

# Create model
model = MultimodalMultitaskModel_DEM_Thermal(
    n_classes_task1=1,
    n_classes_task2=1,
    base_channels=64
)

# Forward pass
dem = torch.randn(batch_size, 1, H, W)
thermal = torch.randn(batch_size, 1, H, W)

pred_task1, pred_task2 = model(dem, thermal)
```

### 2. Data Loading

```python
from data.patchDataLoader_dem_thermal import create_dataloader_dem_thermal

# Create data loader
dataloader = create_dataloader_dem_thermal(
    base_path="/path/to/patch/data",
    huc_codes=["10020007", "10020008"],
    batch_size=8,
    shuffle=True
)

# Use in training loop
for batch in dataloader:
    dem = batch['dem']
    thermal = batch['thermal']
    hydro_mask = batch['hydro_mask']
    flow_dir = batch['flow_dir']
    
    pred1, pred2 = model(dem, thermal)
    # ... calculate loss and train
```

### 3. Training

```bash
# Example training command
python training/train_dem_thermal.py \\
    --data_path /path/to/your/patch/data \\
    --huc_codes 10020007 10020008 \\
    --batch_size 8 \\
    --epochs 50 \\
    --lr 1e-3 \\
    --output_dir ./outputs
```

### 4. Example and Testing

```bash
# Show compatibility guide only
python example_dem_thermal.py --demo_only

# Run inference example (if you have data)
python example_dem_thermal.py \\
    --data_path /path/to/your/data \\
    --huc_codes 10020007 \\
    --model_path ./outputs/best_model_dem_thermal.pth
```

## Migrating from 4-Modality Model

If you have existing code using the 4-modality model:

### Data Loading Changes
```python
# OLD
from data.patchDataLoader_ls6b import MultimodalPatchDataset
batch = dataset[0]
pred1, pred2 = model(batch['m1'], batch['m2'], batch['m3'], batch['m4'])

# NEW  
from data.patchDataLoader_dem_thermal import MultimodalPatchDataset_DEM_Thermal
batch = dataset[0]
pred1, pred2 = model(batch['dem'], batch['thermal'])
```

### Model Changes
```python
# OLD
from models.mdmt_v1 import MultimodalMultitaskModel
model = MultimodalMultitaskModel(...)

# NEW
from models.mdmt_dem_thermal import MultimodalMultitaskModel_DEM_Thermal  
model = MultimodalMultitaskModel_DEM_Thermal(...)
```

## Benefits of DEM + Thermal Model

1. **Reduced Complexity**: Fewer parameters, faster training and inference
2. **Focused Learning**: Concentrates on elevation and thermal relationships
3. **Lower Data Requirements**: Only needs 2 modalities instead of 4
4. **Maintained Performance**: Architecture preserves key design elements

## Model Parameters

- Input: DEM (1 channel) + Thermal (1 channel)
- Output: 2 tasks, each 1 channel with same spatial dimensions as input
- Base channels: 64 (configurable)
- Total parameters: ~98M
- Architecture: Encoder-Fusion-Decoder with attention and skip connections

## Notes

- DEM is treated as the priority modality providing skip connections
- Thermal data is now required (was optional in 4-modality version)
- The fusion mechanism uses hierarchical attention where DEM guides thermal processing
- Compatible with same training frameworks as the original model