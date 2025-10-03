# DEM + Optical Multimodal Multitask Model

This directory contains a simplified version of the multimodal multitask model that uses only **2 modalities**: DEM and Optical data.

## Overview

The original model (`mdmt_v1.py`) used 4 modalities:
- DEM (1 channel) - Priority modality
- Optical (6 channels) - Landsat bands B2,B3,B4,B5,B6,B7
- Thermal (1 channel) - Landsat thermal band
- SAR (1 channel) - Sentinel-1 SAR data

This simplified model (`mdmt_dem_optical.py`) uses only:
- **DEM (1 channel)** - Priority modality with skip connections
- **Optical (6 channels)** - Secondary modality with configurable channel count

## Files

- `mdmt_dem_optical.py` - The DEM + Optical model architecture
- `patchDataLoader_dem_optical.py` - Data loader for DEM + Optical data
- `example_dem_optical.py` - Example usage and compatibility guide

## Model Architecture

```
Input: DEM (1,H,W) + Optical (C,H,W)
  ↓
┌─DEM Encoder────────┐  ┌─Optical Encoder──┐
│ (Priority modality)│  │ (Secondary)      │
│ Returns features + │  │ Returns features │
│ skip connections   │  │ (skips ignored)  │
└────────────────────┘  └──────────────────┘
  ↓                        ↓
┌─────Hierarchical Attention Fusion─────┐
│ DEM features guide optical attention  │
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

1. **Hierarchical Attention Fusion**: DEM (priority modality) guides attention for optical data
2. **Skip Connections**: Only DEM encoder provides skip connections to decoders
3. **Configurable Optical Channels**: Support for 3-6 band optical data (default: 6)
4. **Reduced Complexity**: Significantly fewer parameters than 4-modality model
5. **Same Output Format**: Two task outputs with same spatial dimensions as input

## Data Requirements

The dataset expects patches with these keys:
- `'dem'`: Elevation data (2D array)
- `'optical'`: Optical data (2D or 3D array) - **now required**
- `'hydro_mask'`: Ground truth for task 1 (2D array)
- `'flow_dir'`: Ground truth for task 2 (2D array)

All spatial arrays must have compatible H×W dimensions. Optical data can be:
- 2D (H,W): Will be broadcast to desired channel count
- 3D (H,W,C): Will be padded/truncated to match desired channel count

## Usage

### 1. Basic Model Usage

```python
from models.mdmt_dem_optical import MultimodalMultitaskModel_DEM_Optical
import torch

# Create model with 6-band optical
model = MultimodalMultitaskModel_DEM_Optical(
    n_classes_task1=1,
    n_classes_task2=1,
    base_channels=64,
    optical_channels=6
)

# Forward pass
dem = torch.randn(batch_size, 1, H, W)
optical = torch.randn(batch_size, 6, H, W)  # 6-band optical

pred_task1, pred_task2 = model(dem, optical)
```

### 2. Data Loading

```python
from data.patchDataLoader_dem_optical import create_dataloader_dem_optical

# Create data loader
dataloader = create_dataloader_dem_optical(
    base_path="/path/to/patch/data",
    huc_codes=["10020007", "10020008"],
    batch_size=8,
    shuffle=True,
    optical_channels=6  # Configurable
)

# Use in training loop
for batch in dataloader:
    dem = batch['dem']
    optical = batch['optical']
    hydro_mask = batch['hydro_mask']
    flow_dir = batch['flow_dir']
    
    pred1, pred2 = model(dem, optical)
    # ... calculate loss and train
```

### 3. Training

```bash
# Example training command (would need custom training script)
python training/train_dem_optical.py \
    --data_path /path/to/your/patch/data \
    --huc_codes 10020007 10020008 \
    --batch_size 8 \
    --epochs 50 \
    --lr 1e-3 \
    --optical_channels 6 \
    --output_dir ./outputs
```

### 4. Example and Testing

```bash
# Show compatibility guide only
python example_dem_optical.py --demo_only

# Run inference example (if you have data)
python example_dem_optical.py \
    --data_path /path/to/your/data \
    --huc_codes 10020007 \
    --optical_channels 6 \
    --model_path ./outputs/best_model_dem_optical.pth
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
from data.patchDataLoader_dem_optical import MultimodalPatchDataset_DEM_Optical
batch = dataset[0]
pred1, pred2 = model(batch['dem'], batch['optical'])
```

### Model Changes
```python
# OLD
from models.mdmt_v1 import MultimodalMultitaskModel
model = MultimodalMultitaskModel(...)

# NEW
from models.mdmt_dem_optical import MultimodalMultitaskModel_DEM_Optical  
model = MultimodalMultitaskModel_DEM_Optical(optical_channels=6, ...)
```

## Benefits of DEM + Optical Model

1. **Rich Visual Information**: Optical bands provide spectral information for land cover analysis
2. **Terrain Context**: DEM provides elevation context that guides optical interpretation
3. **Multi-spectral Analysis**: Support for various optical band combinations (RGB, NIR, SWIR)
4. **Lower Data Requirements**: Only needs 2 modalities instead of 4
5. **Flexible Configuration**: Adjustable optical channel count for different sensors

## Optical Band Configurations

### 3-Band RGB (Landsat 8/9)
- B4 (Red), B3 (Green), B2 (Blue)

### 6-Band Multi-spectral (Landsat 8/9) - Default
- B2 (Blue), B3 (Green), B4 (Red), B5 (NIR), B6 (SWIR1), B7 (SWIR2)

### Custom Configurations
- Modify `optical_channels` parameter in model and data loader
- Data loader handles padding/truncation automatically

## Model Parameters

- Input: DEM (1 channel) + Optical (configurable channels, default 6)
- Output: 2 tasks, each 1 channel with same spatial dimensions as input
- Base channels: 64 (configurable)
- Architecture: Encoder-Fusion-Decoder with attention and skip connections

## Performance Considerations

- **Computational Efficiency**: Reduced encoder count compared to 4-modality model
- **Memory Usage**: Lower memory footprint due to fewer modality encoders
- **Training Speed**: Faster training with simplified architecture
- **Inference Speed**: Faster inference with reduced model complexity

## Notes

- DEM is treated as the priority modality providing skip connections
- Optical data is now required (not optional)
- The fusion mechanism uses hierarchical attention where DEM guides optical processing
- Compatible with same training frameworks as the original model
- Supports flexible optical channel configurations for different sensor types