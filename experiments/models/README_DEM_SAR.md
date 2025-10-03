# DEM + SAR Multimodal Multitask Model

This directory contains a simplified version of the multimodal multitask model that uses only **2 modalities**: DEM and SAR data.

## Overview

The original model (`mdmt_v1.py`) used 4 modalities:
- DEM (1 channel) - Priority modality
- Optical (6 channels) - Landsat bands B2,B3,B4,B5,B6,B7
- Thermal (1 channel) - Landsat thermal band
- SAR (1 channel) - Sentinel-1 SAR data

This simplified model (`mdmt_dem_sar.py`) uses only:
- **DEM (1 channel)** - Priority modality with skip connections
- **SAR (1 channel)** - Secondary modality (Sentinel-1 VV polarization)

## Files

- `mdmt_dem_sar.py` - The DEM + SAR model architecture
- `patchDataLoader_dem_sar.py` - Data loader for DEM + SAR data
- `example_dem_sar.py` - Example usage and compatibility guide

## Model Architecture

```
Input: DEM (1,H,W) + SAR (1,H,W)
  ↓
┌─DEM Encoder────────┐  ┌─SAR Encoder──────┐
│ (Priority modality)│  │ (Secondary)      │
│ Returns features + │  │ Returns features │
│ skip connections   │  │ (skips ignored)  │
└────────────────────┘  └──────────────────┘
  ↓                        ↓
┌─────Hierarchical Attention Fusion─────┐
│ DEM features guide SAR attention      │
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

1. **Hierarchical Attention Fusion**: DEM (priority modality) guides attention for SAR data
2. **Skip Connections**: Only DEM encoder provides skip connections to decoders
3. **Reduced Complexity**: Minimal parameter count among all variants
4. **Weather Independence**: SAR provides all-weather, day/night imaging capability
5. **Same Output Format**: Two task outputs with same spatial dimensions as input

## Data Requirements

The dataset expects patches with these keys:
- `'dem'`: Elevation data (2D array)
- `'sar'`: SAR backscatter data (2D array) - **now required**
- `'hydro_mask'`: Ground truth for task 1 (2D array)
- `'flow_dir'`: Ground truth for task 2 (2D array)

All spatial arrays must have the same H×W dimensions.

## Usage

### 1. Basic Model Usage

```python
from models.mdmt_dem_sar import MultimodalMultitaskModel_DEM_SAR
import torch

# Create model
model = MultimodalMultitaskModel_DEM_SAR(
    n_classes_task1=1,
    n_classes_task2=1,
    base_channels=64
)

# Forward pass
dem = torch.randn(batch_size, 1, H, W)
sar = torch.randn(batch_size, 1, H, W)  # SAR backscatter

pred_task1, pred_task2 = model(dem, sar)
```

### 2. Data Loading

```python
from data.patchDataLoader_dem_sar import create_dataloader_dem_sar

# Create data loader
dataloader = create_dataloader_dem_sar(
    base_path="/path/to/patch/data",
    huc_codes=["10020007", "10020008"],
    batch_size=8,
    shuffle=True
)

# Use in training loop
for batch in dataloader:
    dem = batch['dem']
    sar = batch['sar']
    hydro_mask = batch['hydro_mask']
    flow_dir = batch['flow_dir']
    
    pred1, pred2 = model(dem, sar)
    # ... calculate loss and train
```

### 3. Training

```bash
# Example training command (would need custom training script)
python training/train_dem_sar.py \
    --data_path /path/to/your/patch/data \
    --huc_codes 10020007 10020008 \
    --batch_size 8 \
    --epochs 50 \
    --lr 1e-3 \
    --output_dir ./outputs
```

### 4. Example and Testing

```bash
# Show compatibility guide only
python example_dem_sar.py --demo_only

# Run inference example (if you have data)
python example_dem_sar.py \
    --data_path /path/to/your/data \
    --huc_codes 10020007 \
    --model_path ./outputs/best_model_dem_sar.pth
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
from data.patchDataLoader_dem_sar import MultimodalPatchDataset_DEM_SAR
batch = dataset[0]
pred1, pred2 = model(batch['dem'], batch['sar'])
```

### Model Changes
```python
# OLD
from models.mdmt_v1 import MultimodalMultitaskModel
model = MultimodalMultitaskModel(...)

# NEW
from models.mdmt_dem_sar import MultimodalMultitaskModel_DEM_SAR  
model = MultimodalMultitaskModel_DEM_SAR(...)
```

## Benefits of DEM + SAR Model

1. **All-Weather Capability**: SAR works regardless of cloud cover and lighting conditions
2. **Surface Roughness Sensitivity**: SAR detects surface texture and moisture content
3. **Penetration Capability**: C-band SAR can penetrate vegetation to some extent
4. **Complementary to DEM**: Terrain height + backscatter intensity provide rich information
5. **Minimal Data Requirements**: Only 2 single-channel modalities needed
6. **Compact Model**: Smallest parameter count among all model variants

## SAR Data Characteristics

### Sentinel-1 SAR Specifications
- **Band**: C-band (5.405 GHz)
- **Polarization**: VV (vertical transmit, vertical receive)
- **Spatial Resolution**: ~10m ground range
- **Temporal Resolution**: 6-12 day revisit time
- **Coverage**: Global, systematic acquisition

### SAR Backscatter Interpretation
- **High Backscatter**: Rough surfaces, urban areas, calm water with wind
- **Low Backscatter**: Smooth surfaces, calm water, dense vegetation canopy
- **Water Detection**: Generally low backscatter, but wind and waves increase signal
- **Vegetation**: Variable backscatter depending on structure and moisture

## Model Parameters

- Input: DEM (1 channel) + SAR (1 channel)
- Output: 2 tasks, each 1 channel with same spatial dimensions as input
- Base channels: 64 (configurable)
- Total parameters: Minimal among all variants
- Architecture: Encoder-Fusion-Decoder with attention and skip connections

## Performance Considerations

- **Computational Efficiency**: Fastest training and inference due to minimal complexity
- **Memory Usage**: Lowest memory footprint of all model variants
- **Data Availability**: SAR data has consistent global coverage
- **Weather Independence**: No dependence on clear-sky conditions
- **Temporal Consistency**: Less affected by seasonal variations than optical data

## Use Cases

### Ideal Applications
1. **Flood Mapping**: SAR excellent for water detection, DEM provides flow context
2. **All-Weather Monitoring**: Continuous observation regardless of weather
3. **Arctic/Polar Regions**: Where optical data is often unavailable
4. **Rapid Response**: Quick assessment without waiting for clear conditions
5. **Change Detection**: Consistent SAR signatures enable temporal analysis

### Limitations
1. **Speckle Noise**: SAR inherently noisy, may require preprocessing
2. **Look Angle Effects**: Backscatter varies with sensor viewing geometry
3. **Limited Spectral Information**: Single polarization provides less information than multi-spectral optical
4. **Interpretation Complexity**: SAR signatures can be complex to understand

## Data Preprocessing Recommendations

### SAR Data
- **Radiometric Calibration**: Convert to sigma0 (backscatter coefficient)
- **Terrain Correction**: Use DEM for geometric and radiometric terrain correction  
- **Speckle Filtering**: Apply multi-looking or adaptive filtering
- **Logarithmic Scaling**: Convert to dB scale for better dynamic range

### DEM Data
- **Void Filling**: Ensure no data gaps in elevation model
- **Resampling**: Match SAR spatial resolution (typically 10m)
- **Coordinate System**: Ensure consistent projection with SAR data

## Notes

- DEM is treated as the priority modality providing skip connections
- SAR data is now required (not optional)
- The fusion mechanism uses hierarchical attention where DEM guides SAR processing
- Compatible with same training frameworks as the original model
- Particularly effective for water body detection and flow analysis
- Minimal computational requirements make it suitable for operational deployment