# Multi-Layer Attention Analysis

This directory contains individual scripts for generating attention maps across different architectural layers for each model variant.

## Variant-Specific Scripts

- `attention_analysis_landsat6b.py`: **Landsat6b model** - Multi-modal with DEM+Optical+Thermal+SAR (9 channels)
- `attention_analysis_dem_only.py`: **DEM-only model** - DEM only (1 channel)
- `attention_analysis_alphaearth_only.py`: **AlphaEarth-only model** - AlphaEarth features (64 channels)
- `attention_analysis_dem_optical.py`: **DEM+Optical model** - DEM+Optical (7 channels)

## Supporting Files

- `selected_patches.py`: Contains curated patches for analysis
- `checkpoint_utils.py`: Utilities for loading model checkpoints  
- `intelligent_patch_selector.py`: Smart patch selection logic

## Usage

```bash
conda activate gradcam_env

# For landsat6b model (working)
python attention_analysis_landsat6b.py

# For other variants (need checkpoint paths updated)
python attention_analysis_dem_only.py
python attention_analysis_alphaearth_only.py
python attention_analysis_dem_optical.py
```

## Output

Results are saved in `real_gradcam_tests/` with detailed naming:
`multi_layer_attention_{variant}_{huc_id}_{patch_filename}_{timestamp}.png`

## Architecture-Specific Analysis

Each variant analyzes different layers based on its architecture:
- **Single-encoder models**: Standard U-Net layers (encoder → decoder)
- **Multi-modal models**: Dual encoders + fusion layer + decoder
- **Resolution tracking**: 224×224 → 14×14 → 224×224 progression

Each layer shows attention patterns at different spatial resolutions with automatic artifact detection and correction.

## Data Normalization

All scripts now properly normalize input data using **z-score normalization** with per-HUC statistics from `normalization_stats.json` files:

- **DEM**: `(value - elevation_mean) / elevation_stdDev`
- **Optical**: Per-band normalization using `SR_B2_mean/stdDev` through `SR_B7_mean/stdDev`
- **Thermal**: `(value - ST_B10_mean) / ST_B10_stdDev`
- **SAR**: `(value - VV_mean) / VV_stdDev`
- **AlphaEarth**: Per-channel z-score normalization

This ensures the models receive properly normalized inputs identical to training conditions.

## Notes

- Only `attention_analysis_landsat6b.py` currently has a working checkpoint path
- Other scripts need checkpoint paths updated to their respective trained models
- Each variant handles different input modalities and channel configurations
- **Normalization significantly improves attention map quality** (more active pixels, fewer artifacts)

## Actual Data Structure
Based on patch files (.npz), available data keys are:
- `dem`: (224, 224) - Digital Elevation Model
- `optical`: (224, 224, 6) - Landsat optical bands
- `thermal`: (224, 224) - Landsat thermal band
- `sar`: (224, 224) - SAR data
- `alphaearth`: (224, 224, 64) - AlphaEarth foundation model features
- `flow_dir`: (224, 224) - Flow direction
- `hydro_mask`: (224, 224) - Ground truth water mask