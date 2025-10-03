# Model Variants Overview

This document provides an overview of all available multimodal multitask model variants in the experiments folder.

## Available Model Variants

### 1. Full 4-Modality Models

#### `mdmt_v1.py` - Original 4-Modality Model
- **Input**: DEM (1) + Optical (3) + Thermal (1) + SAR (1) = 6 channels total
- **Architecture**: 4 encoders with hierarchical attention fusion
- **Use Case**: Research baseline with all available modalities
- **Parameters**: ~98M (highest complexity)

#### `mdmt_landsat_6b.py` - Enhanced Optical Model  
- **Input**: DEM (1) + Optical (6) + Thermal (1) + SAR (1) = 9 channels total
- **Architecture**: 4 encoders, enhanced optical with 6 Landsat bands
- **Use Case**: Full Landsat 8/9 exploitation (B2,B3,B4,B5,B6,B7)
- **Parameters**: ~98M

### 2. Simplified 2-Modality Models

#### `mdmt_dem_thermal.py` - DEM + Thermal
- **Input**: DEM (1) + Thermal (1) = 2 channels total
- **Architecture**: 2 encoders with hierarchical attention fusion
- **Use Case**: Thermal-based water detection, cloud-free analysis
- **Parameters**: ~98M
- **Status**: ✅ Complete with documentation

#### `mdmt_dem_optical.py` - DEM + Optical  
- **Input**: DEM (1) + Optical (configurable, default 6) = 7 channels total
- **Architecture**: 2 encoders with hierarchical attention fusion
- **Use Case**: Multi-spectral analysis, vegetation/land cover context
- **Parameters**: ~98M
- **Status**: ✅ Complete with documentation

#### `mdmt_dem_sar.py` - DEM + SAR
- **Input**: DEM (1) + SAR (1) = 2 channels total  
- **Architecture**: 2 encoders with hierarchical attention fusion
- **Use Case**: All-weather monitoring, flood detection
- **Parameters**: ~98M (minimal input channels)
- **Status**: ✅ Complete with documentation

### 3. Single-Modality Models

#### `mdmt_alphaearth_only.py` - AlphaEarth-Only
- **Input**: AlphaEarth embeddings (64 channels, configurable)
- **Architecture**: Single encoder with enhanced embedding processing
- **Use Case**: Foundation model evaluation, simplified data pipeline
- **Parameters**: ~98M
- **Status**: ✅ Complete with documentation

## Model Architecture Comparison

| Model | Modalities | Encoders | Input Channels | Weather Dependent | Spectral Info | Best Use Case |
|-------|------------|----------|----------------|-------------------|---------------|---------------|
| `mdmt_v1` | 4 | 4 | 6 | Partial | Moderate | Research baseline |
| `mdmt_landsat_6b` | 4 | 4 | 9 | Partial | High | Full Landsat exploitation |
| `mdmt_dem_thermal` | 2 | 2 | 2 | Yes | Low | Thermal water detection |
| `mdmt_dem_optical` | 2 | 2 | 7 | Yes | High | Multi-spectral analysis |
| `mdmt_dem_sar` | 2 | 2 | 2 | No | None | All-weather monitoring |
| `mdmt_alphaearth_only` | 1 | 1 | 64 | No | High (embedded) | Foundation model evaluation |

## Common Architecture Features

All models share these core design elements:

### 1. Hierarchical Attention Fusion
- DEM is always the **priority modality** providing skip connections
- Secondary modalities are guided by DEM-based attention mechanisms
- Learned attention weights determine importance of secondary features

### 2. U-Net Style Architecture
- **Encoders**: Downsampling with skip connections (priority modality only)
- **Fusion**: Hierarchical attention-based feature combination
- **Decoders**: Upsampling with skip connections for each task

### 3. Multi-task Learning
- **Task 1**: Water segmentation (binary classification)
- **Task 2**: D8 flow direction prediction (8-class classification)
- Shared encoder with task-specific decoders

### 4. Dynamic Loss Weighting
- Uncertainty-based automatic task weighting
- Learnable log-variance parameters
- Prevents task dominance during training

## Data Loaders

Each model variant has a corresponding specialized data loader:

| Model | Data Loader | Required Keys |
|-------|-------------|---------------|
| `mdmt_v1` | `patchDataLoader.py` | `m1`, `m2`, `m3`, `m4` |
| `mdmt_landsat_6b` | `patchDataLoader_ls6b.py` | `m1`, `m2`, `m3`, `m4` |
| `mdmt_dem_thermal` | `patchDataLoader_dem_thermal.py` | `dem`, `thermal` |
| `mdmt_dem_optical` | `patchDataLoader_dem_optical.py` | `dem`, `optical` |
| `mdmt_dem_sar` | `patchDataLoader_dem_sar.py` | `dem`, `sar` |
| `mdmt_alphaearth_only` | `patchDataLoader_alphaearth_only.py` | `alphaearth` |

All data loaders support:
- Per-HUC normalization using `normalization_stats.json`
- Automatic data validation and filtering
- Missing data handling (zero-filling for optional modalities)

## Example Scripts

Each model variant includes a complete example script:

- `example_dem_thermal.py` - DEM + Thermal usage examples
- `example_dem_optical.py` - DEM + Optical usage examples  
- `example_dem_sar.py` - DEM + SAR usage examples
- `example_alphaearth_only.py` - AlphaEarth-only usage examples

Example script features:
- Model loading and inference demonstration
- Data compatibility guides
- Migration instructions from 4-modality models
- Command-line interface for testing

## Training Integration

All model variants are compatible with the existing training infrastructure:

### Lightning Training Module (`train_mdmt_lightning.py`)
- Supports all model architectures
- Dynamic loss weighting
- Comprehensive metrics (Dice, IoU, accuracy)
- W&B logging with visualizations

### Data Module (`data_module.py`)  
- Automatic class balancing
- Train/validation splitting
- Statistical analysis for weighting

### Training Script (`run_lightning_train.py`)
- Configurable hyperparameters
- SLURM job submission support
- Checkpoint management

## Model Selection Guide

### Choose DEM + Thermal when:
- Thermal data is primary focus
- Cloud-free conditions available
- Temperature/thermal patterns important
- Minimal computational resources

### Choose DEM + Optical when:
- Multi-spectral analysis needed
- Vegetation/land cover context important
- Flexible channel configuration required
- Rich visual information available

### Choose DEM + SAR when:
- All-weather monitoring required
- Cloud cover is frequent issue
- Rapid response needed
- Surface roughness/texture important
- Minimal data requirements

### Choose AlphaEarth-only when:
- Leveraging foundation model capabilities
- Simplified data pipeline needed
- Research into embedding effectiveness
- Consistent global data coverage required
- State-of-the-art satellite embeddings available

### Choose 4-Modality models when:
- Maximum performance required
- All modalities available
- Research/comparison purposes
- Computational resources not constrained

## Performance Expectations

### Computational Complexity (relative)
1. **DEM + SAR**: Fastest (minimal input channels)
2. **DEM + Thermal**: Fast (2 single channels)  
3. **DEM + Optical**: Moderate (6+ optical channels)
4. **AlphaEarth-only**: Moderate-High (64 embedding channels)
5. **4-Modality**: Slowest (9 total channels)

### Data Requirements (relative)
1. **DEM + SAR**: Minimal (2 modalities, weather-independent)
2. **DEM + Thermal**: Low (2 modalities, weather-dependent)
3. **DEM + Optical**: Moderate (2 modalities, weather-dependent)
4. **AlphaEarth-only**: Low (1 modality, pre-processed embeddings)
5. **4-Modality**: High (4 modalities, complex data pipeline)

### Expected Performance (for water/flow tasks)
- All models should achieve similar performance given proper training
- 2-modality models may have slight performance reduction vs 4-modality
- SAR-based models excel in cloudy/adverse conditions
- Optical-based models excel in clear conditions with rich spectral information

## Future Extensions

Potential additional model variants:
- **DEM + Thermal + SAR**: 3-modality all-weather with thermal
- **Optical + SAR**: Multi-spectral + radar fusion (no elevation)
- **Temporal variants**: Multi-temporal fusion for change detection
- **Multi-scale variants**: Hierarchical spatial processing