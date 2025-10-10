# All Modalities + AlphaEarth MDMT Implementation Summary

## Overview

This document describes the **All Modalities + AlphaEarth** variant of the MultiModal MultiTask (MDMT) model - the most comprehensive v### Performance Characteristics

### Memory Usage
- **64-channel AlphaEarth**: ~12-14GB GPU memory that combines all available satellite modalities with Google's AlphaEarth foundation model embeddings.

## Architecture Summary

### Input Modalities (Total: 73 channels by default)

1. **DEM (1 channel)** - Digital Elevation Model
   - Topographic information for terrain understanding
   - **Priority modality** - provides skip connections to decoders
   - Per-HUC Z-score normalization

2. **Optical (6 channels)** - Landsat 8/9 Bands B2-B7
   - B2 (Blue): 0.452-0.512 μm
   - B3 (Green): 0.533-0.590 μm  
   - B4 (Red): 0.636-0.673 μm
   - B5 (NIR): 0.851-0.879 μm
   - B6 (SWIR1): 1.566-1.651 μm
   - B7 (SWIR2): 2.107-2.294 μm
   - Per-HUC per-band Z-score normalization

3. **Thermal (1 channel)** - Landsat Band B10
   - B10 (Thermal): 10.60-11.19 μm
   - Surface temperature information
   - Per-HUC Z-score normalization

4. **SAR (1 channel)** - Sentinel-1 VV Polarization
   - Synthetic Aperture Radar data
   - Weather-independent surface information
   - Per-HUC Z-score normalization

5. **AlphaEarth (64 channels)** - Google's Satellite Embeddings
   - Pre-trained foundation model embeddings
   - Rich contextual Earth surface patterns
   - No normalization (pre-processed embeddings)
   - Fixed at 64 channels

### Model Architecture

```python
MultimodalMultitaskModel_All_Modalities_AlphaEarth(
    n_classes_task1=1,           # Water segmentation (binary)
    n_classes_task2=8,           # D8 flow direction (8 classes)
    base_channels=64             # AlphaEarth fixed at 64 channels
)
```

**Architecture Flow:**
1. **Separate Encoders**: Each modality processed through dedicated U-Net encoder
2. **Hierarchical Attention Fusion**: DEM as primary, others as secondary with learned attention
3. **Shared Deep Encoder**: 3-layer deep processing for comprehensive multi-modal integration
4. **Task-Specific Decoders**: Separate decoders for water segmentation and D8 flow direction

**Key Features:**
- **Total Parameters**: ~200M (varies slightly with AlphaEarth channels)
- **Model Size**: ~763 MB
- **Priority Skip Connections**: Only DEM skip connections used in decoders
- **Attention Mechanism**: Learned attention weights for each secondary modality

## File Structure

### Core Implementation Files

```
experiments/
├── models/
│   └── mdmt_all_modalities_alphaearth.py        # Model architecture
├── data/
│   └── patchDataLoader_all_modalities_alphaearth.py  # Data loading pipeline
├── training/
│   ├── train_all_modalities_alphaearth_lightning.py  # Lightning training module
│   ├── run_lightning_train_all_modalities_alphaearth.py  # CLI training script
│   ├── submit_train_all_modalities_alphaearth_single.sh  # Single SLURM job
│   └── submit_train_all_modalities_alphaearth_array.sh   # Array SLURM jobs
└── example_all_modalities_alphaearth.py         # Test/demo script
```

### Training Infrastructure

**Lightning Module**: `MDMT_All_Modalities_AlphaEarth_LitModule`
- Multi-task loss with configurable scaling
- Dynamic uncertainty weighting between tasks
- Comprehensive metrics (Dice, IoU, accuracy)
- Rich validation visualizations (2x5 panel layout)

**Data Module**: `AllModalitiesAlphaEarthDataModule`
- Per-HUC normalization for each modality
- Fixed 64-channel AlphaEarth support
- Efficient batch loading with proper data validation

## Usage Instructions

### 1. Test Architecture

```bash
cd /u/nathanj/national_ml/experiments
python example_all_modalities_alphaearth.py --demo_only
```

### 2. Single Training Run

```bash
cd /u/nathanj/national_ml/experiments/training
sbatch submit_train_all_modalities_alphaearth_single.sh
```

### 3. Array Training (10 runs with different configs)

```bash
cd /u/nathanj/national_ml/experiments/training
sbatch submit_train_all_modalities_alphaearth_array.sh
```

### 4. Custom Training

```bash
python run_lightning_train_all_modalities_alphaearth.py \
    --hucs "10020007,03030005" \
    --epochs 50 \
    --batch_size 4 \
    --lr 5e-4 \
    --water_loss_scale 1.0 \
    --d8_loss_scale 0.5
```

## Training Configuration

### Default Settings
- **Batch Size**: 4 (reduced due to high memory requirements from 73 channels)
- **Learning Rate**: 5e-4 (reduced for stability)
- **Precision**: 32-bit (for numerical stability)
- **Epochs**: 50
- **Early Stopping**: 15 epochs patience
- **Gradient Clipping**: 1.0

### Resource Requirements
- **GPU Memory**: ~12-16GB (depending on batch size and AlphaEarth channels)
- **System RAM**: 100GB
- **CPUs**: 16 cores
- **Training Time**: ~24-48 hours per 50 epochs

### Array Job Configurations
The array job tests different combinations:
- **Seeds**: 10 different random seeds for reproducibility
- **Learning Rates**: [1e-3, 5e-4, 2e-4, 1e-4, 5e-5]
- **Batch Sizes**: [4, 6, 8] (memory-constrained)
- **AlphaEarth Channels**: Fixed at 64 channels

## Data Requirements

### Input Data Keys
Each patch file must contain:
```python
{
    'dem': np.ndarray,          # (H, W) elevation data
    'optical': np.ndarray,      # (H, W, 6) Landsat B2-B7 
    'thermal': np.ndarray,      # (H, W) Landsat B10
    'sar': np.ndarray,          # (H, W) Sentinel-1 VV
    'alphaearth': np.ndarray,   # (H, W, C) embeddings
    'hydro_mask': np.ndarray,   # (H, W) water mask
    'flow_dir': np.ndarray,     # (H, W) D8 flow direction
}
```

### Normalization Requirements
Per-HUC `normalization_stats.json` should contain:
```json
{
    "dem": {
        "elevation_mean": float,
        "elevation_stdDev": float
    },
    "landsat": {
        "landsat_mean": [float, ...],      // 6 values for B2-B7
        "landsat_stdDev": [float, ...],    // 6 values for B2-B7
        "landsat_B10_mean": float,
        "landsat_B10_stdDev": float
    },
    "sentinel1": {
        "sentinel1_VV_mean": float,
        "sentinel1_VV_stdDev": float
    }
}
```

## Logging and Monitoring

### W&B Project
- **Project Name**: `national_ml_all_modalities_alphaearth`
- **Tags**: `["all_modalities", "alphaearth", "ae64", "seed42"]`
- **Experiment Naming**: `all_modalities_alphaearth_{hucs}_seed{seed}`

### Logged Metrics
- **Losses**: Water segmentation, D8 flow direction, total combined
- **Water Metrics**: Dice coefficient, IoU
- **D8 Metrics**: Micro/macro accuracy, invalid percentage
- **Uncertainty**: Dynamic loss weighting log-variances
- **Visualizations**: Multi-panel validation samples every 10 epochs

### Visualization Panels
2x5 grid showing:
- **Top Row**: DEM | Optical RGB | Thermal | SAR | AlphaEarth RGB
- **Bottom Row**: AlphaEarth Avg | Water GT | Water Pred | D8 GT | D8 Pred

## Performance Characteristics

### Memory Usage
- **32-channel AlphaEarth**: ~8-10GB GPU memory
- **64-channel AlphaEarth**: ~12-14GB GPU memory  
- **128-channel AlphaEarth**: ~16-20GB GPU memory

### Model Complexity
- **Parameters**: ~200M (slightly varies with AlphaEarth channels)
- **FLOPs**: Very high due to 73 input channels and deep architecture
- **Training Speed**: Slower than simpler variants due to comprehensive processing

## Comparison with Other Variants

| Variant | Input Channels | Key Strength | Use Case |
|---------|---------------|--------------|----------|
| DEM + Optical | 7 | Balanced performance | General purpose |
| DEM + SAR | 2 | Weather independence | Cloud-prone regions |
| AlphaEarth Only | 64 | Foundation model power | Rapid deployment |
| **All Modalities + AlphaEarth** | **73** | **Maximum information** | **Research/benchmarking** |

## Expected Applications

### Research Applications
- **Ablation Studies**: Compare contribution of each modality
- **Benchmark Performance**: Maximum achievable performance with current data
- **Method Development**: Baseline for new fusion approaches

### Operational Applications
- **High-Stakes Applications**: Where maximum accuracy is crucial
- **Reference Standard**: Gold standard for hydrographic mapping
- **Multi-Sensor Fusion Research**: Understanding optimal sensor combinations

## Integration Notes

This comprehensive variant follows the established MDMT pattern:
- **Consistent API**: Same interface as other variants
- **Standard Metrics**: Comparable evaluation framework
- **Modular Design**: Easy to modify or extend
- **Documentation**: Comprehensive logging and monitoring

The All Modalities + AlphaEarth variant represents the pinnacle of the MDMT architecture, providing the most comprehensive satellite-based hydrographic analysis possible with current data sources.