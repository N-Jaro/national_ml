# Patch Visualization System

## Overview

This directory contains tools for visualizing and analyzing patch data from the National ML multimodal deep learning pipeline. Since HUC 10020007 hasn't been processed yet, we use available processed HUCs to demonstrate the patch visualization capabilities.

## Generated Files

### Visualization Scripts
- **`create_patch_visualization.py`** - Comprehensive patch visualization tool
- **`quick_patch_viz.py`** - Simple command-line interface for quick visualizations

### Sample Data
- **`sample_patches/`** - Random sample patches from HUC 03030005
  - **`train/`** - 16 training patches (80% split)
  - **`test/`** - 4 testing patches (20% split)
  - **`normalization_stats.json`** - Per-HUC normalization statistics

### Visualizations
- **`patch_visualization_huc_03030005_samples_4.png`** - 4-patch visualization (300 DPI)
- **`patch_visualization_huc_03030005_samples_4.pdf`** - Vector PDF version

## Patch Data Structure

Each patch contains multiple modalities:

### Input Data (224×224 pixels each)
- **`dem`**: Digital Elevation Model (1 band, 10m resolution)
- **`optical`**: Landsat optical bands (6 bands: B2-B7, 30m resolution)
- **`thermal`**: Landsat thermal band (1 band: B10, 30m resolution)  
- **`sar`**: Sentinel-1 SAR backscatter (1 band, 10m resolution)
- **`alphaearth`**: Google satellite embeddings (64 bands, 10m resolution)

### Ground Truth Labels (224×224 pixels each)
- **`hydro_mask`**: Binary water segmentation mask (0=land, 1=water)
- **`flow_dir`**: D8 flow direction codes (1,2,4,8,16,32,64,128)

### Georeference Data
- **`patch_X_georef_template.tif`**: Georeference template for spatial reconstruction

## Usage Examples

### Basic Visualization
```bash
# Create visualization for HUC 03030005 (4 random patches)
cd /u/nathanj/national_ml/data/pub_viz_data/huc_10020007
conda run -n pytorch_gpu_cu118 python create_patch_visualization.py
```

### Custom Parameters
```bash
# Visualize 6 patches from a different HUC
conda run -n pytorch_gpu_cu118 python quick_patch_viz.py --huc 03040206 --num_patches 6

# Also copy 50 sample patches for analysis
conda run -n pytorch_gpu_cu118 python quick_patch_viz.py --huc 03040206 --copy_patches --copy_num 50
```

### Available HUCs
Currently processed HUCs available for visualization:
```
03030005, 03040206, 03050108, 03160106, 03160113, 04060102, 05040003, 
05080002, 05090104, 07030005, 07040006, 07130003, 07130004, 07140202, 
08020205, 08020301, 10120203, 10130306, 10170204, 10260010, 10270104, 
11020004, 12070101, 12090104, 12090202, 12090302, 13040209, 13060003, 
13070007, 14010005, 14060004, 16040204, 17010203, 17060109, 17090011, 
17100206, 17110012, 18020111, 18020126, 18020151, 18070103, 18070107, 
18080003, 19020504, 19050105, 19050401, 19080204, 19080302, 19090102
```

## Visualization Features

### Multi-Modal Display
The visualization shows 7 panels per patch:
- **(a) DEM**: Terrain colormap showing elevation
- **(b) Optical RGB**: True color composite (Landsat bands 4-3-2)
- **(c) SAR**: Grayscale SAR backscatter
- **(d) Thermal**: Hot colormap for thermal infrared
- **(e) Water Mask**: Binary mask (gray=land, blue=water)
- **(f) Flow Direction**: D8 flow direction with directional colors
- **(g) Composite**: RGB with water mask overlay

### Quality Features
- **Proper Normalization**: Uses per-HUC statistics for consistent scaling
- **Publication Ready**: 300 DPI PNG + vector PDF outputs
- **Reproducible**: Fixed random seeds for consistent results
- **Geospatial Context**: Maintains spatial relationships

## Patch Statistics (HUC 03030005 Example)

### Data Volume
- **Total Patches**: 565 patches
- **Patch Size**: 224×224 pixels each
- **Modalities**: 7 different data types per patch
- **File Size**: ~2-5 MB per patch (NPZ compressed)

### Spatial Coverage
- **Patch Resolution**: Multi-resolution (10m-30m depending on source)
- **Patch Stride**: 224 pixels (no overlap)
- **Coordinate System**: EPSG:5070 (Albers Equal Area Conic)

### Data Quality
- **Cloud Masking**: Applied to optical data
- **Normalization**: Per-HUC Z-score normalization
- **Georeference**: Each patch has spatial template for reconstruction

## Integration with National ML Pipeline

### Training Pipeline
```python
# Example usage in training scripts
from create_patch_visualization import load_patch_data, load_normalization_stats

# Load patch for training
patch_data = load_patch_data('sample_patches/train/patch_200.npz')
stats = load_normalization_stats('sample_patches/train/normalization_stats.json')

# Extract modalities
dem = patch_data['dem']           # Shape: (1, 224, 224)
optical = patch_data['optical']   # Shape: (6, 224, 224)
sar = patch_data['sar']          # Shape: (1, 224, 224)
hydro_mask = patch_data['hydro_mask']  # Shape: (224, 224)
flow_dir = patch_data['flow_dir']      # Shape: (224, 224)
```

### Model Input Format
The patches are designed to work with the project's multimodal models:
- **DEM**: Single channel elevation data
- **Optical**: 6-band Landsat reflectance  
- **Thermal**: Single channel temperature
- **SAR**: Single channel backscatter
- **AlphaEarth**: 64-band semantic embeddings

## Future Extensions

### When HUC 10020007 is Processed
Once HUC 10020007 data becomes available, you can:
```bash
# Visualize HUC 10020007 patches
python quick_patch_viz.py --huc 10020007 --num_patches 8

# Create comprehensive visualization
python create_patch_visualization.py  # (modify default HUC in script)
```

### Additional Visualizations
Potential enhancements:
- **Attention Maps**: Overlay model attention on patches
- **Prediction Comparison**: Show model predictions vs ground truth
- **Temporal Analysis**: Compare patches across different time periods
- **Multi-HUC Comparison**: Side-by-side comparison of different watersheds

## Troubleshooting

### Common Issues
1. **Missing Environment**: Ensure `pytorch_gpu_cu118` environment is activated
2. **Memory Issues**: Reduce `num_patches` if running out of memory
3. **Missing HUC**: Check available HUCs with `--huc invalid_huc` to see list

### Dependencies
All required packages are in the `pytorch_gpu_cu118` environment:
- `numpy`, `matplotlib` - Core visualization
- `json`, `pathlib` - Data handling
- `PIL` - Image processing (if needed)

---

**Created**: October 13, 2025  
**Purpose**: Patch visualization and analysis for multimodal deep learning pipeline  
**Data Source**: Processed patches from National ML HUC dataset