# Patch Visualization Fix Summary

## Issues Identified and Fixed

### 1. **Data Structure Mismatch**
**Problem**: The visualization code assumed data was in `(C, H, W)` format (channels-first), but actual patches store data in `(H, W)` or `(H, W, C)` format (channels-last).

**Original Code**:
```python
data = patch_data['dem'][0]  # Trying to access first channel - ERROR!
```

**Fixed Code**:
```python
data = patch_data['dem']  # Shape: (224, 224) - direct access
```

### 2. **RGB Composite Creation**
**Problem**: RGB composite function expected `(C, H, W)` format and was trying to transpose incorrectly.

**Original Code**:
```python
rgb = optical_data[[3,2,1], :, :]  # Channels-first indexing
rgb = np.transpose(rgb, (1, 2, 0))  # Wrong transpose
```

**Fixed Code**:
```python
rgb = optical_data[:, :, [3,2,1]]  # Channels-last indexing (H, W, C)
# No transpose needed!
```

### 3. **Data Normalization**
**Problem**: Missing proper data scaling for visualization - data ranges were causing display issues.

**Added**:
```python
# Min-max normalization for consistent visualization
data_norm = (data - data.min()) / (data.max() - data.min() + 1e-8)
```

## Actual Data Structure (Confirmed)

### Patch Data Format
```python
patch_data = {
    'dem': (224, 224),           # Single channel elevation
    'optical': (224, 224, 6),    # 6-band Landsat (B2-B7)
    'thermal': (224, 224),       # Single channel thermal
    'sar': (224, 224),          # Single channel SAR backscatter
    'alphaearth': (224, 224, 64), # 64-band embeddings
    'hydro_mask': (224, 224),    # Binary water mask
    'flow_dir': (224, 224)       # D8 flow direction codes
}
```

### Data Value Ranges (HUC 03030005 Example)
```
dem: 39.348 to 54.182 (elevation in meters)
optical: 0.003 to 0.749 (surface reflectance)
thermal: 0.000 to 322.993 (brightness temperature in Kelvin)
sar: -23.262 to 14.590 (backscatter in dB)
alphaearth: -0.455 to 0.498 (normalized embeddings)
flow_dir: -2 to 128 (D8 codes: 1,2,4,8,16,32,64,128 + no-data)
hydro_mask: 0 to 1 (binary: 0=land, 1=water)
```

## Visualization Improvements

### Before Fix
- ❌ Empty/blank panels with "Error/invalid shape" messages
- ❌ Only water mask and flow direction visible
- ❌ RGB composite not working

### After Fix
- ✅ All 7 modalities properly displayed
- ✅ Proper data scaling and normalization
- ✅ True-color RGB composite (Landsat bands 4-3-2)
- ✅ Colorbars showing data ranges
- ✅ Consistent visualization across patches

## File Generation

### Created Outputs
- **PNG**: High-resolution patch visualization (300 DPI)
- **PDF**: Vector format for publication
- **Sample Data**: Train/test splits with NPZ files and georeference templates

### File Locations
```
/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/
├── visualizations/
│   ├── patch_visualization_huc_03030005_samples_4.png
│   └── patch_visualization_huc_03030005_samples_4.pdf
└── sample_patches/
    ├── train/ (16 patches)
    ├── test/ (4 patches)
    └── normalization_stats.json
```

## Technical Notes

### Optical Band Mapping
- **Landsat Bands**: B2, B3, B4, B5, B6, B7 (stored as channels 0-5)
- **RGB Composite**: Uses bands 4-3-2 (Red-Green-Blue) → indices [3,2,1]
- **Natural Color**: Creates true-color composite for visualization

### D8 Flow Direction
- **Valid Codes**: 1, 2, 4, 8, 16, 32, 64, 128 (representing 8 cardinal directions)
- **Special Values**: -2 or 0 for no-data/flat areas
- **Colormap**: Custom discrete colormap with directional colors

### Data Processing Pipeline Compatibility
The fixed visualization now properly handles the exact data format produced by the National ML processing pipeline, ensuring compatibility with training scripts and model inputs.

---

**Status**: ✅ **PATCH VISUALIZATION FULLY FUNCTIONAL**  
**Result**: All 7 modalities now display correctly with proper scaling and colormaps  
**Next**: Ready for HUC 10020007 when data is regenerated