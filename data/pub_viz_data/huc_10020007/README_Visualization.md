# HUC 10020007 Publication Visualization

## Overview

This directory contains publication-quality visualizations of the multimodal input data and reference labels for HUC 10020007 (Madison River Basin, Montana).

## Generated Files

### Visualization Outputs
- **`huc_10020007_publication_multimodal_visualization.png`** - High-resolution PNG (300 DPI) for digital use
- **`huc_10020007_publication_multimodal_visualization.pdf`** - Vector PDF for print publication

### Scripts
- **`create_publication_visualization.py`** - Main script that generates the publication figure
- **`display_visualization.py`** - Simple script to preview the generated visualization

## Figure Layout

The visualization is arranged in a 3×3 grid with the following panels:

### (a) Digital Elevation Model (DEM)
- **Source**: USGS 3DEP
- **Resolution**: 10m
- **Data**: Elevation in meters
- **Visualization**: Terrain colormap with HUC boundary overlay
- **File**: `huc_10020007_dem_10m.tif`

### (b) SAR (Sentinel-1)
- **Source**: Sentinel-1 VV polarization
- **Resolution**: 10m  
- **Data**: Median backscatter (July 2023)
- **Visualization**: Grayscale with percentile stretch
- **File**: `huc_10020007_sar_10m_2023-07-01_2023-07-31_median.tif`

### (c) Optical (Landsat 8/9)
- **Source**: Landsat 8/9 surface reflectance
- **Resolution**: 30m
- **Data**: True color RGB composite (July 2023)
- **Visualization**: Bands 4-3-2 (Red-Green-Blue)
- **File**: `huc_10020007_landsat_30m_2023-07-01_2023-07-31.tif`

### (d) Thermal (Landsat Band 10)
- **Source**: Landsat 8/9 thermal infrared
- **Resolution**: 30m
- **Data**: Land surface temperature (July 2023)
- **Visualization**: Hot colormap
- **File**: `huc_10020007_landsat_30m_2023-07-01_2023-07-31.tif` (Band 6)

### (e) AlphaEarth Embeddings
- **Source**: Google Earth satellite embeddings
- **Resolution**: 10m
- **Data**: 16-band semantic embeddings (2024)
- **Visualization**: First band with viridis colormap
- **File**: `huc_10020007_alphaearth_10m_2024_16bands.tif`

### (f) Water Segmentation Mask
- **Source**: NHD Plus HR hydrography
- **Resolution**: 10m
- **Data**: Binary water/no-water mask
- **Visualization**: Gray (land) and blue (water)
- **File**: `huc_10020007_hydro_mask_10m.tif`

### (g) D8 Flow Direction
- **Source**: D8 algorithm (PySheds)
- **Resolution**: 10m
- **Data**: Flow direction codes (1,2,4,8,16,32,64,128)
- **Visualization**: Custom D8 colormap
- **File**: `huc_10020007_flow_direction_10m.tif`

## Data Summary

| Panel | Modality | Size | Dimensions | Resolution | Bands |
|-------|----------|------|------------|------------|-------|
| (a) | DEM | 1,183.5 MB | 15,206 × 20,401 | 10m | 1 |
| (b) | SAR | 7,565.5 MB | 18,610 × 14,874 | 10m | 6 |
| (c) | Optical | 1,302.8 MB | 4,685 × 5,205 | 30m | 7 |
| (d) | Thermal | 1,302.8 MB | 4,685 × 5,205 | 30m | 7 |
| (e) | AlphaEarth | 29,999.9 MB | 14,056 × 15,615 | 10m | 16 |
| (f) | Water Mask | 8.3 MB | 15,206 × 20,401 | 10m | 1 |
| (g) | Flow Direction | 2,400.0 MB | 15,206 × 20,401 | 10m | 1 |

**Total Data Volume**: ~44 GB

## Usage

### Generate New Visualization
```bash
cd /u/nathanj/national_ml/data/pub_viz_data/huc_10020007
conda run -n pytorch_gpu_cu118 python create_publication_visualization.py
```

### Preview Generated Visualization
```bash
conda run -n pytorch_gpu_cu118 python display_visualization.py
```

## Technical Details

### Coordinate System
- **CRS**: UTM Zone 12N (EPSG:32612)
- **HUC**: 10020007 (Madison River Basin)
- **Location**: Montana, USA

### Color Schemes
- **DEM**: Terrain colormap (matplotlib)
- **SAR**: Grayscale with 2-98% percentile stretch
- **Optical**: True color composite with percentile normalization
- **Thermal**: Hot colormap (matplotlib)
- **AlphaEarth**: Viridis colormap (matplotlib)
- **Water Mask**: Binary (lightgray=land, blue=water)
- **Flow Direction**: Custom D8 colormap with direction-specific colors

### Processing Notes
- All rasters are reprojected to UTM Zone 12N for consistency
- HUC boundary overlay is applied to the DEM panel (a)
- Percentile stretching is applied where appropriate for visualization
- NoData values are properly masked
- All panels maintain consistent spatial extent

## Publication Guidelines

### Figure Caption Template
```
Figure X. Multimodal input data and reference labels for HUC 10020007 (Madison River Basin, Montana). 
(a) Digital elevation model with HUC boundary (red outline), (b) Sentinel-1 SAR backscatter, 
(c) Landsat true color composite, (d) Landsat thermal infrared, (e) AlphaEarth satellite embeddings, 
(f) water segmentation mask (ground truth), and (g) D8 flow direction. All data are displayed in 
UTM Zone 12N coordinates with 10-30m spatial resolution depending on the source.
```

### File Recommendations
- **Digital/Web**: Use PNG version (300 DPI)
- **Print/Publication**: Use PDF version (vector graphics)
- **Maximum Width**: Suitable for full-width figure (2 columns)

## Dependencies

The visualization requires the following Python packages:
- `rasterio` - Geospatial raster I/O
- `geopandas` - Vector data handling
- `matplotlib` - Plotting and visualization  
- `numpy` - Array processing

These are included in the `pytorch_gpu_cu118` conda environment.

---

Created: October 13, 2025
Environment: pytorch_gpu_cu118
Purpose: Scientific publication figure generation