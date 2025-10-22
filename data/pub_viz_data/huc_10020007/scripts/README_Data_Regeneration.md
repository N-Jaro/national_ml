# HUC 10020007 Data Regenerat| File Name | Description | Resolution | Bands |
|-----------|-------------|------------|-------|
| `huc_10020007_dem_10m.tif` | USGS 3DEP Digital Elevation Model | 10m | 1 |
| `huc_10020007_landsat_30m_2023-07-01_2023-07-31.tif` | Landsat 8/9 composite (optical + thermal) | 30m | 7 |
| `huc_10020007_sar_10m_2023-07-01_2023-07-31_median.tif` | Sentinel-1 VV backscatter median | 10m | 1 |
| `huc_10020007_alphaearth_10m_2023_16bands.tif` | Google AlphaEarth embeddings (2023, visualization subset) | 10m | 16 |
| `huc_10020007_hydro_mask_10m.tif` | Water segmentation mask | 10m | 1 |
| `huc_10020007_flow_direction_10m.tif` | Flow direction (HydroSHEDS) | 10m | 1 |pts

## Overview

This folder contains scripts to regenerate the corrupted data for HUC 10020007 (Madison River Basin, Montana) using Google Earth Engine.

## Files Created

### 1. `regenerate_huc_10020007_data.py`
- **Type**: Python script for local execution
- **Requirements**: `earthengine-api` Python package
- **Features**: 
  - Comprehensive error handling
  - Extended date range fallbacks
  - Detailed progress reporting
  - Automatic authentication handling

### 2. `regenerate_huc_10020007_gee_code_editor.js`
- **Type**: JavaScript for GEE Code Editor
- **Requirements**: Web browser access to Google Earth Engine
- **Features**:
  - Copy-paste ready for Code Editor
  - Visual map display
  - Simplified but complete processing

## Data Products Generated

Both scripts will generate the following files:

| File Name | Description | Resolution | Bands |
|-----------|-------------|------------|-------|
| `huc8_10020007_boundary.geojson` | HUC8 boundary vector (exact watershed) | Vector | N/A |
| `huc_10020007_dem_10m.tif` | USGS 3DEP Digital Elevation Model | 10m | 1 |
| `huc_10020007_landsat_30m_2023-07-01_2023-07-31.tif` | Landsat 8/9 composite (optical + thermal) | 30m | 7 |
| `huc_10020007_sar_10m_2023-07-01_2023-07-31_median.tif` | Sentinel-1 VV backscatter median | 10m | 1 |
| `huc_10020007_alphaearth_10m_2023_16bands.tif` | Google AlphaEarth embeddings (visualization subset) | 10m | 16 |
| `huc_10020007_hydro_mask_10m.tif` | Water segmentation mask | 10m | 1 |
| `huc_10020007_flow_direction_10m.tif` | Flow direction (HydroSHEDS) | 10m | 1 |

## Method 1: Using Python Script (Recommended)

### Prerequisites
```bash
# Install Google Earth Engine API
pip install earthengine-api

# Or in the project environment
conda activate pytorch_gpu_cu118
pip install earthengine-api
```

### Authentication (First Time Only)
```bash
# Authenticate with Google Earth Engine
earthengine authenticate
```

### Run the Script
```bash
cd /u/nathanj/national_ml/data/pub_viz_data/huc_10020007/scripts
python regenerate_huc_10020007_data.py
```

### Expected Output
```
🎯 Processing HUC8: 10020007 (Madison River Basin, Montana)
📅 Primary date range: 2023-07-01 to 2023-07-31
🗺️  Target CRS: EPSG:32612 (UTM Zone 12N)
🌍 Loading HUC8 boundary...
✅ Region loaded: Madison River Basin (Montana)
🏔️  Processing DEM (USGS 3DEP 10m)...
   ✅ DEM processed successfully
🛰️  Processing Landsat 8/9 data...
   📊 Found X Landsat scenes
   ✅ Landsat composite created (7 bands total)
[... continues ...]
🚀 Starting export tasks...
✅ Successfully started: 6 tasks
📁 All files will be saved to Google Drive folder: 'National_ML_HUC_10020007_Regenerated'
```

## Method 2: Using GEE Code Editor

### Steps
1. **Open Google Earth Engine Code Editor**
   - Go to: https://code.earthengine.google.com/
   - Sign in with your Google account

2. **Copy the JavaScript Code**
   - Open `regenerate_huc_10020007_gee_code_editor.js`
   - Copy the entire contents

3. **Paste and Run**
   - Paste the code into the Code Editor
   - Click "Run" button
   - The map will center on HUC 10020007 with boundary overlay

4. **Monitor Tasks**
   - Click on "Tasks" tab (right panel)
   - You'll see 6 export tasks starting
   - Click "RUN" for each task to confirm export

## Monitoring Export Progress

### In GEE Code Editor
- Go to "Tasks" tab
- Tasks will show status: READY → RUNNING → COMPLETED
- Failed tasks will show error messages

### Export Times (Approximate)
- **DEM**: 10-15 minutes
- **Landsat**: 15-20 minutes  
- **SAR**: 20-30 minutes
- **AlphaEarth**: 30-45 minutes (16 bands for visualization)
- **Water Mask**: 5-10 minutes
- **Flow Direction**: 10-15 minutes

## Downloading and Installing Data

### 1. Download from Google Drive
- Go to Google Drive
- Find folder: `National_ML_HUC_10020007_Regenerated`
- Download all 6 `.tif` files

### 2. Move to Project Directory
```bash
# Create directory if it doesn't exist
mkdir -p /u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data

# Move downloaded files (adjust path as needed)
mv ~/Downloads/huc_10020007_*.tif /u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/
```

### 3. Verify Data Integrity
```bash
cd /u/nathanj/national_ml/data/pub_viz_data/huc_10020007
conda run -n pytorch_gpu_cu118 python create_publication_visualization.py
```

## Troubleshooting

### Common Issues

**1. Authentication Errors**
```bash
# Re-authenticate
earthengine authenticate
```

**2. Memory/Timeout Errors**
- Large regions may timeout
- GEE will automatically retry
- Files are saved incrementally

**3. Missing Data Products**
- Some dates may have no imagery
- Scripts use extended date ranges as fallback
- Check console output for warnings

**4. Download Issues**
- Large files (>2GB) may need multiple attempts
- Use Google Drive desktop app for large downloads
- Check available Google Drive storage space

### Expected File Sizes
- **DEM**: ~1.2 GB
- **Landsat**: ~1.3 GB  
- **SAR**: ~7.6 GB
- **AlphaEarth**: ~30 GB (16 bands for visualization)
- **Water Mask**: ~8 MB
- **Flow Direction**: ~2.4 GB

**Total**: ~45-50 GB

## Data Specifications

### Coordinate System
- **CRS**: EPSG:5070 (Albers Equal Area Conic)
- **Datum**: NAD83
- **Units**: Meters
- **Standard for**: CONUS-wide analyses and national datasets

### Temporal Coverage
- **Primary**: July 1-31, 2023
- **Fallback**: June 1 - August 31, 2023 (if primary has insufficient data)
- **AlphaEarth**: 2023 (16 bands for visualization efficiency)

### Processing Details
- **Clipping**: All rasters clipped to HUC bounding box (rectangular extent)
- **Boundary**: HUC vector boundary exported separately for masking/analysis
- **Projection**: All data reprojected to EPSG:5070 (Albers Equal Area Conic)
- **Cloud masking**: Applied to Landsat using QA_PIXEL band
- **SAR preprocessing**: Speckle filtering with focal median
- **Water mask**: 50% occurrence threshold from JRC Global Surface Water
- **Flow direction**: HydroSHEDS 15-arcsecond data

## Validation

After downloading, run the visualization script to verify:
```bash
conda run -n pytorch_gpu_cu118 python create_publication_visualization.py
```

This should generate the same publication figure with regenerated data.

## Support

If you encounter issues:
1. Check the Google Earth Engine status page
2. Verify sufficient Google Drive storage space
3. Try running individual export tasks manually
4. Contact Google Earth Engine support for quota issues

---

**Created**: October 13, 2025  
**Purpose**: Regenerate corrupted HUC 10020007 data using Google Earth Engine  
**Target**: Madison River Basin, Montana (HUC 10020007)