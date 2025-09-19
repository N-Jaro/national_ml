# Current Data Processing Pipeline Documentation

## Overview
The existing pipeline is a Google Earth Engine (GEE) based system that processes HUC8 watersheds to generate multi-source satellite data patches for machine learning applications. The pipeline operates in four main stages with complex data handling for multiple satellite sources.

## Pipeline Architecture

### Stage 1: HUC Processing (`huc_process.py`)
**Purpose**: Generate patch center points and export foundational data

**Process Flow**:
1. **HUC Selection**: Process specified HUC8 watershed IDs from configuration
2. **Geometry Fetching**: Retrieve HUC8 boundaries from USGS/WBD/2017/HUC08
3. **Grid Generation**: Create systematic patch center points using DEM as reference
4. **Exports**: Export HUC boundaries and full-extent DEMs to Google Drive

**Key Features**:
- Uses USGS 3DEP 10m DEM collection as spatial reference
- Generates patch centers on 224×224 pixel grid with 224-pixel stride
- Buffers HUC boundaries by 5km for complete coverage
- Exports data to Google Drive for later download

**Output**:
- HUC boundary GeoJSON files
- Full-extent DEM GeoTIFF files  
- Patch center point coordinates saved locally

### Stage 2: Patch Processing (`patch_process.py`)
**Purpose**: Download multi-source satellite data patches from GEE

**Data Sources & Specifications**:

1. **Digital Elevation Model (DEM)**
   - Source: USGS/3DEP/10m_collection
   - Resolution: 10m
   - Bands: 1 (elevation)
   - Processing: Mosaic collection, reproject to EPSG:5070

2. **Optical Data (Landsat 9)**
   - Source: LANDSAT/LC09/C02/T1_L2
   - Resolution: 30m  
   - Bands: 6 (SR_B2, SR_B3, SR_B4, SR_B5, SR_B6, SR_B7)
   - Processing: Cloud masking, surface reflectance scaling (×0.0000275 - 0.2)
   - Temporal: Median composite over date range (2023-2024)

3. **Thermal Data (Landsat 9)**
   - Source: LANDSAT/LC09/C02/T1_L2  
   - Resolution: 30m (resampled from 100m)
   - Bands: 1 (ST_B10)
   - Processing: Temperature scaling (×0.00341802 + 149.0)
   - Temporal: Median composite over date range

4. **SAR Data (Sentinel-1)**
   - Source: COPERNICUS/S1_GRD
   - Resolution: 10m
   - Bands: 1 (VV polarization)
   - Processing: IW mode only, median composite
   - Temporal: Median composite over date range

5. **AlphaEarth Embeddings**
   - Source: GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL
   - Resolution: 10m
   - Bands: 64 (embedding dimensions)
   - Processing: Mosaic annual collection for 2024
   - Note: High-dimensional feature embeddings from Google's foundation model

**Processing Workflow**:
1. **Image Preparation**: 
   - Fetch and filter image collections for Area of Interest (AOI)
   - Apply cloud masking to Landsat data using QA_PIXEL band
   - Create temporal composites (median) for each data source
   - Reproject all sources to target CRS (EPSG:5070)

2. **Parallel Patch Extraction**:
   - Use ThreadPoolExecutor with 16 workers for concurrent downloads
   - For each patch center point:
     - Calculate patch bounding box (224×224 pixels at target resolution)
     - Download GeoTIFF for each data source via GEE getDownloadURL
     - Load raster data using tifffile
     - Handle band transposition for multi-band data

3. **Data Assembly**:
   - Stack all bands into single NPZ file per patch
   - Save georeference template (DEM GeoTIFF) for spatial alignment
   - Handle missing data gracefully

**Output Format**:
```
patch_X.npz containing:
├── dem: (224, 224) - elevation values
├── optical: (224, 224, 6) - Landsat optical bands  
├── thermal: (224, 224) - Landsat thermal band
├── sar: (224, 224) - Sentinel-1 VV
└── alphaearth: (224, 224, 64) - Google embeddings

patch_X_georef_template.tif - spatial reference
```

### Stage 3: Statistics Processing (`stats_process.py`)
**Purpose**: Calculate normalization statistics for machine learning preprocessing

**Statistical Calculations**:
1. **Per-band Statistics**: Mean, standard deviation, min, max, percentiles
2. **Data Sources Handled**:
   - DEM: 1 band statistics
   - Optical: 6 band statistics (Landsat SR_B2-B7)
   - Thermal: 1 band statistics (Landsat ST_B10)
   - SAR: 1 band statistics (Sentinel-1 VV)
   - AlphaEarth: 64 band statistics (embedding dimensions)

**Processing Approaches**:
1. **Combined Stats**: Attempt to process all data together (memory intensive)
2. **Sampling Stats**: Fallback to statistical sampling for large datasets
3. **Minimal Stats**: Emergency fallback with basic statistics

**Memory Management**:
- Conservative memory usage for AlphaEarth (64 bands)
- Chunked processing for large HUCs
- Graceful degradation when memory limits exceeded

**Output**: 
- `normalization_stats.json` with per-band statistics for all data sources

### Stage 4: Local Reference Processing (`local_reference_process.py`)
**Purpose**: Create hydrography masks and flow direction from NHD data

**Process Flow**:
1. **NHD Data Processing**:
   - Load flowlines (network and non-network) from local shapefiles
   - Load waterbodies from local shapefiles
   - Clip to HUC boundary with buffer

2. **Raster Generation**:
   - Create hydrography mask raster aligned with DEM
   - Generate flow direction raster
   - Use same projection and resolution as patch data

**Output**:
- Hydrography mask GeoTIFF
- Flow direction GeoTIFF  
- Processed vector files (GeoPackage format)

## Configuration Management (`config.py`)

**Key Parameters**:
```python
# Patch specifications
PATCH_SIZE = 224          # Patch dimensions in pixels
PATCH_STRIDE = 224        # Spacing between patch centers
TARGET_DEM_CRS = 'EPSG:5070'  # Albers Equal Area (meters)

# Data source resolutions
SOURCE_RESOLUTIONS = {
    'dem': 10,            # USGS 3DEP
    'optical': 30,        # Landsat optical
    'thermal': 30,        # Landsat thermal  
    'sar': 10,           # Sentinel-1
    'alphaearth': 10     # Google embeddings
}

# Processing parameters
MAX_CONCURRENT_DOWNLOADS = 5
ALPHAEARTH_BANDS_LIMIT = 64
ENABLE_ALPHAEARTH = True
```

## Data Flow Summary

```
1. HUC Selection → 2. Patch Centers → 3. GEE Downloads → 4. Local Assembly
        ↓                 ↓                 ↓                  ↓
   - Select HUCs     - Grid generation  - Multi-source    - NPZ patches
   - Export DEMs     - Spatial sampling - Parallel fetch  - Statistics
   - Drive storage   - Point coordinates- Band stacking   - NHD processing
```

## Current Limitations

### Performance Bottlenecks
1. **GEE API Limits**: Rate limiting affects large-scale processing
2. **Network Dependency**: Requires stable internet for all patch downloads
3. **Sequential Processing**: HUCs processed one at a time
4. **Memory Usage**: AlphaEarth (64 bands) causes memory pressure

### Reliability Issues
1. **GEE Service Dependency**: Pipeline fails if GEE is unavailable
2. **Download Failures**: Individual patch failures require manual retry
3. **Inconsistent Results**: GEE processing may vary between runs
4. **Timeout Issues**: Large patches may exceed GEE timeout limits

### Scalability Constraints
1. **Cost**: GEE compute costs scale with patch count
2. **Bandwidth**: Download speeds limit throughput
3. **Storage**: No local caching of intermediate results
4. **Parallelization**: Limited by GEE concurrent request limits

## Integration with Machine Learning Pipeline

**Data Usage**:
- Patches used for training geospatial machine learning models
- Multi-modal data (optical, SAR, DEM, embeddings) for comprehensive analysis
- Standardized format enables consistent preprocessing
- Statistics enable proper normalization for neural networks

**Quality Assurance**:
- Georeference templates ensure spatial accuracy
- Cloud masking removes problematic optical data
- Temporal compositing reduces noise and gaps
- Validation against known hydrography features

This documentation provides the foundation for understanding the current system's complexity and data requirements, informing the design of the new local processing pipeline.
