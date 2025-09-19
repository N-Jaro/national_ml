# Local Data Processing Pipeline Implementation Plan

## Overview

This document outlines the implementation plan for transitioning from a Google Earth Engine (GEE) based patch-on-demand system to a local raster processing pipeline. The new system will download entire datasets locally and extract patches from local rasters, providing better performance, reliability, and cost efficiency.

## Current Pipeline Architecture

### Data Sources & Band Requirements
1. **DEM**: USGS 3DEP 10m (1 band - elevation)
2. **Optical**: Landsat 9 Collection 2 SR (6 bands: SR_B2-B7, 30m resolution)
3. **Thermal**: Landsat 9 Collection 2 SR (1 band: ST_B10, 30m resolution)
4. **SAR**: Sentinel-1 GRD (1 band: VV polarization, 10m resolution)
5. **AlphaEarth**: Google Satellite Embedding (64 bands, 10m resolution)

### Current Workflow
```
HUC Processing → GEE Patch Download → Statistics Calculation → Local Reference Processing
     ↓                    ↓                      ↓                        ↓
- Generate centers    - Download patches    - Calculate stats     - NHD hydro masks
- Export HUC DEMs     - Multi-source data   - From GEE           - Flow direction
- Export boundaries   - Parallel fetching   - Normalization      - Local processing
```

## New Local Processing Pipeline Architecture

### Phase 1: Bulk Data Management
```
Bulk Download → Data Organization → Spatial Indexing → Quality Control
     ↓                ↓                    ↓               ↓
- Large mosaics   - By source/date    - Spatial trees   - Gap detection
- Temporal composites - CRS alignment   - Quick lookup    - Quality flags
- Regional tiles  - Band management   - Efficient I/O   - Validation
```

### Phase 2: Local Patch Extraction
```
Patch Coordinate Input → Raster Windowing → Multi-band Assembly → Reference Processing → Output Generation
         ↓                      ↓                    ↓                    ↓                ↓
- From HUC centers     - Efficient reading   - Band stacking    - Flow direction   - NPZ format
- Spatial queries      - Memory management   - CRS reprojection - Hydro masks      - Same as current
- Parallel processing  - Multi-source sync   - Quality masking  - NHD processing   - Georef templates
```

## Implementation Modules

### Module 1: `local_config.py`
**Purpose**: Extended configuration for local processing
**Key Features**:
- Local storage paths and organization
- Memory management parameters
- Parallel processing configuration
- Data source priorities (local vs GEE fallback)

```python
class LocalProcessingSettings:
    # Storage configuration
    LOCAL_DATA_ROOT = '/path/to/local/raster/storage'
    CHUNK_SIZE_MB = 512  # For processing large rasters
    MAX_MEMORY_GB = 16   # Memory limit for operations
    
    # Multi-band handling
    LANDSAT_BANDS = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
    ALPHAEARTH_BANDS_LIMIT = 64
    
    # Processing parameters
    PARALLEL_WORKERS = 8
    TILE_SIZE = 4096  # For efficient raster I/O
```

### Module 2: `bulk_downloader.py`
**Purpose**: Download and organize large-scale raster datasets
**Key Features**:
- Handles multi-gigabyte downloads
- Creates temporal composites locally
- Manages band-heavy datasets (AlphaEarth 64 bands)
- Implements resumable downloads

**Critical Considerations for Multi-band Data**:
- **Landsat**: Download full scenes, create median composites with cloud masking
- **AlphaEarth**: Handle 64-band embeddings efficiently, use compression
- **Memory Management**: Process in chunks to avoid memory overflow
- **Storage Optimization**: Use efficient formats (COG, HDF5) for multi-band data

### Module 3: `raster_processor.py`
**Purpose**: Core raster operations and transformations
**Key Features**:
- Temporal compositing (median, percentiles)
- Cloud masking for Landsat
- CRS reprojection and alignment
- Band-wise operations

**Multi-band Processing Strategy**:
```python
def process_landsat_composite(scene_collection, date_range, aoi):
    """
    Process Landsat Collection 2 with 6 optical + 1 thermal bands
    Memory-efficient approach for large areas
    """
    # Process optical bands (6 bands)
    optical_bands = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
    
    # Process thermal band separately
    thermal_band = ['ST_B10']
    
    # Apply cloud masking and scaling
    # Create temporal composite (median)
    # Return aligned multi-band raster
```

### Module 4: `local_patch_extractor.py`
**Purpose**: Extract patches from local rasters efficiently
**Key Features**:
- Windowed reading for memory efficiency
- Multi-source data alignment
- Parallel patch extraction
- Handles variable band counts per source

**Multi-band Extraction Strategy**:
```python
def extract_patch_multiband(center_point, patch_size, sources):
    """
    Extract aligned patches from multiple sources with different band counts:
    - DEM: 1 band
    - Optical: 6 bands  
    - Thermal: 1 band
    - SAR: 1 band
    - AlphaEarth: 64 bands
    
    Total: 75 bands per patch (73 remote sensing + 2 reference layers)
    """
    # Use rasterio windowed reading
    # Handle CRS alignment
    # Manage memory for 64-band AlphaEarth
    # Stack into final patch format
```

### Module 5: `data_manager.py`
**Purpose**: Manage local raster storage and indexing
**Key Features**:
- Spatial indexing for fast patch lookup
- Multi-resolution data management
- Band metadata tracking
- Storage optimization

**Storage Structure**:
```
local_data/
├── landsat/
│   ├── 2023/
│   │   ├── optical_composite_6band.tif
│   │   └── thermal_composite_1band.tif
│   └── 2024/
├── sentinel1/
│   ├── 2023/
│   │   └── sar_composite_1band.tif
├── dem/
│   └── usgs_3dep_10m_1band.tif
├── alphaearth/
│   └── 2024/
│       └── embeddings_64band.tif  # Large file!
└── spatial_index/
    ├── landsat_tiles.gpkg
    ├── dem_tiles.gpkg
    └── alphaearth_tiles.gpkg
```

### Module 6: `local_stats_calculator.py`
**Purpose**: Calculate normalization statistics from local data
**Key Features**:
- Out-of-core statistics using Dask
- Handles high-dimensional data (64 AlphaEarth bands)
- Incremental statistics updates
- Memory-efficient percentile calculations

**Multi-band Statistics Strategy**:
```python
def calculate_multiband_stats(raster_path, band_count):
    """
    Calculate statistics for high-dimensional rasters
    Special handling for AlphaEarth (64 bands)
    """
    if band_count > 32:  # AlphaEarth case
        # Use chunked processing
        # Calculate statistics per band
        # Use Dask for out-of-core computation
    else:
        # Standard in-memory processing
```

### Module 7: `local_reference_processor.py`
**Purpose**: Create hydrography and flow direction reference layers from local NHD data
**Key Features**:
- Process NHD flowlines and waterbodies from local GeoDatabase
- Generate D8 flow direction from local DEMs using PySheds
- Create hydrography masks aligned with patch grid
- Update patch NPZ files with reference layers

**Reference Data Processing**:
```python
def process_reference_data(huc_dem_path, nhd_gdb_path, output_dir):
    """
    Process local reference data:
    1. Calculate D8 flow direction from HUC DEM
    2. Create hydrography mask from NHD vectors
    3. Extract patches aligned with existing grid
    4. Update NPZ files with reference bands
    """
    # Flow direction: uses PySheds for D8 calculation
    # Hydrography mask: rasterizes NHD flowlines + waterbodies
    # Additional bands per patch: flow_dir + hydro_mask = 2 bands
```

### Module 8: `quality_controller.py`
**Purpose**: Ensure data quality and completeness
**Key Features**:
- Validates patch completeness across all bands (including reference layers)
- Detects missing data or artifacts
- Compares results with GEE pipeline for validation
- Handles edge cases in multi-band data

## Implementation Phases

### Phase 1: Foundation (Weeks 1-2)
1. **Setup local storage structure**
2. **Implement `local_config.py`**
3. **Create `data_manager.py` with spatial indexing**
4. **Basic DEM processing (simplest case)**

### Phase 2: Single-band Sources (Weeks 3-4)
1. **Implement SAR processing (Sentinel-1)**
2. **Add thermal band processing (Landsat)**
3. **Test with existing HUC data**

### Phase 3: Multi-band Optical (Weeks 5-6)
1. **Implement Landsat optical processing (6 bands)**
2. **Add cloud masking and temporal compositing**
3. **Memory optimization for multi-band operations**
4. **Validation against GEE results**

### Phase 4: High-dimensional Data (Weeks 7-8)
1. **Implement AlphaEarth processing (64 bands)**
2. **Advanced memory management**
3. **Storage optimization for large files**
4. **Performance benchmarking**

### Phase 5: Reference Data Processing (Weeks 9-10)
1. **Implement local reference processing**
2. **NHD data integration from local GeoDatabase**
3. **D8 flow direction calculation using PySheds**
4. **Hydrography mask generation and rasterization**
5. **Update patch NPZ files with reference layers**

### Phase 6: Integration and Optimization (Weeks 11-12)
1. **Integrate all data sources including reference layers**
2. **Complete `local_patch_extractor.py` with all 75+ bands**
3. **Add `local_stats_calculator.py` for all data types**
4. **Performance optimization and testing**

### Phase 7: Production and Validation (Weeks 13-14)
1. **Complete pipeline testing**
2. **Validation against existing GEE pipeline**
3. **Documentation and user guides**
4. **Migration tools and hybrid processing**

## Memory and Performance Considerations

### Multi-band Data Challenges
1. **AlphaEarth (64 bands)**:
   - Single patch: 224×224×64 = ~3.2M values per patch
   - Float32: ~12.8 MB per patch
   - 1000 patches: ~12.8 GB memory requirement

2. **Full Pipeline (73 bands total)**:
   - Single patch: 224×224×73 = ~3.6M values
   - Float32: ~14.6 MB per patch
   - Batch processing needs careful memory management

### Optimization Strategies
1. **Chunked Processing**: Process patches in smaller batches
2. **Lazy Loading**: Load bands only when needed
3. **Compression**: Use efficient storage formats (LZW, DEFLATE)
4. **Caching**: Cache frequently accessed tiles
5. **Parallel I/O**: Overlap computation and I/O operations

## Risk Mitigation

### Data Availability Risks
- **Fallback to GEE**: Maintain GEE pipeline as backup
- **Partial Processing**: Allow processing with missing sources
- **Data Validation**: Verify downloaded data completeness

### Performance Risks
- **Memory Overflow**: Implement robust memory monitoring
- **Storage Capacity**: Plan for multi-TB storage requirements
- **Processing Time**: Benchmark and optimize critical paths

### Technical Risks
- **CRS Alignment**: Careful handling of different projections
- **Band Registration**: Ensure spatial alignment across sources
- **Temporal Matching**: Handle temporal misalignment between sources

## Success Metrics

1. **Performance**: 2-5x faster patch generation vs GEE
2. **Reliability**: >99% uptime vs GEE dependencies
3. **Quality**: <1% difference in patch statistics vs GEE
4. **Efficiency**: 50-80% reduction in GEE API costs
5. **Scalability**: Handle 10x more patches without performance degradation

## Next Steps

1. **Review and approve this implementation plan**
2. **Set up development environment with required libraries**
3. **Begin Phase 1 implementation with foundation modules**
4. **Establish testing framework with sample HUC data**
5. **Create monitoring and logging infrastructure**

This plan provides a roadmap for building a robust, efficient local processing pipeline that can handle the complex multi-band requirements of your machine learning dataset while maintaining compatibility with your existing workflow.
