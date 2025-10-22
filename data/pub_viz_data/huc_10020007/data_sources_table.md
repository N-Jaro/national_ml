# HUC 10020007 (Madison River Basin) - Data Sources Table

## Study Area Information
- **HUC ID**: 10020007
- **Basin Name**: Madison River Basin
- **States**: Idaho, Montana, Wyoming
- **Area**: 1,635,681 acres (6,619 km²)
- **Geographic Extent**: 
  - Longitude: -112.04° to -110.46° W
  - Latitude: 44.29° to 45.97° N

## Input Data Sources

| Data Type | Source | Platform/Sensor | Temporal Coverage | Spatial Resolution | Bands | Processing Level | File Size | Data Origin |
|-----------|--------|-----------------|-------------------|-------------------|-------|------------------|-----------|-------------|
| **Digital Elevation Model (DEM)** | USGS 3DEP | LiDAR/Photogrammetry | - | 10m | 1 | Processed | 1.2 GB | Google Earth Engine |
| **Optical Imagery** | Landsat 9 | OLI-2 + TIRS-2 | 2023-07-01 to 2023-07-31 | 30m | 7* | Surface Reflectance (L2) | 1.3 GB | Google Earth Engine |
| **Synthetic Aperture Radar** | Sentinel-1 | C-SAR | 2023-07-01 to 2023-07-31 | 10m | 6† | Ground Range Detected (GRD) | 7.6 GB | Google Earth Engine |
| **Foundation Model Embeddings** | AlphaEarth | Google Foundation Model | 2024 | 10m | 16‡ | Processed Embeddings | 30.0 GB | Google Earth Engine |

### Band Details:
- *Landsat 9 Bands: B2 (Blue), B3 (Green), B4 (Red), B5 (NIR), B6 (SWIR1), B7 (SWIR2), B10 (Thermal)
- †Sentinel-1 Bands: VV/VH polarizations with multiple processing variations
- ‡AlphaEarth: 16-dimensional semantic feature embeddings

## Reference Data Sources

| Data Type | Source | Origin | Temporal Coverage | Spatial Resolution | Purpose | File Size | Processing Method |
|-----------|--------|--------|-------------------|-------------------|---------|-----------|-------------------|
| **Hydrography Water Mask** | USGS NHD+ | National Hydrography Dataset Plus | Current | 10m | Water Body Segmentation Target | 8.3 MB | Rasterized from vector |
| **Flow Direction** | USGS NHD+ | National Hydrography Dataset Plus | Current | 10m | Hydrologic Flow Modeling Target | 2.4 GB | D8 Flow Direction Algorithm |

## Spatial Reference System
- **Coordinate Reference System**: NAD83 / Conus Albers (EPSG:5070)
- **Projection**: Albers Equal Area Conic
- **Datum**: North American Datum 1983
- **Unit**: Meters

## Boundary and Sampling Data

| Data Type | Source | Format | Count/Size | Purpose |
|-----------|--------|--------|------------|---------|
| **HUC Boundary** | USGS WBD | GeoJSON | 1 polygon | Study area delineation |
| **Bounding Box** | USGS WBD | GeoJSON | 1 rectangle | Simple extent |
| **Buffered Boundary** | USGS WBD | GeoJSON | 1 polygon | Extended study area |
| **Patch Center Points** | Generated | CSV/GeoJSON | 1,320 points | ML patch sampling locations |

## Data Processing Timeline
- **HUC Processing**: Generated from USGS Watershed Boundary Dataset
- **Satellite Data Collection**: Automated via Google Earth Engine API
- **AlphaEarth Processing**: Foundation model inference on satellite imagery
- **Reference Data**: Derived from authoritative USGS hydrographic datasets
- **Patch Sampling**: Systematic grid generation for machine learning workflows

## Data Quality Metrics
- **DEM Coverage**: 100.0% valid pixels, range: 0.000 to 3,443.172 m
- **Landsat Coverage**: 99.9% valid pixels, range: -0.191 to 1.077 (reflectance)
- **SAR Coverage**: 100.0% valid pixels, range: -30.000 to 0.000 dB
- **AlphaEarth Coverage**: 100.0% valid pixels, range: -2.000 to 2.000 (normalized)
- **Hydro Mask Coverage**: 100.0% valid pixels, binary: 0 (land) to 1 (water)
- **Flow Direction Coverage**: 100.0% valid pixels, range: -2.000 to 128.000 (D8 codes)

## Total Dataset Summary
- **Total File Count**: 11 files (6 raster + 5 vector)
- **Total Data Volume**: 42.6 GB
- **Total Spectral Bands**: 32 bands
- **Modalities**: 4 (Optical, Radar, Elevation, Foundation Features)
- **Reference Channels**: 2 (Water Mask, Flow Direction)

## Data Access and Processing
- **Primary Data Source**: Google Earth Engine (earthengine.google.com)
- **Processing Environment**: Python 3.x with rasterio, geopandas
- **Local Storage**: `/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/`
- **Verification Status**: ✅ All files verified and validated

---
*Generated: October 10, 2025*  
*Project: National ML - Multimodal Multitask Deep Learning for Hydrographic Feature Delineation*