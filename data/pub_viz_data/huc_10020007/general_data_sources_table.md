# Multi-Modal Satellite Dataset - Data Sources and Specifications

## Table 1: Input Data Sources and Specifications

| **Data Type** | **Source** | **Sensor/Platform** | **Temporal Coverage** | **Spatial Resolution** | **Spectral Bands** | **Processing Level** | **Data Origin** | **Purpose** |
|---------------|------------|-------------------|---------------------|---------------------|-------------------|------------------|---------------|-------------|
| **Digital Elevation Model** | USGS 3DEP | LiDAR/Photogrammetry | Multi-temporal compilation (2010-present) | 10m | 1 | Processed DEM | Google Earth Engine | Topographic context and terrain analysis |
| **Optical Imagery** | Landsat 9 | OLI-2 + TIRS-2 | Summer months (monthly composites) | 30m optical, 100m thermal | 7* | Surface Reflectance (L2A) | Google Earth Engine | Surface reflectance and thermal properties |
| **Synthetic Aperture Radar** | Sentinel-1 | C-SAR VV polarization | Summer months (monthly aggregations) | 10m | 6† | Ground Range Detected (GRD) | Google Earth Engine | All-weather water detection and surface characterization |
| **Foundation Model Embeddings** | AlphaEarth | Google Foundation Model | Annual model (recent year) | 10m | 16‡ | Semantic embeddings | Google Earth Engine | AI-derived surface features |

### Spectral Band Details:
- *Landsat 9 Bands: B2 (Blue, 450-510nm), B3 (Green, 530-590nm), B4 (Red, 640-670nm), B5 (NIR, 850-880nm), B6 (SWIR1, 1570-1650nm), B7 (SWIR2, 2110-2290nm), B10 (Thermal, 10.6-11.19μm)
- †Sentinel-1 Bands: VV polarization only with multiple statistical aggregations (mean, median, standard deviation, minimum, maximum, percentiles)
- ‡AlphaEarth: 16-dimensional learned feature representations from foundation model trained on global satellite imagery

## Table 2: Reference Data Sources and Ground Truth Labels

| **Target Variable** | **Source** | **Dataset** | **Temporal Coverage** | **Spatial Resolution** | **Data Format** | **Value Range** | **Purpose** |
|-------------------|------------|-------------|---------------------|---------------------|-----------------|-----------------|-------------|
| **Water Body Mask** | USGS | NHDPlusHR (High Resolution) | Climate baseline: 1990-2019 | 10m | Binary raster | 0 (land), 1 (water) | Water body segmentation ground truth |
| **Flow Direction** | USGS | NHDPlusHR (High Resolution) | Climate baseline: 1990-2019 | 10m | Categorical raster | D8 codes (1,2,4,8,16,32,64,128) + NoData | Hydrologic flow modeling ground truth |

## Data Processing and Quality Standards

### Spatial Reference System
- **Coordinate Reference System**: NAD83 / Conus Albers (EPSG:5070)
- **Projection**: Albers Equal Area Conic
- **Datum**: North American Datum 1983
- **Unit**: Meters
- **Coverage**: Continental United States

### Temporal Data Collection Strategy

#### Multi-Temporal Baseline Data:
- **USGS 3DEP DEM**: Compilation from 2010-present, representing best-available elevation data per area
- **Data Sources**: Primarily LiDAR (higher accuracy) supplemented by photogrammetry where needed
- **Update Strategy**: Continuous updates as new high-quality elevation data becomes available
- **Temporal Consistency**: Each pixel represents the most recent, highest-quality elevation measurement

#### Dynamic Satellite Data (Time-Varying):
- **Landsat 9**: Summer months (June-August) monthly composites from closest available year to NHD+ data
- **Sentinel-1**: 3-month summer aggregations (median/mean) from closest available year to NHD+ data
- **AlphaEarth**: Annual foundation model embeddings from most recent available year
- **Rationale**: Summer timing captures optimal hydrologic conditions with maximum water body visibility

#### Static Reference Data (Temporally Stable):
- **USGS NHDPlusHR**: Represents long-term, persistent hydrographic features based on multi-decade climate analysis
- **Temporal Baseline**: Flow estimates and hydrographic features derived from 1990-2019 climate data (datasets published ≥2022)
- **Climate Data Source**: Daymet version 3 monthly climate summaries (precipitation and temperature: 1990-2019)
- **Update Cycle**: Periodic updates every 2-5 years with revised climate periods and enhanced datasets
- **Temporal Stability**: Designed to capture permanent water features based on 30-year climate normals

#### Key Temporal Design Considerations:
1. **Climate Period Alignment**: NHDPlusHR reference data based on 1990-2019 climate normals (30-year average conditions)
2. **Satellite-Reference Matching**: Current satellite observations (2020s) compared against established 30-year hydrologic baseline
3. **Seasonal Consistency**: Summer satellite observations match typical low-flow conditions used in NHDPlusHR flow estimation
4. **Climate Representativeness**: 30-year NHDPlusHR baseline provides robust statistical foundation for hydrographic patterns
5. **Change Detection Capability**: Current satellite data vs. climate-normal baseline enables identification of hydrologic changes
6. **Training Robustness**: Climate-averaged reference data provides stable ground truth less sensitive to inter-annual variability

### SAR Polarization Selection: VV-Only Justification

**Scientific Rationale for VV Polarization:**
1. **Water Detection Sensitivity**: VV polarization provides superior water body discrimination due to strong specular reflection from smooth water surfaces
2. **Reduced Backscatter from Water**: Water surfaces exhibit very low VV backscatter (-25 to -35 dB), creating high contrast with terrestrial features
3. **Consistency Across Conditions**: VV shows more stable backscatter characteristics across different water body types (rivers, lakes, wetlands)
4. **Computational Efficiency**: Single polarization reduces data volume and processing complexity while maintaining hydrographic detection capability
5. **Literature Support**: VV polarization is established as optimal for water mapping in numerous peer-reviewed studies
6. **Operational Reliability**: VV-only approach reduces potential errors from polarimetric ratio calculations and cross-polarization noise

**VH Polarization Limitations for Water Detection:**
- Higher sensitivity to surface roughness and vegetation, potentially masking water signals
- Less consistent water/land contrast, especially in vegetated wetland areas
- Increased computational overhead without proportional improvement in water detection accuracy

### Data Quality Criteria
- **Cloud Coverage**: <20% for optical imagery
- **SAR Backscatter Quality**: VV backscatter values validated against known water/land boundaries
- **Data Completeness**: >95% valid pixels required per study area
- **Geometric Accuracy**: Sub-pixel registration across all modalities
- **Temporal Consistency**: All input data collected within same seasonal window

## Study Area Sampling Framework

### Hydrologic Unit Code (HUC) Selection
- **Scale**: HUC-8 watersheds (8-digit codes)
- **Geographic Distribution**: Continental United States
- **Size Range**: 700-40,000 km² per watershed
- **Sampling Strategy**: Systematic geographic and climatic representation

### Patch Extraction Protocol
- **Patch Size**: 224×224 pixels
- **Stride**: 224 pixels (no overlap)
- **Sampling**: Grid-based center points within HUC boundaries
- **Quality Control**: Patches with >80% valid data retained

## Data Volume and Specifications

### Typical Dataset per HUC Study Area:
- **Input Rasters**: 30 spectral bands (DEM: 1, Optical: 7, SAR: 6, AlphaEarth: 16)
- **Reference Rasters**: 2 target bands (Water mask: 1, Flow direction: 1)
- **Vector Data**: HUC boundaries, sampling points, metadata
- **Storage**: 10-50 GB per study area (varies by HUC size)

### Processing Infrastructure
- **Data Source**: Google Earth Engine (earthengine.google.com)
- **API Access**: Authenticated Earth Engine Python API
- **Processing**: Cloud-based computation with local storage
- **Format**: GeoTIFF rasters, GeoJSON vectors, CSV metadata

## Data Availability and Access
- **Landsat 9**: Public domain, NASA/USGS
- **Sentinel-1**: Open access, European Space Agency (ESA)
- **USGS 3DEP**: Public domain, U.S. Geological Survey
- **NHD+**: Public domain, U.S. Geological Survey
- **AlphaEarth**: Available through Google Earth Engine
- **Processing Scripts**: Open source, project repository

---
*Table Caption: Standardized multi-modal satellite dataset specifications for hydrographic feature delineation across continental United States watersheds. All study areas utilize identical data sources, processing protocols, and quality standards to ensure consistency for machine learning model development and validation.*

---
*Document Version: 1.0*  
*Generated: October 10, 2025*  
*Project: National ML - Multimodal Multitask Deep Learning for Hydrographic Feature Delineation*