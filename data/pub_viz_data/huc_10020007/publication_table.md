# Publication Table: HUC 10020007 Dataset Summary

## Table 1: Multi-Modal Satellite Data Sources for Madison River Basin (HUC 10020007)

| **Data Type** | **Source** | **Sensor/Platform** | **Temporal Coverage** | **Spatial Res.** | **Bands** | **Size (GB)** | **Purpose** |
|---------------|------------|-------------------|---------------------|------------------|-----------|---------------|-------------|
| Elevation | USGS 3DEP | LiDAR/Photogrammetry | Static (multi-year compilation) | 10m | 1 | 1.2 | Topographic context |
| Optical | Landsat 9 | OLI-2/TIRS-2 | Jul 2023 (monthly composite) | 30m | 7 | 1.3 | Surface reflectance + thermal |
| Radar | Sentinel-1 | C-SAR | Jul 2023 (monthly median) | 10m | 6 | 7.6 | All-weather surface properties |
| Foundation | AlphaEarth | Google AI Model | 2024 (annual model) | 10m | 16 | 30.0 | Semantic feature embeddings |
| **Subtotal** | | | | | **30** | **40.1** | **Input features** |

## Table 2: Reference Data for Model Training and Validation

| **Target Variable** | **Source** | **Origin** | **Temporal Coverage** | **Spatial Res.** | **Encoding** | **Size (GB)** | **Description** |
|-------------------|------------|------------|---------------------|------------------|--------------|---------------|----------------|
| Water Bodies | USGS NHD+ | National Hydrography Dataset | Static (periodically updated) | 10m | Binary (0/1) | 0.008 | Hydrographic water body mask |
| Flow Direction | USGS NHD+ | National Hydrography Dataset | Static (periodically updated) | 10m | D8 (1-128) | 2.4 | Hydrologic flow direction |
| **Subtotal** | | | | | **2.4** | **Ground truth labels** |

## Study Area Characteristics
- **Basin**: Madison River Basin, ID/MT/WY
- **Area**: 6,619 km² (1.64M acres)  
- **Coordinates**: 44.29°-45.97°N, 110.46°-112.04°W
- **CRS**: NAD83 Albers (EPSG:5070)
- **ML Patches**: 1,320 potential sampling locations (224×224 pixels)

## Temporal Data Characteristics

### Dynamic vs. Static Data Comparison:

**📅 Time-Series Satellite Data (Dynamic):**
- **Landsat 9 & Sentinel-1**: Captured during summer 2023 (July) to represent peak vegetation and minimal snow/ice conditions
- **AlphaEarth**: Generated from 2024 satellite imagery compilation, providing most recent AI-derived surface features
- **Rationale**: Summer timing captures optimal hydrologic conditions with maximum water body visibility and consistent surface properties

**🗺️ Reference Hydrographic Data (Static):**
- **USGS NHD+**: Represents "permanent" water features and flow patterns based on long-term hydrologic analysis
- **Update Frequency**: NHD+ undergoes periodic updates (~every 2-5 years) but maintains consistent baseline hydrology
- **Temporal Stability**: Designed to capture persistent hydrographic features rather than seasonal variations

### Key Temporal Considerations:
1. **Seasonal Alignment**: Satellite data timing chosen to match typical NHD+ survey conditions (low-flow summer periods)
2. **Multi-Year vs. Single-Year**: NHD+ represents multi-decade averaged conditions, while satellite data captures specific 2023-2024 timeframe
3. **Change Detection**: This temporal offset allows assessment of how current conditions compare to established hydrographic baselines
4. **Model Training**: Static NHD+ labels provide stable ground truth for training on dynamic satellite observations

## Data Quality Summary
- **Coverage**: >99.9% valid pixels across all modalities
- **Total Volume**: 42.6 GB (32 spectral + 2 reference bands)
- **Processing**: Google Earth Engine → Local validation → ML-ready format
- **Verification**: ✅ All data sources validated and co-registered

---
*Table Caption: Multi-modal satellite dataset for hydrographic feature delineation in the Madison River Basin. Input data combines elevation (DEM), optical imagery (Landsat 9), synthetic aperture radar (Sentinel-1), and foundation model embeddings (AlphaEarth) for comprehensive surface characterization. Reference data from USGS National Hydrography Dataset provides ground truth for water body segmentation and flow direction modeling.*