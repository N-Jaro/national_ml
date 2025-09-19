# Module Specifications for Local Processing Pipeline

## Module 1: `local_config.py`

### Purpose
Extended configuration class that inherits from the existing Settings class and adds local processing specific parameters with special attention to multi-band data handling.

### Key Features
```python
from config import Settings

class LocalProcessingSettings(Settings):
    def __init__(self):
        super().__init__()
        
        # === LOCAL STORAGE CONFIGURATION ===
        self.LOCAL_DATA_ROOT = '/u/nathanj/national_ml/data/local_rasters'
        self.LOCAL_CACHE_ROOT = '/u/nathanj/national_ml/data/local_cache'
        
        # === MEMORY MANAGEMENT ===
        # Critical for handling AlphaEarth (64 bands) and full pipeline (73+ bands)
        self.MAX_MEMORY_GB = 16                    # Total memory limit
        self.CHUNK_SIZE_MB = 512                   # Processing chunk size
        self.MAX_BANDS_IN_MEMORY = 32              # Load bands in batches
        self.ALPHAEARTH_CHUNK_BANDS = 16           # Process AlphaEarth in 16-band chunks
        
        # === PROCESSING CONFIGURATION ===
        self.PARALLEL_WORKERS = 8                 # CPU cores for parallel processing
        self.IO_WORKERS = 4                       # Separate I/O thread pool
        self.TILE_SIZE = 4096                     # Efficient raster I/O tile size
        self.COMPRESSION = 'LZW'                  # GeoTIFF compression
        
        # === MULTI-BAND SPECIFICATIONS ===
        self.BAND_CONFIGS = {
            'dem': {
                'bands': ['elevation'],
                'dtype': 'float32',
                'resolution': 10,
                'crs': 'EPSG:5070'
            },
            'optical': {
                'bands': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
                'dtype': 'float32', 
                'resolution': 30,
                'scaling_factor': 0.0000275,
                'scaling_offset': -0.2,
                'cloud_masking': True
            },
            'thermal': {
                'bands': ['ST_B10'],
                'dtype': 'float32',
                'resolution': 30,
                'scaling_factor': 0.00341802,
                'scaling_offset': 149.0
            },
            'sar': {
                'bands': ['VV'],
                'dtype': 'float32',
                'resolution': 10,
                'polarization': 'VV',
                'instrument_mode': 'IW'
            },
            'alphaearth': {
                'bands': [f'embedding_{i:02d}' for i in range(64)],  # 64 embedding bands
                'dtype': 'float32',
                'resolution': 10,
                'annual_year': 2024,
                'high_memory_usage': True  # Flag for special handling
            }
        }
        
        # === STORAGE OPTIMIZATION ===
        self.USE_COG = True                       # Cloud Optimized GeoTIFF
        self.OVERVIEW_LEVELS = [2, 4, 8, 16]     # Pyramid levels
        self.NODATA_VALUE = -9999
        
        # === FALLBACK CONFIGURATION ===
        self.ENABLE_GEE_FALLBACK = True          # Fallback to GEE if local data missing
        self.PARTIAL_PROCESSING = True           # Allow processing with missing sources
        self.QUALITY_THRESHOLD = 0.8             # Minimum data completeness ratio
```

---

## Module 2: `bulk_downloader.py`

### Purpose
Downloads and manages large-scale raster datasets with special handling for multi-band sources.

### Class Structure
```python
class BulkDataDownloader:
    def __init__(self, settings: LocalProcessingSettings):
        """Initialize with memory and storage management for multi-band data"""
        
    def download_landsat_composite(self, huc_list: list, date_range: tuple):
        """
        Download Landsat optical (6 bands) + thermal (1 band) composites
        
        Memory Strategy:
        - Process optical bands together (6 × 30m = manageable)
        - Process thermal separately to optimize memory
        - Use chunked downloads for large areas
        - Implement cloud masking during download
        """
        
    def download_alphaearth_mosaic(self, huc_list: list, year: int):
        """
        Download AlphaEarth embeddings (64 bands)
        
        Critical Memory Management:
        - 64 bands × 10m resolution = very large files
        - Download in geographic tiles, not all at once
        - Use band subsetting if full 64 bands not needed
        - Implement aggressive compression (DEFLATE + predictor)
        - Stream directly to disk, minimal RAM usage
        """
        
    def download_dem_mosaic(self, huc_list: list):
        """Download USGS 3DEP DEM (1 band) - simplest case"""
        
    def download_sar_composite(self, huc_list: list, date_range: tuple):
        """Download Sentinel-1 SAR composite (1 band)"""
        
    def create_spatial_index(self):
        """Create spatial index for efficient patch lookup"""
```

### Multi-band Download Strategy
1. **Landsat (7 bands total)**:
   - Download scenes covering all HUCs
   - Apply cloud masking during download
   - Create temporal composites (median)
   - Store optical (6 bands) and thermal (1 band) separately

2. **AlphaEarth (64 bands)**:
   - Most memory-intensive component
   - Download annual mosaics in geographic tiles
   - Use streaming I/O to avoid loading all bands simultaneously
   - Consider band selection if not all 64 bands needed

---

## Module 3: `raster_processor.py`

### Purpose
Core raster operations with efficient multi-band processing.

### Class Structure
```python
class MultiBandRasterProcessor:
    def __init__(self, settings: LocalProcessingSettings):
        """Initialize with memory monitoring for multi-band operations"""
        
    def process_landsat_multiband(self, scene_paths: list, output_path: str):
        """
        Process Landsat with 6 optical + 1 thermal bands
        
        Processing Strategy:
        - Load scenes in temporal chunks
        - Apply cloud masking per scene
        - Calculate temporal statistics (median, percentiles)
        - Process optical and thermal bands separately for memory efficiency
        - Use Dask arrays for out-of-core processing if needed
        """
        
    def process_alphaearth_64band(self, mosaic_path: str, output_path: str):
        """
        Process AlphaEarth 64-band embeddings
        
        Memory-Critical Processing:
        - Never load all 64 bands simultaneously
        - Process in band chunks (e.g., 16 bands at a time)
        - Use memory mapping for large arrays
        - Implement band-wise statistics calculation
        - Use HDF5 or Zarr for intermediate storage
        """
        
    def align_multiband_sources(self, source_paths: dict, target_crs: str):
        """
        Align all data sources to common grid
        
        Handles:
        - Different resolutions (10m, 30m)
        - Different band counts (1 to 64 bands)
        - CRS reprojection
        - Pixel alignment
        """
        
    def create_temporal_composite(self, image_list: list, method: str = 'median'):
        """Create temporal composites with cloud masking"""

class CloudMaskProcessor:
    """Specialized class for Landsat cloud masking"""
    
    def apply_qa_mask(self, image_array: np.ndarray, qa_array: np.ndarray):
        """Apply QA_PIXEL based cloud masking"""
        
    def scale_surface_reflectance(self, sr_array: np.ndarray):
        """Apply surface reflectance scaling"""
```

---

## Module 4: `local_patch_extractor.py`

### Purpose
Extract patches from local rasters with efficient multi-band handling.

### Class Structure
```python
class LocalPatchExtractor:
    def __init__(self, settings: LocalProcessingSettings):
        """Initialize with spatial indexing and memory management"""
        
    def extract_patch_multiband(self, center_point: tuple, patch_size: int, 
                               source_rasters: dict) -> dict:
        """
        Extract aligned patches from multiple sources
        
        Input: center_point (lon, lat), patch_size (224), source_rasters
        Output: {
            'dem': array(224, 224),           # 1 band
            'optical': array(224, 224, 6),   # 6 bands  
            'thermal': array(224, 224),      # 1 band
            'sar': array(224, 224),          # 1 band
            'alphaearth': array(224, 224, 64) # 64 bands - MEMORY CRITICAL
        }
        
        Memory Strategy for AlphaEarth:
        - Use windowed reading with rasterio
        - Read bands in chunks (e.g., 16 at a time)
        - Assemble final array incrementally
        - Use memory mapping for large patches
        """
        
    def extract_batch_patches(self, center_points: list, patch_size: int,
                             batch_size: int = 32) -> list:
        """
        Extract multiple patches efficiently
        
        Batch Processing:
        - Process patches in batches to optimize I/O
        - For AlphaEarth: limit batch size based on memory
        - Use parallel workers for I/O bound operations
        - Implement progress monitoring
        """
        
    def validate_patch_completeness(self, patch_data: dict) -> float:
        """Validate patch has complete data across all bands"""

class WindowedReader:
    """Efficient windowed reading for large rasters"""
    
    def read_window_multiband(self, raster_path: str, window: tuple, 
                             band_indices: list = None):
        """
        Read specific window and bands from raster
        
        For AlphaEarth (64 bands):
        - Use band_indices to read subsets
        - Implement chunked reading for memory efficiency
        - Use appropriate data types (float32 vs float64)
        """
```

### Patch Extraction Strategy for Multi-band Data

1. **Memory-Aware Processing**:
   ```python
   # For AlphaEarth (64 bands)
   patch_size = 224
   total_pixels = patch_size * patch_size * 64  # ~3.2M values
   memory_mb = total_pixels * 4 / 1024 / 1024   # ~12.8 MB per patch
   
   # Batch size calculation
   available_memory_mb = settings.MAX_MEMORY_GB * 1024 * 0.8  # 80% of max
   max_batch_size = available_memory_mb // memory_mb
   ```

2. **Efficient I/O Pattern**:
   ```python
   # Read all sources for one patch location at once
   # Minimize file open/close operations
   # Use spatial indexing to identify relevant tiles
   ```

---

## Module 5: `data_manager.py`

### Purpose
Manage local storage and provide efficient access to multi-band rasters.

### Class Structure
```python
class LocalDataManager:
    def __init__(self, settings: LocalProcessingSettings):
        """Initialize storage management with multi-band optimization"""
        
    def organize_storage_structure(self):
        """
        Create optimized storage structure:
        
        local_data/
        ├── dem/
        │   └── merged_10m_1band.tif
        ├── landsat/
        │   ├── optical_6band_composite.tif     # 6 bands
        │   └── thermal_1band_composite.tif     # 1 band  
        ├── sentinel1/
        │   └── sar_1band_composite.tif         # 1 band
        ├── alphaearth/
        │   ├── embeddings_64band_tiles/        # Tiled for memory efficiency
        │   │   ├── tile_0_0.tif               # Subset of 64 bands
        │   │   ├── tile_0_1.tif
        │   │   └── ...
        │   └── spatial_index.gpkg
        └── spatial_indices/
            ├── landsat_grid.gpkg
            ├── alphaearth_grid.gpkg
            └── unified_grid.gpkg
        """
        
    def create_spatial_index(self, raster_path: str, tile_size: int = 4096):
        """Create spatial index for efficient patch lookup"""
        
    def get_overlapping_tiles(self, bbox: tuple, source: str) -> list:
        """Find tiles that overlap with given bounding box"""
        
    def optimize_alphaearth_storage(self, source_path: str, output_dir: str):
        """
        Optimize AlphaEarth storage for efficient access:
        
        1. Create tiled structure (avoid single massive file)
        2. Add overviews for multi-scale access
        3. Use appropriate compression
        4. Create band statistics
        """

class BandManager:
    """Specialized manager for multi-band operations"""
    
    def get_band_statistics(self, raster_path: str, band_indices: list = None):
        """Calculate statistics for specific bands"""
        
    def create_band_subset(self, source_path: str, output_path: str, 
                          band_indices: list):
        """Create subset with selected bands (useful for AlphaEarth)"""
        
    def validate_band_count(self, raster_path: str, expected_count: int):
        """Validate raster has expected number of bands"""
```

---

## Module 6: `local_stats_calculator.py`

### Purpose
Calculate normalization statistics with special handling for high-dimensional data.

### Class Structure
```python
class MultiBandStatsCalculator:
    def __init__(self, settings: LocalProcessingSettings):
        """Initialize with memory management for 64+ band processing"""
        
    def calculate_comprehensive_stats(self, raster_paths: dict) -> dict:
        """
        Calculate statistics for all data sources:
        
        Returns:
        {
            'dem': {'mean': X, 'std': Y, ...},           # 1 band
            'optical': {'band_0': {...}, 'band_1': {...}, ...},  # 6 bands
            'thermal': {'mean': X, 'std': Y, ...},       # 1 band
            'sar': {'mean': X, 'std': Y, ...},          # 1 band
            'alphaearth': {'band_0': {...}, ..., 'band_63': {...}}  # 64 bands
        }
        """
        
    def calculate_alphaearth_stats_chunked(self, raster_path: str, 
                                          chunk_size: int = 16) -> dict:
        """
        Memory-efficient statistics for AlphaEarth (64 bands)
        
        Strategy:
        - Process bands in chunks (e.g., 16 bands at a time)
        - Use Dask for out-of-core computation
        - Calculate online statistics to avoid loading full dataset
        - Use welford's algorithm for numerical stability
        """
        
    def calculate_online_statistics(self, data_generator):
        """Calculate statistics using online algorithms (streaming)"""
        
    def merge_statistics(self, stats_list: list) -> dict:
        """Merge statistics from multiple chunks or tiles"""

class HighDimensionalStatsProcessor:
    """Specialized processor for high-dimensional data (AlphaEarth)"""
    
    def calculate_streaming_stats(self, raster_path: str, 
                                 sample_ratio: float = 0.1):
        """
        Calculate statistics using streaming approach:
        
        For 64-band AlphaEarth:
        - Sample random pixels instead of loading full raster
        - Use stratified sampling across space
        - Ensure statistical representativeness
        """
        
    def calculate_pca_stats(self, raster_path: str, n_components: int = 16):
        """
        Calculate PCA statistics for dimensionality reduction:
        
        Useful for AlphaEarth embeddings:
        - Identify most important embedding dimensions
        - Reduce from 64 to smaller number of bands
        - Maintain most of the information content
        """
```

---

## Module 7: `local_reference_processor.py`

### Purpose
Process local NHD (National Hydrography Dataset) data to create reference layers (flow direction and hydrography masks) that align perfectly with the satellite data patches.

### Class Structure
```python
class LocalReferenceProcessor:
    def __init__(self, settings: LocalProcessingSettings, nhd_gdb_path: str):
        """Initialize with NHD GeoDatabase path and processing settings"""
        
    def calculate_d8_flow_direction(self, dem_path: str, output_path: str):
        """
        Calculate D8 flow direction from DEM using PySheds
        
        Processing Steps:
        1. Load and preprocess DEM (fill pits, depressions)
        2. Resolve flats for proper flow routing
        3. Calculate D8 flow direction (8-directional)
        4. Save as aligned raster with same CRS/resolution as patches
        """
        
    def create_hydrography_mask(self, dem_template_path: str, huc_boundary_path: str, 
                               output_path: str):
        """
        Create hydrography mask from NHD vectors
        
        Processing Steps:
        1. Load NHD layers: NetworkNHDFlowline, NonNetworkNHDFlowline, NHDWaterbody
        2. Clip to HUC boundary with geographic precision
        3. Buffer flowlines by 15m to account for width
        4. Rasterize all features to match DEM template grid
        5. Save as binary mask (1=water, 0=land)
        """
        
    def extract_reference_patches(self, center_points: list, flow_dir_path: str,
                                 hydro_mask_path: str) -> dict:
        """
        Extract reference data patches aligned with satellite patches
        
        Returns patches containing:
        - flow_dir: (224, 224) - D8 flow direction values
        - hydro_mask: (224, 224) - binary hydrography mask
        """
        
    def update_patch_files(self, patch_dir: str, reference_layers: dict):
        """
        Update existing NPZ patch files with reference layers
        
        Process:
        1. Load existing patch NPZ files
        2. Add flow_dir and hydro_mask arrays
        3. Save updated NPZ files with all bands
        """

class NHDDataProcessor:
    """Specialized processor for NHD vector data"""
    
    def load_nhd_layers(self, gdb_path: str, bbox: tuple, layers: list):
        """Load and clip NHD layers from GeoDatabase"""
        
    def buffer_flowlines(self, flowlines_gdf: gpd.GeoDataFrame, buffer_distance: float):
        """Buffer flowlines to represent stream width"""
        
    def rasterize_hydrography(self, vectors_gdf: gpd.GeoDataFrame, 
                             template_raster: str) -> np.ndarray:
        """Rasterize vector hydrography to match template grid"""

class FlowDirectionProcessor:
    """Specialized processor for flow direction calculation"""
    
    def preprocess_dem(self, dem_array: np.ndarray) -> np.ndarray:
        """Preprocess DEM for flow direction calculation"""
        
    def calculate_d8_direction(self, dem_array: np.ndarray) -> np.ndarray:
        """Calculate D8 flow direction using PySheds"""
        
    def validate_flow_network(self, flow_dir_array: np.ndarray) -> bool:
        """Validate flow direction network connectivity"""
```

### Reference Data Integration Strategy

1. **Alignment with Satellite Data**:
   ```python
   # Ensure reference layers match satellite patch grid exactly
   def align_reference_with_patches(dem_template_path, reference_data_path):
       """
       Use DEM template from satellite processing as spatial reference
       Ensures perfect alignment between all data layers
       """
   ```

2. **Memory Efficiency for Reference Processing**:
   ```python
   # Reference layers add 2 more bands per patch
   patch_memory_with_reference = {
       'satellite_bands': 73,  # DEM + optical + thermal + SAR + AlphaEarth
       'reference_bands': 2,   # flow_dir + hydro_mask
       'total_bands': 75,
       'memory_per_patch': '~15.2 MB'  # 224×224×75×4 bytes
   }
   ```

3. **Processing Dependencies**:
   ```python
   # Reference processing depends on satellite processing outputs
   processing_order = [
       "1. Satellite data processing (DEM, optical, etc.)",
       "2. Reference data processing (using DEM as template)",
       "3. Patch extraction and alignment",
       "4. Final NPZ file assembly with all 75 bands"
   ]
   ```

## Updated Memory Management Strategy with Reference Layers

### Revised Memory Considerations

1. **AlphaEarth Processing (64 bands)**:
   - Single patch: 224×224×64×4 bytes = ~12.8 MB
   - 100 patches: ~1.28 GB
   - Never load all patches × all bands simultaneously

2. **Complete Pipeline Memory Footprint (75 bands total)**:
   - Satellite bands: 1 + 6 + 1 + 1 + 64 = 73 bands
   - Reference bands: 1 + 1 = 2 bands (flow_dir + hydro_mask)
   - **Total: 75 bands per patch**
   - Single patch: 224×224×75×4 bytes = **~15.2 MB**
   - Memory budget: Plan for 32-64 patches in memory max

3. **Reference Layer Processing Impact**:
   - Flow direction: 224×224×4 bytes = ~200 KB per patch
   - Hydrography mask: 224×224×1 byte = ~50 KB per patch (binary mask)
   - **Total reference overhead: ~250 KB per patch (minimal impact)**

4. **Processing Order for Memory Efficiency**:
   ```python
   # Optimal processing sequence to minimize memory usage
   processing_sequence = [
       "1. Process satellite data (73 bands) → save to disk",
       "2. Process reference layers for full HUC extent",
       "3. Extract reference patches using windowed reading",
       "4. Combine satellite + reference → final NPZ files",
       "5. Clean up intermediate files"
   ]
   ```

5. **Updated Processing Strategies**:
   - **Chunked Processing**: Process data in spatial or band chunks
   - **Streaming I/O**: Use windowed reading, avoid full file loads
   - **Lazy Loading**: Load bands only when needed
   - **Memory Mapping**: Use mmap for large arrays
   - **Progressive Assembly**: Combine satellite + reference data incrementally
   - **Disk-based Staging**: Use temporary files for intermediate results

### Storage Optimization with Reference Layers

1. **Data Types Optimization**:
   - **Flow direction**: int8 (8 directions + nodata = 9 values)
   - **Hydrography mask**: uint8 (binary: 0 or 1)
   - **Total reference storage**: ~275 KB per patch (compressed)

2. **Compression Strategy**:
   - **Reference layers**: Use LZW compression (high compression ratio for categorical data)
   - **Combined NPZ files**: Use numpy's compressed format
   - **Expected compression**: 60-80% reduction for reference layers

This updated strategy ensures efficient handling of the complete 75-band dataset while maintaining optimal memory usage and processing performance.

---

## Integration with Main Pipeline

```python
class IntegratedLocalProcessor:
    def __init__(self, settings: LocalProcessingSettings):
        self.satellite_processor = LocalPatchExtractor(settings)
        self.reference_processor = LocalReferenceProcessor(settings, nhd_gdb_path)
        
    def process_complete_patches(self, huc_id: str, center_points: list):
        """
        Process complete patches with all 75 bands:
        
        1. Extract satellite data (73 bands)
        2. Calculate reference layers for HUC extent
        3. Extract reference patches aligned with satellite patches
        4. Combine into final NPZ files
        """
        # Process satellite bands (1-73)
        satellite_patches = self.satellite_processor.extract_batch_patches(center_points)
        
        # Process reference layers for full HUC
        reference_layers = self.reference_processor.process_huc_reference(huc_id)
        
        # Extract aligned reference patches (bands 74-75)
        reference_patches = self.reference_processor.extract_reference_patches(
            center_points, reference_layers
        )
        
        # Combine and save complete patches
        for i, (sat_patch, ref_patch) in enumerate(zip(satellite_patches, reference_patches)):
            complete_patch = {**sat_patch, **ref_patch}
            save_path = f"patch_{i}.npz"
            np.savez_compressed(save_path, **complete_patch)
```

This module ensures that the local processing pipeline can create the complete 75-band patches that match the existing workflow, including the critical reference layers needed for hydrological analysis and machine learning applications.
