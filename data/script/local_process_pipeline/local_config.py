"""
Local Processing Configuration Module

Extends the existing Settings class with local processing specific parameters,
with special attention to multi-band data handling and memory management.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Settings

class LocalProcessingSettings(Settings):
    """
    Extended configuration class for local processing pipeline
    Inherits from the existing Settings class and adds local-specific parameters
    """
    
    def __init__(self):
        super().__init__()
        
        # === LOCAL STORAGE CONFIGURATION ===
        self.LOCAL_DATA_ROOT = '/u/nathanj/national_ml/data/local_rasters'
        self.local_raster_base = Path(self.LOCAL_DATA_ROOT)  # Path object for compatibility
        self.LOCAL_CACHE_ROOT = '/u/nathanj/national_ml/data/local_cache'
        self.LOCAL_TEMP_DIR = '/u/nathanj/national_ml/data/local_temp'
        
        # === MEMORY MANAGEMENT ===
        # Critical for handling AlphaEarth (64 bands) and full pipeline (75+ bands)
        self.MAX_MEMORY_GB = 16                    # Total memory limit
        self.CHUNK_SIZE_MB = 512                   # Processing chunk size
        self.MAX_BANDS_IN_MEMORY = 32              # Load bands in batches
        self.ALPHAEARTH_CHUNK_BANDS = 16           # Process AlphaEarth in 16-band chunks
        
        # === PROCESSING CONFIGURATION ===
        self.PARALLEL_WORKERS = 8                 # CPU cores for parallel processing
        self.IO_WORKERS = 4                       # Separate I/O thread pool
        self.TILE_SIZE = 4096                     # Efficient raster I/O tile size
        self.COMPRESSION = 'LZW'                  # GeoTIFF compression
        self.OVERVIEW_LEVELS = [2, 4, 8, 16]     # Pyramid levels
        
        # === MULTI-BAND SPECIFICATIONS ===
        self.BAND_CONFIGS = {
            'dem': {
                'bands': ['elevation'],
                'dtype': 'float32',
                'resolution': 10,
                'crs': 'EPSG:5070',
                'source': 'USGS/3DEP/10m_collection',
                'nodata': -9999
            },
            'optical': {
                'bands': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
                'dtype': 'float32', 
                'resolution': 30,
                'scaling_factor': 0.0000275,
                'scaling_offset': -0.2,
                'cloud_masking': True,
                'source': 'LANDSAT/LC09/C02/T1_L2',
                'qa_band': 'QA_PIXEL',
                'nodata': -9999
            },
            'thermal': {
                'bands': ['ST_B10'],
                'dtype': 'float32',
                'resolution': 30,
                'scaling_factor': 0.00341802,
                'scaling_offset': 149.0,
                'source': 'LANDSAT/LC09/C02/T1_L2',
                'nodata': -9999
            },
            'sar': {
                'bands': ['VV'],
                'dtype': 'float32',
                'resolution': 10,
                'polarization': 'VV',
                'instrument_mode': 'IW',
                'source': 'COPERNICUS/S1_GRD',
                'nodata': -9999
            },
            'alphaearth': {
                'bands': [f'embedding_{i:02d}' for i in range(64)],  # 64 embedding bands
                'dtype': 'float32',
                'resolution': 10,
                'annual_year': 2024,
                'high_memory_usage': True,  # Flag for special handling
                'source': 'GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL',
                'nodata': -9999
            },
            'flow_direction': {
                'bands': ['flow_dir'],
                'dtype': 'int8',  # 8 directions + nodata
                'resolution': 10,
                'source': 'calculated_from_dem',
                'nodata': -1
            },
            'hydrography': {
                'bands': ['hydro_mask'],
                'dtype': 'uint8',  # Binary mask
                'resolution': 10,
                'source': 'nhd_vectors',
                'nodata': 255
            }
        }
        
        # === STORAGE OPTIMIZATION ===
        self.USE_COG = True                       # Cloud Optimized GeoTIFF
        self.NODATA_VALUE = -9999
        self.TILED_STORAGE = True                 # Use tiled storage for large rasters
        self.BLOCK_SIZE = 512                     # Tile block size
        
        # === FALLBACK CONFIGURATION ===
        self.ENABLE_GEE_FALLBACK = True          # Fallback to GEE if local data missing
        self.PARTIAL_PROCESSING = True           # Allow processing with missing sources
        self.QUALITY_THRESHOLD = 0.8             # Minimum data completeness ratio
        
        # === DOWNLOAD CONFIGURATION ===
        self.DOWNLOAD_CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB chunks
        self.MAX_DOWNLOAD_RETRIES = 3
        self.DOWNLOAD_TIMEOUT_SECONDS = 300
        
        # === NHD REFERENCE DATA ===
        self.NHD_GDB_PATH = '/u/nathanj/national_ml/data/raw/nhdplus_gdb/NHDPlus_H_National_Release_2.gdb'
        self.NHD_LAYERS = ['NetworkNHDFlowline', 'NonNetworkNHDFlowline', 'NHDWaterbody']
        self.FLOWLINE_BUFFER_DISTANCE = 15        # Buffer distance in meters
        
        # Aliases for reference processor compatibility
        self.nhd_gdb_path = self.NHD_GDB_PATH
        self.nhd_shapefiles_dir = '/u/nathanj/national_ml/data/raw/nhd_shapefiles'
        
        # === QUALITY CONTROL ===
        self.ENABLE_QUALITY_CHECKS = True
        self.MAX_MISSING_PIXELS_RATIO = 0.1      # Maximum 10% missing pixels per patch
        self.SPATIAL_TOLERANCE_METERS = 5.0      # Spatial alignment tolerance
        
    def get_total_bands_count(self) -> int:
        """Calculate total number of bands across all data sources"""
        total = 0
        for source_config in self.BAND_CONFIGS.values():
            total += len(source_config['bands'])
        return total
    
    def get_memory_per_patch_mb(self) -> float:
        """Calculate memory requirement per patch in MB"""
        total_bands = self.get_total_bands_count()
        pixels_per_patch = self.PATCH_SIZE * self.PATCH_SIZE
        bytes_per_pixel = 4  # float32
        total_bytes = total_bands * pixels_per_patch * bytes_per_pixel
        return total_bytes / (1024 * 1024)  # Convert to MB
    
    def get_max_patches_in_memory(self) -> int:
        """Calculate maximum number of patches that can fit in memory"""
        memory_per_patch_mb = self.get_memory_per_patch_mb()
        available_memory_mb = self.MAX_MEMORY_GB * 1024 * 0.8  # Use 80% of max
        return int(available_memory_mb / memory_per_patch_mb)
    
    def create_storage_directories(self):
        """Create necessary storage directories if they don't exist"""
        directories = [
            self.LOCAL_DATA_ROOT,
            self.LOCAL_CACHE_ROOT,
            self.LOCAL_TEMP_DIR,
            os.path.join(self.LOCAL_DATA_ROOT, 'dem'),
            os.path.join(self.LOCAL_DATA_ROOT, 'landsat'),
            os.path.join(self.LOCAL_DATA_ROOT, 'sentinel1'),
            os.path.join(self.LOCAL_DATA_ROOT, 'alphaearth'),
            os.path.join(self.LOCAL_DATA_ROOT, 'reference'),
            os.path.join(self.LOCAL_DATA_ROOT, 'spatial_indices'),
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            
    def get_storage_path(self, data_source: str, filename: str = None) -> str:
        """Get the storage path for a specific data source"""
        base_path = os.path.join(self.LOCAL_DATA_ROOT, data_source)
        if filename:
            return os.path.join(base_path, filename)
        return base_path
    
    def validate_configuration(self) -> bool:
        """Validate configuration parameters"""
        try:
            # Check memory limits are reasonable
            if self.MAX_MEMORY_GB < 4:
                print("Warning: MAX_MEMORY_GB is very low, may cause performance issues")
            
            # Check band configurations
            total_bands = self.get_total_bands_count()
            print(f"Total bands configured: {total_bands}")
            
            # Check memory requirements
            memory_per_patch = self.get_memory_per_patch_mb()
            max_patches = self.get_max_patches_in_memory()
            print(f"Memory per patch: {memory_per_patch:.1f} MB")
            print(f"Max patches in memory: {max_patches}")
            
            # Check paths exist or can be created
            self.create_storage_directories()
            
            return True
            
        except Exception as e:
            print(f"Configuration validation failed: {e}")
            return False

if __name__ == "__main__":
    # Test the configuration
    settings = LocalProcessingSettings()
    
    print("=== Local Processing Configuration ===")
    print(f"Total bands: {settings.get_total_bands_count()}")
    print(f"Memory per patch: {settings.get_memory_per_patch_mb():.1f} MB") 
    print(f"Max patches in memory: {settings.get_max_patches_in_memory()}")
    
    print("\n=== Band Configuration ===")
    for source, config in settings.BAND_CONFIGS.items():
        print(f"{source}: {len(config['bands'])} bands at {config['resolution']}m")
    
    print(f"\n=== Validating Configuration ===")
    is_valid = settings.validate_configuration()
    print(f"Configuration valid: {is_valid}")
