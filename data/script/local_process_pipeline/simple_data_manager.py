"""
Simplified Local Data Manager Module (without heavy geospatial dependencies)

Basic version for testing the core functionality without requiring
geopandas, rasterio, etc. Will be extended once dependencies are available.
"""

import os
import sqlite3
import json
from typing import Dict, List, Tuple, Optional
import numpy as np
from local_config import LocalProcessingSettings

class SimpleLocalDataManager:
    """
    Simplified data manager for testing core functionality
    """
    
    def __init__(self, settings: LocalProcessingSettings):
        self.settings = settings
        self.settings.create_storage_directories()
        self.spatial_index_db = os.path.join(
            self.settings.LOCAL_DATA_ROOT, 'spatial_indices', 'spatial_index.db'
        )
        self._initialize_spatial_index()
    
    def _initialize_spatial_index(self):
        """Initialize SQLite spatial index database"""
        try:
            conn = sqlite3.connect(self.spatial_index_db)
            cursor = conn.cursor()
            
            # Create spatial index table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS raster_tiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    tile_path TEXT NOT NULL,
                    min_x REAL NOT NULL,
                    min_y REAL NOT NULL,
                    max_x REAL NOT NULL,
                    max_y REAL NOT NULL,
                    crs TEXT NOT NULL,
                    band_count INTEGER,
                    resolution REAL,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source, tile_path)
                )
            ''')
            
            # Create spatial index on bounding box
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_spatial 
                ON raster_tiles (min_x, min_y, max_x, max_y)
            ''')
            
            conn.commit()
            conn.close()
            print(f"Spatial index database initialized: {self.spatial_index_db}")
            
        except Exception as e:
            print(f"Error initializing spatial index: {e}")
    
    def register_raster_metadata(self, source: str, raster_path: str, 
                                bounds: Tuple[float, float, float, float],
                                crs: str, band_count: int, resolution: float) -> bool:
        """
        Register raster metadata in the spatial index
        
        Args:
            source: Data source name
            raster_path: Path to the raster file
            bounds: (min_x, min_y, max_x, max_y)
            crs: Coordinate reference system
            band_count: Number of bands
            resolution: Pixel resolution
            
        Returns:
            bool: Success status
        """
        try:
            conn = sqlite3.connect(self.spatial_index_db)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO raster_tiles 
                (source, tile_path, min_x, min_y, max_x, max_y, crs, band_count, resolution)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (source, raster_path, bounds[0], bounds[1], 
                  bounds[2], bounds[3], crs, band_count, resolution))
            
            conn.commit()
            conn.close()
            
            print(f"Registered {source} raster metadata: {os.path.basename(raster_path)}")
            return True
            
        except Exception as e:
            print(f"Error registering raster metadata {raster_path}: {e}")
            return False
    
    def find_overlapping_rasters(self, bbox: Tuple[float, float, float, float], 
                                source: str = None) -> List[Dict]:
        """
        Find rasters that overlap with given bounding box
        
        Args:
            bbox: (min_x, min_y, max_x, max_y) in target CRS
            source: Optional source filter
            
        Returns:
            List of overlapping raster information
        """
        try:
            conn = sqlite3.connect(self.spatial_index_db)
            cursor = conn.cursor()
            
            query = '''
                SELECT source, tile_path, min_x, min_y, max_x, max_y, crs, band_count, resolution
                FROM raster_tiles 
                WHERE max_x >= ? AND min_x <= ? AND max_y >= ? AND min_y <= ?
            '''
            params = [bbox[0], bbox[2], bbox[1], bbox[3]]
            
            if source:
                query += ' AND source = ?'
                params.append(source)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            conn.close()
            
            overlapping_rasters = []
            for row in results:
                overlapping_rasters.append({
                    'source': row[0],
                    'path': row[1],
                    'bounds': (row[2], row[3], row[4], row[5]),
                    'crs': row[6],
                    'band_count': row[7],
                    'resolution': row[8]
                })
            
            return overlapping_rasters
            
        except Exception as e:
            print(f"Error finding overlapping rasters: {e}")
            return []
    
    def organize_storage_structure(self):
        """Create optimized storage structure for different data sources"""
        storage_structure = {
            'dem': 'merged_10m_1band.tif',
            'landsat': {
                'optical': 'optical_6band_composite.tif',
                'thermal': 'thermal_1band_composite.tif'
            },
            'sentinel1': 'sar_1band_composite.tif',
            'alphaearth': {
                'tiles_dir': 'embeddings_64band_tiles',
                'spatial_index': 'alphaearth_spatial_index.json'
            },
            'reference': {
                'flow_direction': 'flow_direction_mosaic.tif',
                'hydrography': 'hydrography_mask_mosaic.tif'
            }
        }
        
        # Create directory structure
        for source, structure in storage_structure.items():
            source_dir = self.settings.get_storage_path(source)
            
            if isinstance(structure, dict):
                for subdir in structure.values():
                    if subdir.endswith(('.tif', '.json')):
                        continue  # These are files, not directories
                    subdir_path = os.path.join(source_dir, subdir)
                    os.makedirs(subdir_path, exist_ok=True)
                    print(f"Created directory: {subdir_path}")
        
        return storage_structure
    
    def get_storage_info(self) -> Dict:
        """Get information about the storage structure"""
        info = {
            'root_path': self.settings.LOCAL_DATA_ROOT,
            'cache_path': self.settings.LOCAL_CACHE_ROOT,
            'temp_path': self.settings.LOCAL_TEMP_DIR,
            'spatial_index_db': self.spatial_index_db,
            'configured_sources': list(self.settings.BAND_CONFIGS.keys()),
            'total_bands': self.settings.get_total_bands_count()
        }
        
        # Check which directories exist
        info['existing_directories'] = []
        for source in self.settings.BAND_CONFIGS.keys():
            source_path = self.settings.get_storage_path(source)
            if os.path.exists(source_path):
                info['existing_directories'].append(source)
        
        return info

class SimpleBandManager:
    """Simplified band manager for testing"""
    
    def __init__(self, settings: LocalProcessingSettings):
        self.settings = settings
    
    def get_band_config(self, source: str) -> Optional[Dict]:
        """Get band configuration for a data source"""
        return self.settings.BAND_CONFIGS.get(source)
    
    def validate_band_config(self) -> bool:
        """Validate all band configurations"""
        total_bands = 0
        
        print("=== Band Configuration Validation ===")
        for source, config in self.settings.BAND_CONFIGS.items():
            band_count = len(config['bands'])
            total_bands += band_count
            
            print(f"{source:15} | {band_count:2d} bands | {config['dtype']:8} | "
                  f"{config['resolution']:2.0f}m | nodata: {config.get('nodata', 'N/A')}")
        
        print(f"\nTotal bands: {total_bands}")
        print(f"Memory per patch: {self.settings.get_memory_per_patch_mb():.1f} MB")
        print(f"Max patches in memory: {self.settings.get_max_patches_in_memory()}")
        
        return True

if __name__ == "__main__":
    # Test the simplified data manager
    settings = LocalProcessingSettings()
    data_manager = SimpleLocalDataManager(settings)
    band_manager = SimpleBandManager(settings)
    
    print("=== Simplified Data Manager Test ===")
    
    # Test storage structure creation
    print("\n1. Creating storage structure...")
    structure = data_manager.organize_storage_structure()
    print("Storage structure created successfully")
    
    # Test storage info
    print("\n2. Storage Information:")
    info = data_manager.get_storage_info()
    for key, value in info.items():
        if isinstance(value, list):
            print(f"{key}: {len(value)} items")
        else:
            print(f"{key}: {value}")
    
    # Test band configuration validation
    print("\n3. Band Configuration:")
    band_manager.validate_band_config()
    
    # Test spatial index with dummy data
    print("\n4. Testing spatial index with dummy data...")
    test_bounds = (-2000000, 1000000, -1000000, 2000000)  # EPSG:5070 bounds
    success = data_manager.register_raster_metadata(
        source='test_dem',
        raster_path='/dummy/path/test_dem.tif',
        bounds=test_bounds,
        crs='EPSG:5070',
        band_count=1,
        resolution=10.0
    )
    
    if success:
        # Test finding overlapping rasters
        query_bounds = (-1500000, 1200000, -1200000, 1800000)
        overlapping = data_manager.find_overlapping_rasters(query_bounds)
        print(f"Found {len(overlapping)} overlapping rasters for query bounds")
    
    print("\n=== Test Complete ===")
    print("Simplified data manager working correctly!")
    print("Ready to proceed with next implementation phase.")
