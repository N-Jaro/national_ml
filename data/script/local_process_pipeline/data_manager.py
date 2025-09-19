"""
Local Data Manager Module

Manages local raster storage, spatial indexing, and provides efficient access 
to multi-band rasters for the local processing pipeline.
"""

import os
import sqlite3
import json
from typing import Dict, List, Tuple, Optional
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.crs import CRS
from rasterio.windows import Window
from shapely.geometry import box, Point
from local_config import LocalProcessingSettings

class LocalDataManager:
    """
    Manages local storage and provides efficient access to multi-band rasters
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
    
    def register_raster(self, source: str, raster_path: str) -> bool:
        """
        Register a raster file in the spatial index
        
        Args:
            source: Data source name (e.g., 'dem', 'optical', 'alphaearth')
            raster_path: Path to the raster file
            
        Returns:
            bool: Success status
        """
        try:
            with rasterio.open(raster_path) as src:
                bounds = src.bounds
                crs = str(src.crs)
                band_count = src.count
                resolution = src.res[0]  # Assuming square pixels
            
            conn = sqlite3.connect(self.spatial_index_db)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO raster_tiles 
                (source, tile_path, min_x, min_y, max_x, max_y, crs, band_count, resolution)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (source, raster_path, bounds.left, bounds.bottom, 
                  bounds.right, bounds.top, crs, band_count, resolution))
            
            conn.commit()
            conn.close()
            
            print(f"Registered {source} raster: {os.path.basename(raster_path)}")
            return True
            
        except Exception as e:
            print(f"Error registering raster {raster_path}: {e}")
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
    
    def get_raster_info(self, source: str) -> Optional[Dict]:
        """Get information about registered rasters for a data source"""
        try:
            conn = sqlite3.connect(self.spatial_index_db)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT tile_path, band_count, resolution, crs
                FROM raster_tiles 
                WHERE source = ?
                LIMIT 1
            ''', (source,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'path': result[0],
                    'band_count': result[1],
                    'resolution': result[2],
                    'crs': result[3]
                }
            return None
            
        except Exception as e:
            print(f"Error getting raster info for {source}: {e}")
            return None
    
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
                'spatial_index': 'alphaearth_spatial_index.gpkg'
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
                    if subdir.endswith('.tif') or subdir.endswith('.gpkg'):
                        continue  # These are files, not directories
                    os.makedirs(os.path.join(source_dir, subdir), exist_ok=True)
        
        return storage_structure
    
    def validate_data_completeness(self, huc_bounds: Tuple[float, float, float, float]) -> Dict[str, bool]:
        """
        Validate that all required data sources are available for a HUC
        
        Args:
            huc_bounds: (min_x, min_y, max_x, max_y) of HUC in target CRS
            
        Returns:
            Dictionary with availability status for each data source
        """
        availability = {}
        
        for source in self.settings.BAND_CONFIGS.keys():
            if source in ['flow_direction', 'hydrography']:
                # These are calculated, not downloaded
                availability[source] = True
                continue
                
            overlapping = self.find_overlapping_rasters(huc_bounds, source)
            availability[source] = len(overlapping) > 0
        
        return availability
    
    def get_huc_geometry(self, huc_id: str):
        """
        Get the geometry for a HUC region using Google Earth Engine.
        This method fetches HUC boundaries from the USGS WBD dataset.
        """
        import ee
        
        try:
            # Get HUC8 collection from GEE
            huc_collection = ee.FeatureCollection('USGS/WBD/2017/HUC08')
            
            # Filter by HUC ID
            huc_feature = huc_collection.filter(ee.Filter.eq('huc8', huc_id)).first()
            
            # Get the geometry and convert to client-side for local processing
            huc_geom_ee = huc_feature.geometry()
            huc_geom_coords = huc_geom_ee.getInfo()
            
            # Convert to shapely geometry
            from shapely.geometry import shape
            huc_geom = shape(huc_geom_coords)
            
            return huc_geom
            
        except Exception as e:
            print(f"Error getting HUC geometry for {huc_id}: {e}")
            return None
    
    def create_tiles(self, geometry, tile_size_m: float):
        """
        Create tiles for a given geometry based on tile size in meters.
        Returns a list of tile geometries.
        """
        from shapely.geometry import box
        import pyproj
        
        # Get bounds
        bounds = geometry.bounds
        
        # Convert geographic bounds to projected meters for tiling
        transformer = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
        
        min_x_m, min_y_m = transformer.transform(bounds[0], bounds[1])
        max_x_m, max_y_m = transformer.transform(bounds[2], bounds[3])
        
        # Create tile grid
        tiles = []
        
        x = min_x_m
        while x < max_x_m:
            y = min_y_m
            while y < max_y_m:
                # Define tile bounds in projected coordinates
                tile_min_x = x
                tile_max_x = min(x + tile_size_m, max_x_m)
                tile_min_y = y
                tile_max_y = min(y + tile_size_m, max_y_m)
                
                # Convert back to geographic coordinates for GEE compatibility
                transformer_back = pyproj.Transformer.from_crs("EPSG:5070", "EPSG:4326", always_xy=True)
                
                tile_min_lon, tile_min_lat = transformer_back.transform(tile_min_x, tile_min_y)
                tile_max_lon, tile_max_lat = transformer_back.transform(tile_max_x, tile_max_y)
                
                # Create tile geometry
                tile_geom = box(tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat)
                tiles.append(tile_geom)
                
                y += tile_size_m
            x += tile_size_m
        
        return tiles

class BandManager:
    """Specialized manager for multi-band operations"""
    
    def __init__(self, settings: LocalProcessingSettings):
        self.settings = settings
    
    def get_band_statistics(self, raster_path: str, band_indices: List[int] = None) -> Dict:
        """
        Calculate statistics for specific bands
        
        Args:
            raster_path: Path to raster file
            band_indices: List of band indices (1-based), None for all bands
            
        Returns:
            Dictionary with statistics per band
        """
        try:
            with rasterio.open(raster_path) as src:
                if band_indices is None:
                    band_indices = list(range(1, src.count + 1))
                
                stats = {}
                for band_idx in band_indices:
                    band_data = src.read(band_idx, masked=True)
                    
                    # Calculate statistics, handling masked arrays
                    if np.ma.is_masked(band_data):
                        valid_data = band_data.compressed()
                    else:
                        valid_data = band_data[band_data != src.nodata]
                    
                    if len(valid_data) > 0:
                        stats[f'band_{band_idx}'] = {
                            'mean': float(np.mean(valid_data)),
                            'std': float(np.std(valid_data)),
                            'min': float(np.min(valid_data)),
                            'max': float(np.max(valid_data)),
                            'count': int(len(valid_data))
                        }
                    else:
                        stats[f'band_{band_idx}'] = {
                            'mean': None, 'std': None, 'min': None, 
                            'max': None, 'count': 0
                        }
                
                return stats
                
        except Exception as e:
            print(f"Error calculating band statistics: {e}")
            return {}
    
    def create_band_subset(self, source_path: str, output_path: str, 
                          band_indices: List[int]) -> bool:
        """
        Create subset with selected bands (useful for AlphaEarth)
        
        Args:
            source_path: Input raster path
            output_path: Output raster path
            band_indices: List of band indices to extract (1-based)
            
        Returns:
            Success status
        """
        try:
            with rasterio.open(source_path) as src:
                profile = src.profile.copy()
                profile.update(count=len(band_indices))
                
                with rasterio.open(output_path, 'w', **profile) as dst:
                    for i, band_idx in enumerate(band_indices, 1):
                        data = src.read(band_idx)
                        dst.write(data, i)
            
            print(f"Created band subset: {os.path.basename(output_path)}")
            return True
            
        except Exception as e:
            print(f"Error creating band subset: {e}")
            return False
    
    def validate_band_count(self, raster_path: str, expected_count: int) -> bool:
        """Validate raster has expected number of bands"""
        try:
            with rasterio.open(raster_path) as src:
                actual_count = src.count
                
            if actual_count != expected_count:
                print(f"Band count mismatch in {raster_path}: "
                      f"expected {expected_count}, got {actual_count}")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error validating band count: {e}")
            return False

if __name__ == "__main__":
    # Test the data manager
    settings = LocalProcessingSettings()
    data_manager = LocalDataManager(settings)
    band_manager = BandManager(settings)
    
    print("=== Data Manager Test ===")
    
    # Test storage structure creation
    print("Creating storage structure...")
    structure = data_manager.organize_storage_structure()
    print("Storage structure created successfully")
    
    # Test spatial index (if we had actual raster files)
    print("\n=== Spatial Index Test ===")
    print(f"Spatial index database: {data_manager.spatial_index_db}")
    
    # Test band configuration
    print("\n=== Band Manager Test ===")
    for source, config in settings.BAND_CONFIGS.items():
        print(f"{source}: {len(config['bands'])} bands, "
              f"dtype: {config['dtype']}, resolution: {config['resolution']}m")
    
    print("\nData manager initialization complete!")