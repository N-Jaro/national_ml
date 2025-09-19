"""
GEE-Integrated Bulk Data Downloader

Downloads real satellite data from Google Earth Engine for local processing.
Integrates with existing GEE setup and credentials.
"""

import os
import sys
import time
import ee
import requests
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_config import LocalProcessingSettings
from data_manager import LocalDataManager
from config import Settings

class GEEBulkDownloader:
    """
    Downloads real satellite data from Google Earth Engine for local processing
    """
    
    def __init__(self, settings: LocalProcessingSettings, data_manager: LocalDataManager = None):
        self.settings = settings
        self.gee_settings = Settings()  # Original GEE settings
        self.data_manager = data_manager if data_manager else LocalDataManager(settings)
        self.download_log = []
        self._initialize_gee()
        
        # Initialize GEE collections
        self.HUC8_COL = ee.FeatureCollection(self.gee_settings.HUC8_COL_NAME)
        self.DEM_COLLECTION = ee.ImageCollection(self.gee_settings.DEM_SOURCE_IMG_NAME)
    
    def _initialize_gee(self):
        """Initialize Google Earth Engine"""
        try:
            ee.Initialize(project=self.gee_settings.GEE_PROJECT_ID)
            print(f"✓ GEE initialized successfully with project: {self.gee_settings.GEE_PROJECT_ID}")
        except Exception as e:
            print(f"✗ Error initializing GEE: {e}")
            raise
    
    def get_huc_geometry(self, huc_id: str) -> ee.Geometry:
        """Get HUC geometry from GEE"""
        try:
            huc_feature = self.HUC8_COL.filter(ee.Filter.eq('huc8', huc_id)).first()
            
            # Check if HUC exists
            huc_info = huc_feature.getInfo()
            if huc_info is None:
                raise ValueError(f"HUC8 ID '{huc_id}' not found in GEE collection")
            
            huc_name = huc_feature.get('name').getInfo()
            geometry = huc_feature.geometry()
            
            print(f"✓ Found HUC {huc_id}: {huc_name}")
            return geometry
            
        except Exception as e:
            print(f"✗ Error getting HUC geometry for {huc_id}: {e}")
            raise
    
    def download_dem_for_huc(self, huc_id: str, buffer_meters: int = 5000, resolution: float = 10.0) -> str:
        """
        Download DEM data for a specific HUC
        
        Args:
            huc_id: HUC8 identifier
            buffer_meters: Buffer distance around HUC boundary
            resolution: Target resolution in meters (default: 10.0)
            
        Returns:
            Path to downloaded DEM file
        """
        print(f"\n=== Downloading DEM for HUC {huc_id} at {resolution}m resolution ===")
        
        # For 10m resolution, use research-grade method
        if resolution == 10.0:
            return self.download_dem_for_huc_research_grade(huc_id, buffer_meters)
        
        # For other resolutions, use original method
        try:
            # Get HUC geometry
            huc_geometry = self.get_huc_geometry(huc_id)
            huc_geometry_buffered = huc_geometry.buffer(buffer_meters)
            
            # Get DEM data
            print("Preparing DEM mosaic...")
            dem_image = self.DEM_COLLECTION.mosaic().select('elevation')
            
            # Clip to HUC area
            dem_clipped = dem_image.clip(huc_geometry_buffered)
            
            # Calculate appropriate scale to stay within GEE limits
            min_scale = self._calculate_safe_scale(huc_geometry_buffered, self.settings.SOURCE_RESOLUTIONS['dem'])
            
            # Reproject to target CRS with appropriate scale
            dem_reprojected = dem_clipped.reproject(
                crs=self.settings.TARGET_DEM_CRS,
                scale=min_scale
            )
            
            # Prepare output path
            output_path = self.settings.get_storage_path('dem', f'huc_{huc_id}_dem.tif')
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Download via GEE with explicit region and scale
            print("Downloading DEM data...")
            url = dem_reprojected.getDownloadURL({
                'format': 'GEO_TIFF',
                'crs': self.settings.TARGET_DEM_CRS,
                'scale': safe_scale,
                'region': huc_geometry_buffered
            })
            
            response = requests.get(url)
            response.raise_for_status()
            
            # Save to file
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            # Verify the download
            with rasterio.open(output_path) as src:
                print(f"✓ DEM downloaded: {src.count} bands, {src.width}x{src.height}, "
                      f"CRS: {src.crs}")
                bounds = src.bounds
            
            # Register in data manager
            success = self.data_manager.register_raster('dem', output_path)
            
            if success:
                print(f"✓ DEM registered in spatial index")
            
            # Log the download
            self.download_log.append({
                'source': 'dem',
                'huc_id': huc_id,
                'output_path': output_path,
                'bands': 1,
                'timestamp': datetime.now(),
                'status': 'success',
                'size_mb': os.path.getsize(output_path) / (1024 * 1024)
            })
            
            return output_path
            
        except Exception as e:
            print(f"✗ Error downloading DEM for HUC {huc_id}: {e}")
            self.download_log.append({
                'source': 'dem',
                'huc_id': huc_id,
                'output_path': None,
                'timestamp': datetime.now(),
                'status': 'failed',
                'error': str(e)
            })
            raise
    
    def download_dem_for_huc_export(self, huc_id: str, buffer_meters: int = 5000) -> str:
        """
        Download DEM data for a specific HUC using GEE Export (better for large areas)
        
        Args:
            huc_id: HUC8 identifier
            buffer_meters: Buffer distance around HUC boundary
            
        Returns:
            Path to downloaded DEM file
        """
        print(f"\n=== Downloading DEM for HUC {huc_id} (Export Method) ===")
        
        try:
            # Get HUC geometry
            huc_geometry = self.get_huc_geometry(huc_id)
            huc_geometry_buffered = huc_geometry.buffer(buffer_meters)
            
            # Get DEM data
            print("Preparing DEM mosaic...")
            dem_image = self.DEM_COLLECTION.mosaic().select('elevation')
            
            # Clip to HUC area
            dem_clipped = dem_image.clip(huc_geometry_buffered)
            
            # Calculate safe scale
            safe_scale = self._calculate_safe_scale(
                huc_geometry_buffered, 
                self.settings.SOURCE_RESOLUTIONS['dem']
            )
            
            # For very large areas, use a more conservative scale
            if self._should_tile_download(huc_geometry_buffered, safe_scale):
                safe_scale = max(safe_scale, 30.0)  # Use at least 30m resolution
                print(f"  📏 Using conservative scale: {safe_scale}m for large area")
            
            # Reproject to target CRS with safe scale
            dem_reprojected = dem_clipped.reproject(
                crs=self.settings.TARGET_DEM_CRS,
                scale=safe_scale
            )
            
            # Prepare output path
            output_path = self.settings.get_storage_path('dem', f'huc_{huc_id}_dem.tif')
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Use a smaller region for very large HUCs
            bounds_info = huc_geometry_buffered.bounds().getInfo()
            coords = bounds_info['coordinates'][0]
            lons = [point[0] for point in coords]
            lats = [point[1] for point in coords]
            min_x, max_x = min(lons), max(lons)
            min_y, max_y = min(lats), max(lats)
            width_m = max_x - min_x
            height_m = max_y - min_y
            
            if width_m > 100000 or height_m > 100000:  # > 100km in any dimension
                print(f"  ⚠️  Large HUC detected ({width_m/1000:.1f}km x {height_m/1000:.1f}km)")
                print(f"  📉 Using reduced buffer for download")
                reduced_buffer = min(buffer_meters, 2000)  # Max 2km buffer for large HUCs
                huc_geometry_buffered = huc_geometry.buffer(reduced_buffer)
                dem_clipped = dem_image.clip(huc_geometry_buffered)
                dem_reprojected = dem_clipped.reproject(
                    crs=self.settings.TARGET_DEM_CRS,
                    scale=safe_scale
                )
            
            # Download via GEE with explicit region and scale
            print("Downloading DEM data...")
            url = dem_reprojected.getDownloadURL({
                'format': 'GEO_TIFF',
                'crs': self.settings.TARGET_DEM_CRS,
                'scale': safe_scale,
                'region': huc_geometry_buffered
            })
            
            response = requests.get(url, timeout=600)  # 10 minute timeout
            response.raise_for_status()
            
            # Save to file
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            # Verify the download
            with rasterio.open(output_path) as src:
                print(f"✓ DEM downloaded: {src.count} bands, {src.width}x{src.height}, "
                      f"CRS: {src.crs}")
                bounds = src.bounds
            
            # Register in data manager
            success = self.data_manager.register_raster('dem', output_path)
            
            if success:
                print(f"✓ DEM registered in spatial index")
            
            # Log the download
            self.download_log.append({
                'source': 'dem',
                'huc_id': huc_id,
                'file_path': output_path,
                'scale_used': safe_scale,
                'timestamp': datetime.now().isoformat()
            })
            
            return output_path
            
        except Exception as e:
            print(f"✗ Error downloading DEM for HUC {huc_id}: {e}")
            raise

    def download_dem_for_huc_tiled(self, huc_id: str, buffer_meters: int = 5000, tile_size_m: int = 20000) -> str:
        """
        Download DEM data for a large HUC using tiled approach
        
        Args:
            huc_id: HUC8 identifier
            buffer_meters: Buffer distance around HUC boundary
            tile_size_m: Size of each tile in meters (default: 20km)
            
        Returns:
            Path to merged DEM file
        """
        print(f"\n=== Downloading DEM for HUC {huc_id} (Tiled Method) ===")
        
        try:
            # Get HUC geometry
            huc_geometry = self.get_huc_geometry(huc_id)
            huc_geometry_buffered = huc_geometry.buffer(buffer_meters)
            
            # Get bounds for tiling
            bounds_info = huc_geometry_buffered.bounds().getInfo()
            coords = bounds_info['coordinates'][0]
            lons = [point[0] for point in coords]
            lats = [point[1] for point in coords]
            min_x, max_x = min(lons), max(lons)
            min_y, max_y = min(lats), max(lats)
            
            width_m = max_x - min_x
            height_m = max_y - min_y
            
            print(f"  Area: {width_m/1000:.1f}km x {height_m/1000:.1f}km")
            print(f"  Tile size: {tile_size_m/1000:.1f}km")
            
            # Calculate number of tiles needed
            tiles_x = int(np.ceil(width_m / tile_size_m))
            tiles_y = int(np.ceil(height_m / tile_size_m))
            total_tiles = tiles_x * tiles_y
            
            print(f"  Tiles needed: {tiles_x} x {tiles_y} = {total_tiles} tiles")
            
            if total_tiles > 50:  # Reasonable limit
                print(f"  ⚠️  Too many tiles needed ({total_tiles}), increasing tile size")
                tile_size_m = int(max(width_m / 7, height_m / 7, 30000))  # Max 7x7 = 49 tiles
                tiles_x = int(np.ceil(width_m / tile_size_m))
                tiles_y = int(np.ceil(height_m / tile_size_m))
                total_tiles = tiles_x * tiles_y
                print(f"  Adjusted: {tiles_x} x {tiles_y} = {total_tiles} tiles at {tile_size_m/1000:.1f}km")
            
            # Create temp directory for tiles
            temp_dir = os.path.join(self.settings.LOCAL_DATA_ROOT, 'temp_tiles')
            os.makedirs(temp_dir, exist_ok=True)
            
            # Get DEM collection
            dem_image = self.DEM_COLLECTION.mosaic().select('elevation')
            
            # Download each tile
            tile_paths = []
            successful_tiles = 0
            
            for i in range(tiles_x):
                for j in range(tiles_y):
                    tile_name = f"tile_{i}_{j}.tif"
                    tile_path = os.path.join(temp_dir, tile_name)
                    
                    # Calculate tile bounds
                    tile_min_x = min_x + i * tile_size_m
                    tile_max_x = min(tile_min_x + tile_size_m, max_x)
                    tile_min_y = min_y + j * tile_size_m
                    tile_max_y = min(tile_min_y + tile_size_m, max_y)
                    
                    # Create tile geometry
                    tile_coords = [
                        [tile_min_x, tile_min_y],
                        [tile_max_x, tile_min_y], 
                        [tile_max_x, tile_max_y],
                        [tile_min_x, tile_max_y],
                        [tile_min_x, tile_min_y]
                    ]
                    tile_geometry = ee.Geometry.Polygon([tile_coords])
                    
                    try:
                        print(f"  Downloading tile {i+1},{j+1}/{tiles_x},{tiles_y}...")
                        
                        # Clip DEM to tile
                        dem_tile = dem_image.clip(tile_geometry)
                        
                        # Use conservative scale for tiles
                        safe_scale = max(30.0, self.settings.SOURCE_RESOLUTIONS['dem'])
                        
                        dem_reprojected = dem_tile.reproject(
                            crs=self.settings.TARGET_DEM_CRS,
                            scale=safe_scale
                        )
                        
                        # Download tile
                        url = dem_reprojected.getDownloadURL({
                            'format': 'GEO_TIFF',
                            'crs': self.settings.TARGET_DEM_CRS,
                            'scale': safe_scale,
                            'region': tile_geometry
                        })
                        
                        response = requests.get(url, timeout=300)
                        response.raise_for_status()
                        
                        # Save tile
                        with open(tile_path, 'wb') as f:
                            f.write(response.content)
                        
                        tile_paths.append(tile_path)
                        successful_tiles += 1
                        print(f"    ✓ Tile saved: {len(response.content)} bytes")
                        
                    except Exception as e:
                        print(f"    ✗ Tile {i+1},{j+1} failed: {e}")
                        continue
            
            if successful_tiles == 0:
                raise Exception("No tiles downloaded successfully")
            
            print(f"✓ Downloaded {successful_tiles}/{total_tiles} tiles")
            
            # Merge tiles using rasterio
            print("Merging tiles...")
            output_path = self.settings.get_storage_path('dem', f'huc_{huc_id}_dem.tif')
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            if len(tile_paths) == 1:
                # Single tile, just copy
                import shutil
                shutil.copy2(tile_paths[0], output_path)
            else:
                # Multiple tiles, merge them
                from rasterio.merge import merge
                
                src_files = []
                for tile_path in tile_paths:
                    if os.path.exists(tile_path):
                        src_files.append(rasterio.open(tile_path))
                
                if src_files:
                    merged_data, merged_transform = merge(src_files)
                    
                    # Get profile from first file
                    profile = src_files[0].profile
                    profile.update({
                        'height': merged_data.shape[1],
                        'width': merged_data.shape[2], 
                        'transform': merged_transform
                    })
                    
                    # Write merged file
                    with rasterio.open(output_path, 'w', **profile) as dst:
                        dst.write(merged_data)
                    
                    # Close source files
                    for src in src_files:
                        src.close()
                else:
                    raise Exception("No valid tiles to merge")
            
            # Clean up temp tiles
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
            # Verify the merged file
            with rasterio.open(output_path) as src:
                print(f"✓ Merged DEM: {src.count} bands, {src.width}x{src.height}, CRS: {src.crs}")
            
            # Register in data manager
            success = self.data_manager.register_raster('dem', output_path)
            if success:
                print(f"✓ DEM registered in spatial index")
            
            # Log the download
            self.download_log.append({
                'source': 'dem',
                'huc_id': huc_id,
                'file_path': output_path,
                'method': 'tiled',
                'tiles_downloaded': successful_tiles,
                'timestamp': datetime.now().isoformat()
            })
            
            return output_path
            
        except Exception as e:
            print(f"✗ Error downloading tiled DEM for HUC {huc_id}: {e}")
            raise

    def download_dem_for_huc_research_grade(self, huc_id: str, buffer_meters: int = 5000) -> str:
        """
        Download DEM data for a specific HUC using research-grade tiling at 10m resolution
        
        Args:
            huc_id: HUC8 identifier
            buffer_meters: Buffer distance around HUC boundary
            
        Returns:
            Path to downloaded DEM file
        """
        print(f"\n=== Downloading DEM for HUC {huc_id} (Research Grade 10m) ===")
        
        try:
            # Get HUC geometry
            huc_geometry = self.get_huc_geometry(huc_id)
            huc_geometry_buffered = huc_geometry.buffer(buffer_meters)
            
            # Get bounds for area calculation using projected coordinates
            min_x, min_y, max_x, max_y, width_m, height_m = self._get_projected_bounds_meters(huc_geometry_buffered)
            
            print(f"  Area: {width_m/1000:.1f}km x {height_m/1000:.1f}km")
            
            # Research-grade parameters - CORRECTED FOR ACTUAL LIMITS
            resolution = 10.0  # REQUIRED: 10m resolution for research
            max_tile_size_m = 27000  # 27km tiles to stay safely under 50MB at 10m resolution
            
            # Calculate tiling grid
            tiles_x = max(1, int(np.ceil(width_m / max_tile_size_m)))
            tiles_y = max(1, int(np.ceil(height_m / max_tile_size_m)))
            
            actual_tile_width = width_m / tiles_x
            actual_tile_height = height_m / tiles_y
            
            print(f"  Tile size: {actual_tile_width/1000:.1f}km x {actual_tile_height/1000:.1f}km")
            print(f"  Tiles needed: {tiles_x} x {tiles_y} = {tiles_x * tiles_y} tiles")
            
            # Prepare DEM collection
            print("Preparing DEM mosaic...")
            dem_image = self.DEM_COLLECTION.mosaic().select('elevation')
            
            # Create temp directory for tiles
            temp_dir = os.path.join(self.settings.LOCAL_DATA_ROOT, 'temp_tiles')
            os.makedirs(temp_dir, exist_ok=True)
            
            # Download tiles using geographic coordinates for tile boundaries
            tile_paths = []
            successful_tiles = 0
            
            # Get original geographic bounds for proper tile creation
            orig_bounds_info = huc_geometry_buffered.bounds().getInfo()
            orig_coords = orig_bounds_info['coordinates'][0]
            orig_lons = [point[0] for point in orig_coords]
            orig_lats = [point[1] for point in orig_coords]
            orig_min_lon, orig_max_lon = min(orig_lons), max(orig_lons)
            orig_min_lat, orig_max_lat = min(orig_lats), max(orig_lats)
            
            # Calculate tile step in geographic degrees
            lon_step = (orig_max_lon - orig_min_lon) / tiles_x
            lat_step = (orig_max_lat - orig_min_lat) / tiles_y
            
            for j in range(tiles_y):
                for i in range(tiles_x):
                    # Calculate tile bounds in geographic coordinates
                    tile_min_lon = orig_min_lon + i * lon_step
                    tile_max_lon = orig_min_lon + (i + 1) * lon_step
                    tile_min_lat = orig_min_lat + j * lat_step
                    tile_max_lat = orig_min_lat + (j + 1) * lat_step
                    
                    print(f"  Downloading tile {i+1},{j+1}/{tiles_x},{tiles_y}...")
                    
                    try:
                        # Create tile geometry in geographic coordinates
                        tile_geometry = ee.Geometry.Rectangle([tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat])
                        
                        # Clip DEM to tile
                        dem_tile = dem_image.clip(tile_geometry)
                        
                        # Reproject with REQUIRED 10m resolution
                        dem_reprojected = dem_tile.reproject(
                            crs=self.settings.TARGET_DEM_CRS,
                            scale=resolution  # MUST maintain 10m resolution
                        )
                        
                        # Download tile with geographic region
                        url = dem_reprojected.getDownloadURL({
                            'format': 'GEO_TIFF',
                            'crs': self.settings.TARGET_DEM_CRS,
                            'scale': resolution,  # MUST maintain 10m resolution
                            'region': tile_geometry
                        })
                        
                        response = requests.get(url, timeout=300)
                        response.raise_for_status()
                        
                        # Save tile
                        tile_path = os.path.join(temp_dir, f'tile_{i}_{j}.tif')
                        with open(tile_path, 'wb') as f:
                            f.write(response.content)
                        
                        tile_paths.append(tile_path)
                        successful_tiles += 1
                        
                        print(f"    ✓ Tile {i+1},{j+1} downloaded ({len(response.content)/1024/1024:.1f} MB)")
                        
                    except Exception as e:
                        print(f"    ✗ Tile {i+1},{j+1} failed: {e}")
                        continue
            
            if successful_tiles == 0:
                raise Exception("No tiles downloaded successfully")
            
            print(f"✓ Downloaded {successful_tiles}/{tiles_x * tiles_y} tiles successfully")
            
            # Merge tiles
            print("Merging tiles...")
            output_path = self.settings.get_storage_path('dem', f'huc_{huc_id}_dem_10m.tif')
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Read and merge tiles
            from rasterio.merge import merge
            
            src_files = []
            for tile_path in tile_paths:
                if os.path.exists(tile_path):
                    src_files.append(rasterio.open(tile_path))
            
            if src_files:
                mosaic, out_trans = merge(src_files)
                
                # Get profile from first source
                out_meta = src_files[0].meta.copy()
                out_meta.update({
                    "driver": "GTiff",
                    "height": mosaic.shape[1],
                    "width": mosaic.shape[2],
                    "transform": out_trans,
                    "crs": self.settings.TARGET_DEM_CRS
                })
                
                # Write merged DEM
                with rasterio.open(output_path, "w", **out_meta) as dest:
                    dest.write(mosaic)
                
                # Close source files
                for src in src_files:
                    src.close()
                
                print(f"✓ DEM merged and saved: {output_path}")
            else:
                raise Exception("No valid tiles to merge")
            
            # Clean up temp tiles
            for tile_path in tile_paths:
                if os.path.exists(tile_path):
                    os.remove(tile_path)
            
            if os.path.exists(temp_dir) and not os.listdir(temp_dir):
                os.rmdir(temp_dir)
            
            # Verify final output at correct resolution
            with rasterio.open(output_path) as src:
                print(f"✓ Final DEM: {src.count} bands, {src.width}x{src.height}")
                print(f"  Resolution: {abs(src.transform.a):.1f}m x {abs(src.transform.e):.1f}m")
                print(f"  CRS: {src.crs}")
                
                # Verify resolution is correct
                actual_res = abs(src.transform.a)
                if abs(actual_res - resolution) > 0.1:
                    print(f"  ⚠️  Resolution warning: expected {resolution}m, got {actual_res:.1f}m")
                else:
                    print(f"  ✓ Resolution verified: {actual_res:.1f}m")
            
            # Register in data manager
            success = self.data_manager.register_raster('dem', output_path)
            if success:
                print(f"✓ DEM registered in spatial index")
            
            # Log the download
            self.download_log.append({
                'source': 'dem',
                'huc_id': huc_id,
                'file_path': output_path,
                'resolution': resolution,
                'tiles_downloaded': successful_tiles,
                'total_tiles': tiles_x * tiles_y,
                'timestamp': datetime.now().isoformat()
            })
            
            return output_path
            
        except Exception as e:
            print(f"✗ Error downloading research-grade DEM for HUC {huc_id}: {e}")
            raise

    def _mask_landsat_clouds(self, image: ee.Image) -> ee.Image:
        """Apply cloud masking to Landsat image (from your existing pipeline)"""
        qa = image.select('QA_PIXEL')
        cloud_shadow_bit_mask = 1 << 4
        clouds_bit_mask = 1 << 3
        mask = qa.bitwiseAnd(cloud_shadow_bit_mask).eq(0).And(
               qa.bitwiseAnd(clouds_bit_mask).eq(0))
        
        # Apply scaling factors
        optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
        thermal_band = image.select('ST_B10').multiply(0.00341802).add(149.0)
        
        return image.addBands(optical_bands, None, True).addBands(
               thermal_band, None, True).updateMask(mask)
    
    def download_landsat_for_huc(self, huc_id: str, start_date: str, end_date: str) -> Dict[str, str]:
        """
        Download Landsat optical and thermal data for a HUC
        
        Args:
            huc_id: HUC8 identifier
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Dictionary with paths to optical and thermal files
        """
        print(f"\n=== Downloading Landsat for HUC {huc_id} ===")
        
        try:
            # Get HUC geometry
            huc_geometry = self.get_huc_geometry(huc_id)
            huc_geometry_buffered = huc_geometry.buffer(5000)
            
            # Get Landsat collection
            print("Preparing Landsat composite...")
            landsat_col = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')\
                .filterBounds(huc_geometry_buffered)\
                .filterDate(start_date, end_date)\
                .map(self._mask_landsat_clouds)
            
            # Create composites
            optical_composite = landsat_col.select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']).median()
            thermal_composite = landsat_col.select('ST_B10').median()
            
            # Reproject
            optical_reprojected = optical_composite.reproject(
                crs=self.settings.TARGET_DEM_CRS,
                scale=self.settings.SOURCE_RESOLUTIONS['optical']
            )
            thermal_reprojected = thermal_composite.reproject(
                crs=self.settings.TARGET_DEM_CRS,
                scale=self.settings.SOURCE_RESOLUTIONS['thermal']
            )
            
            # Prepare output paths
            optical_path = self.settings.get_storage_path('landsat', f'huc_{huc_id}_optical.tif')
            thermal_path = self.settings.get_storage_path('landsat', f'huc_{huc_id}_thermal.tif')
            os.makedirs(os.path.dirname(optical_path), exist_ok=True)
            
            # Download optical data (6 bands)
            print("Downloading optical data (6 bands)...")
            optical_url = optical_reprojected.getDownloadURL({
                'format': 'GEO_TIFF',
                'crs': self.settings.TARGET_DEM_CRS
            })
            
            response = requests.get(optical_url)
            response.raise_for_status()
            
            with open(optical_path, 'wb') as f:
                f.write(response.content)
            
            # Download thermal data (1 band)
            print("Downloading thermal data (1 band)...")
            thermal_url = thermal_reprojected.getDownloadURL({
                'format': 'GEO_TIFF',
                'crs': self.settings.TARGET_DEM_CRS
            })
            
            response = requests.get(thermal_url)
            response.raise_for_status()
            
            with open(thermal_path, 'wb') as f:
                f.write(response.content)
            
            # Verify downloads
            with rasterio.open(optical_path) as src:
                print(f"✓ Optical downloaded: {src.count} bands, {src.width}x{src.height}")
            
            with rasterio.open(thermal_path) as src:
                print(f"✓ Thermal downloaded: {src.count} bands, {src.width}x{src.height}")
            
            # Register in data manager
            self.data_manager.register_raster('optical', optical_path)
            self.data_manager.register_raster('thermal', thermal_path)
            print(f"✓ Landsat data registered in spatial index")
            
            # Log downloads
            for source, path in [('optical', optical_path), ('thermal', thermal_path)]:
                bands = 6 if source == 'optical' else 1
                self.download_log.append({
                    'source': source,
                    'huc_id': huc_id,
                    'output_path': path,
                    'bands': bands,
                    'timestamp': datetime.now(),
                    'status': 'success',
                    'size_mb': os.path.getsize(path) / (1024 * 1024)
                })
            
            return {'optical': optical_path, 'thermal': thermal_path}
            
        except Exception as e:
            print(f"✗ Error downloading Landsat for HUC {huc_id}: {e}")
            for source in ['optical', 'thermal']:
                self.download_log.append({
                    'source': source,
                    'huc_id': huc_id,
                    'output_path': None,
                    'timestamp': datetime.now(),
                    'status': 'failed',
                    'error': str(e)
                })
            raise
    
    def download_sar_for_huc(self, huc_id: str, start_date: str, end_date: str) -> str:
        """Download Sentinel-1 SAR data for a HUC"""
        print(f"\n=== Downloading SAR for HUC {huc_id} ===")
        
        try:
            # Get HUC geometry
            huc_geometry = self.get_huc_geometry(huc_id)
            huc_geometry_buffered = huc_geometry.buffer(5000)
            
            # Get SAR collection
            print("Preparing SAR composite...")
            sar_col = ee.ImageCollection('COPERNICUS/S1_GRD')\
                .filterBounds(huc_geometry_buffered)\
                .filterDate(start_date, end_date)\
                .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))\
                .filter(ee.Filter.eq('instrumentMode', 'IW'))\
                .select('VV')
            
            sar_composite = sar_col.median()
            
            # Reproject
            sar_reprojected = sar_composite.reproject(
                crs=self.settings.TARGET_DEM_CRS,
                scale=self.settings.SOURCE_RESOLUTIONS['sar']
            )
            
            # Download
            output_path = self.settings.get_storage_path('sentinel1', f'huc_{huc_id}_sar.tif')
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            print("Downloading SAR data...")
            url = sar_reprojected.getDownloadURL({
                'format': 'GEO_TIFF',
                'crs': self.settings.TARGET_DEM_CRS
            })
            
            response = requests.get(url)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            # Verify
            with rasterio.open(output_path) as src:
                print(f"✓ SAR downloaded: {src.count} bands, {src.width}x{src.height}")
            
            # Register
            self.data_manager.register_raster('sar', output_path)
            print(f"✓ SAR data registered in spatial index")
            
            # Log
            self.download_log.append({
                'source': 'sar',
                'huc_id': huc_id,
                'output_path': output_path,
                'bands': 1,
                'timestamp': datetime.now(),
                'status': 'success',
                'size_mb': os.path.getsize(output_path) / (1024 * 1024)
            })
            
            return output_path
            
        except Exception as e:
            print(f"✗ Error downloading SAR for HUC {huc_id}: {e}")
            self.download_log.append({
                'source': 'sar',
                'huc_id': huc_id,
                'output_path': None,
                'timestamp': datetime.now(),
                'status': 'failed',
                'error': str(e)
            })
            raise
    
    def download_all_sources_for_huc(self, huc_id: str, start_date: str = '2023-01-01', 
                                   end_date: str = '2024-12-31') -> Dict[str, str]:
        """
        Download all satellite data sources for a HUC
        
        Args:
            huc_id: HUC8 identifier
            start_date: Start date for temporal data
            end_date: End date for temporal data
            
        Returns:
            Dictionary with paths to all downloaded files
        """
        print(f"\n{'='*60}")
        print(f"DOWNLOADING ALL SOURCES FOR HUC {huc_id}")
        print(f"Date range: {start_date} to {end_date}")
        print(f"{'='*60}")
        
        start_time = time.time()
        results = {}
        
        try:
            # 1. Download DEM (required for other processing)
            results['dem'] = self.download_dem_for_huc(huc_id)
            
            # 2. Download Landsat (optical + thermal)
            landsat_results = self.download_landsat_for_huc(huc_id, start_date, end_date)
            results.update(landsat_results)
            
            # 3. Download SAR
            results['sar'] = self.download_sar_for_huc(huc_id, start_date, end_date)
            
            elapsed_time = time.time() - start_time
            
            print(f"\n{'='*60}")
            print(f"✓ DOWNLOAD COMPLETE FOR HUC {huc_id}")
            print(f"Time elapsed: {elapsed_time:.1f} seconds")
            print(f"Files downloaded: {len(results)}")
            
            # Calculate total size
            total_size_mb = sum(
                entry['size_mb'] for entry in self.download_log 
                if entry['huc_id'] == huc_id and entry['status'] == 'success'
            )
            print(f"Total size: {total_size_mb:.1f} MB")
            print(f"{'='*60}")
            
            return results
            
        except Exception as e:
            print(f"\n✗ DOWNLOAD FAILED FOR HUC {huc_id}: {e}")
            return results
    
    def get_download_summary(self) -> Dict:
        """Get summary of all downloads"""
        summary = {
            'total_downloads': len(self.download_log),
            'successful': len([e for e in self.download_log if e['status'] == 'success']),
            'failed': len([e for e in self.download_log if e['status'] == 'failed']),
            'total_size_mb': sum(e.get('size_mb', 0) for e in self.download_log if e['status'] == 'success'),
            'by_source': {},
            'by_huc': {}
        }
        
        for entry in self.download_log:
            source = entry['source']
            huc_id = entry['huc_id']
            
            if source not in summary['by_source']:
                summary['by_source'][source] = {'count': 0, 'size_mb': 0, 'bands': 0}
            if huc_id not in summary['by_huc']:
                summary['by_huc'][huc_id] = {'count': 0, 'size_mb': 0}
            
            if entry['status'] == 'success':
                summary['by_source'][source]['count'] += 1
                summary['by_source'][source]['size_mb'] += entry.get('size_mb', 0)
                summary['by_source'][source]['bands'] += entry.get('bands', 0)
                
                summary['by_huc'][huc_id]['count'] += 1
                summary['by_huc'][huc_id]['size_mb'] += entry.get('size_mb', 0)
        
        return summary

    def _calculate_safe_scale(self, geometry: ee.Geometry, target_scale: float, max_pixels: int = 30000) -> float:
        """
        Calculate a safe scale for GEE download that stays within pixel limits
        
        Args:
            geometry: The area geometry
            target_scale: Desired scale in meters
            max_pixels: Maximum pixels per dimension (default: 30000 for safety)
            
        Returns:
            Safe scale in meters
        """
        try:
            # Get geometry bounds in projected meters
            min_x, min_y, max_x, max_y, width_m, height_m = self._get_projected_bounds_meters(geometry)
            
            # Calculate minimum scale needed to stay under pixel limit
            min_scale_x = width_m / max_pixels
            min_scale_y = height_m / max_pixels
            min_scale = max(min_scale_x, min_scale_y, target_scale)
            
            if min_scale > target_scale:
                est_pixels_x = width_m / min_scale
                est_pixels_y = height_m / min_scale
                print(f"  ⚠️  Adjusting scale from {target_scale}m to {min_scale:.1f}m")
                print(f"      Area: {width_m:.0f}m x {height_m:.0f}m")
                print(f"      Est. pixels: {est_pixels_x:.0f} x {est_pixels_y:.0f}")
            
            return min_scale
            
        except Exception as e:
            print(f"  ⚠️  Error calculating safe scale, using target scale: {e}")
            return target_scale
    
    def _should_tile_download(self, geometry: ee.Geometry, scale: float, max_pixels: int = 25000) -> bool:
        """
        Check if area is too large and needs to be tiled
        
        Args:
            geometry: Area geometry
            scale: Download scale in meters
            max_pixels: Maximum pixels before tiling
            
        Returns:
            True if tiling is needed
        """
        try:
            min_x, min_y, max_x, max_y, width_m, height_m = self._get_projected_bounds_meters(geometry)
            
            width_pixels = width_m / scale
            height_pixels = height_m / scale
            
            return width_pixels > max_pixels or height_pixels > max_pixels
            
        except Exception:
            return False  # If we can't determine, don't tile

    def _get_projected_bounds_meters(self, geometry: ee.Geometry, target_crs: str = 'EPSG:5070') -> tuple:
        """
        Get geometry bounds in meters using geographic to projected conversion
        
        Args:
            geometry: The geometry to get bounds for
            target_crs: Target projected CRS (default: EPSG:5070 - Albers)
            
        Returns:
            (min_x, min_y, max_x, max_y, width_m, height_m) in meters
        """
        try:
            # Get geographic bounds first (this always works)
            bounds_info = geometry.bounds().getInfo()
            coords = bounds_info['coordinates'][0]
            
            # Extract geographic bounds
            lons = [point[0] for point in coords]
            lats = [point[1] for point in coords]
            min_lon, max_lon = min(lons), max(lons)
            min_lat, max_lat = min(lats), max(lats)
            
            # Convert to approximate meters using spherical earth model
            import math
            lat_center = (min_lat + max_lat) / 2
            
            # Meters per degree at this latitude
            meters_per_deg_lat = 111320  # Constant
            meters_per_deg_lon = 111320 * math.cos(math.radians(lat_center))
            
            # Calculate dimensions in meters
            width_m = (max_lon - min_lon) * meters_per_deg_lon
            height_m = (max_lat - min_lat) * meters_per_deg_lat
            
            # For projected coordinates, we can use the approximate center
            center_x = (min_lon + max_lon) / 2 * meters_per_deg_lon
            center_y = (min_lat + max_lat) / 2 * meters_per_deg_lat
            
            # Calculate projected bounds (approximate)
            min_x = center_x - width_m / 2
            max_x = center_x + width_m / 2
            min_y = center_y - height_m / 2
            max_y = center_y + height_m / 2
            
            return min_x, min_y, max_x, max_y, width_m, height_m
            
        except Exception as e:
            print(f"  ⚠️  Error getting projected bounds: {e}")
            raise

if __name__ == "__main__":
    # Test with a single HUC
    print("=== GEE Bulk Downloader Test ===")
    
    try:
        # Initialize
        settings = LocalProcessingSettings()
        downloader = GEEBulkDownloader(settings)
        
        # Test with a sample HUC
        test_huc = '10020007'  # Known HUC from your data
        
        print(f"\nTesting with HUC {test_huc}...")
        
        # Download all sources
        results = downloader.download_all_sources_for_huc(test_huc)
        
        # Print summary
        print(f"\n=== Download Summary ===")
        summary = downloader.get_download_summary()
        
        print(f"Total downloads: {summary['successful']}/{summary['total_downloads']} successful")
        print(f"Total size: {summary['total_size_mb']:.1f} MB")
        
        print(f"\nBy source:")
        for source, info in summary['by_source'].items():
            print(f"  {source:10}: {info['count']} files, {info['bands']} bands, {info['size_mb']:.1f} MB")
        
        print(f"\nFiles downloaded:")
        for source, path in results.items():
            if path:
                print(f"  {source:10}: {os.path.basename(path)}")
        
        print(f"\n✓ GEE integration test successful!")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
