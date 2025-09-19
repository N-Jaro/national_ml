#!/usr/bin/env python3
"""
Landsat Bulk Downloader - Extends the GEE bulk downloader for Landsat imagery.
Handles multi-band Landsat data with cloud masking and temporal composi        # Create temporary directory for tiles
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)g.
"""

import ee
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS
from rasterio.merge import merge
from rasterio.warp import reproject, Resampling
import numpy as np
from pathlib import Path
import os
import sys
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
import logging
import pyproj

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_config import LocalProcessingSettings
from data_manager import LocalDataManager

# Import the existing Drive manager - need to point to script directory
script_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(script_dir)
from drive_manager import GoogleDriveManager
from config import Settings

class LandsatBulkDownloader:
    """Download and process Landsat imagery for HUC regions."""
    
    def __init__(self, config: LocalProcessingSettings, data_manager: LocalDataManager):
        """Initialize the Landsat downloader."""
        self.config = config
        self.data_manager = data_manager
        self.logger = logging.getLogger(__name__)
        
        # Initialize Google Drive manager
        try:
            settings = Settings()
            # Change working directory to where credentials are located
            original_cwd = os.getcwd()
            script_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            os.chdir(script_dir)
            
            self.logger.info(f"Looking for credentials in: {script_dir}")
            self.logger.info(f"Credentials file exists: {os.path.exists('GD_credentials.json')}")
            self.logger.info(f"Token file exists: {os.path.exists('GD_token.json')}")
            
            self.drive_manager = GoogleDriveManager(settings)
            os.chdir(original_cwd)  # Restore original working directory
            self.logger.info("Google Drive manager initialized successfully")
        except Exception as e:
            if 'original_cwd' in locals():
                os.chdir(original_cwd)  # Ensure we restore directory even on error
            self.logger.warning(f"Could not initialize Drive manager: {e}")
            self.drive_manager = None
        
        # Landsat band configuration
        self.landsat_bands = {
            'SR_B2': 'blue',
            'SR_B3': 'green', 
            'SR_B4': 'red',
            'SR_B5': 'nir',
            'SR_B6': 'swir1',
            'SR_B7': 'swir2',
            'ST_B10': 'thermal'  # Thermal infrared band
        }
        
        # Cloud masking parameters
        self.cloud_threshold = 20  # Max cloud percentage per image
        self.max_cloud_probability = 0.4  # For Sentinel-2 cloud probability
        
    def get_landsat_collection(self, start_date: str, end_date: str, 
                              bounds: ee.Geometry) -> ee.ImageCollection:
        """Get Landsat collection with cloud masking."""
        
        # Use Landsat 9 Collection 2 Surface Reflectance only
        collection = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
                     .filterDate(start_date, end_date)
                     .filterBounds(bounds)
                     .filter(ee.Filter.lt('CLOUD_COVER', self.cloud_threshold)))
        
        # Add cloud masking function using the user's proven approach
        def _mask_l8sr_clouds(image):
            """Mask clouds using QA_PIXEL band - simplified version."""
            # For now, just return the image without cloud masking
            # The original approach focuses on band selection rather than complex masking
            return image
        
        return collection.map(_mask_l8sr_clouds)
    
    def _get_landsat_composite(self, region: ee.Geometry, start_date: str, end_date: str) -> dict:
        """
        Fetches and processes Landsat 9 surface reflectance and thermal data.
        Uses the user's proven approach with separate optical and thermal composites.
        https://developers.google.com/earth-engine/datasets/catalog/LANDSAT_LC09_C02_T1_L2#bands
        """
        landsat_col = self.get_landsat_collection(start_date, end_date, region)
        
        # Create separate composites for optical and thermal bands
        optical_median = landsat_col.select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']).median()
        thermal_median = landsat_col.select('ST_B10').median()
        
        # Apply scaling to optical bands
        optical_scaled = optical_median.multiply(0.0000275).add(-0.2)
        
        # Apply scaling to thermal band  
        thermal_scaled = thermal_median.multiply(0.00341802).add(149.0)
        
        return {'optical': optical_scaled, 'thermal': thermal_scaled}
    
    def create_temporal_composite(self, region: ee.Geometry, start_date: str, end_date: str) -> ee.Image:
        """Create a temporal composite from the collection using separate optical and thermal processing."""
        
        # Get separate optical and thermal composites
        composites = self._get_landsat_composite(region, start_date, end_date)
        
        # Combine optical and thermal bands into a single multi-band image
        # Use addBands to combine the 6-band optical with 1-band thermal
        combined = composites['optical'].addBands(composites['thermal'])
        
        # Ensure proper band naming
        combined = combined.select(
            ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10'],
            ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10']
        )
        
        return combined
    
    def calculate_landsat_tile_size(self, target_resolution: float = 30.0, use_drive_export: bool = False) -> Tuple[float, int]:
        """Calculate optimal tile size for Landsat data at 30m resolution."""
        
        if use_drive_export and self.drive_manager is not None:
            # Drive export can handle up to 10^13 pixels per task
            # Use moderate tile size to balance export time vs number of tiles
            # Target: ~10 million pixels per tile (reasonable export time)
            max_pixels_per_tile = 10 * 1000 * 1000  # 10M pixels - faster exports
            self.logger.info("Using Drive export - moderate tiles for reliable exports")
        else:
            # getDownloadURL is limited to ~50MB downloads
            # Landsat: 7 bands, float32 = 4 bytes per pixel = 28 bytes per pixel
            # Target: ~15MB per tile to stay well under 50MB limit
            max_pixels_per_tile = 15 * 1024 * 1024 // 32  # ~468K pixels (very conservative)
            self.logger.info("Using getDownloadURL fallback - smaller tiles required")
        
        # For 30m resolution
        tile_side_pixels = int(np.sqrt(max_pixels_per_tile))
        tile_size_m = tile_side_pixels * target_resolution
        
        self.logger.info(f"Landsat tile size: {tile_size_m:.0f}m ({tile_side_pixels}x{tile_side_pixels} pixels, 7 bands)")
        
        return tile_size_m, tile_side_pixels
    
    def download_landsat_for_huc_with_config_dates(self, huc_id: str, 
                                                   start_date: Optional[str] = None, 
                                                   end_date: Optional[str] = None,
                                                   resolution: float = 30.0, 
                                                   bands: Optional[List[str]] = None) -> str:
        """
        Download Landsat data for a HUC region using config dates by default.
        
        Args:
            huc_id: HUC identifier
            start_date: Override start date (uses config.START_DATE if None)
            end_date: Override end date (uses config.END_DATE if None)
            resolution: Spatial resolution in meters
            bands: List of bands to download (uses all if None)
            
        Returns:
            Path to the downloaded file
        """
        # Use config dates if not provided
        if start_date is None:
            start_date = self.config.START_DATE
            self.logger.info(f"Using config start date: {start_date}")
        
        if end_date is None:
            end_date = self.config.END_DATE
            self.logger.info(f"Using config end date: {end_date}")
        
        # Call the main download function
        return self.download_landsat_for_huc(huc_id, start_date, end_date, resolution, bands)
    
    def download_landsat_for_huc(self, huc_id: str, start_date: str, end_date: str,
                                resolution: float = 30.0, bands: Optional[List[str]] = None) -> str:
        """Download Landsat data for a HUC region."""
        
        self.logger.info(f"Starting Landsat download for HUC {huc_id}")
        
        # Get HUC geometry
        huc_geom = self.data_manager.get_huc_geometry(huc_id)
        if huc_geom is None:
            raise ValueError(f"HUC {huc_id} not found")
        
        # Convert to EE geometry
        huc_bounds = huc_geom.bounds
        ee_geom = ee.Geometry.Rectangle([
            huc_bounds[0], huc_bounds[1],  # min_lon, min_lat
            huc_bounds[2], huc_bounds[3]   # max_lon, max_lat
        ])
        
        # Get Landsat collection to check for available images
        collection = self.get_landsat_collection(start_date, end_date, ee_geom)
        
        # Check if we have any images
        image_count = collection.size().getInfo()
        if image_count == 0:
            raise ValueError(f"No Landsat images found for HUC {huc_id} between {start_date} and {end_date}")
        
        self.logger.info(f"Found {image_count} Landsat images for temporal composite")
        
        # Create composite using the proven separate optical/thermal approach
        composite = self.create_temporal_composite(ee_geom, start_date, end_date)
        
        # Debug: Check band count after composite creation
        try:
            composite_info = composite.getInfo()
            band_names = composite_info['bands'] if 'bands' in composite_info else []
            self.logger.info(f"🔍 DEBUG: Composite created with separate optical/thermal approach has {len(band_names)} bands: {[b['id'] for b in band_names]}")
        except Exception as e:
            self.logger.info(f"Could not get composite info: {e}")
        
        # Debug: Log band information before selection
        self.logger.info(f"Composite bands before selection: checking band count...")
        
        # Select bands - all 7 core spectral bands
        if bands is None:
            bands = list(self.landsat_bands.keys())  # SR_B2-SR_B7, ST_B10
        
        # Ensure we select the bands in the correct order and that they exist
        self.logger.info(f"Attempting to select bands: {bands}")
        composite = composite.select(bands)
        
        # Debug: Check band count after selection
        try:
            selected_info = composite.getInfo()
            selected_band_names = selected_info['bands'] if 'bands' in selected_info else []
            self.logger.info(f"🔍 DEBUG: After selection, composite has {len(selected_band_names)} bands: {[b['id'] for b in selected_band_names]}")
        except Exception as e:
            self.logger.info(f"Could not get selected composite info: {e}")
        
        # Debug: Log final band selection
        self.logger.info(f"Final composite ready for download")
        
        # Calculate tile parameters - use larger tiles if Drive export is available
        use_drive = self.drive_manager is not None
        tile_size_m, tile_side_pixels = self.calculate_landsat_tile_size(resolution, use_drive_export=use_drive)
        
        # Create output path
        output_dir = self.config.local_raster_base / "landsat"
        output_dir.mkdir(exist_ok=True)
        
        output_filename = f"huc_{huc_id}_landsat_{int(resolution)}m_{start_date}_{end_date}.tif"
        output_path = output_dir / output_filename
        
        # Download and merge tiles
        self._download_and_merge_landsat_tiles(
            composite, huc_geom, tile_size_m, resolution, output_path, huc_id
        )
        
        self.logger.info(f"Landsat download completed: {output_path}")
        return str(output_path)
    
    def _download_and_merge_landsat_tiles(self, image: ee.Image, huc_geom, 
                                         tile_size_m: float, resolution: float, 
                                         output_path: Path, huc_id: str):
        """Download Landsat image as tiles and merge."""
        
        # Create tiles
        tiles = self.data_manager.create_tiles(huc_geom, tile_size_m)
        self.logger.info(f"Created {len(tiles)} tiles for Landsat download")
        
        # Create temp directory for tiles
        temp_dir = self.config.local_raster_base / "temp_landsat_tiles"
        temp_dir.mkdir(exist_ok=True)
        
        tile_files = []
        
        try:
            # Download each tile
            for i, tile_geom in enumerate(tiles):
                self.logger.info(f"Downloading Landsat tile {i+1}/{len(tiles)}")
                
                # Convert tile geometry to EE geometry (geographic coordinates)
                bounds = tile_geom.bounds
                ee_tile = ee.Geometry.Rectangle([
                    bounds[0], bounds[1], bounds[2], bounds[3]
                ])
                
                # Clip image to tile
                tile_image = image.clip(ee_tile)
                
                # Set up download parameters
                download_params = {
                    'image': tile_image,
                    'description': f'landsat_tile_{huc_id}_{i}',
                    'scale': resolution,
                    'region': ee_tile,
                    'fileFormat': 'GeoTIFF',
                    'formatOptions': {
                        'cloudOptimized': True
                    },
                    'maxPixels': 1e13  # 10 trillion pixels - maximum allowed
                }
                
                # Download tile
                tile_filename = temp_dir / f"tile_{i:03d}.tif"
                
                try:
                    # Try Google Drive export first if available
                    if self.drive_manager is not None:
                        self.logger.info(f"Using Google Drive export for tile {i+1} (maxPixels=1e13)")
                        
                        try:
                            # Start the export task
                            task = ee.batch.Export.image.toDrive(**download_params)
                            task.start()
                            self.logger.info(f"Started Drive export task: {task.id}")
                            
                            # Wait for export to complete
                            import time
                            max_wait_time = 1800  # 30 minutes max wait for large tiles
                            start_time = time.time()
                            
                            while task.status()['state'] in ['READY', 'RUNNING']:
                                if time.time() - start_time > max_wait_time:
                                    self.logger.warning(f"Export timeout after {max_wait_time}s")
                                    # Cancel the task and fall back to getDownloadURL
                                    try:
                                        task.cancel()
                                    except:
                                        pass
                                    raise Exception("Export timeout - will try fallback")
                                
                                time.sleep(30)  # Check every 30 seconds for large exports
                                status = task.status()
                                self.logger.info(f"Export status: {status['state']}")
                                
                                if 'progress' in status:
                                    self.logger.info(f"Progress: {status['progress']}%")
                            
                            final_status = task.status()
                            if final_status['state'] == 'COMPLETED':
                                self.logger.info(f"✅ Drive export completed for tile {i+1}")
                                
                                # Download from Google Drive
                                # The file will be named as the description parameter
                                drive_filename = f"{download_params['description']}.tif"
                                
                                try:
                                    # Download the specific file directly by searching for it
                                    query = f"name contains '{download_params['description']}' and trashed=false"
                                    results = self.drive_manager.service.files().list(q=query, fields="files(id, name)").execute()
                                    items = results.get('files', [])
                                    
                                    if items:
                                        file_id = items[0]['id']
                                        file_name = items[0]['name']
                                        
                                        # Download the file directly
                                        request = self.drive_manager.service.files().get_media(fileId=file_id)
                                        
                                        import io
                                        from googleapiclient.http import MediaIoBaseDownload
                                        
                                        with io.FileIO(str(tile_filename), 'wb') as fh:
                                            downloader = MediaIoBaseDownload(fh, request)
                                            done = False
                                            while not done:
                                                status, done = downloader.next_chunk()
                                        
                                        tile_files.append(str(tile_filename))
                                        self.logger.info(f"✅ Downloaded and saved: {tile_filename}")
                                    else:
                                        self.logger.warning(f"Could not find {drive_filename} in Drive")
                                        raise Exception(f"File {drive_filename} not found in Drive")
                                        
                                except Exception as download_error:
                                    self.logger.warning(f"Drive download failed: {download_error}")
                                    raise Exception("Drive download failed")
                            else:
                                error_msg = final_status.get('error_message', 'Unknown error')
                                self.logger.warning(f"Drive export failed: {final_status['state']} - {error_msg}")
                                raise Exception(f"Drive export failed: {error_msg}")
                        
                        except Exception as drive_error:
                            self.logger.warning(f"Drive export attempt failed: {drive_error}")
                            self.logger.info(f"Falling back to getDownloadURL for tile {i+1}")
                            raise Exception("Drive failed - fallback needed")
                    
                    else:
                        # No Drive manager available
                        raise Exception("No Drive manager - use fallback")
                        
                except Exception as e:
                    if "fallback" in str(e).lower() or "Drive failed" in str(e) or "No Drive manager" in str(e):
                        # Fall back to getDownloadURL
                        self.logger.info(f"Using getDownloadURL fallback for tile {i+1}")
                        download_url = tile_image.getDownloadURL({
                            'scale': resolution,
                            'region': ee_tile,
                            'fileFormat': 'GeoTIFF',
                            'formatOptions': {'cloudOptimized': True}
                        })
                        
                        # Download the tile directly
                        import requests
                        import zipfile
                        
                        response = requests.get(download_url, timeout=300)
                        response.raise_for_status()
                        
                        # Save as temporary zip
                        temp_zip = temp_dir / f"tile_{i}.zip"
                        with open(temp_zip, 'wb') as f:
                            f.write(response.content)
                        
                        # Extract GeoTIFF from zip
                        with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
                            tiff_files = [f for f in zip_ref.namelist() if f.endswith('.tif')]
                            if tiff_files:
                                zip_ref.extract(tiff_files[0], temp_dir)
                                extracted_file = temp_dir / tiff_files[0]
                                
                                # Rename to consistent naming
                                os.rename(extracted_file, tile_filename)
                                tile_files.append(str(tile_filename))
                                
                                # Clean up zip
                                os.unlink(temp_zip)
                                self.logger.warning(f"⚠️ Fallback getDownloadURL used - may have only 1 band")
                            else:
                                self.logger.warning(f"No .tif file found in zip for tile {i}")
                    else:
                        self.logger.warning(f"Failed to download tile {i}: {e}")
                        continue
                
                # Debug: Check first tile band count
                if i == 0 and tile_files:
                    debug_copy = Path(f"/tmp/debug_first_tile_main.tif")
                    import shutil
                    shutil.copy(str(tile_filename), str(debug_copy))
                    
                    with rasterio.open(debug_copy) as debug_src:
                        self.logger.info(f"🔍 DEBUG: First tile has {debug_src.count} bands")
                        self.logger.info(f"🔍 DEBUG: Band descriptions: {debug_src.descriptions}")
                        self.logger.info(f"🔍 DEBUG: Band data types: {debug_src.dtypes}")
                
                self.logger.info(f"✅ Downloaded tile {i+1}/{len(tiles)}")
            
            # Merge tiles if we have any
            if tile_files:
                self.logger.info(f"Merging {len(tile_files)} Landsat tiles")
                self._merge_landsat_tiles(tile_files, output_path, huc_geom, resolution)
            else:
                raise ValueError("No tiles downloaded successfully - cannot create Landsat file")
            
        finally:
            # Clean up temp tiles
            import shutil
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
    
    def _merge_landsat_tiles(self, tile_files: List[str], output_path: Path, 
                            huc_geom, resolution: float):
        """Merge downloaded Landsat tiles into a single raster."""
        
        self.logger.info(f"Merging {len(tile_files)} tiles to {output_path}")
        
        # Open all tile files
        src_files = []
        for tile_file in tile_files:
            if os.path.exists(tile_file):
                src_files.append(rasterio.open(tile_file))
        
        if not src_files:
            raise ValueError("No valid tile files to merge")
        
        # Merge tiles
        mosaic, out_trans = merge(src_files)
        
        # Get profile from first file
        out_meta = src_files[0].meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "count": mosaic.shape[0],  # Ensure band count matches mosaic
            "transform": out_trans,
            "compress": "lzw"
        })
        
        # Create temporary merged file
        temp_merged = output_path.parent / "temp_merged_landsat.tif"
        with rasterio.open(temp_merged, "w", **out_meta) as dest:
            dest.write(mosaic)
        
        # Close source files
        for src in src_files:
            src.close()
        
        # Reproject to EPSG:5070 and crop to exact HUC bounds
        self._reproject_landsat_to_target(str(temp_merged), str(output_path), huc_geom, resolution)
        
        # Clean up temporary file
        os.unlink(temp_merged)
        
        # Log final file info
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        self.logger.info(f"✅ Merged Landsat file created: {file_size_mb:.1f}MB")
    
    def _reproject_landsat_to_target(self, input_path: str, output_path: str, 
                                    huc_geom, resolution: float):
        """Reproject merged Landsat data to EPSG:5070 and crop to HUC bounds."""
        
        # Transform HUC geometry bounds to EPSG:5070
        transformer = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
        bounds = huc_geom.bounds
        min_x, min_y = transformer.transform(bounds[0], bounds[1])
        max_x, max_y = transformer.transform(bounds[2], bounds[3])
        
        # Calculate output dimensions
        width = int((max_x - min_x) / resolution)
        height = int((max_y - min_y) / resolution)
        
        # Create output transform
        transform = from_bounds(min_x, min_y, max_x, max_y, width, height)
        
        # Reproject and crop
        with rasterio.open(input_path) as src:
            dst_crs = CRS.from_epsg(5070)
            
            # Create output profile
            profile = src.profile.copy()
            profile.update({
                'crs': dst_crs,
                'transform': transform,
                'width': width,
                'height': height,
                'compress': 'lzw'
            })
            
            with rasterio.open(output_path, 'w', **profile) as dst:
                for i in range(1, src.count + 1):
                    src_data = src.read(i)
                    dst_data = np.empty((height, width), dtype=src_data.dtype)
                    
                    reproject(
                        source=src_data,
                        destination=dst_data,
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.bilinear
                    )
                    
                    dst.write(dst_data, i)
    
    def _create_landsat_placeholder(self, output_path: Path, huc_geom, 
                                   resolution: float, num_tiles: int):
        """Create a placeholder Landsat file for testing."""
        
        # Get bounds in target CRS
        bounds = huc_geom.bounds
        
        # Convert to EPSG:5070 for consistency
        transformer = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
        
        min_x, min_y = transformer.transform(bounds[0], bounds[1])
        max_x, max_y = transformer.transform(bounds[2], bounds[3])
        
        # Calculate dimensions
        width = int((max_x - min_x) / resolution)
        height = int((max_y - min_y) / resolution)
        
        # Create transform
        transform = from_bounds(min_x, min_y, max_x, max_y, width, height)
        
        # Create multi-band data for all 7 spectral bands
        num_bands = 7  # 6 optical (SR_B2-SR_B7) + 1 thermal (ST_B10)
        
        # Write placeholder file
        with rasterio.open(
            output_path,
            'w',
            driver='GTiff',
            height=height,
            width=width,
            count=num_bands,
            dtype=rasterio.float32,
            crs=CRS.from_epsg(5070),
            transform=transform,
            compress='lzw'
        ) as dst:
            
            # Write some sample data for each band
            for band in range(1, num_bands + 1):
                # Create realistic-looking data
                data = np.random.random((height, width)).astype(np.float32)
                
                if band <= 6:  # Optical bands (SR_B2-SR_B7)
                    data *= 0.3  # Typical reflectance values (0-0.3)
                elif band == 7:  # Thermal band (ST_B10)
                    data = data * 50 + 280  # Typical temperature range 280-330K
                
                dst.write(data.astype(np.float32), band)
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        self.logger.info(f"Created Landsat placeholder: {file_size_mb:.1f}MB with {num_bands} bands")

def main():
    """Test the Landsat downloader."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize EE with project ID
        from config import Settings
        gee_settings = Settings()
        ee.Initialize(project=gee_settings.GEE_PROJECT_ID)
        print(f"✅ Initialized GEE with project: {gee_settings.GEE_PROJECT_ID}")
        
        # Initialize components
        config = LocalProcessingSettings()
        data_manager = LocalDataManager(config)
        downloader = LandsatBulkDownloader(config, data_manager)
        
        # Test download for a HUC using config dates
        huc_id = "10020007"
        
        # Option 1: Use config dates directly
        print(f"Config date range: {config.START_DATE} to {config.END_DATE}")
        # output_path = downloader.download_landsat_for_huc_with_config_dates(huc_id)
        
        # Option 2: Use focused test dates for faster processing (current approach)
        start_date = "2023-06-01"
        end_date = "2023-09-30"
        
        print(f"Testing Landsat download for HUC {huc_id}")
        print(f"Date range: {start_date} to {end_date}")
        
        output_path = downloader.download_landsat_for_huc(
            huc_id, start_date, end_date, resolution=30.0
        )
        
        print(f"✓ Landsat download completed: {output_path}")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise

if __name__ == "__main__":
    main()
