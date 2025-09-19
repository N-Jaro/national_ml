#!/usr/bin/env python3
"""
Tiled Landsat Downloader - Downloads real Landsat imagery in manageable tiles.
"""

import ee
import rasterio
import numpy as np
from pathlib import Path
import os
import sys
from datetime import datetime
import geopandas as gpd
from shapely.geometry import box
import pyproj
from rasterio.transform import from_bounds
from rasterio.crs import CRS
from rasterio.merge import merge
import requests
import zipfile
import tempfile
import time
import logging
import math

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_gee():
    """Initialize Google Earth Engine."""
    try:
        # Add parent directory to path to import config
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from config import Settings
        
        # Get project ID from config
        settings = Settings()
        project_id = settings.GEE_PROJECT_ID
        
        # Initialize with project ID
        ee.Initialize(project=project_id)
        logger.info(f"✅ Google Earth Engine initialized with project: {project_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize Google Earth Engine: {e}")
        logger.info("Please run 'earthengine authenticate' first")
        return False

def get_huc_geometry(huc_id: str):
    """Get HUC geometry from local data or create bounding box."""
    
    # For HUC 10020007, use known bounds
    huc_bounds = {
        "10020007": {
            "min_lon": -75.5,
            "min_lat": 42.0, 
            "max_lon": -74.5,
            "max_lat": 42.5
        }
    }
    
    if huc_id in huc_bounds:
        bounds = huc_bounds[huc_id]
        from shapely.geometry import box
        geom = box(bounds["min_lon"], bounds["min_lat"], 
                  bounds["max_lon"], bounds["max_lat"])
        return geom
    else:
        raise ValueError(f"HUC {huc_id} bounds not defined")

def create_landsat_composite(start_date: str, end_date: str, geometry):
    """Create a cloud-free Landsat composite."""
    
    # Convert geometry to EE geometry
    bounds = geometry.bounds
    ee_geometry = ee.Geometry.Rectangle([bounds[0], bounds[1], bounds[2], bounds[3]])
    
    # Get Landsat 8-9 Collection 2 Surface Reflectance
    collection = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
                 .merge(ee.ImageCollection('LANDSAT/LC09/C02/T1_L2'))
                 .filterDate(start_date, end_date)
                 .filterBounds(ee_geometry)
                 .filter(ee.Filter.lt('CLOUD_COVER', 20)))  # Less than 20% cloud cover
    
    logger.info(f"Found {collection.size().getInfo()} Landsat images")
    
    def mask_clouds(image):
        """Mask clouds using QA_PIXEL band."""
        qa = image.select('QA_PIXEL')
        
        # Bits for cloud and cloud shadow
        cloud_bit = 1 << 3
        shadow_bit = 1 << 4
        
        # Create mask
        mask = qa.bitwiseAnd(cloud_bit).eq(0).And(qa.bitwiseAnd(shadow_bit).eq(0))
        
        # Apply scaling for Collection 2
        optical = image.select(['SR_B.*']).multiply(0.0000275).add(-0.2)
        
        return image.addBands(optical, None, True).updateMask(mask)
    
    # Apply cloud masking and create median composite
    composite = collection.map(mask_clouds).median()
    
    # Select RGB + NIR bands only (to keep download size manageable)
    bands = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5']  # Blue, Green, Red, NIR
    composite = composite.select(bands)
    
    return composite

def create_tiles(geometry, tile_size_degrees=0.1):
    """Create tiles for downloading large areas."""
    
    bounds = geometry.bounds
    min_x, min_y, max_x, max_y = bounds
    
    tiles = []
    
    # Calculate number of tiles needed
    x_tiles = math.ceil((max_x - min_x) / tile_size_degrees)
    y_tiles = math.ceil((max_y - min_y) / tile_size_degrees)
    
    logger.info(f"Creating {x_tiles} x {y_tiles} = {x_tiles * y_tiles} tiles")
    
    for i in range(x_tiles):
        for j in range(y_tiles):
            tile_min_x = min_x + i * tile_size_degrees
            tile_max_x = min(min_x + (i + 1) * tile_size_degrees, max_x)
            tile_min_y = min_y + j * tile_size_degrees
            tile_max_y = min(min_y + (j + 1) * tile_size_degrees, max_y)
            
            tile_geom = box(tile_min_x, tile_min_y, tile_max_x, tile_max_y)
            tiles.append((i, j, tile_geom))
    
    return tiles

def download_tile(image, tile_geom, tile_id, temp_dir, resolution=30):
    """Download a single tile."""
    
    bounds = tile_geom.bounds
    ee_geometry = ee.Geometry.Rectangle([bounds[0], bounds[1], bounds[2], bounds[3]])
    
    # Clip image to tile
    clipped = image.clip(ee_geometry)
    
    # Create download parameters
    download_params = {
        'name': f'landsat_tile_{tile_id}',
        'scale': resolution,
        'region': ee_geometry,
        'fileFormat': 'GeoTIFF',
        'formatOptions': {
            'cloudOptimized': True
        }
    }
    
    # Get download URL
    try:
        url = clipped.getDownloadURL(download_params)
        
        # Download the file
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Save to temporary zip file
        temp_zip_path = temp_dir / f"tile_{tile_id}.zip"
        with open(temp_zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Extract the GeoTIFF from the zip
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_files = zip_ref.namelist()
            tiff_file = [f for f in zip_files if f.endswith('.tif')][0]
            
            # Extract to temp directory
            zip_ref.extract(tiff_file, temp_dir)
            temp_tiff_path = temp_dir / tiff_file
            
            # Rename to consistent naming
            final_tile_path = temp_dir / f"tile_{tile_id}.tif"
            os.rename(temp_tiff_path, final_tile_path)
        
        # Clean up zip
        os.unlink(temp_zip_path)
        
        logger.info(f"✅ Downloaded tile {tile_id}")
        return final_tile_path
        
    except Exception as e:
        logger.warning(f"❌ Failed to download tile {tile_id}: {e}")
        return None

def merge_and_reproject_tiles(tile_paths, output_path, target_geometry, resolution=30):
    """Merge tiles and reproject to target CRS."""
    
    if not tile_paths:
        raise ValueError("No tiles to merge")
    
    logger.info(f"Merging {len(tile_paths)} tiles...")
    
    # Open all tile files
    src_files = []
    for tile_path in tile_paths:
        if tile_path and os.path.exists(tile_path):
            src_files.append(rasterio.open(tile_path))
    
    if not src_files:
        raise ValueError("No valid tile files found")
    
    # Merge tiles
    mosaic, out_trans = merge(src_files)
    
    # Get merged profile
    out_meta = src_files[0].meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_trans,
        "compress": "lzw"
    })
    
    # Save merged file temporarily
    temp_merged = output_path.parent / "temp_merged.tif"
    with rasterio.open(temp_merged, "w", **out_meta) as dest:
        dest.write(mosaic)
    
    # Close source files
    for src in src_files:
        src.close()
    
    # Reproject to EPSG:5070
    reproject_to_target_crs(str(temp_merged), str(output_path), target_geometry, resolution)
    
    # Clean up temporary file
    os.unlink(temp_merged)

def reproject_to_target_crs(input_path: str, output_path: str, geometry, resolution: float):
    """Reproject Landsat data to EPSG:5070 and crop to exact bounds."""
    
    # Transform geometry bounds to EPSG:5070
    transformer = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
    bounds = geometry.bounds
    min_x, min_y = transformer.transform(bounds[0], bounds[1])
    max_x, max_y = transformer.transform(bounds[2], bounds[3])
    
    # Calculate output dimensions
    width = int((max_x - min_x) / resolution)
    height = int((max_y - min_y) / resolution)
    
    # Create output transform
    transform = from_bounds(min_x, min_y, max_x, max_y, width, height)
    
    # Reproject and save
    with rasterio.open(input_path) as src:
        from rasterio.warp import calculate_default_transform, reproject, Resampling
        
        dst_crs = CRS.from_epsg(5070)
        
        # Create output file
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

def main():
    """Download real Landsat data for HUC 10020007 in tiles."""
    
    # Initialize GEE
    if not initialize_gee():
        return
    
    # Parameters
    huc_id = "10020007"
    start_date = "2023-06-01"
    end_date = "2023-09-30"
    resolution = 30.0
    
    # Get HUC geometry
    geometry = get_huc_geometry(huc_id)
    logger.info(f"Processing HUC {huc_id}")
    
    # Create Landsat composite
    logger.info("Creating cloud-free Landsat composite...")
    composite = create_landsat_composite(start_date, end_date, geometry)
    
    # Create output directory
    output_dir = Path("/u/nathanj/national_ml/data/local_rasters/landsat")
    output_dir.mkdir(exist_ok=True)
    
    # Output path
    output_filename = f"huc_{huc_id}_landsat_{int(resolution)}m_{start_date}_{end_date}.tif"
    output_path = output_dir / output_filename
    
    # Create temporary directory for tiles
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create tiles
        tiles = create_tiles(geometry, tile_size_degrees=0.15)  # Smaller tiles
        
        # Download each tile
        tile_paths = []
        for i, j, tile_geom in tiles:
            tile_id = f"{i}_{j}"
            tile_path = download_tile(composite, tile_geom, tile_id, temp_path, resolution)
            if tile_path:
                tile_paths.append(tile_path)
        
        # Merge tiles and reproject
        if tile_paths:
            merge_and_reproject_tiles(tile_paths, output_path, geometry, resolution)
            
            # Validate the downloaded data
            with rasterio.open(output_path) as src:
                logger.info(f"✅ Downloaded Landsat data:")
                logger.info(f"   📐 Dimensions: {src.width} x {src.height}")
                logger.info(f"   📊 Bands: {src.count}")
                logger.info(f"   🗺️  CRS: {src.crs}")
                logger.info(f"   📏 Resolution: {src.res[0]:.1f}m")
                
                # Check data ranges
                for i in range(1, src.count + 1):
                    data = src.read(i)
                    valid_data = data[~np.isnan(data)]
                    if len(valid_data) > 0:
                        logger.info(f"   Band {i}: {valid_data.min():.4f} to {valid_data.max():.4f}")
                
                file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                logger.info(f"   📏 File size: {file_size_mb:.1f}MB")
        else:
            logger.error("❌ No tiles downloaded successfully")

if __name__ == "__main__":
    main()