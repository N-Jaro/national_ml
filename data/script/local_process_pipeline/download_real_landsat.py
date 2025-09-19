#!/usr/bin/env python3
"""
Real Landsat Downloader - Downloads actual Landsat imagery from Google Earth Engine.
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
import requests
import zipfile
import tempfile
import time
import logging

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
    
    # For HUC 10020007, use known bounds (you can expand this for other HUCs)
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
        # Create polygon from bounds
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
        thermal = image.select(['ST_B.*']).multiply(0.00341802).add(149.0)
        
        return image.addBands(optical, None, True).addBands(thermal, None, True).updateMask(mask)
    
    # Apply cloud masking and create median composite
    composite = collection.map(mask_clouds).median()
    
    # Select bands we want (RGB + NIR + SWIR)
    bands = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
    composite = composite.select(bands)
    
    return composite

def download_landsat_image(image, geometry, output_path: str, resolution: float = 30):
    """Download Landsat image using GEE."""
    
    bounds = geometry.bounds
    ee_geometry = ee.Geometry.Rectangle([bounds[0], bounds[1], bounds[2], bounds[3]])
    
    # Clip image to geometry
    clipped = image.clip(ee_geometry)
    
    # Create download URL
    download_params = {
        'name': 'landsat_download',
        'scale': resolution,
        'region': ee_geometry,
        'fileFormat': 'GeoTIFF',
        'formatOptions': {
            'cloudOptimized': True
        }
    }
    
    logger.info("Creating download URL...")
    url = clipped.getDownloadURL(download_params)
    
    # Download the file
    logger.info(f"Downloading Landsat data to {output_path}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Save to temporary zip file first
    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
        for chunk in response.iter_content(chunk_size=8192):
            temp_zip.write(chunk)
        temp_zip_path = temp_zip.name
    
    # Extract the GeoTIFF from the zip
    with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
        zip_files = zip_ref.namelist()
        tiff_file = [f for f in zip_files if f.endswith('.tif')][0]
        
        # Extract to temporary location
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_ref.extract(tiff_file, temp_dir)
            temp_tiff = os.path.join(temp_dir, tiff_file)
            
            # Reproject to EPSG:5070 and save to final location
            reproject_to_target_crs(temp_tiff, output_path, geometry, resolution)
    
    # Clean up temp zip
    os.unlink(temp_zip_path)
    
    # Get file size
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info(f"✅ Landsat download completed: {file_size_mb:.1f}MB")

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
        # Reproject
        from rasterio.warp import calculate_default_transform, reproject, Resampling
        
        dst_crs = CRS.from_epsg(5070)
        dst_transform, dst_width, dst_height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds, resolution=resolution
        )
        
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
    """Download real Landsat data for HUC 10020007."""
    
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
    
    # Download the image
    download_landsat_image(composite, geometry, str(output_path), resolution)
    
    # Validate the downloaded data
    with rasterio.open(output_path) as src:
        logger.info(f"✅ Downloaded Landsat data:")
        logger.info(f"   📐 Dimensions: {src.width} x {src.height}")
        logger.info(f"   📊 Bands: {src.count}")
        logger.info(f"   🗺️  CRS: {src.crs}")
        logger.info(f"   📏 Resolution: {src.res[0]:.1f}m")
        
        # Check data ranges
        for i in range(1, min(4, src.count + 1)):  # Check first 3 bands
            data = src.read(i)
            valid_data = data[~np.isnan(data)]
            if len(valid_data) > 0:
                logger.info(f"   Band {i}: {valid_data.min():.4f} to {valid_data.max():.4f}")

if __name__ == "__main__":
    main()