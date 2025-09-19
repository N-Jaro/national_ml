#!/usr/bin/env python3
"""
Test HUC DEM download with proper scale adjustment
"""

import ee
import os
import sys
import requests
import rasterio
sys.path.append('..')
from config import Settings

def test_huc_dem_with_scale():
    """Test downloading DEM for HUC with proper scale calculation"""
    
    print("=== HUC DEM Download with Scale Adjustment ===")
    
    # Initialize
    settings = Settings()
    ee.Initialize(project=settings.GEE_PROJECT_ID)
    print("✓ GEE initialized")
    
    # Get HUC geometry
    huc_id = '10020007'
    huc8_col = ee.FeatureCollection(settings.HUC8_COL_NAME)
    huc_feature = huc8_col.filter(ee.Filter.eq('huc8', huc_id)).first()
    huc_name = huc_feature.get('name').getInfo()
    huc_geometry = huc_feature.geometry()
    
    print(f"✓ Found HUC {huc_id}: {huc_name}")
    
    # Add buffer
    buffer_meters = 1000
    huc_buffered = huc_geometry.buffer(buffer_meters)
    
    # Calculate safe scale
    bounds = huc_buffered.bounds().getInfo()['coordinates'][0]
    min_x, min_y = bounds[0]
    max_x, max_y = bounds[2]
    
    width_m = max_x - min_x
    height_m = max_y - min_y
    
    # Calculate scale to keep under 30000 pixels per dimension
    max_pixels = 30000
    target_scale = 10.0  # 10m target resolution
    
    min_scale_x = width_m / max_pixels
    min_scale_y = height_m / max_pixels
    safe_scale = max(min_scale_x, min_scale_y, target_scale)
    
    est_pixels_x = width_m / safe_scale
    est_pixels_y = height_m / safe_scale
    
    print(f"✓ Area dimensions: {width_m:.0f}m x {height_m:.0f}m")
    print(f"✓ Target scale: {target_scale}m -> Safe scale: {safe_scale:.1f}m")
    print(f"✓ Estimated pixels: {est_pixels_x:.0f} x {est_pixels_y:.0f}")
    
    # Get DEM
    dem = ee.ImageCollection(settings.DEM_SOURCE_IMG_NAME).mosaic().select('elevation')
    dem_clipped = dem.clip(huc_buffered)
    
    # Reproject with safe scale
    dem_reprojected = dem_clipped.reproject(
        crs='EPSG:5070',
        scale=safe_scale
    )
    
    print("✓ DEM prepared with safe scale")
    
    # Download
    try:
        print("Getting download URL...")
        url = dem_reprojected.getDownloadURL({
            'format': 'GEO_TIFF',
            'crs': 'EPSG:5070',
            'scale': safe_scale,
            'region': huc_buffered
        })
        
        print("✓ Download URL obtained, downloading...")
        response = requests.get(url, timeout=300)  # 5 minute timeout
        response.raise_for_status()
        
        output_path = f'/u/nathanj/national_ml/data/local_rasters/dem/huc_{huc_id}_dem_fixed.tif'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        print(f"✓ DEM downloaded: {output_path}")
        print(f"  File size: {len(response.content)} bytes")
        
        # Verify with rasterio
        with rasterio.open(output_path) as src:
            print(f"  Shape: {src.shape}")
            print(f"  CRS: {src.crs}")
            print(f"  Bounds: {src.bounds}")
            
        return True
        
    except Exception as e:
        print(f"✗ Download failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_huc_dem_with_scale()
    if success:
        print("\n🎉 HUC DEM download test successful!")
    else:
        print("\n✗ HUC DEM download test failed")
