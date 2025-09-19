#!/usr/bin/env python3
"""
Minimal DEM download test with very small area
"""

import ee
import os
import sys
import requests
sys.path.append('..')
from config import Settings

def test_minimal_dem_download():
    """Test downloading a very small DEM area"""
    
    print("=== Minimal DEM Download Test ===")
    
    # Initialize
    settings = Settings()
    ee.Initialize(project=settings.GEE_PROJECT_ID)
    print("✓ GEE initialized")
    
    # Get a very small area (just a point buffered)
    point = ee.Geometry.Point([-121.5, 46.0])  # Point in Washington State
    small_area = point.buffer(500)  # 500m buffer = 1km x 1km area
    
    print("✓ Small test area created")
    
    # Get DEM
    dem = ee.ImageCollection(settings.DEM_SOURCE_IMG_NAME).mosaic().select('elevation')
    dem_clipped = dem.clip(small_area)
    
    # Use coarse resolution to ensure small file
    dem_reprojected = dem_clipped.reproject(
        crs='EPSG:5070',
        scale=30  # 30m resolution for small test
    )
    
    print("✓ DEM prepared")
    
    # Download
    try:
        url = dem_reprojected.getDownloadURL({
            'format': 'GEO_TIFF',
            'crs': 'EPSG:5070',
            'scale': 30,
            'region': small_area
        })
        
        print("✓ Download URL obtained")
        
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        output_path = '/u/nathanj/national_ml/data/local_rasters/dem/minimal_test_dem.tif'
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        print(f"✓ Minimal DEM downloaded: {output_path}")
        print(f"  File size: {len(response.content)} bytes")
        
        return True
        
    except Exception as e:
        print(f"✗ Download failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_minimal_dem_download()
    if success:
        print("\n🎉 Minimal download test successful!")
    else:
        print("\n✗ Minimal download test failed")
