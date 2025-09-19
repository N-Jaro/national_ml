#!/usr/bin/env python3
"""
Test real DEM download using GEE bulk downloader
"""

import os
import sys
import time
import rasterio
from gee_bulk_downloader import GEEBulkDownloader
from local_config import LocalProcessingSettings

def test_dem_download():
    """Test downloading DEM data for a real HUC"""
    
    print("=== Testing Real DEM Download ===")
    
    # Initialize downloader
    print("\n1. Initializing GEE bulk downloader...")
    settings = LocalProcessingSettings()
    downloader = GEEBulkDownloader(settings)
    print("✓ Downloader initialized")
    
    # Test HUC (same as existing test data)
    test_huc = '10020007'
    print(f"\n2. Testing with HUC {test_huc}...")
    
    try:
        # Download DEM with small buffer for testing
        print("Downloading DEM data...")
        dem_path = downloader.download_dem_for_huc(test_huc, buffer_meters=1000)
        print(f"✓ DEM download completed: {dem_path}")
        
        # Verify the downloaded file
        print("\n3. Verifying downloaded DEM file...")
        if os.path.exists(dem_path):
            print(f"✓ File exists: {dem_path}")
            
            # Get raster information
            with rasterio.open(dem_path) as src:
                print(f"  - Shape: {src.shape}")
                print(f"  - CRS: {src.crs}")
                print(f"  - Bounds: {src.bounds}")
                print(f"  - Data type: {src.dtypes[0]}")
                print(f"  - No data value: {src.nodata}")
                
                # Read a small sample of data
                sample = src.read(1, window=rasterio.windows.Window(0, 0, 10, 10))
                print(f"  - Sample values shape: {sample.shape}")
                print(f"  - Sample min/max: {sample.min():.2f} / {sample.max():.2f}")
                
            print("✓ DEM file verification successful")
            
        else:
            print(f"✗ File does not exist: {dem_path}")
            return False
            
    except Exception as e:
        print(f"✗ DEM download failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n🎉 DEM download test completed successfully!")
    return True

if __name__ == "__main__":
    success = test_dem_download()
    if success:
        print("\n✓ All tests passed - GEE bulk downloader is working correctly!")
    else:
        print("\n✗ Some tests failed - please check the errors above")
        sys.exit(1)
