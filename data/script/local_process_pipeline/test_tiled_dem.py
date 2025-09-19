#!/usr/bin/env python3
"""
Test tiled DEM download for large HUC areas
"""

import os
import sys
import rasterio
from gee_bulk_downloader import GEEBulkDownloader
from local_config import LocalProcessingSettings

def test_tiled_dem_download():
    """Test the tiled DEM download method"""
    
    print("=== Testing Tiled DEM Download ===")
    
    # Initialize downloader
    print("\n1. Initializing GEE bulk downloader...")
    settings = LocalProcessingSettings()
    downloader = GEEBulkDownloader(settings)
    print("✓ Downloader initialized")
    
    # Test HUC (same as existing test data)
    test_huc = '10020007'
    print(f"\n2. Testing with HUC {test_huc}...")
    
    try:
        # Use the tiled download method with small tiles for testing
        print("Downloading DEM data with tiled method...")
        dem_path = downloader.download_dem_for_huc_tiled(
            test_huc, 
            buffer_meters=1000,  # Small buffer for testing
            tile_size_m=30000    # 30km tiles for testing
        )
        print(f"✓ Tiled DEM download completed: {dem_path}")
        
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
                sample_window = rasterio.windows.Window(0, 0, min(50, src.width), min(50, src.height))
                sample = src.read(1, window=sample_window)
                print(f"  - Sample values shape: {sample.shape}")
                
                if sample.size > 0:
                    valid_sample = sample[sample != (src.nodata if src.nodata is not None else -9999)]
                    if valid_sample.size > 0:
                        print(f"  - Sample min/max: {valid_sample.min():.2f} / {valid_sample.max():.2f}")
                    else:
                        print(f"  - No valid data in sample")
                else:
                    print(f"  - Empty sample")
                
            print("✓ DEM file verification successful")
            
        else:
            print(f"✗ File does not exist: {dem_path}")
            return False
            
    except Exception as e:
        print(f"✗ Tiled DEM download failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n🎉 Tiled DEM download test completed successfully!")
    return True

if __name__ == "__main__":
    success = test_tiled_dem_download()
    if success:
        print("\n✓ Tiled download test passed - Option A scaling solution working!")
    else:
        print("\n✗ Tiled download test failed")
        sys.exit(1)
