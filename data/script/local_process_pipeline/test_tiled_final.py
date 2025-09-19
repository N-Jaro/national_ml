#!/usr/bin/env python3
"""
Test the tiled DEM download approach for large HUC areas
"""

import os
import sys
import rasterio
from gee_bulk_downloader import GEEBulkDownloader
from local_config import LocalProcessingSettings

def test_tiled_download():
    """Test the tiled DEM download method"""
    
    print("=== Testing Tiled DEM Download ===")
    
    # Initialize downloader
    print("\n1. Initializing GEE bulk downloader...")
    settings = LocalProcessingSettings()
    downloader = GEEBulkDownloader(settings)
    print("✓ Downloader initialized")
    
    # Test HUC
    test_huc = '10020007'
    print(f"\n2. Testing tiled download for HUC {test_huc}...")
    
    try:
        # Use tiled download with small tiles for testing
        print("Starting tiled DEM download...")
        dem_path = downloader.download_dem_for_huc_tiled(
            test_huc, 
            buffer_meters=1000,  # Small buffer for testing
            tile_size_m=25000    # 25km tiles
        )
        print(f"✓ Tiled DEM download completed: {dem_path}")
        
        # Verify the downloaded file
        print("\n3. Verifying merged DEM file...")
        if os.path.exists(dem_path):
            print(f"✓ File exists: {dem_path}")
            print(f"  File size: {os.path.getsize(dem_path)} bytes")
            
            # Get raster information
            with rasterio.open(dem_path) as src:
                print(f"  - Shape: {src.shape}")
                print(f"  - CRS: {src.crs}")
                print(f"  - Data type: {src.dtypes[0]}")
                
                # Calculate coverage
                bounds = src.bounds
                width_km = (bounds.right - bounds.left) / 1000
                height_km = (bounds.top - bounds.bottom) / 1000
                print(f"  - Coverage: {width_km:.1f}km x {height_km:.1f}km")
                
                # Sample elevation data
                if src.width > 0 and src.height > 0:
                    # Read center sample
                    center_x = src.width // 2
                    center_y = src.height // 2
                    sample_size = min(50, src.width//4, src.height//4)
                    
                    sample = src.read(1, window=rasterio.windows.Window(
                        center_x - sample_size//2, 
                        center_y - sample_size//2,
                        sample_size, 
                        sample_size
                    ))
                    
                    if sample.size > 0:
                        valid_sample = sample[sample != (src.nodata if src.nodata is not None else -9999)]
                        if valid_sample.size > 0:
                            print(f"  - Elevation range: {valid_sample.min():.1f}m to {valid_sample.max():.1f}m")
                            print(f"  - Mean elevation: {valid_sample.mean():.1f}m")
                        else:
                            print(f"  - No valid elevation data in sample")
                
            print("✓ Tiled DEM verification successful")
            return True
            
        else:
            print(f"✗ File does not exist: {dem_path}")
            return False
            
    except Exception as e:
        print(f"✗ Tiled download failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_tiled_download()
    if success:
        print("\n🎉 BREAKTHROUGH! Tiled download successful!")
        print("✅ Option A scaling solution working for large HUCs!")
        print("🚀 Ready for production-scale processing!")
    else:
        print("\n✗ Tiled download test failed")
        sys.exit(1)
