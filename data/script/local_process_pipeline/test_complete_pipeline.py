#!/usr/bin/env python3
"""
Comprehensive test of the fixed GEE bulk downloader
"""

import os
import sys
import rasterio
from gee_bulk_downloader import GEEBulkDownloader
from local_config import LocalProcessingSettings

def test_complete_fixed_pipeline():
    """Test the complete fixed GEE download pipeline"""
    
    print("=== Testing Complete Fixed GEE Pipeline ===")
    
    # Initialize downloader
    print("\n1. Initializing GEE bulk downloader...")
    settings = LocalProcessingSettings()
    downloader = GEEBulkDownloader(settings)
    print("✓ Downloader initialized")
    
    # Test HUC
    test_huc = '10020007'
    print(f"\n2. Testing with HUC {test_huc}...")
    
    try:
        # Test geometry and bounds calculation
        print("Getting HUC geometry...")
        geometry = downloader.get_huc_geometry(test_huc)
        buffered = geometry.buffer(2000)  # 2km buffer for testing
        
        # Test scale calculation
        print("Calculating safe scale...")
        safe_scale = downloader._calculate_safe_scale(buffered, 10.0)
        print(f"✓ Safe scale: {safe_scale:.1f}m")
        
        # Test tiling decision
        needs_tiling = downloader._should_tile_download(buffered, safe_scale)
        print(f"✓ Needs tiling: {needs_tiling}")
        
        # Attempt actual download with improved method
        print("\\n3. Attempting DEM download...")
        dem_path = downloader.download_dem_for_huc_export(test_huc, buffer_meters=2000)
        print(f"✓ DEM download completed: {dem_path}")
        
        # Verify the downloaded file
        print("\\n4. Verifying downloaded DEM file...")
        if os.path.exists(dem_path):
            print(f"✓ File exists: {dem_path}")
            print(f"  File size: {os.path.getsize(dem_path)} bytes")
            
            # Get raster information
            with rasterio.open(dem_path) as src:
                print(f"  - Shape: {src.shape}")
                print(f"  - CRS: {src.crs}")
                print(f"  - Data type: {src.dtypes[0]}")
                
                # Calculate actual area coverage
                bounds = src.bounds
                width_m = bounds.right - bounds.left
                height_m = bounds.top - bounds.bottom
                print(f"  - Coverage: {width_m/1000:.1f}km x {height_m/1000:.1f}km")
                
                # Sample data statistics
                if src.width > 0 and src.height > 0:
                    sample_size = min(100, src.width, src.height)
                    sample = src.read(1, 
                        window=rasterio.windows.Window(0, 0, sample_size, sample_size)
                    )
                    
                    if sample.size > 0:
                        valid_sample = sample[sample != (src.nodata if src.nodata is not None else -9999)]
                        if valid_sample.size > 0:
                            print(f"  - Elevation range: {valid_sample.min():.1f}m to {valid_sample.max():.1f}m")
                        else:
                            print(f"  - No valid elevation data in sample")
                    else:
                        print(f"  - Empty sample")
                
            print("✓ DEM file verification successful")
            return True
            
        else:
            print(f"✗ File does not exist: {dem_path}")
            return False
            
    except Exception as e:
        print(f"✗ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_complete_fixed_pipeline()
    if success:
        print("\\n🎉 Complete fixed pipeline test SUCCESSFUL!")
        print("✅ Option A iteration complete - Real GEE downloads working!")
    else:
        print("\\n✗ Pipeline test failed")
        sys.exit(1)
