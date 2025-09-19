#!/usr/bin/env python3
"""
Test research-grade DEM download maintaining 10m resolution
"""

import os
import sys
import rasterio
from gee_bulk_downloader import GEEBulkDownloader
from local_config import LocalProcessingSettings

def test_research_grade_dem():
    """Test the research-grade DEM download at required 10m resolution"""
    
    print("=== Testing Research-Grade DEM Download (10m Resolution) ===")
    
    # Initialize downloader
    print("\n1. Initializing GEE bulk downloader...")
    settings = LocalProcessingSettings()
    downloader = GEEBulkDownloader(settings)
    print("✓ Downloader initialized")
    
    # Test HUC
    test_huc = '10020007'
    print(f"\n2. Testing with HUC {test_huc}...")
    
    try:
        # Use research-grade download with required 10m resolution
        print("Downloading DEM data with research-grade method...")
        dem_path = downloader.download_dem_for_huc_research_grade(
            test_huc, 
            buffer_meters=2000  # Conservative buffer for testing
        )
        print(f"✓ Research-grade DEM download completed: {dem_path}")
        
        # Verify the downloaded file
        print("\n3. Verifying research-grade DEM file...")
        if os.path.exists(dem_path):
            print(f"✓ File exists: {dem_path}")
            file_size_mb = os.path.getsize(dem_path) / 1024 / 1024
            print(f"  File size: {file_size_mb:.1f} MB")
            
            # Verify resolution and other properties
            with rasterio.open(dem_path) as src:
                print(f"  - Shape: {src.shape}")
                print(f"  - CRS: {src.crs}")
                print(f"  - Data type: {src.dtypes[0]}")
                
                # CRITICAL: Verify resolution is exactly 10m
                x_res = abs(src.transform.a)
                y_res = abs(src.transform.e)
                print(f"  - X Resolution: {x_res:.3f}m")
                print(f"  - Y Resolution: {y_res:.3f}m")
                
                if abs(x_res - 10.0) < 0.1 and abs(y_res - 10.0) < 0.1:
                    print(f"  ✓ RESOLUTION VERIFIED: 10m x 10m (research requirement met)")
                else:
                    print(f"  ✗ RESOLUTION ERROR: Expected 10m, got {x_res:.1f}m x {y_res:.1f}m")
                    return False
                
                # Calculate actual area coverage
                bounds = src.bounds
                width_km = (bounds.right - bounds.left) / 1000
                height_km = (bounds.top - bounds.bottom) / 1000
                print(f"  - Coverage: {width_km:.1f}km x {height_km:.1f}km")
                
                # Sample elevation data
                if src.width > 0 and src.height > 0:
                    sample_size = min(100, src.width, src.height)
                    sample = src.read(1, 
                        window=rasterio.windows.Window(0, 0, sample_size, sample_size)
                    )
                    
                    if sample.size > 0:
                        valid_sample = sample[sample != (src.nodata if src.nodata is not None else -9999)]
                        if valid_sample.size > 0:
                            print(f"  - Elevation range: {valid_sample.min():.1f}m to {valid_sample.max():.1f}m")
                            print(f"  - Valid pixels: {valid_sample.size}/{sample.size} ({100*valid_sample.size/sample.size:.1f}%)")
                        else:
                            print(f"  - No valid elevation data in sample")
                    else:
                        print(f"  - Empty sample")
                
            print("✓ Research-grade DEM file verification successful")
            return True
            
        else:
            print(f"✗ File does not exist: {dem_path}")
            return False
            
    except Exception as e:
        print(f"✗ Research-grade DEM download failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_research_grade_dem()
    if success:
        print("\n🎉 Research-grade DEM download SUCCESSFUL!")
        print("✅ 10m resolution maintained for research requirements")
        print("✅ Option A - Production pipeline ready!")
    else:
        print("\n✗ Research-grade test failed")
        sys.exit(1)
