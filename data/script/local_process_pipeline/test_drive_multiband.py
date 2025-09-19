#!/usr/bin/env python3
"""
Test script to verify Drive export preserves all 7 bands.
"""

import ee
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Settings
from drive_manager import GoogleDriveManager

def test_single_tile_drive_export():
    """Test Drive export with a single small tile."""
    
    # Initialize EE
    settings = Settings()
    ee.Initialize(project=settings.GEE_PROJECT_ID)
    print(f"✅ Initialized GEE with project: {settings.GEE_PROJECT_ID}")
    
    # Initialize Drive manager
    try:
        # Change to script directory where credentials are located
        original_cwd = os.getcwd()
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        os.chdir(script_dir)
        
        drive_manager = GoogleDriveManager(settings)
        os.chdir(original_cwd)  # Restore original directory
        print("✅ Drive manager initialized successfully")
    except Exception as e:
        if 'original_cwd' in locals():
            os.chdir(original_cwd)
        print(f"❌ Drive manager failed: {e}")
        return
    
    # Define small test area
    test_bounds = ee.Geometry.Rectangle([-109.3, 41.2, -109.2, 41.3])  # Very small area
    
    # Create Landsat composite using the proven approach
    collection = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
                 .filterDate('2023-06-01', '2023-09-30')
                 .filterBounds(test_bounds)
                 .filter(ee.Filter.lt('CLOUD_COVER', 20)))
    
    # Separate optical and thermal composites
    optical_median = collection.select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']).median()
    thermal_median = collection.select('ST_B10').median()
    
    # Apply scaling
    optical_scaled = optical_median.multiply(0.0000275).add(-0.2)
    thermal_scaled = thermal_median.multiply(0.00341802).add(149.0)
    
    # Combine them
    composite = optical_scaled.addBands(thermal_scaled)
    composite = composite.select(
        ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10'],
        ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10']
    )
    
    # Check composite bands
    try:
        composite_info = composite.getInfo()
        band_names = composite_info['bands'] if 'bands' in composite_info else []
        print(f"🔍 Composite has {len(band_names)} bands: {[b['id'] for b in band_names]}")
    except Exception as e:
        print(f"Could not get composite info: {e}")
    
    # Export to Drive
    print("🔍 Starting Drive export...")
    task = ee.batch.Export.image.toDrive(
        image=composite,
        description='test_multiband_landsat',
        scale=30,
        region=test_bounds,
        fileFormat='GeoTIFF',
        formatOptions={'cloudOptimized': True},
        maxPixels=1e13
    )
    task.start()
    print(f"Started Drive export task: {task.id}")
    
    # Wait for completion
    import time
    max_wait = 300  # 5 minutes
    start_time = time.time()
    
    while task.status()['state'] in ['READY', 'RUNNING']:
        if time.time() - start_time > max_wait:
            print(f"❌ Export timeout after {max_wait}s")
            return
        
        time.sleep(10)
        status = task.status()
        print(f"Export status: {status['state']}")
        
        if 'progress' in status:
            print(f"Progress: {status['progress']}%")
    
    final_status = task.status()
    if final_status['state'] == 'COMPLETED':
        print("✅ Drive export completed!")
        
        # Download and check
        try:
            # Get the folder ID for the GEE exports folder
            folder_id = drive_manager.get_gdrive_folder_id('GEE_HUC_Exports_Python_Full_alphaE')
            if not folder_id:
                print("❌ Could not find GEE export folder")
                return
                
            # Download the folder contents to /tmp
            import tempfile
            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"🔍 Downloading to: {temp_dir}")
                drive_manager.download_folder_recursively(folder_id, temp_dir)
                
                # Find the test file
                import glob
                test_files = glob.glob(os.path.join(temp_dir, "**/test_multiband_landsat*.tif"), recursive=True)
                
                if test_files:
                    test_file = test_files[0]
                    print(f"🔍 Found test file: {test_file}")
                    
                    import rasterio
                    with rasterio.open(test_file) as src:
                        print(f"🎉 SUCCESS: Downloaded file has {src.count} bands!")
                        print(f"🔍 Band descriptions: {src.descriptions}")
                        print(f"🔍 Data types: {src.dtypes}")
                        print(f"🔍 Shape: {src.shape}")
                        
                        # Create a permanent copy for verification
                        import shutil
                        permanent_copy = "/tmp/test_multiband_result.tif"
                        shutil.copy(test_file, permanent_copy)
                        print(f"📁 Saved copy to: {permanent_copy}")
                else:
                    print("❌ Could not find test file in downloads")
                    # List what was downloaded
                    all_files = glob.glob(os.path.join(temp_dir, "**/*"), recursive=True)
                    print(f"Downloaded files: {all_files}")
                
        except Exception as e:
            print(f"❌ Download failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        error_msg = final_status.get('error_message', 'Unknown error')
        print(f"❌ Drive export failed: {final_status['state']} - {error_msg}")

if __name__ == "__main__":
    test_single_tile_drive_export()