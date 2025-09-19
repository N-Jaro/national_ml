#!/usr/bin/env python3
"""
Test the main landsat downloader with a single tile.
"""

import ee
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Settings
from local_config import LocalProcessingSettings
from data_manager import LocalDataManager

# Import the main downloader
from landsat_bulk_downloader import LandsatBulkDownloader

def test_main_downloader_single_tile():
    """Test the main downloader with a small area to verify it works with Drive export."""
    
    # Initialize EE
    settings = Settings()
    ee.Initialize(project=settings.GEE_PROJECT_ID)
    print(f"✅ Initialized GEE with project: {settings.GEE_PROJECT_ID}")
    
    # Initialize components
    config = LocalProcessingSettings()
    data_manager = LocalDataManager(config)
    downloader = LandsatBulkDownloader(config, data_manager)
    
    # Test with date range from config (can be overridden for testing)
    huc_id = "10020007"
    
    # Option 1: Use config dates directly
    # start_date = config.START_DATE
    # end_date = config.END_DATE
    
    # Option 2: Use focused test dates for faster processing (current approach)
    start_date = "2023-07-01"  # Shorter date range for testing
    end_date = "2023-07-31"    # Single month for faster processing
    
    # Option 3: Use seasonal subset of config dates
    # start_date = "2023-06-01"  # Summer season from config year
    # end_date = "2023-09-30"    # End of summer
    
    print(f"Config date range available: {config.START_DATE} to {config.END_DATE}")
    print(f"Using test date range: {start_date} to {end_date}")
    
    print(f"Testing main Landsat downloader for HUC {huc_id}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Drive manager available: {downloader.drive_manager is not None}")
    
    try:
        output_path = downloader.download_landsat_for_huc(
            huc_id, start_date, end_date, resolution=30.0
        )
        
        print(f"✅ Main downloader completed: {output_path}")
        
        # Verify the output file
        import rasterio
        if os.path.exists(output_path):
            with rasterio.open(output_path) as src:
                print(f"🎉 SUCCESS: Final merged file has {src.count} bands!")
                print(f"🔍 Band descriptions: {src.descriptions}")
                print(f"🔍 Data types: {src.dtypes}")
                print(f"🔍 Shape: {src.shape}")
                print(f"📁 File size: {os.path.getsize(output_path) / (1024*1024):.1f}MB")
        else:
            print(f"❌ Output file not found: {output_path}")
        
    except Exception as e:
        print(f"❌ Main downloader failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_main_downloader_single_tile()