#!/usr/bin/env python3
"""Quick test of AlphaEarth fixes."""

import sys
import os
import logging

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
logging.basicConfig(level=logging.INFO)

try:
    import ee
    from config import Settings
    from local_config import LocalProcessingSettings
    from data_manager import LocalDataManager
    from alphaearth_bulk_downloader import AlphaEarthBulkDownloader
    
    print("✓ All imports successful")
    
    # Initialize GEE
    gee_settings = Settings()
    ee.Initialize(project=gee_settings.GEE_PROJECT_ID)
    print(f"✓ GEE initialized with project: {gee_settings.GEE_PROJECT_ID}")
    
    # Initialize components
    config = LocalProcessingSettings()
    data_manager = LocalDataManager(config)
    downloader = AlphaEarthBulkDownloader(config, data_manager)
    print("✓ Components initialized")
    
    # Test subset download (16 bands)
    print("\n🔬 Testing AlphaEarth subset download (16 bands)...")
    output_path = downloader.download_alphaearth_subset(
        huc_id="10020007",
        band_count=16,
        year=2024
    )
    print(f"✓ Download completed: {output_path}")
    
    # Check file properties
    import rasterio
    with rasterio.open(output_path) as src:
        print(f"  File size: {os.path.getsize(output_path) / (1024*1024):.1f} MB")
        print(f"  Dimensions: {src.width} x {src.height}")
        print(f"  Bands: {src.count}")
        print(f"  Data type: {src.dtypes[0]}")
        print(f"  CRS: {src.crs}")
        
        # Check band descriptions  
        for i in range(min(5, src.count)):
            try:
                desc = src.descriptions[i] if src.descriptions else f"Band_{i+1}"
                print(f"  Band {i+1}: {desc}")
            except Exception:
                print(f"  Band {i+1}: No description available")
    
    print("\n✅ AlphaEarth test completed successfully!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
