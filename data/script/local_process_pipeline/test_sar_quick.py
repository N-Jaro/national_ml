#!/usr/bin/env python3
"""Quick SAR test with improved functionality."""

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
    from sar_bulk_downloader import SARBulkDownloader
    
    print("✓ All imports successful")
    
    # Initialize GEE
    gee_settings = Settings()
    ee.Initialize(project=gee_settings.GEE_PROJECT_ID)
    print(f"✓ GEE initialized with project: {gee_settings.GEE_PROJECT_ID}")
    
    # Initialize components
    config = LocalProcessingSettings()
    data_manager = LocalDataManager(config)
    downloader = SARBulkDownloader(config, data_manager)
    print("✓ Components initialized")
    
    # Test SAR download
    print("\n🛰️ Testing SAR download...")
    huc_id = "10020007"
    start_date = "2023-06-01"
    end_date = "2023-09-30"
    
    print(f"HUC: {huc_id}")
    print(f"Date range: {start_date} to {end_date}")
    
    output_path = downloader.download_sar_for_huc(
        huc_id, start_date, end_date, resolution=10.0
    )
    print(f"✓ SAR download completed: {output_path}")
    
    # Check file properties
    import rasterio
    import numpy as np
    
    with rasterio.open(output_path) as src:
        print(f"\n📊 SAR File Analysis:")
        print(f"  File size: {os.path.getsize(output_path) / (1024*1024):.1f} MB")
        print(f"  Dimensions: {src.width:,} x {src.height:,} pixels")
        print(f"  Total pixels: {src.width * src.height:,}")
        print(f"  Bands: {src.count}")
        print(f"  Data type: {src.dtypes[0]}")
        print(f"  CRS: {src.crs}")
        print(f"  Compression: {src.compression}")
        print(f"  Tiled: {src.is_tiled}")
        
        # Check band descriptions
        print(f"  Band descriptions:")
        for i in range(src.count):
            desc = src.descriptions[i] if src.descriptions and len(src.descriptions) > i else f'Band_{i+1}'
            print(f"    Band {i+1}: {desc}")
        
        # Sample data analysis
        print(f"\n  Sample data ranges:")
        for i in range(min(src.count, 6)):
            sample = src.read(i+1, window=((0, 100), (0, 100)))
            desc = src.descriptions[i] if src.descriptions and len(src.descriptions) > i else f'Band_{i+1}'
            print(f"    {desc}: {np.min(sample):.3f} to {np.max(sample):.3f} (mean: {np.mean(sample):.3f})")
    
    print("\n✅ SAR test completed successfully!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
