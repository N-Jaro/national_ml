#!/usr/bin/env python3
"""Test SAR data availability and processing."""

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
    
    # Test SAR data availability
    print("\n🛰️ Testing SAR data availability...")
    
    # Get HUC geometry for bounds
    huc_id = "10020007"
    huc_geom = data_manager.get_huc_geometry(huc_id)
    huc_bounds = huc_geom.bounds
    ee_geom = ee.Geometry.Rectangle([
        huc_bounds[0], huc_bounds[1],  # min_lon, min_lat
        huc_bounds[2], huc_bounds[3]   # max_lon, max_lat
    ])
    
    # Check different time periods for SAR data
    test_periods = [
        ("2024-06-01", "2024-09-30", "Summer 2024"),
        ("2023-06-01", "2023-09-30", "Summer 2023"),
        ("2022-06-01", "2022-09-30", "Summer 2022")
    ]
    
    for start_date, end_date, label in test_periods:
        print(f"\n📅 Checking {label} ({start_date} to {end_date}):")
        
        try:
            collection = downloader.get_sentinel1_collection(start_date, end_date, ee_geom)
            count = collection.size().getInfo()
            print(f"  Found {count} Sentinel-1 images")
            
            if count > 0:
                # Get info about first image
                first_image = collection.first()
                bands = first_image.bandNames().getInfo()
                print(f"  Available bands: {bands}")
                
                # Get image date
                date = ee.Date(first_image.get('system:time_start')).format('YYYY-MM-dd').getInfo()
                print(f"  First image date: {date}")
                
                # Found data, let's test download
                print(f"\n🔬 Testing SAR download for {label}...")
                output_path = downloader.download_sar_for_huc(
                    huc_id, start_date, end_date, resolution=10.0
                )
                print(f"✓ SAR download completed: {output_path}")
                
                # Check file properties
                import rasterio
                with rasterio.open(output_path) as src:
                    print(f"  File size: {os.path.getsize(output_path) / (1024*1024):.1f} MB")
                    print(f"  Dimensions: {src.width} x {src.height}")
                    print(f"  Bands: {src.count}")
                    print(f"  Band descriptions: {[src.descriptions[i] if src.descriptions else f'Band_{i+1}' for i in range(min(4, src.count))]}")
                
                break  # Found working data, exit loop
                
        except Exception as e:
            print(f"  ✗ Error for {label}: {e}")
    
    print("\n✅ SAR test completed!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
