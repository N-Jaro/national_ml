#!/usr/bin/env python3
"""
Test script for GEE bulk downloader
"""

import os
import sys
import time

print("=== Testing GEE Bulk Downloader ===")

# Step 1: Test basic imports
print("\n1. Testing imports...")
try:
    import ee
    print(f"✓ Earth Engine version: {ee.__version__}")
except ImportError as e:
    print(f"✗ Earth Engine import failed: {e}")
    exit(1)

# Step 2: Test local imports
print("\n2. Testing local imports...")
try:
    from local_config import LocalProcessingSettings
    print("✓ LocalProcessingSettings imported")
except ImportError as e:
    print(f"✗ LocalProcessingSettings import failed: {e}")
    exit(1)

try:
    sys.path.append('..')
    from config import Settings
    print("✓ Settings imported")
except ImportError as e:
    print(f"✗ Settings import failed: {e}")
    exit(1)

# Step 3: Test data manager import
print("\n3. Testing data manager import...")
try:
    from data_manager import LocalDataManager
    print("✓ LocalDataManager imported")
except ImportError as e:
    print(f"✗ LocalDataManager import failed: {e}")
    exit(1)

# Step 4: Initialize settings
print("\n4. Initializing settings...")
try:
    local_settings = LocalProcessingSettings()
    gee_settings = Settings()
    print("✓ Settings initialized")
    print(f"  - GEE Project: {gee_settings.GEE_PROJECT_ID}")
    print(f"  - HUC8 Collection: {gee_settings.HUC8_COL_NAME}")
    print(f"  - DEM Collection: {gee_settings.DEM_SOURCE_IMG_NAME}")
except Exception as e:
    print(f"✗ Settings initialization failed: {e}")
    exit(1)

# Step 5: Initialize GEE
print("\n5. Initializing GEE...")
try:
    ee.Initialize(project=gee_settings.GEE_PROJECT_ID)
    print(f"✓ GEE initialized with project: {gee_settings.GEE_PROJECT_ID}")
except Exception as e:
    print(f"✗ GEE initialization failed: {e}")
    exit(1)

# Step 6: Test GEE collections access
print("\n6. Testing GEE collections...")
try:
    print("  - Accessing HUC8 collection...")
    huc8_col = ee.FeatureCollection(gee_settings.HUC8_COL_NAME)
    print("  ✓ HUC8 collection loaded")
    
    print("  - Accessing DEM collection...")
    dem_col = ee.ImageCollection(gee_settings.DEM_SOURCE_IMG_NAME)
    print("  ✓ DEM collection loaded")
except Exception as e:
    print(f"✗ GEE collections access failed: {e}")
    exit(1)

# Step 7: Initialize data manager
print("\n7. Initializing data manager...")
try:
    data_manager = LocalDataManager(local_settings)
    print("✓ Data manager initialized")
except Exception as e:
    print(f"✗ Data manager initialization failed: {e}")
    exit(1)

print("\n🎉 All components initialized successfully!")
print("Ready to test GEE bulk downloader class...")

# Step 8: Test the actual downloader class
print("\n8. Testing GEEBulkDownloader class...")
try:
    from gee_bulk_downloader import GEEBulkDownloader
    downloader = GEEBulkDownloader(local_settings)
    print("✓ GEEBulkDownloader initialized successfully")
except Exception as e:
    print(f"✗ GEEBulkDownloader initialization failed: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n🚀 GEE Bulk Downloader is ready for testing!")
