#!/usr/bin/env python3
"""
Debug script to test Landsat band creation in Google Earth Engine.
"""

import ee
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Settings

def test_band_creation():
    """Test the band creation logic step by step."""
    
    # Initialize EE
    settings = Settings()
    ee.Initialize(project=settings.GEE_PROJECT_ID)
    print(f"✅ Initialized GEE with project: {settings.GEE_PROJECT_ID}")
    
    # Define test area (small area around HUC 10020007)
    test_bounds = ee.Geometry.Rectangle([-109.5, 41.0, -109.0, 41.5])
    
    # Get a single Landsat 9 image for testing
    print("🔍 Getting Landsat 9 collection...")
    collection = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
                 .filterDate('2023-06-01', '2023-09-30')
                 .filterBounds(test_bounds)
                 .filter(ee.Filter.lt('CLOUD_COVER', 20)))
    
    # Get first image
    first_image = ee.Image(collection.first())
    
    # Check original bands
    try:
        original_info = first_image.getInfo()
        original_bands = original_info['bands'] if 'bands' in original_info else []
        print(f"🔍 Original image has {len(original_bands)} bands:")
        for band in original_bands:
            print(f"  - {band['id']}")
    except Exception as e:
        print(f"Could not get original image info: {e}")
    
    # Test our band creation function
    def mask_clouds_landsat_test(image):
        """Test version of the band masking function."""
        
        print("🔍 Testing band creation...")
        
        # Select and scale the bands we want with explicit names
        scaled_bands = {
            'SR_B2': image.select('SR_B2').multiply(0.0000275).add(-0.2),
            'SR_B3': image.select('SR_B3').multiply(0.0000275).add(-0.2),
            'SR_B4': image.select('SR_B4').multiply(0.0000275).add(-0.2),
            'SR_B5': image.select('SR_B5').multiply(0.0000275).add(-0.2),
            'SR_B6': image.select('SR_B6').multiply(0.0000275).add(-0.2),
            'SR_B7': image.select('SR_B7').multiply(0.0000275).add(-0.2),
            'ST_B10': image.select('ST_B10').multiply(0.00341802).add(149.0)
        }
        
        # Test individual band selection
        for band_name in ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10']:
            try:
                test_band = scaled_bands[band_name]
                band_info = test_band.getInfo()
                print(f"  ✅ {band_name}: {band_info['bands'][0]['id'] if 'bands' in band_info else 'No band info'}")
            except Exception as e:
                print(f"  ❌ {band_name}: Error - {e}")
        
        # Create multi-band image using ee.Image.cat()
        result = ee.Image.cat([
            scaled_bands['SR_B2'].rename('SR_B2'),
            scaled_bands['SR_B3'].rename('SR_B3'),
            scaled_bands['SR_B4'].rename('SR_B4'),
            scaled_bands['SR_B5'].rename('SR_B5'),
            scaled_bands['SR_B6'].rename('SR_B6'),
            scaled_bands['SR_B7'].rename('SR_B7'),
            scaled_bands['ST_B10'].rename('ST_B10')
        ])
        
        return result
    
    # Apply the function
    processed_image = mask_clouds_landsat_test(first_image)
    
    # Check processed bands
    try:
        processed_info = processed_image.getInfo()
        processed_bands = processed_info['bands'] if 'bands' in processed_info else []
        print(f"🔍 Processed image has {len(processed_bands)} bands:")
        for band in processed_bands:
            print(f"  - {band['id']}")
    except Exception as e:
        print(f"Could not get processed image info: {e}")
    
    # Test temporal composite
    print("🔍 Testing temporal composite...")
    processed_collection = collection.map(mask_clouds_landsat_test)
    composite = processed_collection.median()
    
    try:
        composite_info = composite.getInfo()
        composite_bands = composite_info['bands'] if 'bands' in composite_info else []
        print(f"🔍 Composite has {len(composite_bands)} bands:")
        for band in composite_bands:
            print(f"  - {band['id']}")
    except Exception as e:
        print(f"Could not get composite info: {e}")
    
    # Test band selection
    print("🔍 Testing band selection...")
    band_names = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10']
    selected_composite = composite.select(band_names)
    
    try:
        selected_info = selected_composite.getInfo()
        selected_bands = selected_info['bands'] if 'bands' in selected_info else []
        print(f"🔍 Selected composite has {len(selected_bands)} bands:")
        for band in selected_bands:
            print(f"  - {band['id']}")
    except Exception as e:
        print(f"Could not get selected composite info: {e}")
    
    # Test a simple download URL to see what actually gets exported
    print("🔍 Testing download URL...")
    try:
        download_url = selected_composite.getDownloadURL({
            'scale': 30,
            'region': test_bounds.buffer(-1000),  # Smaller area
            'fileFormat': 'GeoTIFF',
            'formatOptions': {'cloudOptimized': True}
        })
        print(f"✅ Download URL created successfully")
        
        # Download and check the file
        import requests
        import zipfile
        import tempfile
        import rasterio
        
        print("🔍 Downloading test file...")
        response = requests.get(download_url, timeout=60)
        response.raise_for_status()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_zip = os.path.join(temp_dir, "test.zip")
            with open(temp_zip, 'wb') as f:
                f.write(response.content)
            
            with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
                tiff_files = [f for f in zip_ref.namelist() if f.endswith('.tif')]
                if tiff_files:
                    zip_ref.extract(tiff_files[0], temp_dir)
                    extracted_file = os.path.join(temp_dir, tiff_files[0])
                    
                    with rasterio.open(extracted_file) as src:
                        print(f"🔍 Downloaded file has {src.count} bands")
                        print(f"🔍 Band descriptions: {src.descriptions}")
                        print(f"🔍 Data types: {src.dtypes}")
                else:
                    print("❌ No .tif file found in download")
                    
    except Exception as e:
        print(f"❌ Download test failed: {e}")

if __name__ == "__main__":
    test_band_creation()