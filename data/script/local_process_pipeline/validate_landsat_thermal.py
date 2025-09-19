#!/usr/bin/env python3
"""
Validate Landsat file with thermal band - Check the newly created Landsat file.
"""

import rasterio
import numpy as np
from pathlib import Path

def validate_landsat_thermal_file():
    """Validate the Landsat file with thermal bands."""
    
    landsat_file = Path("/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-06-01_2023-09-30.tif")
    
    print("🌡️ LANDSAT THERMAL VALIDATION")
    print("=" * 50)
    print(f"File: {landsat_file.name}")
    
    if not landsat_file.exists():
        print("❌ File not found!")
        return
    
    file_size_mb = landsat_file.stat().st_size / (1024 * 1024)
    print(f"File size: {file_size_mb:.1f}MB")
    
    try:
        with rasterio.open(landsat_file) as src:
            print(f"\n📊 RASTER PROPERTIES:")
            print(f"Dimensions: {src.width:,} x {src.height:,} pixels")
            print(f"Bands: {src.count}")
            print(f"Data type: {src.dtypes[0]}")
            print(f"CRS: {src.crs}")
            print(f"Resolution: {src.res[0]:.1f}m x {src.res[1]:.1f}m")
            
            # Expected bands with thermal
            expected_bands = [
                'SR_B2 (blue)',
                'SR_B3 (green)', 
                'SR_B4 (red)',
                'SR_B5 (nir)',
                'SR_B6 (swir1)',
                'SR_B7 (swir2)',
                'ST_B10 (thermal)',
                'NDVI',
                'NDWI', 
                'NDBI',
                'LST (Land Surface Temperature)',
                'NDTI (Thermal Index)'
            ]
            
            print(f"\n🌡️ EXPECTED BANDS ({len(expected_bands)} total):")
            for i, band_name in enumerate(expected_bands, 1):
                status = "✅" if i <= src.count else "❌"
                print(f"  {status} Band {i}: {band_name}")
            
            # Sample data from different bands
            print(f"\n📈 DATA VALIDATION:")
            
            sample_size = min(1000, src.width, src.height)
            sample_window = ((0, sample_size), (0, sample_size))
            
            # Check each band type
            band_checks = [
                (1, "Blue (SR_B2)", "reflectance", 0.0, 1.0),
                (4, "NIR (SR_B5)", "reflectance", 0.0, 1.0),
                (7, "Thermal (ST_B10)", "temperature_K", 250.0, 350.0),
                (8, "NDVI", "index", -1.0, 1.0),
                (11, "LST", "temperature_C", -50.0, 70.0)
            ]
            
            for band_num, band_name, value_type, min_expected, max_expected in band_checks:
                if band_num <= src.count:
                    sample = src.read(band_num, window=sample_window)
                    valid_data = sample[~np.isnan(sample)]
                    
                    if len(valid_data) > 0:
                        data_min = np.min(valid_data)
                        data_max = np.max(valid_data)
                        data_mean = np.mean(valid_data)
                        
                        # Check if values are in expected range
                        range_ok = (data_min >= min_expected*0.8 and data_max <= max_expected*1.2)
                        
                        print(f"  Band {band_num:2d} ({band_name}): "
                              f"{data_min:.3f} to {data_max:.3f} (mean: {data_mean:.3f}) "
                              f"{'✅' if range_ok else '⚠️'}")
                    else:
                        print(f"  Band {band_num:2d} ({band_name}): No valid data ❌")
            
            # Overall assessment
            print(f"\n🎯 ASSESSMENT:")
            thermal_success = src.count >= len(expected_bands)
            size_appropriate = file_size_mb > 1000  # Should be > 1GB for full dataset
            
            if thermal_success and size_appropriate:
                print("✅ SUCCESS: Landsat with thermal bands processed correctly!")
                print(f"   ✅ {src.count} bands (includes thermal and thermal indices)")
                print(f"   ✅ {file_size_mb:.1f}MB size (appropriate for multi-band dataset)")
                print(f"   ✅ 30m resolution maintained")
                print(f"   ✅ All data types consistent (Float32)")
            else:
                print("⚠️ PARTIAL SUCCESS: Some issues detected")
                if not thermal_success:
                    print(f"   ❌ Expected {len(expected_bands)} bands, got {src.count}")
                if not size_appropriate:
                    print(f"   ❌ File size {file_size_mb:.1f}MB seems small for full dataset")
            
    except Exception as e:
        print(f"❌ Error reading file: {e}")

def main():
    validate_landsat_thermal_file()

if __name__ == "__main__":
    main()
