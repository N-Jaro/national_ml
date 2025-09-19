#!/usr/bin/env python3
"""
Verify multi-band data flow from the complete pipeline.
Check that all data sources have the correct number of bands and properties.
"""

import rasterio
import numpy as np
from pathlib import Path
import os

def verify_pipeline_outputs():
    """Verify all pipeline outputs have correct band counts and properties."""
    
    base_path = Path("/u/nathanj/national_ml/data/local_rasters")
    huc_id = "10020007"
    
    # Expected files and their properties
    expected_files = {
        "DEM": {
            "path": base_path / "dem" / f"huc_{huc_id}_dem_10m.tif",
            "bands": 1,
            "resolution": 10.0,
            "description": "Digital Elevation Model"
        },
        "Landsat": {
            "path": base_path / "landsat" / f"huc_{huc_id}_landsat_30m_2023-07-01_2023-07-31.tif",
            "bands": 7,
            "resolution": 30.0,
            "description": "Landsat 9 Surface Reflectance + Thermal"
        },
        "SAR": {
            "path": base_path / "sentinel1" / f"huc_{huc_id}_sar_10m_2023-07-01_2023-07-31_median.tif",
            "bands": 6,
            "resolution": 10.0,
            "description": "Sentinel-1 SAR (VV/VH polarizations)"
        },
        "AlphaEarth": {
            "path": base_path / "alphaearth" / f"huc_{huc_id}_alphaearth_10m_2024_16bands.tif",
            "bands": 16,
            "resolution": 10.0,
            "description": "AlphaEarth Foundation Model Embeddings"
        },
        "Hydro Mask": {
            "path": base_path / "reference" / f"huc_{huc_id}_hydro_mask_10m.tif",
            "bands": 1,
            "resolution": 10.0,
            "description": "Hydrography Water Body Mask"
        },
        "Flow Direction": {
            "path": base_path / "reference" / f"huc_{huc_id}_flow_direction_10m.tif",
            "bands": 1,
            "resolution": 10.0,
            "description": "Hydrologic Flow Direction"
        }
    }
    
    print("🔍 PIPELINE OUTPUT VERIFICATION")
    print("=" * 60)
    
    total_bands = 0
    verified_files = 0
    
    for name, info in expected_files.items():
        file_path = info["path"]
        expected_bands = info["bands"]
        expected_res = info["resolution"]
        description = info["description"]
        
        print(f"\n📊 {name} ({description})")
        print(f"   File: {file_path.name}")
        
        if not file_path.exists():
            print(f"   ❌ File not found")
            continue
        
        try:
            with rasterio.open(file_path) as src:
                actual_bands = src.count
                height, width = src.shape
                crs = src.crs
                transform = src.transform
                actual_res = abs(transform[0])  # Pixel size
                
                # Calculate file size
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                
                print(f"   📏 Dimensions: {height:,} x {width:,} pixels")
                print(f"   🎨 Bands: {actual_bands} (expected: {expected_bands})")
                print(f"   📐 Resolution: {actual_res:.1f}m (expected: {expected_res:.1f}m)")
                print(f"   🗺️  CRS: {crs}")
                print(f"   💾 File size: {file_size_mb:.1f} MB")
                
                # Verify band count
                if actual_bands == expected_bands:
                    print(f"   ✅ Band count correct")
                    total_bands += actual_bands
                    verified_files += 1
                else:
                    print(f"   ❌ Band count mismatch")
                
                # Verify resolution (with tolerance)
                if abs(actual_res - expected_res) < 1.0:
                    print(f"   ✅ Resolution correct")
                else:
                    print(f"   ❌ Resolution mismatch")
                
                # Check for valid data
                sample_band = src.read(1)
                valid_pixels = (~np.isnan(sample_band)).sum()
                total_pixels = sample_band.size
                coverage = (valid_pixels / total_pixels) * 100
                
                print(f"   📈 Data coverage: {coverage:.1f}% valid pixels")
                
                if coverage > 80:
                    print(f"   ✅ Good data coverage")
                else:
                    print(f"   ⚠️ Low data coverage")
                
                # Quick stats for first band
                valid_data = sample_band[~np.isnan(sample_band)]
                if len(valid_data) > 0:
                    print(f"   📊 Data range: {valid_data.min():.3f} to {valid_data.max():.3f}")
                
        except Exception as e:
            print(f"   ❌ Error reading file: {e}")
    
    print(f"\n🎯 VERIFICATION SUMMARY")
    print(f"=" * 60)
    print(f"✅ Verified files: {verified_files}/{len(expected_files)}")
    print(f"📊 Total bands available: {total_bands}")
    print(f"💾 Total data volume: {sum(info['path'].stat().st_size for info in expected_files.values() if info['path'].exists()) / (1024**3):.1f} GB")
    
    if verified_files == len(expected_files):
        print(f"\n🎉 ALL PIPELINE OUTPUTS VERIFIED SUCCESSFULLY!")
        print(f"   Multi-band data flow is working correctly")
        print(f"   Ready for machine learning workflows")
        
        # Show what's available for ML
        print(f"\n🤖 MACHINE LEARNING READY DATASET:")
        print(f"   • Elevation data: 1 band (DEM)")
        print(f"   • Optical imagery: 7 bands (Landsat)")
        print(f"   • Radar data: 6 bands (SAR)")
        print(f"   • Foundation features: 16 bands (AlphaEarth)")
        print(f"   • Hydrologic features: 2 bands (Water mask + Flow)")
        print(f"   TOTAL: {total_bands} bands of multi-modal satellite data")
        
        return True
    else:
        print(f"\n⚠️ Some files missing or incorrect")
        return False

if __name__ == "__main__":
    success = verify_pipeline_outputs()
    exit(0 if success else 1)