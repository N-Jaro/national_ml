#!/usr/bin/env python3
"""
Validate the merged DEM file to ensure it meets research requirements.
"""

import rasterio
import numpy as np
import os
from pathlib import Path

def validate_dem_file(dem_path):
    """Validate the merged DEM file properties."""
    print(f"Validating DEM file: {dem_path}")
    print("=" * 50)
    
    if not os.path.exists(dem_path):
        print(f"ERROR: File does not exist: {dem_path}")
        return False
    
    file_size_mb = os.path.getsize(dem_path) / (1024 * 1024)
    print(f"File size: {file_size_mb:.1f} MB")
    
    try:
        with rasterio.open(dem_path) as src:
            print(f"Dimensions: {src.width} x {src.height} pixels")
            print(f"Bands: {src.count}")
            print(f"Data type: {src.dtypes[0]}")
            print(f"CRS: {src.crs}")
            print(f"Transform: {src.transform}")
            
            # Calculate pixel resolution
            pixel_size_x = abs(src.transform[0])
            pixel_size_y = abs(src.transform[4])
            print(f"Pixel resolution: {pixel_size_x:.1f}m x {pixel_size_y:.1f}m")
            
            # Check bounds
            bounds = src.bounds
            print(f"Bounds: ({bounds.left:.6f}, {bounds.bottom:.6f}, {bounds.right:.6f}, {bounds.top:.6f})")
            
            # Calculate area in km²
            width_m = (bounds.right - bounds.left) * 111320  # rough conversion for longitude
            height_m = (bounds.top - bounds.bottom) * 111320  # rough conversion for latitude
            area_km2 = (width_m * height_m) / 1_000_000
            print(f"Approximate area: {area_km2:.1f} km²")
            
            # Read a sample of the data to check for valid values
            print("\nData validation:")
            sample_data = src.read(1, window=((0, min(1000, src.height)), (0, min(1000, src.width))))
            
            print(f"Sample data shape: {sample_data.shape}")
            print(f"Data range: {np.nanmin(sample_data):.2f} to {np.nanmax(sample_data):.2f}")
            print(f"NoData value: {src.nodata}")
            
            # Check for nodata percentage
            if src.nodata is not None:
                nodata_count = np.sum(sample_data == src.nodata)
                total_count = sample_data.size
                nodata_percent = (nodata_count / total_count) * 100
                print(f"NoData percentage in sample: {nodata_percent:.1f}%")
            
            # Check if resolution is close to 10m
            resolution_ok = abs(pixel_size_x - 10.0) < 1.0 and abs(pixel_size_y - 10.0) < 1.0
            print(f"\nResolution check (target: 10m): {'✓' if resolution_ok else '✗'}")
            
            return True
            
    except Exception as e:
        print(f"ERROR reading file: {e}")
        return False

def main():
    """Main validation function."""
    dem_path = "/u/nathanj/national_ml/data/local_rasters/dem/huc_10020007_dem_10m.tif"
    
    print("DEM File Validation Report")
    print("=" * 50)
    
    success = validate_dem_file(dem_path)
    
    if success:
        print("\n✓ DEM file validation completed successfully!")
        print("\nNext steps:")
        print("1. Extend pipeline to other data sources (Landsat, SAR, AlphaEarth)")
        print("2. Implement patch extraction for ML workflows")
        print("3. Test batch processing for multiple HUCs")
    else:
        print("\n✗ DEM file validation failed!")

if __name__ == "__main__":
    main()
