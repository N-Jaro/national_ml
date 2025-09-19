#!/usr/bin/env python3
"""
Enhanced validation with proper area calculation and data sampling.
"""

import rasterio
import numpy as np
import os
from pathlib import Path
from pyproj import Proj, transform as pyproj_transform

def validate_dem_file_enhanced(dem_path):
    """Enhanced validation of the merged DEM file."""
    print(f"Enhanced validation of: {dem_path}")
    print("=" * 60)
    
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
            print(f"NoData value: {src.nodata}")
            
            # Calculate pixel resolution
            pixel_size_x = abs(src.transform[0])
            pixel_size_y = abs(src.transform[4])
            print(f"Pixel resolution: {pixel_size_x:.1f}m x {pixel_size_y:.1f}m")
            
            # Get bounds in the native CRS (EPSG:5070)
            bounds = src.bounds
            print(f"Bounds (EPSG:5070): ({bounds.left:.1f}, {bounds.bottom:.1f}, {bounds.right:.1f}, {bounds.top:.1f})")
            
            # Calculate area correctly (already in meters for EPSG:5070)
            width_m = bounds.right - bounds.left
            height_m = bounds.top - bounds.bottom
            area_km2 = (width_m * height_m) / 1_000_000
            print(f"Area: {area_km2:.1f} km²")
            
            # Read multiple samples to check data distribution
            print("\nData validation:")
            
            # Sample from different regions
            samples = []
            sample_locations = [
                ("Top-left", (0, 0)),
                ("Center", (src.height//2, src.width//2)),
                ("Bottom-right", (src.height-1000, src.width-1000))
            ]
            
            for location_name, (row_start, col_start) in sample_locations:
                if row_start >= 0 and col_start >= 0:
                    window = ((row_start, min(row_start + 1000, src.height)), 
                             (col_start, min(col_start + 1000, src.width)))
                    sample = src.read(1, window=window)
                    samples.append((location_name, sample))
                    
                    print(f"\n{location_name} sample:")
                    print(f"  Shape: {sample.shape}")
                    print(f"  Range: {np.nanmin(sample):.2f} to {np.nanmax(sample):.2f}")
                    print(f"  Mean: {np.nanmean(sample):.2f}")
                    print(f"  Std: {np.nanstd(sample):.2f}")
                    
                    # Check for nodata
                    if src.nodata is not None:
                        nodata_count = np.sum(sample == src.nodata)
                        nodata_percent = (nodata_count / sample.size) * 100
                        print(f"  NoData: {nodata_percent:.1f}%")
                    
                    # Check for zeros (which might indicate an issue)
                    zero_count = np.sum(sample == 0.0)
                    zero_percent = (zero_count / sample.size) * 100
                    print(f"  Zeros: {zero_percent:.1f}%")
            
            # Overall statistics on a larger sample
            print(f"\nOverall validation:")
            
            # Read the full first row and column to check for patterns
            first_row = src.read(1, window=((0, 1), (0, src.width)))
            first_col = src.read(1, window=((0, src.height), (0, 1)))
            
            print(f"First row stats: min={np.nanmin(first_row):.2f}, max={np.nanmax(first_row):.2f}, mean={np.nanmean(first_row):.2f}")
            print(f"First col stats: min={np.nanmin(first_col):.2f}, max={np.nanmax(first_col):.2f}, mean={np.nanmean(first_col):.2f}")
            
            # Check if we have realistic elevation values (DEM should not be all zeros)
            has_valid_data = np.nanmax(first_row) > 0 or np.nanmax(first_col) > 0
            
            print(f"\nValidation results:")
            print(f"Resolution (10m target): {'✓' if abs(pixel_size_x - 10.0) < 1.0 else '✗'}")
            print(f"Valid elevation data: {'✓' if has_valid_data else '✗ (all zeros detected)'}")
            print(f"File integrity: ✓")
            
            return has_valid_data
            
    except Exception as e:
        print(f"ERROR reading file: {e}")
        return False

def main():
    """Main validation function."""
    dem_path = "/u/nathanj/national_ml/data/local_rasters/dem/huc_10020007_dem_10m.tif"
    
    print("Enhanced DEM File Validation")
    print("=" * 60)
    
    success = validate_dem_file_enhanced(dem_path)
    
    if success:
        print("\n✓ DEM file appears valid with real elevation data!")
    else:
        print("\n⚠ DEM file may have data issues (all zeros detected)")
        print("This could indicate:")
        print("- Water-only region")
        print("- Download issue")
        print("- Processing error")
        print("\nRecommendation: Check a few tile files to debug")

if __name__ == "__main__":
    main()
