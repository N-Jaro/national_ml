#!/usr/bin/env python3
"""
Quick validation of the generated reference data
"""

import sys
import numpy as np
import rasterio
from pathlib import Path

sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def quick_validate_outputs():
    """Quick validation of pipeline outputs."""
    
    print("🔍 Quick Validation of Pipeline Outputs")
    print("=" * 45)
    
    huc_id = "10020007"
    base_path = Path("/u/nathanj/national_ml/data/local_rasters/reference")
    
    # Files to check
    files_to_check = {
        'Flow Direction': base_path / f"huc_{huc_id}_flow_direction_10m.tif",
        'Hydro Mask': base_path / f"huc_{huc_id}_hydro_mask_10m.tif"
    }
    
    for name, path in files_to_check.items():
        print(f"\n📊 {name}")
        print("-" * 30)
        
        if not path.exists():
            print(f"❌ File not found: {path}")
            continue
            
        try:
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"📏 Size: {size_mb:.1f} MB")
            
            with rasterio.open(path) as src:
                print(f"📐 Dimensions: {src.width} × {src.height}")
                print(f"🔢 Bands: {src.count}")
                print(f"📊 CRS: {src.crs}")
                print(f"🎯 Resolution: {abs(src.transform[0]):.1f}m")
                
                # Read a sample of the data for analysis
                data = src.read(1)
                print(f"🗃️  Data type: {data.dtype}")
                
                # Get basic statistics
                finite_data = data[np.isfinite(data)]
                if len(finite_data) > 0:
                    print(f"📈 Data range: {finite_data.min()} to {finite_data.max()}")
                    print(f"📊 Mean: {finite_data.mean():.2f}")
                    
                    # Get unique values (sample if too many)
                    unique_vals = np.unique(finite_data)
                    if len(unique_vals) <= 20:
                        print(f"🔢 Unique values: {sorted(unique_vals)}")
                    else:
                        print(f"🔢 Unique values: {len(unique_vals)} total")
                        print(f"   First 10: {sorted(unique_vals)[:10]}")
                        print(f"   Last 10: {sorted(unique_vals)[-10:]}")
                    
                    # Specific validation
                    if 'flow' in name.lower():
                        # Check for D8 values
                        d8_codes = [1, 2, 4, 8, 16, 32, 64, 128]
                        present_d8 = [code for code in d8_codes if code in unique_vals]
                        print(f"🌊 D8 codes present: {present_d8}")
                        
                        # Check for problematic values
                        invalid_vals = [val for val in unique_vals if val not in d8_codes + [-2, -1, 0]]
                        if invalid_vals:
                            print(f"⚠️  Invalid values: {invalid_vals[:10]}")
                        
                        # Check for nodata/special values
                        nodata_count = np.sum(data == -1)
                        pit_count = np.sum(data == -2)
                        if nodata_count > 0:
                            print(f"🕳️  No-data pixels: {nodata_count:,} ({(nodata_count/data.size)*100:.2f}%)")
                        if pit_count > 0:
                            print(f"🕳️  Pit pixels: {pit_count:,} ({(pit_count/data.size)*100:.2f}%)")
                    
                    elif 'hydro' in name.lower() or 'mask' in name.lower():
                        # Check for binary mask
                        water_pixels = np.sum(data == 1)
                        land_pixels = np.sum(data == 0)
                        total_pixels = data.size
                        
                        print(f"💧 Water pixels: {water_pixels:,} ({(water_pixels/total_pixels)*100:.2f}%)")
                        print(f"🏞️  Land pixels: {land_pixels:,} ({(land_pixels/total_pixels)*100:.2f}%)")
                        
                        # Check if truly binary
                        non_binary = len([val for val in unique_vals if val not in [0, 1]])
                        if non_binary == 0:
                            print(f"✅ Valid binary mask")
                        else:
                            print(f"⚠️  Non-binary values detected")
                
                else:
                    print(f"⚠️  No finite data found")
                    
        except Exception as e:
            print(f"❌ Error reading {name}: {e}")
    
    print(f"\n✅ Validation complete!")

if __name__ == "__main__":
    quick_validate_outputs()