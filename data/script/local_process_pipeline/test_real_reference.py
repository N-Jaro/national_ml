#!/usr/bin/env python3
"""
Test the updated reference processor with real PySheds algorithms
"""

import sys
import os
from pathlib import Path

# Add the pipeline directory to the path
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

from local_config import LocalProcessingSettings
from data_manager import LocalDataManager
from reference_bulk_processor import ReferenceBulkProcessor

def test_real_reference_processing():
    """Test the updated reference processor with PySheds."""
    
    print("🧪 Testing Updated Reference Processor with Real PySheds Algorithms")
    print("=" * 60)
    
    try:
        # Initialize configuration
        config = LocalProcessingSettings()
        data_manager = LocalDataManager(config)
        processor = ReferenceBulkProcessor(config, data_manager)
        
        # Test HUC
        huc_id = "10020007"
        
        print(f"\n🔬 Testing real reference processing for HUC {huc_id}")
        
        # Test 1: Flow Direction with PySheds
        print(f"\n📊 Test 1: PySheds D8 Flow Direction")
        try:
            flow_dir_path = processor.create_flow_direction(huc_id)
            print(f"  ✅ Flow direction: {Path(flow_dir_path).name}")
            
            # Check file size and properties
            file_path = Path(flow_dir_path)
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"    📏 File size: {size_mb:.1f} MB")
                
                # Read metadata
                import rasterio
                with rasterio.open(flow_dir_path) as src:
                    print(f"    📐 Dimensions: {src.width} x {src.height}")
                    print(f"    🗺️  CRS: {src.crs}")
                    print(f"    📊 Data type: {src.dtypes[0]}")
                    
                    # Check data range
                    data = src.read(1)
                    print(f"    📈 Value range: {data.min()} - {data.max()}")
                    unique_vals = len(set(data.flatten()))
                    print(f"    🔢 Unique values: {unique_vals}")
            
        except Exception as e:
            print(f"  ❌ Flow direction failed: {e}")
        
        # Test 2: Hydrography Mask with NHD
        print(f"\n📊 Test 2: NHD Hydrography Mask")
        try:
            hydro_mask_path = processor.create_hydrography_mask(huc_id)
            print(f"  ✅ Hydro mask: {Path(hydro_mask_path).name}")
            
            # Check file properties
            file_path = Path(hydro_mask_path)
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"    📏 File size: {size_mb:.1f} MB")
                
                import rasterio
                with rasterio.open(hydro_mask_path) as src:
                    data = src.read(1)
                    water_pixels = np.sum(data == 1)
                    total_pixels = data.size
                    water_percent = (water_pixels / total_pixels) * 100
                    print(f"    💧 Water coverage: {water_percent:.2f}% ({water_pixels:,} pixels)")
                    
        except Exception as e:
            print(f"  ❌ Hydro mask failed: {e}")
        
        # Test 3: Patch Update (if patches exist)
        print(f"\n📊 Test 3: Patch Update with Reference Data")
        try:
            patch_results = processor.update_npz_files_with_reference_patches(huc_id)
            if patch_results["total"] > 0:
                success_rate = (patch_results["updated"] / patch_results["total"]) * 100
                print(f"  ✅ Patches updated: {patch_results['updated']}/{patch_results['total']} ({success_rate:.1f}%)")
            else:
                print(f"  ⚠️  No patch files found for {huc_id}")
                
        except Exception as e:
            print(f"  ❌ Patch update failed: {e}")
        
        print(f"\n✅ Real reference processing test completed!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_real_reference_processing()
