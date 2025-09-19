#!/usr/bin/env python3
"""
Test the Reference Bulk Processor
"""

import sys
import os
from pathlib import Path

# Add the pipeline directory to the path
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def test_reference_processor():
    """Test the reference processor with the current implementation."""
    
    print("🧪 Testing Reference Bulk Processor")
    print("=" * 50)
    
    try:
        # Import and initialize
        from local_config import LocalProcessingSettings
        from data_manager import LocalDataManager
        from reference_bulk_processor import ReferenceBulkProcessor
        
        config = LocalProcessingSettings()
        data_manager = LocalDataManager(config)
        processor = ReferenceBulkProcessor(config, data_manager)
        
        print("✅ Successfully initialized reference processor")
        print(f"📁 Output directory: {processor.reference_output_dir}")
        
        # Test HUC
        huc_id = "10020007"
        print(f"\n🎯 Testing with HUC: {huc_id}")
        
        # Test 1: Flow Direction (should work with existing DEM)
        print(f"\n📊 Test 1: Flow Direction Processing")
        try:
            flow_dir_path = processor.create_flow_direction(huc_id)
            print(f"  ✅ Flow direction created: {Path(flow_dir_path).name}")
            
            # Check file properties
            file_path = Path(flow_dir_path)
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"    📏 File size: {size_mb:.1f} MB")
                
                # Validate D8 flow direction
                import rasterio
                import numpy as np
                with rasterio.open(flow_dir_path) as src:
                    data = src.read(1)
                    unique_vals = sorted(np.unique(data))
                    d8_codes = [1, 2, 4, 8, 16, 32, 64, 128]
                    has_all_d8 = all(code in unique_vals for code in d8_codes)
                    print(f"    📐 Dimensions: {src.width} x {src.height}")
                    print(f"    🔢 Unique values: {len(unique_vals)} ({unique_vals})")
                    print(f"    ✅ D8 encoding: {'Valid' if has_all_d8 else 'Invalid'}")
            
        except Exception as e:
            print(f"  ❌ Flow direction failed: {e}")
        
        # Test 2: Hydrography Mask
        print(f"\n📊 Test 2: Hydrography Mask Processing")
        try:
            hydro_mask_path = processor.create_hydrography_mask(huc_id)
            print(f"  ✅ Hydrography mask created: {Path(hydro_mask_path).name}")
            
            # Check file properties
            file_path = Path(hydro_mask_path)
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"    📏 File size: {size_mb:.1f} MB")
                
                # Validate hydro mask
                import rasterio
                import numpy as np
                with rasterio.open(hydro_mask_path) as src:
                    data = src.read(1)
                    water_pixels = np.sum(data == 1)
                    total_pixels = data.size
                    water_percent = (water_pixels / total_pixels) * 100
                    print(f"    📐 Dimensions: {src.width} x {src.height}")
                    print(f"    💧 Water coverage: {water_percent:.2f}% ({water_pixels:,} pixels)")
            
        except Exception as e:
            print(f"  ❌ Hydrography mask failed: {e}")
        
        # Test 3: Process All References
        print(f"\n📊 Test 3: Process All References")
        try:
            results = processor.process_all_references_for_huc(huc_id)
            print(f"  📊 Processing results:")
            
            for ref_type, path in results.items():
                if path and Path(path).exists():
                    size_mb = Path(path).stat().st_size / (1024 * 1024)
                    print(f"    ✅ {ref_type}: {Path(path).name} ({size_mb:.1f}MB)")
                else:
                    print(f"    ❌ {ref_type}: Failed or missing")
            
        except Exception as e:
            print(f"  ❌ Process all references failed: {e}")
        
        # Summary
        print(f"\n📋 Reference Files Summary:")
        reference_dir = config.local_raster_base / "reference"
        if reference_dir.exists():
            ref_files = list(reference_dir.glob(f"*{huc_id}*.tif"))
            ref_files.sort()
            
            total_size_mb = sum(f.stat().st_size for f in ref_files) / (1024 * 1024)
            print(f"  📁 Files created: {len(ref_files)}")
            print(f"  📏 Total size: {total_size_mb:.1f} MB")
            
            for ref_file in ref_files:
                size_mb = ref_file.stat().st_size / (1024 * 1024)
                print(f"    - {ref_file.name} ({size_mb:.1f}MB)")
        
        print(f"\n✅ Reference processor test completed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_reference_processor()