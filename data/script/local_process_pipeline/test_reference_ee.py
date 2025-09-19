#!/usr/bin/env python3
"""
Test the Reference Bulk Processor with Earth Engine
"""

import sys
import os
from pathlib import Path

# Add the pipeline directory to the path
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def test_reference_processor_with_ee():
    """Test the reference processor with Earth Engine properly initialized."""
    
    print("🧪 Testing Reference Bulk Processor with Earth Engine")
    print("=" * 60)
    
    try:
        # Initialize Earth Engine first
        import ee
        ee.Initialize(project='nathanj-national-ml')
        print("✅ Earth Engine initialized")
        
        # Import and initialize processor
        from local_config import LocalProcessingSettings
        from data_manager import LocalDataManager
        from reference_bulk_processor import ReferenceBulkProcessor
        
        config = LocalProcessingSettings()
        data_manager = LocalDataManager(config)
        processor = ReferenceBulkProcessor(config, data_manager)
        
        print("✅ Reference processor initialized")
        print(f"📁 Output directory: {processor.reference_output_dir}")
        print(f"🗂️  NHD GDB: {config.nhd_gdb_path}")
        print(f"📂 NHD Shapefiles: {config.nhd_shapefiles_dir}")
        
        # Test HUC
        huc_id = "10020007"
        print(f"\n🎯 Testing with HUC: {huc_id}")
        
        # Test 1: Flow Direction (should still work)
        print(f"\n📊 Test 1: Flow Direction Processing")
        try:
            flow_dir_path = processor.create_flow_direction(huc_id)
            print(f"  ✅ Flow direction: {Path(flow_dir_path).name}")
            
            # Quick validation
            file_path = Path(flow_dir_path)
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"    📏 Size: {size_mb:.1f} MB")
            
        except Exception as e:
            print(f"  ❌ Flow direction failed: {e}")
        
        # Test 2: Hydrography Mask (should work now)
        print(f"\n📊 Test 2: Hydrography Mask Processing")
        try:
            hydro_mask_path = processor.create_hydrography_mask(huc_id)
            print(f"  ✅ Hydrography mask: {Path(hydro_mask_path).name}")
            
            # Validate hydro mask
            file_path = Path(hydro_mask_path)
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"    📏 Size: {size_mb:.1f} MB")
                
                import rasterio
                import numpy as np
                with rasterio.open(hydro_mask_path) as src:
                    data = src.read(1)
                    water_pixels = np.sum(data == 1)
                    total_pixels = data.size
                    water_percent = (water_pixels / total_pixels) * 100
                    print(f"    📐 Dimensions: {src.width} x {src.height}")
                    print(f"    💧 Water coverage: {water_percent:.2f}%")
            
        except Exception as e:
            print(f"  ❌ Hydrography mask failed: {e}")
            import traceback
            traceback.print_exc()
        
        # Test 3: Process All References Together
        print(f"\n📊 Test 3: Process All References")
        try:
            results = processor.process_all_references_for_huc(huc_id)
            print(f"  📊 Processing results:")
            
            success_count = 0
            for ref_type, path in results.items():
                if path and Path(path).exists():
                    size_mb = Path(path).stat().st_size / (1024 * 1024)
                    print(f"    ✅ {ref_type}: {Path(path).name} ({size_mb:.1f}MB)")
                    success_count += 1
                else:
                    print(f"    ❌ {ref_type}: Failed or missing")
            
            print(f"  📊 Success rate: {success_count}/{len(results)} ({success_count/len(results)*100:.0f}%)")
            
        except Exception as e:
            print(f"  ❌ Process all references failed: {e}")
        
        # Final summary
        print(f"\n📋 Final Reference Files Summary:")
        reference_dir = config.local_raster_base / "reference"
        if reference_dir.exists():
            ref_files = list(reference_dir.glob(f"*{huc_id}*.tif"))
            ref_files.sort()
            
            total_size_mb = sum(f.stat().st_size for f in ref_files) / (1024 * 1024)
            print(f"  📁 Files created: {len(ref_files)}")
            print(f"  📏 Total size: {total_size_mb:.1f} MB")
            
            for ref_file in ref_files:
                size_mb = ref_file.stat().st_size / (1024 * 1024)
                ref_type = 'flow_direction' if 'flow_direction' in ref_file.name else 'hydro_mask' if 'hydro_mask' in ref_file.name else 'other'
                print(f"    - {ref_file.name} ({size_mb:.1f}MB) [{ref_type}]")
        
        print(f"\n✅ Reference processor test with Earth Engine completed!")
        print(f"🎯 Both reference types should now be working:")
        print(f"   💧 Hydrography mask: NHD vector data burned into raster")
        print(f"   🌊 Flow direction: PySheds D8 algorithm with real DEM")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_reference_processor_with_ee()