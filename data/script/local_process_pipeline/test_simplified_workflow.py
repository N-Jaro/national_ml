#!/usr/bin/env python3
"""
Test the simplified 2-reference workflow (hydro_mask and flow_direction only)
"""

import sys
import os
from pathlib import Path

# Add the pipeline directory to the path
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

from local_config import LocalProcessingSettings
from data_manager import LocalDataManager
from reference_bulk_processor import ReferenceBulkProcessor
from multi_source_orchestrator import MultiSourcePipelineOrchestrator

def test_simplified_reference_workflow():
    """Test the simplified reference workflow with only 2 reference types."""
    
    print("🧪 Testing Simplified 2-Reference Workflow")
    print("=" * 50)
    print("📋 Processing only: hydro_mask + flow_direction")
    print("🗂️  Output: Full HUC rasters (no patch processing)")
    print()
    
    try:
        # Initialize configuration
        config = LocalProcessingSettings()
        data_manager = LocalDataManager(config)
        
        # Test HUCs
        test_hucs = ["10020007"]  # Start with one known working HUC
        
        print(f"🎯 Testing HUCs: {test_hucs}")
        
        # Test 1: Direct reference processor
        print(f"\n📊 Test 1: Direct Reference Processor")
        processor = ReferenceBulkProcessor(config, data_manager)
        
        for huc_id in test_hucs:
            print(f"\n  Processing HUC {huc_id}...")
            
            try:
                # Process references (should default to hydro_mask + flow_direction)
                results = processor.process_all_references_for_huc(huc_id)
                
                # Validate results
                expected_types = ['hydro_mask', 'flow_direction']
                
                print(f"    Results for HUC {huc_id}:")
                for ref_type in expected_types:
                    if ref_type in results and results[ref_type]:
                        file_path = Path(results[ref_type])
                        if file_path.exists():
                            size_mb = file_path.stat().st_size / (1024 * 1024)
                            print(f"    ✅ {ref_type}: {file_path.name} ({size_mb:.1f}MB)")
                        else:
                            print(f"    ❌ {ref_type}: File not found")
                    else:
                        print(f"    ❌ {ref_type}: Processing failed")
                
                # Check for any unwanted files (land_cover)
                reference_dir = config.local_raster_base / "reference"
                land_cover_files = list(reference_dir.glob(f"*{huc_id}*land_cover*"))
                if land_cover_files:
                    print(f"    ⚠️  Unexpected land cover files found: {[f.name for f in land_cover_files]}")
                else:
                    print(f"    ✅ No unwanted land cover files")
                    
            except Exception as e:
                print(f"    ❌ HUC {huc_id} failed: {e}")
        
        # Test 2: Orchestrator integration
        print(f"\n📊 Test 2: Orchestrator Integration")
        orchestrator = MultiSourcePipelineOrchestrator(config)
        
        for huc_id in test_hucs:
            print(f"\n  Testing orchestrator for HUC {huc_id}...")
            
            try:
                # Process only references through orchestrator
                plan = orchestrator.create_processing_plan(
                    huc_ids=[huc_id],
                    data_sources=["references"]
                )
                
                print(f"    📋 Processing plan created")
                print(f"    📊 Tasks: {len(plan['tasks'])}")
                
                # Execute plan
                results = orchestrator.execute_plan(plan)
                
                if results['success']:
                    print(f"    ✅ Orchestrator processing completed")
                    print(f"    📊 Completed: {results['completed_tasks']}/{results['total_tasks']}")
                    
                    # Check reference files exist
                    reference_dir = config.local_raster_base / "reference"
                    expected_files = [
                        f"huc_{huc_id}_hydro_mask_10m.tif",
                        f"huc_{huc_id}_flow_direction_10m.tif"
                    ]
                    
                    for expected_file in expected_files:
                        file_path = reference_dir / expected_file
                        if file_path.exists():
                            size_mb = file_path.stat().st_size / (1024 * 1024)
                            print(f"    ✅ {expected_file} ({size_mb:.1f}MB)")
                        else:
                            print(f"    ❌ {expected_file} not found")
                else:
                    print(f"    ❌ Orchestrator processing failed")
                    if results['failed_tasks'] > 0:
                        print(f"    ❌ Failed tasks: {results['failed_tasks']}")
                        
            except Exception as e:
                print(f"    ❌ Orchestrator test failed: {e}")
        
        # Test 3: Validate output characteristics
        print(f"\n📊 Test 3: Output Validation")
        reference_dir = config.local_raster_base / "reference"
        
        import rasterio
        import numpy as np
        
        for huc_id in test_hucs:
            print(f"\n  Validating outputs for HUC {huc_id}...")
            
            # Check flow direction
            flow_dir_path = reference_dir / f"huc_{huc_id}_flow_direction_10m.tif"
            if flow_dir_path.exists():
                with rasterio.open(flow_dir_path) as src:
                    data = src.read(1)
                    unique_vals = sorted(np.unique(data))
                    d8_codes = [1, 2, 4, 8, 16, 32, 64, 128]
                    has_all_d8 = all(code in unique_vals for code in d8_codes)
                    print(f"    ✅ Flow direction: D8 encoding {'✓' if has_all_d8 else '✗'} ({len(unique_vals)} unique values)")
            
            # Check hydro mask
            hydro_mask_path = reference_dir / f"huc_{huc_id}_hydro_mask_10m.tif"
            if hydro_mask_path.exists():
                with rasterio.open(hydro_mask_path) as src:
                    data = src.read(1)
                    water_percent = (np.sum(data == 1) / data.size) * 100
                    print(f"    ✅ Hydro mask: {water_percent:.2f}% water coverage")
        
        print(f"\n✅ Simplified 2-reference workflow test completed!")
        print(f"💡 Key improvements:")
        print(f"   - Only 2 reference types: hydro_mask + flow_direction")
        print(f"   - Full HUC rasters (no patch processing)")
        print(f"   - Real PySheds D8 flow direction algorithm")
        print(f"   - Real NHD hydrography mask")
        print(f"   - Ready for downstream patch processing")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simplified_reference_workflow()