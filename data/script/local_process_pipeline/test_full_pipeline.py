#!/usr/bin/env python3
"""
Full Pipeline Test - From Start to Reference Processing
Runs the complete local processing pipeline for HUC 10020007
"""

import sys
import os
from pathlib import Path
import time

# Add the pipeline directory to the path
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def run_full_pipeline_test():
    """Run the complete pipeline from start to reference processing."""
    
    print("🚀 Starting Full Pipeline Test")
    print("=" * 50)
    print("📋 Test Plan:")
    print("  1. Initialize pipeline components")
    print("  2. Process HUC geometry and boundaries")
    print("  3. Generate reference data (flow direction + hydro mask)")
    print("  4. Validate all outputs")
    print("  5. Generate summary report")
    print("=" * 50)
    
    start_time = time.time()
    huc_id = "10020007"
    
    try:
        # Step 1: Initialize pipeline components
        print(f"\n🔧 Step 1: Initializing Pipeline Components")
        print("-" * 40)
        
        from local_config import LocalProcessingSettings
        from data_manager import LocalDataManager
        from reference_bulk_processor import ReferenceBulkProcessor
        
        config = LocalProcessingSettings()
        data_manager = LocalDataManager(config)
        reference_processor = ReferenceBulkProcessor(config, data_manager)
        
        print(f"✅ Configuration loaded")
        print(f"✅ Data manager initialized")
        print(f"✅ Reference processor initialized")
        
        # Print key paths
        print(f"\n📁 Key Paths:")
        print(f"  🗂️  Input data: {config.local_raster_base}")
        print(f"  📊 Output dir: {reference_processor.reference_output_dir}")
        print(f"  🌊 NHD GDB: {config.nhd_gdb_path}")
        print(f"  🏔️  DEM dir: {config.local_raster_base / 'dem'}")
        
        # Step 2: Check input data availability
        print(f"\n🔍 Step 2: Checking Input Data Availability")
        print("-" * 40)
        
        # Check DEM
        dem_path = config.local_raster_base / "dem" / f"huc_{huc_id}_dem_10m.tif"
        dem_research_path = config.local_raster_base / "dem" / f"huc_{huc_id}_dem_10m_research_grade.tif"
        dem_exists = dem_path.exists() or dem_research_path.exists()
        actual_dem = dem_path if dem_path.exists() else dem_research_path
        
        print(f"🏔️  DEM: {'✅' if dem_exists else '❌'}")
        if dem_exists:
            size_mb = actual_dem.stat().st_size / (1024 * 1024)
            print(f"     Path: {actual_dem.name}")
            print(f"     Size: {size_mb:.1f} MB")
        
        # Check NHD data
        nhd_gdb_exists = config.nhd_gdb_path and Path(config.nhd_gdb_path).exists()
        nhd_shp_exists = config.nhd_shapefiles_dir and Path(config.nhd_shapefiles_dir).exists()
        
        print(f"🌊 NHD GDB: {'✅' if nhd_gdb_exists else '❌'}")
        print(f"🗺️  NHD Shapefiles: {'✅' if nhd_shp_exists else '❌'}")
        
        if not (dem_exists and (nhd_gdb_exists or nhd_shp_exists)):
            print(f"\n❌ Missing required input data. Cannot proceed.")
            return False
        
        # Step 3: Process HUC boundaries (if needed)
        print(f"\n🗺️  Step 3: Processing HUC Boundaries")
        print("-" * 40)
        
        # Check if boundary data exists
        boundary_dir = config.local_raster_base.parent / "processed" / "huc_processing" / huc_id
        boundary_file = boundary_dir / f"huc8_{huc_id}_boundary.geojson"
        
        if boundary_file.exists():
            print(f"✅ HUC boundary already exists: {boundary_file.name}")
        else:
            print(f"⚠️  HUC boundary not found, will be created during processing")
        
        # Step 4: Generate Reference Data
        print(f"\n🌊 Step 4: Generating Reference Data")
        print("-" * 40)
        
        # Process all reference types for the HUC
        print(f"🎯 Processing HUC: {huc_id}")
        
        reference_results = reference_processor.process_all_references_for_huc(huc_id)
        
        print(f"\n📊 Reference Processing Results:")
        for ref_type, path in reference_results.items():
            if path and Path(path).exists():
                size_mb = Path(path).stat().st_size / (1024 * 1024)
                print(f"  ✅ {ref_type}: {Path(path).name} ({size_mb:.1f}MB)")
            else:
                print(f"  ❌ {ref_type}: Failed or missing")
        
        # Step 5: Validate Outputs
        print(f"\n✅ Step 5: Validating Pipeline Outputs")
        print("-" * 40)
        
        validation_results = validate_pipeline_outputs(huc_id, config)
        
        # Step 6: Generate Summary Report
        print(f"\n📊 Step 6: Pipeline Summary Report")
        print("-" * 40)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        success_count = sum(1 for result in validation_results.values() if result['status'] == 'success')
        total_checks = len(validation_results)
        success_rate = (success_count / total_checks) * 100
        
        print(f"⏱️  Processing Time: {processing_time:.1f} seconds")
        print(f"📊 Success Rate: {success_count}/{total_checks} ({success_rate:.0f}%)")
        print(f"🎯 HUC Processed: {huc_id}")
        
        if success_rate >= 80:
            print(f"\n🎉 PIPELINE TEST SUCCESSFUL!")
            print(f"✅ Local processing pipeline is fully operational")
            print(f"✅ No Google Earth Engine dependency required")
            print(f"✅ All reference data generated locally")
        else:
            print(f"\n⚠️  PIPELINE TEST NEEDS ATTENTION")
            print(f"❌ Some components failed validation")
        
        print(f"\n📁 Output Files Location:")
        ref_dir = config.local_raster_base / "reference"
        if ref_dir.exists():
            ref_files = list(ref_dir.glob(f"*{huc_id}*.tif"))
            for ref_file in sorted(ref_files):
                size_mb = ref_file.stat().st_size / (1024 * 1024)
                print(f"  📄 {ref_file.name} ({size_mb:.1f}MB)")
        
        return success_rate >= 80
        
    except Exception as e:
        print(f"\n❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_pipeline_outputs(huc_id, config):
    """Validate all pipeline outputs for completeness and correctness."""
    
    print(f"🔍 Validating outputs for HUC {huc_id}...")
    
    validation_results = {}
    
    # Check reference files
    reference_files = {
        'flow_direction': config.local_raster_base / "reference" / f"huc_{huc_id}_flow_direction_10m.tif",
        'hydro_mask': config.local_raster_base / "reference" / f"huc_{huc_id}_hydro_mask_10m.tif"
    }
    
    for ref_type, path in reference_files.items():
        result = {'status': 'failed', 'message': '', 'details': {}}
        
        try:
            if not path.exists():
                result['message'] = 'File does not exist'
            else:
                import rasterio
                import numpy as np
                
                with rasterio.open(path) as src:
                    data = src.read(1)
                    
                    # Basic file checks
                    result['details']['file_size_mb'] = path.stat().st_size / (1024 * 1024)
                    result['details']['dimensions'] = f"{src.width}x{src.height}"
                    result['details']['crs'] = str(src.crs)
                    result['details']['resolution'] = abs(src.transform[0])
                    
                    # Data-specific validation
                    if ref_type == 'flow_direction':
                        unique_vals = sorted(np.unique(data))
                        d8_codes = [1, 2, 4, 8, 16, 32, 64, 128]
                        has_valid_d8 = all(code in unique_vals for code in d8_codes)
                        
                        result['details']['unique_values'] = len(unique_vals)
                        result['details']['valid_d8_encoding'] = has_valid_d8
                        
                        if has_valid_d8 and len(unique_vals) == 8:
                            result['status'] = 'success'
                            result['message'] = 'Valid D8 flow direction'
                        else:
                            result['message'] = f'Invalid D8 encoding: {unique_vals}'
                    
                    elif ref_type == 'hydro_mask':
                        unique_vals = sorted(np.unique(data))
                        is_binary = all(val in [0, 1] for val in unique_vals)
                        water_percent = (np.sum(data == 1) / data.size) * 100
                        
                        result['details']['unique_values'] = unique_vals.tolist()
                        result['details']['is_binary'] = is_binary
                        result['details']['water_coverage_percent'] = water_percent
                        
                        if is_binary:
                            result['status'] = 'success'
                            result['message'] = f'Valid binary mask ({water_percent:.2f}% water)'
                        else:
                            result['message'] = f'Invalid binary mask: {unique_vals}'
                    
        except Exception as e:
            result['message'] = f'Validation error: {e}'
        
        validation_results[ref_type] = result
        
        # Print validation result
        status_icon = '✅' if result['status'] == 'success' else '❌'
        print(f"  {status_icon} {ref_type}: {result['message']}")
        
        if result['details']:
            for key, value in result['details'].items():
                print(f"     {key}: {value}")
    
    return validation_results

if __name__ == "__main__":
    print("🧪 Full Pipeline Test - HUC 10020007")
    print("Testing complete local processing workflow")
    print()
    
    success = run_full_pipeline_test()
    
    if success:
        print(f"\n🎊 SUCCESS: Pipeline test completed successfully!")
        print(f"🚀 Ready to proceed with visualization")
    else:
        print(f"\n💥 FAILURE: Pipeline test encountered issues")
        print(f"🔧 Please check error messages above")
    
    exit(0 if success else 1)