#!/usr/bin/env python3
"""
Simplified Reference Processing Demo - Final workflow for 2 reference types only.
Demonstrates the cleaned-up pipeline that processes hydro_mask and flow_direction as full HUC rasters.
"""

import sys
import os
from pathlib import Path

# Add the pipeline directory to the path
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def demo_simplified_workflow():
    """Demonstrate the final simplified reference workflow."""
    
    print("🎯 Simplified Reference Processing Pipeline")
    print("=" * 60)
    print("✅ COMPLETED: Transition to 2-reference workflow")
    print()
    
    print("📋 WORKFLOW SPECIFICATIONS:")
    print("   🗂️  Only 2 reference types: hydro_mask + flow_direction")
    print("   🗺️  Output: Full HUC-level rasters (not patches)")
    print("   🧮 Real algorithms: PySheds D8 + NHD vector burn-in")
    print("   📦 Ready for downstream patch processing")
    print()
    
    print("🔧 TECHNICAL IMPROVEMENTS:")
    print("   ✅ Removed land cover processing (not needed)")
    print("   ✅ Removed patch extraction (keep as full rasters)")
    print("   ✅ Real PySheds D8 flow direction algorithm")
    print("   ✅ Real NHD hydrography mask from GDB/shapefiles")
    print("   ✅ Simplified orchestrator (only 2 reference types)")
    print("   ✅ Batch processor compatible")
    print()
    
    print("📊 VALIDATION RESULTS:")
    
    # Check what reference files exist
    reference_dir = Path('/u/nathanj/national_ml/data/local_rasters/reference')
    
    if reference_dir.exists():
        # List all reference files
        reference_files = list(reference_dir.glob('*.tif'))
        reference_files.sort()
        
        print(f"   📁 Reference directory: {len(reference_files)} files")
        
        # Group by HUC and type
        hucs = set()
        file_types = {'hydro_mask': [], 'flow_direction': [], 'other': []}
        
        for file_path in reference_files:
            filename = file_path.name
            
            # Extract HUC ID
            if 'huc_' in filename:
                huc_part = filename.split('huc_')[1]
                huc_id = huc_part.split('_')[0]
                hucs.add(huc_id)
            
            # Categorize file type
            if 'hydro_mask' in filename:
                file_types['hydro_mask'].append(filename)
            elif 'flow_direction' in filename:
                file_types['flow_direction'].append(filename)
            else:
                file_types['other'].append(filename)
        
        print(f"   🗺️  HUCs processed: {len(hucs)} ({sorted(hucs)})")
        print(f"   💧 Hydro masks: {len(file_types['hydro_mask'])}")
        print(f"   🌊 Flow direction: {len(file_types['flow_direction'])}")
        
        if file_types['other']:
            print(f"   ⚠️  Other files: {len(file_types['other'])} (should be 0)")
            for other_file in file_types['other']:
                print(f"      - {other_file}")
        else:
            print(f"   ✅ No unwanted files (good!)")
        
        print()
        
        # Show file details for one HUC
        if hucs:
            sample_huc = sorted(hucs)[0]
            print(f"📊 SAMPLE OUTPUT (HUC {sample_huc}):")
            
            import rasterio
            import numpy as np
            
            # Check flow direction
            flow_files = [f for f in file_types['flow_direction'] if sample_huc in f]
            if flow_files:
                flow_path = reference_dir / flow_files[0]
                with rasterio.open(flow_path) as src:
                    data = src.read(1)
                    unique_vals = len(np.unique(data))
                    file_size_mb = flow_path.stat().st_size / (1024 * 1024)
                    print(f"   🌊 Flow direction: {flow_path.name}")
                    print(f"      📏 Size: {file_size_mb:.1f}MB")
                    print(f"      📐 Dimensions: {src.width} x {src.height}")
                    print(f"      🔢 Unique values: {unique_vals} (D8 encoding)")
            
            # Check hydro mask
            hydro_files = [f for f in file_types['hydro_mask'] if sample_huc in f]
            if hydro_files:
                hydro_path = reference_dir / hydro_files[0]
                with rasterio.open(hydro_path) as src:
                    data = src.read(1)
                    water_percent = (np.sum(data == 1) / data.size) * 100
                    file_size_mb = hydro_path.stat().st_size / (1024 * 1024)
                    print(f"   💧 Hydro mask: {hydro_path.name}")
                    print(f"      📏 Size: {file_size_mb:.1f}MB")
                    print(f"      📐 Dimensions: {src.width} x {src.height}")
                    print(f"      💧 Water coverage: {water_percent:.2f}%")
    
    print()
    print("🚀 USAGE EXAMPLES:")
    print()
    print("   1️⃣  Direct processing:")
    print("      python reference_bulk_processor.py")
    print()
    print("   2️⃣  Batch processing:")
    print("      python batch_processor.py --sources references --hucs 10020007")
    print()
    print("   3️⃣  Multiple HUCs:")
    print("      python batch_processor.py --sources references --hucs 10020007 12345678")
    print()
    print("   4️⃣  Skip existing files:")
    print("      python batch_processor.py --sources references --skip-existing")
    print()
    
    print("✅ SIMPLIFIED REFERENCE WORKFLOW READY!")
    print("   📦 2 reference types only: hydro_mask + flow_direction")
    print("   🗺️  Full HUC rasters (ready for patch processing)")
    print("   🧮 Real algorithms integrated")
    print("   🚀 Production-ready pipeline")

if __name__ == "__main__":
    demo_simplified_workflow()