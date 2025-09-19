#!/usr/bin/env python3
"""
Test the fixed hydrography mask processor
"""

import sys
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def test_fixed_hydro_mask():
    """Test the fixed hydrography mask creation."""
    
    print("🧪 TESTING FIXED HYDROGRAPHY MASK")
    print("=" * 40)
    
    try:
        from local_config import LocalProcessingSettings
        from data_manager import LocalDataManager
        from reference_bulk_processor import ReferenceBulkProcessor
        
        config = LocalProcessingSettings()
        data_manager = LocalDataManager(config)
        processor = ReferenceBulkProcessor(config, data_manager)
        
        huc_id = "10020007"
        
        print(f"🎯 Creating hydrography mask for HUC {huc_id}...")
        hydro_mask_path = processor.create_hydrography_mask(huc_id)
        
        print(f"✅ Created: {hydro_mask_path}")
        
        # Validate the result
        import rasterio
        import numpy as np
        from pathlib import Path
        
        path = Path(hydro_mask_path)
        size_mb = path.stat().st_size / (1024 * 1024)
        
        with rasterio.open(hydro_mask_path) as src:
            data = src.read(1)
            water_pixels = np.sum(data == 1)
            total_pixels = data.size
            water_percent = (water_pixels / total_pixels) * 100
            
            print(f"\n📊 Results:")
            print(f"  📏 File size: {size_mb:.1f} MB")
            print(f"  📐 Dimensions: {src.width} × {src.height}")
            print(f"  💧 Water coverage: {water_percent:.4f}%")
            print(f"  🔢 Water pixels: {water_pixels:,}")
            
            if water_percent > 0:
                print(f"  🎉 SUCCESS: Hydrography mask now has water features!")
                return True
            else:
                print(f"  😞 ISSUE: Still no water coverage")
                return False
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_fixed_hydro_mask()
    if success:
        print(f"\n🎊 Hydrography mask is now working correctly!")
    else:
        print(f"\n🔧 Further debugging needed.")