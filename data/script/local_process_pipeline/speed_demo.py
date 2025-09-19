#!/usr/bin/env python3
"""
Simple Speed Demo - Show existing file detection.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_config import LocalProcessingSettings

def main():
    """Demo speed optimizations."""
    
    print("🚀 Speed Optimization Demo")
    print("=" * 50)
    
    config = LocalProcessingSettings()
    huc_id = "10020007"
    
    # Check existing files
    file_checks = [
        ("DEM", config.local_raster_base / "dem" / f"huc_{huc_id}_dem_10m.tif"),
        ("Landsat", config.local_raster_base / "landsat" / f"huc_{huc_id}_landsat_30m_2023-06-01_2023-09-30.tif"),
        ("SAR", config.local_raster_base / "sentinel1" / f"huc_{huc_id}_sar_10m_2023-06-01_2023-09-30_median.tif"),
        ("AlphaEarth", config.local_raster_base / "alphaearth" / f"huc_{huc_id}_alphaearth_10m_2024_16bands.tif"),
    ]
    
    print("📁 Existing Files Check:")
    total_size_gb = 0
    existing_count = 0
    
    for name, path in file_checks:
        if path.exists():
            size_gb = path.stat().st_size / (1024**3)
            total_size_gb += size_gb
            existing_count += 1
            print(f"   ✅ {name}: {path.name} ({size_gb:.1f} GB)")
        else:
            print(f"   ❌ {name}: Not found")
    
    print(f"\n📊 Summary:")
    print(f"   Existing files: {existing_count}/4")
    print(f"   Total data: {total_size_gb:.1f} GB")
    print(f"   Skip potential: {existing_count} tasks can be skipped")
    
    if existing_count > 0:
        print(f"\n⚡ Speed Benefits:")
        print(f"   ✅ Skip {existing_count} downloads (saves hours)")
        print(f"   ✅ Use {total_size_gb:.1f} GB of existing data")
        print(f"   ✅ Parallel processing of remaining tasks")
        print(f"   ✅ Faster AlphaEarth subset (8 vs 16 bands in fast mode)")
    
    # Show batch processing example
    print(f"\n🚀 Example: Fast Batch Processing")
    print(f"   Command: python batch_processor.py --fast --skip-existing --workers 4")
    print(f"   Effects:")
    print(f"     • Skip existing {existing_count} files")
    print(f"     • Process 4 tasks in parallel")  
    print(f"     • Use reduced AlphaEarth bands")
    print(f"     • Estimated speedup: 3-5x faster")

if __name__ == "__main__":
    main()
