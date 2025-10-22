#!/usr/bin/env python3
"""
Complete AlphaEarth Processing Pipeline
Merge tiles and create visualizations for AlphaEarth data
"""

import numpy as np
import matplotlib.pyplot as plt
import rasterio
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def verify_merged_file(merged_file_path):
    """Verify the merged AlphaEarth file is valid and provide summary statistics."""
    print(f"🔍 Verifying merged file: {merged_file_path}")
    
    if not merged_file_path.exists():
        print(f"❌ File does not exist: {merged_file_path}")
        return False
    
    try:
        with rasterio.open(merged_file_path) as src:
            print(f"✅ File verification successful!")
            print(f"   📏 Dimensions: {src.width:,} × {src.height:,} pixels")
            print(f"   📊 Bands: {src.count}")
            print(f"   📍 CRS: {src.crs}")
            print(f"   🎯 Pixel size: {abs(src.transform[0]):.1f}m")
            
            # Sample data quality
            sample_band = src.read(1)
            valid_pixels = (~np.isnan(sample_band)).sum() if src.nodata is None else (sample_band != src.nodata).sum()
            total_pixels = sample_band.size
            validity_percent = (valid_pixels / total_pixels) * 100
            
            print(f"   💾 File size: {merged_file_path.stat().st_size / (1024**3):.2f} GB")
            print(f"   ✨ Valid pixels: {valid_pixels:,} ({validity_percent:.1f}%)")
            
            # Band statistics
            print(f"   📈 Band statistics (first 3 bands):")
            for i in range(min(3, src.count)):
                band_data = src.read(i + 1)
                valid_data = band_data[~np.isnan(band_data)] if src.nodata is None else band_data[band_data != src.nodata]
                if len(valid_data) > 0:
                    print(f"      Band {i+1}: min={valid_data.min():.3f}, max={valid_data.max():.3f}, mean={valid_data.mean():.3f}")
            
            return True
            
    except Exception as e:
        print(f"❌ File verification failed: {e}")
        return False

def process_alphaearth_complete(data_dir, huc_id):
    """Complete processing: merge tiles and create visualizations."""
    print(f"🚀 Complete AlphaEarth Processing for HUC {huc_id}")
    print("=" * 60)
    
    data_dir = Path(data_dir)
    
    # Step 1: Check if merged file already exists
    merged_file = data_dir / f"huc_{huc_id}_alphaearth_10m_2024_8bands_merged.tif"
    
    if merged_file.exists():
        print(f"✅ Merged file already exists: {merged_file.name}")
        if verify_merged_file(merged_file):
            print(f"📊 Merged file is valid, skipping tile merging")
        else:
            print(f"⚠️  Merged file exists but appears corrupted, will recreate")
            merged_file.unlink()  # Remove corrupted file
    
    # Step 2: Merge tiles if needed
    if not merged_file.exists():
        print(f"🔗 Starting tile merging...")
        from merge_alphaearth_tiles import merge_alphaearth_tiles
        merged_file = merge_alphaearth_tiles(data_dir, huc_id)
        
        if not merged_file or not merged_file.exists():
            print(f"❌ Tile merging failed")
            return False
    
    # Step 3: Create visualizations
    print(f"\n🎨 Creating visualizations...")
    try:
        from create_alphaearth_merged_viz import create_alphaearth_merged_visualization
        create_alphaearth_merged_visualization()
        print(f"✅ Visualizations created successfully")
    except Exception as e:
        print(f"⚠️  Visualization creation failed: {e}")
    
    # Step 4: Final summary
    print(f"\n📋 Processing Summary:")
    print("=" * 40)
    print(f"🎯 HUC: {huc_id}")
    print(f"📁 Data directory: {data_dir}")
    print(f"💾 Merged file: {merged_file}")
    
    # List all related files
    alphaearth_files = list(data_dir.glob(f"*alphaearth*"))
    print(f"📁 AlphaEarth files in directory:")
    for file in sorted(alphaearth_files):
        size_mb = file.stat().st_size / (1024 * 1024)
        if size_mb > 1024:
            size_str = f"{size_mb/1024:.1f} GB"
        else:
            size_str = f"{size_mb:.1f} MB"
        print(f"   • {file.name}: {size_str}")
    
    viz_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007")
    viz_files = list(viz_dir.glob("alphaearth_*.png"))
    if viz_files:
        print(f"🎨 Visualization files:")
        for file in sorted(viz_files):
            print(f"   • {file.name}")
    
    return True

def main():
    """Main function for complete AlphaEarth processing."""
    data_dir = "/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated"
    huc_id = "10020007"
    
    success = process_alphaearth_complete(data_dir, huc_id)
    
    if success:
        print(f"\n🎉 Complete AlphaEarth processing finished successfully!")
        print(f"🔗 You now have:")
        print(f"   • Merged AlphaEarth TIF file (single file from 4 tiles)")
        print(f"   • Visualization showing merged data")
        print(f"   • Tile structure analysis")
    else:
        print(f"\n❌ Processing encountered errors")

if __name__ == "__main__":
    main()