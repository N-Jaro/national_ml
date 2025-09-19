#!/usr/bin/env python3
"""
Debug Landsat Visualization Issues
Investigate why Landsat shows random noise in visualization
"""

import sys
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from pathlib import Path

sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def debug_landsat_visualization():
    """Debug Landsat data and visualization issues."""
    
    print("🔍 DEBUGGING LANDSAT VISUALIZATION")
    print("=" * 40)
    
    huc_id = "10020007"
    
    try:
        from local_config import LocalProcessingSettings
        config = LocalProcessingSettings()
        
        # Landsat file path
        landsat_path = config.local_raster_base / "landsat" / f"huc_{huc_id}_landsat_30m_2023-06-01_2023-09-30.tif"
        
        print(f"📊 Analyzing Landsat file:")
        print(f"  📁 Path: {landsat_path}")
        print(f"  📏 Size: {landsat_path.stat().st_size / (1024*1024):.1f} MB")
        
        with rasterio.open(landsat_path) as src:
            print(f"\n🔍 File Metadata:")
            print(f"  📐 Dimensions: {src.width} × {src.height}")
            print(f"  🔢 Bands: {src.count}")
            print(f"  📊 Data type: {src.dtypes[0]}")
            print(f"  🗺️  CRS: {src.crs}")
            print(f"  🎯 Resolution: {abs(src.transform[0]):.1f}m")
            print(f"  📍 Bounds: {src.bounds}")
            
            if src.nodata is not None:
                print(f"  🚫 NoData value: {src.nodata}")
            
            # Analyze each band
            print(f"\n📊 Band Analysis:")
            for band_idx in range(1, min(src.count + 1, 13)):  # Check up to 12 bands
                print(f"\n  🔢 Band {band_idx}:")
                
                try:
                    band_data = src.read(band_idx)
                    
                    # Basic statistics
                    finite_data = band_data[np.isfinite(band_data)]
                    if src.nodata is not None:
                        finite_data = finite_data[finite_data != src.nodata]
                    
                    if len(finite_data) == 0:
                        print(f"    ❌ No valid data")
                        continue
                    
                    print(f"    📈 Data range: {finite_data.min()} to {finite_data.max()}")
                    print(f"    📊 Mean: {finite_data.mean():.2f}")
                    print(f"    📊 Std: {finite_data.std():.2f}")
                    print(f"    📊 Median: {np.median(finite_data):.2f}")
                    
                    # Check for unusual values
                    zero_pixels = np.sum(finite_data == 0)
                    negative_pixels = np.sum(finite_data < 0)
                    very_high_pixels = np.sum(finite_data > 10000)
                    
                    print(f"    🔍 Zero pixels: {zero_pixels:,} ({(zero_pixels/len(finite_data))*100:.1f}%)")
                    if negative_pixels > 0:
                        print(f"    ⚠️  Negative pixels: {negative_pixels:,}")
                    if very_high_pixels > 0:
                        print(f"    ⚠️  Very high pixels (>10000): {very_high_pixels:,}")
                    
                    # Percentiles for normalization testing
                    p2, p5, p95, p98 = np.percentile(finite_data, [2, 5, 95, 98])
                    print(f"    📊 Percentiles: 2%={p2:.1f}, 5%={p5:.1f}, 95%={p95:.1f}, 98%={p98:.1f}")
                
                except Exception as e:
                    print(f"    ❌ Error reading band {band_idx}: {e}")
            
            # Test RGB composite creation
            print(f"\n🎨 Testing RGB Composite Creation:")
            
            if src.count >= 4:
                # Standard Landsat band order for RGB/NIR
                # Assuming bands are: Blue(1), Green(2), Red(3), NIR(4), etc.
                
                print(f"  📖 Reading RGB bands (assuming Landsat band order)...")
                
                try:
                    # Try different band combinations
                    band_combinations = [
                        {"name": "True Color (RGB)", "bands": [3, 2, 1], "desc": "Red, Green, Blue"},
                        {"name": "False Color (NIR)", "bands": [4, 3, 2], "desc": "NIR, Red, Green"},
                        {"name": "SWIR Composite", "bands": [7, 5, 3], "desc": "SWIR2, SWIR1, Red"} if src.count >= 7 else None
                    ]
                    
                    band_combinations = [combo for combo in band_combinations if combo is not None]
                    
                    for combo in band_combinations:
                        print(f"\n  🔍 Testing {combo['name']} ({combo['desc']}):")
                        
                        try:
                            bands_data = []
                            for band_num in combo['bands']:
                                if band_num <= src.count:
                                    band = src.read(band_num)
                                    bands_data.append(band)
                                else:
                                    print(f"    ❌ Band {band_num} not available")
                                    break
                            
                            if len(bands_data) == 3:
                                r, g, b = bands_data
                                
                                # Test different normalization methods
                                normalization_methods = [
                                    {"name": "Percentile (2-98%)", "method": "percentile"},
                                    {"name": "Min-Max", "method": "minmax"},
                                    {"name": "Standard Score", "method": "zscore"}
                                ]
                                
                                for norm_method in normalization_methods:
                                    print(f"    🔧 {norm_method['name']}:")
                                    
                                    try:
                                        if norm_method['method'] == 'percentile':
                                            # Percentile normalization
                                            def normalize_band(band):
                                                valid = band[np.isfinite(band)]
                                                if src.nodata is not None:
                                                    valid = valid[valid != src.nodata]
                                                if len(valid) == 0:
                                                    return np.zeros_like(band)
                                                p2, p98 = np.percentile(valid, [2, 98])
                                                return np.clip((band - p2) / (p98 - p2), 0, 1)
                                        
                                        elif norm_method['method'] == 'minmax':
                                            # Min-max normalization
                                            def normalize_band(band):
                                                valid = band[np.isfinite(band)]
                                                if src.nodata is not None:
                                                    valid = valid[valid != src.nodata]
                                                if len(valid) == 0:
                                                    return np.zeros_like(band)
                                                return (band - valid.min()) / (valid.max() - valid.min())
                                        
                                        elif norm_method['method'] == 'zscore':
                                            # Z-score normalization
                                            def normalize_band(band):
                                                valid = band[np.isfinite(band)]
                                                if src.nodata is not None:
                                                    valid = valid[valid != src.nodata]
                                                if len(valid) == 0:
                                                    return np.zeros_like(band)
                                                mean, std = valid.mean(), valid.std()
                                                normalized = (band - mean) / std
                                                return np.clip((normalized + 3) / 6, 0, 1)  # Normalize to 0-1
                                        
                                        r_norm = normalize_band(r)
                                        g_norm = normalize_band(g)
                                        b_norm = normalize_band(b)
                                        
                                        rgb = np.dstack([r_norm, g_norm, b_norm])
                                        
                                        # Check result
                                        valid_pixels = np.sum(np.any(rgb > 0, axis=2))
                                        total_pixels = rgb.shape[0] * rgb.shape[1]
                                        valid_percent = (valid_pixels / total_pixels) * 100
                                        
                                        print(f"      📊 Valid pixels: {valid_pixels:,} ({valid_percent:.1f}%)")
                                        print(f"      📈 RGB range: [{rgb.min():.3f}, {rgb.max():.3f}]")
                                        
                                        # Check for noise patterns
                                        if rgb.std() < 0.01:
                                            print(f"      ⚠️  Very low variation - might be empty/uniform")
                                        elif rgb.std() > 0.4:
                                            print(f"      ⚠️  Very high variation - might be noisy")
                                        else:
                                            print(f"      ✅ Reasonable variation")
                                        
                                    except Exception as e:
                                        print(f"      ❌ Normalization failed: {e}")
                        
                        except Exception as e:
                            print(f"    ❌ Composite creation failed: {e}")
                
                except Exception as e:
                    print(f"  ❌ RGB testing failed: {e}")
            
            else:
                print(f"  ⚠️  Not enough bands for RGB composite (only {src.count} bands)")
        
        # Test with a small crop for visualization
        print(f"\n🖼️  Creating test visualization...")
        output_dir = Path("/u/nathanj/national_ml/outputs")
        test_vis_path = output_dir / f"landsat_debug_{huc_id}.png"
        
        create_test_landsat_visualization(landsat_path, test_vis_path)
        
        print(f"💾 Saved test visualization: {test_vis_path.name}")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

def create_test_landsat_visualization(landsat_path, output_path):
    """Create a test visualization of Landsat data with different methods."""
    
    with rasterio.open(landsat_path) as src:
        # Read center crop for faster processing
        height, width = src.height, src.width
        crop_size = min(500, height//4, width//4)
        
        window = rasterio.windows.Window(
            col_off=width//2 - crop_size//2,
            row_off=height//2 - crop_size//2, 
            width=crop_size,
            height=crop_size
        )
        
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle(f'Landsat Visualization Debug - {landsat_path.name}', fontsize=14)
        
        # Test different band combinations and normalizations
        if src.count >= 4:
            # Read bands
            bands = {}
            for i in range(1, min(8, src.count + 1)):
                try:
                    bands[i] = src.read(i, window=window)
                except:
                    pass
            
            visualizations = [
                {"title": "True Color (3,2,1)", "bands": [3,2,1]},
                {"title": "False Color (4,3,2)", "bands": [4,3,2]},
                {"title": "Band 1 (Blue)", "bands": [1], "cmap": "Blues"},
                {"title": "Band 2 (Green)", "bands": [2], "cmap": "Greens"},
                {"title": "Band 3 (Red)", "bands": [3], "cmap": "Reds"},
                {"title": "Band 4 (NIR)", "bands": [4], "cmap": "RdYlGn"}
            ]
            
            for idx, vis in enumerate(visualizations):
                if idx >= 6:
                    break
                    
                ax = axes[idx // 3, idx % 3]
                
                try:
                    if len(vis["bands"]) == 3:
                        # RGB composite
                        rgb_data = []
                        for band_num in vis["bands"]:
                            if band_num in bands:
                                band = bands[band_num]
                                # Percentile normalization
                                valid = band[np.isfinite(band)]
                                if len(valid) > 0:
                                    p2, p98 = np.percentile(valid, [2, 98])
                                    normalized = np.clip((band - p2) / (p98 - p2), 0, 1)
                                else:
                                    normalized = np.zeros_like(band)
                                rgb_data.append(normalized)
                        
                        if len(rgb_data) == 3:
                            rgb = np.dstack(rgb_data)
                            ax.imshow(rgb)
                        else:
                            ax.text(0.5, 0.5, 'Missing\nBands', ha='center', va='center', transform=ax.transAxes)
                    
                    else:
                        # Single band
                        band_num = vis["bands"][0]
                        if band_num in bands:
                            band = bands[band_num]
                            cmap = vis.get("cmap", "viridis")
                            im = ax.imshow(band, cmap=cmap)
                            plt.colorbar(im, ax=ax, shrink=0.6)
                        else:
                            ax.text(0.5, 0.5, f'Band {band_num}\nMissing', ha='center', va='center', transform=ax.transAxes)
                    
                    ax.set_title(vis["title"], fontsize=10)
                    ax.set_xticks([])
                    ax.set_yticks([])
                
                except Exception as e:
                    ax.text(0.5, 0.5, f'Error:\n{str(e)[:20]}', ha='center', va='center', transform=ax.transAxes)
                    ax.set_title(f"{vis['title']} (Error)", fontsize=10)
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()

if __name__ == "__main__":
    debug_landsat_visualization()