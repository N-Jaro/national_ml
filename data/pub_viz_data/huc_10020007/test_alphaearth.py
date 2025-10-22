#!/usr/bin/env python3
"""
Test AlphaEarth visualization from tiled data
"""

import numpy as np
import rasterio
import matplotlib.pyplot as plt
from pathlib import Path

def test_alphaearth_tiled():
    """Test loading and visualizing AlphaEarth tiled data."""
    
    data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated")
    huc_id = "10020007"
    
    # Find AlphaEarth tiles
    tile_pattern = f"huc_{huc_id}_alphaearth_10m_2023_16bands-*.tif"
    tile_files = list(data_dir.glob(tile_pattern))
    
    print(f"Found {len(tile_files)} AlphaEarth tiles:")
    for tile in sorted(tile_files):
        print(f"  • {tile.name}")
    
    if not tile_files:
        print("No tiles found!")
        return
    
    # Load first tile
    tile_file = tile_files[0]
    print(f"\n📊 Analyzing {tile_file.name}:")
    
    with rasterio.open(tile_file) as src:
        print(f"  Shape: {src.shape}")
        print(f"  Bands: {src.count}")
        print(f"  CRS: {src.crs}")
        
        # Read first 3 bands
        n_bands = min(3, src.count)
        rgb_data = src.read(list(range(1, n_bands + 1)))
        
        print(f"  RGB data shape: {rgb_data.shape}")
        
        # Check for valid data
        for i, band in enumerate(rgb_data):
            valid_mask = ~np.isnan(band)
            n_valid = valid_mask.sum()
            if n_valid > 0:
                print(f"  Band {i+1}: {n_valid:,} valid pixels, range: {band[valid_mask].min():.3f} to {band[valid_mask].max():.3f}")
            else:
                print(f"  Band {i+1}: No valid data (all NaN)")
        
        # Create simple visualization
        if n_bands >= 3:
            fig, axes = plt.subplots(1, 4, figsize=(16, 4))
            
            # Individual bands
            for i in range(3):
                band = rgb_data[i]
                valid_data = band[~np.isnan(band)]
                if len(valid_data) > 0:
                    axes[i].imshow(band, cmap='viridis')
                    axes[i].set_title(f'Band {i+1}')
                else:
                    axes[i].text(0.5, 0.5, 'No Valid Data', ha='center', va='center', transform=axes[i].transAxes)
                    axes[i].set_title(f'Band {i+1}')
                axes[i].axis('off')
            
            # RGB composite
            rgb_composite = np.zeros((rgb_data.shape[1], rgb_data.shape[2], 3))
            for i in range(3):
                band = rgb_data[i]
                valid_mask = ~np.isnan(band)
                if valid_mask.sum() > 0:
                    valid_data = band[valid_mask]
                    p2, p98 = np.percentile(valid_data, (2, 98))
                    normalized = np.clip((band - p2) / (p98 - p2 + 1e-8), 0, 1)
                    normalized[~valid_mask] = 0
                    rgb_composite[:, :, i] = normalized
            
            axes[3].imshow(rgb_composite)
            axes[3].set_title('RGB Composite')
            axes[3].axis('off')
            
            plt.tight_layout()
            plt.savefig('/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/alphaearth_test.png', 
                       dpi=150, bbox_inches='tight')
            print(f"\n✅ Test visualization saved: alphaearth_test.png")
        else:
            print("Not enough bands for RGB composite")

if __name__ == "__main__":
    test_alphaearth_tiled()