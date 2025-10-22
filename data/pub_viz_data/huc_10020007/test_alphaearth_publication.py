#!/usr/bin/env python3
"""
Test publication visualization focusing on AlphaEarth
"""

import numpy as np
import rasterio
import matplotlib.pyplot as plt
from pathlib import Path

def load_alphaearth_tiled_data(data_dir, huc_id):
    """Load and merge tiled AlphaEarth data from GEE export."""
    tile_pattern = f"huc_{huc_id}_alphaearth_10m_2023_16bands-*.tif"
    tile_files = list(data_dir.glob(tile_pattern))
    
    if not tile_files:
        print(f"❌ No AlphaEarth tiles found for pattern: {tile_pattern}")
        return None, None, None
    
    print(f"🔗 Found {len(tile_files)} AlphaEarth tiles")
    
    # Find tile with valid data
    for tile_file in sorted(tile_files):
        with rasterio.open(tile_file) as src:
            # Check first band for valid data
            test_data = src.read(1)
            valid_pixels = (~np.isnan(test_data)).sum()
            print(f"   • {tile_file.name}: {valid_pixels:,} valid pixels")
            
            if valid_pixels > 0:
                print(f"   ✅ Using {tile_file.name} (has valid data)")
                # Use first 3 bands for RGB visualization
                n_bands = min(3, src.count)
                rgb_data = src.read(list(range(1, n_bands + 1)))
                transform = src.transform
                crs = src.crs
                return rgb_data, transform, crs
    
    print("❌ No tiles contain valid data")
    return None, None, None

def test_alphaearth_visualization():
    """Test AlphaEarth visualization."""
    
    data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated")
    huc_id = "10020007"
    
    print("🎨 Testing AlphaEarth visualization")
    print("=" * 40)
    
    # Test AlphaEarth loading
    rgb_data, alpha_transform, alpha_crs = load_alphaearth_tiled_data(data_dir, huc_id)
    
    if rgb_data is not None:
        print(f"✅ AlphaEarth data loaded: {rgb_data.shape}")
        
        # Create RGB composite
        rgb_composite = np.zeros((rgb_data.shape[1], rgb_data.shape[2], 3))
        for i in range(3):
            band = rgb_data[i]
            # Mask no-data values and normalize using percentile stretch
            valid_mask = ~np.isnan(band)
            valid_data = band[valid_mask]
            if len(valid_data) > 0:
                p2, p98 = np.percentile(valid_data, (2, 98))
                normalized = np.clip((band - p2) / (p98 - p2 + 1e-8), 0, 1)
                normalized[~valid_mask] = 0
                rgb_composite[:, :, i] = normalized
                print(f"  Band {i+1}: {len(valid_data):,} valid pixels, range {p2:.3f} to {p98:.3f}")
        
        # Create visualization
        fig, ax = plt.subplots(1, 1, figsize=(8, 8))
        ax.imshow(rgb_composite)
        ax.set_title("AlphaEarth RGB Composite\n(Bands 1-2-3)")
        ax.axis('off')
        
        plt.tight_layout()
        plt.savefig('/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/alphaearth_publication_test.png', 
                   dpi=300, bbox_inches='tight')
        print("✅ AlphaEarth visualization saved: alphaearth_publication_test.png")
        
    else:
        print("❌ Failed to load AlphaEarth data")

if __name__ == "__main__":
    test_alphaearth_visualization()