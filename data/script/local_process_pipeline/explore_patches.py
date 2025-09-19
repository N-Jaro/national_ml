#!/usr/bin/env python3
"""
Quick data exploration script to examine specific regions and create sample patches.
"""

import rasterio
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os

def explore_landsat_patches(file_path, patch_size=256, n_patches=6):
    """Create sample patches from different regions of the Landsat image."""
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    print(f"🔍 Creating sample patches from: {file_path}")
    
    with rasterio.open(file_path) as src:
        height, width = src.shape
        n_bands = src.count
        
        print(f"📏 Image: {height}x{width}, {n_bands} bands")
        
        # Read all bands
        bands = src.read()  # Shape: (bands, height, width)
        
        # Calculate indices for water detection
        if n_bands >= 6:
            blue = bands[0]    # SR_B2
            green = bands[1]   # SR_B3
            red = bands[2]     # SR_B4
            nir = bands[3]     # SR_B5
            swir1 = bands[4]   # SR_B6
            
            # MNDWI for water detection
            mndwi = (green - swir1) / (green + swir1 + 1e-8)
            
            # NDVI for vegetation
            ndvi = (nir - red) / (nir + red + 1e-8)
    
    # Create patches from different regions
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Sample Landsat Patches (True Color)', fontsize=16, fontweight='bold')
    
    # Generate diverse sample locations
    sample_locations = []
    
    # Random sampling
    for i in range(n_patches):
        max_row = height - patch_size
        max_col = width - patch_size
        
        if max_row > 0 and max_col > 0:
            row = np.random.randint(0, max_row)
            col = np.random.randint(0, max_col)
            sample_locations.append((row, col))
    
    for idx, (row, col) in enumerate(sample_locations):
        ax_row = idx // 3
        ax_col = idx % 3
        ax = axes[ax_row, ax_col]
        
        # Extract patch
        patch = bands[:, row:row+patch_size, col:col+patch_size]
        
        # Create RGB composite (bands 2, 1, 0 = Red, Green, Blue)
        if n_bands >= 3:
            red_patch = patch[2]    # SR_B4 (Red)
            green_patch = patch[1]  # SR_B3 (Green)
            blue_patch = patch[0]   # SR_B2 (Blue)
            
            # Normalize for display
            def normalize_for_display(band):
                band = np.clip(band, 0, 0.3)
                return (band - band.min()) / (band.max() - band.min() + 1e-8)
            
            rgb = np.stack([
                normalize_for_display(red_patch),
                normalize_for_display(green_patch),
                normalize_for_display(blue_patch)
            ], axis=-1)
            
            ax.imshow(rgb)
            
            # Calculate some statistics for this patch
            if n_bands >= 6:
                patch_mndwi = mndwi[row:row+patch_size, col:col+patch_size]
                patch_ndvi = ndvi[row:row+patch_size, col:col+patch_size]
                
                water_pixels = np.sum(patch_mndwi > 0.1)
                veg_pixels = np.sum(patch_ndvi > 0.5)
                total_pixels = patch_size * patch_size
                
                title = f'Patch {idx+1}: ({row}, {col})\n'
                title += f'Water: {(water_pixels/total_pixels)*100:.1f}%, '
                title += f'Veg: {(veg_pixels/total_pixels)*100:.1f}%'
            else:
                title = f'Patch {idx+1}: ({row}, {col})'
            
            ax.set_title(title, fontsize=10)
        
        ax.axis('off')
    
    plt.tight_layout()
    
    # Save patches visualization
    output_file = Path('/tmp/landsat_sample_patches.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"🖼️ Sample patches saved to: {output_file}")
    plt.show()
    
    # Create spectral index comparison
    if n_bands >= 6:
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        
        # MNDWI
        im1 = axes[0].imshow(mndwi, cmap='RdYlBu_r', vmin=-1, vmax=1)
        axes[0].set_title('MNDWI (Water Index)', fontsize=12)
        axes[0].axis('off')
        plt.colorbar(im1, ax=axes[0], shrink=0.8)
        
        # NDVI
        im2 = axes[1].imshow(ndvi, cmap='RdYlGn', vmin=-1, vmax=1)
        axes[1].set_title('NDVI (Vegetation Index)', fontsize=12)
        axes[1].axis('off')
        plt.colorbar(im2, ax=axes[1], shrink=0.8)
        
        # Combined classification
        classification = np.zeros((height, width, 3))
        
        # Water: Blue
        water_mask = mndwi > 0.1
        classification[water_mask] = [0, 0.5, 1]
        
        # Vegetation: Green
        veg_mask = (ndvi > 0.5) & (~water_mask)
        classification[veg_mask] = [0, 0.8, 0]
        
        # Urban/Bare: Gray
        urban_mask = (ndvi < 0.2) & (mndwi < 0) & (~water_mask)
        classification[urban_mask] = [0.6, 0.6, 0.6]
        
        # Mixed/Other: Brown
        other_mask = (~water_mask) & (~veg_mask) & (~urban_mask)
        classification[other_mask] = [0.8, 0.6, 0.4]
        
        axes[2].imshow(classification)
        axes[2].set_title('Land Cover Classification', fontsize=12)
        axes[2].axis('off')
        
        # Add legend
        from matplotlib.patches import Rectangle
        legend_elements = [
            Rectangle((0, 0), 1, 1, facecolor=[0, 0.5, 1], label='Water'),
            Rectangle((0, 0), 1, 1, facecolor=[0, 0.8, 0], label='Vegetation'),
            Rectangle((0, 0), 1, 1, facecolor=[0.6, 0.6, 0.6], label='Urban/Bare'),
            Rectangle((0, 0), 1, 1, facecolor=[0.8, 0.6, 0.4], label='Mixed/Other')
        ]
        axes[2].legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1, 1))
        
        plt.suptitle('Landsat Spectral Indices and Classification', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        # Save indices visualization
        indices_file = Path('/tmp/landsat_indices_classification.png')
        plt.savefig(indices_file, dpi=300, bbox_inches='tight')
        print(f"📊 Indices and classification saved to: {indices_file}")
        plt.show()
        
        # Print classification statistics
        total_area = height * width * 30 * 30 / 1e6  # km²
        water_area = np.sum(water_mask) * 30 * 30 / 1e6
        veg_area = np.sum(veg_mask) * 30 * 30 / 1e6
        urban_area = np.sum(urban_mask) * 30 * 30 / 1e6
        other_area = np.sum(other_mask) * 30 * 30 / 1e6
        
        print(f"\n🏞️ Land Cover Summary:")
        print(f"   Total area: {total_area:.2f} km²")
        print(f"   Water: {water_area:.2f} km² ({(water_area/total_area)*100:.1f}%)")
        print(f"   Vegetation: {veg_area:.2f} km² ({(veg_area/total_area)*100:.1f}%)")
        print(f"   Urban/Bare: {urban_area:.2f} km² ({(urban_area/total_area)*100:.1f}%)")
        print(f"   Mixed/Other: {other_area:.2f} km² ({(other_area/total_area)*100:.1f}%)")

def main():
    """Main function for patch exploration."""
    
    # Look for the downloaded Landsat file
    landsat_files = [
        "/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-07-01_2023-07-31.tif",
        "/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-06-01_2023-09-30.tif"
    ]
    
    file_to_explore = None
    for file_path in landsat_files:
        if os.path.exists(file_path):
            file_to_explore = file_path
            break
    
    if file_to_explore:
        explore_landsat_patches(file_to_explore)
    else:
        print("❌ No Landsat files found for exploration")

if __name__ == "__main__":
    main()