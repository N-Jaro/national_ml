#!/usr/bin/env python3
"""
Create AlphaEarth-focused visualization with merged tiles
"""

import numpy as np
import matplotlib.pyplot as plt
import rasterio
from pathlib import Path

def load_alphaearth_merged_data(data_dir, huc_id):
    """Load merged AlphaEarth data from single merged file."""
    merged_file = data_dir / f"huc_{huc_id}_alphaearth_10m_2024_8bands_merged.tif"
    
    if not merged_file.exists():
        print(f"❌ Merged AlphaEarth file not found: {merged_file}")
        return None, None, None
    
    print(f"📁 Loading merged AlphaEarth file: {merged_file.name}")
    
    with rasterio.open(merged_file) as src:
        # Read first 3 bands for RGB visualization
        n_bands = min(3, src.count)
        rgb_data = src.read(list(range(1, n_bands + 1)))
        
        # Count valid pixels
        sample_band = src.read(1)
        valid_pixels = (~np.isnan(sample_band)).sum() if src.nodata is None else (sample_band != src.nodata).sum()
        
        print(f"   ✅ Loaded: {src.width:,} × {src.height:,} pixels")
        print(f"   📊 Bands: {src.count} (using first {n_bands} for visualization)")
        print(f"   🎯 Valid pixels: {valid_pixels:,}")
        
        return rgb_data, src.transform, src.crs

def load_alphaearth_tiled_data(data_dir, huc_id):
    """Load and merge tiled AlphaEarth data from GEE export."""
    # First try to load the merged file
    merged_data = load_alphaearth_merged_data(data_dir, huc_id)
    if merged_data[0] is not None:
        return merged_data
    
    # Fallback to tiled data if merged file doesn't exist
    tile_pattern = f"huc_{huc_id}_alphaearth_10m_2024_8bands-*.tif"
    tile_files = list(data_dir.glob(tile_pattern))
    
    if not tile_files:
        print(f"❌ No AlphaEarth tiles found for pattern: {tile_pattern}")
        return None, None, None
    
    print(f"🔗 Found {len(tile_files)} AlphaEarth tiles")
    
    # Parse tile information
    tiles_info = {}
    for tile_file in tile_files:
        # Extract offsets from filename: ...-rowoffset-coloffset.tif
        parts = tile_file.stem.split('-')
        row_offset = int(parts[-2])
        col_offset = int(parts[-1])
        
        with rasterio.open(tile_file) as src:
            valid_pixels = (~np.isnan(src.read(1))).sum()
            tiles_info[(row_offset, col_offset)] = {
                'file': tile_file,
                'shape': src.shape,
                'valid_pixels': valid_pixels,
                'transform': src.transform,
                'crs': src.crs,
                'bands': src.count
            }
            print(f"   • {tile_file.name}: {src.shape} pixels, {valid_pixels:,} valid")
    
    if not tiles_info:
        print("❌ No valid tiles found")
        return None, None, None
    
    # Determine grid structure
    unique_rows = sorted(set(k[0] for k in tiles_info.keys()))
    unique_cols = sorted(set(k[1] for k in tiles_info.keys()))
    print(f"   📐 Grid: {len(unique_rows)}×{len(unique_cols)} tiles")
    
    # Calculate merged dimensions
    total_height = max(unique_rows) + max(tiles_info[(r, c)]['shape'][0] for r, c in tiles_info.keys() if r == max(unique_rows))
    total_width = max(unique_cols) + max(tiles_info[(r, c)]['shape'][1] for r, c in tiles_info.keys() if c == max(unique_cols))
    
    # Get reference info from first valid tile
    ref_tile = next(iter(tiles_info.values()))
    n_bands = min(3, ref_tile['bands'])  # Use first 3 bands for RGB
    
    print(f"   🎯 Merging to {total_height}×{total_width} pixels, {n_bands} bands")
    
    # Initialize merged array
    merged_data = np.full((n_bands, total_height, total_width), np.nan, dtype=np.float32)
    
    # Merge tiles
    valid_data_found = False
    for (row_offset, col_offset), tile_info in tiles_info.items():
        if tile_info['valid_pixels'] > 0:
            print(f"   ✅ Merging tile at ({row_offset}, {col_offset})")
            valid_data_found = True
            
            with rasterio.open(tile_info['file']) as src:
                tile_data = src.read(list(range(1, n_bands + 1)))
                tile_height, tile_width = tile_data.shape[1], tile_data.shape[2]
                
                # Place tile in correct position
                merged_data[:, 
                           row_offset:row_offset + tile_height,
                           col_offset:col_offset + tile_width] = tile_data
    
    if not valid_data_found:
        print("❌ No tiles contain valid data")
        return None, None, None
    
    # Use transform from the top-left tile (0,0) for the merged raster
    ref_transform = tiles_info[(0, 0)]['transform'] if (0, 0) in tiles_info else ref_tile['transform']
    ref_crs = ref_tile['crs']
    
    print(f"   ✅ Merged AlphaEarth: {merged_data.shape}")
    return merged_data, ref_transform, ref_crs

def create_alphaearth_merged_visualization():
    """Create visualization showing the merged AlphaEarth data."""
    
    print("🎨 Creating AlphaEarth merged visualization")
    print("=" * 50)
    
    data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated")
    huc_id = "10020007"
    
    # Load merged AlphaEarth data
    rgb_data, transform, crs = load_alphaearth_tiled_data(data_dir, huc_id)
    
    if rgb_data is None:
        print("❌ Failed to load AlphaEarth data")
        return
    
    print(f"✅ Loaded merged AlphaEarth: {rgb_data.shape}")
    
    # Create RGB composite with proper normalization
    rgb_composite = np.zeros((rgb_data.shape[1], rgb_data.shape[2], 3))
    
    for i in range(3):
        band = rgb_data[i]
        valid_mask = ~np.isnan(band)
        valid_data = band[valid_mask]
        
        if len(valid_data) > 0:
            # Use percentile stretch for better contrast
            p2, p98 = np.percentile(valid_data, (2, 98))
            normalized = np.clip((band - p2) / (p98 - p2 + 1e-8), 0, 1)
            normalized[~valid_mask] = 0  # Set invalid pixels to black
            rgb_composite[:, :, i] = normalized
            print(f"  Band {i+1}: Range {p2:.3f} to {p98:.3f}, {len(valid_data):,} valid pixels")
    
    # Create visualization
    fig, axes = plt.subplots(2, 2, figsize=(16, 16))
    fig.suptitle(f'HUC {huc_id} - Merged AlphaEarth Embeddings\n'
                 f'Google Earth Engine Satellite Embeddings (2024)', 
                 fontsize=16, fontweight='bold')
    
    # Individual bands
    for i in range(3):
        ax = axes[i//2, i%2]
        band = rgb_data[i]
        valid_mask = ~np.isnan(band)
        
        # Create masked array for better visualization
        masked_band = np.ma.masked_where(~valid_mask, band)
        
        im = ax.imshow(masked_band, cmap='viridis')
        ax.set_title(f'Band {i+1} (Embedding Dimension {i+1})', fontsize=12)
        ax.axis('off')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
        cbar.set_label('Embedding Value', fontsize=10)
    
    # RGB composite
    ax = axes[1, 1]
    ax.imshow(rgb_composite)
    ax.set_title('RGB Composite (Bands 1-2-3)', fontsize=12)
    ax.axis('off')
    
    plt.tight_layout()
    
    # Save visualization
    output_file = "/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/alphaearth_merged_visualization.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✅ Saved merged AlphaEarth visualization: {output_file}")
    
    # Create comparison with individual tiles
    fig2, axes2 = plt.subplots(2, 3, figsize=(18, 12))
    fig2.suptitle(f'AlphaEarth Tile Structure - HUC {huc_id}', fontsize=16, fontweight='bold')
    
    # Show tile grid structure
    tile_files = sorted(data_dir.glob(f"huc_{huc_id}_alphaearth_10m_2024_8bands-*.tif"))
    tile_positions = [(0,0), (0,1), (1,0), (1,1)]  # Grid positions
    
    for idx, (tile_file, (grid_row, grid_col)) in enumerate(zip(tile_files, tile_positions)):
        if idx < 4:
            ax = axes2[grid_row, grid_col]
            
            # Extract offsets from filename
            parts = tile_file.stem.split('-')
            row_offset = int(parts[-2])
            col_offset = int(parts[-1])
            
            with rasterio.open(tile_file) as src:
                data = src.read(1)
                valid_pixels = (~np.isnan(data)).sum()
                
                if valid_pixels > 0:
                    # Show first band of tile with data
                    masked_data = np.ma.masked_where(np.isnan(data), data)
                    im = ax.imshow(masked_data, cmap='viridis')
                    ax.set_title(f'Tile ({row_offset}, {col_offset})\\n{valid_pixels:,} valid pixels', fontsize=10)
                    plt.colorbar(im, ax=ax, shrink=0.4)
                else:
                    ax.text(0.5, 0.5, f'Tile ({row_offset}, {col_offset})\\nNo data', 
                           ha='center', va='center', transform=ax.transAxes, fontsize=12)
                    ax.set_title(f'Tile ({row_offset}, {col_offset})', fontsize=10)
                
                ax.axis('off')
    
    # Show merged result
    axes2[1, 2].imshow(rgb_composite)
    axes2[1, 2].set_title('Merged RGB\\nComposite', fontsize=12)
    axes2[1, 2].axis('off')
    
    plt.tight_layout()
    
    # Save tile comparison
    tile_output = "/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/alphaearth_tile_structure.png"
    plt.savefig(tile_output, dpi=300, bbox_inches='tight')
    print(f"✅ Saved tile structure visualization: {tile_output}")

if __name__ == "__main__":
    create_alphaearth_merged_visualization()