#!/usr/bin/env python3
"""
Complete Publication Visualization with AlphaEarth
Creates 7-panel publication-quality visualization for HUC 10020007 with mixed data sources.

Input Data: Regenerated data from GEE (DEM, SAR, Optical, Thermal, AlphaEarth)
Reference Data: Original reference files (hydro_mask, flow_direction)
"""

import numpy as np
import rasterio
import matplotlib.pyplot as plt
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def load_alphaearth_tiled_data(base_path):
    """
    Load AlphaEarth data from tiled structure and merge into single array
    
    AlphaEarth exports use different tiling pattern with pixel coordinates:
    Based on the available files, we have:
    - 0000000000-0000000000 (top-left)
    - 0000000000-0000011776 (top-right) 
    - 0000011776-0000000000 (bottom-left)
    - 0000011776-0000011776 (bottom-right)
    """
    print(f"   🔄 Loading AlphaEarth tiled data from: {base_path}")
    
    tiles = {}
    # Use actual coordinate pattern from the files
    tile_coords = [
        ("0000000000", "0000000000"),  # top-left
        ("0000000000", "0000011776"),  # top-right
        ("0000011776", "0000000000"),  # bottom-left  
        ("0000011776", "0000011776")   # bottom-right
    ]
    
    # Load all tiles (avoid duplicates with " (1)")
    for row_coord, col_coord in tile_coords:
        tile_path = Path(f"{base_path}-{row_coord}-{col_coord}.tif")
        if tile_path.exists():
            print(f"      📁 Loading tile {row_coord},{col_coord}: {tile_path.name}")
            with rasterio.open(tile_path) as src:
                tiles[(row_coord, col_coord)] = src.read()  # Shape: (bands, height, width)
        else:
            print(f"      ❌ Missing tile {row_coord},{col_coord}: {tile_path.name}")
            return None
    
    if len(tiles) != 4:
        print(f"      ⚠️  Only found {len(tiles)} tiles, expected 4")
        return None
    
    # Get dimensions from first tile
    first_tile = tiles[("0000000000", "0000000000")]
    bands, tile_h, tile_w = first_tile.shape
    print(f"      📏 Tile dimensions: {bands} bands × {tile_h}h × {tile_w}w")
    
    # Create merged array
    merged = np.zeros((bands, tile_h * 2, tile_w * 2), dtype=first_tile.dtype)
    
    # Place tiles in correct positions using coordinate mapping
    coord_to_position = {
        ("0000000000", "0000000000"): (0, 0),  # top-left
        ("0000000000", "0000011776"): (0, 1),  # top-right
        ("0000011776", "0000000000"): (1, 0),  # bottom-left
        ("0000011776", "0000011776"): (1, 1)   # bottom-right
    }
    
    for (row_coord, col_coord), (row_idx, col_idx) in coord_to_position.items():
        if (row_coord, col_coord) in tiles:
            start_h = row_idx * tile_h
            end_h = start_h + tile_h  
            start_w = col_idx * tile_w
            end_w = start_w + tile_w
            merged[:, start_h:end_h, start_w:end_w] = tiles[(row_coord, col_coord)]
    
    print(f"      ✅ Merged AlphaEarth: {merged.shape} ({merged.dtype})")
    return merged

def create_complete_publication_visualization():
    """Create complete 7-panel publication visualization with AlphaEarth"""
    
    print("🎨 Creating COMPLETE publication visualization for HUC 10020007")
    print("=" * 70)
    
    # Data paths - mixed sources
    huc_id = "10020007"
    input_data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated")
    reference_data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data")
    
    # Create figure with 7 subplots (2 rows, 4 columns with last row having 3 plots)
    fig = plt.figure(figsize=(20, 10))
    
    # First row: DEM, Optical, SAR, Thermal
    ax1 = plt.subplot(2, 4, 1)  # DEM
    ax2 = plt.subplot(2, 4, 2)  # Optical 
    ax3 = plt.subplot(2, 4, 3)  # SAR
    ax4 = plt.subplot(2, 4, 4)  # Thermal
    
    # Second row: AlphaEarth, Water Mask, Flow Direction
    ax5 = plt.subplot(2, 4, 5)  # AlphaEarth
    ax6 = plt.subplot(2, 4, 6)  # Water Mask
    ax7 = plt.subplot(2, 4, 7)  # Flow Direction
    
    axes = [ax1, ax2, ax3, ax4, ax5, ax6, ax7]
    
    # Dataset definitions with mixed sources
    datasets = [
        # First row
        {
            'path': input_data_dir / f"huc_{huc_id}_dem_10m.tif",
            'title': 'Digital Elevation Model (DEM)',
            'label': '(a)',
            'cmap': 'terrain',
            'ax': ax1
        },
        {
            'path': input_data_dir / f"huc_{huc_id}_landsat_30m_2023-07-01_2023-07-31.tif",
            'title': 'Optical (Landsat 8/9)',
            'label': '(b)',
            'cmap': None,  # RGB composite
            'ax': ax2
        },
        {
            'path': input_data_dir / f"huc_{huc_id}_sar_10m_2023-07-01_2023-07-31_median.tif",
            'title': 'SAR (Sentinel-1)',
            'label': '(c)',
            'cmap': 'gray',
            'ax': ax3
        },
        {
            'path': input_data_dir / f"huc_{huc_id}_landsat_30m_2023-07-01_2023-07-31.tif",
            'title': 'Thermal (Landsat Band 10)',
            'label': '(d)',
            'cmap': 'hot',
            'band': 6,  # Band 10 (thermal) is at index 6 in Landsat stack
            'ax': ax4
        },
        # Second row
        {
            'path': None,  # Special handling for AlphaEarth
            'title': 'AlphaEarth Embeddings (16 bands)',
            'label': '(e)',
            'cmap': 'viridis',
            'ax': ax5,
            'special': 'alphaearth'
        },
        {
            'path': reference_data_dir / f"huc_{huc_id}_hydro_mask_10m.tif",
            'title': 'Water Segmentation Mask',
            'label': '(f)',
            'cmap': None,  # Custom binary
            'ax': ax6
        },
        {
            'path': reference_data_dir / f"huc_{huc_id}_flow_direction_10m.tif", 
            'title': 'D8 Flow Direction',
            'label': '(g)',
            'cmap': None,  # Custom D8
            'ax': ax7
        }
    ]
    
    # Process each dataset
    for i, dataset in enumerate(datasets):
        ax = dataset['ax']
        print(f"📈 Processing {dataset['title']}...")
        
        try:
            # Special handling for AlphaEarth
            if dataset.get('special') == 'alphaearth':
                alphaearth_base = input_data_dir / f"huc_{huc_id}_alphaearth_10m_2023_16bands"
                alphaearth_data = load_alphaearth_tiled_data(alphaearth_base)
                
                if alphaearth_data is not None:
                    # Show RGB composite of first 3 bands
                    rgb_data = alphaearth_data[:3].transpose(1, 2, 0)
                    # Normalize to 0-1 range
                    rgb_norm = (rgb_data - rgb_data.min()) / (rgb_data.max() - rgb_data.min())
                    ax.imshow(rgb_norm, origin='upper')
                else:
                    ax.text(0.5, 0.5, "AlphaEarth data\nnot available", 
                           transform=ax.transAxes, ha='center', va='center')
                
                ax.set_title(f"{dataset['label']} {dataset['title']}")
                ax.axis('off')
                continue
            
            # Regular file processing
            if not dataset['path'].exists():
                print(f"   ❌ File not found: {dataset['path']}")
                ax.text(0.5, 0.5, f"File not found\\n{dataset['title']}", 
                       transform=ax.transAxes, ha='center', va='center')
                ax.set_title(f"{dataset['label']} {dataset['title']}")
                ax.axis('off')
                continue
                
            with rasterio.open(dataset['path']) as src:
                data = src.read()
                
                # Handle different data types
                if dataset['title'] == 'Optical (Landsat 8/9)':
                    # RGB composite (bands 4,3,2 -> RGB)
                    if data.shape[0] >= 3:
                        rgb = np.stack([data[3], data[2], data[1]], axis=0)  # NIR,Red,Green -> RGB
                        rgb_norm = np.zeros_like(rgb, dtype=np.float32)
                        for i in range(3):
                            band_data = rgb[i]
                            p2, p98 = np.percentile(band_data[band_data > 0], [2, 98])
                            rgb_norm[i] = np.clip((band_data - p2) / (p98 - p2), 0, 1)
                        
                        display_data = rgb_norm.transpose(1, 2, 0)
                        ax.imshow(display_data, origin='upper')
                    else:
                        ax.text(0.5, 0.5, "Insufficient bands\\nfor RGB composite", 
                               transform=ax.transAxes, ha='center', va='center')
                
                elif dataset['title'] == 'Thermal (Landsat Band 10)':
                    # Extract thermal band (band 10 at index 6)
                    if data.shape[0] > 6:
                        thermal_data = data[6]
                        im = ax.imshow(thermal_data, cmap=dataset['cmap'], origin='upper')
                        plt.colorbar(im, ax=ax, shrink=0.8)
                    else:
                        ax.text(0.5, 0.5, "Thermal band\\nnot available", 
                               transform=ax.transAxes, ha='center', va='center')
                
                elif dataset['title'] == 'Water Segmentation Mask':
                    # Binary water mask
                    mask_data = data[0] if data.ndim == 3 else data
                    # Create custom colormap: 0=transparent, 1=blue
                    from matplotlib.colors import ListedColormap
                    colors = ['white', 'blue']
                    cmap = ListedColormap(colors)
                    im = ax.imshow(mask_data, cmap=cmap, vmin=0, vmax=1, origin='upper')
                
                elif dataset['title'] == 'D8 Flow Direction':
                    # D8 flow direction (1-8 values)
                    flow_data = data[0] if data.ndim == 3 else data
                    from matplotlib.colors import ListedColormap
                    # D8 colors: 8 directions + no data
                    d8_colors = ['black', 'red', 'orange', 'yellow', 'green', 'cyan', 'blue', 'purple', 'magenta']
                    cmap = ListedColormap(d8_colors[:9])
                    im = ax.imshow(flow_data, cmap=cmap, vmin=0, vmax=8, origin='upper')
                
                else:
                    # Standard single-band display
                    display_data = data[0] if data.ndim == 3 else data
                    im = ax.imshow(display_data, cmap=dataset['cmap'], origin='upper')
                    if dataset['cmap']:  # Add colorbar for non-RGB data
                        plt.colorbar(im, ax=ax, shrink=0.8)
                
                ax.set_title(f"{dataset['label']} {dataset['title']}")
                ax.axis('off')
                
        except Exception as e:
            print(f"   ⚠️  Error processing {dataset['title']}: {e}")
            ax.text(0.5, 0.5, f"Error loading\\n{dataset['title']}", 
                   transform=ax.transAxes, ha='center', va='center')
            ax.set_title(f"{dataset['label']} {dataset['title']}")
            ax.axis('off')
    
    plt.tight_layout()
    
    # Save visualization
    output_png = "/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/visualizations/huc_10020007_COMPLETE_publication_visualization.png"
    output_pdf = output_png.replace('.png', '.pdf')
    
    plt.savefig(output_png, dpi=300, bbox_inches='tight')
    plt.savefig(output_pdf, bbox_inches='tight')
    
    print(f"\\n💾 Saved COMPLETE publication visualization:")
    print(f"   PNG: {output_png}")
    print(f"   PDF: {output_pdf}")
    
    return True

if __name__ == "__main__":
    create_complete_publication_visualization()