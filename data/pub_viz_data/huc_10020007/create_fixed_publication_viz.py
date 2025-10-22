#!/usr/bin/env python3
"""
Fixed Publication Visualization with Proper Coordinate Handling
"""

import numpy as np
import rasterio
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def create_fixed_publication_visualization():
    """Create publication visualization with proper coordinate handling."""
    
    print("🎨 Creating FIXED publication visualization for HUC 10020007")
    print("=" * 60)
    
    # Data paths
    huc_id = "10020007"
    input_data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated")
    reference_data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data")
    
    # Dataset definitions - mixed input/reference sources
    datasets = {
        'dem': {
            'path': input_data_dir / f"huc_{huc_id}_dem_10m.tif",
            'title': 'Digital Elevation Model (DEM)',
            'label': '(a)',
            'cmap': 'terrain'
        },
        'sar': {
            'path': input_data_dir / f"huc_{huc_id}_sar_10m_2023-07-01_2023-07-31_median.tif",
            'title': 'SAR (Sentinel-1)',
            'label': '(b)',
            'cmap': 'gray'
        },
        'optical': {
            'path': input_data_dir / f"huc_{huc_id}_landsat_30m_2023-07-01_2023-07-31.tif",
            'title': 'Optical (Landsat 8/9)',
            'label': '(c)',
            'cmap': None  # RGB
        },
        'thermal': {
            'path': input_data_dir / f"huc_{huc_id}_landsat_30m_2023-07-01_2023-07-31.tif",
            'title': 'Thermal (Landsat Band 10)',
            'label': '(d)',
            'cmap': 'hot'
        },
        'hydro_mask': {
            'path': reference_data_dir / f"huc_{huc_id}_hydro_mask_10m.tif",
            'title': 'Water Segmentation Mask',
            'label': '(e)',
            'cmap': None  # Custom binary
        },
        'flow_dir': {
            'path': reference_data_dir / f"huc_{huc_id}_flow_direction_10m.tif",
            'title': 'D8 Flow Direction',
            'label': '(f)',
            'cmap': None  # Custom D8
        }
    }
    
    # Create figure
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle(f'HUC {huc_id} - Multimodal Input Data and Reference Labels\\n'
                 f'Madison River Basin, Montana', 
                 fontsize=16, fontweight='bold', y=0.95)
    
    axes_flat = axes.flatten()
    
    # Process each dataset
    for idx, (key, dataset) in enumerate(datasets.items()):
        if idx >= len(axes_flat):
            break
            
        ax = axes_flat[idx]
        path = dataset['path']
        
        print(f"📈 Processing {dataset['title']}...")
        
        try:
            if not path.exists():
                ax.text(0.5, 0.5, f"Data not available\\n{dataset['title']}", 
                       transform=ax.transAxes, ha='center', va='center')
                ax.set_title(f"{dataset['label']} {dataset['title']}")
                ax.axis('off')
                continue
            
            with rasterio.open(path) as src:
                
                if key == 'dem':
                    data = src.read(1)
                    # Handle nodata
                    if src.nodata is not None:
                        data = np.ma.masked_equal(data, src.nodata)
                    else:
                        data = np.ma.masked_invalid(data)
                    
                    im = ax.imshow(data, cmap=dataset['cmap'], origin='upper')
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Elevation (m)', fontsize=9)
                
                elif key == 'sar':
                    data = src.read(1)
                    # Handle nodata and apply percentile stretch
                    if src.nodata is not None:
                        valid_mask = data != src.nodata
                    else:
                        valid_mask = ~np.isnan(data)
                    
                    if valid_mask.sum() > 0:
                        valid_data = data[valid_mask]
                        p2, p98 = np.percentile(valid_data, (2, 98))
                        data_stretched = np.clip((data - p2) / (p98 - p2), 0, 1)
                        data_stretched[~valid_mask] = 0
                    else:
                        data_stretched = data
                    
                    im = ax.imshow(data_stretched, cmap=dataset['cmap'], origin='upper')
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Backscatter', fontsize=9)
                
                elif key == 'optical':
                    # Create RGB composite from bands 4, 3, 2 (indices 3, 2, 1)
                    rgb_data = src.read([4, 3, 2])  # R, G, B
                    
                    # Normalize each band
                    rgb_composite = np.zeros((rgb_data.shape[1], rgb_data.shape[2], 3))
                    for i in range(3):
                        band = rgb_data[i]
                        valid_mask = ~np.isnan(band)
                        if valid_mask.sum() > 0:
                            valid_data = band[valid_mask]
                            p2, p98 = np.percentile(valid_data, (2, 98))
                            normalized = np.clip((band - p2) / (p98 - p2), 0, 1)
                            normalized[~valid_mask] = 0
                            rgb_composite[:, :, i] = normalized
                    
                    ax.imshow(rgb_composite, origin='upper')
                
                elif key == 'thermal':
                    # Use band 7 for thermal (Landsat thermal band)
                    thermal_data = src.read(7)
                    thermal_data = np.ma.masked_invalid(thermal_data)
                    
                    im = ax.imshow(thermal_data, cmap=dataset['cmap'], origin='upper')
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Temperature (K)', fontsize=9)
                
                elif key == 'hydro_mask':
                    data = src.read(1)
                    # Binary colormap
                    cmap = ListedColormap(['lightgray', 'blue'])
                    im = ax.imshow(data, cmap=cmap, origin='upper')
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Water', fontsize=9)
                
                elif key == 'flow_dir':
                    data = src.read(1)
                    
                    # D8 flow direction colormap
                    d8_colors = ['#808080', '#FF0000', '#FF8000', '#FFFF00', '#80FF00', 
                                '#00FF00', '#00FF80', '#00FFFF', '#0080FF', '#0000FF']
                    d8_cmap = ListedColormap(d8_colors)
                    
                    # Valid D8 values
                    valid_values = [0, 1, 2, 4, 8, 16, 32, 64, 128]
                    bounds = [-0.5] + [v + 0.5 for v in valid_values]
                    norm = BoundaryNorm(bounds, d8_cmap.N)
                    
                    im = ax.imshow(data, cmap=d8_cmap, norm=norm, origin='upper')
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Flow Direction', fontsize=9)
                
                ax.set_title(f"{dataset['label']} {dataset['title']}", fontsize=11)
                ax.axis('off')
                
        except Exception as e:
            print(f"   ⚠️  Error processing {key}: {e}")
            ax.text(0.5, 0.5, f"Error loading\\n{dataset['title']}", 
                   transform=ax.transAxes, ha='center', va='center')
            ax.set_title(f"{dataset['label']} {dataset['title']}")
            ax.axis('off')
    
    plt.tight_layout()
    
    # Save visualization
    output_png = "/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/visualizations/huc_10020007_FIXED_publication_visualization.png"
    output_pdf = output_png.replace('.png', '.pdf')
    
    plt.savefig(output_png, dpi=300, bbox_inches='tight')
    plt.savefig(output_pdf, bbox_inches='tight')
    
    print(f"\\n💾 Saved FIXED publication visualization:")
    print(f"   PNG: {output_png}")
    print(f"   PDF: {output_pdf}")
    
    return True

if __name__ == "__main__":
    create_fixed_publication_visualization()