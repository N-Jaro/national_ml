#!/usr/bin/env python3
"""
HUC-Clipped Publication Visualization
Creates publication-quality visualization for HUC 10020007 with data clipped to HUC boundary.

Uses the DEM data extent as a proxy for the HUC boundary and masks all other data accordingly.
"""

import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
import matplotlib.pyplot as plt
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def get_valid_data_mask(raster_path):
    """Get a mask of valid (non-NoData) pixels from a raster"""
    with rasterio.open(raster_path) as src:
        data = src.read(1)  # Read first band
        # Create mask where data is valid (not NoData)
        if src.nodata is not None:
            valid_mask = data != src.nodata
        else:
            # If no explicit NoData, assume 0 or very negative values are NoData
            valid_mask = (data != 0) & (data > -9999)
        return valid_mask, src.transform, src.crs

def clip_raster_to_mask(raster_path, reference_mask, reference_transform, reference_crs, target_band=None):
    """
    Clip and reproject a raster to match the reference mask
    """
    with rasterio.open(raster_path) as src:
        # Read the specified band or all bands
        if target_band is not None and src.count > target_band:
            data = src.read(target_band + 1)  # rasterio uses 1-based indexing
        else:
            data = src.read()
        
        # If different CRS, reproject first
        if src.crs != reference_crs:
            if data.ndim == 2:
                data = np.expand_dims(data, 0)
            
            # Calculate target transform and shape
            dst_transform, dst_width, dst_height = calculate_default_transform(
                src.crs, reference_crs, reference_mask.shape[1], reference_mask.shape[0],
                *src.bounds)
            
            # Reproject
            reprojected = np.zeros((data.shape[0], dst_height, dst_width), dtype=data.dtype)
            reproject(
                data, reprojected,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=dst_transform, 
                dst_crs=reference_crs,
                resampling=Resampling.bilinear
            )
            data = reprojected
        
        # Resize to match reference if needed
        if data.shape[-2:] != reference_mask.shape:
            from scipy.ndimage import zoom
            if data.ndim == 3:
                zoom_factors = (1, reference_mask.shape[0]/data.shape[1], reference_mask.shape[1]/data.shape[2])
            else:
                zoom_factors = (reference_mask.shape[0]/data.shape[0], reference_mask.shape[1]/data.shape[1])
            data = zoom(data, zoom_factors, order=1)
        
        # Apply mask
        if data.ndim == 3:
            for i in range(data.shape[0]):
                data[i][~reference_mask] = np.nan
        else:
            data[~reference_mask] = np.nan
            
        return data

def load_alphaearth_tiled_data(base_path):
    """Load AlphaEarth data from tiled structure and merge into single array"""
    print(f"   🔄 Loading AlphaEarth tiled data from: {base_path}")
    
    tiles = {}
    tile_coords = [
        ("0000000000", "0000000000"),  # top-left
        ("0000000000", "0000011776"),  # top-right
        ("0000011776", "0000000000"),  # bottom-left  
        ("0000011776", "0000011776")   # bottom-right
    ]
    
    for row_coord, col_coord in tile_coords:
        tile_path = Path(f"{base_path}-{row_coord}-{col_coord}.tif")
        if tile_path.exists():
            print(f"      📁 Loading tile {row_coord},{col_coord}: {tile_path.name}")
            with rasterio.open(tile_path) as src:
                tiles[(row_coord, col_coord)] = src.read()
        else:
            print(f"      ❌ Missing tile {row_coord},{col_coord}: {tile_path.name}")
            return None
    
    if len(tiles) != 4:
        print(f"      ⚠️  Only found {len(tiles)} tiles, expected 4")
        return None
    
    # Get dimensions and merge
    first_tile = tiles[("0000000000", "0000000000")]
    bands, tile_h, tile_w = first_tile.shape
    merged = np.zeros((bands, tile_h * 2, tile_w * 2), dtype=first_tile.dtype)
    
    coord_to_position = {
        ("0000000000", "0000000000"): (0, 0),
        ("0000000000", "0000011776"): (0, 1),
        ("0000011776", "0000000000"): (1, 0),
        ("0000011776", "0000011776"): (1, 1)
    }
    
    for (row_coord, col_coord), (row_idx, col_idx) in coord_to_position.items():
        if (row_coord, col_coord) in tiles:
            start_h = row_idx * tile_h
            end_h = start_h + tile_h  
            start_w = col_idx * tile_w
            end_w = start_w + tile_w
            merged[:, start_h:end_h, start_w:end_w] = tiles[(row_coord, col_coord)]
    
    print(f"      ✅ Merged AlphaEarth: {merged.shape}")
    return merged

def create_huc_clipped_visualization():
    """Create HUC-clipped publication visualization"""
    
    print("🎨 Creating HUC-CLIPPED publication visualization for HUC 10020007")
    print("=" * 75)
    
    # Data paths
    huc_id = "10020007"
    input_data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated")
    reference_data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data")
    
    # Use DEM as reference for HUC boundary (highest resolution, most accurate extent)
    dem_path = input_data_dir / f"huc_{huc_id}_dem_10m.tif"
    print(f"📍 Using DEM as HUC boundary reference: {dem_path.name}")
    
    # Get reference mask from DEM
    reference_mask, reference_transform, reference_crs = get_valid_data_mask(dem_path)
    print(f"   🗺️  Reference mask shape: {reference_mask.shape}")
    print(f"   🌍 Reference CRS: {reference_crs}")
    
    # Create figure
    fig = plt.figure(figsize=(20, 10))
    
    # Layout: 2 rows, 4 columns
    ax1 = plt.subplot(2, 4, 1)  # DEM
    ax2 = plt.subplot(2, 4, 2)  # Optical 
    ax3 = plt.subplot(2, 4, 3)  # SAR
    ax4 = plt.subplot(2, 4, 4)  # Thermal
    ax5 = plt.subplot(2, 4, 5)  # AlphaEarth
    ax6 = plt.subplot(2, 4, 6)  # Water Mask
    ax7 = plt.subplot(2, 4, 7)  # Flow Direction
    
    datasets = [
        {
            'path': dem_path,
            'title': 'Digital Elevation Model (DEM)',
            'label': '(a)',
            'cmap': 'terrain',
            'ax': ax1,
            'is_reference': True
        },
        {
            'path': input_data_dir / f"huc_{huc_id}_landsat_30m_2023-07-01_2023-07-31.tif",
            'title': 'Optical (Landsat 8/9)',
            'label': '(b)',
            'cmap': None,
            'ax': ax2,
            'type': 'optical'
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
            'ax': ax4,
            'type': 'thermal',
            'band': 6
        },
        {
            'path': None,
            'title': 'AlphaEarth Embeddings (16 bands)',
            'label': '(e)',
            'cmap': 'viridis',
            'ax': ax5,
            'type': 'alphaearth'
        },
        {
            'path': reference_data_dir / f"huc_{huc_id}_hydro_mask_10m.tif",
            'title': 'Water Segmentation Mask',
            'label': '(f)',
            'cmap': None,
            'ax': ax6,
            'type': 'hydro_mask'
        },
        {
            'path': reference_data_dir / f"huc_{huc_id}_flow_direction_10m.tif",
            'title': 'D8 Flow Direction',
            'label': '(g)',
            'cmap': None,
            'ax': ax7,
            'type': 'flow_dir'
        }
    ]
    
    for dataset in datasets:
        ax = dataset['ax']
        print(f"📈 Processing {dataset['title']}...")
        
        try:
            # Special handling for AlphaEarth
            if dataset.get('type') == 'alphaearth':
                alphaearth_base = input_data_dir / f"huc_{huc_id}_alphaearth_10m_2023_16bands"
                alphaearth_data = load_alphaearth_tiled_data(alphaearth_base)
                
                if alphaearth_data is not None:
                    # Clip to reference mask
                    clipped_data = clip_raster_to_mask(
                        dem_path,  # Use DEM path just for CRS reference
                        reference_mask, reference_transform, reference_crs
                    )
                    
                    # Apply mask to AlphaEarth data (assuming same spatial extent)
                    if alphaearth_data.shape[1:] == reference_mask.shape:
                        for i in range(alphaearth_data.shape[0]):
                            alphaearth_data[i][~reference_mask] = np.nan
                    
                    # Create RGB composite from first 3 bands
                    rgb_data = alphaearth_data[:3].transpose(1, 2, 0)
                    rgb_norm = np.zeros_like(rgb_data, dtype=np.float32)
                    
                    for i in range(3):
                        band_data = rgb_data[:, :, i]
                        valid_data = band_data[~np.isnan(band_data)]
                        if len(valid_data) > 0:
                            p2, p98 = np.percentile(valid_data, [2, 98])
                            rgb_norm[:, :, i] = np.clip((band_data - p2) / (p98 - p2), 0, 1)
                    
                    ax.imshow(rgb_norm, origin='upper')
                else:
                    ax.text(0.5, 0.5, "AlphaEarth data\nnot available", 
                           transform=ax.transAxes, ha='center', va='center')
                
                ax.set_title(f"{dataset['label']} {dataset['title']}")
                ax.axis('off')
                continue
            
            # Reference DEM (already has correct extent)
            if dataset.get('is_reference'):
                with rasterio.open(dataset['path']) as src:
                    data = src.read(1)
                    data[~reference_mask] = np.nan
                    im = ax.imshow(data, cmap=dataset['cmap'], origin='upper')
                    plt.colorbar(im, ax=ax, shrink=0.8)
                
                ax.set_title(f"{dataset['label']} {dataset['title']}")
                ax.axis('off')
                continue
            
            # Other datasets - clip to reference
            if not dataset['path'].exists():
                print(f"   ❌ File not found: {dataset['path']}")
                ax.text(0.5, 0.5, f"File not found\\n{dataset['title']}", 
                       transform=ax.transAxes, ha='center', va='center')
                ax.set_title(f"{dataset['label']} {dataset['title']}")
                ax.axis('off')
                continue
            
            # Clip data to reference mask
            if dataset.get('type') == 'optical':
                # RGB composite
                clipped_data = clip_raster_to_mask(
                    dataset['path'], reference_mask, reference_transform, reference_crs
                )
                
                if clipped_data.shape[0] >= 4:  # Need at least 4 bands for NIR-R-G
                    rgb = np.stack([clipped_data[3], clipped_data[2], clipped_data[1]], axis=0)
                    rgb_norm = np.zeros_like(rgb, dtype=np.float32)
                    
                    for i in range(3):
                        band_data = rgb[i]
                        valid_data = band_data[~np.isnan(band_data)]
                        if len(valid_data) > 0:
                            p2, p98 = np.percentile(valid_data, [2, 98])
                            rgb_norm[i] = np.clip((band_data - p2) / (p98 - p2), 0, 1)
                    
                    display_data = rgb_norm.transpose(1, 2, 0)
                    ax.imshow(display_data, origin='upper')
                else:
                    ax.text(0.5, 0.5, "Insufficient bands\\nfor RGB composite", 
                           transform=ax.transAxes, ha='center', va='center')
            
            elif dataset.get('type') == 'thermal':
                # Thermal band
                clipped_data = clip_raster_to_mask(
                    dataset['path'], reference_mask, reference_transform, reference_crs,
                    target_band=dataset.get('band', 6)
                )
                
                if clipped_data is not None:
                    im = ax.imshow(clipped_data, cmap=dataset['cmap'], origin='upper')
                    plt.colorbar(im, ax=ax, shrink=0.8)
                else:
                    ax.text(0.5, 0.5, "Thermal band\\nnot available", 
                           transform=ax.transAxes, ha='center', va='center')
            
            elif dataset.get('type') == 'hydro_mask':
                # Water mask
                clipped_data = clip_raster_to_mask(
                    dataset['path'], reference_mask, reference_transform, reference_crs
                )
                
                from matplotlib.colors import ListedColormap
                colors = ['white', 'blue']
                cmap = ListedColormap(colors)
                im = ax.imshow(clipped_data, cmap=cmap, vmin=0, vmax=1, origin='upper', alpha=0.8)
            
            elif dataset.get('type') == 'flow_dir':
                # Flow direction
                clipped_data = clip_raster_to_mask(
                    dataset['path'], reference_mask, reference_transform, reference_crs
                )
                
                from matplotlib.colors import ListedColormap
                d8_colors = ['black', 'red', 'orange', 'yellow', 'green', 'cyan', 'blue', 'purple', 'magenta']
                cmap = ListedColormap(d8_colors[:9])
                im = ax.imshow(clipped_data, cmap=cmap, vmin=0, vmax=8, origin='upper')
            
            else:
                # Standard single-band
                clipped_data = clip_raster_to_mask(
                    dataset['path'], reference_mask, reference_transform, reference_crs
                )
                
                im = ax.imshow(clipped_data, cmap=dataset['cmap'], origin='upper')
                if dataset['cmap']:
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
    
    # Save
    output_png = "/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/visualizations/huc_10020007_HUC_CLIPPED_publication_visualization.png"
    output_pdf = output_png.replace('.png', '.pdf')
    
    plt.savefig(output_png, dpi=300, bbox_inches='tight')
    plt.savefig(output_pdf, bbox_inches='tight')
    
    print(f"\\n💾 Saved HUC-CLIPPED publication visualization:")
    print(f"   PNG: {output_png}")
    print(f"   PDF: {output_pdf}")
    
    return True

if __name__ == "__main__":
    create_huc_clipped_visualization()