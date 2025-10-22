#!/usr/bin/env python3
"""
Publication-Quality Visualization for H        'alphaearth': {
            'path': 'TILED',  # Special handling for tiled AlphaEarth data
            'title': 'AlphaEarth Embeddings',
            'label': '(e)',
            'cmap': 'viridis',
            'description': 'Google Earth Engine Embeddings (2023)'
        },0007 Input Data

Creates a comprehensive figure showing all input modalities and reference data:
(a) HUC Boundary & DEM
(b) SAR Data
(c) Optical (Landsat RGB)
(d) Thermal (Landsat Band 10)
(e) AlphaEarth Embeddings
(f) Water Segmentation Mask
(g) Flow Direction (D8)

Designed for scientific publication with proper subplot labeling.
"""

import numpy as np
import rasterio
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm, Normalize
from matplotlib.patches import Rectangle
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def load_alphaearth_tiled_data(data_dir, huc_id):
    """Load AlphaEarth data - try merged file first, then fall back to tiles."""
    # First try to load the merged file
    merged_file = data_dir / f"huc_{huc_id}_alphaearth_10m_2024_8bands_merged.tif"
    
    if merged_file.exists():
        print(f"📁 Loading merged AlphaEarth file: {merged_file.name}")
        
        with rasterio.open(merged_file) as src:
            # Read first 3 bands for RGB visualization
            n_bands = min(3, src.count)
            rgb_data = src.read(list(range(1, n_bands + 1)))
            
            # Count valid pixels
            sample_band = src.read(1)
            valid_pixels = (~np.isnan(sample_band)).sum() if src.nodata is None else (sample_band != src.nodata).sum()
            
            print(f"   ✅ Loaded merged file: {src.width:,} × {src.height:,} pixels")
            print(f"   📊 Bands: {src.count} (using first {n_bands} for visualization)")
            print(f"   🎯 Valid pixels: {valid_pixels:,}")
            
            return rgb_data, src.transform, src.crs
    
    # Fallback to tiled data processing
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

def create_publication_visualization():
    """Create publication-quality visualization of all HUC 10020007 data modalities."""
    
    print("🎨 Creating publication visualization for HUC 10020007")
    print("=" * 60)
    
    # Data paths - using extracted regenerated data
    huc_id = "10020007"
    data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated")
    
    # Dataset definitions with proper file paths
    datasets = {
        'dem': {
            'path': data_dir / f"huc_{huc_id}_dem_10m.tif",
            'title': 'Digital Elevation Model (DEM)',
            'label': '(a)',
            'cmap': 'terrain',
            'description': '10m USGS Elevation'
        },
        'sar': {
            'path': data_dir / f"huc_{huc_id}_sar_10m_2023-07-01_2023-07-31_median.tif",
            'title': 'SAR (Sentinel-1)',
            'label': '(b)',
            'cmap': 'gray',
            'description': 'VV Polarization Median'
        },
        'optical': {
            'path': data_dir / f"huc_{huc_id}_landsat_30m_2023-07-01_2023-07-31.tif",
            'title': 'Optical (Landsat 8/9)',
            'label': '(c)',
            'cmap': None,  # RGB composite
            'description': 'True Color Composite'
        },
        'thermal': {
            'path': data_dir / f"huc_{huc_id}_landsat_30m_2023-07-01_2023-07-31.tif",
            'title': 'Thermal (Landsat Band 10)',
            'label': '(d)',
            'cmap': 'hot',
            'description': 'Thermal Infrared'
        },
        'alphaearth': {
            'path': data_dir / f"huc_{huc_id}_alphaearth_10m_2024_8bands_merged.tif",
            'title': 'AlphaEarth Embeddings',
            'label': '(e)',
            'cmap': 'viridis',
            'description': 'Google Earth Embeddings (2024)'
        },
        'hydro_mask': {
            'path': data_dir / f"huc_{huc_id}_hydro_mask_10m.tif",
            'title': 'Water Segmentation Mask',
            'label': '(f)',
            'cmap': ListedColormap(['lightgray', 'blue']),
            'description': 'Ground Truth Water Mask'
        },
        'flow_dir': {
            'path': data_dir / f"huc_{huc_id}_flow_direction_10m.tif",
            'title': 'D8 Flow Direction',
            'label': '(g)',
            'cmap': None,  # Custom D8 colormap
            'description': 'D8 Flow Direction'
        }
    }
    
    # Load HUC boundary
    boundary_path = data_dir / f"huc8_{huc_id}_boundary.geojson"
    
    # Create figure with subplots
    fig, axes = plt.subplots(3, 3, figsize=(18, 15))
    fig.suptitle(f'HUC {huc_id} - Multimodal Input Data and Reference Labels\n'
                 f'Madison River Basin, Montana', 
                 fontsize=16, fontweight='bold', y=0.98)
    
    # Flatten axes for easier indexing
    axes_flat = axes.flatten()
    
    # Store bounds for consistent plotting
    bounds = None
    
    # Process each dataset
    dataset_keys = list(datasets.keys())
    
    for idx, key in enumerate(dataset_keys):
        if idx >= len(axes_flat):
            break
            
        dataset = datasets[key]
        path = dataset['path']
        ax = axes_flat[idx]
        
        print(f"📈 Processing {dataset['title']}...")
        
        try:
            # Special handling for AlphaEarth data (merged or tiled)
            if key == 'alphaearth':
                if path.exists():
                    # Load AlphaEarth data directly from merged file
                    with rasterio.open(path) as src:
                        # Store bounds from first valid dataset
                        if bounds is None:
                            bounds = src.bounds
                        
                        # Read first 3 bands for RGB visualization
                        n_bands = min(3, src.count)
                        rgb_data = src.read(list(range(1, n_bands + 1)))
                        
                        # Create RGB composite from first 3 bands
                        rgb_composite = np.zeros((rgb_data.shape[1], rgb_data.shape[2], 3))
                        for i in range(3):
                            band = rgb_data[i]
                            # Mask no-data values and normalize using percentile stretch
                            valid_mask = ~np.isnan(band)
                            valid_data = band[valid_mask]
                            if len(valid_data) > 0:
                                p2, p98 = np.percentile(valid_data, (2, 98))
                                normalized = np.clip((band - p2) / (p98 - p2 + 1e-8), 0, 1)
                                normalized[~valid_mask] = 0  # Set invalid pixels to black
                                rgb_composite[:, :, i] = normalized
                        
                        # Display the RGB composite
                        ax.imshow(rgb_composite, extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                        
                        print(f"   ✅ Loaded AlphaEarth: {src.width:,} × {src.height:,} pixels, {src.count} bands")
                else:
                    # Try to load using tiled data method as fallback
                    rgb_data, alpha_transform, alpha_crs = load_alphaearth_tiled_data(data_dir, huc_id)
                    if rgb_data is not None:
                        # Create RGB composite from first 3 bands
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
                        
                        # Get bounds from one of the other datasets for consistent extent
                        if bounds is not None:
                            ax.imshow(rgb_composite, extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                        else:
                            ax.imshow(rgb_composite)
                        
                        print(f"   ✅ Loaded tiled AlphaEarth: {rgb_data.shape}")
                    else:
                        ax.text(0.5, 0.5, 'AlphaEarth\nData Missing', ha='center', va='center', 
                               transform=ax.transAxes, fontsize=12, 
                               bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
                        print(f"   ❌ AlphaEarth data not found")
                
                ax.set_title(f"{dataset['label']} {dataset['title']}", fontsize=11, fontweight='bold')
                ax.axis('off')
                
                # Add description text
                ax.text(0.02, 0.02, dataset['description'], transform=ax.transAxes,
                       verticalalignment='bottom', fontsize=9,
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
                
                # Set axis labels
                ax.set_xlabel('Easting (m)', fontsize=9)
                ax.set_ylabel('Northing (m)', fontsize=9)
                ax.tick_params(labelsize=8)
                
                continue
            
            elif not path.exists():
                print(f"⚠️  File not found: {path}")
                ax.text(0.5, 0.5, f"Data not available\n{dataset['title']}", 
                       transform=ax.transAxes, ha='center', va='center',
                       fontsize=12, bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.5))
                ax.set_title(f"{dataset['label']} {dataset['title']}", fontsize=11, fontweight='bold')
                continue
            
            with rasterio.open(path) as src:
                # Store bounds from first valid dataset
                if bounds is None:
                    bounds = src.bounds
                
                # Process data based on modality type
                if key == 'dem':
                    data = src.read(1)
                    data = np.ma.masked_equal(data, src.nodata) if src.nodata is not None else data
                    
                    im = ax.imshow(data, cmap=dataset['cmap'], 
                                  extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                    # Add colorbar
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Elevation (m)', fontsize=9)
                    
                    # Overlay HUC boundary
                    if boundary_path.exists():
                        try:
                            boundary = gpd.read_file(boundary_path)
                            boundary.plot(ax=ax, facecolor='none', edgecolor='red', 
                                        linewidth=2, alpha=0.8)
                        except Exception as e:
                            print(f"   ⚠️  Could not overlay boundary: {e}")
                
                elif key == 'sar':
                    data = src.read(1)
                    data = np.ma.masked_equal(data, src.nodata) if src.nodata is not None else data
                    
                    # Improved SAR visualization with better contrast
                    if np.ma.is_masked(data):
                        valid_data = data[~data.mask]
                    else:
                        valid_data = data[np.isfinite(data)]
                    
                    if len(valid_data) > 0:
                        # Use more aggressive percentile stretch for SAR data
                        p1, p99 = np.percentile(valid_data, [1, 99])
                        # Apply logarithmic transformation for better SAR visualization
                        data_log = np.log10(np.maximum(data - p1 + 1e-8, 1e-8))
                        p1_log, p99_log = np.percentile(data_log[np.isfinite(data_log)], [5, 95])
                        data_stretched = np.clip((data_log - p1_log) / (p99_log - p1_log + 1e-8), 0, 1)
                        
                        print(f"   📊 SAR range: {p1:.3f} to {p99:.3f}, log range: {p1_log:.3f} to {p99_log:.3f}")
                    else:
                        data_stretched = data
                    
                    im = ax.imshow(data_stretched, cmap=dataset['cmap'], 
                                  extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('SAR Backscatter (dB)', fontsize=9)
                
                elif key == 'optical':
                    # Create RGB composite - Landsat bands are typically B2,B3,B4,B5,B6,B7
                    if src.count >= 3:
                        # For Landsat surface reflectance: B2=Blue, B3=Green, B4=Red
                        blue = src.read(1)   # Band 2 (Blue)
                        green = src.read(2)  # Band 3 (Green) 
                        red = src.read(3)    # Band 4 (Red)
                        
                        def normalize_band(band):
                            band = band.astype(np.float32)
                            valid = band[np.isfinite(band) & (band > 0)]
                            if len(valid) == 0:
                                return np.zeros_like(band)
                            # Use percentile stretch for surface reflectance
                            p1, p99 = np.percentile(valid, [1, 99])
                            return np.clip((band - p1) / (p99 - p1), 0, 1)
                        
                        rgb = np.dstack([
                            normalize_band(red),
                            normalize_band(green),
                            normalize_band(blue)
                        ])
                        
                        im = ax.imshow(rgb, extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    else:
                        # Fallback to first band
                        data = src.read(1)
                        im = ax.imshow(data, cmap='viridis',
                                      extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                
                elif key == 'thermal':
                    # Extract thermal band (usually last band in Landsat data)
                    if src.count >= 6:
                        thermal_data = src.read(6)  # Typically Band 10 for thermal
                    else:
                        thermal_data = src.read(src.count)  # Use last available band
                    
                    thermal_data = np.ma.masked_equal(thermal_data, src.nodata) if src.nodata is not None else thermal_data
                    
                    im = ax.imshow(thermal_data, cmap=dataset['cmap'], 
                                  extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Temperature (K)', fontsize=9)
                

                
                elif key == 'hydro_mask':
                    data = src.read(1)
                    
                    im = ax.imshow(data, cmap=dataset['cmap'],
                                  extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Water (0=No, 1=Yes)', fontsize=9)
                    cbar.set_ticks([0, 1])
                
                elif key == 'flow_dir':
                    data = src.read(1)
                    
                    # D8 flow direction custom colormap
                    d8_colors = ['#000000', '#FF0000', '#FF8000', '#FFFF00', '#80FF00', 
                                '#00FF00', '#00FF80', '#00FFFF', '#0080FF', '#0000FF']
                    d8_cmap = ListedColormap(d8_colors)
                    
                    # D8 values: 1, 2, 4, 8, 16, 32, 64, 128
                    unique_vals = np.unique(data[data > 0])
                    if len(unique_vals) > 0:
                        bounds_norm = [0] + sorted(unique_vals.tolist()) + [max(unique_vals) + 1]
                        norm = BoundaryNorm(bounds_norm, d8_cmap.N)
                    else:
                        norm = None
                    
                    im = ax.imshow(data, cmap=d8_cmap, norm=norm,
                                  extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('D8 Direction Code', fontsize=9)
                
                # Set subplot title with label
                ax.set_title(f"{dataset['label']} {dataset['title']}", 
                           fontsize=11, fontweight='bold', pad=10)
                
                # Add description text
                ax.text(0.02, 0.02, dataset['description'], transform=ax.transAxes,
                       verticalalignment='bottom', fontsize=9,
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
                
                # Set axis labels
                ax.set_xlabel('Easting (m)', fontsize=9)
                ax.set_ylabel('Northing (m)', fontsize=9)
                ax.tick_params(labelsize=8)
                
        except Exception as e:
            print(f"❌ Error processing {dataset['title']}: {e}")
            ax.text(0.5, 0.5, f"Error loading\n{dataset['title']}\n{str(e)[:50]}...", 
                   transform=ax.transAxes, ha='center', va='center',
                   fontsize=10, bbox=dict(boxstyle='round', facecolor='red', alpha=0.3))
            ax.set_title(f"{dataset['label']} {dataset['title']} (Error)", fontsize=11)
    
    # Hide unused subplots
    for idx in range(len(dataset_keys), len(axes_flat)):
        axes_flat[idx].set_visible(False)
    
    # Add dataset information panel in the remaining space
    if len(dataset_keys) < len(axes_flat):
        info_ax = axes_flat[-1]
        info_ax.axis('off')
        
        info_text = f"""Dataset Information - HUC {huc_id}
        
Location: Madison River Basin, Montana
Time Period: July 2023 - July 2024
Coordinate System: NAD83 Conus Albers (EPSG:5070)

Data Sources:
• DEM: USGS 3DEP (10m resolution)
• SAR: Sentinel-1 VV polarization
• Optical: Landsat 8/9 surface reflectance
• Thermal: Landsat 8/9 thermal infrared
• AlphaEarth: Google Earth embeddings (2024, 8 bands)
• Water Mask: NHD Plus HR hydrography
• Flow Direction: D8 algorithm (PySheds)

Processing: Multi-modal deep learning pipeline
for hydrographic feature delineation"""
        
        info_ax.text(0.05, 0.95, info_text, transform=info_ax.transAxes,
                    verticalalignment='top', fontsize=10, family='monospace',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.8))
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    output_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/visualizations")
    output_dir.mkdir(exist_ok=True)
    
    output_path = output_dir / f"huc_{huc_id}_publication_multimodal_visualization.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    
    # Also save as PDF for publication
    output_path_pdf = output_dir / f"huc_{huc_id}_publication_multimodal_visualization.pdf"
    plt.savefig(output_path_pdf, bbox_inches='tight', facecolor='white')
    
    print(f"\n💾 Saved publication visualization:")
    print(f"   PNG: {output_path}")
    print(f"   PDF: {output_path_pdf}")
    
    # Print summary statistics
    print(f"\n📊 Dataset Summary:")
    print("=" * 50)
    
    for key, dataset in datasets.items():
        if dataset['path'].exists():
            size_mb = dataset['path'].stat().st_size / (1024 * 1024)
            try:
                with rasterio.open(dataset['path']) as src:
                    print(f"{dataset['label']} {dataset['title']}:")
                    print(f"   Size: {size_mb:.1f} MB")
                    print(f"   Dimensions: {src.width} × {src.height}")
                    print(f"   Resolution: {abs(src.transform[0]):.0f}m")
                    print(f"   Bands: {src.count}")
            except Exception as e:
                print(f"{dataset['label']} {dataset['title']}: Error reading file")
        else:
            print(f"{dataset['label']} {dataset['title']}: File not found")
    
    return output_path, output_path_pdf

if __name__ == "__main__":
    try:
        png_path, pdf_path = create_publication_visualization()
        print(f"\n✅ Publication visualization created successfully!")
        print(f"🎯 Use these files for publication figures")
        
        # Show the plot
        plt.show()
        
    except Exception as e:
        print(f"❌ Visualization failed: {e}")
        import traceback
        traceback.print_exc()