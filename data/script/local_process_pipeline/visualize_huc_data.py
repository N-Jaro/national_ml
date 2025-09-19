#!/usr/bin/env python3
"""
Comprehensive Visualization of HUC 10020007 Data
Shows all input data and generated reference data
"""

import numpy as np
import rasterio
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def visualize_huc_10020007_data():
    """Create comprehensive visualization of all HUC 10020007 data."""
    
    print("🎨 Creating comprehensive visualization for HUC 10020007")
    print("=" * 60)
    
    # Data paths
    huc_id = "10020007"
    base_path = Path("/u/nathanj/national_ml/data")
    local_rasters = base_path / "local_rasters"
    processed = base_path / "processed" / "huc_processing" / huc_id
    notebooks = base_path.parent / "notebooks" / "gee_patches_output"
    
    # Output paths for visualization
    output_dir = Path("/u/nathanj/national_ml/outputs")
    output_dir.mkdir(exist_ok=True)
    
    # Create large figure with subplots
    fig = plt.figure(figsize=(24, 18))
    fig.suptitle(f'Comprehensive Data Visualization - HUC {huc_id}', fontsize=20, fontweight='bold')
    
    # Define data sources and their paths
    data_sources = {
        # Input Data
        'DEM': local_rasters / "dem" / f"huc_{huc_id}_dem_10m.tif",
        'Landsat': local_rasters / "landsat" / f"huc_{huc_id}_landsat_30m_2023-06-01_2023-09-30.tif",
        'AlphaEarth': local_rasters / "alphaearth" / f"huc_{huc_id}_alphaearth_10m_2024_16bands.tif",
        'SAR (2023)': local_rasters / "sentinel1" / f"huc_{huc_id}_sar_10m_2023-06-01_2023-09-30_median.tif",
        'SAR (2024)': local_rasters / "sentinel1" / f"huc_{huc_id}_sar_10m_2024-06-01_2024-09-30_median.tif",
        
        # Generated Reference Data
        'Flow Direction': local_rasters / "reference" / f"huc_{huc_id}_flow_direction_10m.tif",
        'Hydro Mask': local_rasters / "reference" / f"huc_{huc_id}_hydro_mask_10m.tif",
        
        # Original GEE output for comparison
        'GEE Hydro Mask': notebooks / f"hydro_mask_{huc_id}.tif",
    }
    
    # Vector data
    vector_sources = {
        'HUC Boundary': processed / f"huc8_{huc_id}_boundary.geojson",
        'Flowlines Network': notebooks / f"flowlines_network_{huc_id}.gpkg",
        'Flowlines Non-Network': notebooks / f"flowlines_non_network_{huc_id}.gpkg",
        'Waterbodies': notebooks / f"waterbodies_{huc_id}.gpkg",
    }
    
    subplot_idx = 1
    total_plots = len([p for p in data_sources.values() if p.exists()]) + 1  # +1 for vector overlay
    
    # Calculate grid layout
    cols = 4
    rows = (total_plots + cols - 1) // cols
    
    print(f"📊 Found {len([p for p in data_sources.values() if p.exists()])} raster datasets")
    print(f"📊 Found {len([p for p in vector_sources.values() if p.exists()])} vector datasets")
    
    # Store bounds for consistent extent
    bounds = None
    
    # Plot each raster dataset
    for name, path in data_sources.items():
        if not path.exists():
            print(f"⚠️  Skipping {name}: File not found at {path}")
            continue
            
        print(f"📈 Plotting {name}...")
        
        try:
            with rasterio.open(path) as src:
                # Read data - handle different band counts
                if src.count == 1:
                    data = src.read(1)
                elif name == 'Landsat':
                    # Show RGB composite (bands 4,3,2 for Landsat)
                    if src.count >= 4:
                        r = src.read(4)  # NIR
                        g = src.read(3)  # Red  
                        b = src.read(2)  # Green
                        # Normalize for display
                        data = np.dstack([
                            np.clip(r / np.percentile(r[r > 0], 98), 0, 1),
                            np.clip(g / np.percentile(g[g > 0], 98), 0, 1),
                            np.clip(b / np.percentile(b[b > 0], 98), 0, 1)
                        ])
                    else:
                        data = src.read(1)
                elif name == 'AlphaEarth':
                    # Show first 3 bands as RGB
                    if src.count >= 3:
                        r, g, b = src.read([1, 2, 3])
                        data = np.dstack([
                            np.clip(r / np.percentile(r[r > 0], 98), 0, 1),
                            np.clip(g / np.percentile(g[g > 0], 98), 0, 1),
                            np.clip(b / np.percentile(b[b > 0], 98), 0, 1)
                        ])
                    else:
                        data = src.read(1)
                elif name.startswith('SAR'):
                    # SAR data - show first band
                    data = src.read(1)
                else:
                    data = src.read(1)
                
                # Store bounds for consistent plotting
                if bounds is None:
                    bounds = src.bounds
                
                # Create subplot
                ax = fig.add_subplot(rows, cols, subplot_idx)
                
                # Handle different data types for visualization
                if name == 'Flow Direction':
                    # Use D8 flow direction colors
                    d8_colors = ['#000000', '#FF0000', '#FF8000', '#FFFF00', 
                                '#80FF00', '#00FF00', '#00FF80', '#00FFFF', '#0080FF']
                    cmap = ListedColormap(d8_colors[:9])
                    bounds_norm = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256]
                    norm = BoundaryNorm(bounds_norm, cmap.N)
                    im = ax.imshow(data, cmap=cmap, norm=norm, extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                elif name == 'Hydro Mask' or name == 'GEE Hydro Mask':
                    # Binary mask - water in blue
                    cmap = ListedColormap(['lightgray', 'blue'])
                    im = ax.imshow(data, cmap=cmap, extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                elif name == 'DEM':
                    # Elevation with terrain colors
                    im = ax.imshow(data, cmap='terrain', extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                elif len(data.shape) == 3:
                    # RGB composite
                    im = ax.imshow(data, extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                else:
                    # Single band - use viridis
                    im = ax.imshow(data, cmap='viridis', extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                
                ax.set_title(f'{name}\n{src.width}×{src.height} @ {abs(src.transform[0]):.0f}m', 
                           fontsize=12, fontweight='bold')
                ax.set_xlabel('Easting (m)')
                ax.set_ylabel('Northing (m)')
                
                # Add colorbar for single-band data
                if len(data.shape) == 2:
                    cbar = plt.colorbar(im, ax=ax, shrink=0.6)
                    if name == 'DEM':
                        cbar.set_label('Elevation (m)')
                    elif name == 'Flow Direction':
                        cbar.set_label('D8 Direction Code')
                    elif 'Hydro' in name:
                        cbar.set_label('Water (0=Land, 1=Water)')
                    elif 'SAR' in name:
                        cbar.set_label('Backscatter (dB)')
                
                # Add data info
                if len(data.shape) == 2:
                    valid_data = data[~np.isnan(data)]
                    if len(valid_data) > 0:
                        info_text = f"Min: {valid_data.min():.2f}\nMax: {valid_data.max():.2f}\nMean: {valid_data.mean():.2f}"
                        ax.text(0.02, 0.98, info_text, transform=ax.transAxes, 
                               verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                               fontsize=8)
                
                subplot_idx += 1
                
        except Exception as e:
            print(f"❌ Error plotting {name}: {e}")
            continue
    
    # Add vector overlay plot
    if any(p.exists() for p in vector_sources.values()):
        print(f"🗺️  Creating vector overlay...")
        ax = fig.add_subplot(rows, cols, subplot_idx)
        
        # Plot base reference (DEM or first available raster)
        dem_path = local_rasters / "dem" / f"huc_{huc_id}_dem_10m.tif"
        if dem_path.exists():
            with rasterio.open(dem_path) as src:
                dem_data = src.read(1)
                ax.imshow(dem_data, cmap='gray', alpha=0.3, 
                         extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
        
        # Overlay vector data
        colors = {'HUC Boundary': 'red', 'Flowlines Network': 'blue', 
                 'Flowlines Non-Network': 'cyan', 'Waterbodies': 'darkblue'}
        
        for name, path in vector_sources.items():
            if path.exists():
                try:
                    gdf = gpd.read_file(path)
                    if not gdf.empty:
                        color = colors.get(name, 'black')
                        linewidth = 3 if 'Boundary' in name else 1
                        alpha = 0.8 if 'Boundary' in name else 0.6
                        
                        gdf.plot(ax=ax, color=color, linewidth=linewidth, alpha=alpha, label=name)
                        print(f"  ✅ Added {name}: {len(gdf)} features")
                except Exception as e:
                    print(f"  ❌ Error loading {name}: {e}")
        
        ax.set_title('Vector Data Overlay\n(HUC Boundary + Hydrography)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Easting (m)')
        ax.set_ylabel('Northing (m)')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        if bounds:
            ax.set_xlim(bounds.left, bounds.right)
            ax.set_ylim(bounds.bottom, bounds.top)
    
    # Adjust layout and save
    plt.tight_layout()
    
    # Save the visualization
    output_path = output_dir / f"huc_{huc_id}_comprehensive_visualization.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"💾 Saved comprehensive visualization: {output_path}")
    
    # Create summary statistics
    print(f"\n📊 Data Summary for HUC {huc_id}:")
    print("=" * 50)
    
    total_size_mb = 0
    for name, path in data_sources.items():
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            total_size_mb += size_mb
            
            try:
                with rasterio.open(path) as src:
                    print(f"📄 {name}:")
                    print(f"   📏 Size: {size_mb:.1f} MB")
                    print(f"   📐 Dimensions: {src.width} × {src.height}")
                    print(f"   🔢 Bands: {src.count}")
                    print(f"   📊 CRS: {src.crs}")
                    print(f"   🎯 Resolution: {abs(src.transform[0]):.1f}m")
                    
                    if src.count == 1:
                        data = src.read(1)
                        valid_data = data[~np.isnan(data)]
                        if len(valid_data) > 0:
                            print(f"   📈 Range: {valid_data.min():.2f} to {valid_data.max():.2f}")
                    print()
            except Exception as e:
                print(f"   ❌ Error reading metadata: {e}")
    
    print(f"🎯 Total Raster Data: {total_size_mb:.1f} MB")
    
    # Vector data summary
    for name, path in vector_sources.items():
        if path.exists():
            try:
                gdf = gpd.read_file(path)
                size_mb = path.stat().st_size / (1024 * 1024)
                print(f"🗺️  {name}: {len(gdf)} features ({size_mb:.2f} MB)")
            except Exception as e:
                print(f"🗺️  {name}: Error reading ({e})")
    
    print(f"\n✅ Comprehensive visualization complete!")
    print(f"🎨 Output saved to: {output_path}")
    
    return output_path

if __name__ == "__main__":
    try:
        output_path = visualize_huc_10020007_data()
        
        # Show the plot
        plt.show()
        
    except Exception as e:
        print(f"❌ Visualization failed: {e}")
        import traceback
        traceback.print_exc()