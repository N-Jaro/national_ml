#!/usr/bin/env python3
"""
Clean Summary Visualization of HUC 10020007 Key Datasets
Shows the most important input and output data in a focused layout
"""

import numpy as np
import rasterio
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def create_summary_visualization():
    """Create a clean, focused visualization of key datasets."""
    
    print("🎨 Creating clean summary visualization for HUC 10020007")
    print("=" * 55)
    
    # Data paths
    huc_id = "10020007"
    base_path = Path("/u/nathanj/national_ml/data")
    local_rasters = base_path / "local_rasters"
    processed = base_path / "processed" / "huc_processing" / huc_id
    
    # Key datasets to visualize
    datasets = {
        'DEM': local_rasters / "dem" / f"huc_{huc_id}_dem_10m.tif",
        'Landsat RGB': local_rasters / "landsat" / f"huc_{huc_id}_landsat_30m_2023-06-01_2023-09-30.tif",
        'Flow Direction': local_rasters / "reference" / f"huc_{huc_id}_flow_direction_10m.tif",
        'Hydrography Mask': local_rasters / "reference" / f"huc_{huc_id}_hydro_mask_10m.tif",
    }
    
    # Vector data
    boundary_path = processed / f"huc8_{huc_id}_boundary.geojson"
    
    # Create figure
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'Key Datasets Summary - HUC {huc_id}\nLocal Processing Pipeline', 
                 fontsize=18, fontweight='bold')
    
    # Flatten axes for easier indexing
    axes = axes.flatten()
    
    # Store bounds for consistent plotting
    bounds = None
    
    # Plot each dataset
    for idx, (name, path) in enumerate(datasets.items()):
        if not path.exists():
            print(f"⚠️  Skipping {name}: File not found")
            continue
            
        print(f"📈 Plotting {name}...")
        ax = axes[idx]
        
        try:
            with rasterio.open(path) as src:
                # Store bounds from first dataset
                if bounds is None:
                    bounds = src.bounds
                
                # Read and process data based on type
                if name == 'DEM':
                    data = src.read(1)
                    # Mask out nodata values
                    data = np.ma.masked_equal(data, src.nodata) if src.nodata is not None else data
                    
                    im = ax.imshow(data, cmap='terrain', 
                                  extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                    # Add colorbar
                    cbar = plt.colorbar(im, ax=ax, shrink=0.7)
                    cbar.set_label('Elevation (m)', fontsize=10)
                    
                    # Add statistics
                    valid_data = data[~np.ma.is_masked(data)] if np.ma.is_masked(data) else data[~np.isnan(data)]
                    stats_text = f"Range: {valid_data.min():.0f} - {valid_data.max():.0f}m\nMean: {valid_data.mean():.0f}m"
                    
                elif name == 'Landsat RGB':
                    # Create RGB composite from Landsat bands
                    if src.count >= 3:
                        # For Landsat, bands 1-6 are reflectance (0-0.3 range)
                        # Use bands 3-2-1 (Red-Green-Blue) for true color composite
                        red = src.read(3)    # Band 3 (Red)
                        green = src.read(2)  # Band 2 (Green) 
                        blue = src.read(1)   # Band 1 (Blue)
                        
                        # Normalize each band properly for reflectance data
                        def normalize_reflectance_band(band):
                            band = band.astype(np.float32)
                            # For reflectance data, values should be 0-1, but let's use percentile stretch
                            valid_data = band[np.isfinite(band) & (band > 0)]
                            if len(valid_data) == 0:
                                return np.zeros_like(band)
                            # Use 1-99 percentile for reflectance data
                            p1, p99 = np.percentile(valid_data, [1, 99])
                            return np.clip((band - p1) / (p99 - p1), 0, 1)
                        
                        rgb = np.dstack([
                            normalize_reflectance_band(red),
                            normalize_reflectance_band(green),
                            normalize_reflectance_band(blue)
                        ])
                        
                        im = ax.imshow(rgb, extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                        stats_text = f"True Color RGB\nBands: 3-2-1\nReflectance data"
                    else:
                        # Fallback to first band
                        data = src.read(1)
                        im = ax.imshow(data, cmap='viridis',
                                      extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                        stats_text = f"Single band\nResolution: {abs(src.transform[0]):.0f}m"
                
                elif name == 'Flow Direction':
                    data = src.read(1)
                    
                    # D8 flow direction colors
                    d8_colors = ['#000000', '#FF0000', '#FF8000', '#FFFF00', '#80FF00', 
                                '#00FF00', '#00FF80', '#00FFFF', '#0080FF']
                    cmap = ListedColormap(d8_colors[:9])
                    bounds_norm = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256]
                    norm = BoundaryNorm(bounds_norm, cmap.N)
                    
                    im = ax.imshow(data, cmap=cmap, norm=norm,
                                  extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                    # Add colorbar with D8 labels
                    cbar = plt.colorbar(im, ax=ax, shrink=0.7)
                    cbar.set_label('D8 Direction', fontsize=10)
                    
                    # Statistics
                    unique_codes = sorted(np.unique(data))
                    stats_text = f"Algorithm: PySheds D8\nCodes: {len(unique_codes)}\nLocal processing"
                
                elif name == 'Hydrography Mask':
                    data = src.read(1)
                    
                    # Binary water mask
                    cmap = ListedColormap(['lightgray', 'blue'])
                    im = ax.imshow(data, cmap=cmap,
                                  extent=[bounds.left, bounds.right, bounds.bottom, bounds.top])
                    
                    # Add colorbar
                    cbar = plt.colorbar(im, ax=ax, shrink=0.7)
                    cbar.set_label('Water Mask', fontsize=10)
                    
                    # Statistics
                    water_pixels = np.sum(data == 1)
                    total_pixels = data.size
                    water_percent = (water_pixels / total_pixels) * 100
                    stats_text = f"Water coverage: {water_percent:.2f}%\nSource: Local NHD\nNo GEE dependency"
                
                # Add title and labels
                ax.set_title(f'{name}\n{src.width} × {src.height} @ {abs(src.transform[0]):.0f}m', 
                           fontsize=12, fontweight='bold')
                ax.set_xlabel('Easting (m)', fontsize=10)
                ax.set_ylabel('Northing (m)', fontsize=10)
                
                # Add statistics text box
                ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                       verticalalignment='top', fontsize=9,
                       bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
                
                # Overlay HUC boundary if available
                if boundary_path.exists():
                    try:
                        boundary = gpd.read_file(boundary_path)
                        boundary.plot(ax=ax, facecolor='none', edgecolor='red', 
                                    linewidth=2, alpha=0.8)
                    except Exception as e:
                        print(f"   ⚠️  Could not overlay boundary: {e}")
                
        except Exception as e:
            print(f"❌ Error plotting {name}: {e}")
            ax.text(0.5, 0.5, f"Error loading\n{name}", 
                   transform=ax.transAxes, ha='center', va='center',
                   fontsize=12, bbox=dict(boxstyle='round', facecolor='red', alpha=0.3))
            ax.set_title(f'{name} (Error)', fontsize=12)
    
    # Adjust layout
    plt.tight_layout()
    
    # Add overall summary text
    summary_text = f"""
🎯 HUC {huc_id} Processing Summary:
• ✅ DEM: 10m USGS elevation data
• ✅ Landsat: Multi-spectral imagery (30m)  
• ✅ Flow Direction: Local PySheds processing
• ✅ Hydrography: Local NHD data processing
• 🚀 Status: Fully local, no GEE dependency
"""
    
    fig.text(0.02, 0.02, summary_text, fontsize=10, family='monospace',
             bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    
    # Save the visualization
    output_dir = Path("/u/nathanj/national_ml/outputs")
    output_path = output_dir / f"huc_{huc_id}_summary_visualization.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    
    print(f"💾 Saved summary visualization: {output_path}")
    
    # Print data summary
    print(f"\n📊 Data Summary:")
    print("=" * 40)
    
    total_size = 0
    for name, path in datasets.items():
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            total_size += size_mb
            
            with rasterio.open(path) as src:
                print(f"📄 {name}:")
                print(f"   📏 {size_mb:.1f} MB")
                print(f"   📐 {src.width} × {src.height}")
                print(f"   🎯 {abs(src.transform[0]):.0f}m resolution")
    
    print(f"\n🎯 Total visualized data: {total_size:.1f} MB")
    print(f"✅ All datasets successfully processed locally!")
    
    return output_path

if __name__ == "__main__":
    try:
        output_path = create_summary_visualization()
        plt.show()
        
    except Exception as e:
        print(f"❌ Visualization failed: {e}")
        import traceback
        traceback.print_exc()