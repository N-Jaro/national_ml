#!/usr/bin/env python3
"""
Visualize the multi-band Landsat data to verify quality and inspect different spectral bands.
"""

import rasterio
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path
import os

def visualize_landsat_data(file_path):
    """Visualize the multi-band Landsat data."""
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    print(f"📊 Analyzing Landsat data: {file_path}")
    
    with rasterio.open(file_path) as src:
        print(f"📏 Shape: {src.shape}")
        print(f"🎨 Bands: {src.count}")
        print(f"📍 CRS: {src.crs}")
        print(f"🔍 Data type: {src.dtypes}")
        print(f"🌍 Bounds: {src.bounds}")
        
        # Read all bands
        bands = {}
        band_names = ['SR_B2 (Blue)', 'SR_B3 (Green)', 'SR_B4 (Red)', 
                     'SR_B5 (NIR)', 'SR_B6 (SWIR1)', 'SR_B7 (SWIR2)', 'ST_B10 (Thermal)']
        
        for i in range(1, min(src.count + 1, 8)):  # Read up to 7 bands
            band_data = src.read(i)
            band_name = band_names[i-1] if i <= len(band_names) else f'Band {i}'
            bands[band_name] = band_data
            
            # Print statistics
            valid_data = band_data[~np.isnan(band_data)]
            if len(valid_data) > 0:
                print(f"  {band_name}: min={valid_data.min():.4f}, max={valid_data.max():.4f}, "
                      f"mean={valid_data.mean():.4f}, std={valid_data.std():.4f}")
            else:
                print(f"  {band_name}: No valid data")
    
    # Create visualization
    fig, axes = plt.subplots(3, 3, figsize=(18, 15))
    fig.suptitle('Landsat 9 Multi-Band Analysis', fontsize=16, fontweight='bold')
    
    # Plot individual bands
    band_list = list(bands.items())
    for idx, (name, data) in enumerate(band_list[:7]):  # First 7 bands
        row = idx // 3
        col = idx % 3
        ax = axes[row, col]
        
        # Handle different data ranges
        if 'Thermal' in name:
            # Thermal data (Kelvin)
            vmin, vmax = 280, 320
            cmap = 'hot'
        else:
            # Reflectance data (0-1 range, but may have negative values due to scaling)
            p1, p99 = np.percentile(data[~np.isnan(data)], [1, 99])
            vmin, vmax = max(p1, -0.1), min(p99, 1.0)
            cmap = 'viridis'
        
        im = ax.imshow(data, cmap=cmap, vmin=vmin, vmax=vmax)
        ax.set_title(name, fontsize=10, fontweight='bold')
        ax.axis('off')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        if 'Thermal' in name:
            cbar.set_label('Temperature (K)', fontsize=8)
        else:
            cbar.set_label('Reflectance', fontsize=8)
    
    # Create RGB composite (Red, Green, Blue = SR_B4, SR_B3, SR_B2)
    if len(band_list) >= 3:
        row, col = 2, 1
        ax = axes[row, col]
        
        # Get RGB bands
        red = bands['SR_B4 (Red)']
        green = bands['SR_B3 (Green)']
        blue = bands['SR_B2 (Blue)']
        
        # Normalize to 0-1 range for RGB display
        def normalize_band(band):
            band = np.clip(band, 0, 0.3)  # Clip to reasonable reflectance values
            return (band - band.min()) / (band.max() - band.min() + 1e-8)
        
        rgb = np.stack([
            normalize_band(red),
            normalize_band(green),
            normalize_band(blue)
        ], axis=-1)
        
        ax.imshow(rgb)
        ax.set_title('True Color RGB (B4-B3-B2)', fontsize=10, fontweight='bold')
        ax.axis('off')
    
    # Create False Color Composite (NIR, Red, Green = SR_B5, SR_B4, SR_B3)
    if len(band_list) >= 5:
        row, col = 2, 2
        ax = axes[row, col]
        
        # Get NIR, Red, Green bands
        nir = bands['SR_B5 (NIR)']
        red = bands['SR_B4 (Red)']
        green = bands['SR_B3 (Green)']
        
        # Normalize to 0-1 range for RGB display
        false_color = np.stack([
            normalize_band(nir),
            normalize_band(red),
            normalize_band(green)
        ], axis=-1)
        
        ax.imshow(false_color)
        ax.set_title('False Color (NIR-R-G)', fontsize=10, fontweight='bold')
        ax.axis('off')
    
    # Remove empty subplot
    if len(band_list) < 9:
        axes[2, 0].remove()
    
    plt.tight_layout()
    
    # Save the plot
    output_dir = Path('/tmp')
    output_file = output_dir / 'landsat_visualization.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"📊 Visualization saved to: {output_file}")
    
    plt.show()
    
    # Create band statistics plot
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    # Calculate statistics for each band
    stats_data = []
    labels = []
    
    for name, data in band_list[:7]:
        valid_data = data[~np.isnan(data)]
        if len(valid_data) > 0:
            stats_data.append([
                np.percentile(valid_data, 5),   # 5th percentile
                np.percentile(valid_data, 25),  # 25th percentile
                np.median(valid_data),          # Median
                np.percentile(valid_data, 75),  # 75th percentile
                np.percentile(valid_data, 95)   # 95th percentile
            ])
            labels.append(name.split(' ')[0])  # Just the band name
    
    if stats_data:
        # Create box plot
        box_data = []
        for i, (name, data) in enumerate(band_list[:7]):
            valid_data = data[~np.isnan(data)]
            if len(valid_data) > 0:
                # Sample data for box plot (too much data otherwise)
                sample_size = min(10000, len(valid_data))
                sample_indices = np.random.choice(len(valid_data), sample_size, replace=False)
                box_data.append(valid_data[sample_indices])
        
        bp = ax.boxplot(box_data, labels=labels, patch_artist=True)
        
        # Customize colors
        colors = ['lightblue', 'lightgreen', 'lightcoral', 'lightsalmon', 
                 'lightpink', 'lightgray', 'lightyellow']
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
        
        ax.set_title('Band Value Distributions', fontsize=14, fontweight='bold')
        ax.set_ylabel('Value')
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        # Save the statistics plot
        stats_file = output_dir / 'landsat_statistics.png'
        plt.savefig(stats_file, dpi=300, bbox_inches='tight')
        print(f"📈 Statistics plot saved to: {stats_file}")
        
        plt.show()

def main():
    """Main function to visualize the Landsat data."""
    
    # Look for the downloaded Landsat file
    landsat_files = [
        "/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-07-01_2023-07-31.tif",
        "/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-06-01_2023-09-30.tif"
    ]
    
    file_to_visualize = None
    for file_path in landsat_files:
        if os.path.exists(file_path):
            file_to_visualize = file_path
            break
    
    if file_to_visualize:
        print(f"🎯 Found Landsat file: {file_to_visualize}")
        visualize_landsat_data(file_to_visualize)
    else:
        print("❌ No Landsat files found. Available files:")
        landsat_dir = Path("/u/nathanj/national_ml/data/local_rasters/landsat")
        if landsat_dir.exists():
            for file in landsat_dir.glob("*.tif"):
                print(f"  - {file}")
                file_to_visualize = str(file)
                break
        
        if file_to_visualize:
            print(f"🎯 Using: {file_to_visualize}")
            visualize_landsat_data(file_to_visualize)
        else:
            print("❌ No Landsat .tif files found in the directory")

if __name__ == "__main__":
    main()