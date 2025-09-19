#!/usr/bin/env python3
"""
Enhanced Landsat visualization with water body detection and spectral analysis.
"""

import rasterio
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
from scipy import ndimage

def calculate_water_indices(bands_dict):
    """Calculate various water detection indices."""
    
    # Get the required bands
    green = bands_dict.get('SR_B3 (Green)')
    red = bands_dict.get('SR_B4 (Red)')
    nir = bands_dict.get('SR_B5 (NIR)')
    swir1 = bands_dict.get('SR_B6 (SWIR1)')
    
    indices = {}
    
    if green is not None and nir is not None:
        # NDWI (Normalized Difference Water Index)
        indices['NDWI'] = (green - nir) / (green + nir + 1e-8)
    
    if nir is not None and swir1 is not None:
        # MNDWI (Modified Normalized Difference Water Index)
        indices['MNDWI'] = (green - swir1) / (green + swir1 + 1e-8)
    
    if nir is not None and red is not None:
        # NDVI (Normalized Difference Vegetation Index)
        indices['NDVI'] = (nir - red) / (nir + red + 1e-8)
    
    return indices

def detect_water_bodies(bands_dict, threshold_mndwi=0.1):
    """Detect water bodies using spectral indices."""
    
    indices = calculate_water_indices(bands_dict)
    
    water_mask = None
    if 'MNDWI' in indices:
        # Water typically has MNDWI > 0
        water_mask = indices['MNDWI'] > threshold_mndwi
        
        # Remove small isolated pixels (noise reduction)
        water_mask = ndimage.binary_opening(water_mask, structure=np.ones((3,3)))
        water_mask = ndimage.binary_closing(water_mask, structure=np.ones((5,5)))
    
    return water_mask, indices

def enhanced_visualization(file_path):
    """Create enhanced visualizations with water detection."""
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    print(f"🔍 Enhanced analysis of: {file_path}")
    
    with rasterio.open(file_path) as src:
        # Read all bands
        bands = {}
        band_names = ['SR_B2 (Blue)', 'SR_B3 (Green)', 'SR_B4 (Red)', 
                     'SR_B5 (NIR)', 'SR_B6 (SWIR1)', 'SR_B7 (SWIR2)', 'ST_B10 (Thermal)']
        
        for i in range(1, min(src.count + 1, 8)):
            band_data = src.read(i)
            band_name = band_names[i-1] if i <= len(band_names) else f'Band {i}'
            bands[band_name] = band_data
    
    # Calculate water indices and detect water bodies
    water_mask, indices = detect_water_bodies(bands)
    
    # Create comprehensive visualization
    fig = plt.figure(figsize=(20, 16))
    gs = fig.add_gridspec(4, 5, hspace=0.3, wspace=0.3)
    
    # Row 1: Individual bands (first 5)
    band_list = list(bands.items())
    for idx in range(min(5, len(band_list))):
        name, data = band_list[idx]
        ax = fig.add_subplot(gs[0, idx])
        
        if 'Thermal' in name:
            vmin, vmax = 280, 320
            cmap = 'hot'
        else:
            p1, p99 = np.percentile(data[~np.isnan(data)], [1, 99])
            vmin, vmax = max(p1, -0.1), min(p99, 1.0)
            cmap = 'viridis'
        
        im = ax.imshow(data, cmap=cmap, vmin=vmin, vmax=vmax)
        ax.set_title(name, fontsize=9)
        ax.axis('off')
    
    # Row 2: Remaining bands and RGB composite
    for idx in range(2):  # SWIR2 and Thermal
        if idx + 5 < len(band_list):
            name, data = band_list[idx + 5]
            ax = fig.add_subplot(gs[1, idx])
            
            if 'Thermal' in name:
                vmin, vmax = 280, 320
                cmap = 'hot'
            else:
                p1, p99 = np.percentile(data[~np.isnan(data)], [1, 99])
                vmin, vmax = max(p1, -0.1), min(p99, 1.0)
                cmap = 'viridis'
            
            im = ax.imshow(data, cmap=cmap, vmin=vmin, vmax=vmax)
            ax.set_title(name, fontsize=9)
            ax.axis('off')
    
    # True Color RGB
    ax = fig.add_subplot(gs[1, 2])
    if len(band_list) >= 3:
        red = bands['SR_B4 (Red)']
        green = bands['SR_B3 (Green)']
        blue = bands['SR_B2 (Blue)']
        
        def normalize_band(band):
            band = np.clip(band, 0, 0.3)
            return (band - band.min()) / (band.max() - band.min() + 1e-8)
        
        rgb = np.stack([
            normalize_band(red),
            normalize_band(green),
            normalize_band(blue)
        ], axis=-1)
        
        ax.imshow(rgb)
        ax.set_title('True Color RGB', fontsize=9)
        ax.axis('off')
    
    # False Color Composite
    ax = fig.add_subplot(gs[1, 3])
    if len(band_list) >= 5:
        nir = bands['SR_B5 (NIR)']
        red = bands['SR_B4 (Red)']
        green = bands['SR_B3 (Green)']
        
        false_color = np.stack([
            normalize_band(nir),
            normalize_band(red),
            normalize_band(green)
        ], axis=-1)
        
        ax.imshow(false_color)
        ax.set_title('False Color (NIR-R-G)', fontsize=9)
        ax.axis('off')
    
    # SWIR Composite
    ax = fig.add_subplot(gs[1, 4])
    if 'SR_B6 (SWIR1)' in bands and 'SR_B5 (NIR)' in bands and 'SR_B4 (Red)' in bands:
        swir1 = bands['SR_B6 (SWIR1)']
        nir = bands['SR_B5 (NIR)']
        red = bands['SR_B4 (Red)']
        
        swir_composite = np.stack([
            normalize_band(swir1),
            normalize_band(nir),
            normalize_band(red)
        ], axis=-1)
        
        ax.imshow(swir_composite)
        ax.set_title('SWIR Composite', fontsize=9)
        ax.axis('off')
    
    # Row 3: Spectral indices
    index_names = ['NDWI', 'MNDWI', 'NDVI']
    for idx, index_name in enumerate(index_names):
        if index_name in indices:
            ax = fig.add_subplot(gs[2, idx])
            data = indices[index_name]
            
            if index_name in ['NDWI', 'MNDWI']:
                vmin, vmax = -1, 1
                cmap = 'RdYlBu_r'
            else:  # NDVI
                vmin, vmax = -1, 1
                cmap = 'RdYlGn'
            
            im = ax.imshow(data, cmap=cmap, vmin=vmin, vmax=vmax)
            ax.set_title(f'{index_name}', fontsize=9)
            ax.axis('off')
            plt.colorbar(im, ax=ax, shrink=0.8)
    
    # Water detection
    if water_mask is not None:
        ax = fig.add_subplot(gs[2, 3])
        
        # Create RGB background
        if len(band_list) >= 3:
            rgb_bg = rgb.copy()
            rgb_bg = (rgb_bg * 0.7 + 0.3)  # Lighten background
            
            # Overlay water mask in blue
            water_overlay = rgb_bg.copy()
            water_overlay[water_mask] = [0, 0.5, 1]  # Blue for water
            
            ax.imshow(water_overlay)
            ax.set_title('Water Detection', fontsize=9)
            ax.axis('off')
            
            # Calculate water statistics
            total_pixels = water_mask.size
            water_pixels = np.sum(water_mask)
            water_percentage = (water_pixels / total_pixels) * 100
            
            ax.text(0.02, 0.98, f'Water: {water_percentage:.1f}%', 
                   transform=ax.transAxes, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                   fontsize=8)
    
    # Thermal analysis
    ax = fig.add_subplot(gs[2, 4])
    if 'ST_B10 (Thermal)' in bands:
        thermal = bands['ST_B10 (Thermal)']
        thermal_valid = thermal[thermal > 0]  # Remove invalid values
        
        if len(thermal_valid) > 0:
            # Convert to Celsius for display
            thermal_celsius = thermal_valid - 273.15
            
            im = ax.imshow(thermal - 273.15, cmap='hot', vmin=0, vmax=50)
            ax.set_title('Temperature (°C)', fontsize=9)
            ax.axis('off')
            plt.colorbar(im, ax=ax, shrink=0.8)
            
            # Add temperature statistics
            ax.text(0.02, 0.98, f'Avg: {thermal_celsius.mean():.1f}°C', 
                   transform=ax.transAxes, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                   fontsize=8)
    
    # Row 4: Statistical analysis
    ax = fig.add_subplot(gs[3, :])
    
    # Create spectral profile plot
    if len(band_list) >= 6:
        # Sample random points for spectral analysis
        n_samples = 1000
        height, width = band_list[0][1].shape
        sample_rows = np.random.randint(0, height, n_samples)
        sample_cols = np.random.randint(0, width, n_samples)
        
        # Extract spectral profiles (exclude thermal for now)
        spectral_bands = ['SR_B2 (Blue)', 'SR_B3 (Green)', 'SR_B4 (Red)', 
                         'SR_B5 (NIR)', 'SR_B6 (SWIR1)', 'SR_B7 (SWIR2)']
        
        band_centers = [482, 561, 655, 865, 1610, 2200]  # Landsat 9 band centers (nm)
        
        profiles = []
        for band_name in spectral_bands:
            if band_name in bands:
                band_data = bands[band_name]
                profile = band_data[sample_rows, sample_cols]
                profiles.append(profile)
        
        profiles = np.array(profiles)
        
        # Plot mean spectral profile with std
        mean_profile = np.mean(profiles, axis=1)
        std_profile = np.std(profiles, axis=1)
        
        ax.plot(band_centers, mean_profile, 'b-', linewidth=2, label='Mean Reflectance')
        ax.fill_between(band_centers, mean_profile - std_profile, mean_profile + std_profile, 
                       alpha=0.3, color='blue', label='±1 Std Dev')
        
        # Identify water and vegetation pixels for comparison
        if 'MNDWI' in indices and 'NDVI' in indices:
            mndwi_samples = indices['MNDWI'][sample_rows, sample_cols]
            ndvi_samples = indices['NDVI'][sample_rows, sample_cols]
            
            # Water pixels (high MNDWI, low NDVI)
            water_mask_samples = (mndwi_samples > 0.1) & (ndvi_samples < 0.2)
            if np.sum(water_mask_samples) > 10:
                water_profile = np.mean(profiles[:, water_mask_samples], axis=1)
                ax.plot(band_centers, water_profile, 'c-', linewidth=2, label='Water Pixels')
            
            # Vegetation pixels (high NDVI)
            veg_mask_samples = ndvi_samples > 0.5
            if np.sum(veg_mask_samples) > 10:
                veg_profile = np.mean(profiles[:, veg_mask_samples], axis=1)
                ax.plot(band_centers, veg_profile, 'g-', linewidth=2, label='Vegetation Pixels')
        
        ax.set_xlabel('Wavelength (nm)')
        ax.set_ylabel('Reflectance')
        ax.set_title('Spectral Profiles')
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    plt.suptitle('Enhanced Landsat 9 Analysis', fontsize=16, fontweight='bold')
    
    # Save the enhanced visualization
    output_file = Path('/tmp/landsat_enhanced_analysis.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"🎨 Enhanced visualization saved to: {output_file}")
    
    plt.show()
    
    # Print summary statistics
    print("\n📊 Analysis Summary:")
    print(f"   Image dimensions: {band_list[0][1].shape}")
    print(f"   Total area: {(band_list[0][1].shape[0] * band_list[0][1].shape[1] * 30 * 30) / 1e6:.2f} km²")
    
    if water_mask is not None:
        water_area_km2 = (np.sum(water_mask) * 30 * 30) / 1e6
        total_area_km2 = (water_mask.size * 30 * 30) / 1e6
        print(f"   Water coverage: {water_area_km2:.2f} km² ({(water_area_km2/total_area_km2)*100:.1f}%)")
    
    if 'ST_B10 (Thermal)' in bands:
        thermal = bands['ST_B10 (Thermal)']
        thermal_valid = thermal[thermal > 0]
        if len(thermal_valid) > 0:
            temp_celsius = thermal_valid - 273.15
            print(f"   Temperature range: {temp_celsius.min():.1f}°C to {temp_celsius.max():.1f}°C")
            print(f"   Average temperature: {temp_celsius.mean():.1f}°C")

def main():
    """Main function for enhanced visualization."""
    
    # Look for the downloaded Landsat file
    landsat_files = [
        "/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-07-01_2023-07-31.tif",
        "/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-06-01_2023-09-30.tif"
    ]
    
    file_to_analyze = None
    for file_path in landsat_files:
        if os.path.exists(file_path):
            file_to_analyze = file_path
            break
    
    if file_to_analyze:
        enhanced_visualization(file_to_analyze)
    else:
        print("❌ No Landsat files found for enhanced analysis")

if __name__ == "__main__":
    main()