#!/usr/bin/env python3
"""
Data quality validation and summary report for the Landsat pipeline.
"""

import rasterio
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
from datetime import datetime

def create_data_quality_report(file_path):
    """Create a comprehensive data quality report."""
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    print(f"📋 Creating data quality report for: {file_path}")
    
    with rasterio.open(file_path) as src:
        # Basic metadata
        metadata = {
            'file_path': file_path,
            'shape': src.shape,
            'bands': src.count,
            'crs': str(src.crs),
            'bounds': src.bounds,
            'transform': src.transform,
            'dtype': src.dtypes[0] if src.dtypes else None,
            'nodata': src.nodata
        }
        
        # Read all bands
        bands = src.read()
        band_names = ['SR_B2 (Blue)', 'SR_B3 (Green)', 'SR_B4 (Red)', 
                     'SR_B5 (NIR)', 'SR_B6 (SWIR1)', 'SR_B7 (SWIR2)', 'ST_B10 (Thermal)']
    
    # Calculate band statistics
    band_stats = {}
    for i, band_name in enumerate(band_names[:bands.shape[0]]):
        band_data = bands[i]
        valid_data = band_data[~np.isnan(band_data)]
        
        if len(valid_data) > 0:
            band_stats[band_name] = {
                'min': float(valid_data.min()),
                'max': float(valid_data.max()),
                'mean': float(valid_data.mean()),
                'std': float(valid_data.std()),
                'valid_pixels': len(valid_data),
                'total_pixels': band_data.size,
                'fill_percentage': (len(valid_data) / band_data.size) * 100
            }
        else:
            band_stats[band_name] = {
                'min': np.nan, 'max': np.nan, 'mean': np.nan, 'std': np.nan,
                'valid_pixels': 0, 'total_pixels': band_data.size, 'fill_percentage': 0
            }
    
    # Create the quality report visualization
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(3, 4, height_ratios=[1, 1, 1.5], hspace=0.4, wspace=0.3)
    
    # Title and metadata
    fig.suptitle('Landsat 9 Data Quality Report', fontsize=18, fontweight='bold', y=0.95)
    
    # File information
    ax_info = fig.add_subplot(gs[0, :2])
    ax_info.axis('off')
    
    info_text = f"""
File: {os.path.basename(file_path)}
Dimensions: {metadata['shape'][0]:,} × {metadata['shape'][1]:,} pixels
Bands: {metadata['bands']}
CRS: {metadata['crs']}
Data Type: {metadata['dtype']}
Pixel Size: 30m × 30m
Total Area: {(metadata['shape'][0] * metadata['shape'][1] * 30 * 30) / 1e6:.1f} km²
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    ax_info.text(0.05, 0.95, info_text, transform=ax_info.transAxes, fontsize=10,
                verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
    
    # Band statistics table
    ax_stats = fig.add_subplot(gs[0, 2:])
    ax_stats.axis('off')
    
    # Create table data
    table_data = []
    headers = ['Band', 'Min', 'Max', 'Mean', 'Std', 'Fill %']
    
    for band_name, stats in band_stats.items():
        short_name = band_name.split(' ')[0]
        if 'Thermal' in band_name:
            # Convert thermal to Celsius for display
            min_val = stats['min'] - 273.15 if not np.isnan(stats['min']) else np.nan
            max_val = stats['max'] - 273.15 if not np.isnan(stats['max']) else np.nan
            mean_val = stats['mean'] - 273.15 if not np.isnan(stats['mean']) else np.nan
            std_val = stats['std'] if not np.isnan(stats['std']) else np.nan
            row = [short_name, f"{min_val:.1f}°C", f"{max_val:.1f}°C", 
                   f"{mean_val:.1f}°C", f"{std_val:.1f}°C", f"{stats['fill_percentage']:.1f}%"]
        else:
            row = [short_name, f"{stats['min']:.3f}", f"{stats['max']:.3f}", 
                   f"{stats['mean']:.3f}", f"{stats['std']:.3f}", f"{stats['fill_percentage']:.1f}%"]
        table_data.append(row)
    
    # Create table
    table = ax_stats.table(cellText=table_data, colLabels=headers, 
                          cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)
    
    # Style the table
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('#4CAF50')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Quick RGB preview
    ax_rgb = fig.add_subplot(gs[1, 0])
    if bands.shape[0] >= 3:
        red = bands[2]   # SR_B4
        green = bands[1] # SR_B3 
        blue = bands[0]  # SR_B2
        
        def normalize_band(band):
            band = np.clip(band, 0, 0.3)
            return (band - band.min()) / (band.max() - band.min() + 1e-8)
        
        # Downsample for quick display
        step = max(1, bands.shape[1] // 500)
        rgb_small = np.stack([
            normalize_band(red[::step, ::step]),
            normalize_band(green[::step, ::step]),
            normalize_band(blue[::step, ::step])
        ], axis=-1)
        
        ax_rgb.imshow(rgb_small)
        ax_rgb.set_title('True Color Preview', fontweight='bold')
        ax_rgb.axis('off')
    
    # Band value distributions
    ax_hist = fig.add_subplot(gs[1, 1:])
    
    colors = ['blue', 'green', 'red', 'darkred', 'brown', 'gray', 'orange']
    
    for i, (band_name, stats) in enumerate(band_stats.items()):
        if stats['valid_pixels'] > 0:
            band_data = bands[i]
            valid_data = band_data[~np.isnan(band_data)]
            
            # Sample data for histogram (performance)
            sample_size = min(50000, len(valid_data))
            if sample_size < len(valid_data):
                sample_idx = np.random.choice(len(valid_data), sample_size, replace=False)
                hist_data = valid_data[sample_idx]
            else:
                hist_data = valid_data
            
            # Thermal band gets different treatment
            if 'Thermal' in band_name:
                hist_data = hist_data - 273.15  # Convert to Celsius
                range_vals = (-20, 60)
            else:
                range_vals = (-0.2, 1.0)
            
            ax_hist.hist(hist_data, bins=50, alpha=0.6, 
                        label=band_name.split(' ')[0], 
                        color=colors[i % len(colors)],
                        range=range_vals, density=True)
    
    ax_hist.set_xlabel('Value')
    ax_hist.set_ylabel('Density')
    ax_hist.set_title('Band Value Distributions', fontweight='bold')
    ax_hist.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax_hist.grid(True, alpha=0.3)
    
    # Data quality indicators
    ax_quality = fig.add_subplot(gs[2, :])
    
    # Calculate quality metrics
    quality_metrics = {
        '✅ Multi-band data': bands.shape[0] == 7,
        '✅ Correct data types': all(isinstance(stats['mean'], float) and not np.isnan(stats['mean']) 
                                   for stats in band_stats.values()),
        '✅ Reasonable value ranges': all(
            (stats['min'] >= -0.3 and stats['max'] <= 1.2) if 'Thermal' not in name
            else (stats['min'] >= 200 and stats['max'] <= 400)  # Kelvin range
            for name, stats in band_stats.items()
        ),
        '✅ Good data coverage': all(stats['fill_percentage'] > 95 for stats in band_stats.values()),
        '✅ Spatial consistency': metadata['shape'][0] > 1000 and metadata['shape'][1] > 1000,
        '✅ Correct projection': 'EPSG:5070' in metadata['crs']  # Albers Equal Area
    }
    
    # Display quality checklist
    quality_text = "Data Quality Checklist:\n\n"
    for check, passed in quality_metrics.items():
        status = "PASS" if passed else "FAIL"
        quality_text += f"{check}: {status}\n"
    
    # Add some statistics
    total_pixels = metadata['shape'][0] * metadata['shape'][1]
    quality_text += f"\nSummary Statistics:\n"
    quality_text += f"• Total pixels: {total_pixels:,}\n"
    quality_text += f"• File size: {os.path.getsize(file_path) / 1e6:.1f} MB\n"
    quality_text += f"• Average fill rate: {np.mean([stats['fill_percentage'] for stats in band_stats.values()]):.1f}%\n"
    
    # Check for water and vegetation
    if bands.shape[0] >= 5:
        green = bands[1]
        nir = bands[3]
        swir1 = bands[4]
        
        mndwi = (green - swir1) / (green + swir1 + 1e-8)
        ndvi = (nir - bands[2]) / (nir + bands[2] + 1e-8)
        
        water_coverage = np.sum(mndwi > 0.1) / total_pixels * 100
        veg_coverage = np.sum(ndvi > 0.5) / total_pixels * 100
        
        quality_text += f"• Water coverage: {water_coverage:.1f}%\n"
        quality_text += f"• Vegetation coverage: {veg_coverage:.1f}%\n"
    
    ax_quality.text(0.05, 0.95, quality_text, transform=ax_quality.transAxes, 
                   fontsize=11, verticalalignment='top', fontfamily='monospace',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.8))
    ax_quality.axis('off')
    
    # Overall quality score
    quality_score = sum(quality_metrics.values()) / len(quality_metrics) * 100
    score_color = 'green' if quality_score >= 80 else 'orange' if quality_score >= 60 else 'red'
    
    ax_quality.text(0.7, 0.8, f"Overall Quality Score\n{quality_score:.0f}%", 
                   transform=ax_quality.transAxes, fontsize=16, fontweight='bold',
                   horizontalalignment='center', verticalalignment='center',
                   bbox=dict(boxstyle='round,pad=0.8', facecolor=score_color, alpha=0.7))
    
    # Save the quality report
    output_file = Path('/tmp/landsat_quality_report.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"📋 Quality report saved to: {output_file}")
    
    plt.tight_layout()
    plt.show()
    
    # Print summary to console
    print(f"\n📊 LANDSAT 9 DATA QUALITY SUMMARY")
    print(f"=" * 50)
    print(f"File: {os.path.basename(file_path)}")
    print(f"Overall Quality Score: {quality_score:.0f}%")
    print(f"Dimensions: {metadata['shape'][0]:,} × {metadata['shape'][1]:,}")
    print(f"Bands: {metadata['bands']}")
    print(f"Total Area: {(metadata['shape'][0] * metadata['shape'][1] * 30 * 30) / 1e6:.1f} km²")
    print(f"File Size: {os.path.getsize(file_path) / 1e6:.1f} MB")
    
    print(f"\nPipeline Validation:")
    for check, passed in quality_metrics.items():
        print(f"  {check}: {'✅ PASS' if passed else '❌ FAIL'}")
    
    return quality_score >= 80

def main():
    """Main function for data quality validation."""
    
    # Look for the downloaded Landsat file
    landsat_files = [
        "/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-07-01_2023-07-31.tif",
        "/u/nathanj/national_ml/data/local_rasters/landsat/huc_10020007_landsat_30m_2023-06-01_2023-09-30.tif"
    ]
    
    file_to_validate = None
    for file_path in landsat_files:
        if os.path.exists(file_path):
            file_to_validate = file_path
            break
    
    if file_to_validate:
        is_valid = create_data_quality_report(file_to_validate)
        
        if is_valid:
            print(f"\n🎉 SUCCESS: Landsat pipeline is working correctly!")
            print(f"   Multi-band data successfully downloaded and processed.")
            print(f"   Ready for machine learning workflows.")
        else:
            print(f"\n⚠️ WARNING: Data quality issues detected.")
            print(f"   Please review the quality report for details.")
    else:
        print("❌ No Landsat files found for validation")

if __name__ == "__main__":
    main()