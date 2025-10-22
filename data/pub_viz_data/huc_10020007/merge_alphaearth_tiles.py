#!/usr/bin/env python3
"""
Merge AlphaEarth tiled data into a single GeoTIFF file
"""

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def merge_alphaearth_tiles(data_dir, huc_id, output_path=None):
    """
    Merge AlphaEarth tiles into a single GeoTIFF file.
    
    Args:
        data_dir (Path): Directory containing the tiled data
        huc_id (str): HUC identifier (e.g., "10020007")
        output_path (Path, optional): Output path for merged file
    
    Returns:
        Path: Path to the merged file
    """
    print(f"🔗 Merging AlphaEarth tiles for HUC {huc_id}")
    print("=" * 50)
    
    # Find all AlphaEarth tiles for this HUC
    tile_pattern = f"huc_{huc_id}_alphaearth_10m_2024_8bands-*.tif"
    tile_files = sorted(data_dir.glob(tile_pattern))
    
    if not tile_files:
        print(f"❌ No AlphaEarth tiles found for pattern: {tile_pattern}")
        return None
    
    print(f"📁 Found {len(tile_files)} tiles:")
    
    # Parse tile information
    tiles_info = {}
    for tile_file in tile_files:
        # Extract row and column offsets from filename
        parts = tile_file.stem.split('-')
        row_offset = int(parts[-2])
        col_offset = int(parts[-1])
        
        with rasterio.open(tile_file) as src:
            tiles_info[(row_offset, col_offset)] = {
                'file': tile_file,
                'shape': src.shape,
                'transform': src.transform,
                'crs': src.crs,
                'bounds': src.bounds,
                'nodata': src.nodata,
                'dtype': src.dtypes[0],
                'bands': src.count
            }
            
            # Count valid pixels
            sample_band = src.read(1)
            valid_pixels = (~np.isnan(sample_band)).sum() if src.nodata is None else (sample_band != src.nodata).sum()
            tiles_info[(row_offset, col_offset)]['valid_pixels'] = valid_pixels
            
            print(f"   • Tile ({row_offset:10d}, {col_offset:10d}): {src.shape[1]:5d} × {src.shape[0]:5d} pixels, {valid_pixels:8,d} valid")
    
    # Determine the overall bounds and grid structure
    min_row = min(k[0] for k in tiles_info.keys())
    max_row = max(k[0] for k in tiles_info.keys())
    min_col = min(k[1] for k in tiles_info.keys()) 
    max_col = max(k[1] for k in tiles_info.keys())
    
    print(f"\n📐 Grid structure:")
    print(f"   Row range: {min_row:,} to {max_row:,}")
    print(f"   Col range: {min_col:,} to {max_col:,}")
    
    # Calculate total dimensions
    # Find maximum extent by looking at bottom-right tile
    max_row_tile = max(tiles_info.keys(), key=lambda x: x[0])
    max_col_tile = max(tiles_info.keys(), key=lambda x: x[1])
    
    total_height = max_row_tile[0] + tiles_info[max_row_tile]['shape'][0]
    total_width = max_col_tile[1] + tiles_info[max_col_tile]['shape'][1]
    
    print(f"   Total dimensions: {total_width:,} × {total_height:,} pixels")
    
    # Get metadata from a reference tile (preferably with data)
    ref_tile_key = max(tiles_info.keys(), key=lambda k: tiles_info[k]['valid_pixels'])
    ref_tile = tiles_info[ref_tile_key]
    n_bands = ref_tile['bands']
    dtype = ref_tile['dtype']
    crs = ref_tile['crs']
    nodata = ref_tile['nodata']
    
    print(f"   Reference tile: ({ref_tile_key[0]}, {ref_tile_key[1]}) - {ref_tile['valid_pixels']:,} valid pixels")
    print(f"   Bands: {n_bands}, dtype: {dtype}, CRS: {crs}")
    
    # Calculate the transform for the merged raster
    # Use the top-left corner from the (0,0) tile if it exists, otherwise calculate
    if (0, 0) in tiles_info:
        top_left_transform = tiles_info[(0, 0)]['transform']
        merged_transform = top_left_transform
    else:
        # Calculate transform from the minimum bounds
        all_bounds = [info['bounds'] for info in tiles_info.values()]
        min_x = min(b.left for b in all_bounds)
        max_y = max(b.top for b in all_bounds)
        pixel_size = ref_tile['transform'][0]  # Assuming square pixels
        merged_transform = rasterio.transform.from_origin(min_x, max_y, pixel_size, abs(pixel_size))
    
    print(f"   Transform: {merged_transform}")
    
    # Create output path if not provided
    if output_path is None:
        output_path = data_dir / f"huc_{huc_id}_alphaearth_10m_2024_8bands_merged.tif"
    
    # Initialize merged array
    print(f"\n🔄 Creating merged array: {n_bands} bands × {total_height:,} × {total_width:,}")
    merged_data = np.full((n_bands, total_height, total_width), np.nan if nodata is None else nodata, dtype=dtype)
    
    # Merge tiles
    total_valid_pixels = 0
    for (row_offset, col_offset), tile_info in tiles_info.items():
        if tile_info['valid_pixels'] > 0:
            print(f"   ✅ Merging tile ({row_offset:10d}, {col_offset:10d}) - {tile_info['valid_pixels']:8,d} pixels")
            
            with rasterio.open(tile_info['file']) as src:
                tile_data = src.read()  # Read all bands
                tile_height, tile_width = tile_data.shape[1], tile_data.shape[2]
                
                # Place tile data in the correct position
                merged_data[:, 
                           row_offset:row_offset + tile_height,
                           col_offset:col_offset + tile_width] = tile_data
                
                total_valid_pixels += tile_info['valid_pixels']
        else:
            print(f"   ⚠️  Skipping tile ({row_offset:10d}, {col_offset:10d}) - no valid data")
    
    print(f"\n💾 Writing merged file: {output_path}")
    print(f"   Total valid pixels: {total_valid_pixels:,}")
    
    # Write merged raster
    with rasterio.open(
        output_path,
        'w',
        driver='GTiff',
        height=total_height,
        width=total_width,
        count=n_bands,
        dtype=dtype,
        crs=crs,
        transform=merged_transform,
        nodata=nodata,
        compress='lzw',
        tiled=True,
        blockxsize=512,
        blockysize=512
    ) as dst:
        # Write band descriptions
        for i in range(n_bands):
            dst.write(merged_data[i], i + 1)
            dst.set_band_description(i + 1, f'AlphaEarth_Embedding_{i:02d}')
    
    # Verify the output
    with rasterio.open(output_path) as src:
        sample_band = src.read(1)
        final_valid = (~np.isnan(sample_band)).sum() if nodata is None else (sample_band != nodata).sum()
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        print(f"✅ Merged file created successfully!")
        print(f"   📏 Dimensions: {src.width:,} × {src.height:,} pixels")
        print(f"   📊 Bands: {src.count}")
        print(f"   🎯 Valid pixels: {final_valid:,}")
        print(f"   💽 File size: {file_size_mb:.1f} MB")
        print(f"   📍 CRS: {src.crs}")
        print(f"   🗺️  Bounds: {src.bounds}")
    
    return output_path

def main():
    """Main function to merge AlphaEarth tiles."""
    data_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/input_data/National_ML_HUC_10020007_Regenerated")
    huc_id = "10020007"
    
    merged_file = merge_alphaearth_tiles(data_dir, huc_id)
    
    if merged_file and merged_file.exists():
        print(f"\n🎉 Success! Merged AlphaEarth file ready: {merged_file}")
    else:
        print(f"\n❌ Failed to create merged file")

if __name__ == "__main__":
    main()