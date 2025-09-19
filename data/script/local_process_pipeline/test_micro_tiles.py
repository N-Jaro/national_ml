#!/usr/bin/env python3
"""
Micro-tile approach for very large HUC areas
Uses 2km tiles with 100m resolution to stay under GEE limits
"""

import ee
import os
import sys
import requests
import rasterio
import numpy as np
from rasterio.merge import merge
from rasterio.crs import CRS
sys.path.append('..')
from config import Settings

def download_micro_tiled_dem(huc_id='10020007', tile_size_m=2000, buffer_meters=1000):
    """Download DEM using micro-tiles (2km) with coarse resolution"""
    
    print(f"=== Micro-Tiled DEM Download for HUC {huc_id} ===")
    
    # Initialize
    settings = Settings()
    ee.Initialize(project=settings.GEE_PROJECT_ID)
    
    # Get HUC geometry
    huc8_col = ee.FeatureCollection(settings.HUC8_COL_NAME)
    huc_feature = huc8_col.filter(ee.Filter.eq('huc8', huc_id)).first()
    huc_name = huc_feature.get('name').getInfo()
    huc_geometry = huc_feature.geometry()
    huc_buffered = huc_geometry.buffer(buffer_meters)
    
    print(f"✓ Found HUC {huc_id}: {huc_name}")
    
    # Get bounds
    bounds_info = huc_buffered.bounds().getInfo()
    coords = bounds_info['coordinates'][0]
    lons = [point[0] for point in coords]
    lats = [point[1] for point in coords]
    min_x, max_x = min(lons), max(lons)
    min_y, max_y = min(lats), max(lats)
    
    width_m = max_x - min_x
    height_m = max_y - min_y
    
    print(f"  Area: {width_m/1000:.1f}km x {height_m/1000:.1f}km")
    
    # Calculate tiles
    tiles_x = int(np.ceil(width_m / tile_size_m))
    tiles_y = int(np.ceil(height_m / tile_size_m))
    total_tiles = tiles_x * tiles_y
    
    print(f"  Micro-tiles: {tiles_x} x {tiles_y} = {total_tiles} tiles of {tile_size_m/1000:.1f}km")
    
    # Limit total tiles for testing
    if total_tiles > 100:
        print(f"  ⚠️  Too many tiles ({total_tiles}), limiting to first 16 for test")
        tiles_x = min(tiles_x, 4)
        tiles_y = min(tiles_y, 4)
        total_tiles = tiles_x * tiles_y
    
    # Get DEM
    dem = ee.ImageCollection(settings.DEM_SOURCE_IMG_NAME).mosaic().select('elevation')
    
    # Download tiles
    temp_dir = '/u/nathanj/national_ml/data/local_rasters/temp_micro_tiles'
    os.makedirs(temp_dir, exist_ok=True)
    
    successful_tiles = []
    
    for tx in range(tiles_x):
        for ty in range(tiles_y):
            tile_min_x = min_x + tx * tile_size_m
            tile_max_x = min(min_x + (tx + 1) * tile_size_m, max_x)
            tile_min_y = min_y + ty * tile_size_m
            tile_max_y = min(min_y + (ty + 1) * tile_size_m, max_y)
            
            print(f"    Downloading micro-tile {tx+1},{ty+1}/{tiles_x},{tiles_y}...")
            
            try:
                # Create tile geometry
                tile_geom = ee.Geometry.Rectangle([tile_min_x, tile_min_y, tile_max_x, tile_max_y])
                
                # Clip DEM to tile
                dem_tile = dem.clip(tile_geom)
                
                # Use coarse resolution (100m) to keep file size small
                dem_reprojected = dem_tile.reproject(
                    crs='EPSG:5070',
                    scale=100  # 100m resolution to reduce file size
                )
                
                # Download
                url = dem_reprojected.getDownloadURL({
                    'format': 'GEO_TIFF',
                    'crs': 'EPSG:5070',
                    'scale': 100,
                    'region': tile_geom
                })
                
                response = requests.get(url, timeout=120)
                response.raise_for_status()
                
                tile_path = os.path.join(temp_dir, f'tile_{tx}_{ty}.tif')
                with open(tile_path, 'wb') as f:
                    f.write(response.content)
                
                print(f"      ✓ Tile {tx+1},{ty+1} downloaded ({len(response.content)} bytes)")
                successful_tiles.append(tile_path)
                
            except Exception as e:
                print(f"      ✗ Tile {tx+1},{ty+1} failed: {e}")
                continue
    
    print(f"\\n✓ Downloaded {len(successful_tiles)}/{total_tiles} tiles")
    
    if successful_tiles:
        # Merge tiles
        print("Merging tiles...")
        output_path = f'/u/nathanj/national_ml/data/local_rasters/dem/huc_{huc_id}_micro_tiled_dem.tif'
        
        # Open all successful tiles
        tile_datasets = [rasterio.open(tile_path) for tile_path in successful_tiles]
        
        # Merge them
        merged_data, merged_transform = merge(tile_datasets)
        
        # Get metadata from first tile
        merged_meta = tile_datasets[0].meta.copy()
        merged_meta.update({
            'height': merged_data.shape[1],
            'width': merged_data.shape[2],
            'transform': merged_transform
        })
        
        # Write merged DEM
        with rasterio.open(output_path, 'w', **merged_meta) as dst:
            dst.write(merged_data)
        
        # Close tile datasets
        for ds in tile_datasets:
            ds.close()
        
        print(f"✓ Merged DEM saved: {output_path}")
        
        # Cleanup temp tiles
        for tile_path in successful_tiles:
            os.remove(tile_path)
        
        # Verify merged file
        with rasterio.open(output_path) as src:
            print(f"  Final DEM: {src.shape}, CRS: {src.crs}")
        
        return output_path
    else:
        print("✗ No tiles downloaded successfully")
        return None

if __name__ == "__main__":
    result = download_micro_tiled_dem()
    if result:
        print(f"\\n🎉 Micro-tiled download successful: {result}")
    else:
        print("\\n✗ Micro-tiled download failed")
