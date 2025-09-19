#!/usr/bin/env python3
"""
Fix NHD CRS Issue
The NHD data appears to be in geographic coordinates, need to handle CRS properly
"""

import sys
import os
import numpy as np
import geopandas as gpd
import rasterio
from pathlib import Path
from shapely.geometry import box
from rasterio.features import geometry_mask

# Add the pipeline directory to the path
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def fix_nhd_crs_issue():
    """Fix the CRS issue with NHD data."""
    
    print("🔧 FIXING NHD CRS ISSUE")
    print("=" * 30)
    
    huc_id = "10020007"
    
    try:
        from local_config import LocalProcessingSettings
        config = LocalProcessingSettings()
        
        # Get DEM info
        dem_path = config.local_raster_base / "dem" / f"huc_{huc_id}_dem_10m.tif"
        
        with rasterio.open(dem_path) as dem_src:
            bounds = dem_src.bounds
            transform = dem_src.transform
            width = dem_src.width
            height = dem_src.height
            crs = dem_src.crs
        
        print(f"📊 DEM Info:")
        print(f"  🗺️  CRS: {crs}")
        print(f"  📍 Bounds: {bounds}")
        
        # Create bounding box in DEM CRS (EPSG:5070)
        bbox_projected = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        
        # Convert to geographic coordinates for NHD querying
        import pyproj
        transformer = pyproj.Transformer.from_crs("EPSG:5070", "EPSG:4326", always_xy=True)
        
        # Transform corners
        min_x, min_y = transformer.transform(bounds.left, bounds.bottom)
        max_x, max_y = transformer.transform(bounds.right, bounds.top)
        
        # Create geographic bounding box
        bbox_geographic = box(min_x, min_y, max_x, max_y)
        
        print(f"\n📍 Geographic bounds for NHD query:")
        print(f"  🌍 West: {min_x:.6f}, South: {min_y:.6f}")
        print(f"  🌍 East: {max_x:.6f}, North: {max_y:.6f}")
        
        gdb_path = config.nhd_gdb_path
        
        layers_to_test = [
            'NetworkNHDFlowline',
            'NonNetworkNHDFlowline', 
            'NHDWaterbody',
            'NHDArea'
        ]
        
        total_mask = np.zeros((height, width), dtype=np.uint8)
        
        for layer_name in layers_to_test:
            print(f"\n🧪 Processing {layer_name}")
            print("-" * 25)
            
            try:
                # Read layer with geographic bbox
                print(f"  📖 Reading with geographic bbox...")
                gdf = gpd.read_file(gdb_path, layer=layer_name, bbox=bbox_geographic)
                print(f"  📊 Features found: {len(gdf)}")
                
                if len(gdf) == 0:
                    print(f"  ⚠️  No features in geographic bounds")
                    continue
                
                # Check and reproject CRS
                print(f"  🗺️  Original CRS: {gdf.crs}")
                if gdf.crs != 'EPSG:5070':
                    print(f"  🔄 Reprojecting to EPSG:5070...")
                    gdf = gdf.to_crs("EPSG:5070")
                
                # Filter by projected bounds (double-check)
                print(f"  🔍 Filtering by projected bounds...")
                gdf_clipped = gdf[gdf.intersects(bbox_projected)]
                print(f"  📊 Features after clipping: {len(gdf_clipped)}")
                
                if len(gdf_clipped) == 0:
                    print(f"  ⚠️  No features after clipping to projected bounds")
                    continue
                
                # Check geometry types
                geom_types = gdf_clipped.geometry.geom_type.value_counts()
                print(f"  🔢 Geometry types: {dict(geom_types)}")
                
                # Apply processing
                if 'Flowline' in layer_name:
                    print(f"  🔧 Buffering flowlines by 15m...")
                    gdf_clipped = gdf_clipped.copy()
                    gdf_clipped['geometry'] = gdf_clipped.geometry.buffer(15.0)
                
                # Check area
                total_area = gdf_clipped.geometry.area.sum()
                print(f"  📍 Total area: {total_area/1000000:.3f} km²")
                
                # Rasterize
                print(f"  🎨 Rasterizing...")
                mask = geometry_mask(
                    gdf_clipped.geometry,
                    transform=transform,
                    invert=True,
                    out_shape=(height, width)
                )
                
                layer_pixels = np.sum(mask)
                layer_percent = (layer_pixels / mask.size) * 100
                print(f"  ✅ Rasterized: {layer_pixels:,} pixels ({layer_percent:.4f}%)")
                
                # Add to total mask
                total_mask = np.logical_or(total_mask, mask).astype(np.uint8)
                
            except Exception as e:
                print(f"  ❌ Error processing {layer_name}: {e}")
                import traceback
                traceback.print_exc()
        
        # Final result
        total_water_pixels = np.sum(total_mask)
        total_water_percent = (total_water_pixels / total_mask.size) * 100
        
        print(f"\n🎯 FINAL RESULT:")
        print(f"  💧 Total water pixels: {total_water_pixels:,}")
        print(f"  📊 Total water coverage: {total_water_percent:.4f}%")
        
        if total_water_percent > 0:
            print(f"  🎉 SUCCESS! Found water features with CRS fix!")
            
            # Save the corrected mask
            output_dir = config.local_raster_base / "reference"
            corrected_mask_path = output_dir / f"huc_{huc_id}_hydro_mask_crs_fixed.tif"
            
            with rasterio.open(dem_path) as dem_src:
                profile = dem_src.profile.copy()
                profile.update(dtype='uint8', count=1, compress='lzw')
                
                with rasterio.open(corrected_mask_path, 'w', **profile) as dst:
                    dst.write(total_mask, 1)
            
            file_size_mb = corrected_mask_path.stat().st_size / (1024 * 1024)
            print(f"  💾 Saved corrected mask: {corrected_mask_path.name} ({file_size_mb:.1f}MB)")
            
            return True
        else:
            print(f"  😞 Still no water features found even with CRS fix")
            return False
        
    except Exception as e:
        print(f"❌ CRS fix failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = fix_nhd_crs_issue()
    if success:
        print(f"\n🎊 CRS issue resolved! Hydrography mask can now be generated correctly.")
    else:
        print(f"\n🤔 Need to investigate other potential issues...")