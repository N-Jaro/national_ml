#!/usr/bin/env python3
"""
Targeted Debug for NHD GDB Layers
Test the actual layer names found in the GDB
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

def test_actual_nhd_layers():
    """Test the actual NHD layers found in the GDB."""
    
    print("🔍 TESTING ACTUAL NHD LAYERS")
    print("=" * 40)
    
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
        
        # Create bounding box
        bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        
        print(f"📊 HUC {huc_id} bounds: {bounds}")
        print(f"📐 Grid: {width} × {height}")
        
        gdb_path = config.nhd_gdb_path
        
        # Test the actual layers we found:
        # Layer: NetworkNHDFlowline (3D Measured Multi Line String)
        # Layer: NonNetworkNHDFlowline (3D Measured Multi Line String) 
        # Layer: NHDWaterbody (3D Multi Polygon)
        # Layer: NHDArea (3D Multi Polygon)
        
        layers_to_test = [
            'NetworkNHDFlowline',
            'NonNetworkNHDFlowline', 
            'NHDWaterbody',
            'NHDArea'
        ]
        
        total_mask = np.zeros((height, width), dtype=np.uint8)
        
        for layer_name in layers_to_test:
            print(f"\n🧪 Testing {layer_name}")
            print("-" * 30)
            
            try:
                # Read layer with bbox filter
                print(f"  📖 Reading with bbox filter...")
                gdf = gpd.read_file(gdb_path, layer=layer_name, bbox=bbox)
                print(f"  📊 Features found: {len(gdf)}")
                
                if len(gdf) == 0:
                    print(f"  ⚠️  No features in HUC bounds")
                    continue
                
                # Check CRS
                print(f"  🗺️  Original CRS: {gdf.crs}")
                if gdf.crs != 'EPSG:5070':
                    print(f"  🔄 Reprojecting to EPSG:5070...")
                    gdf = gdf.to_crs("EPSG:5070")
                
                # Check geometry types
                geom_types = gdf.geometry.geom_type.value_counts()
                print(f"  🔢 Geometry types: {dict(geom_types)}")
                
                # Apply processing based on layer type
                if 'Flowline' in layer_name:
                    print(f"  🔧 Applying 15m buffer to flowlines...")
                    gdf['geometry'] = gdf.geometry.buffer(15.0)
                    
                    # Check total length
                    total_length = gdf.length.sum()
                    print(f"  📏 Total length: {total_length/1000:.1f} km")
                
                # Check total area
                total_area = gdf.geometry.area.sum()
                print(f"  📍 Total area: {total_area/1000000:.2f} km²")
                
                # Rasterize
                print(f"  🎨 Rasterizing...")
                try:
                    mask = geometry_mask(
                        gdf.geometry,
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
                    print(f"  ❌ Rasterization failed: {e}")
            
            except Exception as e:
                print(f"  ❌ Error processing {layer_name}: {e}")
        
        # Final combined result
        total_water_pixels = np.sum(total_mask)
        total_water_percent = (total_water_pixels / total_mask.size) * 100
        
        print(f"\n🎯 COMBINED RESULT:")
        print(f"  💧 Total water pixels: {total_water_pixels:,}")
        print(f"  📊 Total water coverage: {total_water_percent:.4f}%")
        
        if total_water_percent > 0:
            print(f"  ✅ SUCCESS: Found water features!")
            
            # Save the corrected mask
            output_dir = config.local_raster_base / "reference"
            corrected_mask_path = output_dir / f"huc_{huc_id}_hydro_mask_corrected.tif"
            
            with rasterio.open(dem_path) as dem_src:
                profile = dem_src.profile.copy()
                profile.update(dtype='uint8', count=1, compress='lzw')
                
                with rasterio.open(corrected_mask_path, 'w', **profile) as dst:
                    dst.write(total_mask, 1)
            
            print(f"  💾 Saved corrected mask: {corrected_mask_path.name}")
            
        else:
            print(f"  ⚠️  Still no water features found")
            
            # Let's try without spatial filtering
            print(f"\n🔍 Testing without bbox filter...")
            for layer_name in ['NetworkNHDFlowline', 'NHDWaterbody']:
                try:
                    print(f"  📖 Reading {layer_name} (no bbox)...")
                    gdf_all = gpd.read_file(gdb_path, layer=layer_name)
                    
                    print(f"  📊 Total features in layer: {len(gdf_all)}")
                    if len(gdf_all) > 0:
                        layer_bounds = gdf_all.total_bounds
                        print(f"  📍 Layer bounds: {layer_bounds}")
                        
                        # Check if HUC bounds intersect with layer
                        layer_bbox = box(layer_bounds[0], layer_bounds[1], layer_bounds[2], layer_bounds[3])
                        huc_bbox = bbox
                        
                        if layer_bbox.intersects(huc_bbox):
                            intersection = layer_bbox.intersection(huc_bbox)
                            print(f"  ✅ Bounds intersect! Area: {intersection.area/1000000:.2f} km²")
                        else:
                            print(f"  ❌ No intersection between layer and HUC bounds")
                            
                            # Show distance between centers
                            layer_center = layer_bbox.centroid
                            huc_center = huc_bbox.centroid
                            distance = layer_center.distance(huc_center)
                            print(f"  📏 Distance between centers: {distance/1000:.1f} km")
                
                except Exception as e:
                    print(f"  ❌ Error: {e}")
        
        return total_water_percent > 0
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_actual_nhd_layers()
    if success:
        print(f"\n🎉 Found the issue and created corrected mask!")
    else:
        print(f"\n🔧 Need to investigate further...")