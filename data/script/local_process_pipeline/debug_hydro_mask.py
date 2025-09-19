#!/usr/bin/env python3
"""
Debug Hydrography Mask Issues
Investigate why the hydro mask shows 0% water coverage
"""

import sys
import os
import numpy as np
import geopandas as gpd
import rasterio
from pathlib import Path
from shapely.geometry import box
from rasterio.features import geometry_mask
import fiona

# Add the pipeline directory to the path
sys.path.append('/u/nathanj/national_ml/data/script/local_process_pipeline')

def debug_hydro_mask_creation():
    """Debug the hydrography mask creation process."""
    
    print("🔍 DEBUGGING HYDROGRAPHY MASK CREATION")
    print("=" * 50)
    
    huc_id = "10020007"
    
    try:
        from local_config import LocalProcessingSettings
        config = LocalProcessingSettings()
        
        # Get DEM bounds and transform
        dem_path = config.local_raster_base / "dem" / f"huc_{huc_id}_dem_10m.tif"
        
        print(f"📊 Setup Information:")
        print(f"  🏔️  DEM: {dem_path}")
        print(f"  🗂️  NHD GDB: {config.nhd_gdb_path}")
        print(f"  📂 NHD Shapefiles: {config.nhd_shapefiles_dir}")
        
        # Read DEM to get bounds and transform
        with rasterio.open(dem_path) as dem_src:
            bounds = dem_src.bounds
            transform = dem_src.transform
            width = dem_src.width
            height = dem_src.height
            
            print(f"\n📐 DEM Properties:")
            print(f"  📏 Dimensions: {width} × {height}")
            print(f"  🗺️  CRS: {dem_src.crs}")
            print(f"  📍 Bounds: {bounds}")
            print(f"  🎯 Resolution: {abs(transform[0]):.1f}m")
        
        # Create bounding box
        bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        print(f"\n📦 Bounding Box:")
        print(f"  📍 Area: {bbox.area / 1000000:.1f} km²")
        
        # Test 1: Check NHD GDB layers
        print(f"\n🧪 TEST 1: NHD GDB Layer Analysis")
        print("-" * 40)
        
        gdb_path = config.nhd_gdb_path
        if gdb_path and os.path.exists(gdb_path):
            print(f"✅ GDB exists: {gdb_path}")
            
            # List all layers in GDB
            layers = fiona.listlayers(gdb_path)
            print(f"📋 Available layers ({len(layers)} total):")
            for layer in sorted(layers):
                print(f"  📄 {layer}")
            
            # Test each key layer
            key_layers = ['NHDFlowline', 'NHDWaterbody', 'NHDArea']
            
            for layer_name in key_layers:
                print(f"\n🔍 Testing layer: {layer_name}")
                
                if layer_name not in layers:
                    print(f"  ❌ Layer '{layer_name}' not found in GDB")
                    continue
                    
                    try:
                        # Try to read without bbox first
                        print(f"  📖 Reading layer (no bbox filter)...")
                        gdf_all = gpd.read_file(gdb_path, layer=layer_name)
                        print(f"  📊 Total features: {len(gdf_all)}")
                        print(f"  🗺️  CRS: {gdf_all.crs}")
                        
                        if len(gdf_all) > 0:
                            # Check bounds
                            layer_bounds = gdf_all.total_bounds
                            print(f"  📍 Layer bounds: {layer_bounds}")
                            
                            # Now try with bbox filter
                            print(f"  📖 Reading layer (with bbox filter)...")
                            gdf_filtered = gpd.read_file(gdb_path, layer=layer_name, bbox=bbox)
                            print(f"  📊 Filtered features: {len(gdf_filtered)}")
                            
                            if len(gdf_filtered) > 0:
                                # Test geometry types
                                geom_types = gdf_filtered.geometry.geom_type.value_counts()
                                print(f"  🔢 Geometry types:")
                                for geom_type, count in geom_types.items():
                                    print(f"     {geom_type}: {count}")
                                
                                # Test CRS
                                if gdf_filtered.crs != 'EPSG:5070':
                                    print(f"  🔄 Reprojecting from {gdf_filtered.crs} to EPSG:5070")
                                    gdf_filtered = gdf_filtered.to_crs("EPSG:5070")
                                
                                # Test buffering for flowlines
                                if 'Flowline' in layer_name:
                                    print(f"  🔧 Testing 15m buffer...")
                                    gdf_buffered = gdf_filtered.copy()
                                    gdf_buffered['geometry'] = gdf_buffered.geometry.buffer(15.0)
                                    
                                    # Compare areas
                                    orig_area = gdf_filtered.geometry.area.sum()
                                    buff_area = gdf_buffered.geometry.area.sum()
                                    print(f"     Original area: {orig_area/1000000:.2f} km²")
                                    print(f"     Buffered area: {buff_area/1000000:.2f} km²")
                                    
                                    gdf_to_rasterize = gdf_buffered
                                else:
                                    gdf_to_rasterize = gdf_filtered
                                
                                # Test rasterization
                                print(f"  🎨 Testing rasterization...")
                                try:
                                    mask = geometry_mask(
                                        gdf_to_rasterize.geometry,
                                        transform=transform,
                                        invert=True,
                                        out_shape=(height, width)
                                    )
                                    
                                    water_pixels = np.sum(mask)
                                    water_percent = (water_pixels / mask.size) * 100
                                    print(f"     ✅ Rasterized: {water_pixels:,} pixels ({water_percent:.4f}%)")
                                    
                                except Exception as e:
                                    print(f"     ❌ Rasterization failed: {e}")
                            
                            else:
                                print(f"  ⚠️  No features found within HUC bounds")
                                
                                # Check if bbox overlaps with layer bounds
                                from shapely.geometry import box
                                layer_bbox = box(layer_bounds[0], layer_bounds[1], layer_bounds[2], layer_bounds[3])
                                huc_bbox = bbox
                                
                                overlap = layer_bbox.intersects(huc_bbox)
                                print(f"  📍 Bbox overlap check: {'✅' if overlap else '❌'}")
                                
                                if overlap:
                                    intersection = layer_bbox.intersection(huc_bbox)
                                    print(f"  📍 Intersection area: {intersection.area / 1000000:.2f} km²")
                        
                        else:
                            print(f"  ❌ Layer is empty")
                    
                    except Exception as e:
                        print(f"  ❌ Error processing {layer_name}: {e}")
                        import traceback
                        traceback.print_exc()
            
            except Exception as e:
                print(f"❌ Error listing GDB layers: {e}")
        
        else:
            print(f"❌ GDB not found: {gdb_path}")
        
        # Test 2: Check alternative layers
        print(f"\n🧪 TEST 2: Alternative Layer Names")
        print("-" * 40)
        
        if gdb_path and os.path.exists(gdb_path):
            # Try different layer names that might exist
            alternative_layers = [
                'NHDFlowline_Network',
                'HYDRO_NET_Flowline', 
                'Flowline',
                'WaterbodyArea',
                'NHD_H_Area',
                'NHD_H_Waterbody'
            ]
            
            for alt_layer in alternative_layers:
                if alt_layer in layers:
                    print(f"  ✅ Found alternative: {alt_layer}")
                    try:
                        gdf = gpd.read_file(gdb_path, layer=alt_layer, bbox=bbox)
                        print(f"     📊 Features: {len(gdf)}")
                    except Exception as e:
                        print(f"     ❌ Error: {e}")
        
        # Test 3: Manual mask creation
        print(f"\n🧪 TEST 3: Manual Mask Creation")
        print("-" * 40)
        
        # Create a test mask with some water features
        print(f"  🎨 Creating test mask...")
        test_mask = np.zeros((height, width), dtype=np.uint8)
        
        # Add some test water features (e.g., center square)
        center_y, center_x = height // 2, width // 2
        test_size = 100
        test_mask[center_y-test_size:center_y+test_size, center_x-test_size:center_x+test_size] = 1
        
        test_water_percent = (np.sum(test_mask) / test_mask.size) * 100
        print(f"  ✅ Test mask: {test_water_percent:.4f}% water")
        
        # Save test mask
        output_dir = config.local_raster_base / "reference"
        test_output_path = output_dir / f"huc_{huc_id}_test_hydro_mask.tif"
        
        with rasterio.open(dem_path) as dem_src:
            profile = dem_src.profile.copy()
            profile.update(dtype='uint8', count=1, compress='lzw')
            
            with rasterio.open(test_output_path, 'w', **profile) as dst:
                dst.write(test_mask, 1)
        
        print(f"  💾 Saved test mask: {test_output_path.name}")
        
        print(f"\n📋 DEBUGGING SUMMARY:")
        print(f"  📊 The issue appears to be with NHD layer selection or spatial filtering")
        print(f"  🔧 Next steps:")
        print(f"     1. Verify correct layer names in NHD GDB")
        print(f"     2. Check spatial overlap between HUC bounds and NHD data")
        print(f"     3. Test different buffer sizes or processing methods")
        print(f"     4. Consider using alternative NHD layers or datasets")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_hydro_mask_creation()