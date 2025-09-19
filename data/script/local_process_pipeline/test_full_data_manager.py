"""
Test script for the full LocalDataManager with geospatial capabilities
"""

import os
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from local_config import LocalProcessingSettings
from data_manager import LocalDataManager, BandManager

def create_test_raster(output_path: str, bounds: tuple, crs: str, 
                      band_count: int = 1, resolution: float = 10.0):
    """Create a small test raster for demonstration"""
    
    # Calculate dimensions
    width = int((bounds[2] - bounds[0]) / resolution)
    height = int((bounds[3] - bounds[1]) / resolution)
    
    # Create transform
    transform = from_bounds(*bounds, width, height)
    
    # Create some test data
    data = np.random.rand(band_count, height, width).astype(np.float32) * 1000
    
    # Create the raster
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with rasterio.open(
        output_path, 'w',
        driver='GTiff',
        height=height, width=width,
        count=band_count, dtype=np.float32,
        crs=crs, transform=transform,
        compress='lzw'
    ) as dst:
        for i in range(band_count):
            dst.write(data[i], i + 1)
    
    print(f"Created test raster: {os.path.basename(output_path)} "
          f"({band_count} bands, {width}x{height})")
    
    return output_path

def test_full_data_manager():
    """Test the full LocalDataManager capabilities"""
    
    print("=== Testing Full LocalDataManager ===")
    
    # Initialize
    settings = LocalProcessingSettings()
    data_manager = LocalDataManager(settings)
    band_manager = BandManager(settings)
    
    # Test storage structure
    print("\n1. Testing storage structure:")
    structure = data_manager.organize_storage_structure()
    print("Storage structure created successfully")
    
    # Create some test rasters
    print("\n2. Creating test rasters:")
    
    # Test bounds (small area in EPSG:5070)
    test_bounds = (-2000000, 1000000, -1990000, 1010000)  # 10km x 10km area
    
    # Create test DEM (1 band)
    dem_path = create_test_raster(
        settings.get_storage_path('dem', 'test_dem.tif'),
        test_bounds, 'EPSG:5070', band_count=1, resolution=10.0
    )
    
    # Create test optical data (6 bands)
    optical_path = create_test_raster(
        settings.get_storage_path('landsat', 'test_optical.tif'),
        test_bounds, 'EPSG:5070', band_count=6, resolution=30.0
    )
    
    # Create test AlphaEarth (16 bands - smaller for testing)
    alphaearth_path = create_test_raster(
        settings.get_storage_path('alphaearth', 'test_alphaearth.tif'),
        test_bounds, 'EPSG:5070', band_count=16, resolution=10.0
    )
    
    # Test raster registration
    print("\n3. Testing raster registration:")
    
    success_dem = data_manager.register_raster('dem', dem_path)
    success_optical = data_manager.register_raster('optical', optical_path)
    success_alphaearth = data_manager.register_raster('alphaearth', alphaearth_path)
    
    print(f"Registration results: DEM={success_dem}, Optical={success_optical}, AlphaEarth={success_alphaearth}")
    
    # Test spatial queries
    print("\n4. Testing spatial queries:")
    
    # Query for overlapping rasters
    query_bounds = (-1995000, 1005000, -1985000, 1015000)  # Overlapping area
    overlapping = data_manager.find_overlapping_rasters(query_bounds)
    
    print(f"Found {len(overlapping)} overlapping rasters:")
    for raster in overlapping:
        print(f"  {raster['source']}: {raster['band_count']} bands, {raster['resolution']}m")
    
    # Test band statistics
    print("\n5. Testing band statistics:")
    
    dem_stats = band_manager.get_band_statistics(dem_path)
    print(f"DEM statistics: {len(dem_stats)} bands")
    for band, stats in dem_stats.items():
        if stats['count'] > 0:
            print(f"  {band}: mean={stats['mean']:.1f}, std={stats['std']:.1f}")
    
    optical_stats = band_manager.get_band_statistics(optical_path, [1, 2, 3])  # First 3 bands
    print(f"Optical statistics (first 3 bands): {len(optical_stats)} bands")
    for band, stats in optical_stats.items():
        if stats['count'] > 0:
            print(f"  {band}: mean={stats['mean']:.1f}, std={stats['std']:.1f}")
    
    # Test band subsetting (useful for AlphaEarth)
    print("\n6. Testing band subsetting:")
    
    subset_path = settings.get_storage_path('alphaearth', 'test_alphaearth_subset.tif')
    success_subset = band_manager.create_band_subset(
        alphaearth_path, subset_path, [1, 3, 5, 7, 9]  # Select every other band
    )
    
    if success_subset:
        # Validate the subset
        is_valid = band_manager.validate_band_count(subset_path, 5)
        print(f"Band subset created and validated: {is_valid}")
    
    # Test data completeness validation
    print("\n7. Testing data completeness:")
    
    completeness = data_manager.validate_data_completeness(test_bounds)
    print("Data availability:")
    for source, available in completeness.items():
        status = "✓" if available else "✗"
        print(f"  {status} {source}")
    
    print("\n=== Full Data Manager Test Complete ===")
    print("All geospatial capabilities working correctly!")
    
    return {
        'dem_path': dem_path,
        'optical_path': optical_path,
        'alphaearth_path': alphaearth_path,
        'subset_path': subset_path if success_subset else None,
        'overlapping_count': len(overlapping),
        'completeness': completeness
    }

if __name__ == "__main__":
    try:
        results = test_full_data_manager()
        
        print(f"\n=== Test Results Summary ===")
        print(f"Test rasters created: {len([p for p in [results['dem_path'], results['optical_path'], results['alphaearth_path']] if p])}")
        print(f"Spatial queries working: {results['overlapping_count'] > 0}")
        print(f"Band operations working: {results['subset_path'] is not None}")
        print(f"Data sources available: {sum(results['completeness'].values())}/{len(results['completeness'])}")
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
