#!/usr/bin/env python3
"""
Reference Data Bulk Processor - Downloads and processes reference datasets for ML training.
Handles hydrography masks and flow direction only (2 reference types).
Uses REAL algorithms from local_reference_process.py for production-quality output.
"""

import ee
import rasterio
import numpy as np
import pandas as pd
from pathlib import Path
import os
import sys
from datetime import datetime
from typing import List, Tuple, Optional, Dict
import logging
import geopandas as gpd
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds
from rasterio.crs import CRS
from rasterio import features
from rasterio.windows import from_bounds
from rasterio.enums import Resampling
from pysheds.grid import Grid
from tqdm import tqdm
import concurrent.futures

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_config import LocalProcessingSettings
from data_manager import LocalDataManager


class ReferenceBulkProcessor:
    """Downloads and processes reference datasets for ML training."""
    
    def __init__(self, config: LocalProcessingSettings, data_manager: LocalDataManager):
        self.config = config
        self.data_manager = data_manager
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Ensure output directories exist
        self.reference_output_dir = self.config.local_raster_base / "reference"
        self.reference_output_dir.mkdir(parents=True, exist_ok=True)
    
    def create_hydrography_mask(self, huc_id: str, resolution: float = 10.0) -> str:
        """Create REAL hydrography mask from NHD vector data using DEM bounds."""
        
        self.logger.info(f"Creating REAL hydrography mask for HUC {huc_id}")
        
        # Get bounds from existing DEM instead of using GEE
        dem_path = self.config.local_raster_base / "dem" / f"huc_{huc_id}_dem_{int(resolution)}m.tif"
        if not dem_path.exists():
            # Try research grade DEM
            dem_path = self.config.local_raster_base / "dem" / f"huc_{huc_id}_dem_{int(resolution)}m_research_grade.tif"
            if not dem_path.exists():
                raise FileNotFoundError(f"DEM not found for HUC {huc_id}. Need DEM to determine bounds for hydro mask.")
        
        # Use DEM bounds and CRS
        import rasterio
        with rasterio.open(dem_path) as dem_src:
            bounds = dem_src.bounds  # Already in target CRS (EPSG:5070)
            dem_crs = dem_src.crs
            dem_transform = dem_src.transform
            dem_width = dem_src.width
            dem_height = dem_src.height
        
        self.logger.info(f"Using DEM bounds: {bounds} from {dem_path.name}")
        
        # Create output path
        output_dir = self.config.local_raster_base / "reference"
        output_dir.mkdir(exist_ok=True)
        
        output_filename = f"huc_{huc_id}_hydro_mask_{int(resolution)}m.tif"
        output_path = output_dir / output_filename
        
        # Skip if already exists
        if output_path.exists():
            self.logger.info(f"Hydrography mask already exists: {output_path.name}")
            return str(output_path)
        
        # Skip if already exists
        if output_path.exists():
            self.logger.info(f"Hydrography mask already exists: {output_path.name}")
            return str(output_path)
        
        # Try to use real NHD data from GDB
        try:
            hydro_mask = self._create_real_hydrography_mask_from_gdb(huc_id, bounds, dem_transform, dem_width, dem_height)
            self.logger.info("Successfully created hydrography mask from NHD GDB")
        except Exception as e:
            self.logger.warning(f"Failed to use NHD GDB, trying shapefiles: {e}")
            try:
                hydro_mask = self._create_hydrography_mask_from_shapefiles(huc_id, bounds, dem_transform, dem_width, dem_height)
                self.logger.info("Successfully created hydrography mask from NHD shapefiles")
            except Exception as e2:
                self.logger.error(f"Failed with both GDB and shapefiles: {e2}")
                raise
        
        # Write to file using DEM's profile as template
        with rasterio.open(dem_path) as dem_src:
            profile = dem_src.profile.copy()
            
        # Update profile for hydro mask
        profile.update(dtype='uint8', compress='lzw', nodata=0, count=1)
        
        with rasterio.open(output_path, 'w', **profile) as dst:
            dst.write(hydro_mask, 1)
            dst.set_band_description(1, 'hydrography_mask_nhd')
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        self.logger.info(f"Created REAL hydrography mask: {file_size_mb:.1f}MB")
        
        return str(output_path)
    
    def _create_real_hydrography_mask_from_gdb(self, huc_id: str, bounds, transform, width: int, height: int) -> np.ndarray:
        """Create hydrography mask from NHD GDB using DEM bounds."""
        
        # Look for NHD GDB
        gdb_path = self.config.nhd_gdb_path
        if not gdb_path or not os.path.exists(gdb_path):
            raise FileNotFoundError(f"NHD GDB not found: {gdb_path}")
        
        self.logger.info(f"Using NHD GDB: {gdb_path}")
        self.logger.info(f"Creating GDB hydro mask: {width}x{height} pixels using DEM bounds")
        
        # Initialize mask
        hydro_mask = np.zeros((height, width), dtype=np.uint8)
        
        # Create bounding box in projected coordinates
        from shapely.geometry import box
        bbox_projected = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        
        # Convert to geographic coordinates for NHD querying (NHD data is in NAD83)
        import pyproj
        transformer = pyproj.Transformer.from_crs("EPSG:5070", "EPSG:4326", always_xy=True)
        min_x, min_y = transformer.transform(bounds.left, bounds.bottom)
        max_x, max_y = transformer.transform(bounds.right, bounds.top)
        bbox_geographic = box(min_x, min_y, max_x, max_y)
        
        self.logger.info(f"Geographic bounds for NHD query: {min_x:.6f}, {min_y:.6f}, {max_x:.6f}, {max_y:.6f}")
        
        # Process NHD layers with correct names from GDB inspection
        layers_to_process = [
            'NetworkNHDFlowline',     # Network flowlines (was 'NHDFlowline')
            'NonNetworkNHDFlowline',  # Non-network flowlines  
            'NHDWaterbody',          # Water bodies (lakes, ponds)
            'NHDArea'                # Area features
        ]
        
        for layer_name in layers_to_process:
            try:
                self.logger.info(f"Processing {layer_name} from GDB...")
                
                # Read with geographic bbox first
                gdf = gpd.read_file(gdb_path, layer=layer_name, bbox=bbox_geographic)
                
                if gdf.empty:
                    self.logger.info(f"No {layer_name} features found in bounds")
                    continue
                
                # Reproject to EPSG:5070 (NHD data is in NAD83 geographic)
                if gdf.crs != 'EPSG:5070':
                    self.logger.info(f"Reprojecting {layer_name} from {gdf.crs} to EPSG:5070")
                    gdf = gdf.to_crs("EPSG:5070")
                
                # Filter by projected bounds (double-check spatial filtering)
                gdf_clipped = gdf[gdf.intersects(bbox_projected)]
                self.logger.info(f"Found {len(gdf_clipped)} {layer_name} features after clipping")
                
                if gdf_clipped.empty:
                    continue
                
                # Buffer flowlines for better representation
                if 'Flowline' in layer_name:
                    gdf_clipped = gdf_clipped.copy()
                    gdf_clipped['geometry'] = gdf_clipped.geometry.buffer(15.0)  # 15m buffer
                
                # Burn into raster
                mask = geometry_mask(
                    gdf_clipped.geometry,
                    transform=transform,
                    invert=True,
                    out_shape=(height, width)
                )
                
                hydro_mask = np.logical_or(hydro_mask, mask).astype(np.uint8)
                
                layer_pixels = np.sum(mask)
                layer_percent = (layer_pixels / mask.size) * 100
                self.logger.info(f"Added {len(gdf_clipped)} {layer_name} features ({layer_percent:.4f}% coverage)")
                
            except Exception as e:
                self.logger.warning(f"Failed to process {layer_name}: {e}")
        
        water_pixels = np.sum(hydro_mask)
        water_percent = (water_pixels / hydro_mask.size) * 100
        self.logger.info(f"Hydrography mask: {water_percent:.2f}% water coverage")
        
        return hydro_mask
    
    def _create_hydrography_mask_from_shapefiles(self, huc_id: str, bounds, transform, width: int, height: int) -> np.ndarray:
        """Fallback: Create hydrography mask from NHD shapefiles using DEM bounds."""
        
        shapefile_dir = self.config.nhd_shapefiles_dir
        if not shapefile_dir or not os.path.exists(shapefile_dir):
            raise FileNotFoundError(f"NHD shapefiles directory not found: {shapefile_dir}")
        
        self.logger.info(f"Using NHD shapefiles: {shapefile_dir}")
        self.logger.info(f"Creating shapefile hydro mask: {width}x{height} pixels using DEM bounds")
        
        # Initialize mask
        hydro_mask = np.zeros((height, width), dtype=np.uint8)
        
        # Create bounding box for spatial filtering
        from shapely.geometry import box
        bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        
        # Process shapefiles
        shapefiles = [
            'NHDnetworkflowline.shp',
            'NHDWaterbody.shp',
            'NonNetworkNHDFlowline.shp'
        ]
        
        for shapefile in shapefiles:
            shapefile_path = os.path.join(shapefile_dir, shapefile)
            if not os.path.exists(shapefile_path):
                self.logger.warning(f"Shapefile not found: {shapefile}")
                continue
            
            try:
                self.logger.info(f"Processing {shapefile}...")
                gdf = gpd.read_file(shapefile_path, bbox=bbox)
                
                if gdf.empty:
                    continue
                
                # Ensure CRS matches (reproject to EPSG:5070)
                if gdf.crs != 'EPSG:5070':
                    gdf = gdf.to_crs("EPSG:5070")
                
                # Buffer flowlines
                if 'flowline' in shapefile.lower():
                    gdf['geometry'] = gdf.geometry.buffer(15.0)
                
                # Burn into raster
                mask = geometry_mask(
                    gdf.geometry,
                    transform=transform,
                    invert=True,
                    out_shape=(height, width)
                )
                
                hydro_mask = np.logical_or(hydro_mask, mask).astype(np.uint8)
                self.logger.info(f"Added {len(gdf)} features from {shapefile}")
                
            except Exception as e:
                self.logger.warning(f"Failed to process {shapefile}: {e}")
        
        water_pixels = np.sum(hydro_mask)
        water_percent = (water_pixels / hydro_mask.size) * 100
        self.logger.info(f"Hydrography mask: {water_percent:.2f}% water coverage")
        
        return hydro_mask
    
    def create_flow_direction(self, huc_id: str, resolution: float = 10.0) -> str:
        """Create REAL D8 flow direction from actual DEM using PySheds (from local_reference_process.py)."""
        
        self.logger.info(f"Creating REAL flow direction for HUC {huc_id} using PySheds D8 algorithm")
        
        # Check if DEM exists
        dem_path = self.config.local_raster_base / "dem" / f"huc_{huc_id}_dem_{int(resolution)}m.tif"
        if not dem_path.exists():
            # Try research grade DEM
            dem_path = self.config.local_raster_base / "dem" / f"huc_{huc_id}_dem_{int(resolution)}m_research_grade.tif"
            if not dem_path.exists():
                raise FileNotFoundError(f"DEM not found: {dem_path}")
        
        self.logger.info(f"Using DEM: {dem_path}")
        
        # Create output path
        output_dir = self.config.local_raster_base / "reference"
        output_dir.mkdir(exist_ok=True)
        
        output_filename = f"huc_{huc_id}_flow_direction_{int(resolution)}m.tif"
        output_path = output_dir / output_filename
        
        # Skip if already exists
        if output_path.exists():
            self.logger.info(f"Flow direction already exists: {output_path.name}")
            return str(output_path)
        
        # Use the real PySheds D8 algorithm from local_reference_process.py
        self._calculate_d8_flow_direction_pysheds(str(dem_path), str(output_path))
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        self.logger.info(f"Created REAL flow direction using PySheds: {file_size_mb:.1f}MB")
        
        return str(output_path)
    
    def _calculate_d8_flow_direction_pysheds(self, dem_path: str, output_path: str):
        """
        Calculates a D8 flow direction raster from a DEM GeoTIFF using PySheds.
        This is the REAL algorithm from local_reference_process.py.
        """
        self.logger.info(f"Calculating D8 Flow Direction from '{os.path.basename(dem_path)}'...")
        
        # Pre-process the DEM to ensure it has a valid nodata value
        temp_dem_path = dem_path.replace('.tif', '_corrected.tif')

        self.logger.info("Pre-processing DEM...")
        with rasterio.open(dem_path) as src:
            profile = src.profile
            dem_array = src.read(1)

            # Determine a valid nodata value based on dtype
            dtype = dem_array.dtype
            if np.issubdtype(dtype, np.unsignedinteger):
                nodata_val = 65535
            elif np.issubdtype(dtype, np.floating):
                nodata_val = np.nan
            else:
                nodata_val = -32767

            # Optional: Mask extreme or invalid values
            dem_array = np.where(np.isnan(dem_array) | (dem_array <= -9999), nodata_val, dem_array)
            
            # Update profile
            profile.update(nodata=nodata_val)

            # Save corrected DEM to a temporary file
            with rasterio.open(temp_dem_path, 'w', **profile) as dst:
                dst.write(dem_array, 1)

        try:
            self.logger.info("Calculate Flow Direction (D8) using PySheds...")
            self.logger.info("Loading DEM into PySheds Grid...")
            grid = Grid.from_raster(temp_dem_path)
            dem = grid.read_raster(temp_dem_path)

            self.logger.info("Filling pits...")
            pit_filled_dem = grid.fill_pits(dem)
            
            self.logger.info("Filling depressions...")
            flooded_dem = grid.fill_depressions(pit_filled_dem)

            self.logger.info("Resolving flats...")
            inflated_dem = grid.resolve_flats(flooded_dem)

            self.logger.info("Calculating D8 flow direction...")
            dirmap = (64, 128, 1, 2, 4, 8, 16, 32)
            fdir = grid.flowdir(inflated_dem, dirmap=dirmap, nodata_out=np.int32(-1))

            self.logger.info(f"Saving D8 flow direction to: {output_path}")
            grid.to_raster(fdir, output_path)
            self.logger.info("D8 raster saved successfully.")
            
        except Exception as e:
            self.logger.error(f"Error during PySheds processing: {e}")
            raise
        finally:
            # Clean up the temporary corrected DEM file
            if os.path.exists(temp_dem_path):
                os.remove(temp_dem_path)
                self.logger.info(f"Cleaned up temporary file: {os.path.basename(temp_dem_path)}")
    
    def process_all_references_for_huc(self, huc_id: str, 
                                      reference_types: Optional[List[str]] = None) -> Dict[str, str]:
        """Process reference datasets for a HUC (hydro_mask and flow_direction only)."""
        
        if reference_types is None:
            reference_types = ['hydro_mask', 'flow_direction']
        
        # Only support the 2 required reference types
        supported_types = ['hydro_mask', 'flow_direction']
        reference_types = [t for t in reference_types if t in supported_types]
        
        self.logger.info(f"Processing {len(reference_types)} reference datasets for HUC {huc_id}")
        
        results = {}
        
        for ref_type in reference_types:
            try:
                if ref_type == 'hydro_mask':
                    output_path = self.create_hydrography_mask(huc_id)
                elif ref_type == 'flow_direction':
                    output_path = self.create_flow_direction(huc_id)
                else:
                    self.logger.warning(f"Unsupported reference type: {ref_type}")
                    continue
                
                results[ref_type] = output_path
                self.logger.info(f"✓ {ref_type} completed: {Path(output_path).name}")
                
            except Exception as e:
                self.logger.error(f"✗ Failed to process {ref_type}: {e}")
                results[ref_type] = None
        
        return results


if __name__ == "__main__":
    # Example usage - Process only hydro_mask and flow_direction
    config = LocalProcessingSettings()
    data_manager = LocalDataManager(config)
    processor = ReferenceBulkProcessor(config, data_manager)
    
    # Process references for a test HUC (only 2 reference types)
    huc_id = "10020007"
    results = processor.process_all_references_for_huc(huc_id)
    
    print(f"Reference processing results for HUC {huc_id}:")
    for ref_type, path in results.items():
        if path:
            print(f"  ✓ {ref_type}: {Path(path).name}")
        else:
            print(f"  ✗ {ref_type}: Failed")