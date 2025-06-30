# local_reference_process.py

import os
import numpy as np
import geopandas as gpd
import rasterio
from rasterio import features
from pysheds.grid import Grid
from tqdm import tqdm
from config import Settings

class LocalReferenceProcessor:
    """
    Processes local data to create hydrography and flow direction reference layers
    that perfectly align with previously downloaded GEE patches.
    """
    def __init__(self, settings: Settings, nhd_flowline_shapefile: str, nhd_waterbody_shapefile: str):
        """
        Initializes the processor.

        Args:
            settings (Settings): The global configuration object.
            nhd_flowline_shapefile (str): Path to the local NHD Flowline shapefile.
            nhd_waterbody_shapefile (str): Path to the local NHD Waterbody shapefile.
        """
        self.settings = settings
        print("Loading NHD shapefiles into memory...")
        self.flowlines_gdf = gpd.read_file(nhd_flowline_shapefile)
        self.waterbodies_gdf = gpd.read_file(nhd_waterbody_shapefile)
        print("Shapefiles loaded.")

    def _calculate_d8_flow_direction(self, dem_path: str, output_path: str):
        """
        Calculates a D8 flow direction raster from a DEM GeoTIFF using PySheds.
        """
        if os.path.exists(output_path):
            return # Skip if already processed

        print(f"    - Calculating D8 Flow Direction from '{os.path.basename(dem_path)}'...")
        grid = Grid.from_raster(dem_path, data_name='dem')
        
        # Preprocessing: Fill sinks
        grid.fill_depressions(data='dem', out_name='flooded_dem')
        
        # Calculate D8 flow direction
        grid.flowdir(data='flooded_dem', out_name='d8', routing='d8')
        
        # Save the D8 grid to a raster file
        grid.to_raster('d8', output_path)

    def _create_hydro_mask(self, dem_template_path: str, output_path: str):
        """
        Creates a hydrography mask by burning NHD vectors onto a DEM template grid.
        """
        if os.path.exists(output_path):
            return # Skip if already processed
            
        print(f"    - Creating Hydrography Mask based on '{os.path.basename(dem_template_path)}'...")
        with rasterio.open(dem_template_path) as src:
            meta = src.meta.copy()
            meta.update(compress='lzw', dtype='uint8', count=1, nodata=0)
            bounds = src.bounds

        # Find NHD features that intersect with the HUC bounds
        flowlines_subset = self.flowlines_gdf.cx[bounds.left:bounds.right, bounds.bottom:bounds.top]
        waterbodies_subset = self.waterbodies_gdf.cx[bounds.left:bounds.right, bounds.bottom:bounds.top]
        
        geoms_to_burn = []
        if not flowlines_subset.empty:
            geoms_to_burn.extend(flowlines_subset.geometry.tolist())
        if not waterbodies_subset.empty:
            geoms_to_burn.extend(waterbodies_subset.geometry.tolist())

        if not geoms_to_burn:
            mask = np.zeros((meta['height'], meta['width']), dtype=np.uint8)
        else:
            mask = features.rasterize(
                geometries=geoms_to_burn, out_shape=(meta['height'], meta['width']),
                transform=meta['transform'], fill=0, default_value=1, dtype=np.uint8
            )
        
        with rasterio.open(output_path, 'w', **meta) as dst:
            dst.write(mask, 1)

    def run(self):
        """
        Iterates through HUC folders, generates full reference rasters,
        then extracts patches and updates the .npz files.
        """
        huc_root_folder = os.path.join(self.settings.ROOT_OUTPUT_FOLDER, self.settings.HUC_OUTPUT_FOLDER)
        patch_root_folder = os.path.join(self.settings.ROOT_OUTPUT_FOLDER, self.settings.PATCH_OUTPUT_FOLDER)
        huc_folders = [d for d in os.listdir(huc_root_folder) if os.path.isdir(os.path.join(huc_root_folder, d))]

        print(f"\nFound {len(huc_folders)} HUC folders to process for local reference layers.")

        for huc_id in tqdm(huc_folders, desc="Processing HUCs Locally"):
            huc_dem_path = os.path.join(huc_root_folder, huc_id, f'huc8_{huc_id}_dem.tif')
            huc_dir_patches = os.path.join(patch_root_folder, huc_id)

            if not os.path.exists(huc_dem_path):
                print(f"Warning: DEM for HUC {huc_id} not found. Skipping.")
                continue

            # Define paths for the new full-HUC reference rasters
            flow_dir_path = os.path.join(huc_dir_patches, 'flow_direction.tif')
            hydro_mask_path = os.path.join(huc_dir_patches, 'hydro_mask.tif')

            # 1. Create the full reference rasters for the HUC
            self._calculate_d8_flow_direction(huc_dem_path, flow_dir_path)
            self._create_hydro_mask(huc_dem_path, hydro_mask_path)

            # 2. Extract patches from the new reference rasters
            print("    - Extracting reference patches and updating .npz files...")
            with rasterio.open(flow_dir_path) as flow_src, rasterio.open(hydro_mask_path) as hydro_src:
                patch_files = [f for f in os.listdir(huc_dir_patches) if f.endswith('.npz')]
                for patch_file_name in patch_files:
                    npz_path = os.path.join(huc_dir_patches, patch_file_name)
                    patch_index = patch_file_name.split('_')[1].split('.')[0]
                    template_tif_path = os.path.join(huc_dir_patches, f'patch_{patch_index}_georef_template.tif')
                    
                    if not os.path.exists(template_tif_path): continue

                    # Use the patch template to define the window to read
                    with rasterio.open(template_tif_path) as template_src:
                        window = template_src.window(*template_src.bounds)
                        flow_dir_patch = flow_src.read(1, window=window)
                        hydro_mask_patch = hydro_src.read(1, window=window)

                    # Update the .npz file
                    with np.load(npz_path) as existing_data:
                        updated_data = {key: existing_data[key] for key in existing_data}
                    
                    updated_data['flow_dir'] = flow_dir_patch
                    updated_data['hydro_mask'] = hydro_mask_patch
                    np.savez_compressed(npz_path, **updated_data)

