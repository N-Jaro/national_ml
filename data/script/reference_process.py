# reference_process.py

import ee
import numpy as np
import os
import requests
import io
import tifffile
from tqdm import tqdm
from config import Settings
import concurrent.futures
import warnings # <-- Import the warnings module

class ReferenceProcessor:
    """
    A class to process and generate reference data layers (hydrography mask,
    flow direction) for a given set of patch locations.
    """
    
    def __init__(self, center_points: list, output_folder: str, settings: Settings, huc_id: str):
        """
        Initializes the ReferenceProcessor.

        Args:
            center_points (list): A list of dictionaries for each patch center.
            output_folder (str): The path to the folder where patches are located and will be updated.
            settings (Settings): The global configuration object.
            huc_id (str): The ID of the HUC being processed.
        """
        self.center_points_list = center_points
        self.output_folder = output_folder
        self.settings = settings
        self.huc_id = huc_id
        
        self._initialize_gee()
        
        # Using the reliable, global MERIT Hydro raster dataset
        self.MERIT_HYDRO_IMAGE = ee.Image("MERIT/Hydro/v1_0_1")
        
        self.DEM_IMAGE = ee.Image(self.settings.DEM_SOURCE_IMG_NAME).select('elevation')

    def _initialize_gee(self):
        """Initializes the GEE API if not already done."""
        try:
            ee.Initialize(project=self.settings.GEE_PROJECT_ID)
        except Exception:
            pass

    def _fetch_patch_as_numpy(self, image: ee.Image, center_point: ee.Feature, scale: float) -> np.ndarray:
        """
        Fetches a single, clean, rectangular patch directly as a NumPy array.
        """
        image_proj = image.projection()
        patch_radius_meters = self.settings.PATCH_SIZE * scale / 2.0
        patch_area = center_point.geometry().transform(image_proj, 1).buffer(patch_radius_meters, 1).bounds(1, image_proj)
        
        try:
            url = image.getDownloadURL({
                'region': patch_area,
                'dimensions': f'{self.settings.PATCH_SIZE}x{self.settings.PATCH_SIZE}',
                'format': 'GEO_TIFF'
            })
            response = requests.get(url)
            response.raise_for_status()
            with io.BytesIO(response.content) as f:
                # THE FIX: Suppress the specific, benign ValueError from tifffile
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message="invalid literal for int() with base 10")
                    np_array = tifffile.imread(f)
            return np_array.astype(np.float32)
        except Exception as e:
            print(f"\n--- GEE Fetch Error ---\nCould not fetch patch data. GEE Error: {e}\n-----------------------\n")
            return None

    def _get_hydro_mask(self, aoi: ee.Geometry) -> ee.Image:
        """
        Creates a binary water mask from the MERIT Hydro dataset.
        """
        print("    - Preparing hydrography mask from MERIT Hydro...")
        # Select the river width band ('wth')
        river_width = self.MERIT_HYDRO_IMAGE.select('viswth')
        # Create a binary mask where river width is greater than 0
        hydro_mask_raw = river_width.gt(0)

        # Reproject the mask to match the DEM's grid for perfect alignment
        hydro_mask = hydro_mask_raw.reproject(
            crs=self.DEM_IMAGE.projection().crs(),
            scale=self.settings.SOURCE_RESOLUTIONS['dem']
        ).unmask(0).uint8() # Ensure output is an 8-bit integer (0 or 1)

        return hydro_mask

    def _get_flow_direction(self, aoi: ee.Geometry) -> ee.Image:
        """
        Derives a D8 flow direction layer from the DEM by reclassifying aspect.
        """
        print("    - Preparing flow direction...")
        dem = self.DEM_IMAGE.clip(aoi)
        
        # Manually calculate D8 flow direction from aspect.
        aspect = ee.Terrain.aspect(dem)
        
        # Reclassify the aspect degrees into D8 flow direction codes.
        flow_direction_raw = ee.Image(0).uint8() \
            .where(aspect.gt(0).And(aspect.lte(22.5)), 64) \
            .where(aspect.gt(22.5).And(aspect.lte(67.5)), 128) \
            .where(aspect.gt(67.5).And(aspect.lte(112.5)), 1) \
            .where(aspect.gt(112.5).And(aspect.lte(157.5)), 2) \
            .where(aspect.gt(157.5).And(aspect.lte(202.5)), 4) \
            .where(aspect.gt(202.5).And(aspect.lte(247.5)), 8) \
            .where(aspect.gt(247.5).And(aspect.lte(292.5)), 16) \
            .where(aspect.gt(292.5).And(aspect.lte(337.5)), 32) \
            .where(aspect.gt(337.5), 64)

        # Explicitly reproject to ensure consistent grid alignment.
        flow_direction = flow_direction_raw.reproject(
            crs=self.DEM_IMAGE.projection().crs(),
            scale=self.settings.SOURCE_RESOLUTIONS['dem']
        )
        return flow_direction

    def _process_single_patch(self, args):
        """
        Worker function to fetch reference data and update the .npz file for one patch.
        """
        i, center_feature, ref_images = args
        patch_file_path = os.path.join(self.output_folder, f'patch_{i}.npz')

        if not os.path.exists(patch_file_path):
            return False 

        try:
            dem_resolution = self.settings.SOURCE_RESOLUTIONS['dem']
            hydro_mask_array = self._fetch_patch_as_numpy(ref_images['hydro'], center_feature, dem_resolution)
            flow_dir_array = self._fetch_patch_as_numpy(ref_images['flow'], center_feature, dem_resolution)

            if hydro_mask_array is None and flow_dir_array is None:
                return False

            with np.load(patch_file_path) as existing_data:
                updated_data = {key: existing_data[key] for key in existing_data}
            
            if hydro_mask_array is not None:
                updated_data['hydro_mask'] = hydro_mask_array
            if flow_dir_array is not None:
                updated_data['flow_dir'] = flow_dir_array
            
            if len(updated_data) > len(existing_data):
                np.savez_compressed(patch_file_path, **updated_data)
            return True
        except Exception as e:
            print(f"\nError processing {patch_file_path}: {e}")
            return False

    def run(self):
        """
        Executes the reference data generation workflow.
        """
        center_point_features = [ee.Feature(ee.Geometry.Point([p['lon'], p['lat']])) for p in self.center_points_list]
        aoi_for_prep = ee.FeatureCollection(center_point_features).geometry().buffer(10000)

        print("  Preparing reference GEE images (Hydro Mask & Flow Direction)...")
        hydro_mask_image = self._get_hydro_mask(aoi_for_prep)
        flow_dir_image = self._get_flow_direction(aoi_for_prep)
        print("  Reference image preparation complete.")
        
        ref_images = {'hydro': hydro_mask_image, 'flow': flow_dir_image}
        tasks_args = [(i, center_feature, ref_images) for i, center_feature in enumerate(center_point_features)]
        
        MAX_WORKERS = 16
        print(f"  Fetching and updating {len(tasks_args)} patches with reference data...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(tqdm(executor.map(self._process_single_patch, tasks_args), total=len(tasks_args), desc="  Updating Patches"))
        
        success_count = sum(1 for r in results if r)
        print(f"  Reference data processing complete. {success_count}/{len(tasks_args)} patches successfully updated.")
