# patch_process.py

import ee
import numpy as np
import os
import requests
import io
import tifffile
import json 
from tqdm import tqdm
from config import Settings 
import concurrent.futures 
import threading # New import for the Semaphore

class PatchProcessor:
    """
    A class to process and download multi-source, multi-resolution satellite data 
    patches from Google Earth Engine, using parallel fetching.
    """
    
    def __init__(self, center_points: list, output_folder: str, settings: Settings, huc_id: str):
        self.center_points_list = center_points
        self.output_folder = output_folder
        self.settings = settings
        self.huc_id = huc_id
        self._initialize_gee()
        self.HUC8_COL = ee.FeatureCollection(self.settings.HUC8_COL_NAME)
        # New: Initialize a semaphore to limit concurrent requests
        # A value of 4-5 is a good balance to avoid GEE rate limits
        self.semaphore = threading.BoundedSemaphore(value=4) 

    def _initialize_gee(self):
        try:
            ee.Initialize(project=self.settings.GEE_PROJECT_ID)
        except Exception:
            pass

    def _fetch_patch_as_geotiff_content(self, image: ee.Image, center_point: ee.Feature, scale: float, crs: str) -> bytes:
        """
        Fetches a single patch and returns the raw GeoTIFF file content as bytes,
        ensuring it is in the specified CRS.
        """
        # The image is already reprojected, but we calculate the patch area in its projection
        image_proj = image.projection()
        patch_radius_meters = self.settings.PATCH_SIZE * scale / 2.0
        patch_area = center_point.geometry().transform(image_proj, 1).buffer(patch_radius_meters, 1).bounds(1, image_proj)
        
        try:
            url = image.getDownloadURL({
                'region': patch_area,
                'dimensions': f'{self.settings.PATCH_SIZE}x{self.settings.PATCH_SIZE}',
                'format': 'GEO_TIFF',
                'crs': crs # Explicitly set the CRS for the output GeoTIFF
            })
            
            # Use the semaphore to limit concurrent requests
            with self.semaphore:
                response = requests.get(url)
                response.raise_for_status()
                
            return response.content
        except Exception as e:
            print(f"\n--- GEE Fetch Error ---\nCould not fetch patch data. GEE Error: {e}\n-----------------------\n")
            return None

    def _mask_l8sr_clouds(self, image: ee.Image) -> ee.Image:
        qa = image.select('QA_PIXEL')
        cloud_shadow_bit_mask = 1 << 4
        clouds_bit_mask = 1 << 3
        mask = qa.bitwiseAnd(cloud_shadow_bit_mask).eq(0).And(qa.bitwiseAnd(clouds_bit_mask).eq(0))
        optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
        thermal_band = image.select('ST_B10').multiply(0.00341802).add(149.0)
        return image.addBands(optical_bands, None, True).addBands(thermal_band, None, True).updateMask(mask)

    def _get_landsat_composite(self, region: ee.Geometry) -> dict:
        """
        Fetches and processes Landsat 9 surface reflectance and thermal data.
        https://developers.google.com/earth-engine/datasets/catalog/LANDSAT_LC09_C02_T1_L2#bands
        """
        landsat_col = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2').filterBounds(region).filterDate(self.settings.START_DATE, self.settings.END_DATE).map(self._mask_l8sr_clouds)
        optical_median = landsat_col.select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']).median()
        thermal_median = landsat_col.select('ST_B10').median()
        return {'optical': optical_median, 'thermal': thermal_median}

    def _get_sar_composite(self, region: ee.Geometry) -> ee.Image:
        sar_col = ee.ImageCollection('COPERNICUS/S1_GRD').filterBounds(region).filterDate(self.settings.START_DATE, self.settings.END_DATE).filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')).filter(ee.Filter.eq('instrumentMode', 'IW')).select('VV')
        return sar_col.median()
    
    # New method to get AlphaEarth composite
    def _get_alphaearth_composite(self, region: ee.Geometry) -> ee.Image:
        """
        Fetches and processes Google Satellite Embedding (AlphaEarth) data for a specific year.
        """
        # The dataset is an annual collection. We will use the latest year available.
        year = self.settings.ALPHA_EARTH_YEAR
        alphaearth_col = ee.ImageCollection('GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL')
        filtered_embeddings = alphaearth_col.filter(ee.Filter.calendarRange(year, year, 'year')).filterBounds(region)
        return filtered_embeddings.mosaic()

    def _process_single_patch(self, args):
        """
        Worker function to fetch all data layers for one patch and save it.
        """
        i, center_feature, gee_images = args
        target_crs = self.settings.TARGET_DEM_CRS
        try:
            # Fetch raw GeoTIFF content first, ensuring the correct CRS
            dem_tiff_content = self._fetch_patch_as_geotiff_content(gee_images['dem'], center_feature, self.settings.SOURCE_RESOLUTIONS['dem'], target_crs)
            optical_tiff_content = self._fetch_patch_as_geotiff_content(gee_images['optical'], center_feature, self.settings.SOURCE_RESOLUTIONS['optical'], target_crs)
            thermal_tiff_content = self._fetch_patch_as_geotiff_content(gee_images['thermal'], center_feature, self.settings.SOURCE_RESOLUTIONS['thermal'], target_crs)
            sar_tiff_content = self._fetch_patch_as_geotiff_content(gee_images['sar'], center_feature, self.settings.SOURCE_RESOLUTIONS['sar'], target_crs)
            # New: Fetch AlphaEarth content
            alphaearth_tiff_content = self._fetch_patch_as_geotiff_content(gee_images['alphaearth'], center_feature, self.settings.SOURCE_RESOLUTIONS['alphaearth'], target_crs)

            patch_data = {}
            if dem_tiff_content:
                patch_data['dem'] = tifffile.imread(io.BytesIO(dem_tiff_content))
                # Save the DEM GeoTIFF to use as a georeference template
                with open(os.path.join(self.output_folder, f'patch_{i}_georef_template.tif'), 'wb') as f:
                    f.write(dem_tiff_content)
            
            if optical_tiff_content:
                # Transpose optical data if necessary
                optical_array = tifffile.imread(io.BytesIO(optical_tiff_content))
                if optical_array.ndim == 3 and optical_array.shape[0] < optical_array.shape[1]:
                    optical_array = np.transpose(optical_array, (1, 2, 0))
                patch_data['optical'] = optical_array

            if thermal_tiff_content:
                patch_data['thermal'] = tifffile.imread(io.BytesIO(thermal_tiff_content))
            if sar_tiff_content:
                patch_data['sar'] = tifffile.imread(io.BytesIO(sar_tiff_content))
            # New: Add AlphaEarth data
            if alphaearth_tiff_content:
                patch_data['alphaearth'] = tifffile.imread(io.BytesIO(alphaearth_tiff_content))
            
            if patch_data:
                file_path = os.path.join(self.output_folder, f'patch_{i}.npz')
                np.savez_compressed(file_path, **patch_data)
            return True
        except Exception:
            return False

    def run(self):
        """
        Executes the patch fetching workflow using a thread pool for concurrency.
        """
        center_point_features = [ee.Feature(ee.Geometry.Point([p['lon'], p['lat']])) for p in self.center_points_list]
        aoi_for_composites = ee.FeatureCollection(center_point_features).geometry().buffer(10000)

        print("  Preparing GEE image composites...")
        # --- MODIFIED: Use ImageCollection and mosaic it ---
        dem_collection = ee.ImageCollection(self.settings.DEM_SOURCE_IMG_NAME)
        dem_image_raw = dem_collection.mosaic().select('elevation')
        # --- END MODIFICATION ---
        
        landsat_composites = self._get_landsat_composite(aoi_for_composites)
        optical_image_raw = landsat_composites['optical']
        thermal_image_raw = landsat_composites['thermal']
        sar_image_raw = self._get_sar_composite(aoi_for_composites)
        
        # New: Get AlphaEarth composite
        alphaearth_image_raw = self._get_alphaearth_composite(aoi_for_composites)
        
        # --- THE FIX: Reproject all source images to the target CRS ---
        target_crs = self.settings.TARGET_DEM_CRS
        print(f"  Reprojecting all source images to {target_crs}...")
        
        dem_image = dem_image_raw.reproject(crs=target_crs, scale=self.settings.SOURCE_RESOLUTIONS['dem'])
        optical_image = optical_image_raw.reproject(crs=target_crs, scale=self.settings.SOURCE_RESOLUTIONS['optical'])
        thermal_image = thermal_image_raw.reproject(crs=target_crs, scale=self.settings.SOURCE_RESOLUTIONS['thermal'])
        sar_image = sar_image_raw.reproject(crs=target_crs, scale=self.settings.SOURCE_RESOLUTIONS['sar'])
        # New: Reproject AlphaEarth image
        alphaearth_image = alphaearth_image_raw.reproject(crs=target_crs, scale=self.settings.SOURCE_RESOLUTIONS['alphaearth'])


        print("  Image preparation complete.")
        
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Add 'alphaearth' to the gee_images dictionary
        gee_images = {'dem': dem_image, 'optical': optical_image, 'thermal': thermal_image, 'sar': sar_image, 'alphaearth': alphaearth_image}
        tasks_args = [(i, center_feature, gee_images) for i, center_feature in enumerate(center_point_features)]
        
        MAX_WORKERS = 16
        print(f"  Fetching {len(tasks_args)} patches with up to {MAX_WORKERS} parallel workers...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(tqdm(executor.map(self._process_single_patch, tasks_args), total=len(tasks_args), desc="  Fetching Patches"))
        
        success_count = sum(1 for r in results if r)
        print(f"  Patch fetching complete. {success_count}/{len(tasks_args)} patches successfully processed.")
