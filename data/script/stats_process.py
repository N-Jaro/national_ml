# stats_process.py

import ee
import os
import json
from config import Settings

class StatsProcessor:
    """
    A class dedicated to calculating and saving normalization statistics
    for a list of HUC8 watersheds from Google Earth Engine.
    """
    
    def __init__(self, settings: Settings, huc_id: str):
        """
        Initializes the StatsProcessor.

        Args:
            settings (Settings): The global configuration object.
            huc_id (str): The ID of the HUC to process.
        """
        self.settings = settings
        self.huc_id = huc_id
        self.huc_geometry = None
        
        self._initialize_gee()
        self.HUC8_COL = ee.FeatureCollection(self.settings.HUC8_COL_NAME)

    def _initialize_gee(self):
        """Initializes the GEE API if not already done."""
        try:
            ee.Initialize(project=self.settings.GEE_PROJECT_ID)
        except Exception:
            pass

    def _fetch_huc_geometry(self):
        """Fetches the HUC geometry from GEE using the stored HUC ID."""
        print(f"  Fetching geometry for HUC {self.huc_id}...")
        huc_feature = self.HUC8_COL.filter(ee.Filter.eq('huc8', self.huc_id)).first()
        if huc_feature.getInfo() is None:
            raise FileNotFoundError(f"Could not find geometry for HUC {self.huc_id} on GEE.")
        self.huc_geometry = huc_feature.geometry()

    def _mask_l8sr_clouds(self, image: ee.Image) -> ee.Image:
        """Cloud-masks a Landsat 8/9 Collection 2 SR image."""
        qa = image.select('QA_PIXEL')
        cloud_shadow_bit_mask = 1 << 4
        clouds_bit_mask = 1 << 3
        mask = qa.bitwiseAnd(cloud_shadow_bit_mask).eq(0).And(qa.bitwiseAnd(clouds_bit_mask).eq(0))
        optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
        thermal_band = image.select('ST_B10').multiply(0.00341802).add(149.0)
        return image.addBands(optical_bands, None, True).addBands(thermal_band, None, True).updateMask(mask)

    def _get_landsat_composite(self, region: ee.Geometry) -> dict:
        """Gets a median composite for Landsat optical and thermal bands."""
        landsat_col = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2') \
            .filterBounds(region).filterDate(self.settings.START_DATE, self.settings.END_DATE).map(self._mask_l8sr_clouds)
        optical_median = landsat_col.select(['SR_B4', 'SR_B3', 'SR_B2']).median()
        thermal_median = landsat_col.select('ST_B10').median()
        return {'optical': optical_median, 'thermal': thermal_median}

    def _get_sar_composite(self, region: ee.Geometry) -> ee.Image:
        """Gets a median composite for Sentinel-1 SAR."""
        sar_col = ee.ImageCollection('COPERNICUS/S1_GRD') \
            .filterBounds(region).filterDate(self.settings.START_DATE, self.settings.END_DATE) \
            .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
            .filter(ee.Filter.eq('instrumentMode', 'IW')).select('VV')
        return sar_col.median()

    def run(self):
        """
        Executes the statistics calculation workflow for the HUC.
        """
        self._fetch_huc_geometry()
        
        # A buffered area is useful for fetching image composites to avoid edge effects
        aoi_for_composites = self.huc_geometry.buffer(10000)

        print("  Preparing GEE image composites for statistics...")
        dem_image = ee.Image(self.settings.DEM_SOURCE_IMG_NAME).select('elevation')
        landsat_composites = self._get_landsat_composite(aoi_for_composites)
        optical_image = landsat_composites['optical']
        thermal_image = landsat_composites['thermal']
        sar_image = self._get_sar_composite(aoi_for_composites)
        
        images_to_process = {
            'dem': dem_image, 'optical': optical_image, 
            'thermal': thermal_image, 'sar': sar_image
        }

        print("  Calculating normalization statistics...")
        reducers = ee.Reducer.minMax().combine(
            reducer2=ee.Reducer.mean(), sharedInputs=True
        ).combine(
            reducer2=ee.Reducer.stdDev(), sharedInputs=True
        )

        all_stats = {}
        for name, image in images_to_process.items():
            try:
                scale = self.settings.SOURCE_RESOLUTIONS[name]
                stats = image.reduceRegion(
                    reducer=reducers, geometry=self.huc_geometry,
                    scale=scale, maxPixels=1e9
                ).getInfo()
                all_stats[name] = stats
            except Exception as e:
                print(f"    - Warning: Could not calculate stats for '{name}'. Error: {e}")
                all_stats[name] = {}
        
        # Define the output path inside the PATCH data folder structure for easy access
        huc_patch_folder = os.path.join(self.settings.ROOT_OUTPUT_FOLDER, self.settings.PATCH_OUTPUT_FOLDER, self.huc_id)
        os.makedirs(huc_patch_folder, exist_ok=True)
        stats_path = os.path.join(huc_patch_folder, 'normalization_stats.json')
        
        with open(stats_path, 'w') as f:
            json.dump(all_stats, f, indent=4)
        print(f"  Saved statistics to '{stats_path}'")
