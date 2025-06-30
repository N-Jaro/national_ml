# huc_process.py

import ee
import os
import time
import re
import pandas as pd
from config import Settings

class HUCProcessor:
    """
    Processes HUC8 watersheds to generate patch center points and export
    the full HUC DEM for local processing.
    """
    def __init__(self, settings: Settings):
        self.settings = settings
        self.all_tasks = [] 
        self._initialize_gee()
        self.HUC8_COL = ee.FeatureCollection(settings.HUC8_COL_NAME)
        self.DEM_SOURCE_IMG = ee.Image(settings.DEM_SOURCE_IMG_NAME)

    def _initialize_gee(self):
        try:
            ee.Initialize(project=self.settings.GEE_PROJECT_ID)
        except Exception as e:
            print(f"Error initializing GEE: {e}"); raise

    def _generate_patch_centers(self, raster: ee.Image, geometry: ee.Geometry) -> ee.FeatureCollection:
        
        proj = raster.projection()
        pixel_coords = ee.Image.pixelCoordinates(proj).round()
        offset = self.settings.PATCH_SIZE // 2
        y_mask = pixel_coords.select('y').subtract(offset).mod(self.settings.PATCH_STRIDE).eq(0)
        x_mask = pixel_coords.select('x').subtract(offset).mod(self.settings.PATCH_STRIDE).eq(0)
        center_pixel_mask = y_mask.And(x_mask)
        patch_centers = center_pixel_mask.selfMask().reduceToVectors(geometry=geometry, scale=proj.nominalScale(), geometryType='centroid', eightConnected=False, maxPixels=1e9)
        return patch_centers

    def _process_single_huc(self, huc8_id: str):
        try:
            huc_feature = self.HUC8_COL.filter(ee.Filter.eq('huc8', huc8_id)).first()
            if huc_feature.getInfo() is None:
                print(f"Warning: HUC8 ID '{huc8_id}' not found. Skipping."); return

            huc_name_raw = huc_feature.get('name').getInfo()
            huc_name_clean = re.sub(r'[^a-zA-Z0-9_.-]+', '_', huc_name_raw)[:40]
            print(f"\nProcessing HUC8 {huc8_id}: {huc_name_clean}")
            
            gdrive_folder_name = huc8_id
            
            # --- Prepare HUC DEM for export and grid generation ---
            huc_geometry_buffered = huc_feature.geometry().buffer(self.settings.BUFFER_DISTANCE_METERS)
            dem_clipped = self.DEM_SOURCE_IMG.clip(huc_geometry_buffered)
            dem_reprojected = dem_clipped.reproject(crs=self.settings.TARGET_DEM_CRS, scale=self.settings.SOURCE_RESOLUTIONS['dem'])
            
            # --- 1. Export Full HUC DEM ---
            print(f"  Creating background task to export DEM for HUC {huc8_id}...")
            desc_dem = f'DEM_Export_HUC_{huc8_id}'
            dem_task = ee.batch.Export.image.toDrive(
                image=dem_reprojected.toFloat(), description=desc_dem, folder=gdrive_folder_name,
                fileNamePrefix=f'huc8_{huc8_id}_dem', fileFormat='GeoTIFF'
            )
            self.all_tasks.append({'task': dem_task, 'description': desc_dem})

            # --- 2. Generate and Save Center Points Locally ---
            print(f"  Generating patch centers for HUC {huc8_id}...")
            center_points = self._generate_patch_centers(raster=dem_reprojected, geometry=huc_feature.geometry())
            
            num_centers = center_points.size().getInfo()
            print(f"  Found {num_centers} center points for HUC {huc8_id}.")

            if num_centers == 0:
                print("  No centers found, no CSV will be created."); return
            
            center_points_with_id = center_points.randomColumn('random_id')
            def add_coords(pt):
                unique_id = ee.Number(pt.get('random_id')).multiply(1e16).toLong().format()
                patch_name = ee.String('patch_').cat(ee.String(huc8_id).cat('_')).cat(unique_id)
                return pt.set({'name': patch_name, 'lat': pt.geometry().coordinates().get(1), 'lon': pt.geometry().coordinates().get(0)})
            center_points_with_coords_fc = center_points_with_id.map(add_coords)

            print("  Fetching point data from GEE to save locally...")
            features_list = center_points_with_coords_fc.select(['name', 'lat', 'lon']).getInfo()['features']
            points_data = [f['properties'] for f in features_list]
            df = pd.DataFrame(points_data)
            
            huc_output_folder = os.path.join(self.settings.ROOT_OUTPUT_FOLDER, self.settings.HUC_OUTPUT_FOLDER, huc8_id)
            os.makedirs(huc_output_folder, exist_ok=True)
            csv_path = os.path.join(huc_output_folder, f'huc8_{huc8_id}_center_points.csv')
            df.to_csv(csv_path, index=False)
            print(f"  Successfully saved center points locally to '{csv_path}'")
        except Exception as e:
            print(f"An error occurred while processing HUC {huc8_id}: {e}")

    def run(self):
        
        for huc_id in self.settings.HUC_IDS_TO_PROCESS: self._process_single_huc(huc_id)
        if not self.all_tasks: print("No tasks were created. Exiting."); return
        print(f"\n--- {len(self.all_tasks)} tasks have been created, starting them now... ---")
        for item in self.all_tasks:
            try:
                task, description = item['task'], item['description']
                task.start(); print(f"  Initiated task: {description}")
            except Exception as e:
                print(f"!!! FAILED TO START TASK: {item.get('description', 'Unknown')} - Error: {e} !!!")
        self._monitor_tasks()
    
    def _monitor_tasks(self):
        # ... (monitor logic remains the same)
        print("\n--- Monitoring GEE Tasks ---")
        while any(item['task'].active() for item in self.all_tasks):
            active_tasks = [item for item in self.all_tasks if item['task'].active()]
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Waiting for {len(active_tasks)} tasks to complete...")
            time.sleep(60)
        print("\nAll tasks have completed.")
        failed_tasks = [item for item in self.all_tasks if item['task'].state == ee.batch.Task.State.FAILED]
        if failed_tasks:
            print("\n!!! WARNING: Some tasks failed !!!")
            for item in failed_tasks:
                task = item['task']
                print(f"  - Task: {item['description']}, State: {task.state}, Error: {task.status().get('error_message', 'Unknown')}")
        else:
            print("All tasks completed successfully.")
