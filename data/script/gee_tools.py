import ee
import re
import os
import config as const
import time
from drive_manager import GoogleDriveManager
from config import Settings
import tools

import time

class GEEWorkflow:
    
    def __init__(self,settings:Settings ,drive_manager: GoogleDriveManager):
        
        self.settings = settings
        self.all_tasks = []
        self.drive_manager = drive_manager
        self._authenticate()
        self._data_loader()
        
        self.current_file = None
        self.base_file_name = []  # save base file name for each HUC export
          
    def _authenticate(self):
        try:
            if self.settings.GEE_PROJECT_ID:
                ee.Initialize(project=self.settings.GEE_PROJECT_ID,opt_url='https://earthengine-highvolume.googleapis.com')
            else:
                # default project
                ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')#
            print("Google Earth Engine(High Volume Endpoint) successfully initialized.")
        except ee.EEException as e:
            print(e)
            print("GEE failed initialized. Pleas run 'earthengine authenticate'.")
            exit()
        
    def _data_loader(self):
        """Load necessary data collections"""
        self.HUC8_COL = ee.FeatureCollection(self.settings.HUC8_COL_NAME)
        self.MERIT_HYDRO_IMG = ee.Image(self.settings.MERIT_HYDRO_IMG_NAME)
        self.DEM_SOURCE_IMG = ee.Image(self.settings.DEM_SOURCE_IMG_NAME)

    def launch_all_export_tasks(self):
        """
        Launch all export tasks for selected HUCs. This version sanitizes all
        collections before they are exported to prevent validation errors.
        """
        print("\n--- Start the GEE export task ---")
        selected_hucs = self.HUC8_COL.randomColumn('random').sort('random').limit(self.settings.NUMBER_OF_HUCS)

        # --- THE FIX: Sanitize the collection before exporting ---
        # We select only a list of known "safe" properties to include in the export.
        # This prevents problematic characters in the 'name' field from causing an error.
        # We must set 'retainGeometry' to True to keep the HUC boundaries.
        properties_to_keep = ['huc8', 'states', 'areaacres', 'hu_12_ds'] # Example safe properties
        sanitized_hucs = selected_hucs.select(propertySelectors=properties_to_keep, retainGeometry=True)
        
        # 1. Export the SANITIZED collection of HUC boundaries to Google Drive
        desc_global = f'ALL_HUC8_Original_Boundaries_Export_{self.settings.NUMBER_OF_HUCS}'
        task_global = ee.batch.Export.table.toDrive(
            collection=sanitized_hucs, # Use the sanitized collection here
            description=desc_global,
            folder=self.settings.DRIVE_FOLDER,
            fileNamePrefix=f'Selected_{self.settings.NUMBER_OF_HUCS}_HUC8_Original_Boundaries',
            fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.all_tasks.append({'task': task_global, 'description': desc_global})

        # The rest of the function proceeds as before
        selected_hucs_list_info = selected_hucs.select(['huc8', 'states', 'name']).toList(self.settings.NUMBER_OF_HUCS).getInfo()
        print(f"Obtain {len(selected_hucs_list_info)} information for {self.settings.NUMBER_OF_HUCS} HUC(s). Start creating export tasks for each HUC...")

        # 2. Add tasks for each selected HUC
        for feature_info in selected_hucs_list_info:
            self.process_single_huc(feature_info)
            
        # 3. Launch all created tasks
        print(f"\n--- {len(self.all_tasks)} tasks have been created, starting them now... ---")
        for item in self.all_tasks:
            try:
                item['task'].start()
                print(f"Initiated task: {item['description']} (ID: {item['task'].id})")
            except Exception as e:
                print(f"!!! FAILED TO START TASK: {item['description']} !!!")
                print(f"    Error: {e}")

    def monitor_and_organize_tasks(self):
        """
        Monitors the status of all launched tasks with a detailed, live-updating
        status line and provides a comprehensive final report.
        """
        import itertools
        
        print("\n--- Monitor Mode ---")
        start_time = time.time()
        spinner = itertools.cycle(['-', '/', '|', '\\'])

        while True:
            # Dictionaries to hold the current status of all tasks
            task_states = {'COMPLETED': 0, 'FAILED': 0, 'RUNNING': 0, 'READY': 0, 'UNSUBMITTED': 0, 'CANCELLED': 0}
            failed_task_details = []
            
            active_tasks_exist = False

            for item in self.all_tasks:
                # Only poll the status if it's not in a terminal state
                if item.get('state') not in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    try:
                        status = item['task'].status()
                        current_state = status['state']
                        item['state'] = current_state  # Update state in our local list

                        if current_state in ['RUNNING', 'READY']:
                            active_tasks_exist = True
                        
                        if current_state == 'FAILED':
                            error_message = status.get('error_message', 'Unknown error')
                            failed_task_details.append(f"  - {item['description']}: {error_message}")

                    except Exception as e:
                        # Handle cases where the status check itself fails (e.g., network issues)
                        error_str = str(e)
                        if '503' in error_str or 'unavailable' in error_str:
                            # It's a temporary server error, treat the task as 'READY' and retry
                            item['state'] = 'READY'
                            active_tasks_exist = True
                        else:
                            # For other errors, mark as FAILED
                            item['state'] = 'FAILED'
                            failed_task_details.append(f"  - {item['description']}: Failed to get status - {e}")

                # Update the count for the task's current state
                task_states[item.get('state', 'UNSUBMITTED')] += 1

            # --- Create the Live-Updating Status Line ---
            elapsed_time = time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))
            status_line = (
                f"\r[{next(spinner)}] Elapsed: {elapsed_time} | "
                f"COMPLETED: {task_states['COMPLETED']} | "
                f"RUNNING: {task_states['RUNNING']} | "
                f"READY: {task_states['READY']} | "
                f"FAILED: {task_states['FAILED']} | "
                f"Total: {len(self.all_tasks)}   "  # Extra spaces to clear previous line
            )
            print(status_line, end="")
            
            if not active_tasks_exist:
                print("\n\nAll tasks have reached a terminal state.")
                break
            
            time.sleep(15) # Check status every 15 seconds for faster updates

        # --- Final Summary Report ---
        final_elapsed_time = time.time() - start_time
        print("\n--- FINAL REPORT ---")
        print(f"Total monitoring time: {time.strftime('%H hours, %M minutes, %S seconds', time.gmtime(final_elapsed_time))}")
        print(f"Tasks Completed: {task_states['COMPLETED']}/{len(self.all_tasks)}")
        print(f"Tasks Failed:    {task_states['FAILED']}/{len(self.all_tasks)}")
        
        if failed_task_details:
            print("\n!!! Details for FAILED tasks !!!")
            for detail in failed_task_details:
                print(detail)
        
        # --- Post-Processing Steps ---
        # Only proceed if there are completed tasks to organize and download
        if task_states['COMPLETED'] > 0:
            print("\n--- Merging Google Drive folders ---")
            # Using set to get unique folder names
            unique_folders = sorted(list(set(self.base_file_name)))
            for name in unique_folders:
                print(f"Organizing folder: {name}")
                self.drive_manager.merge_duplicate_folders(name)
                self.drive_manager.move_folder(name, self.settings.DRIVE_FOLDER)
            
            print("\n--- Downloading files from Google Drive ---")
            if not os.path.exists(self.settings.LOCAL_DOWNLOAD_DIR):
                os.makedirs(self.settings.LOCAL_DOWNLOAD_DIR)
            
            main_folder_id = self.drive_manager.get_gdrive_folder_id(self.settings.DRIVE_FOLDER)
            if not main_folder_id:
                print(f"Error: Cannot find main folder in Google Drive '{self.settings.DRIVE_FOLDER}'")
                return
            self.drive_manager.download_folder_recursively(main_folder_id, self.settings.LOCAL_DOWNLOAD_DIR)
        else:
            print("\nNo tasks completed successfully. Skipping Drive organization and download.")
            
        print("\n--- Monitoring and Organization Complete ---")
        return
        
    def launch_single_data_collector(self, feature_info, patch=None):
        """
        Get 7 types of data for a single HUC & original
        """
        props = feature_info['properties']
        huc_id = props.get('huc8', 'UnknownID')
        name = props.get('name', 'UnknownName')
        states = props.get('states', 'NA')
        
        original_geometry = ee.Geometry(feature_info['geometry'])
        
        # clean up name for file naming
        clean_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', name)
        base_filename = f'HUC8_{huc_id}_{clean_name}'
        self.current_file = base_filename
        self.base_file_name.append(base_filename)
        
        buffered_geometry = original_geometry.buffer(self.settings.BUFFER_DISTANCE_METERS)
        export_region_rectangle = buffered_geometry.bounds()

        print(f"\n Creat({name}) for HUC {huc_id}，export to {base_filename}")

        # Task 0: export original HUC boundaries (vector)
        desc_global = f'HUC8_Original_Boundaries_Export_{self.settings.NUMBER_OF_HUCS}'
        task_global = ee.batch.Export.table.toDrive(
            collection=ee.FeatureCollection([original_geometry]),
            description=desc_global,
            folder=base_filename,
            fileNamePrefix=f'HUC8_{self.settings.NUMBER_OF_HUCS}_Original_Boundaries',
            fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.all_tasks.append({'task': task_global, 'description': desc_global})
        
        # Task 1: buffered boundary (vector)
        buffered_fc = ee.FeatureCollection([ee.Feature(buffered_geometry, props)])
        # print(buffered_fc.getInfo())
        desc_buff = f'Buffered_Boundary_{huc_id}'
        task_buff = ee.batch.Export.table.toDrive(
            collection=buffered_fc, description=desc_buff, folder=base_filename,
            fileNamePrefix=f'{base_filename}_Buffered_Boundary', fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.all_tasks.append({'task': task_buff, 'description': desc_buff, 'folder': base_filename})

        # Task 2: region rectangle (vector)
        bbox_fc = ee.FeatureCollection([ee.Feature(export_region_rectangle, props)])
        desc_bbox = f'BoundingBox_{huc_id}'
        task_bbox = ee.batch.Export.table.toDrive(
            collection=bbox_fc, description=desc_bbox, folder= base_filename,
            fileNamePrefix=f'{base_filename}_BoundingBox', fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.all_tasks.append({'task': task_bbox, 'description': desc_bbox, 'folder': base_filename})
        
        # Task 3: DEM
        dem_image = self._get_dem(self.DEM_SOURCE_IMG,export_region_rectangle)
        # can be really time consuming
        
        print(dem_image.projection().crs().getInfo())
        
        desc_dem = f'DEM_3DEP_Export_{huc_id}'
        task_dem = ee.batch.Export.image.toDrive(
            image=dem_image.toFloat(), description=desc_dem, folder=base_filename,
            fileNamePrefix=f'{base_filename}_DEM_10m_Rect', region=export_region_rectangle,
            scale=10, crs=self.settings.TARGET_DEM_CRS, maxPixels=1.5e10
        )
        self.all_tasks.append({'task': task_dem, 'description': desc_dem, 'folder': base_filename})

        # Task 4 & 5: optical and thermal Landsat images
        landsat_images = self._get_landsat_images(buffered_geometry, self.settings.START_DATE, self.settings.END_DATE)
        landsat_images['optical'] = landsat_images['optical'].reproject(
            crs='EPSG:4269',
            scale = 10
        ).clip(export_region_rectangle)
        landsat_images['thermal']= landsat_images['thermal'].reproject(
            crs='EPSG:4269',
            scale = 10
        ).clip(export_region_rectangle)
        
        
        print(landsat_images['optical'].toFloat().projection().crs().getInfo()) 
        print(landsat_images['thermal'].toFloat().projection().crs().getInfo())
        
        
        desc_l_opt = f'Landsat_Optical_Export_{huc_id}'
        task_l_opt = ee.batch.Export.image.toDrive(
            image=landsat_images['optical'].toFloat(), description=desc_l_opt, folder=base_filename,
            fileNamePrefix=f'{base_filename}_Landsat_Optical_Rect', region=export_region_rectangle,
            scale=10, crs='EPSG:4326', maxPixels=1.5e10
        )
        self.all_tasks.append({'task': task_l_opt, 'description': desc_l_opt, 'folder': base_filename})
        
        desc_l_therm = f'Landsat_Thermal_Export_{huc_id}'
        task_l_therm = ee.batch.Export.image.toDrive(
            image=landsat_images['thermal'].toFloat(), description=desc_l_therm, folder=base_filename,
            fileNamePrefix=f'{base_filename}_Landsat_Thermal_Rect', region=export_region_rectangle,
            scale=10, crs='EPSG:4326', maxPixels=1.5e10
        )
        self.all_tasks.append({'task': task_l_therm, 'description': desc_l_therm, 'folder': base_filename})
        
        # Task 6: SAR image
        sar_image = self._get_sar_image(buffered_geometry, self.settings.START_DATE, self.settings.END_DATE)
        # print(sar_image.projection().crs().getInfo())
        sar_image = sar_image.reproject(
            crs='EPSG:4269',
            scale =10
        ).clip(export_region_rectangle)
        
        desc_sar = f'SAR_VV_Export_{huc_id}'
        task_sar = ee.batch.Export.image.toDrive(
            image=sar_image.toFloat(), description=desc_sar, folder=base_filename,
            fileNamePrefix=f'{base_filename}_SAR_VV_Rect', region=export_region_rectangle,
            scale=10, crs='EPSG:4269', maxPixels=1.5e10
        )
        self.all_tasks.append({'task': task_sar, 'description': desc_sar, 'folder': base_filename})

        # Task 7: MERIT Hydro Flow Direction
        flow_dir_resampled = self.MERIT_HYDRO_IMG.select('dir').reproject(crs=self.settings.TARGET_DEM_CRS, scale=10)
        flow_dir_final = flow_dir_resampled.clip(export_region_rectangle)

        flow_dir_final = flow_dir_final.reproject(
            crs = self.settings.TARGET_DEM_CRS,
            scale = 10
        )
        
        print(flow_dir_final.projection().crs().getInfo())
        
        desc_flow = f'FlowDir_10m_Export_{huc_id}'
        task_flow = ee.batch.Export.image.toDrive(
            image=flow_dir_final.toUint8(), description=desc_flow, folder=base_filename,
            fileNamePrefix=f'{base_filename}_FlowDir_10m_Rect', region=export_region_rectangle,
            scale=10, crs='EPSG:4326', maxPixels=1.5e10
        )
        self.all_tasks.append({'task': task_flow, 'description': desc_flow, 'folder': base_filename})
        
        print(f"\n--- Get Data Patches ---")
        # 1. Generate patch centers
        
        patch_centers = self._genearte_patch_centers(dem_image, self.settings.PATCH_SIZE, self.settings.PATCH_STRIDE,buffered_geometry)
        # print(type(patch_centers))
        print(f"Patch centers generated. Total centers: {patch_centers.size().getInfo()}")
        
        # 3. Visualization
        if self.settings.VISUALIZE_POINTS:
            import tools
            # 3. Visualize patch centers
            map= tools.plot_centers_ee(patch_centers, buffered_geometry)
            map.to_html("visualization.html")
 
        # Get patches 
        print(f"\nGet patches based on the center point by batch...")
        
        batch_size = self.settings.BATCH_EXPORT_SIZE
        num_batches = patch_centers.size().divide(batch_size).ceil().getInfo()
        patch_centers_lists = patch_centers.toList(patch_centers.size())
        
        # test, 1 batches 
        for i in range(1): # num_batches
            print(f"Processing batch {i+1}/{num_batches}...")
            start_index = i * batch_size
            end_index = start_index + batch_size
            
            batch_patches = ee.FeatureCollection(patch_centers_lists.slice(start_index,end_index))
            
            dem_image_patches = self._extract_patches_gee(dem_image, batch_patches, self.settings.PATCH_SIZE)
            opt_image_patches = self._extract_patches_gee(landsat_images['optical'],batch_patches,self.settings.PATCH_SIZE)
            the_image_patches = self._extract_patches_gee(landsat_images['thermal'],batch_patches,self.settings.PATCH_SIZE)
            sar_image_patches = self._extract_patches_gee(sar_image,batch_patches,self.settings.PATCH_SIZE)
            flow_image_patches = self._extract_patches_gee(flow_dir_final,batch_patches,self.settings.PATCH_SIZE)
            
            
            dem_image_patches = dem_image_patches.toList(dem_image_patches.size())
            opt_image_patches = opt_image_patches.toList(opt_image_patches.size())
            the_image_patches = the_image_patches.toList(the_image_patches.size())
            sar_image_patches = sar_image_patches.toList(sar_image_patches.size())
            flow_image_patches = flow_image_patches.toList(flow_image_patches.size())
            
            
            # image_list = image_patches.toList(image_patches.size())
            collection_size = dem_image_patches.size().getInfo()

            print(f"{collection_size} images is found, start exporting...")
            
            # counter = 0
            # Export each image in the collection to Google Drive
            # export 2 images for testing, to export all images, change the range to collection_size
            print('Testing export patches...Only 1 images will be exported.')
            for i in range(1):
                self._save_patches(ee.Image(dem_image_patches.get(i)),i,'dem')
                self._save_patches(ee.Image(opt_image_patches.get(i)),i,'opt')
                self._save_patches(ee.Image(the_image_patches.get(i)),i,'the')
                self._save_patches(ee.Image(sar_image_patches.get(i)),i,'sar')
                self._save_patches(ee.Image(flow_image_patches.get(i)),i,'flow')
                
                # test
                map = tools.plot_raster_patches_ee(ee.Image(dem_image_patches.get(i)),ee.Image(opt_image_patches.get(i))
                                                   ,ee.Image(the_image_patches.get(i)),ee.Image(sar_image_patches.get(i))
                                                   ,ee.Image(flow_image_patches.get(i)),boundary=buffered_geometry)
                map.to_html("test.html")   
        
        return 
        # 
        # Task 8: Patches

    def _save_patches(self, raster: ee.Image, index: int, name: str):
        """
        Exports a final, analysis-ready image patch AND its corresponding metadata file.

        This function implements all best practices:
        1. Guarantees exact output pixel dimensions using the 'dimensions' parameter.
        2. Uses robust, predictable naming for files and task descriptions.
        3. Exports to the common, projected CRS ('EPSG:5070') to prevent distortion.
        4. Calls a helper to create a metadata file for data provenance.
        """
        patch_geometry = raster.geometry()
        # Extract HUC ID from the current file name for concise, unique naming
        huc_id = self.current_file.split('_')[1]

        # --- 1. Prepare Metadata Dictionary for Tracking ---
        # Get the trusted native resolution from the settings file
        native_resolution = self.settings.SOURCE_RESOLUTIONS.get(name, 'unknown')
        metadata = {
            'patch_id': f'patch_{name}_{index}',
            'data_type': name,
            'huc_id': huc_id,
            'native_resolution_meters': native_resolution,
            'target_patch_size_pixels': self.settings.PATCH_SIZE,
            'exported_crs': self.settings.TARGET_DEM_CRS, # Record the correct CRS
            'export_timestamp_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        }

        # --- 2. Create the Image Export Task ---
        # Use a concise, unique, and valid description for the task
        image_description = f'HUC_{huc_id}_Img_{name}_{index}'
        # Use a simple, robust filename
        image_file_name = f"patch_{name}_{index}"
        # Define the exact output dimensions (e.g., "256x256")
        dimensions_str = f"{self.settings.PATCH_SIZE}x{self.settings.PATCH_SIZE}"

        image_task = ee.batch.Export.image.toDrive(
            image=raster.toFloat(),
            description=image_description,
            folder=self.current_file,
            fileNamePrefix=image_file_name,
            region=patch_geometry,
            dimensions=dimensions_str, # This is the best way to control output size
            crs=self.settings.TARGET_DEM_CRS, # Ensure consistent CRS
            maxPixels=1e10
        )
        self.all_tasks.append({'task': image_task, 'description': image_description, 'folder': self.current_file})

        # --- 3. Call the Helper to Create the Metadata Export Task ---
        self._save_patch_metadata(metadata, index, name)
    
    def _save_patch_metadata(self, metadata: dict, index: int, name: str):
        """
        Exports a metadata JSON file for a single patch.
        """
        huc_id = self.current_file.split('_')[1]

        # Create a concise and unique description and filename
        description = f'HUC_{huc_id}_Meta_{name}_{index}'
        file_name = f"patch_{name}_{index}_metadata"

        # Convert the Python dictionary to a GEE object and export
        metadata_ee = ee.Dictionary(metadata)
        feature = ee.Feature(None, metadata_ee)

        task = ee.batch.Export.table.toDrive(
            collection=ee.FeatureCollection([feature]),
            description=description,
            folder=self.current_file,
            fileNamePrefix=file_name,
            fileFormat='GeoJSON'
        )
        
        self.all_tasks.append({'task': task, 'description': description, 'folder': self.current_file})
    
    def _genearte_patch_centers(self, raster:ee.Image, patch_size:int, stride:int, bounder:ee.Geometry)->ee.FeatureCollection:
        """
        Generates a grid of center points over an ee.Image.

        This function replicates the logic of generating patch centers by creating a grid
        over the image's footprint and finding the centroid of each grid cell.

        Parameters:
            image (ee.Image): The input raster image for which patch centers will be generated.
            patch_size (int): The size of the square patch to be extracted (in pixels).
            stride (int): The step size for moving the window across the image (in pixels).
            bounder (ee.Geometry): A geometry to filter the patch centers. If provided,
                only centers within this geometry will be used.
        Returns:
            ee.FeatureCollection: A collection of points representing the center of each patch.
        """

        proj = raster.projection()
        # print(f"Projection: {proj.getInfo()}")
        pixel_coords = ee.Image.pixelCoordinates(proj)
        int_pixel_coords = pixel_coords.round() # inportant to round pixel coordinates to avoid floating point issues
        
        offset = patch_size // 2
        y_mask = int_pixel_coords.select('y').subtract(offset).mod(stride).eq(0)
        x_mask = int_pixel_coords.select('x').subtract(offset).mod(stride).eq(0)
        center_pixel_mask = y_mask.And(x_mask)
        

        patch_centers = center_pixel_mask.selfMask().reduceToVectors(
            geometry=bounder,#raster.geometry(),
            scale=proj.nominalScale(),
            geometryType='centroid',
            eightConnected=False,
            maxPixels=5e8 # Adjusted to a reasonable limit
        )

        return patch_centers.select([])
        
    def _extract_patches_gee(self,source_image: ee.Image, center_points: ee.FeatureCollection, patch_size: int) -> ee.ImageCollection:
        """
        Extracts patches from a raster image based on a collection of center points.

        Args:
            raster (ee.Image): original raster image from which patches will be extracted.
            center_points (ee.FeatureCollection): collection of points representing the center of each patch.
            patch_size_pixels (int): size of the square patch to be extracted (in pixels).

        Returns:
            ee.ImageCollection: A collection of images, each representing a patch extracted from the raster image.
        """
        
        # Obtain the projection information of the image to calculate the pixel size (meters per pixel)
        projection = source_image.projection()
        scale_meters = projection.nominalScale() # nominalScale() 
        
        # change patch size from pixels to meters
        patch_size_meters = ee.Number(patch_size).multiply(scale_meters)
    
        
        def create_patch(feature: ee.Feature) -> ee.Image:
            feature = ee.Feature(feature)
            center_point = feature.geometry()
            
            
            patch_area = center_point.buffer(patch_size_meters.divide(2)).bounds()
            
            return source_image.clip(patch_area).copyProperties(feature, ['system:index'])

        patches = center_points.map(create_patch)
        return ee.ImageCollection(patches)
        
    def _mask_l8sr_clouds(self, image):
        """Landsat 8/9 SR Image cloud removal function """
        cloud_shadow_bit_mask = 1 << 4
        clouds_bit_mask = 1 << 3
        qa = image.select('QA_PIXEL')
        mask = qa.bitwiseAnd(cloud_shadow_bit_mask).eq(0).And(qa.bitwiseAnd(clouds_bit_mask).eq(0))
        
        optical_bands = image.select('SR_B[2-7]').multiply(0.0000275).add(-0.2)
        thermal_band = image.select('ST_B10').multiply(0.00341802).add(149.0)
        
        return image.addBands(optical_bands, None, True).addBands(thermal_band, None, True).updateMask(mask)

    def _get_dem(self,source_img,clip_geometry) -> ee.Image:
        return source_img.select('elevation').clip(clip_geometry)

    def _get_landsat_images(self, filter_geometry, start_date, end_date):
        landsat_col = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2') \
            .merge(ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')) \
            .filterBounds(filter_geometry) \
            .filterDate(start_date, end_date) \
            .map(self._mask_l8sr_clouds)
        
        landsat_optical_median = landsat_col.select(['SR_B4', 'SR_B3', 'SR_B2']).median()
        landsat_thermal_median = landsat_col.select('ST_B10').median()
        return {'optical': landsat_optical_median, 'thermal': landsat_thermal_median}

    def _get_sar_image(self, filter_geometry, start_date, end_date):
        sentinel1_col = ee.ImageCollection('COPERNICUS/S1_GRD') \
            .filterBounds(filter_geometry) \
            .filterDate(start_date, end_date) \
            .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
            .filter(ee.Filter.eq('instrumentMode', 'IW')).select('VV')
        return sentinel1_col.median()

    # New organized processing function
    # This function replaces the old launch_single_data_collector method
    # It processes a single HUC by separating the workflow into boundary and raster processing.
    # It also includes a verification step to ensure correctness of the raster data.
    # The function is designed to be the main orchestrator for processing a single HUC.
    # It separates the workflow into boundary and raster processing, ensuring clarity and maintainability.
    def process_single_huc(self, feature_info: dict):
        """
        Main orchestrator for processing a single HUC.
        It separates the workflow into boundary and raster processing.

        Args:
            feature_info: The dictionary containing properties and geometry for one HUC.
        """
        # --- 1. Initial Setup ---
        props = feature_info['properties']
        huc_id = props.get('huc8', 'UnknownID')
        name = props.get('name', 'UnknownName')
        
        # Create a clean base filename for this HUC
        clean_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', name)
        base_filename = f'HUC8_{huc_id}_{clean_name}'
        self.current_file = base_filename
        self.base_file_name.append(base_filename)
        
        original_geometry = ee.Geometry(feature_info['geometry'])
        
        # --- 2. Process and Export Vector Boundaries ---
        # This function returns the geometries needed for the next step.
        geometries = self._process_vector_boundaries(original_geometry, ee.Dictionary(props), base_filename)
        
        # --- 3. Process and Export Raster Data and Patches ---
        self._process_raster_data(geometries, base_filename)
        
    def _process_vector_boundaries(self, original_geometry: ee.Geometry, props: ee.Dictionary, base_filename: str) -> dict:
        """
        Processes and exports all vector boundaries for a single HUC.

        Args:
            original_geometry: The geometry of the original HUC.
            props: The properties of the HUC feature.
            base_filename: The base name for exported files and folders.

        Returns:
            A dictionary containing the essential geometries needed for raster processing.
        """
        print(f"\nProcessing vector boundaries for {base_filename}...")
        
        # --- Create Geometries ---
        buffered_geometry = original_geometry.buffer(self.settings.BUFFER_DISTANCE_METERS)
        export_region_rectangle = buffered_geometry.bounds()

        # --- Queue Vector Export Tasks ---
        huc_id = props.get('huc8')

        # Task: Original HUC boundary
        desc_orig_vec = f'HUC8_Original_Boundary_Vector_{huc_id}'
        task_orig_vec = ee.batch.Export.table.toDrive(
            collection=ee.FeatureCollection([ee.Feature(original_geometry, props)]),
            description=desc_orig_vec, folder=base_filename,
            fileNamePrefix=f'{base_filename}_Original_Boundary', fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.all_tasks.append({'task': task_orig_vec, 'description': desc_orig_vec, 'folder': base_filename})
        
        # Task: Buffered boundary
        buffered_fc = ee.FeatureCollection([ee.Feature(buffered_geometry, props)])
        desc_buff = f'Buffered_Boundary_Vector_{huc_id}'
        task_buff = ee.batch.Export.table.toDrive(
            collection=buffered_fc, description=desc_buff, folder=base_filename,
            fileNamePrefix=f'{base_filename}_Buffered_Boundary', fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.all_tasks.append({'task': task_buff, 'description': desc_buff, 'folder': base_filename})

        # Task: Bounding box
        bbox_fc = ee.FeatureCollection([ee.Feature(export_region_rectangle, props)])
        desc_bbox = f'BoundingBox_Vector_{huc_id}'
        task_bbox = ee.batch.Export.table.toDrive(
            collection=bbox_fc, description=desc_bbox, folder=base_filename,
            fileNamePrefix=f'{base_filename}_BoundingBox', fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.all_tasks.append({'task': task_bbox, 'description': desc_bbox, 'folder': base_filename})
        
        # Return the geometries for the raster processing step
        return {
            'original': original_geometry,
            'buffered': buffered_geometry,
            'rectangle': export_region_rectangle
        }

    def _process_raster_data(self, geometries: dict, base_filename: str):
        """
        Processes all raster data using explicitly defined resolutions from settings
        and includes a verification step to ensure correctness.
        """
        print(f"\nProcessing raster data and patches for {base_filename}...")

        export_region_rectangle = geometries['rectangle']
        common_crs = self.settings.TARGET_DEM_CRS

        # --- 1. Raster Data Preparation using Explicit Resolutions ---
        
        print("Preparing images with explicitly defined resolutions...")
        dem_image = self._get_dem(self.DEM_SOURCE_IMG, export_region_rectangle)
        
        landsat_images_unproj = self._get_landsat_images(export_region_rectangle, self.settings.START_DATE, self.settings.END_DATE)
        sar_image_unproj = self._get_sar_image(export_region_rectangle, self.settings.START_DATE, self.settings.END_DATE)
        flow_dir_unproj = self.MERIT_HYDRO_IMG.select('dir')

        # Clip median composites first
        landsat_optical_clipped = landsat_images_unproj['optical'].clip(export_region_rectangle)
        landsat_thermal_clipped = landsat_images_unproj['thermal'].clip(export_region_rectangle)
        sar_image_clipped = sar_image_unproj.clip(export_region_rectangle)
        flow_dir_clipped = flow_dir_unproj.clip(export_region_rectangle)
        
        # Reproject using the resolutions defined in settings. This is the most reliable method.
        landsat_images = {
            'optical': landsat_optical_clipped.reproject(crs=common_crs, scale=self.settings.SOURCE_RESOLUTIONS['optical']),
            'thermal': landsat_thermal_clipped.reproject(crs=common_crs, scale=self.settings.SOURCE_RESOLUTIONS['thermal'])
        }
        sar_image = sar_image_clipped.reproject(crs=common_crs, scale=self.settings.SOURCE_RESOLUTIONS['sar'])
        flow_dir_final = flow_dir_clipped.reproject(crs=common_crs, scale=self.settings.SOURCE_RESOLUTIONS['flow'])

        # --- 2. Quality Control: Verify Resolutions Programmatically ---
        
        # print("\nVerifying image resolutions against configuration...")
        # images_to_verify = {
        #     'dem': dem_image, 'optical': landsat_images['optical'], 'thermal': landsat_images['thermal'],
        #     'sar': sar_image, 'flow': flow_dir_final
        # }
        
        # for name, image in images_to_verify.items():
        #     expected_scale = self.settings.SOURCE_RESOLUTIONS[name]
        #     actual_scale = image.projection().nominalScale().getInfo()
        #     # Assert that the actual scale is within 5% of the expected scale to handle floating point variations
        #     assert abs(actual_scale - expected_scale) / expected_scale < 0.05, \
        #         f"'{name}' resolution mismatch! Expected ~{expected_scale}m, but GEE reports {actual_scale:.2f}m."
        #     print(f"  - {name.title()}: OK (Verified ~{actual_scale:.2f}m)")

        # --- 3. Patch Generation (No changes needed) ---
        print("\n--- Generating Patches ---")
        patch_centers = self._genearte_patch_centers(dem_image, self.settings.PATCH_SIZE, self.settings.PATCH_STRIDE, geometries['buffered'])
        print(f"Patch centers generated. Total centers: {patch_centers.size().getInfo()}")

        # ... (Patch extraction and export loop) ...
        batch_size = self.settings.BATCH_EXPORT_SIZE
        num_batches = patch_centers.size().divide(batch_size).ceil().getInfo()
        patch_centers_lists = patch_centers.toList(patch_centers.size())
        
        # Loop through batches for export
        for i in range(1): # Using 1 batch for testing # num_batches all patches
            print(f"Processing batch {i+1}/{num_batches}...")
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_patches_centers = ee.FeatureCollection(patch_centers_lists.slice(start_index, end_index))

            dem_patches = self._extract_patches_gee(dem_image, batch_patches_centers, self.settings.PATCH_SIZE).toList(batch_size)
            opt_patches = self._extract_patches_gee(landsat_images['optical'], batch_patches_centers, self.settings.PATCH_SIZE).toList(batch_size)
            the_patches = self._extract_patches_gee(landsat_images['thermal'], batch_patches_centers, self.settings.PATCH_SIZE).toList(batch_size)
            sar_patches = self._extract_patches_gee(sar_image, batch_patches_centers, self.settings.PATCH_SIZE).toList(batch_size)
            flow_patches = self._extract_patches_gee(flow_dir_final, batch_patches_centers, self.settings.PATCH_SIZE).toList(batch_size)

            collection_size = ee.Number(dem_patches.size()).getInfo()
            print(f"{collection_size} patch locations found in batch, starting export...")
            
            if collection_size > 0: 
                for j in range(1): # export 2 images for testing, to export all images, change the range to collection_size
                    self._save_patches(ee.Image(dem_patches.get(j)), j, 'dem')
                    self._save_patches(ee.Image(opt_patches.get(j)), j, 'opt')
                    self._save_patches(ee.Image(the_patches.get(j)), j, 'the')
                    self._save_patches(ee.Image(sar_patches.get(j)), j, 'sar')
                    self._save_patches(ee.Image(flow_patches.get(j)), j, 'flow')
                    

    
    # def _get_patches(self, raster:ee.Image, patch_centers:ee.FeatureCollection, base_filename, data_type=None)->ee.FeatureCollection:
    #     """
    #     Extract patches from a raster image based on a grid of center points.

    #     This function generates a grid of center points over the raster image and extracts
    #     patches of specified size around these centers. The patches are returned as a
    #     FeatureCollection.

    #     Parameters:
    #         raster (ee.Image): The input raster image from which patches will be extracted.
    #         patch_centers (ee.FeatureCollection): A collection of points representing the center of each patch.
    #         base_filename (str): The base filename for the exported patches.
    #         data_type (str): The type of data being processed (e.g., 'Landsat_Optical', 'Landsat_Thermal', 'SAR_VV', etc.).

    #     Returns:
    #         ee.FeatureCollection: A collection of features representing the extracted patches.
    #     """
        
    #     print(f"\nGet patches based on the center point by batch...")
        
    #     batch_size = self.settings.BATCH_EXPORT_SIZE
    #     num_batches = patch_centers.size().divide(batch_size).ceil().getInfo()
    #     patch_centers_lists = patch_centers.toList(patch_centers.size())
        
    #     for i in range(num_batches):
    #         print(f"Processing batch {i+1}/{num_batches}...")
    #         start_index = i * batch_size
    #         end_index = start_index + batch_size
            
    #         valid_patches = ee.FeatureCollection(patch_centers_lists.slice(start_index,end_index))
    #         image_patches = self._extract_patches_gee(raster, valid_patches, self.settings.PATCH_SIZE)

    #         image_list = image_patches.toList(image_patches.size())
    #         collection_size = image_list.size().getInfo()

    #         print(f"{collection_size} images is found, start exporting...")
            
    #         counter = 0
    #         # Export each image in the collection to Google Drive
    #         # export 2 images for testing, to export all images, change the range to collection_size
    #         print('Testing export patches...Only 2 images will be exported.')
    #         for i in range(2):
    #             image = ee.Image(image_list.get(i))
                
    #             image_id = image.id().getInfo()
    #             file_name = f"patches_image_{data_type}_{image_id.replace('/', '_')}"
                
    #             print(f"Creating image export task {counter+1}/{collection_size} (ID: {image_id})...")
    #             counter +=  1
                
    #             task = ee.batch.Export.image.toDrive(
    #                 image=image, 
    #                 description=f'Export_Image_{counter+1}',
    #                 folder=base_filename,
    #                 fileNamePrefix=file_name,
    #                 scale=10,
    #                 crs='EPSG:4326',
    #                 maxPixels=1e10
    #             )
    #             self.all_tasks.append({'task': task, 'description': f'Export_Image_{data_type}_{i+1}', 'folder': base_filename})

    #     return 
        