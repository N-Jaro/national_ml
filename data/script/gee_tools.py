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
        """Launch all export tasks for selected HUCs"""
        """Test function to randomly select HUCs and export their data"""

        selected_hucs = self.HUC8_COL.randomColumn('random').sort('random').limit(self.settings.NUMBER_OF_HUCS)
        
        # 1. export original HUC boundaries to Google Drive
        desc_global = f'ALL_HUC8_Original_Boundaries_Export_{self.settings.NUMBER_OF_HUCS}'
        task_global = ee.batch.Export.table.toDrive(
            collection=selected_hucs,
            description=desc_global,
            folder=self.settings.DRIVE_FOLDER,
            fileNamePrefix=f'Selected_{self.settings.NUMBER_OF_HUCS}_HUC8_Original_Boundaries',
            fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.all_tasks.append({'task': task_global, 'description': desc_global})

        selected_hucs_list_info = selected_hucs.select(['huc8', 'states', 'name']).toList(self.settings.NUMBER_OF_HUCS).getInfo()
        print(f"Obtain {len(selected_hucs_list_info)} information of one HUC. Start creating export tasks for each HUC...")

        # 2. add tasks for each selected HUC
        for feature_info in selected_hucs_list_info:
            self.launch_single_data_collector(feature_info,self.settings.PATCH)
            
        # 3. launch the global HUC boundary export task
        print(f"\n--- {len(self.all_tasks)}tasks have been created, started... ---")
        for item in self.all_tasks:
            item['task'].start()
            print(f"Initiated task: {item['description']} (ID: {item['task'].id})")

    def monitor_and_organize_tasks(self):
        """Monitor the status of all launched tasks and organize them in Google Drive"""
        print("\n---Monitor Mode ---")
    
        while True:
            active_tasks = []
            all_done = True
            for item in self.all_tasks:
                # only check tasks that are not already completed or failed
                if item.get('state') != 'COMPLETED' and item.get('state') != 'FAILED':
                    try:
                        status = item['task'].status()
                        current_state = status['state']
                        item['state'] = current_state # update state in the item

                        if current_state in ['RUNNING', 'READY']:
                            active_tasks.append(item['description'])
                            all_done = False 
                        elif current_state == 'FAILED':
                            print(f"!!! Task Failure: {item['description']} !!!")
                            print(f"    Erro message: {status.get('error_message', 'Unknown error')}")

                    except ee.ee_exception.EEException as e:
                        # catch GEE-specific errors
                        error_str = str(e)
                        if '503' in error_str or 'unavailable' in error_str:
                            # if it's a temporary server error, we can retry later
                            print(f"Warning: Temory server error(503) occur when checking task'{item['description']}' status. Retrying later...")
                            active_tasks.append(item['description']) # keep it in the active list
                            all_done = False
                        else:
                            # for other errors, we mark the task as failed
                            print(f"!!! Task Failed (Unknow GEE error): {item['description']} !!!")
                            print(f"Error Message: {e}")
                            item['state'] = 'FAILED'
            
            if all_done:
                print("ALL Task FINISHED!")
                break
            
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Ongoing task: ({len(active_tasks)}/{len(self.all_tasks)}): {', '.join(active_tasks[:3])}...")
            time.sleep(60) # check every 60 seconds
        
        print("\n--- Get Data Patches ---")

        
        # Because all tasks are parallel, multiple folders with the same name will appear in google drive and need to be merged.
        print("\n--- Merging Google Drive folders ---")
        for name in self.base_file_name:
            print(f"Merging folders: {name}")
            self.drive_manager.merge_duplicate_folders(name)
            # move all files to the main folder
            print(name)
            self.drive_manager.move_folder(name, self.settings.DRIVE_FOLDER)
            
        print("\n--- Download files from Google Drive ---")
        if not os.path.exists(self.settings.LOCAL_DOWNLOAD_DIR):
            os.makedirs(self.settings.LOCAL_DOWNLOAD_DIR)
        
        
        # get the main folder ID
        main_folder_id = self.drive_manager.get_gdrive_folder_id(self.settings.DRIVE_FOLDER)
        if not main_folder_id:
            print(f"Error：Can not found main folder in Google Drive '{self.settings.DRIVE_FOLDER}'")
            return

        self.drive_manager.download_folder_recursively(main_folder_id, self.settings.LOCAL_DOWNLOAD_DIR)
        

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

        
    def _save_patches(self,raster:ee.Image,index,name):
        # image = 
                
        image_id = raster.id().getInfo()
        file_name = f"patches_image_{name}_{image_id.replace('/', '_')}"
        
        # print(f"Creating image export task {counter+1}/{collection_size} (ID: {image_id})...")
        # counter +=  1
        
        task = ee.batch.Export.image.toDrive(
            image=raster, 
            description=f'Export_Image_{index+1}',
            folder=self.current_file,
            fileNamePrefix=file_name,
            scale=10,
            crs='EPSG:4326', # TODO: dangrous
            maxPixels=1e10
        )
        self.all_tasks.append({'task': task, 'description': f'Export_Image_{name}_{index+1}', 'folder': self.current_file})

        return
    
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
            maxPixels=5e8  # c
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

    # Delete the old launch_single_data_collector function and replace it with this:

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
        Processes all raster data: prepares full-size images, discovers resolutions,
        and generates and exports patches.

        Args:
            geometries: A dictionary containing 'buffered' and 'rectangle' geometries.
            base_filename: The base name for exported files and folders.
        """
        print(f"\nProcessing raster data and patches for {base_filename}...")

        # Unpack geometries needed for raster operations
        original_geometry = geometries['original']
        buffered_geometry = geometries['buffered']
        export_region_rectangle = geometries['rectangle']
        common_crs = self.settings.TARGET_DEM_CRS

        # --- Raster Data Preparation ---
        dem_image = self._get_dem(self.DEM_SOURCE_IMG, export_region_rectangle)
        
        landsat_images_unproj = self._get_landsat_images(export_region_rectangle, self.settings.START_DATE, self.settings.END_DATE)
        landsat_optical_proj = landsat_images_unproj['optical'].reproject(crs=common_crs)
        landsat_thermal_proj = landsat_images_unproj['thermal'].reproject(crs=common_crs)
        landsat_images = {
            'optical': landsat_optical_proj.clip(export_region_rectangle),
            'thermal': landsat_thermal_proj.clip(export_region_rectangle)
        }
        
        sar_image_unproj = self._get_sar_image(export_region_rectangle, self.settings.START_DATE, self.settings.END_DATE)
        sar_image_proj = sar_image_unproj.reproject(crs=common_crs)
        sar_image = sar_image_proj.clip(export_region_rectangle)
        
        flow_dir_proj = self.MERIT_HYDRO_IMG.select('dir').reproject(crs=common_crs)
        flow_dir_final = flow_dir_proj.clip(export_region_rectangle)

        # --- Discover Resolutions Dynamically ---
        print("Discovering native resolutions from source images...")
        try:
            source_resolutions = {
                'dem': dem_image.projection().nominalScale().getInfo(),
                'optical': landsat_images['optical'].projection().nominalScale().getInfo(),
                'thermal': landsat_images['thermal'].projection().nominalScale().getInfo(),
                'sar': sar_image.projection().nominalScale().getInfo(),
                'flow': flow_dir_final.projection().nominalScale().getInfo()
            }
            print(f"Discovered Resolutions (m): {source_resolutions}")
        except Exception as e:
            print(f"Could not dynamically discover resolutions. Error: {e}. Falling back to settings.")
            source_resolutions = self.settings.SOURCE_RESOLUTIONS

        # --- Patch Generation ---
        print("--- Generating Patches ---")
        patch_centers = self._genearte_patch_centers(dem_image, self.settings.PATCH_SIZE, self.settings.PATCH_STRIDE, original_geometry)
        print(f"Patch centers generated. Total centers: {patch_centers.size().getInfo()}")

        # ... (Patch extraction and export loop) ...
        batch_size = self.settings.BATCH_EXPORT_SIZE
        num_batches = patch_centers.size().divide(batch_size).ceil().getInfo()
        patch_centers_lists = patch_centers.toList(patch_centers.size())
        
        # Loop through batches for export
        for i in range(1): # Using 1 batch for testing
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
                for j in range(1): # Exporting 1 patch for testing
                    self._save_patches(ee.Image(dem_patches.get(j)), j, 'dem', source_resolutions['dem'])
                    self._save_patches(ee.Image(opt_patches.get(j)), j, 'opt', source_resolutions['optical'])
                    self._save_patches(ee.Image(the_patches.get(j)), j, 'the', source_resolutions['thermal'])
                    self._save_patches(ee.Image(sar_patches.get(j)), j, 'sar', source_resolutions['sar'])
                    self._save_patches(ee.Image(flow_patches.get(j)), j, 'flow', source_resolutions['flow'])
                    
