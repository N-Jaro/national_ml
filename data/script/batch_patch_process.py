# batch_patch_process.py

import ee
import numpy as np
import os
import time
import json
from tqdm import tqdm
from config import Settings
import itertools

class BatchPatchProcessor:
    """
    Optimized patch processor that uses GEE batch exports to minimize requests.
    """
    
    def __init__(self, center_points: list, output_folder: str, settings: Settings, huc_id: str):
        self.center_points_list = center_points
        self.output_folder = output_folder
        self.settings = settings
        self.huc_id = huc_id
        self.all_tasks = []
        self._initialize_gee()
        
    def _initialize_gee(self):
        try:
            ee.Initialize(project=self.settings.GEE_PROJECT_ID)
        except Exception:
            pass

    def _mask_l8sr_clouds(self, image: ee.Image) -> ee.Image:
        """Cloud-masks a Landsat 8/9 Collection 2 SR image."""
        qa = image.select('QA_PIXEL')
        cloud_shadow_bit_mask = 1 << 4
        clouds_bit_mask = 1 << 3
        mask = qa.bitwiseAnd(cloud_shadow_bit_mask).eq(0).And(qa.bitwiseAnd(clouds_bit_mask).eq(0))
        optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
        thermal_band = image.select('ST_B10').multiply(0.00341802).add(149.0)
        return image.addBands(optical_bands, None, True).addBands(thermal_band, None, True).updateMask(mask)

    def _get_global_composites(self):
        """
        Create global composites once and reuse them across all HUCs.
        This significantly reduces redundant processing.
        """
        print("  Creating global satellite composites (once per pipeline run)...")
        
        # Use a larger region that covers multiple HUCs
        global_region = ee.Geometry.Rectangle([-180, -85, 180, 85])  # Global coverage
        
        # Create composites
        landsat_col = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2') \
            .filterDate(self.settings.START_DATE, self.settings.END_DATE) \
            .map(self._mask_l8sr_clouds)
        
        optical_composite = landsat_col.select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']).median()
        thermal_composite = landsat_col.select('ST_B10').median()
        
        sar_col = ee.ImageCollection('COPERNICUS/S1_GRD') \
            .filterDate(self.settings.START_DATE, self.settings.END_DATE) \
            .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
            .filter(ee.Filter.eq('instrumentMode', 'IW')).select('VV')
        sar_composite = sar_col.median()
        
        # DEM
        dem_collection = ee.ImageCollection(self.settings.DEM_SOURCE_IMG_NAME)
        dem_composite = dem_collection.mosaic().select('elevation')
        
        # AlphaEarth - Use more aggressive processing to ensure single output
        composites_dict = {
            'dem': dem_composite,
            'optical': optical_composite,
            'thermal': thermal_composite,
            'sar': sar_composite
        }
        
        # Only add AlphaEarth if enabled
        if self.settings.ENABLE_ALPHAEARTH:
            year = self.settings.ALPHA_EARTH_YEAR
            alphaearth_col = ee.ImageCollection('GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL')
            
            # Filter and reduce to single image more explicitly
            alphaearth_filtered = alphaearth_col.filter(ee.Filter.calendarRange(year, year, 'year'))
            
            # Use all 64 bands but ensure single output with proper band naming
            band_limit = self.settings.ALPHAEARTH_BANDS_LIMIT
            band_indices = list(range(band_limit))
            band_names = [f'emb_{i:02d}' for i in range(band_limit)]  # emb_00, emb_01, etc.
            
            # Use first() and select specific bands to force single image
            alphaearth_composite = alphaearth_filtered.first().select(band_indices).rename(band_names)
            composites_dict['alphaearth'] = alphaearth_composite
        
        return composites_dict

    def _create_patch_collection(self, composites: dict, center_points_batch: list):
        """
        Create a FeatureCollection with patches as features for batch export.
        """
        patch_features = []
        target_crs = self.settings.TARGET_DEM_CRS
        
        for i, center_point in enumerate(center_points_batch):
            center_geom = ee.Geometry.Point([center_point['lon'], center_point['lat']])
            
            # Calculate patch bounds for each resolution
            patch_bounds = {}
            for data_type, scale in self.settings.SOURCE_RESOLUTIONS.items():
                patch_radius_meters = self.settings.PATCH_SIZE * scale / 2.0
                patch_bounds[data_type] = center_geom.buffer(patch_radius_meters).bounds()
            
            # Sample each composite at the center point
            sampled_data = {}
            for data_type, composite in composites.items():
                if data_type in self.settings.SOURCE_RESOLUTIONS:
                    scale = self.settings.SOURCE_RESOLUTIONS[data_type]
                    bounds = patch_bounds[data_type]
                    
                    # Clip and reproject the composite
                    clipped = composite.clip(bounds).reproject(
                        crs=target_crs, 
                        scale=scale
                    )
                    sampled_data[data_type] = clipped
            
            # Create feature with all data as bands
            feature_props = {
                'patch_id': i,
                'lon': center_point['lon'],
                'lat': center_point['lat'],
                'patch_name': center_point.get('patch_name', f'patch_{i}')
            }
            
            patch_feature = ee.Feature(center_geom, feature_props)
            patch_features.append(patch_feature)
        
        return ee.FeatureCollection(patch_features), sampled_data

    def _export_patch_batch(self, batch_id: int, composites: dict, center_points_batch: list):
        """
        Export a batch of patches as separate image exports for each data type.
        """
        print(f"    Exporting batch {batch_id} ({len(center_points_batch)} patches)...")
        
        target_crs = self.settings.TARGET_DEM_CRS
        gdrive_folder = f"{self.huc_id}_patches_batch_{batch_id}"
        
        # Create a region that encompasses all patches in this batch
        all_points = [ee.Geometry.Point([p['lon'], p['lat']]) for p in center_points_batch]
        batch_region = ee.FeatureCollection(all_points).geometry().buffer(10000).bounds()
        
        # Export each data type separately
        for data_type, composite in composites.items():
            if data_type not in self.settings.SOURCE_RESOLUTIONS:
                continue
                
            scale = self.settings.SOURCE_RESOLUTIONS[data_type]
            
            # Clip and reproject
            clipped_composite = composite.clip(batch_region).reproject(
                crs=target_crs, 
                scale=scale
            )
            
            # Special handling for AlphaEarth to prevent multiple files
            if data_type == 'alphaearth':
                # Ensure all 64 bands are properly named and exported as single file
                band_limit = self.settings.ALPHAEARTH_BANDS_LIMIT
                band_names = [f'emb_{i:02d}' for i in range(band_limit)]
                clipped_composite = clipped_composite.select(band_names)
                max_pixels = min(self.settings.MAX_PIXELS_PER_EXPORT, 5e7)  # Larger limit for all 64 bands
            else:
                max_pixels = self.settings.MAX_PIXELS_PER_EXPORT
            
            # Create export task
            export_desc = f'Patches_Batch_{batch_id}_{data_type}_HUC_{self.huc_id}'
            
            # Create export task with additional parameters for large multi-band data
            export_params = {
                'image': clipped_composite,
                'description': export_desc,
                'folder': gdrive_folder,
                'fileNamePrefix': f'batch_{batch_id}_{data_type}',
                'fileFormat': 'GeoTIFF',
                'region': batch_region,
                'scale': scale,
                'crs': target_crs,
                'maxPixels': max_pixels,
                'skipEmptyTiles': True,  # Skip empty tiles to reduce file count
            }
            
            # Add special parameters for AlphaEarth to force single file
            if data_type == 'alphaearth':
                export_params.update({
                    'fileDimensions': 2048,      # Smaller tile size for 64-band data
                    'formatOptions': {
                        'cloudOptimized': True,   # Use cloud-optimized GeoTIFF
                        'tiled': True            # Enable tiling
                    }
                })
            else:
                export_params['fileDimensions'] = 4096  # Standard tile size for other data
            
            export_task = ee.batch.Export.image.toDrive(**export_params)
            
            self.all_tasks.append({
                'task': export_task,
                'description': export_desc,
                'batch_id': batch_id,
                'data_type': data_type,
                'center_points': center_points_batch
            })

    def run(self):
        """
        Execute the optimized batch patch processing workflow.
        """
        if not self.settings.USE_BATCH_EXPORTS:
            print("  Batch exports disabled, falling back to individual downloads...")
            # Fall back to original method
            return self._run_individual_downloads()
        
        print(f"  Processing {len(self.center_points_list)} patches using batch exports...")
        
        # Create global composites once
        composites = self._get_global_composites()
        
        # Split center points into batches
        batch_size = self.settings.PATCHES_PER_EXPORT
        center_point_batches = [
            self.center_points_list[i:i + batch_size] 
            for i in range(0, len(self.center_points_list), batch_size)
        ]
        
        print(f"  Split into {len(center_point_batches)} batches of up to {batch_size} patches each")
        
        # Submit batch export tasks
        for batch_id, center_points_batch in enumerate(center_point_batches):
            self._export_patch_batch(batch_id, composites, center_points_batch)
            
            # Add delay between batch submissions to avoid overwhelming GEE
            time.sleep(self.settings.REQUEST_DELAY_SECONDS)
        
        # Start all tasks
        print(f"  Starting {len(self.all_tasks)} export tasks...")
        for task_info in self.all_tasks:
            task_info['task'].start()
        
        # Monitor task completion
        self._monitor_tasks()
        
        # After all exports are complete, download and process batch files into patches
        print(f"  All batch exports complete. Starting batch file processing...")
        self._process_batch_files()
        
        print(f"  Batch processing complete for HUC {self.huc_id}")

    def _monitor_tasks(self):
        """
        Monitor the progress of all export tasks.
        """
        print("  Monitoring export tasks...")
        completed_tasks = set()
        
        while len(completed_tasks) < len(self.all_tasks):
            for i, task_info in enumerate(self.all_tasks):
                if i in completed_tasks:
                    continue
                    
                task = task_info['task']
                state = task.status()['state']
                
                if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    completed_tasks.add(i)
                    print(f"    Task {task_info['description']}: {state}")
                    
                    if state == 'FAILED':
                        error_msg = task.status().get('error_message', 'Unknown error')
                        print(f"      Error: {error_msg}")
            
            if len(completed_tasks) < len(self.all_tasks):
                print(f"    {len(completed_tasks)}/{len(self.all_tasks)} tasks complete. Waiting...")
                time.sleep(30)  # Check every 30 seconds

    def _run_individual_downloads(self):
        """
        Fallback method using individual downloads with rate limiting.
        """
        from patch_process import PatchProcessor
        
        # Use original method but with rate limiting
        original_processor = PatchProcessor(
            self.center_points_list, 
            self.output_folder, 
            self.settings, 
            self.huc_id
        )
        
        # Override the parallel processing to add rate limiting
        center_point_features = [ee.Feature(ee.Geometry.Point([p['lon'], p['lat']])) for p in self.center_points_list]
        aoi_for_composites = ee.FeatureCollection(center_point_features).geometry().buffer(10000)
        
        composites = original_processor._get_global_composites()
        
        # Process patches sequentially with delays to avoid rate limits
        print(f"  Processing {len(self.center_points_list)} patches with rate limiting...")
        
        for i, center_point in enumerate(tqdm(self.center_points_list, desc="Downloading patches")):
            try:
                center_feature = ee.Feature(ee.Geometry.Point([center_point['lon'], center_point['lat']]))
                original_processor._process_single_patch((i, center_feature, composites))
                
                # Add delay between downloads
                if i < len(self.center_points_list) - 1:  # Don't delay after last patch
                    time.sleep(self.settings.REQUEST_DELAY_SECONDS)
                    
            except Exception as e:
                print(f"    Error processing patch {i}: {e}")
                continue

    def _process_batch_files(self):
        """
        Download batch files from Google Drive and extract individual patches.
        """
        print("  Processing batch files into individual patches...")
        
        # Import required libraries for post-processing
        import rasterio
        from rasterio.windows import Window
        import numpy as np
        from gdrive_manager import GoogleDriveManager
        
        # Initialize Google Drive manager
        drive_manager = GoogleDriveManager(settings=self.settings)
        
        # Group tasks by batch for processing
        batches = {}
        for task_info in self.all_tasks:
            batch_id = task_info['batch_id']
            if batch_id not in batches:
                batches[batch_id] = {
                    'center_points': task_info['center_points'],
                    'files': {}
                }
            batches[batch_id]['files'][task_info['data_type']] = task_info['description']
        
        # Process each batch
        for batch_id, batch_info in batches.items():
            print(f"    Processing batch {batch_id}...")
            
            # Download batch files from Google Drive
            batch_folder = f"{self.huc_id}_patches_batch_{batch_id}"
            local_batch_folder = os.path.join(self.output_folder, 'temp_batch_files', batch_folder)
            os.makedirs(local_batch_folder, exist_ok=True)
            
            try:
                # Download all batch files for this batch
                drive_manager.download_folder_contents(
                    gdrive_folder_name=batch_folder,
                    local_destination=local_batch_folder
                )
                
                # Extract patches from batch files
                self._extract_patches_from_batch(
                    batch_id, 
                    batch_info['center_points'], 
                    local_batch_folder
                )
                
                # Clean up temporary batch files
                import shutil
                shutil.rmtree(local_batch_folder)
                print(f"    Batch {batch_id} processing complete.")
                
            except Exception as e:
                print(f"    Error processing batch {batch_id}: {e}")
                continue

    def _extract_patches_from_batch(self, batch_id: int, center_points: list, batch_folder: str):
        """
        Extract individual patches from batch GeoTIFF files.
        """
        import rasterio
        from rasterio.windows import from_bounds
        import numpy as np
        
        print(f"      Extracting {len(center_points)} patches from batch {batch_id}...")
        
        # Find batch files in the folder
        batch_files = {}
        for filename in os.listdir(batch_folder):
            if filename.endswith('.tif'):
                # Extract data type from filename: batch_0_dem.tif -> dem
                data_type = filename.split('_')[-1].replace('.tif', '')
                batch_files[data_type] = os.path.join(batch_folder, filename)
        
        if not batch_files:
            print(f"      No batch files found in {batch_folder}")
            return
        
        # Process each center point to extract its patch
        for i, center_point in enumerate(center_points):
            patch_data = {}
            patch_index = batch_id * self.settings.PATCHES_PER_EXPORT + i
            
            # Extract patch from each data type
            for data_type, file_path in batch_files.items():
                try:
                    patch_array = self._extract_single_patch(
                        file_path, 
                        center_point, 
                        data_type
                    )
                    
                    if patch_array is not None:
                        patch_data[data_type] = patch_array
                        
                        # Save DEM as georeferencing template
                        if data_type == 'dem':
                            template_path = os.path.join(self.output_folder, f'patch_{patch_index}_georef_template.tif')
                            self._save_patch_as_geotiff(patch_array, file_path, center_point, template_path, data_type)
                            
                except Exception as e:
                    print(f"        Error extracting {data_type} for patch {patch_index}: {e}")
                    continue
            
            # Save patch data if we have any valid data
            if patch_data:
                patch_file = os.path.join(self.output_folder, f'patch_{patch_index}.npz')
                np.savez_compressed(patch_file, **patch_data)
            else:
                print(f"        Warning: No valid data for patch {patch_index}")

    def _extract_single_patch(self, file_path: str, center_point: dict, data_type: str):
        """
        Extract a single patch from a batch GeoTIFF file.
        """
        import rasterio
        from rasterio.windows import from_bounds
        
        try:
            with rasterio.open(file_path) as src:
                # Calculate patch bounds in the file's CRS
                center_lon, center_lat = center_point['lon'], center_point['lat']
                
                # Transform center point to file CRS if needed
                from rasterio.warp import transform_bounds
                if src.crs.to_string() != 'EPSG:4326':
                    # Transform from WGS84 to file CRS
                    from pyproj import Transformer
                    transformer = Transformer.from_crs('EPSG:4326', src.crs, always_xy=True)
                    center_x, center_y = transformer.transform(center_lon, center_lat)
                else:
                    center_x, center_y = center_lon, center_lat
                
                # Calculate patch bounds based on resolution and patch size
                scale = self.settings.SOURCE_RESOLUTIONS.get(data_type, 30)
                patch_radius_meters = self.settings.PATCH_SIZE * scale / 2.0
                
                # Create patch bounds
                left = center_x - patch_radius_meters
                right = center_x + patch_radius_meters  
                bottom = center_y - patch_radius_meters
                top = center_y + patch_radius_meters
                
                # Create window from bounds
                window = from_bounds(left, bottom, right, top, src.transform)
                
                # Read the patch
                patch_array = src.read(window=window)
                
                # Handle different array shapes
                if patch_array.ndim == 3:
                    # Multi-band data (bands, height, width) -> (height, width, bands)
                    if patch_array.shape[0] < patch_array.shape[1]:  # More bands than pixels
                        patch_array = np.transpose(patch_array, (1, 2, 0))
                elif patch_array.ndim == 2:
                    # Single band data - keep as is
                    pass
                
                # Resize to exact patch size if needed
                target_size = (self.settings.PATCH_SIZE, self.settings.PATCH_SIZE)
                if patch_array.shape[:2] != target_size:
                    from skimage.transform import resize
                    if patch_array.ndim == 3:
                        patch_array = resize(patch_array, target_size + (patch_array.shape[2],), preserve_range=True)
                    else:
                        patch_array = resize(patch_array, target_size, preserve_range=True)
                    patch_array = patch_array.astype(np.float32)
                
                return patch_array
                
        except Exception as e:
            print(f"        Error reading patch from {file_path}: {e}")
            return None

    def _save_patch_as_geotiff(self, patch_array, source_file, center_point, output_path, data_type):
        """
        Save a patch as a GeoTIFF file for georeferencing template.
        """
        import rasterio
        from rasterio.transform import from_bounds
        
        try:
            # Calculate patch bounds
            scale = self.settings.SOURCE_RESOLUTIONS.get(data_type, 30)
            patch_radius_meters = self.settings.PATCH_SIZE * scale / 2.0
            
            center_lon, center_lat = center_point['lon'], center_point['lat']
            
            # Transform to target CRS
            from pyproj import Transformer
            transformer = Transformer.from_crs('EPSG:4326', self.settings.TARGET_DEM_CRS, always_xy=True)
            center_x, center_y = transformer.transform(center_lon, center_lat)
            
            left = center_x - patch_radius_meters
            right = center_x + patch_radius_meters
            bottom = center_y - patch_radius_meters
            top = center_y + patch_radius_meters
            
            # Create transform
            transform = from_bounds(left, bottom, right, top, 
                                  self.settings.PATCH_SIZE, self.settings.PATCH_SIZE)
            
            # Write GeoTIFF
            with rasterio.open(
                output_path, 'w',
                driver='GTiff',
                height=self.settings.PATCH_SIZE,
                width=self.settings.PATCH_SIZE,
                count=1,
                dtype=patch_array.dtype,
                crs=self.settings.TARGET_DEM_CRS,
                transform=transform
            ) as dst:
                if patch_array.ndim == 3:
                    dst.write(patch_array[:,:,0], 1)  # Write first band for template
                else:
                    dst.write(patch_array, 1)
                    
        except Exception as e:
            print(f"        Error saving GeoTIFF template: {e}")
            return None
            
