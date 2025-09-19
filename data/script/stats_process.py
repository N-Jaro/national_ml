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
        optical_median = landsat_col.select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']).median()
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
        # First try the optimized combined approach
        try:
            all_stats = self._run_combined_stats()
        except Exception as e:
            print(f"  Combined stats approach failed: {e}")
            print("  Switching to memory-efficient sampling approach...")
            try:
                all_stats = self._calculate_stats_by_sampling()
            except Exception as sampling_e:
                print(f"  Sampling approach also failed: {sampling_e}")
                print("  Using minimal fallback statistics...")
                all_stats = self._minimal_stats_fallback()
        
        # Save results
        huc_patch_folder = os.path.join(self.settings.ROOT_OUTPUT_FOLDER, self.settings.PATCH_OUTPUT_FOLDER, self.huc_id)
        os.makedirs(huc_patch_folder, exist_ok=True)
        stats_path = os.path.join(huc_patch_folder, 'normalization_stats.json')
        
        with open(stats_path, 'w') as f:
            json.dump(all_stats, f, indent=4)
        print(f"  Saved statistics to '{stats_path}'")

    def _run_combined_stats(self):
        """
        Original combined statistics approach.
        """
        self._fetch_huc_geometry()
        
        # A buffered area is useful for fetching image composites to avoid edge effects
        aoi_for_composites = self.huc_geometry.buffer(10000)

        print("  Preparing GEE image composites for statistics...")
        
        # --- MODIFIED: Use ImageCollection and mosaic it ---
        dem_collection = ee.ImageCollection(self.settings.DEM_SOURCE_IMG_NAME)
        dem_image = dem_collection.mosaic().select('elevation')
        # --- END MODIFICATION ---
        
        landsat_composites = self._get_landsat_composite(aoi_for_composites)
        optical_image = landsat_composites['optical']
        thermal_image = landsat_composites['thermal']
        sar_image = self._get_sar_composite(aoi_for_composites)
        
        # Combine all images into a single multi-band image for one reduceRegion call
        print("  Combining all data sources for efficient statistics calculation...")
        
        # Rename bands to avoid conflicts
        dem_renamed = dem_image.rename(['dem_elevation'])
        optical_renamed = optical_image.rename(['opt_B2', 'opt_B3', 'opt_B4', 'opt_B5', 'opt_B6', 'opt_B7'])
        thermal_renamed = thermal_image.rename(['thermal_B10'])
        sar_renamed = sar_image.rename(['sar_VV'])
        
        # Reproject all to the same resolution for consistency (use coarsest resolution to reduce memory)
        target_scale = 60  # Use 60m to reduce memory usage
        target_crs = self.settings.TARGET_DEM_CRS
        
        dem_reproj = dem_renamed.reproject(crs=target_crs, scale=target_scale)
        optical_reproj = optical_renamed.reproject(crs=target_crs, scale=target_scale)
        thermal_reproj = thermal_renamed.reproject(crs=target_crs, scale=target_scale)
        sar_reproj = sar_renamed.reproject(crs=target_crs, scale=target_scale)
        
        # Combine into single image
        combined_image = dem_reproj.addBands([optical_reproj, thermal_reproj, sar_reproj])

        print("  Calculating normalization statistics...")
        reducers = ee.Reducer.minMax().combine(
            reducer2=ee.Reducer.mean(), sharedInputs=True
        ).combine(
            reducer2=ee.Reducer.stdDev(), sharedInputs=True
        )

        # Single reduceRegion call for all data
        try:
            all_stats_combined = combined_image.reduceRegion(
                reducer=reducers, 
                geometry=self.huc_geometry,
                scale=target_scale, 
                maxPixels=1e8,  # Reduced from 1e9
                bestEffort=True  # Allow best effort processing if exact region is too large
            ).getInfo()
            
            # Parse the combined results back into separate data sources
            all_stats = {
                'dem': {},
                'optical': {},
                'thermal': {},
                'sar': {}
            }
            
            # Parse combined results
            for stat_key, stat_value in all_stats_combined.items():
                if stat_key.startswith('dem_'):
                    clean_key = stat_key.replace('dem_', '')
                    all_stats['dem'][clean_key] = stat_value
                elif stat_key.startswith('opt_'):
                    clean_key = stat_key.replace('opt_', '')
                    all_stats['optical'][clean_key] = stat_value
                elif stat_key.startswith('thermal_'):
                    clean_key = stat_key.replace('thermal_', '')
                    all_stats['thermal'][clean_key] = stat_value
                elif stat_key.startswith('sar_'):
                    clean_key = stat_key.replace('sar_', '')
                    all_stats['sar'][clean_key] = stat_value
                    
        except Exception as e:
            print(f"    - Warning: Could not calculate combined stats. Error: {e}")
            print("    - Falling back to individual calculations with progressive scaling...")
            
            # Progressive fallback strategy with increasingly aggressive memory reduction
            all_stats = {}
            images_to_process = {
                'dem': dem_image, 'optical': optical_image, 
                'thermal': thermal_image, 'sar': sar_image
            }
            
            # Try multiple scales if memory issues persist
            scales_to_try = [120, 240, 480, 960]  # Progressively coarser scales
            pixel_limits = [1e6, 5e5, 1e5, 5e4]   # Progressively lower pixel limits
            
            for name, image in images_to_process.items():
                success = False
                for scale, max_pixels in zip(scales_to_try, pixel_limits):
                    try:
                        print(f"    - Trying {name} at {scale}m resolution with {max_pixels:.0e} max pixels...")
                        
                        # For optical data, process bands individually if still failing
                        if name == 'optical' and scale >= 240:
                            print(f"    - Processing optical bands individually for {name}...")
                            band_stats = {}
                            optical_bands = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
                            
                            for band in optical_bands:
                                try:
                                    single_band = image.select(band)
                                    band_stat = single_band.reduceRegion(
                                        reducer=reducers, 
                                        geometry=self.huc_geometry,
                                        scale=scale, 
                                        maxPixels=max_pixels, 
                                        bestEffort=True
                                    ).getInfo()
                                    band_stats.update(band_stat)
                                except Exception as band_e:
                                    print(f"      - Failed to process band {band}: {band_e}")
                                    continue
                            
                            if band_stats:
                                all_stats[name] = band_stats
                                success = True
                                break
                        else:
                            # Regular processing for other data types
                            stats = image.reduceRegion(
                                reducer=reducers, 
                                geometry=self.huc_geometry,
                                scale=scale, 
                                maxPixels=max_pixels, 
                                bestEffort=True
                            ).getInfo()
                            all_stats[name] = stats
                            success = True
                            break
                            
                    except Exception as e:
                        if "memory limit" in str(e).lower() or "too many pixels" in str(e).lower():
                            print(f"      - Scale {scale}m failed: {e}")
                            continue  # Try next coarser scale
                        else:
                            print(f"      - Unexpected error at scale {scale}m: {e}")
                            break  # Don't try coarser scales for non-memory errors
                
                if not success:
                    print(f"    - All scales failed for '{name}'. Using sampling approach...")
                    # Last resort: sample approach
                    try:
                        # Sample points within the HUC instead of using the full region
                        sample_points = self.huc_geometry.centroid().buffer(5000, 100).coordinates()
                        sample_region = ee.Geometry.Polygon(sample_points)
                        
                        stats = image.reduceRegion(
                            reducer=reducers,
                            geometry=sample_region,
                            scale=480,  # Very coarse
                            maxPixels=1e4,  # Very low limit
                            bestEffort=True
                        ).getInfo()
                        all_stats[name] = stats
                        print(f"    - Successfully sampled stats for '{name}' using centroid approach")
                    except Exception as final_e:
                        print(f"    - Final fallback failed for '{name}': {final_e}")
                        all_stats[name] = {}
        
        # Save results
        return all_stats

    def _minimal_stats_fallback(self):
        """
        Absolute minimal statistics calculation using only HUC centroid.
        """
        print("  Using minimal centroid-based statistics (last resort)...")
        
        self._fetch_huc_geometry()
        centroid = self.huc_geometry.centroid()
        
        # Create minimal composites
        dem_collection = ee.ImageCollection(self.settings.DEM_SOURCE_IMG_NAME)
        dem_image = dem_collection.mosaic().select('elevation')
        
        # Use very basic statistics - just sample at centroid
        all_stats = {}
        
        try:
            # Sample DEM at centroid
            dem_sample = dem_image.sample(centroid, 30).first().getInfo()
            all_stats['dem'] = {
                'elevation_mean': dem_sample['properties']['elevation'],
                'elevation_min': dem_sample['properties']['elevation'],
                'elevation_max': dem_sample['properties']['elevation'],
                'elevation_stdDev': 0
            }
        except Exception as e:
            print(f"    - Even centroid sampling failed: {e}")
            all_stats = {
                'dem': {}, 'optical': {}, 'thermal': {}, 'sar': {}
            }
        
        return all_stats
    
    def _calculate_stats_by_sampling(self):
        """
        Alternative method: Calculate statistics using systematic sampling instead of full region.
        This is much more memory efficient for large HUCs.
        """
        print("  Using sampling-based statistics calculation (memory efficient)...")
        
        self._fetch_huc_geometry()
        
        # Create a systematic sample grid within the HUC
        sample_scale = 1000  # 1km spacing for samples
        sample_points = self.huc_geometry.centroid().buffer(50000).coordinates()
        sample_region = ee.Geometry.Polygon(sample_points)
        
        # Create sample points on a grid
        grid_spacing = 5000  # 5km grid
        sample_grid = ee.FeatureCollection.randomPoints(
            region=self.huc_geometry, 
            points=100,  # Sample 100 random points
            seed=42
        )
        
        print("  Preparing GEE image composites for sampling...")
        
        # Use buffered area for composites
        aoi_for_composites = self.huc_geometry.buffer(10000)
        
        # Create composites
        dem_collection = ee.ImageCollection(self.settings.DEM_SOURCE_IMG_NAME)
        dem_image = dem_collection.mosaic().select('elevation')
        
        landsat_composites = self._get_landsat_composite(aoi_for_composites)
        optical_image = landsat_composites['optical']
        thermal_image = landsat_composites['thermal']
        sar_image = self._get_sar_composite(aoi_for_composites)
        
        images_to_process = {
            'dem': dem_image, 'optical': optical_image, 
            'thermal': thermal_image, 'sar': sar_image
        }
        
        print("  Sampling statistics from point locations...")
        all_stats = {}
        
        for name, image in images_to_process.items():
            try:
                scale = self.settings.SOURCE_RESOLUTIONS.get(name, 30)
                
                # Sample the image at the grid points
                sampled = image.sampleRegions(
                    collection=sample_grid,
                    scale=scale,
                    geometries=False
                )
                
                # Calculate statistics from the sampled values
                # This will be much more memory efficient
                stats_dict = {}
                
                # Get all band names
                band_names = image.bandNames().getInfo()
                
                for band in band_names:
                    band_values = sampled.aggregate_array(band)
                    
                    # Calculate statistics
                    stats_dict[f'{band}_mean'] = band_values.reduce(ee.Reducer.mean()).getInfo()
                    stats_dict[f'{band}_stdDev'] = band_values.reduce(ee.Reducer.stdDev()).getInfo()
                    stats_dict[f'{band}_min'] = band_values.reduce(ee.Reducer.min()).getInfo()
                    stats_dict[f'{band}_max'] = band_values.reduce(ee.Reducer.max()).getInfo()
                
                all_stats[name] = stats_dict
                print(f"    - Successfully calculated sampled stats for '{name}'")
                
            except Exception as e:
                print(f"    - Warning: Could not calculate sampled stats for '{name}'. Error: {e}")
                all_stats[name] = {}
        
        return all_stats
