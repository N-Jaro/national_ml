
import os
import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio import features
from rasterio.windows import from_bounds
from rasterio.enums import Resampling
from pysheds.grid import Grid
from tqdm import tqdm
from config import Settings
import concurrent.futures # Import for parallel processing

class LocalReferenceProcessor:
    """
    Processes local data to create hydrography and flow direction reference layers
    that perfectly align with previously downloaded GEE patches.
    """
    def __init__(self, settings: Settings, nhd_gdb_path: str):
        """
        Initializes the processor.

        Args:
            settings (Settings): The global configuration object.
            nhd_gdb_path (str): Path to the local NHD GeoDatabase (.gdb) file.
        """
        self.settings = settings
        self.nhd_gdb_path = nhd_gdb_path
        print("NHD GeoDatabase path has been set.")

    def _calculate_d8_flow_direction(self, dem_path: str, output_path: str):
        """
        Calculates a D8 flow direction raster from a DEM GeoTIFF using PySheds.
        """
        if os.path.exists(output_path):
            return # Skip if already processed

        print(f"    - Calculating D8 Flow Direction from '{os.path.basename(dem_path)}'...")
        
        # Pre-process the DEM to ensure it has a valid nodata value
        temp_dem_path = dem_path.replace('.tif', '_corrected.tif')

        print("      - Step 1: Pre-processing DEM...")
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
            print("      - Step 2: Calclate Flow Direction (D8) ...")
            print("        - Loading DEM into PySheds Grid...")
            grid = Grid.from_raster(temp_dem_path)
            dem = grid.read_raster(temp_dem_path)

            print("        - Filling pits...")
            pit_filled_dem = grid.fill_pits(dem)
            
            print("        - Filling depressions...")
            flooded_dem = grid.fill_depressions(pit_filled_dem)

            print("        - Resolving flats...")
            inflated_dem = grid.resolve_flats(flooded_dem)

            print("        - Calculating D8 flow direction...")
            dirmap = (64, 128, 1, 2, 4, 8, 16, 32)
            fdir = grid.flowdir(inflated_dem, dirmap=dirmap, nodata_out = np.int32(-1))

            print(f"        - Saving D8 flow direction to: {output_path}")
            grid.to_raster(fdir, output_path)
            print("        - D8 raster saved successfully.")
            
        except Exception as e:
            print(f"      - Error during PySheds processing: {e}")
        finally:
            # Clean up the temporary corrected DEM file
            if os.path.exists(temp_dem_path):
                os.remove(temp_dem_path)
                print(f"      - Cleaned up temporary file: {os.path.basename(temp_dem_path)}")

    def _create_hydro_mask(self, dem_template_path: str, huc_boundary_path: str, output_path: str):
        """
        Creates a hydrography mask by burning NHD vectors from a GDB onto a DEM template grid.
        """
        if os.path.exists(output_path):
            return
            
        print(f"    - Creating Hydrography Mask based on '{os.path.basename(dem_template_path)}'...")
        
        # Step 1: Load template raster metadata and HUC boundary
        with rasterio.open(dem_template_path) as src:
            meta = src.meta.copy()
            raster_crs = src.crs
            raster_transform = src.transform
            raster_shape = (src.height, src.width)
        
        huc8_geo = gpd.read_file(huc_boundary_path)
        
        # THE FIX: Perform the initial bounding box query in the NHD's native CRS.
        # We assume NHD GDB is in a geographic CRS like EPSG:4269 or WGS84 (EPSG:4326)
        # First, get the HUC bounds in a geographic CRS
        huc8_geographic = huc8_geo.to_crs("EPSG:4269")
        bbox_geographic = tuple(huc8_geographic.total_bounds)

        # Step 2: Read & clip NHD layers from the GeoDatabase
        layers_to_read = ["NetworkNHDFlowline", "NHDWaterbody", "NonNetworkNHDFlowline"] 
        combined_gdfs = []

        for layer in layers_to_read:
            print(f"      - Reading and clipping {layer}...")
            try:
                # Use the geographic bounding box for an efficient initial read
                gdf = gpd.read_file(self.nhd_gdb_path, layer=layer, bbox=bbox_geographic)
                # Clip precisely to the geographic HUC geometry
                gdf_clipped = gdf[gdf.intersects(huc8_geographic.unary_union)]
                if not gdf_clipped.empty:
                    combined_gdfs.append(gdf_clipped)
            except Exception as e:
                print(f"        - Could not read or clip layer '{layer}'. It may not exist for this region. Error: {e}")

        if not combined_gdfs:
            print("      - No NHD features found in the HUC boundary. Creating empty mask.")
            mask = np.zeros(raster_shape, dtype=np.uint8)
        else:
            # Step 3: Combine, Reproject, and Buffer
            print("      - Combining layers...")
            # The CRS of the concatenated GDF will be the CRS of the source GDB data
            combined_gdf = gpd.GeoDataFrame(pd.concat(combined_gdfs, ignore_index=True), crs=gdf.crs)
            
            print("      - Reprojecting to match DEM and buffering 15m...")
            # Reproject to match the raster's CRS *before* buffering in meters
            combined_gdf_reproj = combined_gdf.to_crs(raster_crs)
            buffered = combined_gdf_reproj.buffer(15)

            # Step 4: Rasterize
            print("      - Rasterizing buffered features...")
            mask = features.rasterize(
                ((geom, 1) for geom in buffered if geom is not None and geom.is_valid),
                out_shape=raster_shape,
                transform=raster_transform,
                fill=0,
                dtype="uint8"
            )

        # Step 5: Save Raster
        print(f"      - Saving raster to: {os.path.basename(output_path)}")
        meta.update({'dtype': 'uint8', 'count': 1, 'compress': 'lzw', 'nodata': 0})
        with rasterio.open(output_path, 'w', **meta) as dst:
            dst.write(mask, 1)

    def _update_single_patch_file(self, args):
        """
        Worker function to update a single .npz file with reference data patches.
        """
        npz_path, template_tif_path, flow_dir_path, hydro_mask_path = args
        
        try:
            # Use the patch template to define the window to read
            with rasterio.open(template_tif_path) as template_src:
                bounds = template_src.bounds
            
            output_shape = (self.settings.PATCH_SIZE, self.settings.PATCH_SIZE)
            
            # Read the corresponding window from the large reference rasters
            with rasterio.open(flow_dir_path) as flow_src:
                flow_window = from_bounds(*bounds, transform=flow_src.transform)
                flow_dir_patch = flow_src.read(1, window=flow_window, out_shape=output_shape, resampling=Resampling.nearest)
            
            with rasterio.open(hydro_mask_path) as hydro_src:
                hydro_window = from_bounds(*bounds, transform=hydro_src.transform)
                hydro_mask_patch = hydro_src.read(1, window=hydro_window, out_shape=output_shape, resampling=Resampling.nearest)
            
            # Update the .npz file
            with np.load(npz_path) as existing_data:
                updated_data = {key: existing_data[key] for key in existing_data}
            
            updated_data['flow_dir'] = flow_dir_patch
            updated_data['hydro_mask'] = hydro_mask_patch
            np.savez_compressed(npz_path, **updated_data)
            return True
        except Exception as e:
            # print(f"Skipping {os.path.basename(npz_path)}: could not process. Reason: {e}")
            return False

    def run(self):
        """
        Iterates through HUC folders, generates full reference rasters,
        then extracts patches and updates the .npz files in parallel.
        """
        huc_root_folder = os.path.join(self.settings.ROOT_OUTPUT_FOLDER, self.settings.HUC_OUTPUT_FOLDER)
        patch_root_folder = os.path.join(self.settings.ROOT_OUTPUT_FOLDER, self.settings.PATCH_OUTPUT_FOLDER)
        huc_folders = [d for d in os.listdir(huc_root_folder) if os.path.isdir(os.path.join(huc_root_folder, d))]

        print(f"\nFound {len(huc_folders)} HUC folders to process for local reference layers.")

        for huc_id in tqdm(huc_folders, desc="Processing HUCs Locally"):
            huc_dem_path = os.path.join(huc_root_folder, huc_id, f'huc8_{huc_id}_dem.tif')
            huc_boundary_path = os.path.join(huc_root_folder, huc_id, f'huc8_{huc_id}_boundary.geojson')
            huc_dir_patches = os.path.join(patch_root_folder, huc_id)

            if not os.path.exists(huc_dem_path) or not os.path.exists(huc_boundary_path): continue

            flow_dir_path = os.path.join(huc_dir_patches, 'flow_direction.tif')
            hydro_mask_path = os.path.join(huc_dir_patches, 'hydro_mask.tif')

            # 1. Create the full reference rasters for the HUC (this is sequential)
            self._calculate_d8_flow_direction(huc_dem_path, flow_dir_path)
            self._create_hydro_mask(huc_dem_path, huc_boundary_path, hydro_mask_path)

            # 2. Extract patches and update .npz files in parallel
            print("    - Extracting reference patches and updating .npz files...")
            if not os.path.exists(flow_dir_path) or not os.path.exists(hydro_mask_path):
                print(f"Warning: Reference TIFs for HUC {huc_id} not created. Skipping patch update.")
                continue

            patch_files = [f for f in os.listdir(huc_dir_patches) if f.endswith('.npz')]
            
            # Create a list of arguments for the worker function
            tasks_args = []
            for patch_file_name in patch_files:
                npz_path = os.path.join(huc_dir_patches, patch_file_name)
                patch_index = patch_file_name.split('_')[1].split('.')[0]
                template_tif_path = os.path.join(huc_dir_patches, f'patch_{patch_index}_georef_template.tif')
                if os.path.exists(template_tif_path):
                    tasks_args.append((npz_path, template_tif_path, flow_dir_path, hydro_mask_path))

            # Use a ThreadPoolExecutor to process patches concurrently
            MAX_WORKERS = 16
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                results = list(tqdm(executor.map(self._update_single_patch_file, tasks_args), total=len(tasks_args), desc=f"  Updating .npz for HUC {huc_id}", leave=False))
            
            success_count = sum(1 for r in results if r)
            print(f"    - Update complete. {success_count}/{len(tasks_args)} patches updated.")