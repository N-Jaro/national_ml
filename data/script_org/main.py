import ee
import time
import re
import os

from config import Settings
from drive_manager import GoogleDriveManager
from task_manager import TaskManager
from patch_generator import PatchGenerator
from data_exporter import DataExporter
from visualization_manager import VisualizationManager


def main():
    # Initialize settings and services
    settings = Settings()
    drive_manager = GoogleDriveManager(settings)
    task_manager = TaskManager()
    patch_generator = PatchGenerator(settings)
    data_exporter = DataExporter(settings, task_manager)
    visualizer = VisualizationManager()

    # Authenticate and initialize GEE
    try:
        if settings.GEE_PROJECT_ID:
            ee.Initialize(project=settings.GEE_PROJECT_ID, opt_url='https://earthengine-highvolume.googleapis.com')
        else:
            ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')
        print("Google Earth Engine initialized successfully.")
    except ee.EEException as e:
        print(e)
        print("Failed to initialize GEE. Try running 'earthengine authenticate'.")
        return

    # Load data
    huc_collection = ee.FeatureCollection(settings.HUC8_COL_NAME)
    merit_hydro_img = ee.Image(settings.MERIT_HYDRO_IMG_NAME)
    dem_source_img = ee.Image(settings.DEM_SOURCE_IMG_NAME)

    # Randomly select HUCs
    selected_hucs = huc_collection.randomColumn('random').sort('random').limit(settings.NUMBER_OF_HUCS)
    selected_hucs_info = selected_hucs.select(['huc8', 'states', 'name']).toList(settings.NUMBER_OF_HUCS).getInfo()

    # Export selected boundaries globally
    data_exporter.export_vector(
        selected_hucs,
        description=f'ALL_HUC8_Original_Boundaries_Export_{settings.NUMBER_OF_HUCS}',
        folder=settings.DRIVE_FOLDER,
        filename_prefix=f'Selected_{settings.NUMBER_OF_HUCS}_HUC8_Original_Boundaries'
    )

    for feature_info in selected_hucs_info:
        props = feature_info['properties']
        huc_id = props.get('huc8', 'UnknownID')
        name = props.get('name', 'UnknownName')
        states = props.get('states', 'NA')
        original_geom = ee.Geometry(feature_info['geometry'])
        clean_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', name)
        base_filename = f'HUC8_{huc_id}_{clean_name}'

        buffered_geom = original_geom.buffer(settings.BUFFER_DISTANCE_METERS)
        region_rect = buffered_geom.bounds()

        # Export boundary and region vectors
        data_exporter.export_vector(
            ee.FeatureCollection([ee.Feature(buffered_geom, props)]),
            description=f'Buffered_Boundary_{huc_id}',
            folder=base_filename,
            filename_prefix=f'{base_filename}_Buffered_Boundary'
        )

        data_exporter.export_vector(
            ee.FeatureCollection([ee.Feature(region_rect, props)]),
            description=f'BoundingBox_{huc_id}',
            folder=base_filename,
            filename_prefix=f'{base_filename}_BoundingBox'
        )

        # Export DEM
        dem_image = dem_source_img.select('elevation').clip(region_rect)
        data_exporter.export_dem(dem_image, region_rect, base_filename, huc_id)

        # Export flow direction
        flow_image = merit_hydro_img.select('dir').reproject(crs=settings.TARGET_DEM_CRS, scale=10).clip(region_rect)
        data_exporter.export_flow_direction(flow_image, region_rect, base_filename, huc_id)

        # Generate and extract patches
        patch_centers = patch_generator.generate_patch_centers(
            dem_image, settings.PATCH_SIZE, settings.PATCH_STRIDE, buffered_geom
        )
        print(f"Generated patch centers for {huc_id}: {patch_centers.size().getInfo()} centers")

        if settings.VISUALIZE_POINTS:
            visualizer.plot_patch_centers(patch_centers, buffered_geom)

        patches = patch_generator.extract_patches(dem_image, patch_centers, settings.PATCH_SIZE)
        patch_list = patches.toList(patches.size())
        for i in range(1):  # Only export one for test
            img = ee.Image(patch_list.get(i))
            data_exporter.export_patch_image(img, i, 'dem', base_filename)

    # Start and monitor tasks
    task_manager.start_all_tasks()
    task_manager.monitor_tasks()


if __name__ == '__main__':
    main()
