import ee

class DataExporter:
    def __init__(self, settings, task_manager):
        self.settings = settings
        self.task_manager = task_manager

    def export_vector(self, feature_collection: ee.FeatureCollection, description: str, folder: str, filename_prefix: str):
        task = ee.batch.Export.table.toDrive(
            collection=feature_collection,
            description=description,
            folder=folder,
            fileNamePrefix=filename_prefix,
            fileFormat=self.settings.EXPORT_VECTOR_FORMAT
        )
        self.task_manager.add_task(task, description, folder)

    def export_image(self, image: ee.Image, description: str, folder: str, filename_prefix: str, region: ee.Geometry, scale: int, crs: str):
        task = ee.batch.Export.image.toDrive(
            image=image,
            description=description,
            folder=folder,
            fileNamePrefix=filename_prefix,
            region=region,
            scale=scale,
            crs=crs,
            maxPixels=1.5e10
        )
        self.task_manager.add_task(task, description, folder)

    def export_dem(self, dem_image: ee.Image, region: ee.Geometry, base_filename: str, huc_id: str):
        desc = f'DEM_Export_{huc_id}'
        prefix = f'{base_filename}_DEM_10m_Rect'
        self.export_image(dem_image.toFloat(), desc, base_filename, prefix, region, 10, self.settings.TARGET_DEM_CRS)

    def export_landsat(self, landsat_image: ee.Image, region: ee.Geometry, base_filename: str, huc_id: str, band_type: str):
        desc = f'Landsat_{band_type}_Export_{huc_id}'
        prefix = f'{base_filename}_Landsat_{band_type}_Rect'
        self.export_image(landsat_image.toFloat(), desc, base_filename, prefix, region, 10, 'EPSG:4326')

    def export_sar(self, sar_image: ee.Image, region: ee.Geometry, base_filename: str, huc_id: str):
        desc = f'SAR_VV_Export_{huc_id}'
        prefix = f'{base_filename}_SAR_VV_Rect'
        self.export_image(sar_image.toFloat(), desc, base_filename, prefix, region, 10, 'EPSG:4269')

    def export_flow_direction(self, flow_image: ee.Image, region: ee.Geometry, base_filename: str, huc_id: str):
        desc = f'FlowDir_10m_Export_{huc_id}'
        prefix = f'{base_filename}_FlowDir_10m_Rect'
        self.export_image(flow_image.toUint8(), desc, base_filename, prefix, region, 10, 'EPSG:4326')

    def export_patch_image(self, raster: ee.Image, index: int, name: str, folder: str):
        image_id = raster.id().getInfo()
        filename = f"patches_image_{name}_{image_id.replace('/', '_')}"
        description = f"Export_Image_{name}_{index+1}"

        task = ee.batch.Export.image.toDrive(
            image=raster,
            description=description,
            folder=folder,
            fileNamePrefix=filename,
            scale=10,
            crs='EPSG:4326',
            maxPixels=1e10
        )
        self.task_manager.add_task(task, description, folder)
