import ee

class PatchGenerator:
    def __init__(self, settings):
        self.settings = settings

    def generate_patch_centers(self, raster: ee.Image, patch_size: int, stride: int, boundary: ee.Geometry) -> ee.FeatureCollection:
        """
        Generate patch center points over a raster using stride and patch size.
        """
        proj = raster.projection()
        pixel_coords = ee.Image.pixelCoordinates(proj).round()

        offset = patch_size // 2
        y_mask = pixel_coords.select('y').subtract(offset).mod(stride).eq(0)
        x_mask = pixel_coords.select('x').subtract(offset).mod(stride).eq(0)
        center_mask = y_mask.And(x_mask)

        centers = center_mask.selfMask().reduceToVectors(
            geometry=boundary,
            scale=proj.nominalScale(),
            geometryType='centroid',
            eightConnected=False,
            maxPixels=5e8
        )

        return centers.select([])

    def extract_patches(self, image: ee.Image, center_points: ee.FeatureCollection, patch_size: int) -> ee.ImageCollection:
        """
        Extract square patches from an image centered on points in a FeatureCollection.
        """
        projection = image.projection()
        scale_meters = projection.nominalScale()
        patch_size_meters = ee.Number(patch_size).multiply(scale_meters)

        def create_patch(feature: ee.Feature) -> ee.Image:
            center = ee.Feature(feature).geometry()
            region = center.buffer(patch_size_meters.divide(2)).bounds()
            return image.clip(region).copyProperties(feature, ['system:index'])

        return ee.ImageCollection(center_points.map(create_patch))
