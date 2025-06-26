import tools

class VisualizationManager:
    def __init__(self):
        pass

    def plot_patch_centers(self, patch_centers, boundary, output_path='visualization.html'):
        """
        Generate and save an interactive map showing patch centers.
        """
        map_object = tools.plot_centers_ee(patch_centers, boundary)
        map_object.to_html(output_path)
        print(f"Patch center map saved to {output_path}")

    def plot_patch_rasters(self, dem_image, optical_image, thermal_image, sar_image, flow_image, boundary, output_path='test.html'):
        """
        Generate and save an interactive map visualizing multiple raster layers.
        """
        map_object = tools.plot_raster_patches_ee(
            dem_image, optical_image, thermal_image, sar_image, flow_image, boundary=boundary
        )
        map_object.to_html(output_path)
        print(f"Raster patch visualization saved to {output_path}")
