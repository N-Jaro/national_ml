import ee
import geemap

def plot_centers_ee( all_centers_ee: ee.FeatureCollection, 
                      boundary_ee: ee.FeatureCollection):
    """
    Visualizes the raster image with all patch centers and valid patch centers on an interactive map.

    Parameters:
        all_centers_ee (ee.FeatureCollection): all patch centers.
        boundary_ee (ee.FeatureCollection): boundary of the area of interest.
    """
    
    Map = geemap.Map()

    # raster_vis_params = {'min': 0, 'max': 3000, 'palette': ['black', 'white']}
    # Map.addLayer(raster_ee, raster_vis_params, 'Raster Background')

    outline = ee.Image().byte().paint(
        featureCollection=boundary_ee,
        color=1,  # color doesn't matter, it's for the mask
        width=2   # outline width
    )
    Map.addLayer(outline, {'palette': 'lime'}, 'Boundary') 
    Map.addLayer(all_centers_ee, {'color': 'blue'}, 'All Patch Centers')
    # Map.addLayer(valid_centers_ee, {'color': 'red'}, 'Valid Patch Centers')
    Map.centerObject(boundary_ee, zoom=10) 

    Map.addLayerControl()

    return Map


def plot_raster_patches_ee(dem:ee.Image,
                           opti:ee.Image,
                           therm:ee.Image,
                           sar:ee.Image,
                           flow:ee.Image,
                           boundary:ee.Geometry):
    """
    Visualize the raster paches on an interactive map.
    Parameters:
        dem (ee.Image): Digital Elevation Model image.
        opti (ee.Image): Optical image.
        therm (ee.Image): Thermal image.
        sar (ee.Image): SAR image.
        flow (ee.Image): Flow geometry for visualization.
        bo
    """
    Map = geemap.Map()
    
    
    raster_vis_params = {'min': 0, 'max': 3000, 'palette': ['black', 'white']}
    vis_params_rgb = {
    # 'bands': ['B4', 'B3', 'B2'],  # <-- 指定B4=Red, B3=Green, B2=Blue
    'min': 0.0,
    'max': 3000.0
    # 'palette' 参数被完全移除
    }
    Map.addLayer(dem, raster_vis_params, 'DEM Raster')
    Map.addLayer(opti,vis_params_rgb,'Optical Raster')
    Map.addLayer(therm,vis_params_rgb,'Thermal Raster')
    Map.addLayer(sar,vis_params_rgb,'Sar Raster')
    Map.addLayer(flow,vis_params_rgb, 'Flow Raster')
    
    outline = ee.Image().byte().paint(
        featureCollection=boundary,
        color=1,  # color doesn't matter, it's for the mask
        width=2   # outline width
    )
    Map.addLayer(outline, {'palette': 'lime'}, 'Boundary') 
    Map.addLayerControl()
    
    return Map
