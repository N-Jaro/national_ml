// Google Earth Engine JavaScript Code Editor Script
// HUC 10020007 (Madison River Basin) Data Regeneration
// Copy and paste this entire script into the GEE Code Editor

// ==============================================================================
// --- CONFIGURATION ---
// ==============================================================================

var HUC8_ID = '10020007';  // Madison River Basin, Montana
var START_DATE = '2023-07-01';
var END_DATE = '2023-07-31';
var EXTENDED_START_DATE = '2023-06-01';
var EXTENDED_END_DATE = '2023-08-31';
var EXPORT_FOLDER = 'National_ML_HUC_10020007_Regenerated';
var CRS = 'EPSG:5070';  // Albers Equal Area Conic (CONUS standard)

print('🎯 Processing HUC8:', HUC8_ID, '(Madison River Basin, Montana)');
print('📅 Date range:', START_DATE, 'to', END_DATE);
print('🗺️  Target CRS: EPSG:5070 (Albers Equal Area Conic)');

// ==============================================================================
// --- GET REGION OF INTEREST ---
// ==============================================================================

var huc8_collection = ee.FeatureCollection('USGS/WBD/2017/HUC08');
var region_feature = huc8_collection.filter(ee.Filter.eq('huc8', HUC8_ID)).first();

// Get HUC boundary geometry for vector export
var huc_boundary = region_feature.geometry();

// Get bounding box for raster clipping (rectangular extent)
var bbox = huc_boundary.bounds();

print('✅ Region loaded:', region_feature.get('name'));
print('📐 Using bounding box for raster clipping');
print('🔷 HUC boundary will be exported separately');

// ==============================================================================
// --- PROCESS DATASETS ---
// ==============================================================================

// --- DEM ---
print('🏔️ Processing DEM...');
var dem = ee.ImageCollection('USGS/3DEP/10m_collection')
    .filterBounds(bbox)
    .mosaic()
    .clip(bbox)
    .rename('elevation');

// --- Landsat 8/9 ---
print('🛰️ Processing Landsat...');

function maskLandsatClouds(image) {
    var qa = image.select('QA_PIXEL');
    var mask = qa.bitwiseAnd(1 << 3).eq(0)  // Cloud
        .and(qa.bitwiseAnd(1 << 4).eq(0));    // Cloud shadow

    var optical = image.select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'])
        .multiply(0.0000275).add(-0.2);
    var thermal = image.select('ST_B10').multiply(0.00341802).add(149.0);

    return image.addBands(optical, null, true)
        .addBands(thermal, null, true)
        .updateMask(mask);
}

var landsat8 = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
    .filterBounds(bbox)
    .filterDate(START_DATE, END_DATE)
    .map(maskLandsatClouds);

var landsat9 = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
    .filterBounds(bbox)
    .filterDate(START_DATE, END_DATE)
    .map(maskLandsatClouds);

var landsat_combined = landsat8.merge(landsat9);
var landsat_count = landsat_combined.size();

print('📊 Found Landsat scenes:', landsat_count);

var landsat_median = ee.Algorithms.If(
    landsat_count.gt(0),
    landsat_combined.median(),
    landsat8.merge(landsat9)
        .filterDate(EXTENDED_START_DATE, EXTENDED_END_DATE)
        .map(maskLandsatClouds)
        .median()
);

var landsat_final = ee.Image(landsat_median)
    .select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10'],
        ['B2_Blue', 'B3_Green', 'B4_Red', 'B5_NIR', 'B6_SWIR1', 'B7_SWIR2', 'B10_Thermal'])
    .clip(bbox);

// --- Sentinel-1 SAR ---
print('📡 Processing Sentinel-1...');

function preprocessSAR(image) {
    var db = ee.Image(10).multiply(image.log10());
    return db.focal_median({ radius: 1, kernelType: 'square', units: 'pixels' });
}

var sar_collection = ee.ImageCollection('COPERNICUS/S1_GRD')
    .filterBounds(bbox)
    .filterDate(START_DATE, END_DATE)
    .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
    .filter(ee.Filter.eq('instrumentMode', 'IW'))
    .select('VV')
    .map(preprocessSAR);

var sar_count = sar_collection.size();
print('📊 Found SAR scenes:', sar_count);

var sar_median = ee.Algorithms.If(
    sar_count.gt(0),
    sar_collection.median(),
    ee.ImageCollection('COPERNICUS/S1_GRD')
        .filterBounds(bbox)
        .filterDate(EXTENDED_START_DATE, EXTENDED_END_DATE)
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
        .filter(ee.Filter.eq('instrumentMode', 'IW'))
        .select('VV')
        .map(preprocessSAR)
        .median()
);

var sar_final = ee.Image(sar_median).clip(bbox).rename('VV_backscatter_dB');

// --- AlphaEarth ---
print('🌍 Processing AlphaEarth 2023 (visualization subset)...');
var alphaearth_collection = ee.ImageCollection('GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL');

// Use 2023 data specifically as requested
var alphaearth_2023 = alphaearth_collection
    .filter(ee.Filter.calendarRange(2023, 2023, 'year'))
    .filterBounds(bbox)
    .first();

// For visualization, select only first 16 bands (reduces file size from ~120GB to ~30GB)
// AlphaEarth bands are named A00, A01, A02, etc.
var alphaearth_subset = alphaearth_2023.select(
    ['A00', 'A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07',
        'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15']
);

var alphaearth_final = alphaearth_subset.clip(bbox);

// Verify band count
var band_count = alphaearth_final.bandNames().size();
print('📊 AlphaEarth bands selected for visualization:', band_count);

// --- Reference Data ---
print('🗺️ Creating reference data...');

// Water mask
var gsw = ee.Image('JRC/GSW1_4/GlobalSurfaceWater');
var water_mask = gsw.select('occurrence').gte(50).clip(bbox).rename('water_mask');

// Flow direction
var flow_dir = ee.Image('WWF/HydroSHEDS/15DIR').clip(bbox).rename('flow_direction');

// ==============================================================================
// --- EXPORT TASKS ---
// ==============================================================================

print('🚀 Starting exports...');

// Export HUC Boundary (vector)
Export.table.toDrive({
    collection: ee.FeatureCollection([region_feature]),
    description: 'huc8_' + HUC8_ID + '_boundary',
    folder: EXPORT_FOLDER,
    fileNamePrefix: 'huc8_' + HUC8_ID + '_boundary',
    fileFormat: 'GeoJSON'
});

// Export DEM
Export.image.toDrive({
    image: dem.toFloat(),
    description: 'huc_' + HUC8_ID + '_dem_10m',
    folder: EXPORT_FOLDER,
    fileNamePrefix: 'huc_' + HUC8_ID + '_dem_10m',
    region: bbox,
    scale: 10,
    crs: CRS,
    maxPixels: 1e10
});

// Export Landsat
Export.image.toDrive({
    image: landsat_final.toFloat(),
    description: 'huc_' + HUC8_ID + '_landsat_30m_' + START_DATE + '_' + END_DATE,
    folder: EXPORT_FOLDER,
    fileNamePrefix: 'huc_' + HUC8_ID + '_landsat_30m_' + START_DATE + '_' + END_DATE,
    region: bbox,
    scale: 30,
    crs: CRS,
    maxPixels: 1e10
});

// Export SAR
Export.image.toDrive({
    image: sar_final.toFloat(),
    description: 'huc_' + HUC8_ID + '_sar_10m_' + START_DATE + '_' + END_DATE + '_median',
    folder: EXPORT_FOLDER,
    fileNamePrefix: 'huc_' + HUC8_ID + '_sar_10m_' + START_DATE + '_' + END_DATE + '_median',
    region: bbox,
    scale: 10,
    crs: CRS,
    maxPixels: 1e10
});

// Export AlphaEarth
Export.image.toDrive({
    image: alphaearth_final.toFloat(),
    description: 'huc_' + HUC8_ID + '_alphaearth_10m_2023_16bands',
    folder: EXPORT_FOLDER,
    fileNamePrefix: 'huc_' + HUC8_ID + '_alphaearth_10m_2023_16bands',
    region: bbox,
    scale: 10,
    crs: CRS,
    maxPixels: 1e10
});

// Export Water Mask
Export.image.toDrive({
    image: water_mask.toFloat(),
    description: 'huc_' + HUC8_ID + '_hydro_mask_10m',
    folder: EXPORT_FOLDER,
    fileNamePrefix: 'huc_' + HUC8_ID + '_hydro_mask_10m',
    region: bbox,
    scale: 10,
    crs: CRS,
    maxPixels: 1e10
});

// Export Flow Direction
Export.image.toDrive({
    image: flow_dir.toFloat(),
    description: 'huc_' + HUC8_ID + '_flow_direction_10m',
    folder: EXPORT_FOLDER,
    fileNamePrefix: 'huc_' + HUC8_ID + '_flow_direction_10m',
    region: bbox,
    scale: 10,
    crs: CRS,
    maxPixels: 1e10
});

print('✅ All export tasks started!');
print('📁 Files will be saved to Google Drive folder:', EXPORT_FOLDER);
print('🔍 Check the Tasks tab to monitor progress');

// Add region to map for visualization
Map.addLayer(huc_boundary, { color: 'red' }, 'HUC ' + HUC8_ID + ' Boundary');
Map.addLayer(bbox, { color: 'blue', fillColor: '00000000' }, 'Bounding Box');
Map.centerObject(huc_boundary, 10);