// =============================================================================
//    Random HUC8 Selection and Data Export
//    (Rectangular Rasters, Buffered HUC, Per-HUC Folders, All Boundaries, Flow Dir, Bounding Box)
//    (Corrected CRS handling for 3DEP DEM)
// =============================================================================

// --- Parameters ---
var numberOfHucs = 2; // How many HUC8s to select randomly
var bufferDistanceMeters = 5000; // 5 kilometers for buffering the HUC
var startDate = '2023-01-01'; // Start date for imagery search
var endDate = '2024-12-31';   // End date for imagery search
var driveFolder = 'GEE_HUC_Exports_With_Rect_Vector_CRSfix'; // Main folder name
var exportVectorFormat = 'GeoJSON'; // Format for boundaries and bounding box

// Center map (optional)
Map.setCenter(-98.5, 39.8, 4);

// --- Load Base Datasets ---
var huc8 = ee.FeatureCollection('USGS/WBD/2017/HUC08');
var meritHydro = ee.Image('MERIT/Hydro/v1_0_1'); // For Flow Direction
var demSource = ee.Image('USGS/3DEP/10m');
print('Base datasets loaded.');

// *** Define the target CRS for DEM and derived products explicitly ***
var targetDemCRS = 'EPSG:4269'; // Native CRS of USGS/3DEP/10m
print('Target DEM CRS for exports and Flow Dir resampling:', targetDemCRS);

// --- Randomly Select HUCs ---
var huc8WithRandom = huc8.randomColumn('random');
var selectedHucs = huc8WithRandom.sort('random').limit(numberOfHucs);

print('Selected HUCs FeatureCollection (will trigger exports):', selectedHucs);
Map.addLayer(selectedHucs, { color: 'FFFF00' }, 'Selected HUC8 Boundaries (Original)', true, 0.5);

// --- Export Selected ORIGINAL HUC Boundaries (to main driveFolder) ---
Export.table.toDrive({
    collection: selectedHucs,
    description: 'ALL_HUC8_Original_Boundaries_Export_' + numberOfHucs,
    folder: driveFolder,
    fileNamePrefix: 'Selected_' + numberOfHucs + '_HUC8_Original_Boundaries',
    fileFormat: exportVectorFormat,
});
print('Original HUC boundaries collection export task (' + exportVectorFormat + ') created.');


// --- Helper Function: Cloud Masking for Landsat ---
function maskL8srClouds(image) {
    var cloudBitMask = (1 << 3); var cloudShadowBitMask = (1 << 4);
    var qa = image.select('QA_PIXEL');
    var mask = qa.bitwiseAnd(cloudBitMask).eq(0).and(qa.bitwiseAnd(cloudShadowBitMask).eq(0));
    var opticalBands = image.select('SR_B[2-7]').multiply(0.0000275).add(-0.2);
    var thermalBand = image.select('ST_B10').multiply(0.00341802).add(149.0);
    return image.addBands(opticalBands, null, true).addBands(thermalBand, null, true).updateMask(mask);
}

// --- Image Generation Functions ---
function getDEM(clipGeometry) {
    return demSource.select('elevation').clip(clipGeometry);
}

function getLandsatImages(filterGeometry, startDate, endDate) {
    var landsatCol = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
        .merge(ee.ImageCollection('LANDSAT/LC08/C02/T1_L2'))
        .filterBounds(filterGeometry)
        .filterDate(startDate, endDate)
        .map(maskL8srClouds);
    var landsatOpticalMedian = landsatCol.select(['SR_B4', 'SR_B3', 'SR_B2']).median();
    var landsatThermalMedian = landsatCol.select('ST_B10').median();
    return { optical: landsatOpticalMedian, thermal: landsatThermalMedian };
}

function getSARImage(filterGeometry, startDate, endDate) {
    var sentinel1Col = ee.ImageCollection('COPERNICUS/S1_GRD')
        .filterBounds(filterGeometry)
        .filterDate(startDate, endDate)
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
        .filter(ee.Filter.eq('instrumentMode', 'IW')).select('VV');
    var sentinel1Median = sentinel1Col.median();
    return sentinel1Median;
}

// --- Trigger Individual HUC Exports using a Client-Side Loop ---
var selectedHucsList = selectedHucs.select(['huc8', 'states', 'name']).toList(numberOfHucs);
var selectedHucsListInfo = selectedHucsList.getInfo();

print('Client-side list of HUCs retrieved, initiating individual HUC export tasks...');

selectedHucsListInfo.forEach(function (featureInfo) {
    var originalGeometry = ee.Geometry.MultiPolygon(featureInfo.geometry.coordinates);
    var hucId = featureInfo.properties.huc8;
    var name = featureInfo.properties.name || 'Unknown';
    var states = featureInfo.properties.states || 'NA';

    var cleanName = name.replace(/[^a-zA-Z0-9_.-]/g, '_');
    var baseFilenameForFolder = 'HUC8_' + hucId + '_' + cleanName;
    var hucSpecificFolder = driveFolder + '/' + baseFilenameForFolder;

    var bufferedGeometry = originalGeometry.buffer(bufferDistanceMeters);
    var exportRegionRectangle = bufferedGeometry.bounds();

    print('Setting up exports for HUC8: ' + hucId + ' (' + name + ', ' + states + ') into folder: ' + hucSpecificFolder);

    // Export Buffered HUC Boundary (Polygonal)
    var bufferedFeatureCollection = ee.FeatureCollection([
        ee.Feature(bufferedGeometry, {
            'huc8': hucId, 'name': name, 'original_states': states, 'buffer_m': bufferDistanceMeters, 'type': 'buffered_polygon'
        })
    ]);
    Export.table.toDrive({
        collection: bufferedFeatureCollection, description: 'Buffered_Polygon_Boundary_Export_' + hucId,
        folder: hucSpecificFolder, fileNamePrefix: baseFilenameForFolder + '_5km_Buffered_Polygon_Boundary',
        fileFormat: exportVectorFormat
    });

    // Export THE RECTANGULAR BOUNDING BOX ITSELF
    var boundingBoxFeatureCollection = ee.FeatureCollection([
        ee.Feature(exportRegionRectangle, {
            'huc8': hucId, 'name': name, 'original_states': states,
            'buffer_applied_before_bbox_m': bufferDistanceMeters, 'type': 'bounding_box_of_buffered_huc'
        })
    ]);
    Export.table.toDrive({
        collection: boundingBoxFeatureCollection, description: 'BoundingBox_of_BufferedHUC_Export_' + hucId,
        folder: hucSpecificFolder, fileNamePrefix: baseFilenameForFolder + '_BoundingBox_of_5km_Buffered',
        fileFormat: exportVectorFormat
    });

    // 1. Export DEM
    var demImage = getDEM(exportRegionRectangle);
    Export.image.toDrive({
        image: demImage.toFloat(), description: 'DEM_3DEP_Export_' + hucId,
        folder: hucSpecificFolder, fileNamePrefix: baseFilenameForFolder + '_DEM_10m_Rect',
        region: exportRegionRectangle, scale: 10,
        crs: targetDemCRS, // *** USE EXPLICIT CRS STRING ***
        maxPixels: 1.5e10
    });

    // 2. Export Landsat (typically exported to EPSG:4326 if no CRS specified and data is geographic)
    var landsatImages = getLandsatImages(bufferedGeometry, startDate, endDate);
    Export.image.toDrive({
        image: landsatImages.optical.toFloat(), description: 'Landsat_Optical_Export_' + hucId,
        folder: hucSpecificFolder, fileNamePrefix: baseFilenameForFolder + '_Landsat_Optical_Rect',
        region: exportRegionRectangle, scale: 30, crs: 'EPSG:4326', maxPixels: 1.5e10
    });
    Export.image.toDrive({
        image: landsatImages.thermal.toFloat(), description: 'Landsat_Thermal_Export_' + hucId,
        folder: hucSpecificFolder, fileNamePrefix: baseFilenameForFolder + '_Landsat_Thermal_Rect',
        region: exportRegionRectangle, scale: 30, crs: 'EPSG:4326', maxPixels: 1.5e10
    });

    // 3. Export SAR (typically exported to EPSG:4326 if no CRS specified)
    var sarImage = getSARImage(bufferedGeometry, startDate, endDate);
    Export.image.toDrive({
        image: sarImage.toFloat(), description: 'SAR_VV_Export_' + hucId,
        folder: hucSpecificFolder, fileNamePrefix: baseFilenameForFolder + '_SAR_VV_Rect',
        region: exportRegionRectangle, scale: 10, crs: 'EPSG:4326', maxPixels: 1.5e10
    });


    // 4. Export Resampled Flow Direction
    var flowDirectionOriginal = meritHydro.select('dir');

    // *** MODIFIED: Rely on reproject() for nearest neighbor resampling ***
    var flowDirectionResampled = flowDirectionOriginal
        .reproject({
            crs: targetDemCRS,
            scale: 10
            // reproject() defaults to nearest neighbor when changing resolution
            // and for categorical data like flow direction codes.
        });

    var flowDirectionFinal = flowDirectionResampled.clip(exportRegionRectangle);
    Export.image.toDrive({
        image: flowDirectionFinal.toUint8(), description: 'FlowDir_10m_Export_' + hucId,
        folder: hucSpecificFolder, fileNamePrefix: baseFilenameForFolder + '_FlowDir_10m_Rect',
        region: exportRegionRectangle, scale: 10,
        crs: targetDemCRS,
        maxPixels: 1.5e10
    });
});

print('--- Export Task Setup Complete ---');
print('Go to the "Tasks" tab on the right panel and click "Run" for each desired export.');
var totalTasks = (numberOfHucs * 7) + 1;
print(totalTasks + ' tasks were generated.');
print('WARNING: Manually running ' + totalTasks + ' tasks can be time-consuming.');
print('Exports for each HUC will be in a subfolder named after the HUC ID and name within: ' + driveFolder);

// --- End of Script ---