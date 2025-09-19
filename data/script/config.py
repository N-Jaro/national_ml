class Settings:
    """
    A centralized class for managing all configuration parameters for the data processing pipeline.
    """
    def __init__(self):
                
        # --- Stage 1: HUC Processing Parameters ---
        self.HUC_IDS_TO_PROCESS = [
            '03160113', '19090102', '17090011', '03040206', '18070103',
            '12090302', '14010005', '10260010', '19050105', '05040003',
            '17100206', '11020004', '19020504', '19050401', '03050108',
            '10170204', '07140202', '12070101', '10120203', '18020151',
            '12090202', '07040006', '05080002', '18020126', '08020205',
            '18020111', '13060003', '18070107', '07130003', '17110012',
            '03030005', '04060102', '17010203', '14060004', '19080302',
            '07130004', '19080204', '10270104', '12090104', '13070007',
            '03160106', '07030005', '05090104', '13040209', '17060109',
            '16040204', '08020301', '10130306', '18080003', '07120005'
        ]
        
        # --- File and Folder Paths ---
        self.ROOT_OUTPUT_FOLDER = '/u/nathanj/national_ml/data/processed' # Root folder for all outputs
        self.HUC_OUTPUT_FOLDER = 'huc_processing' # Subfolder for Stage 1
        self.PATCH_OUTPUT_FOLDER = 'patch_dataset' # Subfolder for Stage 2
        
        self.NUMBER_OF_HUCS = 1  # randomly selected HUCs number # This is for testing purposes
        self.BUFFER_DISTANCE_METERS = 5000  # buffer distance in meters
        self.START_DATE = '2023-01-01'
        self.END_DATE = '2024-12-31'
        
        self.DRIVE_FOLDER = 'GEE_HUC_Exports_Python_Full_alphaE'  # Google Drive folder name for exports
        self.LOCAL_DOWNLOAD_DIR = 'gee_downloads_alphaE' # download directory for GEE exports
        self.EXPORT_VECTOR_FORMAT = 'GeoJSON'  # export vector format, can be 'GeoJSON' or 'KML'
        self.TARGET_DEM_CRS = 'EPSG:5070' #This is the meter as the native unit # target CRS for DEM exports
        self.GDRIVE_CREDENTIALS_FILE = 'GD_credentials.json' # Google Drive API credentials file
        self.GDRIVE_TOKEN_FILE = 'GD_token.json'  # Google Drive API token file
        self.GEE_PROJECT_ID = 'nathanj-national-ml'  # GEE project ID for exports
        self.GDRIVE_SCOPES = ['https://www.googleapis.com/auth/drive']  # Google Drive API scopes

        # data 
        self.HUC8_COL_NAME = 'USGS/WBD/2017/HUC08' #ee.FeatureCollection('USGS/WBD/2017/HUC08')
        self.MERIT_HYDRO_IMG_NAME = 'MERIT/Hydro/v1_0_1' #ee.Image('MERIT/Hydro/v1_0_1')
        self.DEM_SOURCE_IMG_NAME ='USGS/3DEP/10m_collection' # USGS/3DEP/10m has been deprecated, use USGS/3DEP/10m_collection instead
        self.ALPHA_EARTH_YEAR = 2024
        
        self.SOURCE_RESOLUTIONS = {
            'dem': 10,        # USGS 3DEP DEM is 10m
            'optical': 30,    # Landsat 8/9 optical bands are 30m
            'thermal': 30,    # Landsat 8/9 thermal is resampled to 30m in C2
            'sar': 10,        # Sentinel-1 GRD is processed to 10m
            'alphaearth': 10, # AlphaEarth data is at 10-meter resolution
            # 'flow': 30,      # MERIT Hydro flow direction is 30m
            # 'slope': 30,      # Slope is derived from DEM at 30m resolution
            # 'aspect': 30      # Aspect is derived from DEM at 30m
        }
        
        # patches
        self.PATCH = True
        self.PATCH_SIZE = 224
        self.PATCH_STRIDE = 224
        self.BATCH_EXPORT_SIZE = 500
        
        # GEE Optimization settings
        self.MAX_CONCURRENT_DOWNLOADS = 5  # Limit concurrent downloads
        self.REQUEST_DELAY_SECONDS = 1.0   # Delay between requests
        self.USE_BATCH_EXPORTS = True      # Use GEE batch exports instead of direct downloads
        self.PATCHES_PER_EXPORT = 100      # Number of patches per batch export
        self.MAX_PIXELS_PER_EXPORT = 1e8   # Limit pixels per export task
        
        # Memory management for statistics
        self.ENABLE_STATS_CALCULATION = True   # Set to False to skip stats entirely
        self.USE_SAMPLING_STATS = False        # Force use of sampling approach
        self.STATS_MEMORY_CONSERVATIVE = True  # Use aggressive memory optimization
        
        # Data source configuration
        self.ENABLE_ALPHAEARTH = False          # Set to False to skip AlphaEarth processing
        self.ALPHAEARTH_BANDS_LIMIT = 64       # Use all 64 AlphaEarth embedding bands
        
        self.VISUALIZE_POINTS = True  # whether to visualize points on the map
        

        # Google Drive API
        self.SCOPES = ['https://www.googleapis.com/auth/drive']