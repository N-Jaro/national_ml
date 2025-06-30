class Settings:
    """
    A centralized class for managing all configuration parameters for the data processing pipeline.
    """
    def __init__(self):
                
        # --- Stage 1: HUC Processing Parameters ---
        self.HUC_IDS_TO_PROCESS = [
            '10020007', # Lake Washington/Cedar/Sammamish
            # '10190005', # Upper Yellowstone
            # '07080104'  # Upper Illinois
        ]
        
        # --- File and Folder Paths ---
        self.ROOT_OUTPUT_FOLDER = 'pipeline_output'
        self.HUC_OUTPUT_FOLDER = 'huc_processing' # Subfolder for Stage 1
        self.PATCH_OUTPUT_FOLDER = 'patch_dataset' # Subfolder for Stage 2
        
        self.NUMBER_OF_HUCS = 1  # randomly selected HUCs number # This is for testing purposes
        self.BUFFER_DISTANCE_METERS = 5000  # buffer distance in meters
        self.START_DATE = '2023-01-01'
        self.END_DATE = '2024-12-31'
        
        self.DRIVE_FOLDER = 'GEE_HUC_Exports_Python_Full'  # Google Drive folder name for exports
        self.LOCAL_DOWNLOAD_DIR = 'gee_downloads' # download directory for GEE exports
        self.EXPORT_VECTOR_FORMAT = 'GeoJSON'  # export vector format, can be 'GeoJSON' or 'KML'
        self.TARGET_DEM_CRS = 'EPSG:5070' #This is the meter as the native unit # target CRS for DEM exports
        self.GDRIVE_CREDENTIALS_FILE = 'credentials.json' # Google Drive API credentials file
        self.GDRIVE_TOKEN_FILE = 'token.json'  # Google Drive API token file
        self.GEE_PROJECT_ID = 'nathanj-national-ml'  # GEE project ID for exports
        self.GDRIVE_SCOPES = ['https://www.googleapis.com/auth/drive']  # Google Drive API scopes

        # data 
        self.HUC8_COL_NAME = 'USGS/WBD/2017/HUC08' #ee.FeatureCollection('USGS/WBD/2017/HUC08')
        self.MERIT_HYDRO_IMG_NAME = 'MERIT/Hydro/v1_0_1' #ee.Image('MERIT/Hydro/v1_0_1')
        self.DEM_SOURCE_IMG_NAME ='USGS/3DEP/10m'
        
        self.SOURCE_RESOLUTIONS = {
            'dem': 10,        # USGS 3DEP DEM is 10m
            'optical': 30,    # Landsat 8/9 optical bands are 30m
            'thermal': 30,    # Landsat 8/9 thermal is resampled to 30m in C2
            'sar': 10,        # Sentinel-1 GRD is processed to 10m
            'flow': 30,      # MERIT Hydro flow direction is 30m
            'slope': 30,      # Slope is derived from DEM at 30m resolution
            'aspect': 30      # Aspect is derived from DEM at 30m
        }
        
        # patches
        self.PATCH = True
        self.PATCH_SIZE = 224
        self.PATCH_STRIDE = 224
        self.BATCH_EXPORT_SIZE = 500
        
        self.VISUALIZE_POINTS = True  # whether to visualize points on the map
        

        # Google Drive API
        self.SCOPES = ['https://www.googleapis.com/auth/drive']