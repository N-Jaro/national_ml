# run_pipeline.py

import os
import pandas as pd
from config import Settings
from huc_process import HUCProcessor
from patch_process import PatchProcessor
from stats_process import StatsProcessor
from local_reference_process import LocalReferenceProcessor # <-- Import new local processor
from gdrive_manager import GoogleDriveManager

def run_pipeline():
    """
    Main orchestrator for the multi-stage data processing pipeline.
    """
    settings = Settings()
    huc_root_folder = os.path.join(settings.ROOT_OUTPUT_FOLDER, settings.HUC_OUTPUT_FOLDER)
    patch_root_folder = os.path.join(settings.ROOT_OUTPUT_FOLDER, settings.PATCH_OUTPUT_FOLDER)

    # --- STAGE 1: HUC Processing (Center Points & Full DEM Export) ---
    print("--- STARTING STAGE 1: HUC Processing ---")
    huc_processor = HUCProcessor(settings=settings)
    huc_processor.run()
    
    # --- STAGE 2: Automated Download from Google Drive ---
    print("\n--- STARTING STAGE 2: Automated Download from Google Drive ---")
    try:
        drive_manager = GoogleDriveManager(settings=settings)
        for huc_id in settings.HUC_IDS_TO_PROCESS:
            drive_manager.merge_and_download_huc_outputs(huc_id=huc_id, local_destination_path=os.path.join(huc_root_folder, huc_id))
        print("\nAutomated download and cleanup complete.")
    except Exception as e:
        print(f"\n!!! AUTOMATED DOWNLOAD FAILED: {e} !!!"); input("\nPress Enter to continue...")

    # # --- STAGE 3: Statistics Calculation ---
    # print("\n--- STARTING STAGE 3: Calculating Normalization Statistics ---")
    # for huc_id in settings.HUC_IDS_TO_PROCESS:
    #     try:
    #         print(f"\nProcessing stats for HUC {huc_id}...")
    #         stats_processor = StatsProcessor(settings=settings, huc_id=huc_id)
    #         stats_processor.run()
    #     except Exception as e:
    #         print(f"An error occurred during statistics calculation for HUC {huc_id}: {e}")

    # # --- STAGE 4: Raw Patch Fetching ---
    # print("\n--- STARTING STAGE 4: Raw Patch Fetching ---")
    # for huc_id in settings.HUC_IDS_TO_PROCESS:
    #     huc_folder_path = os.path.join(huc_root_folder, huc_id)
    #     if not os.path.isdir(huc_folder_path): continue
    #     try:
    #         csv_path = os.path.join(huc_folder_path, f'huc8_{huc_id}_center_points.csv')
    #         if not os.path.exists(csv_path): continue
            
    #         print(f"\nProcessing raw patches for HUC {huc_id}...")
    #         center_points = pd.read_csv(csv_path).to_dict('records')
    #         if not center_points: continue

    #         patch_huc_output_folder = os.path.join(patch_root_folder, huc_id)
    #         patch_processor = PatchProcessor(center_points=center_points, output_folder=patch_huc_output_folder, settings=settings, huc_id=huc_id)
    #         patch_processor.run()
    #     except Exception as e:
    #         print(f"An error occurred during patch processing for HUC {huc_id}: {e}")

    # # --- STAGE 5: Local Reference Data Generation ---
    # print("\n--- STARTING STAGE 5: Local Reference Data Processing ---")
    # try:
    #     nhd_flowlines_path = "path/to/your/NHDFlowline.shp"
    #     nhd_waterbodies_path = "path/to/your/NHDWaterbody.shp"
    #     if not os.path.exists(nhd_flowlines_path) or not os.path.exists(nhd_waterbodies_path):
    #         print("Warning: NHD Shapefiles not found. Skipping local reference processing.")
    #     else:
    #         local_processor = LocalReferenceProcessor(settings=settings, nhd_flowline_shapefile=nhd_flowlines_path, nhd_waterbody_shapefile=nhd_waterbodies_path)
    #         local_processor.run()
    #         print("\nLocal reference processing complete.")
    # except Exception as e:
    #     print(f"An error occurred during local reference processing: {e}")
            
    print("\n--- PIPELINE COMPLETE ---")

if __name__ == '__main__':
    run_pipeline()
