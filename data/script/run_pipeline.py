import os
import pandas as pd
import argparse # <-- Import for command-line arguments
from config import Settings
from huc_process import HUCProcessor
from patch_process import PatchProcessor
from stats_process import StatsProcessor
from local_reference_process import LocalReferenceProcessor
from gdrive_manager import GoogleDriveManager

def run_pipeline(huc_ids_to_process: list):
    """
    Main orchestrator for the multi-stage data processing pipeline.
    This version processes only the HUC IDs passed as an argument.
    """
    # =========================================================================
    # ==                      INITIALIZE SETTINGS                            ==
    # =========================================================================
    
    settings = Settings()
    
    huc_root_folder = os.path.join(settings.ROOT_OUTPUT_FOLDER, settings.HUC_OUTPUT_FOLDER)
    patch_root_folder = os.path.join(settings.ROOT_OUTPUT_FOLDER, settings.PATCH_OUTPUT_FOLDER)

    # =========================================================================
    # ==      STAGE 1: Generate HUC Center Points                            ==
    # =========================================================================
    
    print(f"--- STARTING STAGE 1: HUC Center Point Generation for HUC(s): {huc_ids_to_process} ---")
    # Pass the specific HUC IDs to the processor
    huc_processor = HUCProcessor(settings=settings, huc_ids_to_process=huc_ids_to_process)
    huc_processor.run()
    
    # download and cleanup huc code bouandaries and DEMs
    print("\n--- STARTING AUTOMATED DOWNLOAD AND CLEANUP ---")
    drive_manager = GoogleDriveManager(settings=settings)
    for huc_id in huc_ids_to_process:
        drive_manager.merge_and_download_huc_outputs(huc_id=huc_id, local_destination_path=os.path.join(huc_root_folder, huc_id))
    
    print("\n--- STAGE 1 COMPLETE ---")

    # =========================================================================
    # ==      STAGE 2: Calculate Normalization Statistics                    ==
    # =========================================================================

    print("\n--- STARTING STAGE 2: Calculating Normalization Statistics ---")
    
    for huc_id in huc_ids_to_process:
        try:
            print(f"\nProcessing stats for HUC {huc_id}...")
            stats_processor = StatsProcessor(settings=settings, huc_id=huc_id)
            stats_processor.run()
        except Exception as e:
            print(f"An error occurred during statistics calculation for HUC {huc_id}: {e}")

    print("\n--- STAGE 2 COMPLETE ---")

    # =========================================================================
    # ==      STAGE 3: Fetch Raw Image Patches                               ==
    # =========================================================================
    
    print("\n--- STARTING STAGE 3: Raw Patch Fetching and Processing ---")
    
    for huc_id in huc_ids_to_process:
        huc_folder_path = os.path.join(huc_root_folder, huc_id)
        if not os.path.isdir(huc_folder_path): continue
        try:
            csv_path = os.path.join(huc_folder_path, f'huc8_{huc_id}_center_points.csv')
            if not os.path.exists(csv_path): continue
            
            print(f"\nProcessing raw patches for HUC {huc_id}...")
            center_points = pd.read_csv(csv_path).to_dict('records')
            if not center_points: continue

            patch_huc_output_folder = os.path.join(patch_root_folder, huc_id)
            patch_processor = PatchProcessor(
                center_points=center_points, output_folder=patch_huc_output_folder,
                settings=settings, huc_id=huc_id
            )
            patch_processor.run()
        except Exception as e:
            print(f"An error occurred during patch processing for HUC {huc_id}: {e}")
            
    print("\n--- STAGE 3 COMPLETE ---")

    # =========================================================================
    # ==      STAGE 4: Local NHD Hydrography Mask Generation                 ==
    # =========================================================================

    print("\n--- STARTING STAGE 4: Local NHD Hydrography Processing ---")
    try:
        nhd_gdb_path = "/u/nathanj/national_ml/data/raw/nhdplus_gdb/NHDPlus_H_National_Release_2.gdb"
        
        if not os.path.exists(nhd_gdb_path):
            print("Warning: NHD GeoDatabase not found. Skipping local hydrography processing.")
        else:
            # This processor will find all HUC folders in the output directory
            local_processor = LocalReferenceProcessor(settings=settings, nhd_gdb_path=nhd_gdb_path, huc_ids_to_process=huc_ids_to_process)
            local_processor.run()
            print("\nLocal NHD processing complete.")
            
    except Exception as e:
        print(f"An error occurred during local reference processing: {e}")
            
    print("\n--- PIPELINE COMPLETE ---")


if __name__ == '__main__':
    # --- Setup Command-Line Argument Parsing ---
    parser = argparse.ArgumentParser(description="Run the GEE data processing pipeline for specific HUC8 IDs.")
    parser.add_argument(
        '--hucs', 
        nargs='+',  # This allows one or more HUC IDs to be passed
        required=True, 
        help='A list of HUC8 IDs to process.'
    )
    args = parser.parse_args()
    
    # --- Run the pipeline with the provided HUCs ---
    run_pipeline(huc_ids_to_process=args.hucs)
