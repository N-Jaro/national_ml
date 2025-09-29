import os
import glob
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

    # Check if all required output files already exist for the specified HUC IDs
    def check_huc_complete(huc_id):
        huc_folder = os.path.join(huc_root_folder, huc_id)
        if not os.path.exists(huc_folder):
            return False
        required_files = [
            f'huc8_{huc_id}_boundary.geojson',
            f'huc8_{huc_id}_center_points.csv',
            f'huc8_{huc_id}_center_points_features.geojson',
            f'huc8_{huc_id}_dem.tif'
        ]
        return all(os.path.exists(os.path.join(huc_folder, f)) and os.path.getsize(os.path.join(huc_folder, f)) > 0 for f in required_files)
    
    def check_patch_complete(huc_id):
        patch_folder = os.path.join(patch_root_folder, huc_id)
        if not os.path.exists(patch_folder):
            return False
        # Check for normalization stats, at least 5 patch files, and flow/hydro files
        stats_file = os.path.join(patch_folder, 'normalization_stats.json')
        flow_file = os.path.join(patch_folder, 'flow_direction.tif')
        hydro_file = os.path.join(patch_folder, 'hydro_mask.tif')
        
        import glob
        patch_files = glob.glob(os.path.join(patch_folder, 'patch_*.npz'))
        
        return (os.path.exists(stats_file) and os.path.getsize(stats_file) > 0 and
                os.path.exists(flow_file) and os.path.getsize(flow_file) > 0 and
                os.path.exists(hydro_file) and os.path.getsize(hydro_file) > 0 and
                len(patch_files) >= 5)
    
    all_huc_complete = all(check_huc_complete(huc_id) for huc_id in huc_ids_to_process)
    all_patch_complete = all(check_patch_complete(huc_id) for huc_id in huc_ids_to_process)

    if all_huc_complete and all_patch_complete:
        print(f"All processing already complete for HUC(s) {huc_ids_to_process}. Skipping pipeline execution.")
        print("--- PIPELINE SKIPPED ---")
        return

    # =========================================================================
    # ==      STAGE 1: Generate HUC Center Points                            ==
    # =========================================================================
    
    hucs_needing_stage1 = [huc_id for huc_id in huc_ids_to_process if not check_huc_complete(huc_id)]
    
    if not hucs_needing_stage1:
        print(f"--- SKIPPING STAGE 1: HUC processing already complete for all HUC(s): {huc_ids_to_process} ---")
    else:
        print(f"--- STARTING STAGE 1: HUC Center Point Generation for HUC(s): {hucs_needing_stage1} ---")
        # Pass the specific HUC IDs to the processor
        huc_processor = HUCProcessor(settings=settings, huc_ids_to_process=hucs_needing_stage1)
        huc_processor.run()
        
        # download and cleanup huc code bouandaries and DEMs
        print("\n--- STARTING AUTOMATED DOWNLOAD AND CLEANUP ---")
        drive_manager = GoogleDriveManager(settings=settings)
        for huc_id in hucs_needing_stage1:
            drive_manager.merge_and_download_huc_outputs(huc_id=huc_id, local_destination_path=os.path.join(huc_root_folder, huc_id))
        
        print("\n--- STAGE 1 COMPLETE ---")

    # =========================================================================
    # ==      STAGE 2: Calculate Normalization Statistics                    ==
    # =========================================================================

    # Check which HUCs have complete Stage 1 outputs (required for Stage 2)
    hucs_with_complete_stage1 = [huc_id for huc_id in huc_ids_to_process if check_huc_complete(huc_id)]
    
    if not hucs_with_complete_stage1:
        print("\n--- SKIPPING STAGE 2: No HUC folders with complete Stage 1 outputs found ---")
    else:
        print("\n--- STARTING STAGE 2: Calculating Normalization Statistics ---")
        
        for huc_id in hucs_with_complete_stage1:
            # Check if stats already exist
            stats_file = os.path.join(patch_root_folder, huc_id, 'normalization_stats.json')
            if os.path.exists(stats_file) and os.path.getsize(stats_file) > 0:
                print(f"\nSkipping stats for HUC {huc_id} - already exists")
                continue
                
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
    
    hucs_needing_patches = []
    for huc_id in huc_ids_to_process:
        if not check_huc_complete(huc_id):
            print(f"\nSkipping patch processing for HUC {huc_id} - Stage 1 not complete")
            continue
            
        # Check if patches already exist
        patch_folder = os.path.join(patch_root_folder, huc_id)
        import glob
        existing_patches = glob.glob(os.path.join(patch_folder, 'patch_*.npz')) if os.path.exists(patch_folder) else []
        
        if len(existing_patches) >= 5:  # Assume at least 5 patches means processing is complete
            print(f"\nSkipping patch processing for HUC {huc_id} - {len(existing_patches)} patches already exist")
            continue
            
        hucs_needing_patches.append(huc_id)
    
    if not hucs_needing_patches:
        print("\n--- SKIPPING STAGE 3: Patch processing already complete for all valid HUCs ---")
    else:
        print(f"\n--- STARTING STAGE 3: Raw Patch Fetching and Processing for HUC(s): {hucs_needing_patches} ---")
        
        for huc_id in hucs_needing_patches:
            huc_folder_path = os.path.join(huc_root_folder, huc_id)
            try:
                csv_path = os.path.join(huc_folder_path, f'huc8_{huc_id}_center_points.csv')
                if not os.path.exists(csv_path): 
                    print(f"Skipping HUC {huc_id} - CSV file not found")
                    continue
                
                print(f"\nProcessing raw patches for HUC {huc_id}...")
                center_points = pd.read_csv(csv_path).to_dict('records')
                if not center_points: 
                    print(f"No center points found for HUC {huc_id}")
                    continue

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
