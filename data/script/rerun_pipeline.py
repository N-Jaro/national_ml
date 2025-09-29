import os
import glob
import pandas as pd
import argparse
from config import Settings
from huc_process import HUCProcessor
from patch_process import PatchProcessor
from stats_process import StatsProcessor
from local_reference_process import LocalReferenceProcessor
from gdrive_manager import GoogleDriveManager

def check_stage1_complete(huc_root_folder: str, huc_id: str) -> bool:
    """
    Check if Stage 1 (HUC processing) is complete by verifying required files exist.
    """
    huc_folder = os.path.join(huc_root_folder, huc_id)
    if not os.path.exists(huc_folder):
        return False
    
    required_files = [
        f'huc8_{huc_id}_boundary.geojson',
        f'huc8_{huc_id}_center_points.csv',
        f'huc8_{huc_id}_center_points_features.geojson',
        f'huc8_{huc_id}_dem.tif'
    ]
    
    for filename in required_files:
        filepath = os.path.join(huc_folder, filename)
        if not os.path.exists(filepath):
            print(f"    Missing: {filename}")
            return False
        # Also check if file is not empty
        if os.path.getsize(filepath) == 0:
            print(f"    Empty file: {filename}")
            return False
    
    return True

def check_stage2_complete(patch_root_folder: str, huc_id: str) -> bool:
    """
    Check if Stage 2 (Stats processing) is complete by verifying normalization_stats.json exists.
    """
    patch_folder = os.path.join(patch_root_folder, huc_id)
    if not os.path.exists(patch_folder):
        return False
    
    stats_file = os.path.join(patch_folder, 'normalization_stats.json')
    if not os.path.exists(stats_file):
        print(f"    Missing: normalization_stats.json")
        return False
    
    # Check if file is not empty
    if os.path.getsize(stats_file) == 0:
        print(f"    Empty file: normalization_stats.json")
        return False
    
    return True

def check_stage3_complete(patch_root_folder: str, huc_id: str, min_patches: int = 5) -> bool:
    """
    Check if Stage 3 (Patch processing) is complete by verifying patch files exist.
    """
    patch_folder = os.path.join(patch_root_folder, huc_id)
    if not os.path.exists(patch_folder):
        return False
    
    # Check for patch .npz files
    patch_files = glob.glob(os.path.join(patch_folder, 'patch_*.npz'))
    if len(patch_files) < min_patches:
        print(f"    Found only {len(patch_files)} patch files, expected at least {min_patches}")
        return False
    
    # Check for at least one georef template file
    template_files = glob.glob(os.path.join(patch_folder, 'patch_*_georef_template.tif'))
    if len(template_files) == 0:
        print(f"    Missing georef template files")
        return False
    
    return True

def check_stage4_complete(patch_root_folder: str, huc_id: str) -> bool:
    """
    Check if Stage 4 (Local reference processing) is complete by verifying flow and hydro files exist.
    """
    patch_folder = os.path.join(patch_root_folder, huc_id)
    if not os.path.exists(patch_folder):
        return False
    
    required_files = ['flow_direction.tif', 'hydro_mask.tif']
    
    for filename in required_files:
        filepath = os.path.join(patch_folder, filename)
        if not os.path.exists(filepath):
            print(f"    Missing: {filename}")
            return False
        # Check if file is not empty
        if os.path.getsize(filepath) == 0:
            print(f"    Empty file: {filename}")
            return False
    
    return True

def rerun_pipeline(huc_ids_to_process: list, force_stages: list = None):
    """
    Enhanced pipeline orchestrator that checks for actual output files instead of just folders.
    Only reruns stages that are incomplete or specifically forced.
    
    Args:
        huc_ids_to_process: List of HUC IDs to process
        force_stages: List of stage numbers to force rerun (e.g., [1, 3] to force stages 1 and 3)
    """
    # Initialize settings
    settings = Settings()
    
    huc_root_folder = os.path.join(settings.ROOT_OUTPUT_FOLDER, settings.HUC_OUTPUT_FOLDER)
    patch_root_folder = os.path.join(settings.ROOT_OUTPUT_FOLDER, settings.PATCH_OUTPUT_FOLDER)
    
    if force_stages is None:
        force_stages = []
    
    print(f"\n=== PIPELINE RERUN ANALYSIS FOR HUC(s): {huc_ids_to_process} ===")
    
    # Analyze current state for each HUC
    stage_status = {}
    for huc_id in huc_ids_to_process:
        print(f"\n--- Analyzing HUC {huc_id} ---")
        
        stage1_complete = check_stage1_complete(huc_root_folder, huc_id)
        stage2_complete = check_stage2_complete(patch_root_folder, huc_id)
        stage3_complete = check_stage3_complete(patch_root_folder, huc_id)
        stage4_complete = check_stage4_complete(patch_root_folder, huc_id)
        
        stage_status[huc_id] = {
            'stage1': stage1_complete,
            'stage2': stage2_complete,
            'stage3': stage3_complete,
            'stage4': stage4_complete
        }
        
        print(f"  Stage 1 (HUC processing): {'✓ Complete' if stage1_complete else '✗ Incomplete'}")
        print(f"  Stage 2 (Stats): {'✓ Complete' if stage2_complete else '✗ Incomplete'}")
        print(f"  Stage 3 (Patches): {'✓ Complete' if stage3_complete else '✗ Incomplete'}")
        print(f"  Stage 4 (Local ref): {'✓ Complete' if stage4_complete else '✗ Incomplete'}")

    # Determine what needs to be run
    hucs_needing_stage1 = [huc for huc in huc_ids_to_process 
                          if not stage_status[huc]['stage1'] or 1 in force_stages]
    hucs_needing_stage2 = [huc for huc in huc_ids_to_process 
                          if not stage_status[huc]['stage2'] or 2 in force_stages]
    hucs_needing_stage3 = [huc for huc in huc_ids_to_process 
                          if not stage_status[huc]['stage3'] or 3 in force_stages]
    hucs_needing_stage4 = [huc for huc in huc_ids_to_process 
                          if not stage_status[huc]['stage4'] or 4 in force_stages]
    
    print(f"\n=== EXECUTION PLAN ===")
    print(f"Stage 1 (HUC processing): {len(hucs_needing_stage1)} HUCs - {hucs_needing_stage1}")
    print(f"Stage 2 (Stats): {len(hucs_needing_stage2)} HUCs - {hucs_needing_stage2}")
    print(f"Stage 3 (Patches): {len(hucs_needing_stage3)} HUCs - {hucs_needing_stage3}")
    print(f"Stage 4 (Local ref): {len(hucs_needing_stage4)} HUCs - {hucs_needing_stage4}")

    # Stage 1: HUC Processing
    if hucs_needing_stage1:
        print(f"\n--- STARTING STAGE 1: HUC Center Point Generation for HUC(s): {hucs_needing_stage1} ---")
        huc_processor = HUCProcessor(settings=settings, huc_ids_to_process=hucs_needing_stage1)
        huc_processor.run()
        
        # Download and cleanup HUC outputs
        print("\n--- STARTING AUTOMATED DOWNLOAD AND CLEANUP ---")
        drive_manager = GoogleDriveManager(settings=settings)
        for huc_id in hucs_needing_stage1:
            huc_output_path = os.path.join(huc_root_folder, huc_id)
            try:
                drive_manager.merge_and_download_huc_outputs(huc_id=huc_id, local_destination_path=huc_output_path)
            except Exception as e:
                print(f"Error downloading outputs for HUC {huc_id}: {e}")
        
        print("\n--- STAGE 1 COMPLETE ---")
    else:
        print("\n--- SKIPPING STAGE 1: All HUC processing already complete ---")

    # Stage 2: Statistics Processing
    if hucs_needing_stage2:
        print(f"\n--- STARTING STAGE 2: Calculating Normalization Statistics for HUC(s): {hucs_needing_stage2} ---")
        
        for huc_id in hucs_needing_stage2:
            # Verify Stage 1 is complete before running Stage 2
            if not check_stage1_complete(huc_root_folder, huc_id):
                print(f"Skipping stats for HUC {huc_id} - Stage 1 not complete")
                continue
                
            try:
                print(f"\nProcessing stats for HUC {huc_id}...")
                stats_processor = StatsProcessor(settings=settings, huc_id=huc_id)
                stats_processor.run()
            except Exception as e:
                print(f"An error occurred during statistics calculation for HUC {huc_id}: {e}")
        
        print("\n--- STAGE 2 COMPLETE ---")
    else:
        print("\n--- SKIPPING STAGE 2: All statistics processing already complete ---")

    # Stage 3: Patch Processing
    if hucs_needing_stage3:
        print(f"\n--- STARTING STAGE 3: Raw Patch Fetching and Processing for HUC(s): {hucs_needing_stage3} ---")
        
        for huc_id in hucs_needing_stage3:
            # Verify prerequisites
            huc_folder_path = os.path.join(huc_root_folder, huc_id)
            if not check_stage1_complete(huc_root_folder, huc_id):
                print(f"Skipping patch processing for HUC {huc_id} - Stage 1 not complete")
                continue
            
            try:
                csv_path = os.path.join(huc_folder_path, f'huc8_{huc_id}_center_points.csv')
                if not os.path.exists(csv_path):
                    print(f"Skipping patch processing for HUC {huc_id} - CSV file not found")
                    continue
                
                print(f"\nProcessing raw patches for HUC {huc_id}...")
                center_points = pd.read_csv(csv_path).to_dict('records')
                if not center_points:
                    print(f"No center points found for HUC {huc_id}")
                    continue

                patch_huc_output_folder = os.path.join(patch_root_folder, huc_id)
                patch_processor = PatchProcessor(
                    center_points=center_points, 
                    output_folder=patch_huc_output_folder,
                    settings=settings, 
                    huc_id=huc_id
                )
                patch_processor.run()
            except Exception as e:
                print(f"An error occurred during patch processing for HUC {huc_id}: {e}")
        
        print("\n--- STAGE 3 COMPLETE ---")
    else:
        print("\n--- SKIPPING STAGE 3: All patch processing already complete ---")

    # Stage 4: Local NHD Hydrography Processing
    if hucs_needing_stage4:
        print(f"\n--- STARTING STAGE 4: Local NHD Hydrography Processing for HUC(s): {hucs_needing_stage4} ---")
        
        try:
            nhd_gdb_path = "/u/nathanj/national_ml/data/raw/nhdplus_gdb/NHDPlus_H_National_Release_2.gdb"
            
            if not os.path.exists(nhd_gdb_path):
                print("Warning: NHD GeoDatabase not found. Skipping local hydrography processing.")
            else:
                # Filter to only HUCs that need Stage 4 and have completed Stage 3
                valid_hucs = []
                for huc_id in hucs_needing_stage4:
                    if check_stage3_complete(patch_root_folder, huc_id):
                        valid_hucs.append(huc_id)
                    else:
                        print(f"Skipping Stage 4 for HUC {huc_id} - Stage 3 not complete")
                
                if valid_hucs:
                    local_processor = LocalReferenceProcessor(
                        settings=settings, 
                        nhd_gdb_path=nhd_gdb_path, 
                        huc_ids_to_process=valid_hucs
                    )
                    local_processor.run()
                    print("\nLocal NHD processing complete.")
                else:
                    print("No valid HUCs for Stage 4 processing.")
                    
        except Exception as e:
            print(f"An error occurred during local reference processing: {e}")
        
        print("\n--- STAGE 4 COMPLETE ---")
    else:
        print("\n--- SKIPPING STAGE 4: All local reference processing already complete ---")
            
    print("\n=== RERUN PIPELINE COMPLETE ===")
    
    # Final status report
    print(f"\n=== FINAL STATUS REPORT ===")
    for huc_id in huc_ids_to_process:
        print(f"\n--- HUC {huc_id} ---")
        stage1_final = check_stage1_complete(huc_root_folder, huc_id)
        stage2_final = check_stage2_complete(patch_root_folder, huc_id)
        stage3_final = check_stage3_complete(patch_root_folder, huc_id)
        stage4_final = check_stage4_complete(patch_root_folder, huc_id)
        
        print(f"  Stage 1: {'✓ Complete' if stage1_final else '✗ Incomplete'}")
        print(f"  Stage 2: {'✓ Complete' if stage2_final else '✗ Incomplete'}")
        print(f"  Stage 3: {'✓ Complete' if stage3_final else '✗ Incomplete'}")
        print(f"  Stage 4: {'✓ Complete' if stage4_final else '✗ Incomplete'}")
        
        all_complete = stage1_final and stage2_final and stage3_final and stage4_final
        print(f"  Overall: {'✓ ALL STAGES COMPLETE' if all_complete else '✗ SOME STAGES INCOMPLETE'}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Rerun the GEE data processing pipeline for specific HUC8 IDs with intelligent stage detection.")
    parser.add_argument(
        '--hucs', 
        nargs='+',
        required=True, 
        help='A list of HUC8 IDs to process.'
    )
    parser.add_argument(
        '--force', 
        nargs='*',
        type=int,
        choices=[1, 2, 3, 4],
        default=[],
        help='Force rerun specific stages (1=HUC, 2=Stats, 3=Patches, 4=LocalRef). Example: --force 1 3'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Analyze what would be run without actually executing'
    )
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("=== DRY RUN MODE: Analysis only, no execution ===")
        # Run analysis only by setting a flag or modifying the function
        # For now, we'll just run the analysis part
        settings = Settings()
        huc_root_folder = os.path.join(settings.ROOT_OUTPUT_FOLDER, settings.HUC_OUTPUT_FOLDER)
        patch_root_folder = os.path.join(settings.ROOT_OUTPUT_FOLDER, settings.PATCH_OUTPUT_FOLDER)
        
        print(f"\n=== PIPELINE STATUS ANALYSIS FOR HUC(s): {args.hucs} ===")
        
        for huc_id in args.hucs:
            print(f"\n--- Analyzing HUC {huc_id} ---")
            
            stage1_complete = check_stage1_complete(huc_root_folder, huc_id)
            stage2_complete = check_stage2_complete(patch_root_folder, huc_id)
            stage3_complete = check_stage3_complete(patch_root_folder, huc_id)
            stage4_complete = check_stage4_complete(patch_root_folder, huc_id)
            
            print(f"  Stage 1 (HUC processing): {'✓ Complete' if stage1_complete else '✗ Incomplete'}")
            print(f"  Stage 2 (Stats): {'✓ Complete' if stage2_complete else '✗ Incomplete'}")
            print(f"  Stage 3 (Patches): {'✓ Complete' if stage3_complete else '✗ Incomplete'}")
            print(f"  Stage 4 (Local ref): {'✓ Complete' if stage4_complete else '✗ Incomplete'}")
        
        # Show what would be executed
        hucs_needing_stage1 = [huc for huc in args.hucs 
                              if not check_stage1_complete(huc_root_folder, huc) or 1 in args.force]
        hucs_needing_stage2 = [huc for huc in args.hucs 
                              if not check_stage2_complete(patch_root_folder, huc) or 2 in args.force]
        hucs_needing_stage3 = [huc for huc in args.hucs 
                              if not check_stage3_complete(patch_root_folder, huc) or 3 in args.force]
        hucs_needing_stage4 = [huc for huc in args.hucs 
                              if not check_stage4_complete(patch_root_folder, huc) or 4 in args.force]
        
        print(f"\n=== WOULD EXECUTE ===")
        print(f"Stage 1: {len(hucs_needing_stage1)} HUCs - {hucs_needing_stage1}")
        print(f"Stage 2: {len(hucs_needing_stage2)} HUCs - {hucs_needing_stage2}")
        print(f"Stage 3: {len(hucs_needing_stage3)} HUCs - {hucs_needing_stage3}")
        print(f"Stage 4: {len(hucs_needing_stage4)} HUCs - {hucs_needing_stage4}")
        
    else:
        # Run the actual pipeline
        rerun_pipeline(huc_ids_to_process=args.hucs, force_stages=args.force)