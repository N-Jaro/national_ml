#!/usr/bin/env python3

"""
Simple test script to verify the file checking logic works correctly
"""

import os
import glob

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

def test_huc_status(huc_id: str):
    """Test the status checking for a specific HUC"""
    
    # Use the actual paths from config
    root_output = '/u/nathanj/national_ml/data/processed/test'
    huc_root = os.path.join(root_output, 'huc_processing')
    patch_root = os.path.join(root_output, 'patch_dataset')
    
    print(f"\n=== TESTING STATUS CHECK FOR HUC {huc_id} ===")
    
    print(f"\n--- Analyzing HUC {huc_id} ---")
    
    stage1_complete = check_stage1_complete(huc_root, huc_id)
    stage2_complete = check_stage2_complete(patch_root, huc_id)
    stage3_complete = check_stage3_complete(patch_root, huc_id)
    stage4_complete = check_stage4_complete(patch_root, huc_id)
    
    print(f"  Stage 1 (HUC processing): {'✓ Complete' if stage1_complete else '✗ Incomplete'}")
    print(f"  Stage 2 (Stats): {'✓ Complete' if stage2_complete else '✗ Incomplete'}")
    print(f"  Stage 3 (Patches): {'✓ Complete' if stage3_complete else '✗ Incomplete'}")
    print(f"  Stage 4 (Local ref): {'✓ Complete' if stage4_complete else '✗ Incomplete'}")
    
    all_complete = stage1_complete and stage2_complete and stage3_complete and stage4_complete
    print(f"  Overall: {'✓ ALL STAGES COMPLETE' if all_complete else '✗ SOME STAGES INCOMPLETE'}")
    
    return {
        'stage1': stage1_complete,
        'stage2': stage2_complete,
        'stage3': stage3_complete,
        'stage4': stage4_complete
    }

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python test_file_checks.py HUC_ID")
        print("Example: python test_file_checks.py 19010207")
        sys.exit(1)
    
    huc_id = sys.argv[1]
    test_huc_status(huc_id)