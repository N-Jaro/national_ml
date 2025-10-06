import os
import glob
import numpy as np
from typing import List, Set

def check_npz_keys_fast(file_path: str) -> Set[str]:
    """
    Fast way to check what keys are in an .npz file without loading the arrays.
    This is much faster than loading the full data.
    """
    try:
        with np.load(file_path, mmap_mode='r') as data:
            return set(data.keys())
    except:
        return set()

def validate_patch_files_fast(huc_dir: str, required_keys: List[str]) -> List[str]:
    """
    Fast validation of patch files in a HUC directory.
    Only loads file headers to check keys, not the full arrays.
    
    Args:
        huc_dir: Directory containing patch_*.npz files
        required_keys: List of required keys (e.g., ['dem', 'hydro_mask', 'flow_dir'])
    
    Returns:
        List of valid file paths
    """
    if not os.path.isdir(huc_dir):
        return []
    
    valid_files = []
    patch_files = glob.glob(os.path.join(huc_dir, "patch_*.npz"))
    
    required_set = set(required_keys)
    
    for patch_file in patch_files:
        file_keys = check_npz_keys_fast(patch_file)
        if required_set.issubset(file_keys):
            valid_files.append(patch_file)
    
    return valid_files

def count_valid_patches_by_huc(base_path: str, huc_codes: List[str], required_keys: List[str]) -> dict:
    """
    Quickly count how many valid patches each HUC has without loading data.
    Useful for debugging and dataset statistics.
    """
    results = {}
    
    for huc_code in huc_codes:
        huc_dir = os.path.join(base_path, huc_code)
        valid_files = validate_patch_files_fast(huc_dir, required_keys)
        results[huc_code] = {
            'valid_patches': len(valid_files),
            'valid_files': valid_files
        }
    
    return results

# Example usage for different modalities:
REQUIRED_KEYS = {
    'alphaearth': ['alphaearth', 'hydro_mask', 'flow_dir'],
    'dem_thermal': ['dem', 'thermal', 'hydro_mask', 'flow_dir'],
    'dem_sar': ['dem', 'sar', 'hydro_mask', 'flow_dir'],
    'dem_optical': ['dem', 'optical', 'hydro_mask', 'flow_dir'],
    'dem_alphaearth': ['dem', 'alphaearth', 'hydro_mask', 'flow_dir'],
    'landsat6b': ['optical', 'hydro_mask', 'flow_dir']  # landsat key is actually 'optical'
}

if __name__ == "__main__":
    # Example: Check what data is available for specific HUCs
    test_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    test_hucs = ["02080201", "08070202", "08050002"]
    
    print("Checking data availability for test HUCs...")
    print("=" * 60)
    
    for variant, keys in REQUIRED_KEYS.items():
        print(f"\n{variant.upper()} variant (requires: {keys}):")
        results = count_valid_patches_by_huc(test_path, test_hucs, keys)
        
        for huc, info in results.items():
            print(f"  {huc}: {info['valid_patches']} valid patches")
        
        total_patches = sum(info['valid_patches'] for info in results.values())
        print(f"  Total: {total_patches} patches across {len(test_hucs)} HUCs")