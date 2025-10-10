#!/usr/bin/env python3
"""
Count patches for test HUCs in the national ML dataset.
This script counts patches in the test HUC list for evaluation purposes.
"""

import os
import glob
import pandas as pd
from pathlib import Path
from collections import defaultdict
import argparse

def load_test_hucs(test_huc_file):
    """Load test HUC codes from file."""
    test_hucs = []
    with open(test_huc_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                test_hucs.append(line)
    return test_hucs

def count_patches_by_directory_structure(patch_base_dir, test_hucs):
    """Count patches assuming directory structure organized by HUC."""
    patch_counts = {}
    total_patches = 0
    
    print(f"Searching for patches in: {patch_base_dir}")
    
    for huc in test_hucs:
        count = 0
        
        # Try different possible directory structures
        possible_paths = [
            os.path.join(patch_base_dir, huc),
            os.path.join(patch_base_dir, f"huc_{huc}"),
            os.path.join(patch_base_dir, huc[:2], huc),  # nested by first 2 digits
        ]
        
        for huc_dir in possible_paths:
            if os.path.exists(huc_dir):
                # Count different file types that might be patches
                patterns = ['*.tif', '*.tiff', '*.npy', '*.npz', '*.h5', '*.hdf5']
                for pattern in patterns:
                    files = glob.glob(os.path.join(huc_dir, pattern))
                    count += len(files)
                    
                    # Also check subdirectories
                    subdirs = glob.glob(os.path.join(huc_dir, '*', pattern))
                    count += len(subdirs)
                break
        
        patch_counts[huc] = count
        total_patches += count
        
        if count == 0:
            print(f"Warning: No patches found for HUC {huc}")
        elif count < 150:
            print(f"Warning: HUC {huc} has only {count} patches (< 150 threshold)")
    
    return patch_counts, total_patches

def count_patches_by_metadata_file(patch_base_dir, test_hucs):
    """Count patches using metadata files (CSV, parquet, etc.)."""
    patch_counts = defaultdict(int)
    total_patches = 0
    
    # Look for metadata files
    metadata_files = []
    for ext in ['*.csv', '*.parquet', '*.pkl', '*.json']:
        metadata_files.extend(glob.glob(os.path.join(patch_base_dir, ext)))
        metadata_files.extend(glob.glob(os.path.join(patch_base_dir, '**', ext), recursive=True))
    
    print(f"Found metadata files: {metadata_files}")
    
    for metadata_file in metadata_files:
        try:
            if metadata_file.endswith('.csv'):
                df = pd.read_csv(metadata_file)
            elif metadata_file.endswith('.parquet'):
                df = pd.read_parquet(metadata_file)
            else:
                continue
                
            # Look for HUC column (various possible names)
            huc_columns = ['huc', 'huc_code', 'huc8', 'huc_8', 'HUC', 'HUC_8', 'watershed']
            huc_col = None
            
            for col in huc_columns:
                if col in df.columns:
                    huc_col = col
                    break
            
            if huc_col:
                # Count patches for each test HUC
                for huc in test_hucs:
                    count = len(df[df[huc_col].astype(str) == huc])
                    patch_counts[huc] += count
                    total_patches += count
                    
                print(f"Processed {metadata_file}: found {len(df)} total patches")
            else:
                print(f"No HUC column found in {metadata_file}. Columns: {list(df.columns)}")
                
        except Exception as e:
            print(f"Error processing {metadata_file}: {e}")
    
    return dict(patch_counts), total_patches

def main():
    parser = argparse.ArgumentParser(description='Count patches for test HUCs')
    parser.add_argument('--patch_dir', 
                       default='/projects/bcrm/nathanj/data/processed/test/patch_dataset',
                       help='Base directory containing patch data')
    parser.add_argument('--test_huc_file', 
                       default='test_huc_list.txt',
                       help='File containing test HUC codes')
    parser.add_argument('--method', 
                       choices=['directory', 'metadata', 'both'], 
                       default='both',
                       help='Method to count patches')
    
    args = parser.parse_args()
    
    # Load test HUCs
    test_huc_file = args.test_huc_file
    if not os.path.isabs(test_huc_file):
        test_huc_file = os.path.join(os.path.dirname(__file__), test_huc_file)
    
    test_hucs = load_test_hucs(test_huc_file)
    print(f"Loaded {len(test_hucs)} test HUCs from {test_huc_file}")
    
    # Count patches
    patch_counts = {}
    total_patches = 0
    
    if args.method in ['directory', 'both']:
        print("\n=== Counting by directory structure ===")
        dir_counts, dir_total = count_patches_by_directory_structure(args.patch_dir, test_hucs)
        for huc, count in dir_counts.items():
            patch_counts[huc] = patch_counts.get(huc, 0) + count
        total_patches += dir_total
    
    if args.method in ['metadata', 'both']:
        print("\n=== Counting by metadata files ===")
        meta_counts, meta_total = count_patches_by_metadata_file(args.patch_dir, test_hucs)
        for huc, count in meta_counts.items():
            patch_counts[huc] = patch_counts.get(huc, 0) + count
        total_patches += meta_total
    
    # Results
    print(f"\n=== PATCH COUNT SUMMARY ===")
    print(f"Total test HUCs: {len(test_hucs)}")
    print(f"Total patches: {total_patches:,}")
    
    if total_patches > 0:
        print(f"Average patches per HUC: {total_patches / len(test_hucs):.1f}")
        
        # Show HUCs with low patch counts
        low_count_hucs = [(huc, count) for huc, count in patch_counts.items() if count < 150]
        if low_count_hucs:
            print(f"\nHUCs with < 150 patches ({len(low_count_hucs)} HUCs):")
            for huc, count in sorted(low_count_hucs, key=lambda x: x[1]):
                print(f"  {huc}: {count}")
        
        # Show HUCs with highest patch counts
        print(f"\nTop 10 HUCs by patch count:")
        top_hucs = sorted(patch_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for huc, count in top_hucs:
            print(f"  {huc}: {count:,}")
        
        # Show HUCs with no patches
        zero_count_hucs = [huc for huc, count in patch_counts.items() if count == 0]
        if zero_count_hucs:
            print(f"\nHUCs with no patches found ({len(zero_count_hucs)} HUCs):")
            for huc in zero_count_hucs:
                print(f"  {huc}")
    
    # Save results to file
    results_file = 'test_huc_patch_counts.csv'
    results_df = pd.DataFrame([
        {'huc': huc, 'patch_count': patch_counts.get(huc, 0)} 
        for huc in test_hucs
    ])
    results_df.to_csv(results_file, index=False)
    print(f"\nResults saved to: {results_file}")
    
    return patch_counts, total_patches

if __name__ == "__main__":
    main()