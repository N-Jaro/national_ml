#!/usr/bin/env python3
"""
Create a test_huc_list.txt by sampling HUCs with sufficient patches, grouped by region
"""

import os
import glob
import random
from collections import defaultdict

def count_patches_in_huc(huc_path):
    """Count number of patch files in a HUC directory"""
    if not os.path.exists(huc_path):
        return 0
    
    # Count .npz files in the HUC directory
    patch_files = glob.glob(os.path.join(huc_path, "*.npz"))
    return len(patch_files)

def get_hucs_with_sufficient_patches(test_path, min_patches=100):
    """Get HUCs with at least min_patches patches"""
    hucs_with_patches = []
    
    if not os.path.exists(test_path):
        print(f"Error: Path {test_path} does not exist")
        return hucs_with_patches
    
    print(f"Scanning {test_path} for HUCs with at least {min_patches} patches...")
    
    for item in os.listdir(test_path):
        item_path = os.path.join(test_path, item)
        if os.path.isdir(item_path) and item.isdigit() and len(item) == 8:
            patch_count = count_patches_in_huc(item_path)
            if patch_count >= min_patches:
                hucs_with_patches.append((item, patch_count))
                print(f"  {item}: {patch_count} patches ✓")
            else:
                print(f"  {item}: {patch_count} patches (skipped)")
    
    return hucs_with_patches

def group_hucs_by_region(hucs_with_patches):
    """Group HUCs by their first 2 digits (region code)"""
    regions = defaultdict(list)
    
    for huc, patch_count in hucs_with_patches:
        region_code = huc[:2]
        regions[region_code].append((huc, patch_count))
    
    # Sort each region's HUCs by patch count (descending)
    for region_code in regions:
        regions[region_code].sort(key=lambda x: x[1], reverse=True)
    
    return regions

def sample_hucs_from_regions(regions, max_per_region=10):
    """Sample up to max_per_region HUCs from each region"""
    sampled_hucs = []
    
    print(f"\nSampling up to {max_per_region} HUCs from each region:")
    print("=" * 60)
    
    for region_code in sorted(regions.keys()):
        region_hucs = regions[region_code]
        # Take up to max_per_region HUCs from this region
        selected = region_hucs[:max_per_region]
        
        print(f"Region {region_code}: {len(region_hucs)} available, selected {len(selected)}")
        for huc, patch_count in selected:
            print(f"  {huc}: {patch_count} patches")
            sampled_hucs.append(huc)
        
        if len(region_hucs) > max_per_region:
            print(f"  ... and {len(region_hucs) - max_per_region} more available")
        print()
    
    return sorted(sampled_hucs)

def main():
    test_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    min_patches = 100
    max_per_region = 10
    
    # Set random seed for reproducible sampling
    random.seed(42)
    
    print("Creating test HUC list from existing dataset...")
    print("=" * 60)
    
    # Get HUCs with sufficient patches
    hucs_with_patches = get_hucs_with_sufficient_patches(test_path, min_patches)
    
    if not hucs_with_patches:
        print(f"❌ No HUCs found with at least {min_patches} patches!")
        return
    
    print(f"\nFound {len(hucs_with_patches)} HUCs with at least {min_patches} patches")
    
    # Group by region (first 2 digits)
    regions = group_hucs_by_region(hucs_with_patches)
    print(f"Found {len(regions)} different regions: {sorted(regions.keys())}")
    
    # Sample HUCs from each region
    sampled_hucs = sample_hucs_from_regions(regions, max_per_region)
    
    print("=" * 60)
    print(f"Final selection: {len(sampled_hucs)} HUCs")
    
    # Create the test HUC list file
    with open('test_huc_list.txt', 'w') as f:
        f.write("# Test HUC List for MDMT Evaluation\n")
        f.write("# Sampled from existing HUCs with sufficient patches\n")
        f.write(f"# Criteria: At least {min_patches} patches per HUC\n")
        f.write(f"# Sampling: Up to {max_per_region} HUCs per region (first 2 digits)\n")
        f.write(f"# Total HUCs: {len(sampled_hucs)} from {len(regions)} regions\n")
        f.write("# Generated on 2025-10-06\n")
        f.write("# Format: One HUC code per line\n")
        f.write("\n")
        
        for huc in sampled_hucs:
            f.write(f"{huc}\n")
    
    print(f"✅ Created test_huc_list.txt with {len(sampled_hucs)} HUCs")
    print(f"   Coverage: {len(regions)} regions with up to {max_per_region} HUCs each")
    print(f"   All selected HUCs have at least {min_patches} patches")
    
    # Show summary by region
    print("\nSummary by region:")
    for region_code in sorted(regions.keys()):
        region_selected = [huc for huc in sampled_hucs if huc.startswith(region_code)]
        print(f"  Region {region_code}: {len(region_selected)} HUCs selected")
    
    print(f"\n🎯 Ready for evaluation with {len(sampled_hucs)} diverse, high-quality test HUCs!")

if __name__ == "__main__":
    main()