#!/usr/bin/env python3
"""
Check if all HUCs in test_huc_list.txt exist in the test dataset
"""

import os
import sys

def read_test_huc_list():
    """Read HUCs from test_huc_list.txt"""
    hucs = []
    with open('test_huc_list.txt', 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                hucs.append(line)
    return hucs

def get_available_hucs(test_path):
    """Get available HUCs from test dataset directory"""
    available_hucs = []
    if os.path.exists(test_path):
        for item in os.listdir(test_path):
            item_path = os.path.join(test_path, item)
            if os.path.isdir(item_path) and item.isdigit() and len(item) == 8:
                available_hucs.append(item)
    return sorted(available_hucs)

def main():
    test_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    
    print("Checking HUCs in test_huc_list.txt against available test dataset...")
    print("=" * 70)
    
    # Read test HUC list
    test_hucs = read_test_huc_list()
    print(f"HUCs in test_huc_list.txt: {len(test_hucs)}")
    
    # Get available HUCs
    available_hucs = get_available_hucs(test_path)
    print(f"Available HUCs in test dataset: {len(available_hucs)}")
    print()
    
    # Check which HUCs exist and which don't
    existing_hucs = []
    missing_hucs = []
    
    for huc in test_hucs:
        if huc in available_hucs:
            existing_hucs.append(huc)
        else:
            missing_hucs.append(huc)
    
    print(f"✅ HUCs that EXIST in test dataset: {len(existing_hucs)}")
    print(f"❌ HUCs that are MISSING from test dataset: {len(missing_hucs)}")
    print()
    
    if missing_hucs:
        print("MISSING HUCs:")
        print("-" * 40)
        for huc in missing_hucs:
            print(f"  {huc}")
        print()
    
    if existing_hucs:
        print("EXISTING HUCs (first 20):")
        print("-" * 40)
        for huc in existing_hucs[:20]:
            print(f"  {huc}")
        if len(existing_hucs) > 20:
            print(f"  ... and {len(existing_hucs) - 20} more")
        print()
    
    # Summary
    print("SUMMARY:")
    print("=" * 40)
    print(f"Total HUCs in test list: {len(test_hucs)}")
    print(f"HUCs available in dataset: {len(existing_hucs)}")
    print(f"HUCs missing from dataset: {len(missing_hucs)}")
    print(f"Success rate: {len(existing_hucs)/len(test_hucs)*100:.1f}%")
    
    if missing_hucs:
        print("\n⚠️  WARNING: Some HUCs in the test list are not available in the test dataset!")
        print("   Consider removing missing HUCs or checking if they exist elsewhere.")
        return 1
    else:
        print("\n✅ SUCCESS: All HUCs in test list are available in the test dataset!")
        return 0

if __name__ == "__main__":
    sys.exit(main())