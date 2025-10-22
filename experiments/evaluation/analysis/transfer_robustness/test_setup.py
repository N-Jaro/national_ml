#!/usr/bin/env python3
"""
Test configuration and basic setup for transfer/robustness analysis.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.append(str(project_root))

from config import config

def test_configuration():
    """Test basic configuration setup."""
    print("🧪 TESTING CONFIGURATION")
    print("=" * 40)
    
    # Print configuration summary
    config.summary()
    
    # Test path validation
    try:
        config.validate_paths()
        print("\n✅ Path validation passed!")
    except FileNotFoundError as e:
        print(f"\n❌ Path validation failed: {e}")
        return False
    
    # Test terrain classification
    print(f"\nTerrain classification examples:")
    reliefs = [10.0, 25.0, 50.0, 100.0, 200.0, 500.0]
    for relief in reliefs:
        terrain = config.get_terrain_classification(relief)
        print(f"  {relief:6.1f}m → {terrain}")
    
    # Test modalities
    print(f"\nExpected modalities: {config.expected_modalities}")
    print(f"Optical channels: {config.optical_channels}")
    
    return True

def test_huc_list():
    """Test loading HUC list."""
    print(f"\n🧪 TESTING HUC LIST")
    print("=" * 40)
    
    try:
        huc_list = []
        with open(config.huc_list_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    huc_list.append(line)
        
        print(f"✅ Loaded {len(huc_list)} HUCs:")
        for huc in huc_list:
            print(f"  {huc}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading HUC list: {e}")
        return False

def test_data_availability():
    """Test if test data is available for our HUCs."""
    print(f"\n🧪 TESTING DATA AVAILABILITY")
    print("=" * 40)
    
    import os
    from pathlib import Path
    
    # Load HUC list
    huc_list = []
    with open(config.huc_list_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                huc_list.append(line)
    
    available_hucs = []
    missing_hucs = []
    
    for huc_code in huc_list:
        huc_dir = os.path.join(config.test_data_path, huc_code)
        
        if os.path.exists(huc_dir):
            # Check for patch files
            patch_files = list(Path(huc_dir).glob("patch_*.npz"))
            if patch_files:
                available_hucs.append((huc_code, len(patch_files)))
                print(f"  ✅ {huc_code}: {len(patch_files)} patches")
            else:
                missing_hucs.append(huc_code)
                print(f"  ⚠️  {huc_code}: directory exists but no patches")
        else:
            missing_hucs.append(huc_code)
            print(f"  ❌ {huc_code}: directory not found")
    
    print(f"\nSummary:")
    print(f"  Available HUCs: {len(available_hucs)}")
    print(f"  Missing HUCs: {len(missing_hucs)}")
    
    if available_hucs:
        total_patches = sum(count for _, count in available_hucs)
        print(f"  Total patches: {total_patches}")
        
        if config.limit_patches_per_huc:
            limited_patches = min(config.limit_patches_per_huc, total_patches)
            print(f"  Limited to {config.limit_patches_per_huc} per HUC = ~{limited_patches} patches")
    
    return len(available_hucs) > 0

def main():
    """Run all tests."""
    print("🚀 TRANSFER/ROBUSTNESS ANALYSIS - SETUP TEST")
    print("=" * 60)
    
    tests_passed = 0
    total_tests = 3
    
    # Test configuration
    if test_configuration():
        tests_passed += 1
    
    # Test HUC list loading
    if test_huc_list():
        tests_passed += 1
    
    # Test data availability
    if test_data_availability():
        tests_passed += 1
    
    print(f"\n📊 TEST SUMMARY")
    print("=" * 20)
    print(f"Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("✅ All tests passed! Ready for analysis.")
        return True
    else:
        print("❌ Some tests failed. Check configuration.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)