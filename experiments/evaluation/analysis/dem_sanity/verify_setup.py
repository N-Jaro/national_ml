#!/usr/bin/env python3
"""
DEM Sanity Analysis - Setup Verification

This script verifies that all required components are available before running the analysis.
"""

import os
import sys
from pathlib import Path
import torch

def check_file_exists(file_path, description):
    """Check if a file exists and print status"""
    if os.path.exists(file_path):
        print(f"✅ {description}: {file_path}")
        return True
    else:
        print(f"❌ {description}: {file_path} (NOT FOUND)")
        return False

def check_directory_exists(dir_path, description):
    """Check if a directory exists and print status"""
    if os.path.isdir(dir_path):
        print(f"✅ {description}: {dir_path}")
        return True
    else:
        print(f"❌ {description}: {dir_path} (NOT FOUND)")
        return False

def check_huc_data(data_path, huc_list_path):
    """Check if HUC data is available"""
    try:
        with open(huc_list_path, 'r') as f:
            huc_codes = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        print(f"\n🔍 Checking data for {len(huc_codes)} HUCs...")
        
        available_hucs = 0
        missing_hucs = []
        
        for huc_code in huc_codes[:10]:  # Check first 10 for speed
            huc_dir = os.path.join(data_path, huc_code)
            if os.path.isdir(huc_dir):
                # Check for required files
                normalization_stats = os.path.join(huc_dir, "normalization_stats.json")
                patch_files = list(Path(huc_dir).glob("patch_*.npz"))
                
                if os.path.exists(normalization_stats) and len(patch_files) > 0:
                    available_hucs += 1
                    if available_hucs <= 3:  # Show first 3
                        print(f"  ✅ {huc_code}: {len(patch_files)} patches, normalization stats available")
                else:
                    missing_hucs.append(huc_code)
                    if len(missing_hucs) <= 3:  # Show first 3 missing
                        print(f"  ❌ {huc_code}: Missing data files")
            else:
                missing_hucs.append(huc_code)
                if len(missing_hucs) <= 3:  # Show first 3 missing
                    print(f"  ❌ {huc_code}: Directory not found")
        
        if available_hucs > 3:
            print(f"  ✅ ... and {available_hucs-3} more HUCs with data")
        
        if len(missing_hucs) > 3:
            print(f"  ❌ ... and {len(missing_hucs)-3} more HUCs with missing data")
        
        print(f"\n📊 Data Summary: {available_hucs}/{len(huc_codes[:10])} HUCs have required data (checked first 10)")
        
        return available_hucs > 0
        
    except Exception as e:
        print(f"❌ Error checking HUC data: {e}")
        return False

def main():
    print("🔬 DEM Sanity Analysis - Setup Verification")
    print("=" * 60)
    
    # Define paths
    dem_ae_checkpoint = "/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251004_112528_run2/checkpoints/mdmt-dem-alphaearth-epoch=37-val_loss=0.4154.ckpt"
    ae_only_checkpoint = "/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251003_162616_run10/checkpoints/mdmt-alphaearth-only-epoch=29-val_loss=0.8005.ckpt"
    huc_list_path = "/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt"
    test_data_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    
    # Check requirements
    all_good = True
    
    print("\n🧠 Model Checkpoints:")
    all_good &= check_file_exists(dem_ae_checkpoint, "DEM+AlphaEarth model")
    all_good &= check_file_exists(ae_only_checkpoint, "AlphaEarth-only model")
    
    print("\n📋 Test Configuration:")
    all_good &= check_file_exists(huc_list_path, "HUC list file")
    all_good &= check_directory_exists(test_data_path, "Test data directory")
    
    print("\n🗂️ Data Availability:")
    if os.path.exists(huc_list_path) and os.path.isdir(test_data_path):
        all_good &= check_huc_data(test_data_path, huc_list_path)
    
    print("\n🖥️ Environment:")
    print(f"✅ Python: {sys.version.split()[0]}")
    print(f"✅ PyTorch: {torch.__version__}")
    print(f"✅ CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"✅ CUDA device: {torch.cuda.get_device_name(0)}")
    
    # Check required modules
    print("\n📦 Required Modules:")
    try:
        import numpy as np
        print(f"✅ NumPy: {np.__version__}")
    except ImportError:
        print("❌ NumPy: Not available")
        all_good = False
    
    try:
        import pandas as pd
        print(f"✅ Pandas: {pd.__version__}")
    except ImportError:
        print("❌ Pandas: Not available")
        all_good = False
    
    try:
        import matplotlib
        print(f"✅ Matplotlib: {matplotlib.__version__}")
    except ImportError:
        print("❌ Matplotlib: Not available")
        all_good = False
    
    try:
        from scipy import ndimage
        print(f"✅ SciPy: Available")
    except ImportError:
        print("❌ SciPy: Not available")
        all_good = False
    
    try:
        from sklearn.metrics import precision_score
        print(f"✅ Scikit-learn: Available")
    except ImportError:
        print("❌ Scikit-learn: Not available")
        all_good = False
    
    try:
        from tqdm import tqdm
        print(f"✅ tqdm: Available")
    except ImportError:
        print("❌ tqdm: Not available")
        all_good = False
    
    # Final status
    print("\n" + "=" * 60)
    if all_good:
        print("🎉 SETUP VERIFICATION PASSED!")
        print("✅ All requirements are met. You can run the DEM sanity analysis.")
        print("\n🚀 Next steps:")
        print("   • Quick test: python quick_test_dem_sanity.py")
        print("   • Full analysis: sbatch run_dem_sanity_analysis.sh")
    else:
        print("🚨 SETUP VERIFICATION FAILED!")
        print("❌ Some requirements are missing. Please fix the issues above.")
        print("\n🔧 Common fixes:")
        print("   • Check checkpoint paths are correct")
        print("   • Ensure test data is processed and available")
        print("   • Activate the correct conda environment: conda activate pytorch_gpu_cu118")
    
    print("=" * 60)
    
    return 0 if all_good else 1

if __name__ == "__main__":
    exit(main())