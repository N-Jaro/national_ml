#!/usr/bin/env python3
"""
Environment Validation Script for pytorch_gpu_cu118
Verifies that all required packages are working correctly.
"""

import sys
import importlib
from packaging import version

def test_import(module_name, min_version=None):
    """Test importing a module and optionally check version."""
    try:
        module = importlib.import_module(module_name)
        
        if hasattr(module, '__version__'):
            current_version = module.__version__
            if min_version and version.parse(current_version) < version.parse(min_version):
                print(f"❌ {module_name}: {current_version} (required >= {min_version})")
                return False
            else:
                print(f"✅ {module_name}: {current_version}")
        else:
            print(f"✅ {module_name}: imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"❌ {module_name}: {str(e)}")
        return False

def main():
    print("=" * 60)
    print("PyTorch GPU CUDA 11.8 Environment Validation")
    print("=" * 60)
    
    # Core packages for national_ml
    required_packages = [
        ("torch", "2.5.0"),
        ("torchvision", "0.20.0"),
        ("torchaudio", "2.5.0"),
        ("numpy", "2.0.0"),
        ("pandas", "2.0.0"),
        ("rasterio", "1.4.0"),
        ("geopandas", "1.0.0"),
        ("ee", "1.6.0"),
        ("pysheds", None),
        ("tifffile", None),
        ("tqdm", None),
        ("requests", None),
    ]
    
    print("\n--- Testing Core Package Imports ---")
    all_passed = True
    
    for package, min_ver in required_packages:
        if not test_import(package, min_ver):
            all_passed = False
    
    # Training-specific packages
    training_packages = [
        ("pytorch_lightning", "2.5.0"),
        ("wandb", "0.22.0"),
        ("torchmetrics", "1.8.0"),
    ]
    
    print("\n--- Training Package Tests ---")
    for package, min_ver in training_packages:
        if not test_import(package, min_ver):
            all_passed = False
    
    # Special tests
    print("\n--- Special Functionality Tests ---")
    
    # PyTorch CUDA test
    try:
        import torch
        cuda_available = torch.cuda.is_available()
        cuda_version = torch.version.cuda if hasattr(torch.version, 'cuda') else "Unknown"
        device_count = torch.cuda.device_count()
        
        print(f"✅ PyTorch CUDA: Available={cuda_available}, Version={cuda_version}, Devices={device_count}")
        
        if cuda_available:
            device_name = torch.cuda.get_device_name(0)
            print(f"  GPU Device: {device_name}")
        else:
            print("  Note: CUDA not available on head node (expected)")
            
    except Exception as e:
        print(f"❌ PyTorch CUDA test failed: {e}")
        all_passed = False
    
    # PyTorch Lightning test
    try:
        import pytorch_lightning as pl
        print(f"✅ PyTorch Lightning: Version {pl.__version__}")
        
    except Exception as e:
        print(f"❌ PyTorch Lightning test failed: {e}")
        all_passed = False
    
    # W&B test  
    try:
        import wandb
        print(f"✅ Weights & Biases: Version {wandb.__version__}")
        
    except Exception as e:
        print(f"❌ W&B test failed: {e}")
        all_passed = False
    
    # Rasterio test
    try:
        import rasterio
        from rasterio.env import GDALVersion
        gdal_version = GDALVersion.runtime()
        print(f"✅ Rasterio GDAL: {gdal_version}")
        
    except Exception as e:
        print(f"❌ Rasterio GDAL test failed: {e}")
        all_passed = False
    
    # Earth Engine test
    try:
        import ee
        print("✅ Earth Engine: Imported (authentication required for full functionality)")
        
    except Exception as e:
        print(f"❌ Earth Engine test failed: {e}")
        all_passed = False
    
    print("\n--- Python Environment Info ---")
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    
    # Check if we're in the right conda environment
    if 'pytorch_gpu_cu118' in sys.executable:
        print("✅ Running in pytorch_gpu_cu118 environment")
    else:
        print("⚠️  Not running in pytorch_gpu_cu118 environment")
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print("Environment is ready for national_ml pipeline.")
    else:
        print("❌ SOME TESTS FAILED!")
        print("Please check the error messages above and reinstall if needed.")
    print("=" * 60)
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())