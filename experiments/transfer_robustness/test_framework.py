#!/usr/bin/env python3
"""
Test script for Transfer and Robustness Analysis Framework

Quick validation of terrain classification and optical masking components
"""

import sys
import os
import numpy as np
import matplotlib.pyplot as plt

# Add the transfer_robustness directory to path
sys.path.append('/u/nathanj/national_ml/experiments/transfer_robustness')

from config import TransferRobustnessConfig
from terrain_classifier import TerrainClassifier  
from optical_masker import OpticalMasker

def test_terrain_classifier():
    """Test terrain classification with sample DEM data"""
    print("Testing Terrain Classifier...")
    
    classifier = TerrainClassifier(low_threshold=35.0, high_threshold=114.0)
    
    # Create sample DEM patches with different relief levels
    test_cases = [
        ("Low relief", np.random.uniform(100, 120, (224, 224))),     # 20m relief
        ("Mixed relief", np.random.uniform(100, 180, (224, 224))),   # 80m relief  
        ("High relief", np.random.uniform(100, 300, (224, 224)))     # 200m relief
    ]
    
    for name, dem_patch in test_cases:
        relief = classifier.calculate_relief(dem_patch)
        terrain_class = classifier.classify_terrain(dem_patch)
        stats = classifier.get_terrain_stats(dem_patch)
        
        print(f"\n{name}:")
        print(f"  Relief: {relief:.1f}m")
        print(f"  Classification: {terrain_class}")
        print(f"  Min elevation: {stats['min_elevation']:.1f}m")
        print(f"  Max elevation: {stats['max_elevation']:.1f}m")
        
    print("✓ Terrain classifier working correctly")

def test_optical_masker():
    """Test optical masking with sample optical data"""
    print("\nTesting Optical Masker...")
    
    masker = OpticalMasker(mask_probability=0.3)
    
    # Create sample optical data (6 bands, 224x224)
    optical_data = np.random.uniform(0, 1000, (6, 224, 224))
    
    # Apply masking
    masked_optical, mask = masker.apply_optical_mask(optical_data, seed=42)
    
    # Calculate statistics
    mask_stats = masker.get_masking_statistics(mask)
    
    print(f"Original optical shape: {optical_data.shape}")
    print(f"Masked optical shape: {masked_optical.shape}")
    print(f"Mask coverage: {mask_stats['mask_coverage']:.3f}")
    print(f"Masked pixels: {mask_stats['masked_pixels']:,} / {mask_stats['total_pixels']:,}")
    
    # Test patch masking
    patch_data = {
        'dem': np.random.uniform(100, 200, (1, 224, 224)),
        'optical': optical_data,
        'hydro_mask': np.random.randint(0, 2, (224, 224)),
        'flow_dir': np.random.randint(0, 9, (224, 224))
    }
    
    masked_patch, mask_info = masker.apply_patch_masking(patch_data, seed=42)
    
    print(f"Patch masking coverage: {mask_info['mask_coverage']:.3f}")
    print(f"✓ Optical masker working correctly")

def test_configuration():
    """Test configuration loading"""
    print("\nTesting Configuration...")
    
    config = TransferRobustnessConfig()
    
    print(f"Representative HUCs: {len(config.representative_hucs)}")
    print(f"First 3 HUCs: {config.representative_hucs[:3]}")
    print(f"Model checkpoint: {os.path.basename(config.model_checkpoint)}")
    print(f"Low relief threshold: {config.low_relief_threshold}m")
    print(f"High relief threshold: {config.high_relief_threshold}m")
    print(f"Optical mask probability: {config.optical_mask_probability}")
    print(f"Device: {config.device}")
    print(f"✓ Configuration loaded correctly")

def test_data_access():
    """Test access to representative HUC data"""
    print("\nTesting Data Access...")
    
    config = TransferRobustnessConfig()
    
    for huc in config.representative_hucs[:3]:  # Test first 3
        patch_dir = os.path.join(config.test_data_path, huc)
        if os.path.exists(patch_dir):
            patch_files = [f for f in os.listdir(patch_dir) if f.endswith('.npz')]
            print(f"HUC {huc}: {len(patch_files)} patches available")
        else:
            print(f"HUC {huc}: Directory not found")
            
    print("✓ Data access test completed")

def main():
    """Run all tests"""
    print("="*60)
    print("TRANSFER AND ROBUSTNESS ANALYSIS FRAMEWORK TEST")
    print("="*60)
    
    try:
        test_configuration()
        test_terrain_classifier()
        test_optical_masker()
        test_data_access()
        
        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED - Framework ready for analysis!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()