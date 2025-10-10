#!/usr/bin/env python3
"""
Example script to test the All Modalities + AlphaEarth MDMT model.

This script demonstrates the comprehensive model that combines:
- DEM (1 channel): Topographic information 
- Optical (6 channels): Landsat B2-B7 spectral data
- Thermal (1 channel): Landsat B10 thermal data  
- SAR (1 channel): Sentinel-1 VV radar data
- AlphaEarth (64 channels): Google's satellite embeddings

Total: 73 input channels for the most comprehensive analysis.

Usage:
    python example_all_modalities_alphaearth.py
    python example_all_modalities_alphaearth.py --demo_only
"""

import os
import sys
import argparse
import torch
import numpy as np

# Add experiments directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.mdmt_all_modalities_alphaearth import MultimodalMultitaskModel_All_Modalities_AlphaEarth
from data.patchDataLoader_all_modalities_alphaearth import MultimodalPatchDataset_All_Modalities_AlphaEarth, create_dataloader_all_modalities_alphaearth


def demo_model_architecture():
    """Demonstrate the model architecture with synthetic data."""
    print("=" * 80)
    print("ALL MODALITIES + ALPHAEARTH MDMT MODEL ARCHITECTURE DEMO")
    print("=" * 80)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    # Test with fixed 64-channel AlphaEarth configuration
    print(f"\n--- Testing All Modalities + AlphaEarth (64 channels) ---")
    
    model = MultimodalMultitaskModel_All_Modalities_AlphaEarth(
        n_classes_task1=1,       # Water segmentation (binary)
        n_classes_task2=8,       # D8 flow direction (8 classes)
        base_channels=64
    ).to(device)
    
    # Create synthetic input data
    batch_size = 2
    H, W = 224, 224
    
    dem = torch.randn(batch_size, 1, H, W).to(device)                    # DEM elevation
    optical = torch.randn(batch_size, 6, H, W).to(device)               # Landsat B2-B7
    thermal = torch.randn(batch_size, 1, H, W).to(device)               # Landsat B10
    sar = torch.randn(batch_size, 1, H, W).to(device)                   # Sentinel-1 VV
    alphaearth = torch.randn(batch_size, 64, H, W).to(device)           # AlphaEarth embeddings (fixed 64)
    
    total_channels = 73  # 1 + 6 + 1 + 1 + 64
    
    print(f"Input shapes:")
    print(f"  DEM: {dem.shape}")
    print(f"  Optical (Landsat B2-B7): {optical.shape}")
    print(f"  Thermal (Landsat B10): {thermal.shape}")
    print(f"  SAR (Sentinel-1 VV): {sar.shape}")
    print(f"  AlphaEarth: {alphaearth.shape}")
    print(f"  Total input channels: {total_channels}")
    
    try:
        with torch.no_grad():
            output_water, output_d8 = model(dem, optical, thermal, sar, alphaearth)
        
        print(f"Output shapes:")
        print(f"  Water segmentation: {output_water.shape}")
        print(f"  D8 flow direction: {output_d8.shape}")
        
        # Verify output shapes
        assert output_water.shape == (batch_size, 1, H, W)
        assert output_d8.shape == (batch_size, 8, H, W)
        
        # Model statistics
        total_params = sum(p.numel() for p in model.parameters())
        trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        model_size_mb = total_params * 4 / (1024 * 1024)
        
        print(f"Model statistics:")
        print(f"  Total parameters: {total_params:,}")
        print(f"  Trainable parameters: {trainable_params:,}")
        print(f"  Model size: {model_size_mb:.2f} MB")
        print(f"✅ Architecture test PASSED for 64-channel AlphaEarth")
        
    except Exception as e:
        print(f"❌ Architecture test FAILED for 64-channel AlphaEarth: {e}")
        return False
    
    print(f"\n🎉 Architecture test PASSED!")
    print(f"The comprehensive All Modalities + AlphaEarth model (64-channel) is ready for training!")
    return True


def test_data_loading():
    """Test data loading with real patch data (if available)."""
    print("\n" + "=" * 80)
    print("DATA LOADING TEST")
    print("=" * 80)
    
    base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
    test_hucs = ["10020007"]  # Use a known HUC for testing
    
    if not os.path.exists(base_path):
        print(f"❌ Base path does not exist: {base_path}")
        print("This is expected if patch data hasn't been processed yet.")
        return False
    
    try:
        # Test dataset creation
        dataset = MultimodalPatchDataset_All_Modalities_AlphaEarth(
            base_path=base_path,
            huc_codes=test_hucs,
            warn_missing_stats=False,
        )
        
        if len(dataset) == 0:
            print(f"❌ No valid patch files found for HUCs: {test_hucs}")
            print("This is expected if all-modalities patches haven't been processed yet.")
            return False
        
        print(f"✅ Dataset created successfully")
        print(f"  Dataset size: {len(dataset)} patches")
        print(f"  HUCs: {test_hucs}")
        
        # Test sample loading
        sample = dataset[0]
        print(f"  Sample keys: {list(sample.keys())}")
        
        # Check all required modalities are present
        required_keys = ['dem', 'optical', 'thermal', 'sar', 'alphaearth', 'hydro_mask', 'flow_dir']
        for key in required_keys:
            if key in sample:
                shape = sample[key].shape
                print(f"  {key}: {shape}")
            else:
                print(f"❌ Missing required key: {key}")
                return False
        
        # Test dataloader
        dataloader = create_dataloader_all_modalities_alphaearth(
            base_path=base_path,
            huc_codes=test_hucs,
            batch_size=2,
            shuffle=False,
            num_workers=0,  # Use 0 for testing to avoid multiprocessing issues
        )
        
        batch = next(iter(dataloader))
        print(f"✅ DataLoader test passed")
        print(f"  Batch size: {batch['dem'].shape[0]}")
        
        total_channels = (batch['dem'].shape[1] + batch['optical'].shape[1] + 
                         batch['thermal'].shape[1] + batch['sar'].shape[1] + 
                         batch['alphaearth'].shape[1])
        print(f"  Total input channels: {total_channels}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data loading test failed: {e}")
        return False


def test_end_to_end():
    """Test end-to-end model with real data (if available)."""
    print("\n" + "=" * 80)
    print("END-TO-END TEST")
    print("=" * 80)
    
    base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
    test_hucs = ["10020007"]
    
    if not os.path.exists(base_path):
        print("❌ Cannot run end-to-end test: patch data not available")
        return False
    
    try:
        # Create dataset and dataloader
        dataloader = create_dataloader_all_modalities_alphaearth(
            base_path=base_path,
            huc_codes=test_hucs,
            batch_size=2,
            shuffle=False,
            num_workers=0,
        )
        
        # Create model
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        model = MultimodalMultitaskModel_All_Modalities_AlphaEarth(
            n_classes_task1=1,
            n_classes_task2=8
        ).to(device)
        
        # Test forward pass with real data
        batch = next(iter(dataloader))
        
        # Move batch to device
        dem = batch['dem'].to(device)
        optical = batch['optical'].to(device)
        thermal = batch['thermal'].to(device)
        sar = batch['sar'].to(device)
        alphaearth = batch['alphaearth'].to(device)
        
        with torch.no_grad():
            output_water, output_d8 = model(dem, optical, thermal, sar, alphaearth)
        
        print(f"✅ End-to-end test PASSED")
        print(f"  Input batch size: {dem.shape[0]}")
        print(f"  Water output: {output_water.shape}")
        print(f"  D8 output: {output_d8.shape}")
        
        # Test predictions make sense
        water_probs = torch.sigmoid(output_water)
        d8_probs = torch.softmax(output_d8, dim=1)
        
        print(f"  Water prob range: [{water_probs.min():.3f}, {water_probs.max():.3f}]")
        print(f"  D8 max prob: {d8_probs.max():.3f}")
        
        return True
        
    except Exception as e:
        print(f"❌ End-to-end test failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Test All Modalities + AlphaEarth MDMT model")
    parser.add_argument("--demo_only", action="store_true",
                       help="Only run architecture demo with synthetic data")
    
    args = parser.parse_args()
    
    print("🚀 Testing All Modalities + AlphaEarth MDMT Model")
    print("This is the most comprehensive variant combining all satellite modalities!")
    print("")
    
    # Always run architecture demo
    arch_success = demo_model_architecture()
    
    if args.demo_only:
        print("\n✅ Demo completed successfully!" if arch_success else "\n❌ Demo failed!")
        return
    
    # Run data loading tests
    data_success = test_data_loading()
    e2e_success = False
    
    if data_success:
        e2e_success = test_end_to_end()
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Architecture demo: {'✅ PASSED' if arch_success else '❌ FAILED'}")
    print(f"Data loading: {'✅ PASSED' if data_success else '❌ FAILED'}")
    print(f"End-to-end: {'✅ PASSED' if e2e_success else '❌ FAILED'}")
    
    if arch_success and (args.demo_only or (data_success and e2e_success)):
        print("\n🎉 All tests PASSED! The All Modalities + AlphaEarth model is ready!")
        print("\nTo train the model:")
        print("cd /u/nathanj/national_ml/experiments/training")
        print("sbatch submit_train_all_modalities_alphaearth_single.sh")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")


if __name__ == "__main__":
    main()