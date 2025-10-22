#!/usr/bin/env python3
"""
Quick Test for Segmentation-Only Model Architecture
Validates the model, dataloader, and training setup before SLURM submission
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

def test_segmentation_only_architecture():
    """Test the segmentation-only model architecture"""
    print("🔬 Testing Segmentation-Only Architecture")
    print("=" * 60)
    
    try:
        sys.path.append(str(Path(__file__).parent.parent / 'models'))
        from segmentation_only_model_dem_alphaearth import SegmentationOnlyModel_DEM_AlphaEarth
        
        import torch
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        print(f"Device: {device}")
        
        # Create model
        model = SegmentationOnlyModel_DEM_AlphaEarth(
            n_classes=1,
            alphaearth_channels=64
        ).to(device)
        
        # Test data
        batch_size = 2
        dem = torch.randn(batch_size, 1, 224, 224).to(device)
        alphaearth = torch.randn(batch_size, 64, 224, 224).to(device)
        
        print(f"Input shapes:")
        print(f"  DEM: {dem.shape}")
        print(f"  AlphaEarth: {alphaearth.shape}")
        
        # Forward pass
        with torch.no_grad():
            water_seg = model(dem, alphaearth)
            
        print(f"Output shape: {water_seg.shape}")
        
        # Check output shape
        expected_shape = (batch_size, 1, 224, 224)
        assert water_seg.shape == expected_shape, f"Expected {expected_shape}, got {water_seg.shape}"
        
        # Count parameters
        total_params = sum(p.numel() for p in model.parameters())
        print(f"Total parameters: {total_params:,}")
        
        print("✅ Model architecture test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Model architecture test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_segmentation_only_dataloader():
    """Test the segmentation-only dataloader"""
    print("\n🔬 Testing Segmentation-Only DataLoader")
    print("=" * 60)
    
    try:
        sys.path.append(str(Path(__file__).parent.parent / 'data'))
        from segmentation_only_dataloader_dem_alphaearth import create_segmentation_only_dataloader
        
        # Test data path
        test_data_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
        test_hucs = ["10020007"]
        
        print(f"Test data path: {test_data_path}")
        print(f"Test HUCs: {test_hucs}")
        
        # Create dataloader
        dataloader = create_segmentation_only_dataloader(
            base_path=test_data_path,
            huc_codes=test_hucs,
            batch_size=2,
            shuffle=False,
            num_workers=0,  # No multiprocessing for test
            expected_alphaearth_channels=64,
            debug=True
        )
        
        print(f"Dataset size: {len(dataloader.dataset)}")
        
        # Test loading a batch
        batch = next(iter(dataloader))
        
        print(f"Batch shapes:")
        print(f"  DEM: {batch['dem'].shape}")
        print(f"  AlphaEarth: {batch['alphaearth'].shape}")
        print(f"  Hydro Mask: {batch['hydro_mask'].shape} (TARGET)")
        print(f"  Flow Dir: {batch['flow_dir'].shape} (ignored)")
        
        # Check data ranges
        print(f"Data ranges:")
        print(f"  DEM: [{batch['dem'].min():.3f}, {batch['dem'].max():.3f}]")
        print(f"  AlphaEarth: [{batch['alphaearth'].min():.3f}, {batch['alphaearth'].max():.3f}]")
        print(f"  Hydro Mask: [{batch['hydro_mask'].min():.3f}, {batch['hydro_mask'].max():.3f}]")
        
        print("✅ DataLoader test passed!")
        return True
        
    except Exception as e:
        print(f"❌ DataLoader test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_segmentation_only_lightning():
    """Test the Lightning module"""
    print("\n🔬 Testing Segmentation-Only Lightning Module")
    print("=" * 60)
    
    try:
        sys.path.append(str(Path(__file__).parent.parent / 'training'))
        from train_segmentation_only_dem_alphaearth_lightning import (
            MDMT_SegmentationOnly_DEM_AlphaEarth_LitModule,
            SegmentationOnlyDEMAlphaEarthDataModule
        )
        
        # Create Lightning module
        model = MDMT_SegmentationOnly_DEM_AlphaEarth_LitModule(
            alphaearth_channels=64,
            learning_rate=1e-4
        )
        
        print(f"Lightning module created:")
        print(f"  Loss type: combined_focal_dice")
        print(f"  Learning rate: 1e-4")
        print(f"  AlphaEarth channels: 64")
        
        # Test data module (minimal)
        data_module = SegmentationOnlyDEMAlphaEarthDataModule(
            train_data_path="/projects/bcrm/nathanj/data/processed/test/patch_dataset",
            train_huc_codes=["10020007"],
            val_huc_codes=["10020007"],
            batch_size=2,
            num_workers=0,
            alphaearth_channels=64
        )
        
        # Setup data
        data_module.setup("fit")
        
        print(f"Data module setup:")
        print(f"  Train samples: {len(data_module.train_dataset)}")
        print(f"  Val samples: {len(data_module.val_dataset)}")
        
        # Test training step
        train_loader = data_module.train_dataloader()
        batch = next(iter(train_loader))
        
        # Move to same device as model
        import torch
        device = next(model.parameters()).device
        batch = {k: v.to(device) if isinstance(v, torch.Tensor) else v for k, v in batch.items()}
        
        # Test forward pass
        loss = model.training_step(batch, 0)
        print(f"Training step loss: {loss.item():.4f}")
        
        # Test validation step
        val_loss = model.validation_step(batch, 0)
        print(f"Validation step loss: {val_loss.item():.4f}")
        
        print("✅ Lightning module test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Lightning module test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("🚀 Segmentation-Only Model Complete Test Suite")
    print("=" * 70)
    print("Purpose: Validate architecture before SLURM training submission")
    print("Focus: Single-task baseline for multitask benefit analysis")
    print("")
    
    tests = [
        ("Model Architecture", test_segmentation_only_architecture),
        ("DataLoader", test_segmentation_only_dataloader),
        ("Lightning Module", test_segmentation_only_lightning)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        success = test_func()
        results.append((test_name, success))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    all_passed = True
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name}: {status}")
        if not success:
            all_passed = False
    
    print("\n" + "="*70)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print("\n🚀 Ready for SLURM training:")
        print("   Development: sbatch submit_segmentation_only_training_dev.sh")
        print("   Production:  sbatch submit_segmentation_only_training.sh")
        print("\n🔬 After training completes:")
        print("   1. Update multitask_benefit_analysis.py with checkpoint path")
        print("   2. Run connectivity comparison analysis")
        print("   3. Prove multitask learning benefits!")
    else:
        print("❌ SOME TESTS FAILED!")
        print("🔧 Fix the issues above before submitting SLURM jobs")
    
    print("="*70)
    return 0 if all_passed else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)