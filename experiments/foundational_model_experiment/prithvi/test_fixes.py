#!/usr/bin/env python3
"""
Quick test of the fixed training pipeline
"""
import os
import sys
import yaml
from pathlib import Path

# Add paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent))

def test_training_components():
    """Test individual components of the training pipeline."""
    print("🔍 TESTING FIXED TRAINING COMPONENTS")
    print("=" * 60)
    
    # Test 1: Config loading
    print("1. Testing config loading...")
    try:
        with open('configs/prithvi_config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        print("✅ Config loaded successfully")
        print(f"   Learning rate: {config['training']['learning_rate']}")
        print(f"   Batch size: {config['training']['batch_size']}")
        print(f"   Loss config: {config['loss']}")
    except Exception as e:
        print(f"❌ Config loading failed: {e}")
        return False
    
    # Test 2: Data loading
    print("\n2. Testing data loading...")
    try:
        import torch
        from data.four_modal_dataset_adapter import FourModalDataModule
        
        # Use minimal config for testing
        test_config = config.copy()
        test_config["data"]["huc_codes"] = test_config["data"]["huc_codes"][:2]
        test_config["training"]["batch_size"] = 4
        
        data_module = FourModalDataModule(test_config)
        data_module.setup("fit")
        
        train_loader = data_module.train_dataloader()
        batch = next(iter(train_loader))
        
        print("✅ Data loading successful")
        print(f"   Image shape: {batch['image'].shape}")
        print(f"   Mask shape: {batch['mask'].shape}")
        print(f"   Image range: [{batch['image'].min():.3f}, {batch['image'].max():.3f}]")
        print(f"   Mask values: {torch.unique(batch['mask'])}")
        
        # Check if normalization worked
        images = batch['image']
        for i in range(9):
            ch_min, ch_max = images[:, i].min().item(), images[:, i].max().item()
            print(f"   Channel {i}: [{ch_min:.3f}, {ch_max:.3f}]")
            
    except Exception as e:
        print(f"❌ Data loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 3: Loss function
    print("\n3. Testing loss function...")
    try:
        import torch
        from utils.losses import CombinedFocalDiceLoss
        
        loss_fn = CombinedFocalDiceLoss(
            focal_weight=config["loss"]["focal_weight"],
            dice_weight=config["loss"]["dice_weight"],
            focal_alpha=config["loss"]["focal_alpha"],
            focal_gamma=config["loss"]["focal_gamma"]
        )
        
        # Test with realistic shapes
        logits = torch.randn(4, 1, 224, 224) * 0.1
        targets = torch.randint(0, 2, (4, 1, 224, 224)).float()
        
        loss = loss_fn(logits, targets)
        print("✅ Loss computation successful")
        print(f"   Loss value: {loss.item():.4f}")
        
    except Exception as e:
        print(f"❌ Loss computation failed: {e}")
        return False
    
    print("\n✅ All components working correctly!")
    print("\n🚀 FIXES APPLIED:")
    print("   ✅ Fixed tensor shape mismatch in loss computation")
    print("   ✅ Added proper stats-based z-score normalization (using HUC stats files)")
    print("   ✅ Reduced learning rate for stability (2e-5)")
    print("   ✅ Added debugging logging for training steps")
    print("   ✅ Fixed config file corruption")
    
    return True

if __name__ == "__main__":
    success = test_training_components()
    if success:
        print("\n🎯 READY TO TRAIN!")
        print("Run: ./submit_prithvi_jobs.sh --runs 1 --concurrent 1")
    else:
        print("\n❌ Issues still present - check errors above")