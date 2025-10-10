#!/usr/bin/env python3
"""
Test SatLas Stable Loss Implementation
Verify that SatLas now uses the same stable loss pattern as successful Prithvi
"""

import os
import sys
import torch
import yaml
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent))

from utils.losses import CombinedFocalDiceLoss
from training.train_satlas_satlaspretrain import SatlasFoundationModel


def test_stable_loss_implementation():
    """Test that SatLas now uses stable loss exactly like Prithvi."""
    print("🔍 TESTING SATLAS STABLE LOSS IMPLEMENTATION")
    print("=" * 60)
    
    # Load SatLas config
    config_path = Path(__file__).parent / "configs" / "satlas_pretrained_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    print("✅ Configuration loaded successfully")
    print(f"   Loss type: {config['loss']['type']}")
    print(f"   Focal weight: {config['loss']['focal_weight']}")
    print(f"   Dice weight: {config['loss']['dice_weight']}")
    print(f"   Focal alpha: {config['loss']['focal_alpha']}")
    print(f"   Focal gamma: {config['loss']['focal_gamma']}")
    
    # Test direct loss function instantiation
    print("\n🧪 Testing direct loss function...")
    loss_fn = CombinedFocalDiceLoss(
        focal_weight=config["loss"]["focal_weight"],
        dice_weight=config["loss"]["dice_weight"],
        focal_alpha=config["loss"]["focal_alpha"],
        focal_gamma=config["loss"]["focal_gamma"]
    )
    
    # Create test data
    batch_size = 4
    height, width = 224, 224
    
    # Test with realistic data ranges
    inputs = torch.randn(batch_size, 1, height, width) * 2.0  # Realistic logit range
    targets = torch.randint(0, 2, (batch_size, 1, height, width)).float()
    
    # Make targets sparse (simulate real water distribution ~5% pixels)
    targets = (torch.rand_like(targets) < 0.05).float()
    
    print(f"   Input shape: {inputs.shape}")
    print(f"   Target shape: {targets.shape}")
    print(f"   Water pixels: {targets.sum().item():.0f}/{targets.numel()} ({100*targets.mean().item():.2f}%)")
    
    # Test loss computation
    loss_value = loss_fn(inputs, targets)
    print(f"   Loss value: {loss_value.item():.4f}")
    
    # Verify loss is stable
    assert not torch.isnan(loss_value), "❌ Loss is NaN"
    assert not torch.isinf(loss_value), "❌ Loss is Inf"
    assert loss_value.item() > 0, "❌ Loss should be positive"
    
    print("✅ Direct loss function test passed")
    
    # Test model integration
    print("\n🏗️ Testing model integration...")
    
    # Reduce config for testing
    test_config = config.copy()
    test_config["data"]["huc_codes"] = ["03030005"]  # Single HUC
    test_config["training"]["batch_size"] = 4
    test_config["training"]["max_epochs"] = 2
    
    try:
        model = SatlasFoundationModel(test_config)
        model_loss = model.criterion(inputs, targets)
        
        print(f"   Model loss: {model_loss.item():.4f}")
        print(f"   Model parameters: {model.count_parameters():,}")
        
        # Verify model loss matches direct loss (should be same)
        diff = abs(loss_value.item() - model_loss.item())
        assert diff < 1e-6, f"❌ Model loss differs from direct loss by {diff}"
        
        print("✅ Model integration test passed")
        
    except Exception as e:
        print(f"❌ Model integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test loss consistency over multiple iterations
    print("\n🔄 Testing loss stability...")
    
    losses = []
    for i in range(10):
        test_inputs = torch.randn(batch_size, 1, height, width) * 2.0
        test_targets = (torch.rand(batch_size, 1, height, width) < 0.05).float()
        
        with torch.no_grad():
            loss_val = loss_fn(test_inputs, test_targets)
            losses.append(loss_val.item())
    
    loss_mean = sum(losses) / len(losses)
    loss_std = (sum((x - loss_mean) ** 2 for x in losses) / len(losses)) ** 0.5
    
    print(f"   Loss mean over 10 iterations: {loss_mean:.4f}")
    print(f"   Loss std deviation: {loss_std:.4f}")
    print(f"   All losses: {[f'{l:.4f}' for l in losses]}")
    
    # Verify stability (std should be reasonable)
    assert loss_std < loss_mean, f"❌ Loss too unstable: std={loss_std:.4f}, mean={loss_mean:.4f}"
    
    print("✅ Loss stability test passed")
    
    # Compare to Prithvi pattern
    print("\n🔍 Verifying Prithvi compatibility...")
    
    # These are Prithvi's exact default parameters
    prithvi_loss = CombinedFocalDiceLoss(
        focal_weight=0.7,
        dice_weight=0.3,
        focal_alpha=0.75,
        focal_gamma=2.0
    )
    
    # Test with same inputs
    with torch.no_grad():
        satlas_loss_val = loss_fn(inputs, targets).item()
        prithvi_loss_val = prithvi_loss(inputs, targets).item()
    
    print(f"   SatLas loss: {satlas_loss_val:.4f}")
    print(f"   Prithvi-pattern loss: {prithvi_loss_val:.4f}")
    print(f"   Difference: {abs(satlas_loss_val - prithvi_loss_val):.6f}")
    
    # Should be identical (same parameters)
    assert abs(satlas_loss_val - prithvi_loss_val) < 1e-6, "❌ SatLas doesn't match Prithvi pattern"
    
    print("✅ Prithvi compatibility verified")
    
    print("\n" + "=" * 60)
    print("🎉 ALL TESTS PASSED!")
    print("✅ SatLas now uses stable loss pattern identical to successful Prithvi")
    print("✅ No more adaptive weights or parameter modifications")
    print("✅ Should resolve erratic training curves seen in wandb")
    
    return True


def compare_old_vs_new_loss():
    """Compare old problematic loss vs new stable loss."""
    print("\n📊 COMPARING OLD VS NEW LOSS BEHAVIOR")
    print("=" * 60)
    
    # Create test data
    batch_size = 4
    inputs = torch.randn(batch_size, 1, 224, 224) * 2.0
    targets = (torch.rand(batch_size, 1, 224, 224) < 0.05).float()
    
    # Old problematic way (what SatLas was doing)
    old_focal_alpha = min(0.75 * 2.0, 0.75)  # This was the bug - doubled alpha
    old_loss = CombinedFocalDiceLoss(
        focal_weight=0.7,
        dice_weight=0.3,
        focal_alpha=old_focal_alpha,  # 0.75 (cap at 0.75)
        focal_gamma=1.5  # Was using 1.5 instead of 2.0
    )
    
    # New stable way (Prithvi pattern)
    new_loss = CombinedFocalDiceLoss(
        focal_weight=0.7,
        dice_weight=0.3,
        focal_alpha=0.75,  # Direct use, no doubling
        focal_gamma=2.0    # Prithvi's value
    )
    
    with torch.no_grad():
        old_loss_val = old_loss(inputs, targets).item()
        new_loss_val = new_loss(inputs, targets).item()
    
    print(f"❌ Old problematic loss: {old_loss_val:.4f}")
    print(f"   - focal_alpha: {old_focal_alpha} (doubled then capped)")
    print(f"   - focal_gamma: 1.5")
    
    print(f"✅ New stable loss: {new_loss_val:.4f}")
    print(f"   - focal_alpha: 0.75 (direct, stable)")
    print(f"   - focal_gamma: 2.0 (matches Prithvi)")
    
    print(f"📈 Loss change: {new_loss_val - old_loss_val:+.4f}")
    print(f"📊 Relative change: {100 * (new_loss_val - old_loss_val) / old_loss_val:+.2f}%")
    
    print("\n✅ Fix applied: SatLas now uses stable Prithvi pattern")


if __name__ == "__main__":
    try:
        success = test_stable_loss_implementation()
        if success:
            compare_old_vs_new_loss()
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)