#!/usr/bin/env python3
"""
Test the fixed loss function
"""
import torch
import sys
from pathlib import Path

# Add paths
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.losses import CombinedFocalDiceLoss

def test_fixed_loss():
    """Test loss function with correct shapes."""
    print("🔍 TESTING FIXED LOSS FUNCTION")
    print("=" * 50)
    
    # Create loss function
    loss_fn = CombinedFocalDiceLoss(
        focal_weight=0.7,
        dice_weight=0.3,
        focal_alpha=0.75,
        focal_gamma=1.5
    )
    
    batch_size, height, width = 4, 224, 224
    
    # Test with correct shapes
    logits = torch.randn(batch_size, 1, height, width) * 0.1  # Model output shape
    targets = torch.randint(0, 2, (batch_size, 1, height, width)).float()  # Same shape as logits
    
    print(f"✅ Logits shape: {logits.shape}")
    print(f"✅ Targets shape: {targets.shape}")
    
    try:
        loss = loss_fn(logits, targets)
        water_ratio = targets.mean().item()
        print(f"✅ Loss computed successfully: {loss.item():.4f}")
        print(f"✅ Water ratio: {water_ratio:.4f}")
        
        if torch.isnan(loss) or torch.isinf(loss):
            print("❌ Loss is NaN/Inf!")
        else:
            print("✅ Loss is valid")
            
    except Exception as e:
        print(f"❌ Loss computation failed: {e}")
    
    # Test severe class imbalance scenario (realistic for water segmentation)
    print("\n🔍 Testing severe class imbalance scenario:")
    sparse_targets = torch.bernoulli(torch.full((batch_size, 1, height, width), 0.02)).float()  # 2% water
    
    try:
        loss = loss_fn(logits, sparse_targets)
        water_ratio = sparse_targets.mean().item()
        print(f"✅ Sparse loss: {loss.item():.4f}, water ratio: {water_ratio:.4f}")
    except Exception as e:
        print(f"❌ Sparse loss failed: {e}")

if __name__ == "__main__":
    test_fixed_loss()