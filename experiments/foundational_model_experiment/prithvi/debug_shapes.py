#!/usr/bin/env python3
"""
Simple test to debug tensor shapes without TerraTorch imports
"""

import os
import sys
from pathlib import Path
import yaml
import torch
import numpy as np

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).parent.parent))

def simple_shape_test():
    print("🧪 Simple Tensor Shape Analysis")
    print("=" * 40)
    
    # Simulate the shapes we expect vs what might be happening
    print("Expected shapes:")
    print("  images: (B, 9, H, W) = (8, 9, 224, 224)")
    print("  masks: (B, H, W) = (8, 224, 224)")
    print("  model output: (B, 1, H, W) = (8, 1, 224, 224)")
    print("  pred_probs: (B, 1, H, W) = (8, 1, 224, 224)")
    print("  pred_masks: (B, 1, H, W) = (8, 1, 224, 224)")
    
    # Create sample tensors to test the visualization extraction
    print("\n🔬 Testing tensor extraction for visualization:")
    
    B, H, W = 8, 224, 224
    
    # Simulate model outputs
    masks = torch.randint(0, 2, (B, H, W)).float()  # Ground truth
    outputs = torch.randn(B, 1, H, W)  # Model output logits
    pred_probs = torch.sigmoid(outputs)  # (B, 1, H, W)
    pred_masks = (pred_probs >= 0.5).float()  # (B, 1, H, W)
    
    print(f"✅ Created test tensors:")
    print(f"   masks: {masks.shape}")
    print(f"   outputs: {outputs.shape}")
    print(f"   pred_probs: {pred_probs.shape}")
    print(f"   pred_masks: {pred_masks.shape}")
    
    # Test extraction for first sample (what happens in visualization)
    print(f"\n🎨 Testing sample extraction (i=0):")
    i = 0
    
    gt_mask = masks[i].float()  # (H, W)
    pred_mask = pred_masks[i, 0]  # (H, W) - extracting from (1, H, W)
    pred_prob = pred_probs[i, 0]  # (H, W) - extracting from (1, H, W)
    
    print(f"   gt_mask: {gt_mask.shape}")
    print(f"   pred_mask: {pred_mask.shape}")
    print(f"   pred_prob: {pred_prob.shape}")
    
    # Test normalization function
    def safe_normalize(x):
        x = x.detach().cpu().float()
        x_min, x_max = x.min(), x.max()
        if x_max > x_min:
            x = (x - x_min) / (x_max - x_min)
        x = torch.clamp(x, 0, 1).numpy().astype(np.float32)
        return x
    
    print(f"\n🔄 Testing normalization:")
    gt_norm = safe_normalize(gt_mask)
    pred_mask_norm = safe_normalize(pred_mask)
    pred_prob_norm = safe_normalize(pred_prob)
    
    print(f"   gt_norm: {gt_norm.shape}, dtype: {gt_norm.dtype}")
    print(f"   pred_mask_norm: {pred_mask_norm.shape}, dtype: {pred_mask_norm.dtype}")
    print(f"   pred_prob_norm: {pred_prob_norm.shape}, dtype: {pred_prob_norm.dtype}")
    
    # Test uint8 conversion
    def safe_to_uint8_rgb(img):
        if img.ndim == 2:  # Grayscale (H, W)
            img = np.stack([img, img, img], axis=-1)  # Convert to (H, W, 3)
        elif img.ndim == 3 and img.shape[-1] == 1:  # (H, W, 1) 
            img = np.repeat(img, 3, axis=-1)  # Convert to (H, W, 3)
        elif img.ndim == 3 and img.shape[-1] == 3:  # Already RGB (H, W, 3)
            pass  # Keep as is
        else:
            # Fallback: convert to grayscale and then RGB
            if img.ndim == 3:
                img = img.mean(axis=-1)  # Average channels to get grayscale
            img = np.stack([img, img, img], axis=-1)  # Convert to (H, W, 3)
        
        # Ensure values are in [0, 1] then convert to uint8
        img = np.clip(img, 0, 1)
        return (img * 255).astype(np.uint8)
    
    print(f"\n🎨 Testing uint8 RGB conversion:")
    gt_rgb = safe_to_uint8_rgb(gt_norm)
    pred_mask_rgb = safe_to_uint8_rgb(pred_mask_norm)
    pred_prob_rgb = safe_to_uint8_rgb(pred_prob_norm)
    
    print(f"   gt_rgb: {gt_rgb.shape}, dtype: {gt_rgb.dtype}")
    print(f"   pred_mask_rgb: {pred_mask_rgb.shape}, dtype: {pred_mask_rgb.dtype}")
    print(f"   pred_prob_rgb: {pred_prob_rgb.shape}, dtype: {pred_prob_rgb.dtype}")
    
    # Verify they can be concatenated
    print(f"\n🔗 Testing concatenation compatibility:")
    try:
        # Test horizontal concatenation (what we do in visualization)
        bottom_row = np.concatenate([gt_rgb, pred_mask_rgb, pred_prob_rgb, np.zeros_like(gt_rgb)], axis=1)
        print(f"✅ Horizontal concatenation successful: {bottom_row.shape}")
        
        # Create a dummy top row
        top_row = np.concatenate([gt_rgb, gt_rgb, gt_rgb, gt_rgb], axis=1)  # 4 panels
        
        # Test vertical concatenation
        panel = np.concatenate([top_row, bottom_row], axis=0)
        print(f"✅ Vertical concatenation successful: {panel.shape}")
        print(f"✅ Final panel ready for matplotlib/wandb: {panel.shape}")
        
    except Exception as e:
        print(f"❌ Concatenation failed: {e}")
        print("This explains the visualization issue!")
    
    print(f"\n🎯 Analysis Summary:")
    print("   If all tests pass, the issue is likely in:")
    print("   1. Model output shape being different than expected")
    print("   2. TerraTorch model returning wrong tensor dimensions")
    print("   3. Prithvi model output needing different processing")
    
    print(f"\n💡 Next steps:")
    print("   1. Check actual model output shape in logs")
    print("   2. Verify Prithvi model output format")
    print("   3. Fix tensor extraction if needed")

if __name__ == "__main__":
    simple_shape_test()