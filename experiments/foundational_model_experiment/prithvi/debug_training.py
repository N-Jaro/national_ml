#!/usr/bin/env python3
"""
Debug script to identify training issues
"""
import os
import sys
import yaml
import torch
import numpy as np
from pathlib import Path

# Add paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent))

from data.four_modal_dataset_adapter import FourModalDataModule
from training.train_prithvi import PrithviFoundationModel
from utils.losses import CombinedFocalDiceLoss

def debug_data_loading():
    """Debug data loading to check for issues."""
    print("🔍 DEBUGGING DATA LOADING")
    print("=" * 50)
    
    # Load config
    with open('configs/prithvi_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Use minimal config for debugging
    config["data"]["huc_codes"] = config["data"]["huc_codes"][:2]  # Just 2 HUCs
    config["training"]["batch_size"] = 4
    
    # Setup data module
    data_module = FourModalDataModule(config)
    data_module.setup("fit")
    
    # Get train loader
    train_loader = data_module.train_dataloader()
    val_loader = data_module.val_dataloader()
    
    print(f"✅ Train batches: {len(train_loader)}")
    print(f"✅ Val batches: {len(val_loader)}")
    
    # Check first batch
    batch = next(iter(train_loader))
    images = batch['image']  # (B, 9, H, W)
    masks = batch['mask']    # (B, H, W)
    
    print(f"✅ Image shape: {images.shape}")
    print(f"✅ Mask shape: {masks.shape}")
    print(f"✅ Image dtype: {images.dtype}")
    print(f"✅ Mask dtype: {masks.dtype}")
    
    # Check value ranges
    print(f"🔍 Image value range: {images.min():.4f} to {images.max():.4f}")
    print(f"🔍 Mask value range: {masks.min():.4f} to {masks.max():.4f}")
    print(f"🔍 Mask unique values: {torch.unique(masks)}")
    
    # Check class distribution
    water_pixels = masks.sum().item()
    total_pixels = masks.numel()
    water_ratio = water_pixels / total_pixels
    print(f"🔍 Water pixel ratio: {water_ratio:.4f} ({water_pixels}/{total_pixels})")
    
    # Check if masks are binary
    mask_values = torch.unique(masks)
    is_binary = len(mask_values) <= 2 and 0 in mask_values and 1 in mask_values
    print(f"🔍 Masks are binary: {is_binary}")
    
    if not is_binary:
        print(f"❌ ISSUE: Masks are not binary! Values: {mask_values}")
    
    # Check for NaN/Inf
    has_nan_images = torch.isnan(images).any()
    has_inf_images = torch.isinf(images).any()
    has_nan_masks = torch.isnan(masks).any()
    has_inf_masks = torch.isinf(masks).any()
    
    print(f"🔍 Images have NaN: {has_nan_images}")
    print(f"🔍 Images have Inf: {has_inf_images}")
    print(f"🔍 Masks have NaN: {has_nan_masks}")
    print(f"🔍 Masks have Inf: {has_inf_masks}")
    
    if has_nan_images or has_inf_images or has_nan_masks or has_inf_masks:
        print("❌ ISSUE: Found NaN/Inf values in data!")
    
    return images, masks

def debug_model_forward():
    """Debug model forward pass."""
    print("\n🔍 DEBUGGING MODEL FORWARD PASS")
    print("=" * 50)
    
    # Load config
    with open('configs/prithvi_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Create model
    model = PrithviFoundationModel(config)
    model.eval()
    
    # Create dummy input
    batch_size, channels, height, width = 2, 9, 224, 224
    dummy_input = torch.randn(batch_size, channels, height, width)
    dummy_mask = torch.randint(0, 2, (batch_size, height, width)).float()
    
    print(f"✅ Model created with {model.count_parameters():,} parameters")
    print(f"✅ Dummy input shape: {dummy_input.shape}")
    print(f"✅ Dummy mask shape: {dummy_mask.shape}")
    
    # Forward pass
    try:
        with torch.no_grad():
            output = model.forward(dummy_input)
            print(f"✅ Forward pass successful")
            print(f"✅ Output shape: {output.shape}")
            print(f"✅ Output range: {output.min():.4f} to {output.max():.4f}")
            
            # Check for NaN/Inf in output
            has_nan = torch.isnan(output).any()
            has_inf = torch.isinf(output).any()
            print(f"🔍 Output has NaN: {has_nan}")
            print(f"🔍 Output has Inf: {has_inf}")
            
            if has_nan or has_inf:
                print("❌ ISSUE: Model output contains NaN/Inf!")
                
    except Exception as e:
        print(f"❌ ISSUE: Forward pass failed: {e}")
        return None, None
    
    return output, dummy_mask

def debug_loss_computation():
    """Debug loss computation."""
    print("\n🔍 DEBUGGING LOSS COMPUTATION")
    print("=" * 50)
    
    # Load config
    with open('configs/prithvi_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Create loss function
    criterion = CombinedFocalDiceLoss(
        focal_weight=config["loss"]["focal_weight"],
        dice_weight=config["loss"]["dice_weight"],
        focal_alpha=config["loss"]["focal_alpha"],
        focal_gamma=config["loss"]["focal_gamma"]
    )
    
    print(f"✅ Loss function created")
    print(f"   Focal weight: {config['loss']['focal_weight']}")
    print(f"   Dice weight: {config['loss']['dice_weight']}")
    print(f"   Focal alpha: {config['loss']['focal_alpha']}")
    print(f"   Focal gamma: {config['loss']['focal_gamma']}")
    
    # Test with realistic data
    batch_size, height, width = 4, 224, 224
    
    # Simulate model output (logits)
    logits = torch.randn(batch_size, 1, height, width) * 0.1  # Small values
    
    # Simulate different mask scenarios
    scenarios = {
        "balanced": torch.randint(0, 2, (batch_size, height, width)).float(),
        "mostly_background": torch.zeros(batch_size, height, width).float(),
        "mostly_water": torch.ones(batch_size, height, width).float(),
        "realistic": torch.bernoulli(torch.full((batch_size, height, width), 0.05)).float()  # 5% water
    }
    
    for name, masks in scenarios.items():
        try:
            loss = criterion(logits, masks)
            water_ratio = masks.mean().item()
            print(f"✅ {name:15} - Loss: {loss.item():.4f}, Water ratio: {water_ratio:.4f}")
            
            # Check for invalid loss
            if torch.isnan(loss) or torch.isinf(loss):
                print(f"❌ ISSUE: Invalid loss for {name} scenario!")
                
        except Exception as e:
            print(f"❌ ISSUE: Loss computation failed for {name}: {e}")

def debug_full_training_step():
    """Debug a full training step."""
    print("\n🔍 DEBUGGING FULL TRAINING STEP")
    print("=" * 50)
    
    # Get real data
    images, masks = debug_data_loading()
    if images is None or masks is None:
        print("❌ Cannot proceed - data loading failed")
        return
    
    # Load config and create model
    with open('configs/prithvi_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    model = PrithviFoundationModel(config)
    model.train()
    
    # Simulate training step
    try:
        # Forward pass
        model_output = model.forward(images)
        outputs = model_output.output if hasattr(model_output, 'output') else model_output
        
        print(f"✅ Forward pass: {outputs.shape}")
        print(f"✅ Output range: {outputs.min():.4f} to {outputs.max():.4f}")
        
        # Loss computation
        loss = model.criterion(outputs, masks)
        print(f"✅ Loss computation: {loss.item():.4f}")
        
        # Backward pass
        loss.backward()
        print(f"✅ Backward pass successful")
        
        # Check gradients
        total_norm = 0
        param_count = 0
        for p in model.parameters():
            if p.grad is not None:
                param_norm = p.grad.data.norm(2)
                total_norm += param_norm.item() ** 2
                param_count += 1
        total_norm = total_norm ** (1. / 2)
        print(f"✅ Gradient norm: {total_norm:.4f} (from {param_count} parameters)")
        
        if total_norm == 0:
            print("❌ ISSUE: Zero gradients - model not learning!")
        elif total_norm > 100:
            print("❌ ISSUE: Very large gradients - potential instability!")
            
    except Exception as e:
        print(f"❌ ISSUE: Training step failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🐛 PRITHVI TRAINING DEBUG")
    print("=" * 60)
    
    # Run all debug tests
    debug_data_loading()
    debug_model_forward()
    debug_loss_computation()
    debug_full_training_step()
    
    print("\n✅ Debug complete!")