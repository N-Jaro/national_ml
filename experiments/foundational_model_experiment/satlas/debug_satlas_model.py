#!/usr/bin/env python3
"""
Debug script for SATLAS model to identify training issues.
"""

import torch
import torch.nn.functional as F
import numpy as np
import sys
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))

# Import SATLAS model and data
from training.train_satlas_satlaspretrain import SatlasPretrainedModel
from data.four_modal_dataset_adapter import FourModalDataModule
import yaml

def debug_satlas_model():
    """Debug SATLAS model with actual data."""
    
    print("🔍 SATLAS Model Debug Analysis")
    print("=" * 50)
    
    # Load config
    config_path = Path(__file__).parent / "configs" / "satlas_pretrained_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Modify config for debugging
    config["data"]["huc_codes"] = ["03030005"]  # Single HUC
    config["training"]["batch_size"] = 2
    
    print(f"✅ Config loaded: {len(config['data']['huc_codes'])} HUCs")
    
    try:
        # Test data loading
        print("\n📊 Testing Data Loading...")
        data_module = FourModalDataModule(config)
        data_module.setup("fit")
        
        train_loader = data_module.train_dataloader()
        val_loader = data_module.val_dataloader()
        
        print(f"✅ Data loaded: {len(data_module.train_dataset)} train, {len(data_module.val_dataset)} val")
        
        # Get sample batches
        train_batch = next(iter(train_loader))
        val_batch = next(iter(val_loader))
        
        print(f"Train batch - Image: {train_batch['image'].shape}, Mask: {train_batch['mask'].shape}")
        print(f"Val batch - Image: {val_batch['image'].shape}, Mask: {val_batch['mask'].shape}")
        
        # Analyze data distributions
        train_image = train_batch['image']
        train_mask = train_batch['mask']
        val_image = val_batch['image']
        val_mask = val_batch['mask']
        
        print(f"\n📈 Data Analysis:")
        print(f"Train image - min: {train_image.min():.3f}, max: {train_image.max():.3f}, mean: {train_image.mean():.3f}")
        print(f"Train mask - min: {train_mask.min():.3f}, max: {train_mask.max():.3f}, mean: {train_mask.mean():.3f}")
        print(f"Val image - min: {val_image.min():.3f}, max: {val_image.max():.3f}, mean: {val_image.mean():.3f}")
        print(f"Val mask - min: {val_mask.min():.3f}, max: {val_mask.max():.3f}, mean: {val_mask.mean():.3f}")
        
        # Check if validation data is different from training data
        train_mean = train_image.mean().item()
        val_mean = val_image.mean().item()
        
        if abs(train_mean - val_mean) < 1e-6:
            print("⚠️  WARNING: Train and validation data appear very similar!")
        else:
            print("✅ Train and validation data have different distributions")
        
        print(f"\n🧠 Testing Model Creation...")
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Using device: {device}")
        
        # Create model
        model = SatlasPretrainedModel(config)
        model = model.to(device)
        
        print(f"✅ Model created with {model.count_parameters():,} parameters")
        
        # Test forward pass
        print(f"\n🔄 Testing Forward Pass...")
        
        # Move data to device
        train_image = train_image.to(device)
        train_mask = train_mask.to(device)
        val_image = val_image.to(device)
        val_mask = val_mask.to(device)
        
        model.eval()
        with torch.no_grad():
            # Test train data
            train_output = model(train_image)
            train_loss = model.criterion(train_output, train_mask)
            
            # Test val data  
            val_output = model(val_image)
            val_loss = model.criterion(val_output, val_mask)
            
            print(f"Train forward - Output: {train_output.shape}, Loss: {train_loss.item():.4f}")
            print(f"Val forward - Output: {val_output.shape}, Loss: {val_loss.item():.4f}")
            
            # Check if outputs are different
            if torch.allclose(train_output, val_output, atol=1e-4):
                print("⚠️  WARNING: Model outputs are nearly identical for train/val!")
            else:
                print("✅ Model produces different outputs for train/val data")
        
        # Test metrics computation
        print(f"\n📊 Testing Metrics...")
        
        # Compute predictions
        train_pred = torch.sigmoid(train_output) > 0.5
        val_pred = torch.sigmoid(val_output) > 0.5
        
        # Remove extra dimensions if present
        if train_pred.dim() == 4 and train_pred.size(1) == 1:
            train_pred = train_pred.squeeze(1)
        if val_pred.dim() == 4 and val_pred.size(1) == 1:
            val_pred = val_pred.squeeze(1)
        
        # Compute basic metrics manually
        def compute_metrics(pred, target):
            pred = pred.float()
            target = target.float()
            
            # Accuracy
            acc = (pred == target).float().mean()
            
            # IoU
            intersection = (pred * target).sum()
            union = pred.sum() + target.sum() - intersection
            iou = intersection / (union + 1e-6)
            
            # F1
            tp = (pred * target).sum()
            fp = (pred * (1 - target)).sum()
            fn = ((1 - pred) * target).sum()
            precision = tp / (tp + fp + 1e-6)
            recall = tp / (tp + fn + 1e-6)
            f1 = 2 * precision * recall / (precision + recall + 1e-6)
            
            return acc.item(), iou.item(), f1.item()
        
        train_acc, train_iou, train_f1 = compute_metrics(train_pred, train_mask)
        val_acc, val_iou, val_f1 = compute_metrics(val_pred, val_mask)
        
        print(f"Train metrics - Acc: {train_acc:.4f}, IoU: {train_iou:.4f}, F1: {train_f1:.4f}")
        print(f"Val metrics - Acc: {val_acc:.4f}, IoU: {val_iou:.4f}, F1: {val_f1:.4f}")
        
        # Check if metrics are suspiciously similar
        if abs(train_acc - val_acc) < 1e-4 and abs(train_iou - val_iou) < 1e-4:
            print("⚠️  WARNING: Train and val metrics are nearly identical!")
            print("This suggests the model is not learning or there's a data issue.")
        
        # Test multiple forward passes to check consistency
        print(f"\n🔄 Testing Forward Pass Consistency...")
        
        model.eval()
        outputs = []
        for i in range(3):
            with torch.no_grad():
                output = model(val_image)
                outputs.append(output)
        
        # Check if outputs are consistent (they should be in eval mode)
        consistent = all(torch.allclose(outputs[0], out, atol=1e-4) for out in outputs[1:])
        print(f"Forward pass consistency: {'✅ Consistent' if consistent else '⚠️  Inconsistent'}")
        
        # Test training mode vs eval mode
        print(f"\n🏋️ Testing Training vs Eval Mode...")
        
        model.train()
        with torch.no_grad():
            train_mode_output = model(val_image)
        
        model.eval()
        with torch.no_grad():
            eval_mode_output = model(val_image)
        
        mode_difference = torch.abs(train_mode_output - eval_mode_output).mean().item()
        print(f"Train vs Eval mode difference: {mode_difference:.6f}")
        
        if mode_difference < 1e-6:
            print("⚠️  WARNING: No difference between train and eval modes!")
            print("This might indicate no dropout/batchnorm layers are active.")
        
        print(f"\n✅ Debug Analysis Complete!")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_satlas_model()