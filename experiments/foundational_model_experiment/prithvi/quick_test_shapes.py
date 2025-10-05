#!/usr/bin/env python3
"""
Quick test for Prithvi visualization debugging with 1 HUC only
"""

import os
import sys
import argparse
from pathlib import Path
import yaml
import logging

import torch
import pytorch_lightning as pl

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).parent.parent))

from data.four_modal_dataset_adapter import FourModalDataModule
from training.train_prithvi import PrithviFoundationModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def quick_test():
    print("🧪 Quick Prithvi Visualization Test (1 HUC only)")
    print("=" * 50)
    
    # Load test config
    with open("configs/prithvi_test_config.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    print(f"✅ Using test config with HUC: {config['data']['huc_codes']}")
    
    # Set up data module
    logger.info("Setting up data module...")
    data_module = FourModalDataModule(config)
    data_module.setup("fit")
    
    print(f"✅ Data loaded:")
    print(f"   Train patches: {len(data_module.train_dataset)}")
    print(f"   Val patches: {len(data_module.val_dataset)}")
    
    # Set up model
    logger.info("Setting up model...")
    model = PrithviFoundationModel(config)
    
    print(f"✅ Model created with {model.count_parameters():,} parameters")
    
    # Get a single batch for testing
    val_loader = data_module.val_dataloader()
    batch = next(iter(val_loader))
    
    print(f"✅ Got test batch:")
    print(f"   Images shape: {batch['image'].shape}")
    print(f"   Masks shape: {batch['mask'].shape}")
    print(f"   Images range: [{batch['image'].min():.3f}, {batch['image'].max():.3f}]")
    
    # Test forward pass
    print("\n🔬 Testing model forward pass...")
    model.eval()
    with torch.no_grad():
        try:
            images = batch['image']
            model_output = model.forward(images)
            outputs = model_output.output if hasattr(model_output, 'output') else model_output
            
            print(f"✅ Model forward pass successful:")
            print(f"   Input shape: {images.shape}")
            print(f"   Output shape: {outputs.shape}")
            print(f"   Output range: [{outputs.min():.3f}, {outputs.max():.3f}]")
            
            # Test prediction processing
            pred_probs = torch.sigmoid(outputs)
            pred_masks = (pred_probs >= 0.5).float()
            
            print(f"✅ Prediction processing:")
            print(f"   Pred probs shape: {pred_probs.shape}")
            print(f"   Pred masks shape: {pred_masks.shape}")
            print(f"   Pred probs range: [{pred_probs.min():.3f}, {pred_probs.max():.3f}]")
            
            # Test individual sample extraction
            print(f"\n🎨 Testing sample extraction for visualization:")
            i = 0  # First sample
            gt_mask = batch['mask'][i].float()
            pred_mask_sample = pred_masks[i, 0] if pred_masks.dim() == 4 else pred_masks[i]
            pred_prob_sample = pred_probs[i, 0] if pred_probs.dim() == 4 else pred_probs[i]
            
            print(f"   GT mask shape: {gt_mask.shape}")
            print(f"   Pred mask sample shape: {pred_mask_sample.shape}")
            print(f"   Pred prob sample shape: {pred_prob_sample.shape}")
            
            if gt_mask.shape == pred_mask_sample.shape == pred_prob_sample.shape == (224, 224):
                print("✅ All shapes match! Visualization should work correctly.")
            else:
                print("❌ Shape mismatch detected! This explains the visualization issue.")
                
        except Exception as e:
            print(f"❌ Model forward pass failed: {e}")
            import traceback
            print(traceback.format_exc())
    
    print(f"\n🎯 Summary:")
    print(f"   - Data loading: WORKING")
    print(f"   - Model creation: WORKING") 
    print(f"   - Forward pass: {'WORKING' if 'outputs' in locals() else 'FAILED'}")
    print(f"   - Shape compatibility: {'GOOD' if 'gt_mask' in locals() and gt_mask.shape == (224, 224) else 'NEEDS_FIX'}")

if __name__ == "__main__":
    quick_test()