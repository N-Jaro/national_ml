#!/usr/bin/env python3
"""
Debug script to compare model outputs between original and ultra-fast evaluators
"""

import torch
import sys
sys.path.append('/u/nathanj/national_ml/experiments')
sys.path.append('/u/nathanj/national_ml/experiments/data')
sys.path.append('/u/nathanj/national_ml/experiments/models')

from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
from patchDataLoader_alphaearth_only import MultimodalPatchDataset_AlphaEarth_Only
from torch.utils.data import DataLoader

def load_model_direct(checkpoint_path: str, device: str = "cpu"):
    """Load model exactly like original evaluator"""
    model = MultitaskModel_AlphaEarth_Only(
        n_classes_task1=1,  # Water segmentation (binary)
        n_classes_task2=8,  # D8 flow direction (8 classes)
        base_channels=64,
        alphaearth_channels=64
    )
    
    checkpoint = torch.load(checkpoint_path, map_location=device)
    
    if 'state_dict' in checkpoint:
        state_dict = checkpoint['state_dict']
        
        # Remove 'model.' prefix from keys if present
        model_state_dict = {}
        for key, value in state_dict.items():
            if key.startswith('model.'):
                new_key = key[6:]  # Remove 'model.' prefix
                model_state_dict[new_key] = value
            else:
                model_state_dict[key] = value
        
        model.load_state_dict(model_state_dict, strict=False)
    
    model = model.to(device)
    model.eval()
    return model

def debug_single_batch():
    """Debug a single batch to compare outputs"""
    
    checkpoint_path = "/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251002_115437_run1/checkpoints/mdmt-alphaearth-only-epoch=33-val_loss=0.8076.ckpt"
    test_data_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    huc_id = "02080201"
    device = "cpu"
    
    # Load model
    print("Loading model...")
    model = load_model_direct(checkpoint_path, device)
    
    # Create dataset
    print(f"Creating dataset for HUC {huc_id}...")
    dataset = MultimodalPatchDataset_AlphaEarth_Only(
        base_path=test_data_path,
        huc_codes=[huc_id],
        alphaearth_channels=64
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    # Create dataloader
    dataloader = DataLoader(
        dataset,
        batch_size=4,
        shuffle=False,
        num_workers=0,
        pin_memory=False
    )
    
    # Process first batch
    print("\nProcessing first batch...")
    with torch.no_grad():
        for batch_idx, batch in enumerate(dataloader):
            if batch_idx >= 1:  # Only process first batch
                break
                
            alphaearth = batch['alphaearth'].to(device)
            water_mask = batch['hydro_mask'].to(device)
            flow_dir = batch['flow_dir'].to(device)
            
            print(f"Input shapes:")
            print(f"  AlphaEarth: {alphaearth.shape}")
            print(f"  Water mask: {water_mask.shape}")
            print(f"  Flow dir: {flow_dir.shape}")
            
            # Forward pass
            water_pred, d8_pred = model(alphaearth)
            
            print(f"\nModel output shapes:")
            print(f"  Water pred: {water_pred.shape}")
            print(f"  D8 pred: {d8_pred.shape}")
            
            print(f"\nWater prediction stats:")
            print(f"  Min: {water_pred.min().item():.6f}")
            print(f"  Max: {water_pred.max().item():.6f}")
            print(f"  Mean: {water_pred.mean().item():.6f}")
            
            print(f"\nD8 prediction stats:")
            print(f"  Min: {d8_pred.min().item():.6f}")
            print(f"  Max: {d8_pred.max().item():.6f}")  
            print(f"  Mean: {d8_pred.mean().item():.6f}")
            
            print(f"\nWater target stats:")
            print(f"  Min: {water_mask.min().item():.6f}")
            print(f"  Max: {water_mask.max().item():.6f}")
            print(f"  Mean: {water_mask.mean().item():.6f}")
            print(f"  Unique values: {torch.unique(water_mask)}")
            
            print(f"\nD8 target stats:")
            print(f"  Min: {flow_dir.min().item():.6f}")
            print(f"  Max: {flow_dir.max().item():.6f}")
            print(f"  Mean: {flow_dir.float().mean().item():.6f}")
            print(f"  Unique values: {torch.unique(flow_dir)}")
            
            # Test water segmentation processing
            water_pred_sigmoid = torch.sigmoid(water_pred)
            water_pred_binary = (water_pred_sigmoid > 0.5).float()
            
            print(f"\nWater segmentation processing:")
            print(f"  Sigmoid min/max: {water_pred_sigmoid.min().item():.6f}/{water_pred_sigmoid.max().item():.6f}")
            print(f"  Binary pred unique: {torch.unique(water_pred_binary)}")
            print(f"  Predicted water pixels: {water_pred_binary.sum().item()}")
            print(f"  Actual water pixels: {water_mask.sum().item()}")
            
            # Test D8 processing
            d8_pred_classes = torch.argmax(d8_pred, dim=1)
            print(f"\nD8 classification processing:")
            print(f"  Predicted classes unique: {torch.unique(d8_pred_classes)}")
            print(f"  D8 argmax shape: {d8_pred_classes.shape}")
            
            break

if __name__ == "__main__":
    debug_single_batch()