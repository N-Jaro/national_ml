#!/usr/bin/env python3
"""
Debug the ultra-fast evaluator to see why water metrics are 0
"""

import torch
import sys
sys.path.append('/u/nathanj/national_ml/experiments')
sys.path.append('/u/nathanj/national_ml/experiments/data')
sys.path.append('/u/nathanj/national_ml/experiments/models')

from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
from patchDataLoader_alphaearth_only import MultimodalPatchDataset_AlphaEarth_Only
from torch.utils.data import DataLoader
import numpy as np

def load_model_like_ultra_fast(checkpoint_path: str, device: str = "cpu"):
    """Load model like ultra-fast evaluator with CORRECTED loading"""
    model = MultitaskModel_AlphaEarth_Only(
        n_classes_task1=1,  # Water segmentation (binary)
        n_classes_task2=8,  # D8 flow direction (8 classes)
        base_channels=64,
        alphaearth_channels=64
    )
    
    # Load checkpoint EXACTLY like original and debug script
    checkpoint = torch.load(checkpoint_path, map_location=device)
    
    # Extract model state dict exactly like original
    if 'state_dict' in checkpoint:
        state_dict = checkpoint['state_dict']
        
        # Remove 'model.' prefix from keys if present - EXACTLY like original
        model_state_dict = {}
        for key, value in state_dict.items():
            if key.startswith('model.'):
                new_key = key[6:]  # Remove 'model.' prefix
                model_state_dict[new_key] = value
            else:
                model_state_dict[key] = value
        
        # Load only matching keys - EXACTLY like original
        model_keys = set(model.state_dict().keys())
        checkpoint_keys = set(model_state_dict.keys())
        
        matching_keys = model_keys.intersection(checkpoint_keys)
        missing_keys = model_keys - checkpoint_keys
        unexpected_keys = checkpoint_keys - model_keys
        
        print(f"Model loading: {len(matching_keys)} matching, {len(missing_keys)} missing, {len(unexpected_keys)} unexpected")
        
        # Create filtered state dict with only matching keys
        filtered_state_dict = {k: model_state_dict[k] for k in matching_keys}
        
        # Load the filtered state dict
        model.load_state_dict(filtered_state_dict, strict=False)
    else:
        model.load_state_dict(checkpoint, strict=False)
    
    model = model.to(device)
    model.eval()
    return model

def debug_ultra_fast():
    """Debug ultra-fast evaluator water predictions"""
    
    checkpoint_path = "/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251002_115437_run1/checkpoints/mdmt-alphaearth-only-epoch=33-val_loss=0.8076.ckpt"
    test_data_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    huc_id = "02080201"
    device = "cpu"
    
    # Load model like ultra-fast evaluator
    print("Loading model like ultra-fast evaluator...")
    model = load_model_like_ultra_fast(checkpoint_path, device)
    
    # Create dataset
    print(f"Creating dataset for HUC {huc_id}...")
    dataset = MultimodalPatchDataset_AlphaEarth_Only(
        base_path=test_data_path,
        huc_codes=[huc_id],
        alphaearth_channels=64
    )
    
    dataloader = DataLoader(
        dataset,
        batch_size=4,
        shuffle=False,
        num_workers=0,
        pin_memory=False
    )
    
    # Process just first batch for debugging
    water_predictions = []
    water_targets = []
    
    with torch.no_grad():
        for batch_idx, batch in enumerate(dataloader):
            if batch_idx >= 1:  # Only first batch
                break
                
            alphaearth = batch['alphaearth'].to(device)
            water_mask = batch['hydro_mask'].to(device)
            flow_dir = batch['flow_dir'].to(device)
            
            print(f"\nBatch {batch_idx}:")
            print(f"AlphaEarth shape: {alphaearth.shape}")
            print(f"Water mask shape: {water_mask.shape}")
            print(f"Flow dir shape: {flow_dir.shape}")
            
            # Forward pass
            water_pred, d8_pred = model(alphaearth)
            
            print(f"Water pred shape: {water_pred.shape}")
            print(f"D8 pred shape: {d8_pred.shape}")
            
            # Check water prediction values
            print(f"\nWater prediction stats:")
            print(f"  Raw logits - min: {water_pred.min().item():.6f}, max: {water_pred.max().item():.6f}, mean: {water_pred.mean().item():.6f}")
            
            # Apply sigmoid like in compute_metrics  
            if water_pred.dim() == 4 and water_pred.shape[1] == 1:
                water_pred_squeezed = water_pred.squeeze(1)
            else:
                water_pred_squeezed = water_pred
                
            print(f"After squeeze shape: {water_pred_squeezed.shape}")
            
            water_pred_sigmoid = torch.sigmoid(water_pred_squeezed)
            water_pred_binary = (water_pred_sigmoid > 0.5).float()
            
            print(f"  Sigmoid - min: {water_pred_sigmoid.min().item():.6f}, max: {water_pred_sigmoid.max().item():.6f}, mean: {water_pred_sigmoid.mean().item():.6f}")
            print(f"  Binary predictions - unique values: {torch.unique(water_pred_binary)}")
            print(f"  Predicted water pixels: {water_pred_binary.sum().item()}")
            
            # Check water targets
            print(f"\nWater target stats:")
            print(f"  Shape: {water_mask.shape}")
            print(f"  Unique values: {torch.unique(water_mask)}")
            print(f"  Actual water pixels: {water_mask.sum().item()}")
            
            # Check if there's any overlap
            intersection = (water_pred_binary * water_mask).sum()
            print(f"  Intersection (TP): {intersection.item()}")
            
            # Check sklearn inputs
            water_pred_np = water_pred_binary.flatten().cpu().numpy().astype(int)
            water_target_np = water_mask.flatten().cpu().numpy().astype(int)
            
            print(f"\nNumpy arrays for sklearn:")
            print(f"  Water pred unique: {np.unique(water_pred_np)}")
            print(f"  Water target unique: {np.unique(water_target_np)}")
            print(f"  Arrays equal: {np.array_equal(water_pred_np, water_target_np)}")
            
            # Compute metrics manually
            from sklearn.metrics import precision_score, recall_score, f1_score
            try:
                precision = precision_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
                recall = recall_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
                f1 = f1_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
                
                print(f"\nSklearn metrics:")
                print(f"  Precision: {precision:.6f}")
                print(f"  Recall: {recall:.6f}")
                print(f"  F1: {f1:.6f}")
                
            except Exception as e:
                print(f"Error computing sklearn metrics: {e}")
            
            break

if __name__ == "__main__":
    debug_ultra_fast()