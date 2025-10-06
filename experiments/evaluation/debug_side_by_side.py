#!/usr/bin/env python3
"""
Side-by-side comparison of original and ultra-fast evaluators
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
from sklearn.metrics import precision_recall_fscore_support

def load_model_exactly_like_original(checkpoint_path: str, device: str = "cpu"):
    """Load model EXACTLY like the original simple evaluator"""
    model = MultitaskModel_AlphaEarth_Only(
        n_classes_task1=1,  # Water segmentation (binary)
        n_classes_task2=8,  # D8 flow direction (8 classes)
        base_channels=64,
        alphaearth_channels=64
    )
    
    # Load checkpoint exactly like original
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

def compute_metrics_exactly_like_original(water_pred: torch.Tensor, water_target: torch.Tensor,
                                         d8_pred: torch.Tensor, d8_target: torch.Tensor):
    """Compute metrics EXACTLY like the original evaluator"""
    
    # Water segmentation - EXACTLY like original
    if water_pred.dim() == 4 and water_pred.shape[1] == 1:
        water_pred = water_pred.squeeze(1)  # Remove channel dimension: [B,1,H,W] -> [B,H,W]
    
    water_pred_sigmoid = torch.sigmoid(water_pred)
    water_pred_binary = (water_pred_sigmoid > 0.5).float()
    
    # Convert to numpy for sklearn metrics
    water_pred_np = water_pred_binary.flatten().cpu().numpy().astype(int)
    water_target_np = water_target.flatten().cpu().numpy().astype(int)
    
    # Water metrics
    intersection = (water_pred_binary * water_target).sum()
    pred_sum = water_pred_binary.sum()
    target_sum = water_target.sum()
    dice = (2.0 * intersection / (pred_sum + target_sum + 1e-8)).item()
    
    intersection = (water_pred_binary * water_target).sum()
    union = ((water_pred_binary + water_target) > 0).float().sum()
    iou = (intersection / (union + 1e-8)).item()
    
    water_acc = (water_pred_binary == water_target).float().mean().item()
    
    # D8 classification - EXACTLY like original (with the bug)
    d8_pred_classes = torch.argmax(d8_pred, dim=1)  # Class indices 0-7
    
    # Use ORIGINAL mapping (with the 9-class bug)
    d8_class_to_value = {0: 1, 1: 2, 2: 4, 3: 8, 4: 16, 5: 32, 6: 64, 7: 128, 8: 255}  # Original has class 8->255 but model only outputs 0-7
    d8_pred_values = torch.zeros_like(d8_pred_classes)
    for class_idx, d8_value in d8_class_to_value.items():
        if class_idx < 8:  # Only use classes 0-7 since model outputs 8 classes
            d8_pred_values[d8_pred_classes == class_idx] = d8_value
    
    # D8 accuracy - EXACTLY like original (includes -2 values in denominator)
    d8_acc = (d8_pred_values == d8_target).float().mean().item()  # This includes -2 comparisons!
    
    return {
        'water_iou': iou,
        'water_dice': dice,
        'water_accuracy': water_acc,
        'd8_accuracy': d8_acc,
    }

def test_side_by_side():
    """Test side-by-side comparison"""
    
    checkpoint_path = "/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251002_115437_run1/checkpoints/mdmt-alphaearth-only-epoch=33-val_loss=0.8076.ckpt"
    test_data_path = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"
    huc_id = "02080201"
    device = "cpu"
    
    # Load model exactly like original
    print("Loading model exactly like original...")
    model = load_model_exactly_like_original(checkpoint_path, device)
    
    # Create dataset exactly like original
    print(f"Creating dataset for HUC {huc_id}...")
    dataset = MultimodalPatchDataset_AlphaEarth_Only(
        base_path=test_data_path,
        huc_codes=[huc_id],
        alphaearth_channels=64
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    # Create dataloader exactly like original
    dataloader = DataLoader(
        dataset,
        batch_size=4,  # EXACTLY like original
        shuffle=False,
        num_workers=0,  # Avoid multiprocessing issues
        pin_memory=False
    )
    
    # Process exactly like original
    water_predictions = []
    water_targets = []
    d8_predictions = []
    d8_targets = []
    
    batch_count = 0
    with torch.no_grad():
        for batch_idx, batch in enumerate(dataloader):
            # Process ALL batches to get true comparison
                
            # Move data to device - EXACTLY like original
            alphaearth = batch['alphaearth'].to(device)
            water_mask = batch['hydro_mask'].to(device)
            flow_dir = batch['flow_dir'].to(device)
            
            # Forward pass - EXACTLY like original
            water_pred, d8_pred = model(alphaearth)
            
            # Collect predictions and targets - EXACTLY like original
            water_predictions.append(water_pred.cpu())
            water_targets.append(water_mask.cpu())
            d8_predictions.append(d8_pred.cpu())
            d8_targets.append(flow_dir.cpu())
            
            batch_count += 1
            
            print(f"Batch {batch_idx}: Water pred {water_pred.shape}, D8 pred {d8_pred.shape}")
    
    if not water_predictions:
        print("No predictions collected!")
        return
    
    # Concatenate all predictions - EXACTLY like original
    water_pred_all = torch.cat(water_predictions, dim=0)
    water_target_all = torch.cat(water_targets, dim=0)
    d8_pred_all = torch.cat(d8_predictions, dim=0)
    d8_target_all = torch.cat(d8_targets, dim=0)
    
    print(f"\nFinal tensor shapes:")
    print(f"Water pred: {water_pred_all.shape}")
    print(f"Water target: {water_target_all.shape}")
    print(f"D8 pred: {d8_pred_all.shape}")
    print(f"D8 target: {d8_target_all.shape}")
    
    # Compute metrics exactly like original
    metrics = compute_metrics_exactly_like_original(water_pred_all, water_target_all, d8_pred_all, d8_target_all)
    
    print(f"\nMetrics (first {batch_count} batches):")
    print(f"Water IoU: {metrics['water_iou']:.3f}")
    print(f"Water Dice: {metrics['water_dice']:.3f}")
    print(f"Water Accuracy: {metrics['water_accuracy']:.3f}")
    print(f"D8 Accuracy: {metrics['d8_accuracy']:.3f}")
    
    # Check D8 target statistics
    print(f"\nD8 target analysis:")
    unique_d8 = torch.unique(d8_target_all)
    print(f"Unique D8 values: {unique_d8}")
    print(f"Count of -2 values: {(d8_target_all == -2).sum().item()}")
    print(f"Total D8 pixels: {d8_target_all.numel()}")
    print(f"Fraction of -2 pixels: {(d8_target_all == -2).float().mean().item():.3f}")

if __name__ == "__main__":
    test_side_by_side()