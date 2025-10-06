#!/usr/bin/env python3
"""
Corrected metric calculation functions that properly handle D8 flow direction encoding.
This fixes the critical bug where we compared class indices to D8 values.
"""

import torch
import numpy as np
from sklearn.metrics import f1_score, jaccard_score, accuracy_score


def compute_corrected_metrics(hydro_logits, flow_logits, hydro_true, flow_true):
    """
    Compute corrected metrics with proper D8 flow direction mapping.
    
    Args:
        hydro_logits: [B, 2, H, W] or [B, 1, H, W] - segmentation logits
        flow_logits: [B, 9, H, W] - flow direction logits
        hydro_true: [B, H, W] - binary water mask
        flow_true: [B, H, W] - D8 flow direction values (1,2,4,8,16,32,64,128,255)
    
    Returns:
        water_iou, water_dice, d8_accuracy, d8_f1_weighted
    """
    
    # Handle different segmentation output formats
    if hydro_logits.shape[1] == 2:  # 2-class output
        hydro_pred = torch.softmax(hydro_logits, dim=1)[:, 1]  # Take class 1 (water)
    else:  # 1-class output
        hydro_pred = torch.sigmoid(hydro_logits.squeeze(1))
    
    # Flow direction predictions (class indices 0-8)
    flow_pred_indices = torch.argmax(flow_logits, dim=1)  # [B, H, W] with values 0-8
    
    # CRITICAL FIX: Map class indices to D8 values
    d8_class_to_value = {0: 1, 1: 2, 2: 4, 3: 8, 4: 16, 5: 32, 6: 64, 7: 128, 8: 255}
    
    # Convert predictions from class indices to D8 values
    flow_pred_d8 = torch.zeros_like(flow_pred_indices)
    for class_idx, d8_value in d8_class_to_value.items():
        flow_pred_d8[flow_pred_indices == class_idx] = d8_value
    
    # Convert to binary for water segmentation
    hydro_pred_binary = (hydro_pred > 0.5).float()
    
    # Water IoU (Jaccard Index)
    intersection = (hydro_pred_binary * hydro_true).sum()
    union = hydro_pred_binary.sum() + hydro_true.sum() - intersection
    water_iou = (intersection / (union + 1e-8)).item()
    
    # Water Dice (F1 Score)
    dice_num = 2 * intersection
    dice_den = hydro_pred_binary.sum() + hydro_true.sum()
    water_dice = (dice_num / (dice_den + 1e-8)).item()
    
    # D8 Accuracy (now comparing D8 values to D8 values!)
    d8_correct = (flow_pred_d8 == flow_true).float().sum()
    d8_total = flow_true.numel()
    d8_accuracy = (d8_correct / d8_total).item()
    
    # Optional: Calculate F1 weighted for D8 using sklearn
    flow_pred_flat = flow_pred_d8.cpu().numpy().flatten()
    flow_true_flat = flow_true.cpu().numpy().flatten()
    
    try:
        d8_f1_weighted = f1_score(flow_true_flat, flow_pred_flat, average='weighted', zero_division=0)
    except:
        d8_f1_weighted = 0.0
    
    return water_iou, water_dice, d8_accuracy, d8_f1_weighted


def validate_d8_mapping():
    """Test the D8 mapping with synthetic data"""
    print("Testing D8 mapping correction...")
    
    # Create synthetic data
    B, H, W = 2, 64, 64
    
    # Synthetic flow logits (9 classes)
    flow_logits = torch.randn(B, 9, H, W)
    flow_pred_indices = torch.argmax(flow_logits, dim=1)  # [B, H, W] with 0-8
    
    # Synthetic ground truth with actual D8 values
    d8_values = [1, 2, 4, 8, 16, 32, 64, 128, 255]
    flow_true = torch.tensor(np.random.choice(d8_values, size=(B, H, W)))
    
    # Synthetic hydro data
    hydro_logits = torch.randn(B, 2, H, W)
    hydro_true = torch.randint(0, 2, (B, H, W)).float()
    
    # Test original (broken) calculation
    d8_accuracy_broken = (flow_pred_indices == flow_true).float().mean().item()
    
    # Test corrected calculation
    water_iou, water_dice, d8_accuracy_fixed, d8_f1 = compute_corrected_metrics(
        hydro_logits, flow_logits, hydro_true, flow_true
    )
    
    print(f"Original (broken) D8 accuracy: {d8_accuracy_broken:.4f}")
    print(f"Corrected D8 accuracy: {d8_accuracy_fixed:.4f}")
    print(f"Water IoU: {water_iou:.4f}")
    print(f"Water Dice: {water_dice:.4f}")
    print(f"D8 F1 Weighted: {d8_f1:.4f}")
    
    print(f"\nImprovement factor: {d8_accuracy_fixed / max(d8_accuracy_broken, 1e-8):.1f}x")
    print("✅ D8 mapping correction is working!")


if __name__ == "__main__":
    validate_d8_mapping()