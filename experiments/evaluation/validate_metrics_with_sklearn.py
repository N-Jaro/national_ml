#!/usr/bin/env python3
"""
Validate our custom metrics against scikit-learn's well-established implementations.
This will help us identify if there are bugs in our metric calculations.
"""

import os
import sys
import torch
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, jaccard_score
from sklearn.metrics import confusion_matrix, classification_report
import logging

# Add the project root to Python path
sys.path.append('/u/nathanj/national_ml')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def compute_custom_metrics(hydro_pred, hydro_true, flow_pred, flow_true):
    """Our current custom metric implementation"""
    # Ensure predictions are squeezed properly
    if hydro_pred.dim() == 4:  # [B, 1, H, W]
        hydro_pred = hydro_pred.squeeze(1)  # [B, H, W]
    if flow_pred.dim() == 4:   # [B, 1, H, W] 
        flow_pred = flow_pred.squeeze(1)   # [B, H, W]
    
    # Convert to binary for hydro_mask
    hydro_pred_binary = (hydro_pred > 0.5).float()
    
    # Water IoU
    intersection = (hydro_pred_binary * hydro_true).sum()
    union = hydro_pred_binary.sum() + hydro_true.sum() - intersection
    water_iou = (intersection / (union + 1e-8)).item()
    
    # Water Dice 
    dice_num = 2 * intersection
    dice_den = hydro_pred_binary.sum() + hydro_true.sum()
    water_dice = (dice_num / (dice_den + 1e-8)).item()
    
    # D8 Accuracy
    d8_correct = (flow_pred == flow_true).float().sum()
    d8_total = flow_true.numel()
    d8_acc = (d8_correct / d8_total).item()
    
    return water_iou, water_dice, d8_acc


def compute_sklearn_metrics(hydro_pred, hydro_true, flow_pred, flow_true):
    """Scikit-learn metric implementation for comparison"""
    # Ensure predictions are squeezed properly
    if hydro_pred.dim() == 4:  # [B, 1, H, W]
        hydro_pred = hydro_pred.squeeze(1)  # [B, H, W]
    if flow_pred.dim() == 4:   # [B, 1, H, W] 
        flow_pred = flow_pred.squeeze(1)   # [B, H, W]
    
    # Convert to binary for hydro_mask
    hydro_pred_binary = (hydro_pred > 0.5).float()
    
    # Flatten for sklearn
    hydro_pred_flat = hydro_pred_binary.cpu().numpy().flatten()
    hydro_true_flat = hydro_true.cpu().numpy().flatten()
    flow_pred_flat = flow_pred.cpu().numpy().flatten()
    flow_true_flat = flow_true.cpu().numpy().flatten()
    
    # Water metrics using sklearn
    water_iou_sklearn = jaccard_score(hydro_true_flat, hydro_pred_flat, average='binary')
    water_f1_sklearn = f1_score(hydro_true_flat, hydro_pred_flat, average='binary')
    
    # D8 accuracy using sklearn
    d8_acc_sklearn = accuracy_score(flow_true_flat, flow_pred_flat)
    
    # Additional D8 metrics
    d8_f1_macro = f1_score(flow_true_flat, flow_pred_flat, average='macro')
    d8_f1_weighted = f1_score(flow_true_flat, flow_pred_flat, average='weighted')
    
    return water_iou_sklearn, water_f1_sklearn, d8_acc_sklearn, d8_f1_macro, d8_f1_weighted


def analyze_d8_distribution(flow_true, flow_pred):
    """Analyze the distribution of D8 flow direction classes"""
    flow_true_flat = flow_true.cpu().numpy().flatten()
    flow_pred_flat = flow_pred.cpu().numpy().flatten()
    
    unique_true, counts_true = np.unique(flow_true_flat, return_counts=True)
    unique_pred, counts_pred = np.unique(flow_pred_flat, return_counts=True)
    
    logger.info("=== D8 Flow Direction Analysis ===")
    logger.info("True labels distribution:")
    for val, count in zip(unique_true, counts_true):
        percentage = count / len(flow_true_flat) * 100
        logger.info(f"  Class {val}: {count} ({percentage:.2f}%)")
    
    logger.info("Predicted labels distribution:")
    for val, count in zip(unique_pred, counts_pred):
        percentage = count / len(flow_pred_flat) * 100
        logger.info(f"  Class {val}: {count} ({percentage:.2f}%)")
    
    # Confusion matrix for D8 (limit to most common classes to avoid huge matrix)
    cm = confusion_matrix(flow_true_flat, flow_pred_flat)
    logger.info(f"D8 Confusion Matrix shape: {cm.shape}")
    
    return unique_true, counts_true, unique_pred, counts_pred


def create_synthetic_data():
    """Create synthetic data to test our metric functions"""
    logger.info("Creating synthetic test data...")
    
    batch_size, height, width = 2, 64, 64
    
    # Create synthetic hydro predictions and targets
    hydro_pred = torch.rand(batch_size, 1, height, width)  # [0, 1] probabilities
    hydro_true = torch.randint(0, 2, (batch_size, height, width)).float()  # {0, 1}
    
    # Create synthetic D8 flow direction data
    # D8 classes: 1, 2, 4, 8, 16, 32, 64, 128, 255 (but models might predict any value)
    d8_classes = [1, 2, 4, 8, 16, 32, 64, 128, 255]
    flow_true = torch.tensor(np.random.choice(d8_classes, size=(batch_size, height, width)))
    # Model predictions might be continuous, but let's make them close to valid classes
    flow_pred = flow_true.clone().float()
    # Add some noise to make it more realistic
    noise_mask = torch.rand_like(flow_pred) < 0.3  # 30% of predictions are wrong
    flow_pred[noise_mask] = torch.tensor(np.random.choice(d8_classes, size=noise_mask.sum().item())).float()
    
    return hydro_pred, hydro_true, flow_pred, flow_true


def main():
    logger.info("Starting metric validation with scikit-learn...")
    
    # Test with synthetic data first
    logger.info("\n" + "="*60)
    logger.info("TESTING WITH SYNTHETIC DATA")
    logger.info("="*60)
    
    hydro_pred, hydro_true, flow_pred, flow_true = create_synthetic_data()
    
    logger.info(f"Data shapes:")
    logger.info(f"  hydro_pred: {hydro_pred.shape}, hydro_true: {hydro_true.shape}")
    logger.info(f"  flow_pred: {flow_pred.shape}, flow_true: {flow_true.shape}")
    
    # Compute our custom metrics
    water_iou_custom, water_dice_custom, d8_acc_custom = compute_custom_metrics(
        hydro_pred, hydro_true, flow_pred, flow_true
    )
    
    # Compute sklearn metrics
    water_iou_sklearn, water_f1_sklearn, d8_acc_sklearn, d8_f1_macro, d8_f1_weighted = compute_sklearn_metrics(
        hydro_pred, hydro_true, flow_pred, flow_true
    )
    
    # Compare results
    logger.info("\n=== METRIC COMPARISON ===")
    logger.info("Water Segmentation Metrics:")
    logger.info(f"  IoU - Custom: {water_iou_custom:.4f}, Sklearn: {water_iou_sklearn:.4f}")
    logger.info(f"  Dice/F1 - Custom: {water_dice_custom:.4f}, Sklearn: {water_f1_sklearn:.4f}")
    logger.info(f"  Dice vs F1 difference: {abs(water_dice_custom - water_f1_sklearn):.6f}")
    
    logger.info("D8 Flow Direction Metrics:")
    logger.info(f"  Accuracy - Custom: {d8_acc_custom:.4f}, Sklearn: {d8_acc_sklearn:.4f}")
    logger.info(f"  F1 Macro: {d8_f1_macro:.4f}")
    logger.info(f"  F1 Weighted: {d8_f1_weighted:.4f}")
    
    # Analyze D8 distribution
    analyze_d8_distribution(flow_true, flow_pred)
    
    # Test edge cases
    logger.info("\n" + "="*60)
    logger.info("TESTING EDGE CASES")
    logger.info("="*60)
    
    # Perfect predictions
    logger.info("\n--- Perfect Predictions ---")
    hydro_pred_perfect = hydro_true.unsqueeze(1).float()  # Add channel dimension
    flow_pred_perfect = flow_true.float()
    
    water_iou_perfect, water_dice_perfect, d8_acc_perfect = compute_custom_metrics(
        hydro_pred_perfect, hydro_true, flow_pred_perfect, flow_true
    )
    logger.info(f"Perfect predictions - IoU: {water_iou_perfect:.4f}, Dice: {water_dice_perfect:.4f}, D8 Acc: {d8_acc_perfect:.4f}")
    
    # All zeros predictions
    logger.info("\n--- All Zeros Predictions ---")
    hydro_pred_zeros = torch.zeros_like(hydro_pred)
    flow_pred_zeros = torch.zeros_like(flow_pred)
    
    water_iou_zeros, water_dice_zeros, d8_acc_zeros = compute_custom_metrics(
        hydro_pred_zeros, hydro_true, flow_pred_zeros, flow_true
    )
    logger.info(f"All zeros predictions - IoU: {water_iou_zeros:.4f}, Dice: {water_dice_zeros:.4f}, D8 Acc: {d8_acc_zeros:.4f}")
    
    # All ones predictions for hydro
    logger.info("\n--- All Ones Hydro Predictions ---")
    hydro_pred_ones = torch.ones_like(hydro_pred)
    
    water_iou_ones, water_dice_ones, _ = compute_custom_metrics(
        hydro_pred_ones, hydro_true, flow_pred, flow_true
    )
    logger.info(f"All ones hydro predictions - IoU: {water_iou_ones:.4f}, Dice: {water_dice_ones:.4f}")
    
    logger.info("\n" + "="*60)
    logger.info("VALIDATION COMPLETE")
    logger.info("="*60)
    logger.info("\nKey Insights:")
    logger.info("1. Compare Dice vs F1 - they should be identical for binary classification")
    logger.info("2. D8 accuracy is naturally low due to 9-class classification problem")
    logger.info("3. Check if our IoU calculation matches sklearn's Jaccard score")
    logger.info("4. Consider using F1 macro/weighted for D8 instead of accuracy")


if __name__ == "__main__":
    main()