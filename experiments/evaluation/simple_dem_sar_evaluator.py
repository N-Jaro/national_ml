#!/usr/bin/env python3
"""
Simple DEM + SAR Model Evaluator
Direct checkpoint loading without Lightning automatic loading
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any
import json
import pandas as pd
from datetime import datetime

import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader
import numpy as np
from tqdm import tqdm
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix, classification_report

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import model and data loader
from models.mdmt_dem_sar import MultimodalMultitaskModel_DEM_SAR
from data.patchDataLoader_dem_sar import MultimodalPatchDataset_DEM_SAR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_huc_list(file_path: str) -> List[str]:
    """Load HUC codes from file"""
    huc_list = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    huc_list.append(line)
        logger.info(f"Loaded {len(huc_list)} HUC codes from {file_path}")
        return huc_list
    except FileNotFoundError:
        logger.warning(f"HUC list file not found: {file_path}")
        return []

def load_model_direct(checkpoint_path: str, device: str = "cpu") -> torch.nn.Module:
    """Load model directly from checkpoint without Lightning wrapper"""
    try:
        # Create model instance
        model = MultimodalMultitaskModel_DEM_SAR(
            n_classes_task1=1,  # Water segmentation (binary)
            n_classes_task2=8,  # D8 flow direction (8 classes)
            base_channels=64
        )
        
        # Load checkpoint
        checkpoint = torch.load(checkpoint_path, map_location=device)
        
        # Extract model state dict
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
            
            # Load only matching keys
            model_keys = set(model.state_dict().keys())
            checkpoint_keys = set(model_state_dict.keys())
            
            matching_keys = model_keys.intersection(checkpoint_keys)
            missing_keys = model_keys - checkpoint_keys
            unexpected_keys = checkpoint_keys - model_keys
            
            logger.info(f"Loading {len(matching_keys)} matching parameters")
            if missing_keys:
                logger.warning(f"Missing keys: {len(missing_keys)} (model has keys not in checkpoint)")
            if unexpected_keys:
                logger.warning(f"Unexpected keys: {len(unexpected_keys)} (checkpoint has keys not in model)")
            
            # Create filtered state dict with only matching keys
            filtered_state_dict = {k: model_state_dict[k] for k in matching_keys}
            
            # Load the filtered state dict
            model.load_state_dict(filtered_state_dict, strict=False)
        
        model.eval()
        model.to(device)
        
        logger.info(f"Successfully loaded DEM + SAR model from {checkpoint_path}")
        return model
        
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        import traceback
        traceback.print_exc()
        return None

def compute_metrics(water_pred: torch.Tensor, water_target: torch.Tensor,
                   d8_pred: torch.Tensor, d8_target: torch.Tensor) -> Dict[str, float]:
    """Compute comprehensive evaluation metrics for water segmentation and D8 classification"""
    metrics = {}
    
    # Ensure correct tensor shapes (squeeze channel dimension if needed)
    if water_pred.dim() == 4 and water_pred.shape[1] == 1:
        water_pred = water_pred.squeeze(1)  # Remove channel dimension: [B,1,H,W] -> [B,H,W]
    
    # =========================
    # WATER SEGMENTATION METRICS (FOCUS ON WATER CLASS = 1)
    # =========================
    water_pred_sigmoid = torch.sigmoid(water_pred)
    water_pred_binary = (water_pred_sigmoid > 0.5).float()  # Binary: 1 = water, 0 = non-water
    
    # Convert to numpy for sklearn metrics
    water_pred_np = water_pred_binary.flatten().cpu().numpy().astype(int)
    water_target_np = water_target.flatten().cpu().numpy().astype(int)
    
    # Dice coefficient for WATER CLASS (intersection of water pixels only)
    intersection = (water_pred_binary * water_target).sum()  # TP: both pred and target = 1
    pred_sum = water_pred_binary.sum()  # All predicted water pixels
    target_sum = water_target.sum()     # All true water pixels
    dice = (2.0 * intersection / (pred_sum + target_sum + 1e-8)).item()
    
    # IoU for WATER CLASS (intersection over union of water pixels)
    intersection = (water_pred_binary * water_target).sum()  # TP: both pred and target = 1
    union = ((water_pred_binary + water_target) > 0).float().sum()  # All pixels that are water in either pred or target
    iou = (intersection / (union + 1e-8)).item()
    
    # Overall pixel accuracy (includes both water and non-water pixels)
    water_acc = (water_pred_binary == water_target).float().mean().item()
    
    # Precision, Recall, F1-score for WATER CLASS (pos_label=1)
    # These metrics specifically focus on water detection performance
    try:
        # Handle case where there might be only one class
        if len(np.unique(water_target_np)) == 1:
            # Only one class present in targets
            if water_target_np[0] == 1:  # Only water pixels in this patch
                water_precision = precision_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
                water_recall = recall_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
                water_f1 = f1_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
            else:  # Only non-water pixels in this patch
                water_precision = precision_score(water_target_np, water_pred_np, pos_label=1, zero_division=1.0)
                water_recall = recall_score(water_target_np, water_pred_np, pos_label=1, zero_division=1.0)
                water_f1 = f1_score(water_target_np, water_pred_np, pos_label=1, zero_division=1.0)
        else:
            # Both water and non-water pixels present (normal case)
            water_precision = precision_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
            water_recall = recall_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
            water_f1 = f1_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
    except Exception as e:
        logging.warning(f"Error computing water segmentation metrics: {e}")
        water_precision = water_recall = water_f1 = 0.0
    
    # =========================
    # D8 CLASSIFICATION METRICS
    # =========================
    d8_pred_classes = torch.argmax(d8_pred, dim=1)  # Class indices 0-8
    
    # Map class indices to D8 values
    d8_class_to_value = {0: 1, 1: 2, 2: 4, 3: 8, 4: 16, 5: 32, 6: 64, 7: 128, 8: 255}
    d8_pred_values = torch.zeros_like(d8_pred_classes)
    for class_idx, d8_value in d8_class_to_value.items():
        d8_pred_values[d8_pred_classes == class_idx] = d8_value
    
    # D8 accuracy (corrected)
    d8_acc = (d8_pred_values == d8_target).float().mean().item()
    
    # Convert to numpy for sklearn metrics  
    d8_pred_classes_np = d8_pred_classes.flatten().cpu().numpy()
    
    # Map D8 target values back to class indices for sklearn metrics
    d8_value_to_class = {1: 0, 2: 1, 4: 2, 8: 3, 16: 4, 32: 5, 64: 6, 128: 7, 255: 8}
    d8_target_flat = d8_target.flatten().cpu().numpy()
    d8_target_classes_np = np.zeros_like(d8_target_flat)
    for d8_value, class_idx in d8_value_to_class.items():
        d8_target_classes_np[d8_target_flat == d8_value] = class_idx
    
    # D8 classification metrics
    try:
        d8_precision_macro = precision_score(d8_target_classes_np, d8_pred_classes_np, average='macro', zero_division=0.0)
        d8_recall_macro = recall_score(d8_target_classes_np, d8_pred_classes_np, average='macro', zero_division=0.0)
        d8_f1_macro = f1_score(d8_target_classes_np, d8_pred_classes_np, average='macro', zero_division=0.0)
        
        d8_precision_weighted = precision_score(d8_target_classes_np, d8_pred_classes_np, average='weighted', zero_division=0.0)
        d8_recall_weighted = recall_score(d8_target_classes_np, d8_pred_classes_np, average='weighted', zero_division=0.0)
        d8_f1_weighted = f1_score(d8_target_classes_np, d8_pred_classes_np, average='weighted', zero_division=0.0)
    except Exception as e:
        logging.warning(f"Error computing D8 classification metrics: {e}")
        d8_precision_macro = d8_recall_macro = d8_f1_macro = 0.0
        d8_precision_weighted = d8_recall_weighted = d8_f1_weighted = 0.0
    
    # Compile all metrics
    metrics = {
        # Water segmentation metrics
        "water_dice": dice,
        "water_iou": iou,
        "water_accuracy": water_acc,
        "water_precision": water_precision,
        "water_recall": water_recall,
        "water_f1": water_f1,
        
        # D8 classification metrics
        "d8_accuracy": d8_acc,
        "d8_precision_macro": d8_precision_macro,
        "d8_recall_macro": d8_recall_macro,
        "d8_f1_macro": d8_f1_macro,
        "d8_precision_weighted": d8_precision_weighted,
        "d8_recall_weighted": d8_recall_weighted,
        "d8_f1_weighted": d8_f1_weighted
    }
    
    return metrics

def evaluate_huc(model: torch.nn.Module, huc_id: str, test_data_path: str, 
                device: str = "cpu", limit_batches: int = None) -> Dict[str, Any]:
    """Evaluate model on a single HUC"""
    
    # Create dataset for this HUC
    dataset = MultimodalPatchDataset_DEM_SAR(
        base_path=test_data_path,
        huc_codes=[huc_id]
    )
    
    if len(dataset) == 0:
        logger.warning(f"No data found for HUC {huc_id}")
        return {}
    
    dataloader = DataLoader(
        dataset,
        batch_size=4,
        shuffle=False,
        num_workers=0,  # Avoid multiprocessing issues
        pin_memory=False
    )
    
    water_predictions = []
    water_targets = []
    d8_predictions = []
    d8_targets = []
    
    model.eval()
    with torch.no_grad():
        for batch_idx, batch in enumerate(tqdm(dataloader, desc=f"Evaluating HUC {huc_id}")):
            if limit_batches and batch_idx >= limit_batches:
                break
                
            # Move data to device
            dem = batch['dem'].to(device)
            sar = batch['sar'].to(device)
            water_mask = batch['hydro_mask'].to(device)
            flow_dir = batch['flow_dir'].to(device)
            
            # Forward pass
            water_pred, d8_pred = model(dem, sar)
            
            # Collect predictions and targets
            water_predictions.append(water_pred.cpu())
            water_targets.append(water_mask.cpu())
            d8_predictions.append(d8_pred.cpu())
            d8_targets.append(flow_dir.cpu())
    
    if not water_predictions:
        logger.warning(f"No predictions collected for HUC {huc_id}")
        return {}
    
    # Concatenate all predictions
    water_pred_all = torch.cat(water_predictions, dim=0)
    water_target_all = torch.cat(water_targets, dim=0)
    d8_pred_all = torch.cat(d8_predictions, dim=0)
    d8_target_all = torch.cat(d8_targets, dim=0)
    
    # Compute metrics
    metrics = compute_metrics(water_pred_all, water_target_all, d8_pred_all, d8_target_all)
    
    logger.info(f"HUC {huc_id}: Water Dice={metrics['water_dice']:.3f}, Water IoU={metrics['water_iou']:.3f}, Water F1={metrics['water_f1']:.3f}, D8 Acc={metrics['d8_accuracy']:.3f}, D8 F1-macro={metrics['d8_f1_macro']:.3f}")
    
    return metrics

def main():
    parser = argparse.ArgumentParser(description="Simple DEM + SAR Model Evaluator")
    parser.add_argument("--checkpoint", type=str, required=True,
                       help="Path to model checkpoint")
    parser.add_argument("--huc-list", type=str, 
                       default="/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt",
                       help="Path to HUC list file")
    parser.add_argument("--test-data-path", type=str,
                       default="/projects/bcrm/nathanj/data/processed/test/patch_dataset",
                       help="Path to test data directory")
    parser.add_argument("--output-dir", type=str,
                       default="/u/nathanj/national_ml/experiments/evaluation/simple_dem_sar_results",
                       help="Output directory for results")
    parser.add_argument("--limit-batches", type=int,
                       help="Limit number of batches per HUC (for testing)")
    parser.add_argument("--device", type=str, choices=["cpu", "cuda", "auto"], default="auto",
                       help="Device to use for evaluation")
    
    args = parser.parse_args()
    
    # Set device
    device = args.device
    if device == "auto":
        device = "cuda" if torch.cuda.is_available() else "cpu"
    
    # Load model
    logger.info("Loading DEM + SAR model...")
    model = load_model_direct(args.checkpoint, device)
    if model is None:
        return 1
    
    # Load HUC list
    huc_codes = load_huc_list(args.huc_list)
    if not huc_codes:
        logger.error("No HUC codes found")
        return 1
    
    logger.info(f"Evaluating on {len(huc_codes)} HUCs")
    
    # Run evaluation
    results = []
    for huc_id in huc_codes:
        try:
            metrics = evaluate_huc(model, huc_id, args.test_data_path, device, args.limit_batches)
            if metrics:
                result = {
                    "timestamp": datetime.now().isoformat(),
                    "model_variant": "mdmt_dem_sar",
                    "checkpoint_path": args.checkpoint,
                    "huc_id": huc_id,
                    **metrics
                }
                results.append(result)
        except Exception as e:
            logger.error(f"Error evaluating HUC {huc_id}: {e}")
            continue
    
    if not results:
        print("❌ No evaluation results generated")
        return 1
    
    # Save results
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save CSV
    results_df = pd.DataFrame(results)
    csv_path = output_dir / f"simple_dem_sar_evaluation_{timestamp}.csv"
    results_df.to_csv(csv_path, index=False)
    
    # Save JSON
    json_path = output_dir / f"simple_dem_sar_evaluation_{timestamp}.json"
    results_df.to_json(json_path, orient='records', indent=2)
    
   # Print summary
    print("\n" + "="*80)
    print("DEM + SAR EVALUATION COMPLETED")
    print("="*80)
    print(f"Total evaluations: {len(results_df)}")
    print(f"HUCs evaluated: {results_df['huc_id'].nunique()}")
    print("\nWater Segmentation Metrics:")
    print(f"  Mean Water IoU: {results_df['water_iou'].mean():.3f}")
    print(f"  Mean Water Dice: {results_df['water_dice'].mean():.3f}")
    print(f"  Mean Water Precision: {results_df['water_precision'].mean():.3f}")
    print(f"  Mean Water Recall: {results_df['water_recall'].mean():.3f}")
    print(f"  Mean Water F1-Score: {results_df['water_f1'].mean():.3f}")
    print(f"  Mean Water Accuracy: {results_df['water_accuracy'].mean():.3f}")
    print("\nD8 Flow Direction Classification Metrics:")
    print(f"  Mean D8 Accuracy: {results_df['d8_accuracy'].mean():.3f}")
    print(f"  Mean D8 Precision (Macro): {results_df['d8_precision_macro'].mean():.3f}")
    print(f"  Mean D8 Recall (Macro): {results_df['d8_recall_macro'].mean():.3f}")
    print(f"  Mean D8 F1-Score (Macro): {results_df['d8_f1_macro'].mean():.3f}")
    print(f"  Mean D8 Precision (Weighted): {results_df['d8_precision_weighted'].mean():.3f}")
    print(f"  Mean D8 Recall (Weighted): {results_df['d8_recall_weighted'].mean():.3f}")
    print(f"  Mean D8 F1-Score (Weighted): {results_df['d8_f1_weighted'].mean():.3f}")
    print(f"\nResults saved to: {csv_path}")
    print("="*80)
    
    return 0

if __name__ == "__main__":
    exit(main())