#!/usr/bin/env python3
"""
ULTRA-FAST Landsat 6B Evaluator using lazy loading approach.
This version skips upfront validation and validates files on-demand.
"""

import torch
import numpy as np
import os
import glob
import logging
from typing import Dict, Any, List
from torch.utils.data import Dataset, DataLoader
from tqdm import tqdm
from sklearn.metrics import precision_recall_fscore_support, accuracy_score, precision_score, recall_score, f1_score
import pandas as pd
from datetime import datetime
import argparse
import wandb

# Import model (adjust path as needed)
import sys
sys.path.append('/u/nathanj/national_ml/experiments')
sys.path.append('/u/nathanj/national_ml/src')
sys.path.append('/u/nathanj/national_ml/experiments/models')
from mdmt_landsat_6b import MultimodalMultitaskModel_ls6b

# Add path to data module and import dataset
sys.path.append('/u/nathanj/national_ml/experiments/data')
from patchDataLoader_ls6b_fast import MultimodalPatchDataset_LS6B_Fast as MultimodalPatchDataset

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def append_to_csv(result_dict: Dict[str, Any], csv_path: str):
    """Append a single result to CSV file incrementally."""
    df_row = pd.DataFrame([result_dict])
    
    # If file doesn't exist, create with header
    if not os.path.exists(csv_path):
        df_row.to_csv(csv_path, index=False, mode='w')
    else:
        # Append without header
        df_row.to_csv(csv_path, index=False, mode='a', header=False)

def setup_wandb(run_name: str, checkpoint_path: str, csv_path: str = None):
    """Initialize wandb run for logging."""
    # Extract run info from checkpoint path
    run_info = {
        'variant': 'landsat6b',
        'run_name': run_name,
        'checkpoint': os.path.basename(checkpoint_path),
        'checkpoint_path': checkpoint_path,
        'timestamp': datetime.now().isoformat()
    }
    
    # Add CSV path if provided
    if csv_path:
        run_info['csv_output_path'] = csv_path
    
    # Initialize wandb
    wandb.init(
        project="mdmt_landsat6b_evaluation",
        name=run_name,
        config=run_info,
        tags=["landsat6b", "evaluation", "ultra_fast"]
    )
    
    return wandb

def get_device(device_arg: str = "auto") -> str:
    """
    Automatically select the best available device.
    Args:
        device_arg: Device specification ("auto", "cpu", "cuda", "cuda:0", etc.)
    Returns:
        Device string suitable for torch.device()
    """
    if device_arg == "auto":
        if torch.cuda.is_available():
            # Get the GPU with most free memory
            gpu_count = torch.cuda.device_count()
            if gpu_count > 0:
                best_gpu = 0
                max_free_memory = 0
                
                for i in range(gpu_count):
                    torch.cuda.set_device(i)
                    free_memory = torch.cuda.get_device_properties(i).total_memory - torch.cuda.memory_allocated(i)
                    if free_memory > max_free_memory:
                        max_free_memory = free_memory
                        best_gpu = i
                
                device = f"cuda:{best_gpu}"
                gpu_name = torch.cuda.get_device_name(best_gpu)
                free_gb = max_free_memory / (1024**3)
                logger.info(f"Auto-selected device: {device} ({gpu_name}, {free_gb:.1f}GB free)")
                return device
        
        logger.info("Auto-selected device: cpu (no CUDA available)")
        return "cpu"
    else:
        # User specified device
        if device_arg.startswith("cuda") and torch.cuda.is_available():
            logger.info(f"Using user-specified device: {device_arg}")
            return device_arg
        elif device_arg == "cpu":
            logger.info("Using user-specified device: cpu")
            return device_arg
        else:
            logger.warning(f"Requested device '{device_arg}' not available, falling back to cpu")
            return "cpu"

# FastAlphaEarthDataset removed - using proper MultimodalPatchDataset_LS6B_Fast instead

def compute_metrics_original(water_pred: torch.Tensor, water_target: torch.Tensor,
                   d8_pred: torch.Tensor, d8_target: torch.Tensor) -> Dict[str, float]:
    """Compute comprehensive evaluation metrics - EXACT COPY from simple_alphaearth_evaluator.py"""
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
        logger.warning(f"Error computing water segmentation metrics: {e}")
        water_precision = water_recall = water_f1 = 0.0
    
    # =========================
    # D8 CLASSIFICATION METRICS
    # =========================
    d8_pred_classes = torch.argmax(d8_pred, dim=1)  # Class indices 0-8
    
    # Map class indices to D8 values - EXACT ORIGINAL MAPPING
    d8_class_to_value = {0: 1, 1: 2, 2: 4, 3: 8, 4: 16, 5: 32, 6: 64, 7: 128, 8: 255}
    d8_pred_values = torch.zeros_like(d8_pred_classes)
    for class_idx, d8_value in d8_class_to_value.items():
        d8_pred_values[d8_pred_classes == class_idx] = d8_value
    
    # D8 accuracy - EXACT ORIGINAL (includes -2 values)
    d8_acc = (d8_pred_values == d8_target).float().mean().item()
    
    # Convert to numpy for sklearn metrics  
    d8_pred_classes_np = d8_pred_classes.flatten().cpu().numpy()
    
    # Map D8 target values back to class indices - EXACT ORIGINAL MAPPING
    d8_value_to_class = {1: 0, 2: 1, 4: 2, 8: 3, 16: 4, 32: 5, 64: 6, 128: 7, 255: 8}
    d8_target_flat = d8_target.flatten().cpu().numpy()
    d8_target_classes_np = np.zeros_like(d8_target_flat)
    for d8_value, class_idx in d8_value_to_class.items():
        d8_target_classes_np[d8_target_flat == d8_value] = class_idx
    
    # D8 classification metrics - EXACT ORIGINAL
    try:
        d8_precision_macro = precision_score(d8_target_classes_np, d8_pred_classes_np, average='macro', zero_division=0.0)
        d8_recall_macro = recall_score(d8_target_classes_np, d8_pred_classes_np, average='macro', zero_division=0.0)
        d8_f1_macro = f1_score(d8_target_classes_np, d8_pred_classes_np, average='macro', zero_division=0.0)
        
        d8_precision_weighted = precision_score(d8_target_classes_np, d8_pred_classes_np, average='weighted', zero_division=0.0)
        d8_recall_weighted = recall_score(d8_target_classes_np, d8_pred_classes_np, average='weighted', zero_division=0.0)
        d8_f1_weighted = f1_score(d8_target_classes_np, d8_pred_classes_np, average='weighted', zero_division=0.0)
    except Exception as e:
        logger.warning(f"Error computing D8 classification metrics: {e}")
        d8_precision_macro = d8_recall_macro = d8_f1_macro = 0.0
        d8_precision_weighted = d8_recall_weighted = d8_f1_weighted = 0.0
    
    # Compile all metrics - EXACT ORIGINAL RETURN FORMAT
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

def compute_metrics(predictions: np.ndarray, targets: np.ndarray, task: str = "water") -> Dict[str, float]:
    """Compute comprehensive metrics"""
    if task == "water":
        # Water segmentation metrics (binary: 0=no water, 1=water)
        tp = np.sum((predictions == 1) & (targets == 1))
        fp = np.sum((predictions == 1) & (targets == 0))
        fn = np.sum((predictions == 0) & (targets == 1))
        tn = np.sum((predictions == 0) & (targets == 0))
        
        # Handle division by zero
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        
        iou = tp / (tp + fp + fn) if (tp + fp + fn) > 0 else 0.0
        dice = 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) > 0 else 0.0
        accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0.0
        
        return {
            'iou': iou,
            'dice': dice,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'accuracy': accuracy
        }
    
    elif task == "d8":
        # D8 flow direction classification metrics
        accuracy = accuracy_score(targets, predictions)
        precision_macro, recall_macro, f1_macro, _ = precision_recall_fscore_support(
            targets, predictions, average='macro', zero_division=0
        )
        precision_weighted, recall_weighted, f1_weighted, _ = precision_recall_fscore_support(
            targets, predictions, average='weighted', zero_division=0
        )
        
        return {
            'accuracy': accuracy,
            'precision_macro': precision_macro,
            'recall_macro': recall_macro,
            'f1_macro': f1_macro,
            'precision_weighted': precision_weighted,
            'recall_weighted': recall_weighted,
            'f1_weighted': f1_weighted
        }

def fast_evaluate_huc(model: torch.nn.Module, huc_id: str, 
                      test_data_path: str, device: str = "cpu", 
                      limit_batches: int = None) -> Dict[str, Any]:
    """Fast evaluation for a single HUC"""
    
    logger.info(f"Evaluating HUC {huc_id}...")
    
    # Use the SAME dataset class as the original evaluator for compatibility
    dataset = MultimodalPatchDataset(
        base_path=test_data_path,
        huc_codes=[huc_id]
    )
    
    if len(dataset) == 0:
        logger.warning(f"No data found for HUC {huc_id}")
        return {}
    
    # Optimize batch size for GPU
    batch_size = 8 if device.startswith('cuda') else 4  # Larger batches for GPU
    
    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0,  # Avoid multiprocessing issues  
        pin_memory=device.startswith('cuda')  # Use pin_memory for GPU acceleration
    )
    
    water_predictions = []
    water_targets = []
    d8_predictions = []
    d8_targets = []
    
    with torch.no_grad():
        for batch_idx, batch in enumerate(dataloader):
            if limit_batches and batch_idx >= limit_batches:
                break
            
            # Move to device (non_blocking for GPU efficiency)
            m1 = batch['m1'].to(device, non_blocking=True)  # DEM
            m2 = batch['m2'].to(device, non_blocking=True)  # Optical (6-band)
            m3 = batch['m3'].to(device, non_blocking=True)  # Thermal
            m4 = batch['m4'].to(device, non_blocking=True)  # SAR
            water_mask = batch['hydro_mask'].to(device, non_blocking=True)
            flow_dir = batch['flow_dir'].to(device, non_blocking=True)
            
            # Forward pass - Landsat6B model takes 4 multimodal inputs
            water_pred, d8_pred = model(m1, m2, m3, m4)
            
            # Collect predictions and targets as TENSORS (like original)
            water_predictions.append(water_pred.cpu())
            water_targets.append(water_mask.cpu())
            d8_predictions.append(d8_pred.cpu())
            d8_targets.append(flow_dir.cpu())
            
            # Clear GPU cache periodically to prevent OOM
            if device.startswith('cuda') and batch_idx % 50 == 0:
                torch.cuda.empty_cache()
    
    if not water_predictions:
        logger.warning(f"No predictions collected for HUC {huc_id}")
        return {}
    
    # Concatenate all predictions - EXACTLY like original
    water_pred_all = torch.cat(water_predictions, dim=0)
    water_target_all = torch.cat(water_targets, dim=0)
    d8_pred_all = torch.cat(d8_predictions, dim=0)
    d8_target_all = torch.cat(d8_targets, dim=0)
    
    # Use the ORIGINAL compute_metrics function signature
    metrics = compute_metrics_original(water_pred_all, water_target_all, d8_pred_all, d8_target_all)
    
    # The original function returns the complete metrics dict - use it directly
    results = metrics.copy()  # Already contains all required metrics
    results['total_samples'] = len(water_predictions)
    
    return results

def fast_evaluate_all_hucs(checkpoint_path: str, huc_codes: List[str], 
                          test_data_path: str = "/projects/bcrm/nathanj/data/processed/test/patch_dataset",
                          device: str = "cpu", limit_batches: int = None, 
                          run_name: str = "unknown", use_wandb: bool = True) -> List[Dict[str, Any]]:
    """Ultra-fast evaluation function that evaluates each HUC separately"""
    
    logger.info("Loading Landsat 6B model...")
    model = MultimodalMultitaskModel_ls6b(
        n_classes_task1=1,  # Water segmentation (binary)
        n_classes_task2=8,  # D8 flow direction (8 classes)
        base_channels=64
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
        
        logger.info(f"Model loading: {len(matching_keys)} matching, {len(missing_keys)} missing, {len(unexpected_keys)} unexpected")
        
        # Create filtered state dict with only matching keys
        filtered_state_dict = {k: model_state_dict[k] for k in matching_keys}
        
        # Load the filtered state dict
        model.load_state_dict(filtered_state_dict, strict=False)
    else:
        model.load_state_dict(checkpoint, strict=False)
    
    model = model.to(device)
    model.eval()
    
    logger.info(f"Starting per-HUC evaluation on {len(huc_codes)} HUCs...")
    
    # Setup output directories and files
    output_dir = "/u/nathanj/national_ml/experiments/evaluation/ultra_fast_results"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"landsat6b_{run_name}_{timestamp}"
    csv_path = os.path.join(output_dir, f"{csv_filename}.csv")
    
    # Setup wandb
    if use_wandb:
        wb_run = setup_wandb(csv_filename, checkpoint_path, csv_path)
        
        # Create wandb table - standardized columns
        columns = ["huc_id", "water_dice", "water_iou", "water_accuracy", "water_precision", "water_recall", "water_f1", 
                  "d8_accuracy", "d8_precision_macro", "d8_recall_macro", "d8_f1_macro", 
                  "d8_precision_weighted", "d8_recall_weighted", "d8_f1_weighted", "total_samples"]
        wandb_table = wandb.Table(columns=columns)
    
    results = []
    for i, huc_id in enumerate(huc_codes, 1):
        try:
            logger.info(f"Evaluating HUC {huc_id} ({i}/{len(huc_codes)})...")
            huc_metrics = fast_evaluate_huc(model, huc_id, test_data_path, device, limit_batches)
            
            if huc_metrics:
                result = {
                    "timestamp": datetime.now().isoformat(),
                    "model_variant": "mdmt_landsat6b",
                    "checkpoint_path": checkpoint_path,
                    "run_name": run_name,  
                    "huc_id": huc_id,
                    **huc_metrics
                }
                results.append(result)
                
                # Incremental CSV writing
                append_to_csv(result, csv_path)
                
                # Add to wandb table - standardized columns
                if use_wandb:
                    wandb_table.add_data(
                        huc_id,
                        huc_metrics['water_dice'],
                        huc_metrics['water_iou'],
                        huc_metrics['water_accuracy'],
                        huc_metrics['water_precision'], 
                        huc_metrics['water_recall'],
                        huc_metrics['water_f1'],
                        huc_metrics['d8_accuracy'],
                        huc_metrics['d8_precision_macro'],
                        huc_metrics['d8_recall_macro'],
                        huc_metrics['d8_f1_macro'],
                        huc_metrics['d8_precision_weighted'],
                        huc_metrics['d8_recall_weighted'],
                        huc_metrics['d8_f1_weighted'],
                        huc_metrics['total_samples']
                    )
                    
                    # Log individual HUC metrics
                    wandb.log({
                        f"huc_metrics/{huc_id}/water_f1": huc_metrics['water_f1'],
                        f"huc_metrics/{huc_id}/d8_f1_macro": huc_metrics['d8_f1_macro'],
                        "progress": i / len(huc_codes)
                    })
                
                logger.info(f"HUC {huc_id}: Water F1={huc_metrics['water_f1']:.3f}, D8 F1-macro={huc_metrics['d8_f1_macro']:.3f}")
                
        except Exception as e:
            logger.error(f"Error evaluating HUC {huc_id}: {e}")
            continue
    
    # Log final wandb table and summary metrics
    if use_wandb and results:
        wandb.log({"huc_results_table": wandb_table})
        
        # Calculate and log summary statistics
        water_f1_scores = [r['water_f1'] for r in results]
        d8_f1_scores = [r['d8_f1_macro'] for r in results]
        
        summary_metrics = {
            "summary/mean_water_f1": np.mean(water_f1_scores),
            "summary/std_water_f1": np.std(water_f1_scores),
            "summary/mean_d8_f1": np.mean(d8_f1_scores),
            "summary/std_d8_f1": np.std(d8_f1_scores),
            "summary/total_hucs": len(results),
            "summary/csv_path": csv_path
        }
        wandb.log(summary_metrics)
        
        logger.info(f"Summary: Water F1={np.mean(water_f1_scores):.3f}±{np.std(water_f1_scores):.3f}, "
                   f"D8 F1={np.mean(d8_f1_scores):.3f}±{np.std(d8_f1_scores):.3f}")
        
        wandb.finish()
    
    logger.info(f"Results saved to: {csv_path}")
    return results

def main():
    parser = argparse.ArgumentParser(description="Ultra-fast Landsat 6B Evaluator")
    parser.add_argument("--checkpoint", required=True, help="Path to model checkpoint")
    parser.add_argument("--huc-list", default="/u/nathanj/national_ml/experiments/evaluation/test_huc_small.txt")
    parser.add_argument("--device", default="auto", 
                       help="Device to use: 'auto' (default, auto-select best), 'cpu', 'cuda', 'cuda:0', etc.")
    parser.add_argument("--limit-batches", type=int, help="Limit batches for testing")
    
    args = parser.parse_args()
    
    # Auto-select device
    device = get_device(args.device)
    
    # Load HUC codes
    with open(args.huc_list, 'r') as f:
        huc_codes = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    logger.info(f"Loaded {len(huc_codes)} HUC codes")
    
    # Extract run name from checkpoint path for wandb and csv naming
    checkpoint_path = args.checkpoint
    run_name = "unknown"
    if "run" in checkpoint_path:
        try:
            # Look for run pattern in path (e.g., "run1", "run2", etc.)
            parts = checkpoint_path.split("/")
            for part in parts:
                if "run" in part:
                    # Extract run number from patterns like "combineFL_ls6b_20250925_120906_run3"
                    if "_run" in part:
                        run_number = part.split("_run")[-1]
                        run_name = f"run{run_number}"
                        break
                    # Extract run number from patterns like "run3"
                    elif part.startswith("run") and len(part) > 3 and part[3:].isdigit():
                        run_name = part
                        break
        except:
            pass
    
    # Run per-HUC evaluation with incremental output and wandb logging
    if device.startswith('cuda'):
        torch.cuda.synchronize()
    start_time = datetime.now()
    results = fast_evaluate_all_hucs(args.checkpoint, huc_codes, device=device, 
                                   limit_batches=args.limit_batches, run_name=run_name, use_wandb=True)
    if device.startswith('cuda'):
        torch.cuda.synchronize()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    if not results:
        print("❌ No evaluation results generated")
        return 1
    
    # Calculate summary statistics
    water_ious = [r['water_iou'] for r in results]
    water_f1s = [r['water_f1'] for r in results]
    d8_accs = [r['d8_accuracy'] for r in results]
    d8_f1s = [r['d8_f1_macro'] for r in results]
    total_samples = sum(r['total_samples'] for r in results)
    
    # Print results
    # Add device info and GPU memory usage if applicable
    device_info = f"Device: {device}"
    if device.startswith('cuda'):
        if torch.cuda.is_available():
            gpu_memory = torch.cuda.get_device_properties(device).total_memory / (1024**3)
            used_memory = torch.cuda.memory_allocated(device) / (1024**3)
            device_info += f" ({torch.cuda.get_device_name(device)}, {used_memory:.1f}/{gpu_memory:.1f}GB)"
    
    print("\n" + "="*80)
    print("ULTRA-FAST LANDSAT 6B EVALUATION COMPLETED (PER-HUC)")
    print("="*80)
    print(f"Evaluation time: {duration:.1f} seconds")
    print(f"HUCs evaluated: {len(results)}")
    print(f"Total samples: {total_samples}")
    print(f"{device_info}")
    print()
    
    print("Water Segmentation Metrics (Mean across HUCs):")
    print(f"  Mean IoU: {np.mean(water_ious):.3f}")
    print(f"  Mean F1-Score: {np.mean(water_f1s):.3f}")
    print()
    
    print("D8 Flow Direction Metrics (Mean across HUCs):")
    print(f"  Mean Accuracy: {np.mean(d8_accs):.3f}")
    print(f"  Mean F1-Score (Macro): {np.mean(d8_f1s):.3f}")
    print()
    
    print("Per-HUC Results:")
    for result in results:
        huc_id = result['huc_id']
        water_f1 = result['water_f1']
        d8_f1 = result['d8_f1_macro']
        samples = result['total_samples']
        print(f"  {huc_id}: Water F1={water_f1:.3f}, D8 F1-macro={d8_f1:.3f}, Samples={samples}")
    print("="*80)
    
    # Save results to CSV (matching original format)
    output_dir = "/u/nathanj/national_ml/experiments/evaluation/ultra_fast_results"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Extract run name from checkpoint path
    run_name = "unknown"
    if "run" in args.checkpoint:
        try:
            # Look for lightning_logs directory which contains the run info
            parts = args.checkpoint.split("/")
            for part in parts:
                if "run" in part and "_" in part:
                    run_name = part.split("_")[-1]  # Extract run number (e.g., "run3")
                    break
        except:
            pass
    
    csv_path = os.path.join(output_dir, f"landsat6b_{run_name}_{timestamp}.csv")
    
    df = pd.DataFrame(results)
    df.to_csv(csv_path, index=False)
    print(f"Results saved to: {csv_path}")
    
    return 0

if __name__ == "__main__":
    main()