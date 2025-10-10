#!/usr/bin/env python3
"""
ULTRA-FAST Clay Foundation Model Evaluator using lazy loading approach.
This version skips upfront validation and validates files on-demand.
Follows the same evaluation pattern as MDMT variants for fair comparison.
"""

import torch
import numpy as np
import os
import glob
import logging
from typing import Dict, Any, List
from torch.utils.data import Dataset, DataLoader

from sklearn.metrics import precision_recall_fscore_support, accuracy_score, precision_score, recall_score, f1_score
import pandas as pd
from datetime import datetime
import argparse
import wandb
import sys
from pathlib import Path

# Add paths for foundational model imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))  # Add foundational experiment directory

# Import Clay model
from clay.training.train_clay import ClayFoundationModel

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_evaluation_config(base_path: str, huc_codes: List[str], batch_size: int) -> dict:
    """Create config compatible with FourModalDataModule for evaluation."""
    return {
        "data": {
            "base_path": base_path,
            "huc_codes": huc_codes,
            "modalities": ["dem", "optical", "thermal", "sar"],
            "split_method": "patch_level",
            "split_ratio": 0.9,  # Not used in evaluation but required by data module
            "random_seed": 42,
            "optical_channels": 6,
            "total_channels": 9,
            "image_size": 224,  # Will be resized to 256 for Clay
            "normalize_per_huc": True
        },
        "training": {
            "batch_size": batch_size
        }
    }

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
        'variant': 'clay_foundation',
        'run_name': run_name,
        'checkpoint_path': checkpoint_path,
        'csv_path': csv_path
    }
    
    # Initialize wandb
    wandb.init(
        project="foundational_clay_evaluation",
        name=f"clay_{run_name}_evaluation",
        config=run_info,
        tags=["evaluation", "clay", "foundation_model", "ultra_fast"]
    )
    
    return wandb.run

def log_summary_to_wandb(results_df: pd.DataFrame, model_variant: str, run_name: str):
    """Log aggregated summary metrics to wandb."""
    
    # Calculate summary statistics across all HUCs
    summary_metrics = {}
    
    # Water segmentation metrics
    water_metrics = ['water_dice', 'water_iou', 'water_accuracy', 'water_precision', 'water_recall', 'water_f1']
    for metric in water_metrics:
        if metric in results_df.columns:
            summary_metrics[f'summary_{metric}_mean'] = results_df[metric].mean()
            summary_metrics[f'summary_{metric}_std'] = results_df[metric].std()
            summary_metrics[f'summary_{metric}_median'] = results_df[metric].median()
    
    # D8 flow direction metrics
    d8_metrics = ['d8_accuracy', 'd8_precision_macro', 'd8_recall_macro', 'd8_f1_macro',
                  'd8_precision_weighted', 'd8_recall_weighted', 'd8_f1_weighted']
    for metric in d8_metrics:
        if metric in results_df.columns:
            summary_metrics[f'summary_{metric}_mean'] = results_df[metric].mean()
            summary_metrics[f'summary_{metric}_std'] = results_df[metric].std()
            summary_metrics[f'summary_{metric}_median'] = results_df[metric].median()
    
    # Additional summary metrics
    summary_metrics.update({
        'total_hucs_evaluated': len(results_df),
        'total_samples': results_df['total_samples'].sum(),
        'mean_samples_per_huc': results_df['total_samples'].mean(),
        'model_variant': model_variant,
        'run_name': run_name
    })
    
    # Log summary metrics
    wandb.log(summary_metrics)
    
    # Also log per-HUC metrics table
    wandb.log({"huc_metrics_table": wandb.Table(dataframe=results_df)})
    
    logger.info(f"Logged summary metrics to wandb: {len(summary_metrics)} metrics for {len(results_df)} HUCs")

def get_device(device_arg: str = None) -> str:
    """Determine the best available device."""
    if device_arg is None:
        # Auto-select
        if torch.cuda.is_available():
            logger.info(f"Auto-selected device: cuda (GPU available)")
            return "cuda"
        else:
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

# Import Clay model and data module
from clay.training.train_clay import ClayFoundationModel
from clay.data.four_modal_dataset_adapter import FourModalDataModule

def compute_metrics_original(water_pred: torch.Tensor, water_target: torch.Tensor,
                   d8_pred: torch.Tensor = None, d8_target: torch.Tensor = None) -> Dict[str, float]:
    """Compute comprehensive evaluation metrics - foundation models only do water segmentation"""
    metrics = {}
    
    # Ensure correct tensor shapes (squeeze channel dimension if needed)
    if water_pred.dim() == 4 and water_pred.shape[1] == 1:
        water_pred = water_pred.squeeze(1)  # Remove channel dimension: [B,1,H,W] -> [B,H,W]
    
    # =========================
    # WATER SEGMENTATION METRICS (FOCUS ON WATER CLASS = 1)
    # =========================
    
    # Apply sigmoid to get probabilities, then threshold
    water_prob = torch.sigmoid(water_pred)
    water_binary = (water_prob > 0.5).float()
    
    # Flatten for metric computation
    water_pred_flat = water_binary.view(-1).cpu().numpy()
    water_target_flat = water_target.view(-1).cpu().numpy()
    
    # Dice Coefficient (F1 for binary segmentation)
    intersection = np.sum(water_pred_flat * water_target_flat)
    dice = (2.0 * intersection) / (np.sum(water_pred_flat) + np.sum(water_target_flat) + 1e-8)
    metrics['water_dice'] = dice
    
    # IoU (Jaccard Index)
    union = np.sum(water_pred_flat) + np.sum(water_target_flat) - intersection
    iou = intersection / (union + 1e-8)
    metrics['water_iou'] = iou
    
    # Basic classification metrics
    metrics['water_accuracy'] = accuracy_score(water_target_flat, water_pred_flat)
    metrics['water_precision'] = precision_score(water_target_flat, water_pred_flat, zero_division=0)
    metrics['water_recall'] = recall_score(water_target_flat, water_pred_flat, zero_division=0)
    metrics['water_f1'] = f1_score(water_target_flat, water_pred_flat, zero_division=0)
    
    # =========================
    # D8 FLOW DIRECTION METRICS
    # =========================
    # Foundation models only do water segmentation - skip D8 metrics
    metrics['d8_accuracy'] = 0.0  # Placeholder for compatibility
    metrics['d8_precision_macro'] = 0.0
    metrics['d8_recall_macro'] = 0.0
    metrics['d8_f1_macro'] = 0.0
    metrics['d8_precision_weighted'] = 0.0
    metrics['d8_recall_weighted'] = 0.0
    metrics['d8_f1_weighted'] = 0.0
    
    return metrics

def evaluate_single_huc(model: torch.nn.Module, huc_code: str, base_path: str, 
                       device: str, batch_size: int = 32) -> Dict[str, Any]:
    """Evaluate model on a single HUC."""
    
    logger.info(f"Evaluating HUC: {huc_code}")
    
    # Use the same data pipeline as training but with ALL patches for evaluation
    try:
        # Create config for the specific HUC
        config = create_evaluation_config(base_path, [huc_code], batch_size)
        
        # Create the adapter to get all patch metadata (same preprocessing as training)
        from clay.data.four_modal_dataset_adapter import FourModalPatchDatasetAdapter, FourModalPatchDataset
        
        adapter = FourModalPatchDatasetAdapter(
            base_path=config['data']['base_path'],
            huc_codes=config['data']['huc_codes'],
            config=config
        )
        
        # Use ALL patches for evaluation (not just validation split)
        all_patches = adapter.patch_metadata
        
        if len(all_patches) == 0:
            logger.warning(f"No valid patches found for HUC {huc_code}")
            return None
            
        logger.info(f"Using ALL {len(all_patches)} patches for evaluation of HUC {huc_code} (not just validation split)")
        
        # Create dataset with all patches using same preprocessing as training
        evaluation_dataset = FourModalPatchDataset(
            all_patches,
            config,
            huc_norm=adapter.huc_norm,
            is_training=False  # Use validation-style processing (no augmentations)
        )
        
        # Create evaluation dataloader with single-threaded processing
        from torch.utils.data import DataLoader
        dataloader = DataLoader(
            evaluation_dataset,
            batch_size=batch_size,
            shuffle=False,
            num_workers=0,  # Single-threaded to avoid CUDA issues
            pin_memory=True if device.startswith('cuda') else False
        )
        
    except Exception as e:
        logger.error(f"Failed to create evaluation dataset for HUC {huc_code}: {e}")
        return None
    
    model.eval()
    all_water_preds = []
    all_water_targets = []
    all_d8_preds = []
    all_d8_targets = []
    
    total_batches = len(dataloader)
    with torch.no_grad():
        for batch_idx, batch in enumerate(dataloader):
            # Log progress every 10 batches
            if batch_idx % 10 == 0:
                logger.info(f"Processing batch {batch_idx + 1}/{total_batches} for HUC {huc_code}")
            
            # Move to device - use keys from FourModalDataModule
            images = batch['image'].to(device, non_blocking=True)
            hydro_masks = batch['mask'].to(device, non_blocking=True)  # FourModalDataModule uses 'mask' not 'hydro_mask'
            # Note: FourModalDataModule doesn't provide flow_dir since foundation models only do water segmentation
            flow_dirs = None  # Foundation models don't predict flow direction
            
            # Forward pass
            outputs = model(images)
            
            # Handle TerraTorch ModelOutput format - foundation models only do water segmentation
            if hasattr(outputs, 'output'):
                # TerraTorch foundation models return ModelOutput with 'output' attribute
                water_pred = outputs.output
                # Ensure water_pred has channel dimension: (B, H, W) -> (B, 1, H, W)
                if water_pred.dim() == 3:
                    water_pred = water_pred.unsqueeze(1)
            else:
                # Fallback for other output formats
                water_pred = outputs
            
            # Collect predictions and targets (only water segmentation)
            all_water_preds.append(water_pred.cpu())
            all_water_targets.append(hydro_masks.cpu())
    
    # Concatenate all batches
    all_water_preds = torch.cat(all_water_preds, dim=0)
    all_water_targets = torch.cat(all_water_targets, dim=0)
    
    # Compute metrics (only water segmentation)
    metrics = compute_metrics_original(all_water_preds, all_water_targets, None, None)
    metrics['total_samples'] = len(all_water_targets)
    
    return metrics

def main():
    parser = argparse.ArgumentParser(description='Ultra-fast Clay Foundation Model Evaluator')
    parser.add_argument('--checkpoint', type=str, required=True, 
                       help='Path to trained Clay model checkpoint')
    parser.add_argument('--data_path', type=str, 
                       default='/projects/bcrm/nathanj/data/processed/test/patch_dataset',
                       help='Path to patch dataset')
    parser.add_argument('--output_dir', type=str, 
                       default='/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_results',
                       help='Output directory for results')
    parser.add_argument('--batch_size', type=int, default=32, help='Batch size for evaluation')
    parser.add_argument('--device', type=str, default=None, help='Device to use (cuda/cpu, auto-detect if None)')
    parser.add_argument('--huc_file', type=str, 
                       default='/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt',
                       help='File containing list of test HUC codes')
    parser.add_argument('--run_name', type=str, default=None, help='Run name for output files')
    parser.add_argument('--wandb_mode', type=str, default='online', choices=['online', 'offline', 'disabled'],
                       help='Wandb logging mode')
    
    args = parser.parse_args()
    
    # Set wandb mode
    os.environ['WANDB_MODE'] = args.wandb_mode
    
    # Determine device
    device = get_device(args.device)
    
    # Initialize CUDA context if using GPU to prevent worker initialization errors
    if device.startswith('cuda'):
        import torch
        torch.cuda.init()
        torch.cuda.empty_cache()
        logger.info(f"Initialized CUDA context on {device}")
    
    # Extract run name from checkpoint if not provided
    if args.run_name is None:
        checkpoint_name = os.path.basename(args.checkpoint)
        args.run_name = checkpoint_name.split('.')[0]
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Generate output CSV filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"clay_{args.run_name}_{timestamp}.csv"
    csv_path = os.path.join(args.output_dir, csv_filename)
    
    logger.info(f"Starting Clay evaluation with checkpoint: {args.checkpoint}")
    logger.info(f"Results will be saved to: {csv_path}")
    
    # Setup wandb
    if args.wandb_mode != 'disabled':
        wandb_run = setup_wandb(args.run_name, args.checkpoint, csv_path)
    
    # Load model
    logger.info("Loading Clay model...")
    try:
        # Load the checkpoint with proper model config
        model = ClayFoundationModel.load_from_checkpoint(args.checkpoint)
        model = model.to(device)
        model.eval()
        logger.info(f"Successfully loaded Clay model with {sum(p.numel() for p in model.parameters()):,} parameters")
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        return
    
    # Load test HUC codes
    with open(args.huc_file, 'r') as f:
        test_hucs = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    logger.info(f"Evaluating on {len(test_hucs)} test HUCs")
    
    # Evaluate each HUC
    results = []
    failed_hucs = []
    
    for i, huc_code in enumerate(test_hucs):
        try:
            logger.info(f"Processing HUC {i+1}/{len(test_hucs)}: {huc_code}")
            
            # Evaluate single HUC
            huc_metrics = evaluate_single_huc(model, huc_code, args.data_path, device, args.batch_size)
            
            if huc_metrics is None:
                failed_hucs.append(huc_code)
                continue
            
            # Create result record
            result_record = {
                'timestamp': datetime.now().isoformat(),
                'model_variant': 'clay_foundation',
                'checkpoint_path': args.checkpoint,
                'run_name': args.run_name,
                'huc_id': huc_code,
                **huc_metrics
            }
            
            # Append to CSV incrementally
            append_to_csv(result_record, csv_path)
            results.append(result_record)
            
            # Log per-HUC metrics to wandb
            if args.wandb_mode != 'disabled':
                wandb_metrics = {f'huc_{huc_code}_{k}': v for k, v in huc_metrics.items()}
                wandb.log(wandb_metrics)
            
            logger.info(f"Completed {huc_code}: Dice={huc_metrics['water_dice']:.4f}, IoU={huc_metrics['water_iou']:.4f}")
            
        except Exception as e:
            logger.error(f"Failed to process HUC {huc_code}: {e}")
            failed_hucs.append(huc_code)
            continue
    
    # Log summary metrics to wandb
    if results and args.wandb_mode != 'disabled':
        results_df = pd.DataFrame(results)
        log_summary_to_wandb(results_df, 'clay_foundation', args.run_name)
    
    logger.info(f"Evaluation completed!")
    logger.info(f"Successfully processed: {len(results)} HUCs")
    logger.info(f"Failed: {len(failed_hucs)} HUCs")
    if failed_hucs:
        logger.info(f"Failed HUCs: {failed_hucs}")
    logger.info(f"Results saved to: {csv_path}")
    
    if args.wandb_mode != 'disabled':
        wandb.finish()

if __name__ == "__main__":
    main()