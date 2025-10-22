#!/usr/bin/env python3
"""
DEM Sanity Analysis: Prove DEM Provides Physical Topographic Signal

What: Start from DEM+AlphaEarth; replace DEM with (a) constant plane, (b) smoothed DEM.
Why: If performance drops toward AlphaEarth-only, reviewers see DEM adds topographic signal.
Metrics: mIoU, ClDice; delta vs DEM+AlphaEarth and vs AlphaEarth-only.
Figure: Small bar group showing the drop.

This analysis tests:
1. DEM+AlphaEarth (baseline)
2. ConstantDEM+AlphaEarth (DEM replaced with constant plane)  
3. SmoothedDEM+AlphaEarth (DEM smoothed with Gaussian filter)
4. AlphaEarth-only (reference baseline)

Expected outcome: Performance should drop from DEM+AE → SmoothedDEM+AE → ConstantDEM+AE → AE-only,
proving that topographic detail in DEM contributes meaningfully to model performance.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any, Tuple
import json
import pandas as pd
from datetime import datetime

import torch
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
import numpy as np
from tqdm import tqdm
from sklearn.metrics import precision_score, recall_score, f1_score
from scipy import ndimage
import matplotlib.pyplot as plt
import seaborn as sns

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

# Import models and data loaders
from models.mdmt_dem_alphaearth import MultimodalMultitaskModel_DEM_AlphaEarth
from models.mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
from data.patchDataLoader_dem_alphaearth import MultimodalPatchDataset_DEM_AlphaEarth

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DEMModifiedDataset(Dataset):
    """
    Dataset wrapper that modifies DEM data on-the-fly for sanity testing.
    Modes:
    - 'original': Original DEM data (baseline)
    - 'constant': Replace DEM with constant plane (mean elevation)
    - 'smoothed': Apply Gaussian smoothing to DEM (reduces topographic detail)
    """
    def __init__(self, base_dataset: MultimodalPatchDataset_DEM_AlphaEarth, 
                 dem_modification: str = 'original', gaussian_sigma: float = 3.0):
        self.base_dataset = base_dataset
        self.dem_modification = dem_modification
        self.gaussian_sigma = gaussian_sigma
        
        # Pre-compute constant plane values for each HUC if needed
        self.huc_constant_planes = {}
        if dem_modification == 'constant':
            self._compute_constant_planes()
    
    def _compute_constant_planes(self):
        """Pre-compute mean elevation for each HUC to use as constant plane."""
        logger.info("Computing constant plane values for each HUC...")
        
        huc_elevations = {}
        
        # Sample data to compute HUC-level statistics
        for idx in range(len(self.base_dataset)):
            sample = self.base_dataset[idx]
            file_path = sample['path']
            
            # Extract HUC from file path
            huc_code = Path(file_path).parent.name
            
            if huc_code not in huc_elevations:
                huc_elevations[huc_code] = []
            
            # Collect elevation data
            dem_data = sample['dem'].squeeze().numpy()  # Remove channel dimension
            huc_elevations[huc_code].append(dem_data.flatten())
        
        # Compute mean elevation for each HUC
        for huc_code, elevation_lists in huc_elevations.items():
            all_elevations = np.concatenate(elevation_lists)
            mean_elevation = np.mean(all_elevations)
            self.huc_constant_planes[huc_code] = mean_elevation
            logger.info(f"HUC {huc_code}: Mean elevation = {mean_elevation:.2f}m")
    
    def __len__(self):
        return len(self.base_dataset)
    
    def __getitem__(self, idx):
        sample = self.base_dataset[idx]
        
        # Get original DEM data
        dem_original = sample['dem'].clone()  # Shape: (1, H, W)
        
        if self.dem_modification == 'original':
            # No modification
            modified_dem = dem_original
            
        elif self.dem_modification == 'constant':
            # Replace with constant plane (mean elevation for this HUC)
            file_path = sample['path']
            huc_code = Path(file_path).parent.name
            
            if huc_code in self.huc_constant_planes:
                constant_value = self.huc_constant_planes[huc_code]
                modified_dem = torch.full_like(dem_original, constant_value)
            else:
                # Fallback: use mean of current patch
                patch_mean = dem_original.mean()
                modified_dem = torch.full_like(dem_original, patch_mean)
                
        elif self.dem_modification == 'smoothed':
            # Apply Gaussian smoothing to reduce topographic detail
            dem_np = dem_original.squeeze().numpy()  # Convert to numpy
            smoothed_dem = ndimage.gaussian_filter(dem_np, sigma=self.gaussian_sigma)
            modified_dem = torch.from_numpy(smoothed_dem).unsqueeze(0)  # Add channel dim back
            
        else:
            raise ValueError(f"Unknown DEM modification: {self.dem_modification}")
        
        # Return modified sample
        return {
            'dem': modified_dem,
            'alphaearth': sample['alphaearth'],
            'hydro_mask': sample['hydro_mask'],
            'flow_dir': sample['flow_dir'],
            'path': sample['path']
        }

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

def load_dem_alphaearth_model(checkpoint_path: str, device: str = "cpu") -> torch.nn.Module:
    """Load DEM+AlphaEarth model from checkpoint"""
    try:
        model = MultimodalMultitaskModel_DEM_AlphaEarth(
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
            
            # Load only matching keys
            model_keys = set(model.state_dict().keys())
            checkpoint_keys = set(model_state_dict.keys())
            matching_keys = model_keys.intersection(checkpoint_keys)
            
            filtered_state_dict = {k: model_state_dict[k] for k in matching_keys}
            model.load_state_dict(filtered_state_dict, strict=False)
        
        model.eval()
        model.to(device)
        
        logger.info(f"Successfully loaded DEM+AlphaEarth model from {checkpoint_path}")
        return model
        
    except Exception as e:
        logger.error(f"Failed to load DEM+AlphaEarth model: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_alphaearth_only_model(checkpoint_path: str, device: str = "cpu") -> torch.nn.Module:
    """Load AlphaEarth-only model from checkpoint"""
    try:
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
            
            # Load only matching keys
            model_keys = set(model.state_dict().keys())
            checkpoint_keys = set(model_state_dict.keys())
            matching_keys = model_keys.intersection(checkpoint_keys)
            
            filtered_state_dict = {k: model_state_dict[k] for k in matching_keys}
            model.load_state_dict(filtered_state_dict, strict=False)
        
        model.eval()
        model.to(device)
        
        logger.info(f"Successfully loaded AlphaEarth-only model from {checkpoint_path}")
        return model
        
    except Exception as e:
        logger.error(f"Failed to load AlphaEarth-only model: {e}")
        import traceback
        traceback.print_exc()
        return None

def compute_metrics(water_pred: torch.Tensor, water_target: torch.Tensor) -> Dict[str, float]:
    """Compute water segmentation metrics (focus on mIoU and Dice for publication)"""
    
    # Ensure correct tensor shapes
    if water_pred.dim() == 4 and water_pred.shape[1] == 1:
        water_pred = water_pred.squeeze(1)  # Remove channel dimension
    
    # Convert logits to probabilities and binary predictions
    water_pred_sigmoid = torch.sigmoid(water_pred)
    water_pred_binary = (water_pred_sigmoid > 0.5).float()
    
    # Dice coefficient (ClDice in publication context)
    intersection = (water_pred_binary * water_target).sum()
    pred_sum = water_pred_binary.sum()
    target_sum = water_target.sum()
    dice = (2.0 * intersection / (pred_sum + target_sum + 1e-8)).item()
    
    # IoU (mIoU for water class)
    intersection = (water_pred_binary * water_target).sum()
    union = ((water_pred_binary + water_target) > 0).float().sum()
    iou = (intersection / (union + 1e-8)).item()
    
    # Additional metrics for completeness
    water_acc = (water_pred_binary == water_target).float().mean().item()
    
    # Convert to numpy for sklearn metrics
    water_pred_np = water_pred_binary.flatten().cpu().numpy().astype(int)
    water_target_np = water_target.flatten().cpu().numpy().astype(int)
    
    try:
        if len(np.unique(water_target_np)) > 1:
            water_precision = precision_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
            water_recall = recall_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
            water_f1 = f1_score(water_target_np, water_pred_np, pos_label=1, zero_division=0.0)
        else:
            water_precision = water_recall = water_f1 = 0.0
    except Exception as e:
        logger.warning(f"Error computing additional metrics: {e}")
        water_precision = water_recall = water_f1 = 0.0
    
    return {
        "water_dice": dice,
        "water_iou": iou,
        "water_accuracy": water_acc,
        "water_precision": water_precision,
        "water_recall": water_recall,
        "water_f1": water_f1
    }

def evaluate_model_variant(model: torch.nn.Module, dataset: Dataset, variant_name: str,
                          device: str = "cpu", limit_batches: int = None) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Evaluate model on dataset variant and return overall + per-HUC metrics"""
    
    dataloader = DataLoader(
        dataset,
        batch_size=8,  # Slightly larger batch for efficiency
        shuffle=False,
        num_workers=0,  # Avoid multiprocessing issues
        pin_memory=False
    )
    
    water_predictions = []
    water_targets = []
    batch_huc_info = []  # Track which HUC each batch belongs to
    
    model.eval()
    with torch.no_grad():
        for batch_idx, batch in enumerate(tqdm(dataloader, desc=f"Evaluating {variant_name}")):
            if limit_batches and batch_idx >= limit_batches:
                break
            
            # Handle different model types
            if 'dem' in batch:
                # DEM+AlphaEarth model: forward(self, dem, alphaearth)
                dem = batch['dem'].to(device)
                alphaearth = batch['alphaearth'].to(device)
                water_pred, _ = model(dem, alphaearth)  # Ignore D8 predictions for this analysis
            else:
                # AlphaEarth-only model: forward(self, alphaearth)
                alphaearth = batch['alphaearth'].to(device)
                water_pred, _ = model(alphaearth)
            
            water_mask = batch['hydro_mask'].to(device)
            
            # Extract HUC info from paths
            huc_codes = []
            for path in batch['path']:
                huc_code = Path(path).parent.name
                huc_codes.append(huc_code)
            
            # Store per-batch info for HUC-level analysis
            batch_huc_info.append({
                'water_pred': water_pred.cpu(),
                'water_target': water_mask.cpu(),
                'huc_codes': huc_codes,
                'batch_size': len(huc_codes)
            })
            
            # Collect predictions and targets
            water_predictions.append(water_pred.cpu())
            water_targets.append(water_mask.cpu())
    
    if not water_predictions:
        logger.warning(f"No predictions collected for {variant_name}")
        return {}, []
    
    # Concatenate all predictions for overall metrics
    water_pred_all = torch.cat(water_predictions, dim=0)
    water_target_all = torch.cat(water_targets, dim=0)
    
    # Compute overall metrics
    overall_metrics = compute_metrics(water_pred_all, water_target_all)
    
    # Compute per-HUC metrics
    huc_metrics = {}
    for batch_info in batch_huc_info:
        for i, huc_code in enumerate(batch_info['huc_codes']):
            if huc_code not in huc_metrics:
                huc_metrics[huc_code] = {
                    'predictions': [],
                    'targets': []
                }
            
            # Extract this sample's predictions and targets
            sample_pred = batch_info['water_pred'][i:i+1]  # Keep batch dimension
            sample_target = batch_info['water_target'][i:i+1]
            
            huc_metrics[huc_code]['predictions'].append(sample_pred)
            huc_metrics[huc_code]['targets'].append(sample_target)
    
    # Compute metrics for each HUC
    per_huc_results = []
    for huc_code, huc_data in huc_metrics.items():
        try:
            # Concatenate all predictions/targets for this HUC
            huc_pred = torch.cat(huc_data['predictions'], dim=0)
            huc_target = torch.cat(huc_data['targets'], dim=0)
            
            # Compute metrics for this HUC
            huc_metric_values = compute_metrics(huc_pred, huc_target)
            
            per_huc_results.append({
                'huc_code': huc_code,
                'variant': variant_name,
                'n_patches': len(huc_data['predictions']),
                **huc_metric_values
            })
            
        except Exception as e:
            logger.warning(f"Error computing metrics for HUC {huc_code}: {e}")
            continue
    
    logger.info(f"{variant_name}: mIoU={overall_metrics['water_iou']:.4f}, Dice={overall_metrics['water_dice']:.4f} (across {len(per_huc_results)} HUCs)")
    
    return overall_metrics, per_huc_results

def create_comparison_plot(results_df: pd.DataFrame, output_path: str):
    """Create publication-quality comparison plot"""
    
    # Prepare data for plotting
    plot_data = []
    variant_order = ['DEM+AlphaEarth', 'SmoothedDEM+AlphaEarth', 'ConstantDEM+AlphaEarth', 'AlphaEarth-only']
    
    for variant in variant_order:
        if variant in results_df['variant'].values:
            variant_data = results_df[results_df['variant'] == variant]
            plot_data.append({
                'Variant': variant,
                'mIoU': variant_data['water_iou'].mean(),
                'Dice': variant_data['water_dice'].mean(),
                'mIoU_std': variant_data['water_iou'].std(),
                'Dice_std': variant_data['water_dice'].std()
            })
    
    plot_df = pd.DataFrame(plot_data)
    
    # Create the plot
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    
    x = np.arange(len(plot_df))
    width = 0.35
    
    # Create bars
    bars1 = ax.bar(x - width/2, plot_df['mIoU'], width, 
                   yerr=plot_df['mIoU_std'], capsize=5,
                   label='mIoU', alpha=0.8, color='skyblue')
    bars2 = ax.bar(x + width/2, plot_df['Dice'], width,
                   yerr=plot_df['Dice_std'], capsize=5, 
                   label='Dice Coefficient', alpha=0.8, color='lightcoral')
    
    # Customize the plot
    ax.set_xlabel('Model Variant', fontsize=12)
    ax.set_ylabel('Performance Score', fontsize=12)
    ax.set_title('DEM Sanity Analysis: Topographic Signal Contribution', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(plot_df['Variant'], rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Add value labels on bars
    def add_value_labels(bars, values, stds):
        for bar, val, std in zip(bars, values, stds):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + std + 0.01,
                   f'{val:.3f}', ha='center', va='bottom', fontsize=10)
    
    add_value_labels(bars1, plot_df['mIoU'], plot_df['mIoU_std'])
    add_value_labels(bars2, plot_df['Dice'], plot_df['Dice_std'])
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Comparison plot saved to: {output_path}")

def main():
    parser = argparse.ArgumentParser(description="DEM Sanity Analysis")
    parser.add_argument("--dem-alphaearth-checkpoint", type=str, required=True,
                       help="Path to DEM+AlphaEarth model checkpoint")
    parser.add_argument("--alphaearth-only-checkpoint", type=str, required=True,
                       help="Path to AlphaEarth-only model checkpoint")
    parser.add_argument("--huc-list", type=str,
                       default="/u/nathanj/national_ml/experiments/evaluation/test_huc_small.txt",
                       help="Path to HUC list file")
    parser.add_argument("--test-data-path", type=str,
                       default="/projects/bcrm/nathanj/data/processed/test/patch_dataset",
                       help="Path to test data directory")
    parser.add_argument("--output-dir", type=str,
                       default="/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity_results",
                       help="Output directory for results")
    parser.add_argument("--gaussian-sigma", type=float, default=3.0,
                       help="Gaussian smoothing sigma for smoothed DEM variant")
    parser.add_argument("--limit-batches", type=int,
                       help="Limit number of batches per variant (for testing)")
    parser.add_argument("--device", type=str, choices=["cpu", "cuda", "auto"], default="auto",
                       help="Device to use for evaluation")
    
    args = parser.parse_args()
    
    # Set device
    device = args.device
    if device == "auto":
        device = "cuda" if torch.cuda.is_available() else "cpu"
    
    logger.info(f"Using device: {device}")
    
    # Load HUC list
    huc_codes = load_huc_list(args.huc_list)
    if not huc_codes:
        logger.error("No HUC codes found")
        return 1
    
    logger.info(f"Analyzing {len(huc_codes)} HUCs")
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load models
    logger.info("Loading models...")
    dem_alphaearth_model = load_dem_alphaearth_model(args.dem_alphaearth_checkpoint, device)
    alphaearth_only_model = load_alphaearth_only_model(args.alphaearth_only_checkpoint, device)
    
    if dem_alphaearth_model is None or alphaearth_only_model is None:
        logger.error("Failed to load required models")
        return 1
    
    # Create base dataset
    base_dataset = MultimodalPatchDataset_DEM_AlphaEarth(
        base_path=args.test_data_path,
        huc_codes=huc_codes,
        alphaearth_channels=64
    )
    
    if len(base_dataset) == 0:
        logger.error("No data found in base dataset")
        return 1
    
    logger.info(f"Base dataset contains {len(base_dataset)} patches")
    
    # Run evaluations for each variant
    results = []
    all_per_huc_results = []
    timestamp = datetime.now().isoformat()
    
    # 1. DEM+AlphaEarth (original baseline)
    logger.info("\n=== Evaluating DEM+AlphaEarth (Original) ===")
    original_dataset = DEMModifiedDataset(base_dataset, dem_modification='original')
    original_metrics, original_per_huc = evaluate_model_variant(
        dem_alphaearth_model, original_dataset, "DEM+AlphaEarth", device, args.limit_batches
    )
    if original_metrics:
        results.append({
            "timestamp": timestamp,
            "variant": "DEM+AlphaEarth",
            "dem_modification": "original",
            "checkpoint_dem_ae": args.dem_alphaearth_checkpoint,
            "checkpoint_ae_only": args.alphaearth_only_checkpoint,
            **original_metrics
        })
        all_per_huc_results.extend(original_per_huc)
    
    # 2. SmoothedDEM+AlphaEarth
    logger.info("\n=== Evaluating SmoothedDEM+AlphaEarth ===")
    smoothed_dataset = DEMModifiedDataset(base_dataset, dem_modification='smoothed', 
                                        gaussian_sigma=args.gaussian_sigma)
    smoothed_metrics, smoothed_per_huc = evaluate_model_variant(
        dem_alphaearth_model, smoothed_dataset, "SmoothedDEM+AlphaEarth", device, args.limit_batches
    )
    if smoothed_metrics:
        results.append({
            "timestamp": timestamp,
            "variant": "SmoothedDEM+AlphaEarth",
            "dem_modification": "smoothed",
            "gaussian_sigma": args.gaussian_sigma,
            "checkpoint_dem_ae": args.dem_alphaearth_checkpoint,
            "checkpoint_ae_only": args.alphaearth_only_checkpoint,
            **smoothed_metrics
        })
        all_per_huc_results.extend(smoothed_per_huc)
    
    # 3. ConstantDEM+AlphaEarth
    logger.info("\n=== Evaluating ConstantDEM+AlphaEarth ===")
    constant_dataset = DEMModifiedDataset(base_dataset, dem_modification='constant')
    constant_metrics, constant_per_huc = evaluate_model_variant(
        dem_alphaearth_model, constant_dataset, "ConstantDEM+AlphaEarth", device, args.limit_batches
    )
    if constant_metrics:
        results.append({
            "timestamp": timestamp,
            "variant": "ConstantDEM+AlphaEarth", 
            "dem_modification": "constant",
            "checkpoint_dem_ae": args.dem_alphaearth_checkpoint,
            "checkpoint_ae_only": args.alphaearth_only_checkpoint,
            **constant_metrics
        })
        all_per_huc_results.extend(constant_per_huc)
    
    # 4. AlphaEarth-only (reference baseline)
    logger.info("\n=== Evaluating AlphaEarth-only (Reference) ===")
    # For AlphaEarth-only model, we only need the alphaearth data
    from data.patchDataLoader_alphaearth_only import MultimodalPatchDataset_AlphaEarth_Only
    ae_only_dataset = MultimodalPatchDataset_AlphaEarth_Only(
        base_path=args.test_data_path,
        huc_codes=huc_codes,
        alphaearth_channels=64
    )
    ae_only_metrics, ae_only_per_huc = evaluate_model_variant(
        alphaearth_only_model, ae_only_dataset, "AlphaEarth-only", device, args.limit_batches
    )
    if ae_only_metrics:
        results.append({
            "timestamp": timestamp,
            "variant": "AlphaEarth-only",
            "dem_modification": "none",
            "checkpoint_dem_ae": args.dem_alphaearth_checkpoint,
            "checkpoint_ae_only": args.alphaearth_only_checkpoint,
            **ae_only_metrics
        })
        all_per_huc_results.extend(ae_only_per_huc)
    
    if not results:
        logger.error("No evaluation results generated")
        return 1
    
    # Save results
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save detailed results
    results_df = pd.DataFrame(results)
    csv_path = output_dir / f"dem_sanity_analysis_{timestamp_str}.csv"
    results_df.to_csv(csv_path, index=False)
    
    json_path = output_dir / f"dem_sanity_analysis_{timestamp_str}.json"
    results_df.to_json(json_path, orient='records', indent=2)
    
    # Save per-HUC results
    if all_per_huc_results:
        per_huc_df = pd.DataFrame(all_per_huc_results)
        per_huc_csv_path = output_dir / f"dem_sanity_per_huc_{timestamp_str}.csv"
        per_huc_df.to_csv(per_huc_csv_path, index=False)
        
        per_huc_json_path = output_dir / f"dem_sanity_per_huc_{timestamp_str}.json"
        per_huc_df.to_json(per_huc_json_path, orient='records', indent=2)
        
        logger.info(f"Per-HUC results saved to: {per_huc_csv_path}")
        
        # Create per-HUC summary statistics
        huc_summary = per_huc_df.groupby('huc_code').agg({
            'water_iou': ['mean', 'std', 'min', 'max'],
            'water_dice': ['mean', 'std', 'min', 'max'],
            'n_patches': 'first'
        }).round(4)
        
        huc_summary.columns = ['_'.join(col).strip() for col in huc_summary.columns.values]
        huc_summary_path = output_dir / f"dem_sanity_huc_summary_{timestamp_str}.csv"
        huc_summary.to_csv(huc_summary_path)
        
        logger.info(f"HUC summary statistics saved to: {huc_summary_path}")
    
    # Create comparison plot
    plot_path = output_dir / f"dem_sanity_comparison_{timestamp_str}.png"
    create_comparison_plot(results_df, str(plot_path))
    
    # Print comprehensive summary
    print("\n" + "="*80)
    print("DEM SANITY ANALYSIS RESULTS")
    print("="*80)
    print("Expected: DEM+AE > SmoothedDEM+AE > ConstantDEM+AE > AE-only")
    print("(If performance drops as expected, DEM provides meaningful topographic signal)")
    print("-"*80)
    
    for _, row in results_df.iterrows():
        variant = row['variant']
        miou = row['water_iou']
        dice = row['water_dice']
        print(f"{variant:25s}: mIoU = {miou:.4f}, Dice = {dice:.4f}")
    
    # Calculate performance drops
    if len(results_df) >= 2:
        print("\n" + "-"*80)
        print("PERFORMANCE DROPS (relative to DEM+AlphaEarth baseline):")
        print("-"*80)
        
        baseline_miou = results_df[results_df['variant'] == 'DEM+AlphaEarth']['water_iou'].iloc[0]
        baseline_dice = results_df[results_df['variant'] == 'DEM+AlphaEarth']['water_dice'].iloc[0]
        
        for _, row in results_df.iterrows():
            if row['variant'] != 'DEM+AlphaEarth':
                variant = row['variant']
                miou_drop = baseline_miou - row['water_iou']
                dice_drop = baseline_dice - row['water_dice']
                miou_pct = (miou_drop / baseline_miou) * 100
                dice_pct = (dice_drop / baseline_dice) * 100
                
                print(f"{variant:25s}: mIoU drop = {miou_drop:+.4f} ({miou_pct:+.1f}%), "
                      f"Dice drop = {dice_drop:+.4f} ({dice_pct:+.1f}%)")
    
    print("\n" + "-"*80)
    print("INTERPRETATION:")
    print("-"*80)
    print("• If ConstantDEM+AE << DEM+AE: DEM elevations provide crucial topographic info")
    print("• If SmoothedDEM+AE < DEM+AE: Fine topographic details matter")
    print("• If all variants >> AE-only: Multi-modal approach is beneficial")
    print("• Large drops = Strong evidence that DEM is not just 'extra channels'")
    
    print(f"\nResults saved to: {csv_path}")
    print(f"Visualization saved to: {plot_path}")
    print("="*80)
    
    return 0

if __name__ == "__main__":
    exit(main())