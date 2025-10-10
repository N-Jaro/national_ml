#!/usr/bin/env python3
"""
Simple modality importance test - just compare predictions
"""

import sys
import os
import numpy as np
import torch
import matplotlib.pyplot as plt
from pathlib import Path
import json

# Add paths to find models
current_dir = Path(__file__).parent
root_dir = current_dir.parent.parent.parent.parent
experiments_dir = root_dir / 'experiments'
sys.path.append(str(experiments_dir))

from models.mdmt_landsat_6b import MultimodalMultitaskModel_ls6b

def _zscore(arr, mean, std, eps=1e-6):
    return (arr - mean) / np.maximum(std, eps)

def load_normalization_stats(huc_id):
    stats_file = f'/u/nathanj/national_ml/data/processed/patch_dataset/{huc_id}/normalization_stats.json'
    if not os.path.exists(stats_file):
        return None
    with open(stats_file, 'r') as f:
        stats = json.load(f)
    return stats

def normalize_data(dem, optical, thermal, sar, stats):
    if stats is None:
        return dem, optical, thermal, sar
    
    # Normalize DEM
    if 'dem' in stats:
        dem_mean = stats['dem']['elevation_mean']
        dem_std = stats['dem']['elevation_stdDev']
        dem = _zscore(dem, dem_mean, dem_std)
    
    # Normalize optical
    if 'optical' in stats and optical is not None:
        bands = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
        for i, band in enumerate(bands):
            if i < optical.shape[2]:
                mean = stats['optical'][f'{band}_mean']
                std = stats['optical'][f'{band}_stdDev']
                optical[:, :, i] = _zscore(optical[:, :, i], mean, std)
    
    # Normalize thermal
    if 'thermal' in stats and thermal is not None:
        thermal_mean = stats['thermal']['ST_B10_mean']
        thermal_std = stats['thermal']['ST_B10_stdDev']
        thermal = _zscore(thermal, thermal_mean, thermal_std)
    
    # Normalize SAR
    if 'sar' in stats and sar is not None:
        sar_mean = stats['sar']['VV_mean']
        sar_std = stats['sar']['VV_stdDev']
        sar = _zscore(sar, sar_mean, sar_std)
    
    return dem, optical, thermal, sar

def simple_modality_test():
    """Simple test - just compare predictions with/without each modality"""
    
    print("🔬 Simple Modality Importance Test")
    print("=" * 40)
    
    # Load patch
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/03030005')
    patch_files = list(patch_dir.glob('patch_*.npz'))[:3]  # Test multiple patches
    
    # Load model
    print("📦 Loading model...")
    model = MultimodalMultitaskModel_ls6b(n_classes_task1=1, n_classes_task2=8)
    checkpoint_path = '/u/nathanj/national_ml/experiments/training/lightning_logs/combineFL_ls6b_20250925_214001_run7/checkpoints/mdmt-epoch=67-val_loss=0.4040.ckpt'
    
    checkpoint = torch.load(checkpoint_path, map_location='cpu', weights_only=False)
    if 'state_dict' in checkpoint:
        state_dict = checkpoint['state_dict']
        cleaned_state_dict = {}
        for key, value in state_dict.items():
            clean_key = key.replace('model.', '') if key.startswith('model.') else key
            cleaned_state_dict[clean_key] = value
        state_dict = cleaned_state_dict
    else:
        state_dict = checkpoint
    
    model.load_state_dict(state_dict, strict=False)
    model.eval()
    print("✅ Model loaded")
    
    for patch_file in patch_files:
        print(f"\n📊 Testing: {patch_file.name}")
        
        # Load and prepare data
        with np.load(patch_file) as data:
            dem = data['dem']
            optical = data['optical'] 
            thermal = data['thermal']
            sar = data['sar']
            hydro_mask = data['hydro_mask']
        
        # Calculate water percentage
        water_percentage = 100 * np.sum(hydro_mask > 0.5) / hydro_mask.size
        print(f"💧 Water percentage: {water_percentage:.1f}%")
        
        # Load normalization and normalize
        huc_id = patch_file.parent.name
        stats = load_normalization_stats(huc_id)
        dem, optical, thermal, sar = normalize_data(dem, optical, thermal, sar, stats)
        
        # Create input tensors
        dem_tensor = torch.FloatTensor(dem[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
        optical_tensor = torch.FloatTensor(optical.transpose(2, 0, 1).astype(np.float32)).unsqueeze(0)
        thermal_tensor = torch.FloatTensor(thermal[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
        sar_tensor = torch.FloatTensor(sar[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
        
        # Zero tensors for ablation
        zero_dem = torch.zeros_like(dem_tensor)
        zero_optical = torch.zeros_like(optical_tensor)
        zero_thermal = torch.zeros_like(thermal_tensor)
        zero_sar = torch.zeros_like(sar_tensor)
        
        with torch.no_grad():
            # Full prediction
            full_output, _ = model(dem_tensor, optical_tensor, thermal_tensor, sar_tensor)
            full_pred = torch.sigmoid(full_output).squeeze().numpy()
            
            # Ablation tests
            no_dem_output, _ = model(zero_dem, optical_tensor, thermal_tensor, sar_tensor)
            no_dem_pred = torch.sigmoid(no_dem_output).squeeze().numpy()
            
            no_optical_output, _ = model(dem_tensor, zero_optical, thermal_tensor, sar_tensor)
            no_optical_pred = torch.sigmoid(no_optical_output).squeeze().numpy()
            
            no_thermal_output, _ = model(dem_tensor, optical_tensor, zero_thermal, sar_tensor)
            no_thermal_pred = torch.sigmoid(no_thermal_output).squeeze().numpy()
            
            no_sar_output, _ = model(dem_tensor, optical_tensor, thermal_tensor, zero_sar)
            no_sar_pred = torch.sigmoid(no_sar_output).squeeze().numpy()
        
        # Calculate importance as absolute difference from full prediction
        dem_importance = np.abs(full_pred - no_dem_pred)
        optical_importance = np.abs(full_pred - no_optical_pred)
        thermal_importance = np.abs(full_pred - no_thermal_pred)
        sar_importance = np.abs(full_pred - no_sar_pred)
        
        # Summary stats
        results = {
            'DEM': {
                'mean_importance': dem_importance.mean(),
                'max_importance': dem_importance.max(),
                'water_pixels_importance': dem_importance[hydro_mask > 0.5].mean() if np.any(hydro_mask > 0.5) else 0
            },
            'Optical': {
                'mean_importance': optical_importance.mean(),
                'max_importance': optical_importance.max(),
                'water_pixels_importance': optical_importance[hydro_mask > 0.5].mean() if np.any(hydro_mask > 0.5) else 0
            },
            'Thermal': {
                'mean_importance': thermal_importance.mean(),
                'max_importance': thermal_importance.max(),
                'water_pixels_importance': thermal_importance[hydro_mask > 0.5].mean() if np.any(hydro_mask > 0.5) else 0
            },
            'SAR': {
                'mean_importance': sar_importance.mean(),
                'max_importance': sar_importance.max(),
                'water_pixels_importance': sar_importance[hydro_mask > 0.5].mean() if np.any(hydro_mask > 0.5) else 0
            }
        }
        
        print("  📊 Modality Importance Results:")
        print("     (Higher = More Important)")
        
        # Sort by mean importance
        sorted_results = sorted(results.items(), key=lambda x: x[1]['mean_importance'], reverse=True)
        
        for i, (modality, stats) in enumerate(sorted_results, 1):
            print(f"     {i}. {modality:8}: mean={stats['mean_importance']:.4f}, max={stats['max_importance']:.4f}, water_pixels={stats['water_pixels_importance']:.4f}")
        
        print(f"     Baseline prediction: mean={full_pred.mean():.4f}, max={full_pred.max():.4f}")

if __name__ == "__main__":
    simple_modality_test()