#!/usr/bin/env python3
"""
Debug modality attention calculation to understand why optical shows 0% contribution
"""

import sys
import os
import numpy as np
import torch
import torch.nn.functional as F
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
    """Z-score normalization using per-HUC statistics."""
    return (arr - mean) / np.maximum(std, eps)

def load_normalization_stats(huc_id):
    """Load normalization statistics for a given HUC."""
    stats_file = f'/u/nathanj/national_ml/data/processed/patch_dataset/{huc_id}/normalization_stats.json'
    
    if not os.path.exists(stats_file):
        print(f"⚠️  No normalization stats found at {stats_file}")
        return None
    
    with open(stats_file, 'r') as f:
        stats = json.load(f)
    
    return stats

def normalize_data(dem, optical, thermal, sar, stats):
    """Normalize data using HUC-specific statistics."""
    if stats is None:
        print("⚠️  Warning: Using raw data (no normalization stats)")
        return dem, optical, thermal, sar
    
    # Normalize DEM
    if 'dem' in stats:
        dem_mean = stats['dem']['elevation_mean']
        dem_std = stats['dem']['elevation_stdDev']
        dem = _zscore(dem, dem_mean, dem_std)
        print(f"DEM normalized: [{dem.min():.3f}, {dem.max():.3f}]")
    
    # Normalize optical (6 bands: B2, B3, B4, B5, B6, B7)
    if 'optical' in stats and optical is not None:
        bands = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
        for i, band in enumerate(bands):
            if i < optical.shape[2]:  # Check if band exists
                mean = stats['optical'][f'{band}_mean']
                std = stats['optical'][f'{band}_stdDev']
                before_range = [optical[:, :, i].min(), optical[:, :, i].max()]
                optical[:, :, i] = _zscore(optical[:, :, i], mean, std)
                after_range = [optical[:, :, i].min(), optical[:, :, i].max()]
                print(f"Optical {band} normalized: {before_range} -> {after_range}")
    
    # Normalize thermal
    if 'thermal' in stats and thermal is not None:
        thermal_mean = stats['thermal']['ST_B10_mean']
        thermal_std = stats['thermal']['ST_B10_stdDev']
        thermal = _zscore(thermal, thermal_mean, thermal_std)
        print(f"Thermal normalized: [{thermal.min():.3f}, {thermal.max():.3f}]")
    
    # Normalize SAR
    if 'sar' in stats and sar is not None:
        sar_mean = stats['sar']['VV_mean']
        sar_std = stats['sar']['VV_stdDev']
        sar = _zscore(sar, sar_mean, sar_std)
        print(f"SAR normalized: [{sar.min():.3f}, {sar.max():.3f}]")
    
    return dem, optical, thermal, sar

def debug_modality_forward(model, input_tensor, modality_mask, modality_name):
    """Debug what happens during modality-specific forward pass"""
    print(f"\n🔍 Debugging {modality_name} forward pass:")
    
    # Apply modality mask
    masked_tensor = input_tensor.clone() * modality_mask
    
    print(f"  Original input range: [{input_tensor.min():.3f}, {input_tensor.max():.3f}]")
    print(f"  Mask shape: {modality_mask.shape}")
    print(f"  Mask non-zero elements: {torch.count_nonzero(modality_mask)}")
    print(f"  Masked input range: [{masked_tensor.min():.3f}, {masked_tensor.max():.3f}]")
    
    # Split into modalities
    m1 = masked_tensor[:, :1, :, :]   # DEM
    m2 = masked_tensor[:, 1:7, :, :]  # Optical
    m3 = masked_tensor[:, 7:8, :, :]  # Thermal
    m4 = masked_tensor[:, 8:9, :, :]  # SAR
    
    print(f"  m1 (DEM) range: [{m1.min():.3f}, {m1.max():.3f}], non-zero: {torch.count_nonzero(m1)}")
    print(f"  m2 (Optical) range: [{m2.min():.3f}, {m2.max():.3f}], non-zero: {torch.count_nonzero(m2)}")
    print(f"  m3 (Thermal) range: [{m3.min():.3f}, {m3.max():.3f}], non-zero: {torch.count_nonzero(m3)}")
    print(f"  m4 (SAR) range: [{m4.min():.3f}, {m4.max():.3f}], non-zero: {torch.count_nonzero(m4)}")
    
    # Try forward pass
    with torch.no_grad():
        try:
            output1, output2 = model(m1, m2, m3, m4)
            print(f"  ✅ Forward pass successful")
            print(f"  Output1 range: [{output1.min():.3f}, {output1.max():.3f}]")
            print(f"  Output1 mean: {output1.mean():.3f}")
            return True, output1.mean().item()
        except Exception as e:
            print(f"  ❌ Forward pass failed: {e}")
            return False, 0.0

def debug_modality_attention():
    """Debug modality attention calculation"""
    
    print("🐛 Debug: Modality Attention Investigation")
    print("=" * 50)
    
    # Load test patch
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/03030005')
    patch_files = list(patch_dir.glob('patch_*.npz'))[:1]
    patch_file = patch_files[0]
    
    # Load and prepare data
    with np.load(patch_file) as data:
        dem = data['dem']
        optical = data['optical'] 
        thermal = data['thermal']
        sar = data['sar']
    
    # Load normalization and normalize
    huc_id = patch_file.parent.name
    stats = load_normalization_stats(huc_id)
    dem, optical, thermal, sar = normalize_data(dem, optical, thermal, sar, stats)
    
    # Create input tensor
    dem_tensor = dem[np.newaxis, :, :].astype(np.float32)
    optical_tensor = optical.transpose(2, 0, 1).astype(np.float32)
    thermal_tensor = thermal[np.newaxis, :, :].astype(np.float32)
    sar_tensor = sar[np.newaxis, :, :].astype(np.float32)
    
    input_data = np.concatenate([dem_tensor, optical_tensor, thermal_tensor, sar_tensor], axis=0)
    input_tensor = torch.FloatTensor(input_data).unsqueeze(0)
    
    print(f"Final input tensor shape: {input_tensor.shape}")
    print(f"Final input tensor range: [{input_tensor.min():.3f}, {input_tensor.max():.3f}]")
    
    # Load model
    model = MultimodalMultitaskModel_ls6b(n_classes_task1=1, n_classes_task2=8)
    checkpoint_path = '/u/nathanj/national_ml/experiments/training/lightning_logs/combineFL_ls6b_20250925_214001_run7/checkpoints/mdmt-epoch=67-val_loss=0.4040.ckpt'
    
    checkpoint = torch.load(checkpoint_path, map_location='cpu')
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
    
    # Test normal forward pass first
    print(f"\n🧪 Testing normal forward pass (all modalities):")
    with torch.no_grad():
        m1 = input_tensor[:, :1, :, :]
        m2 = input_tensor[:, 1:7, :, :]
        m3 = input_tensor[:, 7:8, :, :]
        m4 = input_tensor[:, 8:9, :, :]
        output1, output2 = model(m1, m2, m3, m4)
        print(f"  ✅ Normal forward pass successful")
        print(f"  Output1 range: [{output1.min():.3f}, {output1.max():.3f}]")
        print(f"  Output1 mean: {output1.mean():.3f}")
    
    # Create modality masks
    modality_masks = {
        'DEM': torch.zeros_like(input_tensor),
        'Optical': torch.zeros_like(input_tensor),
        'Thermal': torch.zeros_like(input_tensor),
        'SAR': torch.zeros_like(input_tensor)
    }
    modality_masks['DEM'][:, :1, :, :] = 1.0
    modality_masks['Optical'][:, 1:7, :, :] = 1.0
    modality_masks['Thermal'][:, 7:8, :, :] = 1.0
    modality_masks['SAR'][:, 8:9, :, :] = 1.0
    
    # Test each modality mask
    forward_results = {}
    for modality_name, mask in modality_masks.items():
        success, mean_output = debug_modality_forward(model, input_tensor, mask, modality_name)
        forward_results[modality_name] = (success, mean_output)
    
    print(f"\n📊 Forward Pass Results Summary:")
    for modality_name, (success, mean_output) in forward_results.items():
        status = "✅" if success else "❌"
        print(f"  {status} {modality_name}: mean_output = {mean_output:.6f}")

if __name__ == "__main__":
    debug_modality_attention()