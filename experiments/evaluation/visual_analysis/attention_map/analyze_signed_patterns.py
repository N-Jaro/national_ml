#!/usr/bin/env python3
"""
Analyze signed ablation patterns across multiple patches
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

def analyze_signed_patterns():
    """Analyze signed ablation patterns to understand modality interactions"""
    
    print("🔍 Analyzing Signed Ablation Patterns")
    print("=" * 45)
    
    # Load patches
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/03030005')
    patch_files = list(patch_dir.glob('patch_*.npz'))[:8]
    
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
    
    # Collect signed differences for all patches
    all_results = []
    
    for patch_idx, patch_file in enumerate(patch_files):
        print(f"\n📊 Processing {patch_file.name}...")
        
        # Load and prepare data
        with np.load(patch_file) as data:
            dem = data['dem']
            optical = data['optical'] 
            thermal = data['thermal']
            sar = data['sar']
            hydro_mask = data['hydro_mask']
        
        water_percentage = 100 * np.sum(hydro_mask > 0.5) / hydro_mask.size
        
        # Normalize
        huc_id = patch_file.parent.name
        stats = load_normalization_stats(huc_id)
        dem, optical, thermal, sar = normalize_data(dem, optical, thermal, sar, stats)
        
        # Create tensors
        dem_tensor = torch.FloatTensor(dem[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
        optical_tensor = torch.FloatTensor(optical.transpose(2, 0, 1).astype(np.float32)).unsqueeze(0)
        thermal_tensor = torch.FloatTensor(thermal[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
        sar_tensor = torch.FloatTensor(sar[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
        
        # Get baseline prediction
        with torch.no_grad():
            baseline_output, _ = model(dem_tensor, optical_tensor, thermal_tensor, sar_tensor)
            baseline_prediction = torch.sigmoid(baseline_output).squeeze().numpy()
        
        # Apply threshold (0.33) to get binary predictions
        threshold = 0.33
        baseline_binary = (baseline_prediction > threshold).astype(np.float32)
        
        # Ablation study
        zero_dem = torch.zeros_like(dem_tensor)
        zero_optical = torch.zeros_like(optical_tensor)
        zero_thermal = torch.zeros_like(thermal_tensor)
        zero_sar = torch.zeros_like(sar_tensor)
        
        ablation_tests = {
            'DEM': (zero_dem, optical_tensor, thermal_tensor, sar_tensor),
            'Optical': (dem_tensor, zero_optical, thermal_tensor, sar_tensor),
            'Thermal': (dem_tensor, optical_tensor, zero_thermal, sar_tensor),
            'SAR': (dem_tensor, optical_tensor, thermal_tensor, zero_sar)
        }
        
        signed_diffs = {}
        binary_diffs = {}
        with torch.no_grad():
            for modality_name, (m1, m2, m3, m4) in ablation_tests.items():
                output1_ablated, _ = model(m1, m2, m3, m4)
                prediction_ablated = torch.sigmoid(output1_ablated).squeeze().numpy()
                
                # Raw probability differences
                signed_diff = baseline_prediction - prediction_ablated
                signed_diffs[modality_name] = signed_diff.mean()
                
                # Binary prediction differences (after thresholding)
                binary_ablated = (prediction_ablated > threshold).astype(np.float32)
                binary_diff = baseline_binary - binary_ablated
                binary_diffs[modality_name] = binary_diff.mean()
        
        result = {
            'patch_name': patch_file.stem,
            'water_percentage': water_percentage,
            'baseline_mean': baseline_prediction.mean(),
            'baseline_binary_percent': baseline_binary.mean() * 100,
            'signed_diffs': signed_diffs,
            'binary_diffs': binary_diffs
        }
        all_results.append(result)
        
        # Print quick summary
        print(f"   💧 Water: {water_percentage:.1f}%, Pred: {baseline_prediction.mean():.4f}, Binary: {baseline_binary.mean()*100:.1f}%")
        print(f"   📊 Raw Probability Changes:")
        for modality, diff in signed_diffs.items():
            direction = "↑ HELPS" if diff > 0.001 else "↓ CONFUSES" if diff < -0.001 else "≈ NEUTRAL"
            print(f"     {modality:8}: {diff:+.4f} {direction}")
        print(f"   🎯 Binary Decision Changes (threshold=0.33):")
        for modality, diff in binary_diffs.items():
            direction = "↑ HELPS" if diff > 0.001 else "↓ CONFUSES" if diff < -0.001 else "≈ NEUTRAL"
            print(f"     {modality:8}: {diff:+.4f} {direction}")
    
    # Comprehensive analysis
    print(f"\n{'='*70}")
    print("🎯 COMPREHENSIVE SIGNED ABLATION ANALYSIS")
    print(f"{'='*70}")
    
    print(f"\n📊 Raw Probability Changes Summary:")
    print(f"{'Patch':<10} {'Water%':<8} {'Binary%':<8} {'DEM':<8} {'Optical':<8} {'Thermal':<8} {'SAR':<8}")
    print("-" * 78)
    
    for result in all_results:
        diffs = result['signed_diffs']
        print(f"{result['patch_name']:<10} {result['water_percentage']:<8.1f} {result['baseline_binary_percent']:<8.1f} {diffs['DEM']:+8.4f} {diffs['Optical']:+8.4f} {diffs['Thermal']:+8.4f} {diffs['SAR']:+8.4f}")
    
    print(f"\n🎯 Binary Decision Changes Summary (threshold=0.33):")
    print(f"{'Patch':<10} {'Water%':<8} {'Binary%':<8} {'DEM':<8} {'Optical':<8} {'Thermal':<8} {'SAR':<8}")
    print("-" * 78)
    
    for result in all_results:
        diffs = result['binary_diffs']
        print(f"{result['patch_name']:<10} {result['water_percentage']:<8.1f} {result['baseline_binary_percent']:<8.1f} {diffs['DEM']:+8.4f} {diffs['Optical']:+8.4f} {diffs['Thermal']:+8.4f} {diffs['SAR']:+8.4f}")
    
    # Pattern analysis
    print(f"\n🔍 Pattern Analysis:")
    
    # Average signed differences
    modality_totals = {'DEM': 0, 'Optical': 0, 'Thermal': 0, 'SAR': 0}
    binary_totals = {'DEM': 0, 'Optical': 0, 'Thermal': 0, 'SAR': 0}
    
    for result in all_results:
        for modality, diff in result['signed_diffs'].items():
            modality_totals[modality] += diff
        for modality, diff in result['binary_diffs'].items():
            binary_totals[modality] += diff
    
    n_patches = len(all_results)
    modality_averages = {mod: total/n_patches for mod, total in modality_totals.items()}
    binary_averages = {mod: total/n_patches for mod, total in binary_totals.items()}
    
    print(f"\n📈 Average Raw Probability Effects (across {n_patches} patches):")
    for modality, avg_diff in modality_averages.items():
        if avg_diff > 0.001:
            effect = "🔴 HELPS water detection"
        elif avg_diff < -0.001:
            effect = "🔵 CONFUSES water detection"
        else:
            effect = "⚪ NEUTRAL effect"
        print(f"  {modality:8}: {avg_diff:+.4f} - {effect}")
    
    print(f"\n🎯 Average Binary Decision Effects (threshold=0.33, across {n_patches} patches):")
    for modality, avg_diff in binary_averages.items():
        if avg_diff > 0.001:
            effect = "🔴 HELPS water detection"
        elif avg_diff < -0.001:
            effect = "🔵 CONFUSES water detection"
        else:
            effect = "⚪ NEUTRAL effect"
        print(f"  {modality:8}: {avg_diff:+.4f} - {effect}")
    
    # Water percentage correlation
    print(f"\n🌊 Water Percentage Correlations:")
    water_percentages = [r['water_percentage'] for r in all_results]
    
    for modality in ['DEM', 'Optical', 'Thermal', 'SAR']:
        signed_diffs = [r['signed_diffs'][modality] for r in all_results]
        correlation = np.corrcoef(water_percentages, signed_diffs)[0, 1]
        
        if abs(correlation) > 0.3:
            strength = "STRONG" if abs(correlation) > 0.6 else "MODERATE"
            direction = "positive" if correlation > 0 else "negative"
            print(f"  {modality:8}: {correlation:+.3f} ({strength} {direction} correlation)")
        else:
            print(f"  {modality:8}: {correlation:+.3f} (weak correlation)")
    
    # Consistent patterns
    print(f"\n📋 Raw Probability Consistency Analysis:")
    for modality in ['DEM', 'Optical', 'Thermal', 'SAR']:
        signed_diffs = [r['signed_diffs'][modality] for r in all_results]
        positive_count = sum(1 for diff in signed_diffs if diff > 0.001)
        negative_count = sum(1 for diff in signed_diffs if diff < -0.001)
        neutral_count = n_patches - positive_count - negative_count
        
        print(f"  {modality:8}: {positive_count}/{n_patches} HELPS, {negative_count}/{n_patches} CONFUSES, {neutral_count}/{n_patches} NEUTRAL")
        
        if positive_count >= 0.7 * n_patches:
            consistency = "CONSISTENTLY HELPS"
        elif negative_count >= 0.7 * n_patches:
            consistency = "CONSISTENTLY CONFUSES"
        else:
            consistency = "MIXED EFFECTS"
        print(f"            → {consistency}")
    
    print(f"\n🎯 Binary Decision Consistency Analysis (threshold=0.33):")
    for modality in ['DEM', 'Optical', 'Thermal', 'SAR']:
        binary_diffs = [r['binary_diffs'][modality] for r in all_results]
        positive_count = sum(1 for diff in binary_diffs if diff > 0.001)
        negative_count = sum(1 for diff in binary_diffs if diff < -0.001)
        neutral_count = n_patches - positive_count - negative_count
        
        print(f"  {modality:8}: {positive_count}/{n_patches} HELPS, {negative_count}/{n_patches} CONFUSES, {neutral_count}/{n_patches} NEUTRAL")
        
        if positive_count >= 0.7 * n_patches:
            consistency = "CONSISTENTLY HELPS"
        elif negative_count >= 0.7 * n_patches:
            consistency = "CONSISTENTLY CONFUSES"
        else:
            consistency = "MIXED EFFECTS"
        print(f"            → {consistency}")
    
    print(f"\n✅ Signed ablation pattern analysis complete!")
    
    return all_results

if __name__ == "__main__":
    analyze_signed_patterns()