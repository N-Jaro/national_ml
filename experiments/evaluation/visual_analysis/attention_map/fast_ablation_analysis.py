#!/usr/bin/env python3
"""
Fast multi-patch ablation study visualization
"""

import sys
import os
import numpy as np
import torch
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
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

def get_fast_ablation_importance(model, dem_tensor, optical_tensor, thermal_tensor, sar_tensor, baseline_prediction):
    """
    Fast ablation study - return both magnitude and signed differences
    """
    modality_importance = {}
    modality_signed_diffs = {}
    
    # Create zero tensors for ablation
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
    
    with torch.no_grad():
        for modality_name, (m1, m2, m3, m4) in ablation_tests.items():
            # Get prediction without this modality
            output1_ablated, _ = model(m1, m2, m3, m4)
            prediction_ablated = torch.sigmoid(output1_ablated).squeeze().numpy()
            
            # Calculate signed difference: full_prediction - ablated_prediction
            # Positive = removing modality decreased prediction (modality was helping)
            # Negative = removing modality increased prediction (modality was hurting)
            signed_diff = baseline_prediction - prediction_ablated
            
            # Calculate importance as absolute difference
            importance = np.abs(signed_diff)
            
            modality_importance[modality_name] = importance
            modality_signed_diffs[modality_name] = signed_diff
    
    return modality_importance, modality_signed_diffs

def get_binary_ablation_diffs(model, dem_tensor, optical_tensor, thermal_tensor, sar_tensor, baseline_binary, threshold):
    """
    Get binary decision differences after thresholding
    """
    modality_binary_diffs = {}
    
    # Create zero tensors for ablation
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
    
    with torch.no_grad():
        for modality_name, (m1, m2, m3, m4) in ablation_tests.items():
            # Get prediction without this modality
            output1_ablated, _ = model(m1, m2, m3, m4)
            prediction_ablated = torch.sigmoid(output1_ablated).squeeze().numpy()
            
            # Apply threshold to get binary prediction
            binary_ablated = (prediction_ablated > threshold).astype(np.float32)
            
            # Calculate binary difference: baseline_binary - ablated_binary
            binary_diff = baseline_binary - binary_ablated
            modality_binary_diffs[modality_name] = binary_diff
    
    return modality_binary_diffs

def analyze_patch_fast(model, patch_file, patch_idx):
    """Fast analysis of a single patch"""
    
    print(f"📊 Patch {patch_idx}: {patch_file.name}")
    
    # Load and prepare data
    with np.load(patch_file) as data:
        dem = data['dem']
        optical = data['optical'] 
        thermal = data['thermal']
        sar = data['sar']
        hydro_mask = data['hydro_mask']
    
    # Calculate water percentage
    water_percentage = 100 * np.sum(hydro_mask > 0.5) / hydro_mask.size
    print(f"   💧 Water: {water_percentage:.1f}%")
    
    # Load normalization and normalize
    huc_id = patch_file.parent.name
    stats = load_normalization_stats(huc_id)
    dem, optical, thermal, sar = normalize_data(dem, optical, thermal, sar, stats)
    
    # Create input tensors
    dem_tensor = torch.FloatTensor(dem[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
    optical_tensor = torch.FloatTensor(optical.transpose(2, 0, 1).astype(np.float32)).unsqueeze(0)
    thermal_tensor = torch.FloatTensor(thermal[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
    sar_tensor = torch.FloatTensor(sar[np.newaxis, :, :].astype(np.float32)).unsqueeze(0)
    
    # Get baseline prediction
    with torch.no_grad():
        baseline_output, _ = model(dem_tensor, optical_tensor, thermal_tensor, sar_tensor)
        baseline_prediction = torch.sigmoid(baseline_output).squeeze().numpy()
    
    print(f"   🎯 Prediction: mean={baseline_prediction.mean():.4f}")
    
    # Fast ablation study with binary analysis
    ablation_importance, ablation_signed_diffs = get_fast_ablation_importance(
        model, dem_tensor, optical_tensor, thermal_tensor, sar_tensor, baseline_prediction
    )
    
    # Also get binary differences (threshold = 0.33)
    threshold = 0.33
    baseline_binary = (baseline_prediction > threshold).astype(np.float32)
    ablation_binary_diffs = get_binary_ablation_diffs(
        model, dem_tensor, optical_tensor, thermal_tensor, sar_tensor, baseline_binary, threshold
    )
    
    # Create enhanced visualization with binary analysis
    fig, axes = plt.subplots(4, 4, figsize=(16, 16))
    
    # Row 1: Input modalities
    axes[0, 0].imshow(dem, cmap='terrain')
    axes[0, 0].set_title('DEM Input')
    axes[0, 0].axis('off')
    
    # RGB optical
    if optical.shape[2] >= 3:
        red_band = optical[:, :, 2]
        green_band = optical[:, :, 1] 
        blue_band = optical[:, :, 0]
        rgb_optical = np.stack([red_band, green_band, blue_band], axis=2)
        for i in range(3):
            band = rgb_optical[:, :, i]
            p2, p98 = np.percentile(band, [2, 98])
            band_clipped = np.clip(band, p2, p98)
            if p98 > p2:
                rgb_optical[:, :, i] = (band_clipped - p2) / (p98 - p2)
            else:
                rgb_optical[:, :, i] = 0.5
        rgb_optical = np.clip(rgb_optical, 0, 1)
        axes[0, 1].imshow(rgb_optical)
        axes[0, 1].set_title(f'Optical RGB - Water: {water_percentage:.1f}%')
        axes[0, 1].axis('off')
    
    axes[0, 2].imshow(thermal, cmap='plasma')
    axes[0, 2].set_title('Thermal Input')
    axes[0, 2].axis('off')
    
    axes[0, 3].imshow(sar, cmap='gray')
    axes[0, 3].set_title('SAR Input')
    axes[0, 3].axis('off')
    
        # Row 2: Raw probability signed differences
    modality_names = ['DEM', 'Optical', 'Thermal', 'SAR']
    ablation_means = {name: importance.mean() for name, importance in ablation_importance.items()}
    sorted_ablation = sorted(ablation_means.items(), key=lambda x: x[1], reverse=True)
    
    for idx, modality_name in enumerate(modality_names):
        # Get the signed difference (full - ablated) to show direction of change
        signed_diff = ablation_signed_diffs[modality_name]
        
        # Use symmetric colormap around zero
        vmax = max(0.001, np.abs(signed_diff).max())
        im = axes[1, idx].imshow(signed_diff, cmap='RdBu_r', vmin=-vmax, vmax=vmax)
        
        # Add ranking info
        rank = next((i+1 for i, (name, _) in enumerate(sorted_ablation) if name == modality_name), 0)
        mean_signed = signed_diff.mean()
        axes[1, idx].set_title(f'{modality_name} Raw Prob Change\n(#{rank}, Δ={mean_signed:+.4f})')
        axes[1, idx].axis('off')
        cbar = plt.colorbar(im, ax=axes[1, idx], fraction=0.046, pad=0.04)
        cbar.set_label('Probability Change', fontsize=8)
    
    # Row 3: Binary decision differences (threshold = 0.33)
    for idx, modality_name in enumerate(modality_names):
        binary_diff = ablation_binary_diffs[modality_name]
        
        # Use symmetric colormap around zero
        vmax = max(0.001, np.abs(binary_diff).max())
        im = axes[2, idx].imshow(binary_diff, cmap='RdBu_r', vmin=-vmax, vmax=vmax)
        
        mean_binary = binary_diff.mean()
        axes[2, idx].set_title(f'{modality_name} Binary Change\n(thresh=0.33, Δ={mean_binary:+.4f})')
        axes[2, idx].axis('off')
        cbar = plt.colorbar(im, ax=axes[2, idx], fraction=0.046, pad=0.04)
        cbar.set_label('Binary Change', fontsize=8)
    
        # Row 4: Predictions, ground truth, and summary
    im = axes[3, 0].imshow(baseline_prediction, cmap='Blues', vmin=0, vmax=1)
    axes[3, 0].set_title(f'Raw Prediction (mean: {baseline_prediction.mean():.3f})')
    axes[3, 0].axis('off')
    plt.colorbar(im, ax=axes[3, 0], fraction=0.046, pad=0.04)
    
    # Binary prediction
    binary_pred_display = (baseline_prediction > threshold).astype(np.float32)
    axes[3, 1].imshow(binary_pred_display, cmap='Blues', vmin=0, vmax=1)
    axes[3, 1].set_title(f'Binary Pred (thresh=0.33, {binary_pred_display.mean()*100:.1f}%)')
    axes[3, 1].axis('off')
    
    axes[3, 2].imshow(hydro_mask, cmap='Blues')
    axes[3, 2].set_title('Ground Truth')
    axes[3, 2].axis('off')
    
    # Enhanced text summary
    axes[3, 3].text(0.05, 0.95, 'Ablation Summary:', fontsize=12, fontweight='bold', transform=axes[3, 3].transAxes)
    axes[3, 3].text(0.05, 0.85, 'Raw Prob Changes:', fontsize=10, fontweight='bold', transform=axes[3, 3].transAxes)
    
    for i, (modality, importance) in enumerate(sorted_ablation):
        signed_mean = ablation_signed_diffs[modality].mean()
        binary_mean = ablation_binary_diffs[modality].mean()
        color = 'red' if signed_mean > 0 else 'blue'
        axes[3, 3].text(0.05, 0.77 - i*0.15, f'{i+1}. {modality}: {signed_mean:+.4f}', 
                       fontsize=9, color=color, transform=axes[3, 3].transAxes)
        # Add binary change
        binary_color = 'red' if binary_mean > 0 else 'blue'
        axes[3, 3].text(0.05, 0.71 - i*0.15, f'    Binary: {binary_mean:+.4f}', 
                       fontsize=8, color=binary_color, transform=axes[3, 3].transAxes)
    
    # Add legend
    axes[3, 3].text(0.05, 0.15, 'Legend:', fontsize=10, fontweight='bold', transform=axes[3, 3].transAxes)
    axes[3, 3].text(0.05, 0.08, 'Red: Helps detection', fontsize=8, color='red', transform=axes[3, 3].transAxes)
    axes[3, 3].text(0.05, 0.02, 'Blue: Confuses detection', fontsize=8, color='blue', transform=axes[3, 3].transAxes)
    axes[3, 3].axis('off')
    
    plt.tight_layout()
    
    # Save
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path('/u/nathanj/national_ml/experiments/evaluation/visual_analysis/attention_map/fast_ablation_viz')
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / f'enhanced_signed_binary_ablation_{huc_id}_{patch_file.stem}_water{water_percentage:.1f}pct.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"   ✅ Saved: {output_file.name}")
    
    # Rankings
    print(f"   📊 Rankings: ", end="")
    for i, (modality, importance) in enumerate(sorted_ablation):
        print(f"{i+1}.{modality}({importance:.3f})", end=" " if i < len(sorted_ablation)-1 else "\n")
    
    return {
        'patch_name': patch_file.stem,
        'water_percentage': water_percentage,
        'ablation_results': ablation_means,
        'baseline_prediction_mean': baseline_prediction.mean(),
        'rankings': [modality for modality, _ in sorted_ablation]
    }

def fast_multi_patch_ablation():
    """Fast multi-patch ablation analysis"""
    
    print("🚀 Fast Multi-Patch Ablation Analysis")
    print("=" * 50)
    
    # Load patches
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/03030005')
    patch_files = list(patch_dir.glob('patch_*.npz'))[:8]  # Analyze first 8 patches
    
    if len(patch_files) == 0:
        print("❌ No patch files found!")
        return
    
    print(f"📊 Found {len(patch_files)} patches to analyze")
    
    # Load model once
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
    
    # Process patches
    print(f"\n🔬 Processing patches...")
    all_results = []
    
    for patch_idx, patch_file in enumerate(patch_files):
        try:
            result = analyze_patch_fast(model, patch_file, patch_idx)
            all_results.append(result)
        except Exception as e:
            print(f"   ❌ Error processing {patch_file.name}: {e}")
            continue
    
    # Summary
    print(f"\n{'='*70}")
    print("📊 FAST ABLATION SUMMARY")
    print(f"{'='*70}")
    
    # Summary table
    print(f"\n📈 Results Summary:")
    print(f"{'Patch':<10} {'Water%':<8} {'#1':<10} {'#2':<10} {'#3':<10} {'#4':<10}")
    print("-" * 70)
    
    for result in all_results:
        rankings = result['rankings']
        print(f"{result['patch_name']:<10} {result['water_percentage']:<8.1f} {rankings[0]:<10} {rankings[1]:<10} {rankings[2]:<10} {rankings[3]:<10}")
    
    # Count rankings
    print(f"\n🏆 Modality Ranking Frequency (#1 positions):")
    ranking_counts = {'DEM': 0, 'Optical': 0, 'Thermal': 0, 'SAR': 0}
    
    for result in all_results:
        first_place = result['rankings'][0]
        ranking_counts[first_place] += 1
    
    sorted_ranking_counts = sorted(ranking_counts.items(), key=lambda x: x[1], reverse=True)
    total_patches = len(all_results)
    
    for modality, count in sorted_ranking_counts:
        percentage = (count / total_patches) * 100 if total_patches > 0 else 0
        print(f"  {modality:8}: {count}/{total_patches} patches ({percentage:.1f}%)")
    
    # Average importance
    print(f"\n📊 Average Ablation Importance:")
    modality_totals = {'DEM': 0, 'Optical': 0, 'Thermal': 0, 'SAR': 0}
    
    for result in all_results:
        for modality, importance in result['ablation_results'].items():
            modality_totals[modality] += importance
    
    if all_results:
        modality_averages = {mod: modality_totals[mod]/len(all_results) for mod in modality_totals}
        sorted_averages = sorted(modality_averages.items(), key=lambda x: x[1], reverse=True)
        
        for i, (modality, avg_importance) in enumerate(sorted_averages, 1):
            print(f"  {i}. {modality:8}: {avg_importance:.4f}")
    
    print(f"\n✅ Fast ablation analysis complete!")
    print(f"📁 Visualizations saved to: fast_ablation_viz/")
    
    return all_results

if __name__ == "__main__":
    fast_multi_patch_ablation()