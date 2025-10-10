#!/usr/bin/env python3
"""
Test the enhanced signed ablation visualization on one patch
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

def test_signed_ablation():
    """Test signed ablation visualization on one patch"""
    
    print("🔬 Testing Enhanced Signed Ablation Visualization")
    print("=" * 55)
    
    # Load one patch
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/03030005')
    patch_file = list(patch_dir.glob('patch_*.npz'))[0]  # Just first patch
    
    print(f"📊 Testing patch: {patch_file.name}")
    
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
    
    # Get baseline prediction
    with torch.no_grad():
        baseline_output, _ = model(dem_tensor, optical_tensor, thermal_tensor, sar_tensor)
        baseline_prediction = torch.sigmoid(baseline_output).squeeze().numpy()
    
    print(f"🎯 Baseline prediction: mean={baseline_prediction.mean():.4f}")
    
    # Ablation study with signed differences
    print("🧪 Running ablation study...")
    
    modality_signed_diffs = {}
    modality_importance = {}
    
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
            print(f"  🔍 Testing {modality_name} removal...")
            
            # Get prediction without this modality
            output1_ablated, _ = model(m1, m2, m3, m4)
            prediction_ablated = torch.sigmoid(output1_ablated).squeeze().numpy()
            
            # Calculate signed difference: full_prediction - ablated_prediction
            signed_diff = baseline_prediction - prediction_ablated
            importance = np.abs(signed_diff)
            
            modality_signed_diffs[modality_name] = signed_diff
            modality_importance[modality_name] = importance
            
            print(f"    Mean signed change: {signed_diff.mean():+.4f}")
            print(f"    Mean |change|: {importance.mean():.4f}")
    
    # Create enhanced visualization
    print("🎨 Creating enhanced visualization...")
    fig, axes = plt.subplots(3, 4, figsize=(16, 12))
    
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
    
    # Row 2: Signed ablation difference maps
    modality_names = ['DEM', 'Optical', 'Thermal', 'SAR']
    ablation_means = {name: importance.mean() for name, importance in modality_importance.items()}
    sorted_ablation = sorted(ablation_means.items(), key=lambda x: x[1], reverse=True)
    
    for idx, modality_name in enumerate(modality_names):
        signed_diff = modality_signed_diffs[modality_name]
        
        # Use symmetric colormap around zero
        vmax = max(0.001, np.abs(signed_diff).max())
        im = axes[1, idx].imshow(signed_diff, cmap='RdBu_r', vmin=-vmax, vmax=vmax)
        
        # Add ranking info
        rank = next((i+1 for i, (name, _) in enumerate(sorted_ablation) if name == modality_name), 0)
        mean_imp = ablation_means[modality_name]
        mean_signed = signed_diff.mean()
        axes[1, idx].set_title(f'{modality_name} Change\n(#{rank}, |Δ|={mean_imp:.4f}, Δ={mean_signed:+.4f})')
        axes[1, idx].axis('off')
        cbar = plt.colorbar(im, ax=axes[1, idx], fraction=0.046, pad=0.04)
        cbar.set_label('Prediction Change', fontsize=8)
    
    # Row 3: Predictions and summary
    im = axes[2, 0].imshow(baseline_prediction, cmap='Blues', vmin=0, vmax=1)
    axes[2, 0].set_title(f'Full Prediction (mean: {baseline_prediction.mean():.3f})')
    axes[2, 0].axis('off')
    plt.colorbar(im, ax=axes[2, 0], fraction=0.046, pad=0.04)
    
    axes[2, 1].imshow(hydro_mask, cmap='Blues')
    axes[2, 1].set_title('Ground Truth')
    axes[2, 1].axis('off')
    
    # Enhanced text summary
    axes[2, 2].text(0.05, 0.95, 'Ablation Rankings:', fontsize=12, fontweight='bold', transform=axes[2, 2].transAxes)
    axes[2, 2].text(0.05, 0.85, '|Change| (Signed Δ):', fontsize=10, fontweight='bold', transform=axes[2, 2].transAxes)
    
    for i, (modality, importance) in enumerate(sorted_ablation):
        signed_mean = modality_signed_diffs[modality].mean()
        color = 'red' if signed_mean > 0 else 'blue'
        axes[2, 2].text(0.05, 0.75 - i*0.12, f'{i+1}. {modality}: {importance:.4f}', 
                       fontsize=10, transform=axes[2, 2].transAxes)
        axes[2, 2].text(0.05, 0.69 - i*0.12, f'    (Δ={signed_mean:+.4f})', 
                       fontsize=9, color=color, transform=axes[2, 2].transAxes)
    
    # Add legend
    axes[2, 2].text(0.05, 0.25, 'Legend:', fontsize=10, fontweight='bold', transform=axes[2, 2].transAxes)
    axes[2, 2].text(0.05, 0.18, 'Red: Removal decreased pred', fontsize=9, color='red', transform=axes[2, 2].transAxes)
    axes[2, 2].text(0.05, 0.12, '(modality was helping)', fontsize=8, color='red', transform=axes[2, 2].transAxes)
    axes[2, 2].text(0.05, 0.05, 'Blue: Removal increased pred', fontsize=9, color='blue', transform=axes[2, 2].transAxes)
    axes[2, 2].text(0.05, -0.01, '(modality was confusing)', fontsize=8, color='blue', transform=axes[2, 2].transAxes)
    axes[2, 2].axis('off')
    
    axes[2, 3].axis('off')
    
    plt.tight_layout()
    
    # Save
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path('/u/nathanj/national_ml/experiments/evaluation/visual_analysis/attention_map/')
    
    output_file = output_dir / f'test_signed_ablation_{huc_id}_{patch_file.stem}_{timestamp}.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"✅ Saved test visualization: {output_file.name}")
    
    # Summary of insights
    print(f"\n🎯 Key Insights:")
    for modality, signed_diff in modality_signed_diffs.items():
        mean_signed = signed_diff.mean()
        if mean_signed > 0.001:
            print(f"  🔴 {modality}: Removal DECREASED predictions (helps water detection)")
        elif mean_signed < -0.001:
            print(f"  🔵 {modality}: Removal INCREASED predictions (confuses water detection)")
        else:
            print(f"  ⚪ {modality}: Minimal impact on predictions")

if __name__ == "__main__":
    test_signed_ablation()