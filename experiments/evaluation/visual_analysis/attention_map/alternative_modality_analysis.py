#!/usr/bin/env python3
"""
Alternative modality analysis using ablation studies and earlier encoder layers
"""

import sys
import os
import numpy as np
import torch
import torch.nn.functional as F
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

def get_ablation_importance(model, input_tensor, baseline_prediction):
    """
    Get modality importance using ablation study - removing each modality and seeing prediction change
    """
    modality_importance = {}
    
    # Create zero tensors for ablation
    zero_dem = torch.zeros_like(input_tensor[:, :1, :, :])
    zero_optical = torch.zeros_like(input_tensor[:, 1:7, :, :])
    zero_thermal = torch.zeros_like(input_tensor[:, 7:8, :, :])
    zero_sar = torch.zeros_like(input_tensor[:, 8:9, :, :])
    
    ablation_tests = {
        'DEM': (zero_dem, input_tensor[:, 1:7, :, :], input_tensor[:, 7:8, :, :], input_tensor[:, 8:9, :, :]),
        'Optical': (input_tensor[:, :1, :, :], zero_optical, input_tensor[:, 7:8, :, :], input_tensor[:, 8:9, :, :]),
        'Thermal': (input_tensor[:, :1, :, :], input_tensor[:, 1:7, :, :], zero_thermal, input_tensor[:, 8:9, :, :]),
        'SAR': (input_tensor[:, :1, :, :], input_tensor[:, 1:7, :, :], input_tensor[:, 7:8, :, :], zero_sar)
    }
    
    with torch.no_grad():
        for modality_name, (m1, m2, m3, m4) in ablation_tests.items():
            # Get prediction without this modality
            output1_ablated, _ = model(m1, m2, m3, m4)
            prediction_ablated = torch.sigmoid(output1_ablated).squeeze().numpy()
            
            # Calculate importance as difference from baseline
            importance = np.abs(baseline_prediction - prediction_ablated)
            modality_importance[modality_name] = importance
            
            # Stats
            mean_importance = importance.mean()
            max_importance = importance.max()
            print(f"  🔍 {modality_name}: mean_diff={mean_importance:.4f}, max_diff={max_importance:.4f}")
    
    return modality_importance

def get_input_gradients(model, input_tensor):
    """
    Get gradients with respect to input (input * gradient)
    """
    input_tensor.requires_grad_(True)
    
    m1 = input_tensor[:, :1, :, :]
    m2 = input_tensor[:, 1:7, :, :]
    m3 = input_tensor[:, 7:8, :, :]
    m4 = input_tensor[:, 8:9, :, :]
    
    output1, _ = model(m1, m2, m3, m4)
    
    # Use output mean as target
    target = output1.mean()
    target.backward()
    
    # Get input gradients
    input_gradients = input_tensor.grad.detach().clone()
    input_tensor.requires_grad_(False)
    
    # Calculate input * gradient (integrated gradients approximation)
    saliency = input_tensor.squeeze() * input_gradients.squeeze()
    
    # Split back into modalities
    modality_saliency = {
        'DEM': saliency[0],
        'Optical': saliency[1:7].mean(dim=0),  # Average across optical bands
        'Thermal': saliency[7],
        'SAR': saliency[8]
    }
    
    return modality_saliency

def alternative_modality_analysis():
    """Alternative modality analysis using ablation and input gradients"""
    
    print("🔬 Alternative Modality Analysis - Ablation + Input Gradients")
    print("=" * 65)
    
    # Load multiple patches for detailed analysis
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/03030005')
    patch_files = list(patch_dir.glob('patch_*.npz'))[:5]  # Analyze first 5 patches
    
    if len(patch_files) == 0:
        print("❌ No patch files found!")
        return
    
    print(f"📊 Found {len(patch_files)} patches to analyze")
    
    # Load model once
    print("📦 Loading model...")
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
    print("✅ Model loaded")
    
    # Process each patch and collect results
    all_results = []
    for patch_idx, patch_file in enumerate(patch_files):
        print(f"\n{'='*60}")
        print(f"📊 Analyzing patch {patch_idx}: {patch_file.name}")
        print(f"{'='*60}")
        
        result = analyze_single_patch(model, patch_file, patch_idx)
        all_results.append(result)
    
    # Create summary comparison
    print(f"\n{'='*80}")
    print("🎯 MULTI-PATCH ABLATION SUMMARY")
    print(f"{'='*80}")
    
    # Summary table
    print(f"\n📊 Ablation Results Summary Table:")
    print(f"{'Patch':<8} {'Water%':<8} {'#1 Most Important':<20} {'#2':<12} {'#3':<12} {'#4 Least':<12}")
    print("-" * 80)
    
    for result in all_results:
        sorted_ablation = sorted(result['ablation_results'].items(), key=lambda x: x[1], reverse=True)
        rankings = [f"{mod}({score:.3f})" for mod, score in sorted_ablation]
        print(f"{result['patch_name']:<8} {result['water_percentage']:<8.1f} {rankings[0]:<20} {rankings[1]:<12} {rankings[2]:<12} {rankings[3]:<12}")
    
    # Average rankings
    print(f"\n📈 Average Modality Importance Across All Patches:")
    modality_totals = {'DEM': 0, 'Optical': 0, 'Thermal': 0, 'SAR': 0}
    modality_counts = {'DEM': 0, 'Optical': 0, 'Thermal': 0, 'SAR': 0}
    
    for result in all_results:
        for modality, importance in result['ablation_results'].items():
            modality_totals[modality] += importance
            modality_counts[modality] += 1
    
    modality_averages = {mod: modality_totals[mod]/modality_counts[mod] for mod in modality_totals}
    sorted_averages = sorted(modality_averages.items(), key=lambda x: x[1], reverse=True)
    
    for i, (modality, avg_importance) in enumerate(sorted_averages, 1):
        print(f"  {i}. {modality:8}: {avg_importance:.4f} (average across {modality_counts[modality]} patches)")
    
    # Water percentage correlation
    print(f"\n🌊 Water Percentage vs Modality Patterns:")
    water_percentages = [r['water_percentage'] for r in all_results]
    print(f"  Water range: {min(water_percentages):.1f}% - {max(water_percentages):.1f}%")
    
    # Find patterns
    high_water_patches = [r for r in all_results if r['water_percentage'] > 5.0]
    low_water_patches = [r for r in all_results if r['water_percentage'] <= 5.0]
    
    if high_water_patches and low_water_patches:
        print(f"  High water patches (>{5.0}%): {len(high_water_patches)}")
        print(f"  Low water patches (≤{5.0}%): {len(low_water_patches)}")
        
        # Compare modality importance between high/low water
        for modality in ['DEM', 'Optical', 'Thermal', 'SAR']:
            high_water_avg = np.mean([r['ablation_results'][modality] for r in high_water_patches])
            low_water_avg = np.mean([r['ablation_results'][modality] for r in low_water_patches])
            print(f"    {modality:8}: High water={high_water_avg:.4f}, Low water={low_water_avg:.4f}")
    
    print(f"\n✅ Multi-patch ablation analysis complete!")
    print(f"📁 Visualizations saved to: ablation_visualizations/")
    
    return all_results

def analyze_single_patch(model, patch_file, patch_idx):
    
    """Analyze a single patch with ablation and input gradients"""
    
    print(f"📊 Analyzing patch: {patch_file.name}")
    
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
    
    # Create input tensor
    dem_tensor = dem[np.newaxis, :, :].astype(np.float32)
    optical_tensor = optical.transpose(2, 0, 1).astype(np.float32)
    thermal_tensor = thermal[np.newaxis, :, :].astype(np.float32)
    sar_tensor = sar[np.newaxis, :, :].astype(np.float32)
    
    input_data = np.concatenate([dem_tensor, optical_tensor, thermal_tensor, sar_tensor], axis=0)
    input_tensor = torch.FloatTensor(input_data).unsqueeze(0)
    
    # Get baseline prediction
    with torch.no_grad():
        m1 = input_tensor[:, :1, :, :]
        m2 = input_tensor[:, 1:7, :, :]
        m3 = input_tensor[:, 7:8, :, :]
        m4 = input_tensor[:, 8:9, :, :]
        baseline_output, _ = model(m1, m2, m3, m4)
        baseline_prediction = torch.sigmoid(baseline_output).squeeze().numpy()
    
    print(f"🎯 Baseline prediction: mean={baseline_prediction.mean():.4f}, max={baseline_prediction.max():.4f}")
    
    # Method 1: Ablation study
    print(f"\n🧪 Method 1: Ablation Study (Remove Each Modality)")
    print("-" * 50)
    ablation_importance = get_ablation_importance(model, input_tensor, baseline_prediction)
    
    # Method 2: Input gradients
    print(f"\n🧪 Method 2: Input Gradients (Input × Gradient)")
    print("-" * 50)
    input_saliency = get_input_gradients(model, input_tensor)
    
    for modality_name, saliency in input_saliency.items():
        saliency_magnitude = np.abs(saliency.numpy())
        mean_sal = saliency_magnitude.mean()
        max_sal = saliency_magnitude.max()
        print(f"  🔍 {modality_name}: mean_saliency={mean_sal:.4f}, max_saliency={max_sal:.4f}")
    
    # Create visualization
    print(f"\n🎨 Creating comparative visualization...")
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
    
    # Row 2: Ablation importance
    modality_names = ['DEM', 'Optical', 'Thermal', 'SAR']
    for idx, modality_name in enumerate(modality_names):
        importance = ablation_importance[modality_name]
        im = axes[1, idx].imshow(importance, cmap='Reds', vmin=0, vmax=importance.max())
        axes[1, idx].set_title(f'{modality_name} Ablation Importance')
        axes[1, idx].axis('off')
        plt.colorbar(im, ax=axes[1, idx], fraction=0.046, pad=0.04)
    
    # Row 3: Input gradients
    for idx, modality_name in enumerate(modality_names):
        saliency = np.abs(input_saliency[modality_name].numpy())
        im = axes[2, idx].imshow(saliency, cmap='Greens', vmin=0, vmax=saliency.max())
        axes[2, idx].set_title(f'{modality_name} Input Gradients')
        axes[2, idx].axis('off')
        plt.colorbar(im, ax=axes[2, idx], fraction=0.046, pad=0.04)
    
    # Row 4: Baseline prediction and ground truth
    im = axes[3, 0].imshow(baseline_prediction, cmap='Blues', vmin=0, vmax=1)
    axes[3, 0].set_title(f'Baseline Prediction (mean: {baseline_prediction.mean():.3f})')
    axes[3, 0].axis('off')
    plt.colorbar(im, ax=axes[3, 0], fraction=0.046, pad=0.04)
    
    axes[3, 1].imshow(hydro_mask, cmap='Blues')
    axes[3, 1].set_title('Ground Truth')
    axes[3, 1].axis('off')
    
    axes[3, 2].axis('off')
    axes[3, 3].axis('off')
    
    plt.tight_layout()
    
    # Save
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path('/u/nathanj/national_ml/experiments/evaluation/visual_analysis/attention_map/ablation_visualizations')
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / f'ablation_analysis_{huc_id}_{patch_file.stem}_water{water_percentage:.1f}pct_{timestamp}.png'
    plt.savefig(output_file, dpi=200, bbox_inches='tight')
    plt.close()
    
    print(f"✅ Saved: {output_file.name}")
    
    # Summary for this patch
    print(f"\n📊 Patch {patch_idx} Analysis Summary (Water: {water_percentage:.1f}%):")
    print("-" * 60)
    
    print(f"\n🧪 Ablation Study Results (Mean Importance):")
    ablation_means = {name: importance.mean() for name, importance in ablation_importance.items()}
    sorted_ablation = sorted(ablation_means.items(), key=lambda x: x[1], reverse=True)
    for i, (modality, importance) in enumerate(sorted_ablation, 1):
        print(f"  {i}. {modality:8}: {importance:.4f}")
    
    print(f"\n🧪 Input Gradient Results (Mean Saliency):")
    saliency_means = {name: np.abs(saliency.numpy()).mean() for name, saliency in input_saliency.items()}
    sorted_saliency = sorted(saliency_means.items(), key=lambda x: x[1], reverse=True)
    for i, (modality, saliency) in enumerate(sorted_saliency, 1):
        print(f"  {i}. {modality:8}: {saliency:.4f}")
    
    # Calculate water-specific importance
    water_mask = hydro_mask > 0.5
    if np.any(water_mask):
        print(f"\n💧 Water Pixels Specific Importance:")
        water_ablation_means = {name: importance[water_mask].mean() for name, importance in ablation_importance.items()}
        sorted_water_ablation = sorted(water_ablation_means.items(), key=lambda x: x[1], reverse=True)
        for i, (modality, importance) in enumerate(sorted_water_ablation, 1):
            print(f"  {i}. {modality:8}: {importance:.4f}")
    
    return {
        'patch_name': patch_file.stem,
        'water_percentage': water_percentage,
        'ablation_results': ablation_means,
        'gradient_results': saliency_means,
        'baseline_prediction_mean': baseline_prediction.mean(),
        'baseline_prediction_max': baseline_prediction.max()
    }

if __name__ == "__main__":
    alternative_modality_analysis()