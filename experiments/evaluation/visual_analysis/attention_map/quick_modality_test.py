#!/usr/bin/env python3
"""
Quick modality attention analysis for landsat6b - Final layer only
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
root_dir = current_dir.parent.parent.parent.parent  # /u/nathanj/national_ml
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
    
    # Normalize optical (6 bands: B2, B3, B4, B5, B6, B7)
    if 'optical' in stats and optical is not None:
        bands = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
        for i, band in enumerate(bands):
            if i < optical.shape[2]:  # Check if band exists
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

def get_modality_attention_from_encoder(model, input_tensor, encoder_layer, modality_mask, modality_name):
    """Get attention for a specific modality from its individual encoder"""
    gradients = []
    activations = []
    
    def backward_hook(module, grad_input, grad_output):
        gradients.append(grad_output[0])
    
    def forward_hook(module, input, output):
        activations.append(output)
    
    handle_f = encoder_layer.register_forward_hook(forward_hook)
    handle_b = encoder_layer.register_backward_hook(backward_hook)
    
    try:
        model.zero_grad()
        masked_tensor = input_tensor.clone() * modality_mask
        
        m1 = masked_tensor[:, :1, :, :]
        m2 = masked_tensor[:, 1:7, :, :]
        m3 = masked_tensor[:, 7:8, :, :]
        m4 = masked_tensor[:, 8:9, :, :]
        output1, output2 = model(m1, m2, m3, m4)
        
        # Use absolute mean as target to handle negative outputs
        target = output1.abs().mean()
        target.backward()
        
        if gradients and activations:
            gradient = gradients[0]
            activation = activations[0]
            
            weights = torch.mean(gradient, dim=(2, 3), keepdim=True)
            cam = torch.sum(weights * activation, dim=1, keepdim=True)
            
            # Take absolute value of CAM to get attention magnitude
            cam = torch.abs(cam)
            
            cam = torch.nn.functional.interpolate(
                cam, size=(224, 224), mode='bilinear', align_corners=False
            )
            
            cam = cam.squeeze().detach().cpu().numpy()
            if cam.max() > cam.min():
                cam = (cam - cam.min()) / (cam.max() - cam.min())
            
            return cam
        else:
            return np.zeros((224, 224))
            
    except Exception as e:
        print(f"Error in {modality_name} encoder attention: {e}")
        return np.zeros((224, 224))
    
    finally:
        handle_f.remove()
        handle_b.remove()

def get_modality_attention(model, input_tensor, target_layer, modality_mask):
    """Get attention for a specific modality (legacy function for compatibility)"""
    gradients = []
    activations = []
    
    def backward_hook(module, grad_input, grad_output):
        gradients.append(grad_output[0])
    
    def forward_hook(module, input, output):
        activations.append(output)
    
    handle_f = target_layer.register_forward_hook(forward_hook)
    handle_b = target_layer.register_backward_hook(backward_hook)
    
    try:
        model.zero_grad()
        masked_tensor = input_tensor.clone() * modality_mask
        
        m1 = masked_tensor[:, :1, :, :]
        m2 = masked_tensor[:, 1:7, :, :]
        m3 = masked_tensor[:, 7:8, :, :]
        m4 = masked_tensor[:, 8:9, :, :]
        output1, output2 = model(m1, m2, m3, m4)
        
        # Use absolute mean as target to handle negative outputs
        target = output1.abs().mean()
        target.backward()
        
        if gradients and activations:
            gradient = gradients[0]
            activation = activations[0]
            
            weights = torch.mean(gradient, dim=(2, 3), keepdim=True)
            cam = torch.sum(weights * activation, dim=1, keepdim=True)
            
            # Don't apply ReLU directly - handle negative values properly
            # Take absolute value of CAM to get attention magnitude
            cam = torch.abs(cam)
            
            cam = torch.nn.functional.interpolate(
                cam, size=(224, 224), mode='bilinear', align_corners=False
            )
            
            cam = cam.squeeze().detach().cpu().numpy()
            if cam.max() > cam.min():
                cam = (cam - cam.min()) / (cam.max() - cam.min())
            
            return cam
        else:
            return np.zeros((224, 224))
            
    except Exception as e:
        print(f"Error in modality attention: {e}")
        return np.zeros((224, 224))
    
    finally:
        handle_f.remove()
        handle_b.remove()

def quick_modality_test():
    """Quick test focusing on the final decoder layer - multiple patches"""
    
    print("🎯 Quick Modality Attention Test - Multiple Patches")
    print("=" * 60)
    
    # Load test patches - test multiple patches
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/03030005')
    patch_files = list(patch_dir.glob('patch_*.npz'))[:5]  # Test 5 patches
    
    if not patch_files:
        print("❌ No patch files found!")
        return
    
    print(f"📊 Testing with {len(patch_files)} patches from HUC 03030005")
    
    # Load model once (outside the patch loop)
    print("📦 Loading model...")
    model = MultimodalMultitaskModel_ls6b(n_classes_task1=1, n_classes_task2=8)
    
    checkpoint_path = '/u/nathanj/national_ml/experiments/training/lightning_logs/combineFL_ls6b_20250925_214001_run7/checkpoints/mdmt-epoch=67-val_loss=0.4040.ckpt'
    
    try:
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
        
    except Exception as e:
        print(f"❌ Error loading checkpoint: {e}")
        return
    
    # We'll target individual encoder layers for each modality
    print(f"🎯 Strategy: Target individual encoder outputs for each modality")
    
    # Get HUC ID and load normalization stats once
    huc_id = patch_files[0].parent.name
    stats = load_normalization_stats(huc_id)
    
    # Process each patch
    all_results = []
    
    for patch_idx, patch_file in enumerate(patch_files):
        print(f"\n{'='*60}")
        print(f"📊 Processing Patch {patch_idx + 1}/{len(patch_files)}: {patch_file.name}")
        print(f"{'='*60}")
        
        # Load and prepare data
        with np.load(patch_file) as data:
            dem = data['dem']
            optical = data['optical'] 
            thermal = data['thermal']
            sar = data['sar']
            hydro_mask = data['hydro_mask']
        
        # Calculate water percentage
        water_pixels = np.sum(hydro_mask > 0.5)
        total_pixels = hydro_mask.size
        water_percentage = 100 * water_pixels / total_pixels
        
        print(f"💧 Water percentage: {water_percentage:.1f}%")
        
        # Normalize data using HUC-specific statistics
        dem, optical, thermal, sar = normalize_data(dem, optical, thermal, sar, stats)
        
        # Create input tensor for landsat6b model
        dem_tensor = dem[np.newaxis, :, :].astype(np.float32)
        optical_tensor = optical.transpose(2, 0, 1).astype(np.float32)
        thermal_tensor = thermal[np.newaxis, :, :].astype(np.float32)
        sar_tensor = sar[np.newaxis, :, :].astype(np.float32)
        
        input_data = np.concatenate([dem_tensor, optical_tensor, thermal_tensor, sar_tensor], axis=0)
        input_tensor = torch.FloatTensor(input_data).unsqueeze(0)
        
        # Generate model prediction
        with torch.no_grad():
            m1 = input_tensor[:, :1, :, :]
            m2 = input_tensor[:, 1:7, :, :]
            m3 = input_tensor[:, 7:8, :, :]
            m4 = input_tensor[:, 8:9, :, :]
            prediction, _ = model(m1, m2, m3, m4)
            prediction = torch.sigmoid(prediction).squeeze().numpy()
        
        print(f"🎯 Prediction range: [{prediction.min():.3f}, {prediction.max():.3f}], mean: {prediction.mean():.3f}")
        
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
        
        print("📊 Computing modality attention from appropriate encoder layers...")
        modality_attention = {}
        
        # Define the appropriate target layers for each modality
        encoder_targets = {
            'DEM': model.encoder1.down4.maxpool_conv[1].double_conv[3],  # Final conv of encoder1 (priority encoder)
            'Optical': model.encoder2.down4.maxpool_conv[1].double_conv[3],  # Final conv of encoder2
            'Thermal': model.encoder3.down4.maxpool_conv[1].double_conv[3],  # Final conv of encoder3
            'SAR': model.encoder4.down4.maxpool_conv[1].double_conv[3]   # Final conv of encoder4
        }
        
        for modality_name, mask in modality_masks.items():
            print(f"  🔍 {modality_name} (from encoder{['DEM', 'Optical', 'Thermal', 'SAR'].index(modality_name) + 1})...", end=" ")
            
            # Use the appropriate encoder layer for this modality
            encoder_layer = encoder_targets[modality_name]
            attention = get_modality_attention_from_encoder(model, input_tensor, encoder_layer, mask, modality_name)
            modality_attention[modality_name] = attention
            
            active_pixels = np.sum(attention > 0.1)
            total_pixels = attention.size
            print(f"✅ {100*active_pixels/total_pixels:.1f}% active")
        
        # Create visualization for this patch
        print("🎨 Creating visualization...")
        fig, axes = plt.subplots(3, 4, figsize=(16, 12))
        
        # Row 1: Input modalities
        axes[0, 0].imshow(dem, cmap='terrain')
        axes[0, 0].set_title(f'DEM Input - {patch_file.stem}')
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
        else:
            axes[0, 1].axis('off')
        
        axes[0, 2].imshow(thermal, cmap='plasma')
        axes[0, 2].set_title('Thermal Input')
        axes[0, 2].axis('off')
        
        axes[0, 3].imshow(sar, cmap='gray')
        axes[0, 3].set_title('SAR Input')
        axes[0, 3].axis('off')
        
        # Row 2: Modality attention maps
        modality_names = ['DEM', 'Optical', 'Thermal', 'SAR']
        for idx, modality_name in enumerate(modality_names):
            attention = modality_attention[modality_name]
            im = axes[1, idx].imshow(attention, cmap='hot', vmin=0, vmax=1)
            
            active_pct = 100 * np.sum(attention > 0.1) / attention.size
            axes[1, idx].set_title(f'{modality_name} Attention ({active_pct:.1f}%)')
            axes[1, idx].axis('off')
            plt.colorbar(im, ax=axes[1, idx], fraction=0.046, pad=0.04)
        
        # Row 3: Combined attention, prediction, ground truth
        combined_attention = np.mean(list(modality_attention.values()), axis=0)
        im = axes[2, 0].imshow(combined_attention, cmap='hot', vmin=0, vmax=1)
        axes[2, 0].set_title('Combined Attention')
        axes[2, 0].axis('off')
        plt.colorbar(im, ax=axes[2, 0], fraction=0.046, pad=0.04)
        
        im = axes[2, 1].imshow(prediction, cmap='Blues', vmin=0, vmax=1)
        axes[2, 1].set_title(f'Prediction (mean: {prediction.mean():.3f})')
        axes[2, 1].axis('off')
        plt.colorbar(im, ax=axes[2, 1], fraction=0.046, pad=0.04)
        
        axes[2, 2].imshow(hydro_mask, cmap='Blues')
        axes[2, 2].set_title('Ground Truth')
        axes[2, 2].axis('off')
        
        axes[2, 3].axis('off')
        
        plt.tight_layout()
        
        # Save individual patch visualization
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path('/u/nathanj/national_ml/experiments/evaluation/visual_analysis/attention_map/real_gradcam_tests')
        output_dir.mkdir(exist_ok=True)
        
        output_file = output_dir / f'modality_attention_{huc_id}_{patch_file.stem}_{timestamp}.png'
        plt.savefig(output_file, dpi=200, bbox_inches='tight')
        plt.close()
        
        print(f"✅ Saved: {output_file.name}")
        
        # Store results for summary
        patch_results = {
            'patch_name': patch_file.stem,
            'water_percentage': water_percentage,
            'prediction_mean': prediction.mean(),
            'modality_attention': {}
        }
        
        for modality_name, attention in modality_attention.items():
            mean_val = attention.mean()
            max_val = attention.max()
            active_pixels = np.sum(attention > 0.1)
            total_pixels = attention.size
            active_pct = 100*active_pixels/total_pixels
            
            patch_results['modality_attention'][modality_name] = {
                'mean': mean_val,
                'max': max_val,
                'active_pct': active_pct
            }
        
        all_results.append(patch_results)
        
        # Print summary for this patch
        print(f"\n📊 Patch {patch_file.stem} Modality Summary:")
        print(f"{'Modality':<15} {'Mean':<8} {'Max':<8} {'Active%':<8} {'Focus'}")
        print("-" * 55)
        
        for modality_name, attention in modality_attention.items():
            mean_val = attention.mean()
            max_val = attention.max()
            active_pixels = np.sum(attention > 0.1)
            total_pixels = attention.size
            active_pct = 100*active_pixels/total_pixels
            
            if active_pct > 50:
                focus = "🔥 High"
            elif active_pct > 20:
                focus = "🟡 Medium"
            elif active_pct > 5:
                focus = "🔵 Low"
            else:
                focus = "⚫ None"
            
            print(f"{modality_name:<15} {mean_val:.3f}    {max_val:.3f}    {active_pct:>5.1f}%    {focus}")
    
    # Print overall summary across all patches
    print(f"\n{'🌟 OVERALL SUMMARY ACROSS ALL PATCHES 🌟':^60}")
    print("=" * 60)
    
    print(f"{'Patch':<15} {'Water%':<8} {'Pred':<8} {'DEM%':<8} {'Opt%':<8} {'Thermal%':<8} {'SAR%'}")
    print("-" * 70)
    
    for results in all_results:
        patch_name = results['patch_name']
        water_pct = results['water_percentage']
        pred_mean = results['prediction_mean']
        
        dem_pct = results['modality_attention']['DEM']['active_pct']
        opt_pct = results['modality_attention']['Optical']['active_pct']
        thermal_pct = results['modality_attention']['Thermal']['active_pct']
        sar_pct = results['modality_attention']['SAR']['active_pct']
        
        print(f"{patch_name:<15} {water_pct:>5.1f}%   {pred_mean:>5.3f}   {dem_pct:>5.1f}%   {opt_pct:>5.1f}%   {thermal_pct:>7.1f}%   {sar_pct:>5.1f}%")

if __name__ == "__main__":
    quick_modality_test()