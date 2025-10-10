#!/usr/bin/env python3
"""
Quick test of modality-specific attention maps for landsat6b model
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
import pickle

# Add experiments directory to path
experiments_dir = Path(__file__).parent.parent.parent.parent / 'experiments'
sys.path.append(str(experiments_dir))

from models.mdmt_landsat_6b import MDMT_landsat_6b

def load_normalization_stats(huc_id):
    """Load normalization statistics for the HUC"""
    # Look for normalization stats file
    possible_paths = [
        f'/u/nathanj/national_ml/data/processed/patch_dataset/normalization_stats.json',
        f'/u/nathanj/national_ml/data/processed/patch_dataset/{huc_id}/normalization_stats.json'
    ]
    
    for stats_path in possible_paths:
        if os.path.exists(stats_path):
            with open(stats_path, 'r') as f:
                all_stats = json.load(f)
                if huc_id in all_stats:
                    return all_stats[huc_id]
    
    print(f"⚠️  No normalization stats found for HUC {huc_id}")
    return None

def _zscore(data, mean, std):
    """Apply z-score normalization"""
    return (data - mean) / (std + 1e-8)

def normalize_data(patch_data, huc_id):
    """Normalize data using HUC-specific statistics"""
    stats = load_normalization_stats(huc_id)
    if stats is None:
        print("⚠️  Using raw data without normalization")
        return patch_data
    
    normalized_data = patch_data.copy()
    
    # DEM normalization (channel 0)
    if 'elevation_mean' in stats and 'elevation_stdDev' in stats:
        normalized_data[0] = _zscore(patch_data[0], stats['elevation_mean'], stats['elevation_stdDev'])
    
    # Optical bands normalization (channels 1-6)
    optical_bands = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
    for i, band in enumerate(optical_bands, 1):
        mean_key = f'{band}_mean'
        std_key = f'{band}_stdDev'
        if mean_key in stats and std_key in stats:
            normalized_data[i] = _zscore(patch_data[i], stats[mean_key], stats[std_key])
    
    # Thermal normalization (channel 7) - using B10
    if 'ST_B10_mean' in stats and 'ST_B10_stdDev' in stats:
        normalized_data[7] = _zscore(patch_data[7], stats['ST_B10_mean'], stats['ST_B10_stdDev'])
    
    # SAR normalization (channel 8) - using VV
    if 'VV_mean' in stats and 'VV_stdDev' in stats:
        normalized_data[8] = _zscore(patch_data[8], stats['VV_mean'], stats['VV_stdDev'])
    
    return normalized_data

def get_modality_attention(model, input_tensor, target_layer, modality_mask):
    """Get attention for a specific modality by masking other inputs."""
    
    # Register hooks to capture gradients and activations
    gradients = []
    activations = []
    
    def backward_hook(module, grad_input, grad_output):
        gradients.append(grad_output[0])
    
    def forward_hook(module, input, output):
        activations.append(output)
    
    # Register hooks
    handle_f = target_layer.register_forward_hook(forward_hook)
    handle_b = target_layer.register_backward_hook(backward_hook)
    
    try:
        # Forward pass with masked input
        model.zero_grad()
        
        # Apply modality mask - zero out other modalities
        masked_tensor = input_tensor.clone()
        masked_tensor = masked_tensor * modality_mask
        
        m1 = masked_tensor[:, :1, :, :]
        m2 = masked_tensor[:, 1:7, :, :]
        m3 = masked_tensor[:, 7:8, :, :]
        m4 = masked_tensor[:, 8:9, :, :]
        output1, output2 = model(m1, m2, m3, m4)
        
        # Use the mean of output1 as the target
        target = output1.mean()
        
        # Backward pass
        target.backward()
        
        # Get the gradient and activation
        if gradients and activations:
            gradient = gradients[0]
            activation = activations[0]
            
            # Calculate Grad-CAM
            weights = torch.mean(gradient, dim=(2, 3), keepdim=True)
            cam = torch.sum(weights * activation, dim=1, keepdim=True)
            cam = torch.relu(cam)
            
            # Resize to input size
            cam = torch.nn.functional.interpolate(
                cam, size=(224, 224), mode='bilinear', align_corners=False
            )
            
            # Convert to numpy and normalize
            cam = cam.squeeze().detach().cpu().numpy()
            if cam.max() > cam.min():
                cam = (cam - cam.min()) / (cam.max() - cam.min())
            
            return cam
        else:
            return np.zeros((224, 224))
            
    except Exception as e:
        print(f"      ❌ Error in modality attention calculation: {e}")
        return np.zeros((224, 224))
    
    finally:
        # Remove hooks
        handle_f.remove()
        handle_b.remove()

def test_modality_attention():
    """Test modality-specific attention for landsat6b model"""
    
    print("🎯 Testing Modality-Specific Attention - landsat6b")
    print("=" * 55)
    
    # Load test patch
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/01030003')
    patch_files = list(patch_dir.glob('patch_*.npz'))[:1]  # Just test one patch
    
    if not patch_files:
        print("❌ No patch files found!")
        return
    
    patch_file = patch_files[0]
    print(f"📊 Testing with patch: {patch_file.name}")
    
    # Load patch data
    with np.load(patch_file) as data:
        dem = data['dem']
        optical = data['optical'] 
        thermal = data['thermal']
        sar = data['sar']
        hydro_mask = data['hydro_mask']
    
    print(f"   DEM shape: {dem.shape}")
    print(f"   Optical shape: {optical.shape}")
    print(f"   Thermal shape: {thermal.shape}")
    print(f"   SAR shape: {sar.shape}")
    
    # Stack into single array for landsat6b (9 channels)
    patch_data = np.stack([
        dem,
        optical[0], optical[1], optical[2], optical[3], optical[4], optical[5],
        thermal,
        sar
    ], axis=0)
    
    # Extract HUC from path and normalize
    huc_id = patch_file.parent.name
    patch_data = normalize_data(patch_data, huc_id)
    
    # Convert to tensor
    input_tensor = torch.FloatTensor(patch_data).unsqueeze(0)
    print(f"✅ Input tensor shape: {input_tensor.shape}")
    
    # Load model
    print("📦 Loading landsat6b model...")
    checkpoint_path = '/u/nathanj/national_ml/outputs/models/mdmt-epoch=67-val_loss=0.4040.ckpt'
    
    if not os.path.exists(checkpoint_path):
        print(f"❌ Checkpoint not found: {checkpoint_path}")
        return
    
    checkpoint = torch.load(checkpoint_path, map_location='cpu')
    model = MDMT_landsat_6b.load_from_checkpoint(checkpoint_path, map_location='cpu')
    model.eval()
    print("✅ Model loaded successfully")
    
    # Get a target layer (use decoder output layer)
    target_layer = model.decoder.outc.conv
    print(f"🎯 Target layer: {target_layer}")
    
    # Create modality masks
    modality_masks = {
        'DEM': torch.zeros_like(input_tensor),
        'Optical': torch.zeros_like(input_tensor),
        'Thermal': torch.zeros_like(input_tensor),
        'SAR': torch.zeros_like(input_tensor)
    }
    modality_masks['DEM'][:, :1, :, :] = 1.0  # Channel 0: DEM
    modality_masks['Optical'][:, 1:7, :, :] = 1.0  # Channels 1-6: Optical
    modality_masks['Thermal'][:, 7:8, :, :] = 1.0  # Channel 7: Thermal
    modality_masks['SAR'][:, 8:9, :, :] = 1.0  # Channel 8: SAR
    
    print("\n📊 Computing modality-specific attention maps...")
    modality_attention = {}
    
    for modality_name, mask in modality_masks.items():
        print(f"  🔍 Computing {modality_name} attention...")
        try:
            attention = get_modality_attention(model, input_tensor, target_layer, mask)
            modality_attention[modality_name] = attention
            
            # Statistics
            active_pixels = np.sum(attention > 0.1)
            total_pixels = attention.size
            print(f"      ✅ Success - Active pixels: {100*active_pixels/total_pixels:.1f}%")
            
        except Exception as e:
            print(f"      ❌ Failed: {e}")
            modality_attention[modality_name] = np.zeros((224, 224))
    
    # Create visualization
    print("\n🎨 Creating visualization...")
    fig, axes = plt.subplots(2, 4, figsize=(16, 8))
    
    # Top row: Input modalities
    axes[0, 0].imshow(dem, cmap='terrain')
    axes[0, 0].set_title('DEM Input')
    axes[0, 0].axis('off')
    
    # RGB optical
    if optical.shape[0] >= 3:
        rgb_optical = optical[[2,1,0], :, :]  # Red, Green, Blue bands
        rgb_optical = np.transpose(rgb_optical, (1, 2, 0))
        for i in range(3):
            band = rgb_optical[:, :, i]
            if band.max() > band.min():
                rgb_optical[:, :, i] = (band - band.min()) / (band.max() - band.min())
        axes[0, 1].imshow(rgb_optical)
        axes[0, 1].set_title('Optical Input (RGB)')
        axes[0, 1].axis('off')
    
    axes[0, 2].imshow(thermal, cmap='plasma')
    axes[0, 2].set_title('Thermal Input')
    axes[0, 2].axis('off')
    
    axes[0, 3].imshow(sar, cmap='gray')
    axes[0, 3].set_title('SAR Input')
    axes[0, 3].axis('off')
    
    # Bottom row: Modality attention maps
    modality_names = ['DEM', 'Optical', 'Thermal', 'SAR']
    for idx, modality_name in enumerate(modality_names):
        attention = modality_attention[modality_name]
        im = axes[1, idx].imshow(attention, cmap='hot', vmin=0, vmax=1)
        axes[1, idx].set_title(f'{modality_name} Attention')
        axes[1, idx].axis('off')
        plt.colorbar(im, ax=axes[1, idx], fraction=0.046, pad=0.04)
    
    plt.tight_layout()
    
    # Save
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path('/u/nathanj/national_ml/experiments/evaluation/visual_analysis/attention_map/real_gradcam_tests')
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / f'modality_attention_test_{huc_id}_{patch_file.stem}_{timestamp}.png'
    plt.savefig(output_file, dpi=200, bbox_inches='tight')
    plt.close()
    
    print(f"\n✅ Modality attention test saved: {output_file.name}")
    
    # Print summary
    print("\n📊 Modality Attention Summary:")
    print(f"{'Modality':<15} {'Mean':<8} {'Max':<8} {'Active%':<8}")
    print("-" * 45)
    
    for modality_name, attention in modality_attention.items():
        mean_val = attention.mean()
        max_val = attention.max()
        active_pixels = np.sum(attention > 0.1)
        total_pixels = attention.size
        
        print(f"{modality_name:<15} {mean_val:.3f}    {max_val:.3f}    {100*active_pixels/total_pixels:>5.1f}%")

if __name__ == "__main__":
    test_modality_attention()