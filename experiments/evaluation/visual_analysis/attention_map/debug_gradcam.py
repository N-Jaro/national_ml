#!/usr/bin/env python3
"""
Debug the actual Grad-CAM calculation to see why optical attention is 0
"""

import sys
import os
import numpy as np
import torch
import torch.nn.functional as F
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

def debug_gradcam_calculation(model, input_tensor, target_layer, modality_mask, modality_name):
    """Debug the detailed Grad-CAM calculation"""
    print(f"\n🔍 Detailed Grad-CAM Debug for {modality_name}:")
    
    # Register hooks to capture gradients and activations
    gradients = []
    activations = []
    
    def backward_hook(module, grad_input, grad_output):
        gradients.append(grad_output[0])
        print(f"  📥 Gradient captured: shape={grad_output[0].shape}, range=[{grad_output[0].min():.3f}, {grad_output[0].max():.3f}]")
    
    def forward_hook(module, input, output):
        activations.append(output)
        print(f"  📤 Activation captured: shape={output.shape}, range=[{output.min():.3f}, {output.max():.3f}]")
    
    # Register hooks
    handle_f = target_layer.register_forward_hook(forward_hook)
    handle_b = target_layer.register_backward_hook(backward_hook)
    
    try:
        # Forward pass with masked input
        model.zero_grad()
        masked_tensor = input_tensor.clone() * modality_mask
        
        m1 = masked_tensor[:, :1, :, :]
        m2 = masked_tensor[:, 1:7, :, :]
        m3 = masked_tensor[:, 7:8, :, :]
        m4 = masked_tensor[:, 8:9, :, :]
        output1, output2 = model(m1, m2, m3, m4)
        
        print(f"  🎯 Output1: shape={output1.shape}, range=[{output1.min():.3f}, {output1.max():.3f}], mean={output1.mean():.3f}")
        
        # Try different target strategies
        targets_to_test = [
            ("mean", output1.mean()),
            ("abs_mean", output1.abs().mean()), 
            ("positive_mean", torch.relu(output1).mean()),
            ("max", output1.max()),
            ("sum", output1.sum())
        ]
        
        for target_name, target in targets_to_test:
            print(f"\n  🎯 Testing target: {target_name} = {target:.3f}")
            
            # Clear previous gradients
            model.zero_grad()
            gradients.clear()
            
            # Backward pass
            target.backward(retain_graph=True)
            
            if gradients:
                gradient = gradients[0]
                activation = activations[0]
                
                print(f"    📊 Gradient: shape={gradient.shape}, range=[{gradient.min():.3f}, {gradient.max():.3f}], mean={gradient.mean():.3f}")
                print(f"    📊 Activation: shape={activation.shape}, range=[{activation.min():.3f}, {activation.max():.3f}], mean={activation.mean():.3f}")
                
                # Calculate Grad-CAM
                weights = torch.mean(gradient, dim=(2, 3), keepdim=True)
                print(f"    🏋️  Weights: shape={weights.shape}, range=[{weights.min():.3f}, {weights.max():.3f}], mean={weights.mean():.3f}")
                
                cam = torch.sum(weights * activation, dim=1, keepdim=True)
                print(f"    📈 CAM (before ReLU): shape={cam.shape}, range=[{cam.min():.3f}, {cam.max():.3f}], mean={cam.mean():.3f}")
                
                cam = torch.relu(cam)
                print(f"    📈 CAM (after ReLU): shape={cam.shape}, range=[{cam.min():.3f}, {cam.max():.3f}], mean={cam.mean():.3f}")
                
                # Check how many pixels are active
                cam_resized = torch.nn.functional.interpolate(cam, size=(224, 224), mode='bilinear', align_corners=False)
                cam_np = cam_resized.squeeze().detach().cpu().numpy()
                
                if cam_np.max() > cam_np.min():
                    cam_normalized = (cam_np - cam_np.min()) / (cam_np.max() - cam_np.min())
                else:
                    cam_normalized = cam_np
                
                active_pixels = np.sum(cam_normalized > 0.1)
                total_pixels = cam_normalized.size
                
                print(f"    ✅ Final CAM: active pixels = {active_pixels}/{total_pixels} ({100*active_pixels/total_pixels:.1f}%)")
            else:
                print(f"    ❌ No gradients captured for {target_name}")
                
    except Exception as e:
        print(f"    ❌ Error: {e}")
    
    finally:
        # Remove hooks
        handle_f.remove()
        handle_b.remove()

def debug_gradcam():
    """Debug Grad-CAM calculation in detail"""
    
    print("🐛 Debug: Detailed Grad-CAM Calculation")
    print("=" * 50)
    
    # Load and prepare data (same as before)
    patch_dir = Path('/u/nathanj/national_ml/data/processed/patch_dataset/03030005')
    patch_files = list(patch_dir.glob('patch_*.npz'))[:1]
    patch_file = patch_files[0]
    
    with np.load(patch_file) as data:
        dem = data['dem']
        optical = data['optical'] 
        thermal = data['thermal']
        sar = data['sar']
    
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
    
    # Target the final decoder layer
    target_layer = model.decoder_task1.outc.conv
    
    # Create optical mask
    optical_mask = torch.zeros_like(input_tensor)
    optical_mask[:, 1:7, :, :] = 1.0
    
    # Debug optical Grad-CAM calculation
    debug_gradcam_calculation(model, input_tensor, target_layer, optical_mask, "Optical")

if __name__ == "__main__":
    debug_gradcam()