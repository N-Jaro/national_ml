#!/usr/bin/env python3
"""
Multi-Layer Attention Analysis for Landsat6b Model
Generates Grad-CAM visualizations across different architectural layers.
"""

import os
import sys
import torch
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from scipy.ndimage import gaussian_filter, median_filter
import warnings
import json
warnings.filterwarnings('ignore')

# Add model paths
sys.path.append('/u/nathanj/national_ml/experiments/models')
sys.path.append('/u/nathanj/national_ml/experiments/evaluation/visual_analysis/attention_map')

from mdmt_landsat_6b import MultimodalMultitaskModel_ls6b
from selected_patches import SELECTED_PATCHES

def _zscore(arr, mean, std, eps=1e-6):
    """Z-score normalization using per-HUC statistics."""
    return (arr - mean) / np.maximum(std, eps)

def load_normalization_stats(huc_id):
    """Load normalization statistics for a given HUC."""
    stats_path = f"/projects/bcrm/nathanj/data/processed/test/patch_dataset/{huc_id}/normalization_stats.json"
    try:
        with open(stats_path, 'r') as f:
            stats = json.load(f)
        return stats
    except Exception as e:
        print(f"⚠️  Warning: Could not load normalization stats for HUC {huc_id}: {e}")
        return None

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

def get_gradients_manually(model, input_tensor, target_layer):
    """Get gradients manually without pytorch_grad_cam to avoid compatibility issues."""
    
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
        # Forward pass
        model.zero_grad()
        m1 = input_tensor[:, :1, :, :]
        m2 = input_tensor[:, 1:7, :, :]
        m3 = input_tensor[:, 7:8, :, :]
        m4 = input_tensor[:, 8:9, :, :]
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
        print(f"      ❌ Error in manual gradient calculation: {e}")
        return np.zeros((224, 224))
    
    finally:
        # Remove hooks
        handle_f.remove()
        handle_b.remove()

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

def generate_multi_layer_attention_maps():
    """Generate attention maps across multiple model layers for one variant."""
    
    device = torch.device('cpu')
    print(f"🎯 Enhanced Multi-Layer Attention Analysis - landsat6b")
    print("=" * 60)
    
    # Get test patch
    if not SELECTED_PATCHES:
        print("❌ No selected patches found!")
        return
    
    huc_id = list(SELECTED_PATCHES.keys())[0]
    patch_info = SELECTED_PATCHES[huc_id][0]
    
    print(f"📊 Analyzing patch from HUC {huc_id}")
    print(f"   Water percentage: {patch_info['water_percentage']:.1f}%")
    print(f"   File: {Path(patch_info['file_path']).name}")
    
    # Load and prepare data
    try:
        data = np.load(patch_info['file_path'])
        dem = data.get('dem', None)
        hydro_mask = data.get('hydro_mask', None)
        optical = data.get('optical', None)
        thermal = data.get('thermal', None)
        sar = data.get('sar', None)
        
        if dem is None or hydro_mask is None:
            print(f"❌ Missing required data")
            return
        
        # Load normalization statistics for this HUC
        stats = load_normalization_stats(huc_id)
        
        # Normalize data using HUC-specific statistics
        dem, optical, thermal, sar = normalize_data(dem, optical, thermal, sar, stats)
        
        # Create input tensor for Landsat6b model
        # m1: DEM (1 channel)
        # m2: Optical (6 channels) 
        # m3: Thermal (1 channel)
        # m4: SAR (1 channel)
        dem_tensor = dem[np.newaxis, :, :].astype(np.float32)  # (1, 224, 224)
        
        if optical is not None:
            optical_tensor = optical.transpose(2, 0, 1).astype(np.float32)  # (6, 224, 224)
        else:
            optical_tensor = np.zeros((6, 224, 224), dtype=np.float32)
            
        thermal_tensor = thermal[np.newaxis, :, :].astype(np.float32) if thermal is not None else np.zeros((1, 224, 224), dtype=np.float32)
        sar_tensor = sar[np.newaxis, :, :].astype(np.float32) if sar is not None else np.zeros((1, 224, 224), dtype=np.float32)
        
        # Combine all: (9, 224, 224)
        input_data = np.concatenate([dem_tensor, optical_tensor, thermal_tensor, sar_tensor], axis=0)
        
    except Exception as e:
        print(f"❌ Error loading patch: {e}")
        return
    
    print(f"✅ Loaded patch data: {input_data.shape}")
    
    # Load model
    print(f"📦 Loading landsat6b model...")
    model = MultimodalMultitaskModel_ls6b(n_classes_task1=1, n_classes_task2=8)
    
    checkpoint_path = '/u/nathanj/national_ml/experiments/training/lightning_logs/combineFL_ls6b_20250925_214001_run7/checkpoints/mdmt-epoch=67-val_loss=0.4040.ckpt'
    
    try:
        checkpoint = torch.load(checkpoint_path, map_location=device)
        
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
        model = model.to(device)
        model.eval()
        print(f"✅ Model loaded successfully")
    except Exception as e:
        print(f"❌ Error loading model: {e}")
        return
    
    # Generate model prediction
    print(f"🎯 Generating model prediction...")
    try:
        with torch.no_grad():
            input_tensor = torch.from_numpy(input_data).float().unsqueeze(0).to(device)
            m1 = input_tensor[:, :1, :, :]
            m2 = input_tensor[:, 1:7, :, :]
            m3 = input_tensor[:, 7:8, :, :]
            m4 = input_tensor[:, 8:9, :, :]
            output1, output2 = model(m1, m2, m3, m4)
            prediction = torch.sigmoid(output1).cpu().numpy()[0, 0]
        
        print(f"✅ Prediction generated - Range: [{prediction.min():.3f}, {prediction.max():.3f}]")
    except Exception as e:
        print(f"❌ Error generating prediction: {e}")
        return
    
    # Define layers to analyze across the architecture
    layer_configs = {
        'Encoder_Shallow_112x112': 'encoder1.down1.maxpool_conv.1.double_conv.5',
        'Encoder_Mid_56x56': 'encoder1.down2.maxpool_conv.1.double_conv.5',
        'Encoder_Deep_28x28': 'encoder1.down3.maxpool_conv.1.double_conv.5',
        'Encoder_Deepest_14x14': 'encoder1.down4.maxpool_conv.1.double_conv.5',
        'Decoder_Early_28x28': 'decoder_task1.up1.conv.double_conv.5',
        'Decoder_Mid_56x56': 'decoder_task1.up2.conv.double_conv.5',
        'Decoder_Late_112x112': 'decoder_task1.up3.conv.double_conv.5',
        'Decoder_Final_224x224': 'decoder_task1.up4.conv.double_conv.5'
    }
    
    # Generate attention maps
    attention_maps = {}
    print(f"\n🎯 Generating attention maps for {len(layer_configs)} layers...")
    
    input_tensor = torch.from_numpy(input_data).float().unsqueeze(0).to(device)
    input_tensor.requires_grad_(True)
    
    for layer_name, layer_path in layer_configs.items():
        print(f"   Processing: {layer_name}")
        
        try:
            # Get target layer
            layer = model
            for attr in layer_path.split('.'):
                if attr.isdigit():
                    layer = layer[int(attr)]
                else:
                    layer = getattr(layer, attr)
            
            # Generate overall attention map using manual gradients
            attention_map = get_gradients_manually(model, input_tensor, layer)
            
            # Generate modality-specific attention maps
            print(f"        📊 Computing modality-specific attention...")
            
            # Create modality masks (1 where modality is present, 0 elsewhere)
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
            
            modality_attention = {}
            for modality_name, mask in modality_masks.items():
                try:
                    modality_attention[modality_name] = get_modality_attention(
                        model, input_tensor, layer, mask
                    )
                    print(f"          ✅ {modality_name} attention computed")
                except Exception as e:
                    print(f"          ❌ {modality_name} attention failed: {e}")
                    modality_attention[modality_name] = np.zeros((224, 224))
            
            # Store modality attention maps with layer info
            attention_maps[f"{layer_name}_modalities"] = modality_attention
            
            # Enhanced post-processing for upsampling artifacts
            if attention_map.shape == (224, 224):
                # Check for severe upsampling artifacts
                top_quarter = attention_map[:56, :]
                edge_mean = top_quarter.mean()
                center_mean = attention_map[56:168, 56:168].mean()
                
                if edge_mean > center_mean * 2.0:
                    print(f"      🔧 Detected artifacts, applying correction...")
                    # Strong smoothing for problematic cases
                    attention_map = gaussian_filter(attention_map, sigma=2.5)
                    attention_map = median_filter(attention_map, size=5)
                    attention_map = gaussian_filter(attention_map, sigma=1.5)
                    # Normalize again
                    attention_map = (attention_map - attention_map.min()) / (attention_map.max() - attention_map.min() + 1e-8)
                else:
                    print(f"      ✅ Clean attention map")
            
            attention_maps[layer_name] = attention_map
            print(f"      ✅ Success - Range: [{attention_map.min():.3f}, {attention_map.max():.3f}]")
            
        except Exception as e:
            print(f"      ❌ Failed: {e}")
            attention_maps[layer_name] = np.zeros((224, 224))
    
    # Separate overall attention maps from modality attention maps
    overall_attention_maps = {k: v for k, v in attention_maps.items() if not k.endswith('_modalities')}
    modality_attention_maps = {k: v for k, v in attention_maps.items() if k.endswith('_modalities')}
    
    # Create comprehensive visualization with modality analysis
    n_layers = len(overall_attention_maps)
    n_cols = 6  # Increased for modality maps
    n_rows = 4  # Top row: inputs, Second row: modalities, Bottom rows: layer attention
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 2.5, n_rows * 2.5))
    
    # Top row: Input data and prediction
    axes[0, 0].imshow(dem, cmap='terrain')
    axes[0, 0].set_title('Input DEM')
    axes[0, 0].axis('off')
    
    # Show optical bands (RGB approximation)
    if optical.shape[0] >= 3:
        # Use bands 3,2,1 (RGB) and normalize for display
        rgb_optical = optical[[2,1,0], :, :]  # Red, Green, Blue bands
        rgb_optical = np.transpose(rgb_optical, (1, 2, 0))
        # Normalize to 0-1 range for display
        for i in range(3):
            band = rgb_optical[:, :, i]
            if band.max() > band.min():
                rgb_optical[:, :, i] = (band - band.min()) / (band.max() - band.min())
        axes[0, 1].imshow(rgb_optical)
        axes[0, 1].set_title('Optical (RGB)')
        axes[0, 1].axis('off')
    else:
        axes[0, 1].axis('off')
    
    axes[0, 2].imshow(thermal.squeeze(), cmap='plasma')
    axes[0, 2].set_title('Thermal')
    axes[0, 2].axis('off')
    
    axes[0, 3].imshow(sar.squeeze(), cmap='gray')
    axes[0, 3].set_title('SAR')
    axes[0, 3].axis('off')
    
    pred_im = axes[0, 4].imshow(prediction, cmap='Blues', vmin=0, vmax=1)
    axes[0, 4].set_title(f'Model Prediction')
    axes[0, 4].axis('off')
    plt.colorbar(pred_im, ax=axes[0, 4], fraction=0.046, pad=0.04)
    
    axes[0, 5].imshow(hydro_mask, cmap='Blues')
    axes[0, 5].set_title('Ground Truth')
    axes[0, 5].axis('off')
    
    # Second row: Modality-specific attention maps
    if modality_attention_maps:
        # Use the first layer's modality attention maps
        first_modality_layer = list(modality_attention_maps.values())[0]
        modality_names = ['DEM', 'Optical', 'Thermal', 'SAR']
        
        for idx, modality_name in enumerate(modality_names):
            if modality_name in first_modality_layer:
                attention = first_modality_layer[modality_name]
                im = axes[1, idx].imshow(attention, cmap='hot', vmin=0, vmax=1)
                axes[1, idx].set_title(f'{modality_name} Attention')
                axes[1, idx].axis('off')
                plt.colorbar(im, ax=axes[1, idx], fraction=0.046, pad=0.04)
            else:
                axes[1, idx].axis('off')
        
        # Combined modality attention
        if len(first_modality_layer) >= 2:
            combined_modality = np.mean(list(first_modality_layer.values()), axis=0)
            im = axes[1, 4].imshow(combined_modality, cmap='hot', vmin=0, vmax=1)
            axes[1, 4].set_title('Combined Modality Attention')
            axes[1, 4].axis('off')
            plt.colorbar(im, ax=axes[1, 4], fraction=0.046, pad=0.04)
        else:
            axes[1, 4].axis('off')
        
        axes[1, 5].axis('off')
    else:
        for j in range(n_cols):
            axes[1, j].axis('off')
    
    # Combined overall attention map
    if len(overall_attention_maps) >= 2:
        combined_attention = np.mean(list(overall_attention_maps.values()), axis=0)
        im = axes[2, 0].imshow(combined_attention, cmap='hot', vmin=0, vmax=1)
        axes[2, 0].set_title('Combined Layer Attention')
        axes[2, 0].axis('off')
        plt.colorbar(im, ax=axes[2, 0], fraction=0.046, pad=0.04)
    else:
        axes[2, 0].axis('off')
    
    # Individual layer attention maps (start from position [2,1])
    plot_idx = 0
    for i in range(2, n_rows):
        for j in range(n_cols):
            if i == 2 and j == 0:
                continue  # Skip combined attention slot
            if plot_idx < len(overall_attention_maps):
                layer_name = list(overall_attention_maps.keys())[plot_idx]
                attention_map = overall_attention_maps[layer_name]
                
                im = axes[i, j].imshow(attention_map, cmap='hot', vmin=0, vmax=1)
                # Clean layer name for display
                clean_name = layer_name.replace('_', ' ').replace('x', '×')
                axes[i, j].set_title(f'{clean_name}', fontsize=10)
                axes[i, j].axis('off')
                plt.colorbar(im, ax=axes[i, j], fraction=0.046, pad=0.04)
                plot_idx += 1
            else:
                axes[i, j].axis('off')
    
    plt.tight_layout()
    
    # Save with detailed naming
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    patch_filename = Path(patch_info['file_path']).stem
    output_dir = Path('/u/nathanj/national_ml/experiments/evaluation/visual_analysis/attention_map/real_gradcam_tests')
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / f'multi_layer_attention_landsat6b_{huc_id}_{patch_filename}_{timestamp}.png'
    plt.savefig(output_file, dpi=200, bbox_inches='tight')
    plt.close()
    
    print(f"\n✅ Multi-layer attention analysis saved: {output_file.name}")
    
    # Print detailed summary
    print(f"\n📊 Layer-by-Layer Analysis Summary:")
    print(f"{'Layer Name':<30} {'Resolution':<12} {'Range':<20} {'Active%':<8} {'Artifacts':<10}")
    print("-" * 85)
    
    for layer_name, attention_map in overall_attention_maps.items():
        active_pixels = np.sum(attention_map > 0.1)
        total_pixels = attention_map.size
        
        # Determine approximate resolution from layer name
        if '224x224' in layer_name:
            resolution = '224×224'
        elif '112x112' in layer_name:
            resolution = '112×112'
        elif '56x56' in layer_name:
            resolution = '56×56'
        elif '28x28' in layer_name:
            resolution = '28×28'
        elif '14x14' in layer_name:
            resolution = '14×14'
        else:
            resolution = 'Unknown'
        
        # Check for artifacts (simplified)
        top_quarter = attention_map[:56, :]
        artifact_detected = "Yes" if top_quarter.mean() > attention_map.mean() * 2.0 else "No"
        
        print(f"{layer_name:<30} {resolution:<12} [{attention_map.min():.2f}, {attention_map.max():.2f}]"
              f"        {100*active_pixels/total_pixels:>5.1f}%   {artifact_detected:<10}")
    
    # Print modality-specific analysis summary
    if modality_attention_maps:
        print(f"\n📊 Modality-Specific Attention Summary:")
        print(f"{'Modality':<15} {'Mean Attention':<15} {'Max Attention':<15} {'Active%':<8}")
        print("-" * 60)
        
        # Use first layer's modality maps for summary
        first_modality_layer = list(modality_attention_maps.values())[0]
        for modality_name, attention_map in first_modality_layer.items():
            mean_attention = attention_map.mean()
            max_attention = attention_map.max()
            active_pixels = np.sum(attention_map > 0.1)
            total_pixels = attention_map.size
            
            print(f"{modality_name:<15} {mean_attention:<15.3f} {max_attention:<15.3f} {100*active_pixels/total_pixels:>5.1f}%")
    
    return attention_maps

if __name__ == "__main__":
    generate_multi_layer_attention_maps()