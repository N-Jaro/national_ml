#!/usr/bin/env python3
"""
Multi-Layer Attention Analysis for AlphaEarth-Only Model
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

from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
from selected_patches import SELECTED_PATCHES

def normalize_alphaearth(alphaearth):
    """
    Normalize AlphaEarth features using per-feature z-score normalization.
    AlphaEarth embeddings are typically already normalized, but we can apply additional normalization.
    """
    # Apply per-channel normalization to AlphaEarth features
    alphaearth_normalized = np.zeros_like(alphaearth)
    for i in range(alphaearth.shape[2]):
        channel = alphaearth[:, :, i]
        mean = channel.mean()
        std = channel.std()
        alphaearth_normalized[:, :, i] = (channel - mean) / (std + 1e-6)
    
    return alphaearth_normalized

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
        output1, output2 = model(input_tensor)  # AlphaEarth-only model takes single input
        
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

def generate_multi_layer_attention_maps():
    """Generate attention maps across multiple model layers for AlphaEarth-only variant."""
    
    device = torch.device('cpu')
    print(f"🎯 Multi-Layer Attention Analysis - AlphaEarth-Only Model")
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
    
    # Load and prepare data (AlphaEarth-only uses AlphaEarth features only)
    try:
        data = np.load(patch_info['file_path'])
        hydro_mask = data.get('hydro_mask', None)
        # Check for AlphaEarth data
        alphaearth = data.get('alphaearth', None)
        
        if alphaearth is None or hydro_mask is None:
            print(f"❌ Missing required AlphaEarth data")
            return
        
        # Normalize AlphaEarth features
        alphaearth = normalize_alphaearth(alphaearth)
        
        # AlphaEarth data is (224, 224, 64), need to transpose to (64, 224, 224)
        input_data = alphaearth.transpose(2, 0, 1).astype(np.float32)  # Shape: (64, 224, 224)
        
    except Exception as e:
        print(f"❌ Error loading patch: {e}")
        return
    
    print(f"✅ Loaded patch data: {input_data.shape}")
    
    # Load model
    print(f"📦 Loading AlphaEarth-only model...")
    model = MultitaskModel_AlphaEarth_Only(n_classes_task1=1, n_classes_task2=8)
    
    # Note: You'll need to update this path to the actual AlphaEarth-only checkpoint
    checkpoint_path = '/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_checkpoint.ckpt'
    
    try:
        if os.path.exists(checkpoint_path):
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
        else:
            print(f"❌ Checkpoint not found: {checkpoint_path}")
            print("   Please update the checkpoint path for AlphaEarth-only model")
            return
    except Exception as e:
        print(f"❌ Error loading model: {e}")
        return
    
    # Generate model prediction
    print(f"🎯 Generating model prediction...")
    try:
        with torch.no_grad():
            input_tensor = torch.from_numpy(input_data).float().unsqueeze(0).to(device)
            output1, output2 = model(input_tensor)
            prediction = torch.sigmoid(output1).cpu().numpy()[0, 0]
        
        print(f"✅ Prediction generated - Range: [{prediction.min():.3f}, {prediction.max():.3f}]")
    except Exception as e:
        print(f"❌ Error generating prediction: {e}")
        return
    
    # Define layers to analyze for AlphaEarth-only architecture
    layer_configs = {
        'Encoder_Initial_224x224': 'inc.double_conv.5',
        'Encoder_Down1_112x112': 'down1.maxpool_conv.1.double_conv.5',
        'Encoder_Down2_56x56': 'down2.maxpool_conv.1.double_conv.5',
        'Encoder_Down3_28x28': 'down3.maxpool_conv.1.double_conv.5',
        'Encoder_Down4_14x14': 'down4.maxpool_conv.1.double_conv.5',
        'Decoder_Up1_28x28': 'up1.conv.double_conv.5',
        'Decoder_Up2_56x56': 'up2.conv.double_conv.5',
        'Decoder_Up3_112x112': 'up3.conv.double_conv.5',
        'Decoder_Up4_224x224': 'up4.conv.double_conv.5'
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
            
            # Generate attention map using manual gradients
            attention_map = get_gradients_manually(model, input_tensor, layer)
            
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
    
    # Create comprehensive multi-layer visualization
    n_layers = len(attention_maps)
    n_cols = 4
    n_rows = 3  # Top row: inputs, Bottom 2 rows: attention maps
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 3, n_rows * 2.5))
    
    # Top row: Input data and prediction
    # Show first AlphaEarth channel as input visualization
    axes[0, 0].imshow(alphaearth[:, :, 0], cmap='viridis')
    axes[0, 0].set_title('AlphaEarth Ch1')
    axes[0, 0].axis('off')
    
    pred_im = axes[0, 1].imshow(prediction, cmap='Blues', vmin=0, vmax=1)
    axes[0, 1].set_title(f'Model Prediction')
    axes[0, 1].axis('off')
    plt.colorbar(pred_im, ax=axes[0, 1], fraction=0.046, pad=0.04)
    
    axes[0, 2].imshow(hydro_mask, cmap='Blues')
    axes[0, 2].set_title('Ground Truth')
    axes[0, 2].axis('off')
    
    # Combined attention map
    if len(attention_maps) >= 2:
        combined_attention = np.mean(list(attention_maps.values()), axis=0)
        im = axes[0, 3].imshow(combined_attention, cmap='hot', vmin=0, vmax=1)
        axes[0, 3].set_title('Combined Attention')
        axes[0, 3].axis('off')
        plt.colorbar(im, ax=axes[0, 3], fraction=0.046, pad=0.04)
    else:
        axes[0, 3].axis('off')
    
    # Plot attention maps in remaining spots
    plot_idx = 0
    for i in range(1, n_rows):
        for j in range(n_cols):
            if plot_idx < len(attention_maps):
                layer_name = list(attention_maps.keys())[plot_idx]
                attention_map = attention_maps[layer_name]
                
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
    
    output_file = output_dir / f'multi_layer_attention_alphaearth_only_{huc_id}_{patch_filename}_{timestamp}.png'
    plt.savefig(output_file, dpi=200, bbox_inches='tight')
    plt.close()
    
    print(f"\n✅ Multi-layer attention analysis saved: {output_file.name}")
    
    # Print detailed summary
    print(f"\n📊 Layer-by-Layer Analysis Summary:")
    print(f"{'Layer Name':<30} {'Resolution':<12} {'Range':<20} {'Active%':<8} {'Artifacts':<10}")
    print("-" * 85)
    
    for layer_name, attention_map in attention_maps.items():
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
    
    return attention_maps

if __name__ == "__main__":
    generate_multi_layer_attention_maps()