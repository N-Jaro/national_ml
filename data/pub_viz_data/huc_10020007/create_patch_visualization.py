#!/usr/bin/env python3
"""
Patch Visualization Script for National ML Project

This script randomly selects and visualizes patch data from t    fig.suptitle(f'Multimodal Patch Visualization - HUC {huc_id}\\n'
                 f'{len(selected_patches)} patches (224×224 pixels each)\\n'
                 f'Blue rectangles show 10m resolution footprint on 30m data (Optical, Thermal)', 
                 fontsize=14, fontweight='bold', y=0.98)rocessed dataset.
It can be used to examine training/testing patches for quality assessment and 
publication figures.

Features:
- Random patch selection from available HUCs
- Multi-modal visualization (DEM, Optical, SAR, Thermal, etc.)
- Normalization and proper scaling
- Ground truth overlay (water mask, flow direction)
- Publication-quality output
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.colors import ListedColormap, BoundaryNorm
import json
import random
import glob
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def load_patch_data(patch_path):
    """Load patch data from NPZ file."""
    try:
        patch = np.load(patch_path)
        # Data is stored as (H, W) or (H, W, C) format
        return {
            'dem': patch['dem'],                    # (224, 224)
            'optical': patch['optical'],            # (224, 224, 6)
            'thermal': patch['thermal'],            # (224, 224)
            'sar': patch['sar'],                   # (224, 224)
            'alphaearth': patch['alphaearth'],     # (224, 224, 64)
            'hydro_mask': patch['hydro_mask'],     # (224, 224)
            'flow_dir': patch['flow_dir']          # (224, 224)
        }
    except Exception as e:
        print(f"Error loading {patch_path}: {e}")
        return None

def load_normalization_stats(stats_path):
    """Load normalization statistics."""
    try:
        with open(stats_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading stats {stats_path}: {e}")
        return None

def normalize_data(data, stats, modality, band_name):
    """Normalize data using stored statistics."""
    if modality not in stats or band_name not in stats[modality]:
        return data
    
    mean = stats[modality][f"{band_name}_mean"]
    std = stats[modality][f"{band_name}_stdDev"]
    
    # Z-score normalization
    return (data - mean) / std

def create_rgb_composite(optical_data):
    """Create RGB composite from optical bands."""
    try:
        if optical_data.ndim == 3 and optical_data.shape[2] >= 3:
            # Use bands 4,3,2 (R,G,B) for natural color (0-indexed: 3,2,1)
            # Data is in (H, W, C) format
            rgb = optical_data[:, :, [3,2,1]]  # Shape: (224, 224, 3)
            
            # Normalize to 0-1 range using percentile stretch
            rgb_norm = np.zeros_like(rgb)
            for i in range(3):
                band = rgb[:, :, i]
                p2, p98 = np.percentile(band, (2, 98))
                rgb_norm[:, :, i] = np.clip((band - p2) / (p98 - p2 + 1e-8), 0, 1)
            
            return rgb_norm
        else:
            return np.zeros((224, 224, 3))
    except Exception as e:
        print(f"Error creating RGB composite: {e}")
        return np.zeros((224, 224, 3))

def visualize_patches(huc_id='03030005', num_patches=6, random_seed=42):
    """
    Create patch visualization for a given HUC.
    
    Parameters:
    - huc_id: HUC identifier (default: 03030005)
    - num_patches: Number of random patches to visualize
    - random_seed: Random seed for reproducibility
    """
    
    print(f"🎯 Creating patch visualization for HUC {huc_id}")
    print("=" * 60)
    
    # Set random seed for reproducibility
    random.seed(random_seed)
    np.random.seed(random_seed)
    
    # Data paths
    base_path = Path("/u/nathanj/national_ml/data/processed/patch_dataset")
    huc_path = base_path / huc_id
    
    if not huc_path.exists():
        print(f"❌ HUC {huc_id} not found in processed data")
        print("Available HUCs:")
        for huc_dir in sorted(base_path.glob("*")):
            if huc_dir.is_dir():
                print(f"   • {huc_dir.name}")
        return
    
    # Load normalization stats
    stats_path = huc_path / "normalization_stats.json"
    stats = load_normalization_stats(stats_path)
    if stats is None:
        print("❌ Could not load normalization statistics")
        return
    
    # Find all patch files
    patch_files = sorted(list(huc_path.glob("patch_*.npz")))
    if len(patch_files) == 0:
        print("❌ No patch files found")
        return
    
    print(f"📊 Found {len(patch_files)} patches")
    
    # Randomly select patches
    selected_patches = random.sample(patch_files, min(num_patches, len(patch_files)))
    print(f"🎲 Selected {len(selected_patches)} random patches")
    
    # Create visualization (7 columns for the new layout)
    fig, axes = plt.subplots(len(selected_patches), 7, figsize=(21, 3*len(selected_patches)))
    if len(selected_patches) == 1:
        axes = axes.reshape(1, -1)
    
    fig.suptitle(f'Random Patch Visualization - HUC {huc_id}\n'
                 f'{len(selected_patches)} patches (224×224 pixels each)\n'
                 f'Blue rectangles show 10m resolution footprint on 30m data (Optical, Thermal)', 
                 fontsize=14, fontweight='bold', y=0.98)
    
    # Column labels with resolution information
    column_labels = ['(a) DEM\n10m', '(b) Optical RGB\n30m', '(c) SAR\n10m', '(d) Thermal\n30m', 
                    '(e) AlphaEarth\n10m', '(f) Water Mask\n10m', '(g) Flow Direction\n10m']
    
    for patch_idx, patch_file in enumerate(selected_patches):
        print(f"📈 Processing {patch_file.name}...")
        
        # Load patch data
        patch_data = load_patch_data(patch_file)
        if patch_data is None:
            continue
        
        # Extract patch number for title
        patch_num = patch_file.stem.split('_')[1]
        
        # Process each modality
        for col_idx, (label, modality) in enumerate(zip(column_labels, 
                                                       ['dem', 'optical', 'sar', 'thermal', 
                                                        'alphaearth', 'hydro_mask', 'flow_dir'])):
            ax = axes[patch_idx, col_idx]
            
            try:
                if modality == 'dem' and 'dem' in patch_data:
                    data = patch_data['dem']  # Shape: (224, 224)
                    # Normalize for visualization
                    data_norm = (data - data.min()) / (data.max() - data.min() + 1e-8)
                    im = ax.imshow(data_norm, cmap='terrain')
                    plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    
                elif modality == 'optical' and 'optical' in patch_data:
                    rgb = create_rgb_composite(patch_data['optical'])
                    ax.imshow(rgb)
                    
                    # Add 10m resolution footprint overlay (blue rectangle)
                    # Optical is 30m resolution, show where 10m data (DEM, SAR) would be
                    center = 224 // 2
                    footprint_size = 224 // 3  # ~74 pixels (10m footprint on 30m data)
                    start = center - footprint_size // 2
                    
                    rect = patches.Rectangle((start, start), footprint_size, footprint_size, 
                                           linewidth=2, edgecolor='blue', facecolor='none', alpha=0.8)
                    ax.add_patch(rect)
                    
                elif modality == 'sar' and 'sar' in patch_data:
                    data = patch_data['sar']  # Shape: (224, 224)
                    # Normalize for visualization
                    data_norm = (data - data.min()) / (data.max() - data.min() + 1e-8)
                    im = ax.imshow(data_norm, cmap='gray')
                    plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    
                elif modality == 'thermal' and 'thermal' in patch_data:
                    data = patch_data['thermal']  # Shape: (224, 224)
                    # Normalize for visualization
                    data_norm = (data - data.min()) / (data.max() - data.min() + 1e-8)
                    im = ax.imshow(data_norm, cmap='hot')
                    plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    
                    # Add 10m resolution footprint overlay (blue rectangle)
                    # Thermal is 30m resolution, show where 10m data (DEM, SAR) would be
                    center = 224 // 2
                    footprint_size = 224 // 3  # ~74 pixels (10m footprint on 30m data)
                    start = center - footprint_size // 2
                    
                    rect = patches.Rectangle((start, start), footprint_size, footprint_size, 
                                           linewidth=2, edgecolor='blue', facecolor='none', alpha=0.8)
                    ax.add_patch(rect)
                    
                elif modality == 'hydro_mask' and 'hydro_mask' in patch_data:
                    data = patch_data['hydro_mask'].squeeze()
                    cmap = ListedColormap(['lightgray', 'blue'])
                    im = ax.imshow(data, cmap=cmap)
                    plt.colorbar(im, ax=ax, shrink=0.6, aspect=20, 
                               ticks=[0, 1], label='Water')
                    
                elif modality == 'flow_dir' and 'flow_dir' in patch_data:
                    data = patch_data['flow_dir'].squeeze()
                    
                    # D8 flow direction colormap
                    d8_colors = ['#000000', '#FF0000', '#FF8000', '#FFFF00', '#80FF00', 
                                '#00FF00', '#00FF80', '#00FFFF', '#0080FF', '#0000FF']
                    d8_cmap = ListedColormap(d8_colors)
                    
                    unique_vals = np.unique(data[data > 0])
                    if len(unique_vals) > 0:
                        bounds = [0] + sorted(unique_vals.tolist()) + [max(unique_vals) + 1]
                        norm = BoundaryNorm(bounds, d8_cmap.N)
                        im = ax.imshow(data, cmap=d8_cmap, norm=norm)
                    else:
                        im = ax.imshow(data, cmap='gray')
                    
                    plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                    
                elif modality == 'alphaearth' and 'alphaearth' in patch_data:
                    # AlphaEarth embeddings visualization (use first 3 bands as RGB)
                    data = patch_data['alphaearth']  # Shape: (224, 224, 64)
                    if data.shape[2] >= 3:
                        # Use first 3 bands as RGB composite
                        rgb_data = data[:, :, :3]
                        # Normalize each band to 0-1 range
                        rgb_norm = np.zeros_like(rgb_data)
                        for i in range(3):
                            band = rgb_data[:, :, i]
                            p2, p98 = np.percentile(band, (2, 98))
                            rgb_norm[:, :, i] = np.clip((band - p2) / (p98 - p2 + 1e-8), 0, 1)
                        ax.imshow(rgb_norm)
                    else:
                        # Fallback: show first band as grayscale
                        band_data = data[:, :, 0]
                        data_norm = (band_data - band_data.min()) / (band_data.max() - band_data.min() + 1e-8)
                        im = ax.imshow(data_norm, cmap='viridis')
                        plt.colorbar(im, ax=ax, shrink=0.6, aspect=20)
                
                else:
                    ax.text(0.5, 0.5, f'No {modality}\\nData', transform=ax.transAxes, 
                           ha='center', va='center', fontsize=10)
                    
            except Exception as e:
                ax.text(0.5, 0.5, f'Error\\n{str(e)[:20]}...', transform=ax.transAxes, 
                       ha='center', va='center', fontsize=8)
            
            # Set title for first row
            if patch_idx == 0:
                ax.set_title(label, fontsize=11, fontweight='bold', pad=10)
            
            # Set row label
            if col_idx == 0:
                ax.set_ylabel(f'Patch {patch_num}', fontsize=10, fontweight='bold')
            
            # Remove axis ticks
            ax.set_xticks([])
            ax.set_yticks([])
            
            # Add patch boundaries
            rect = patches.Rectangle((0, 0), 223, 223, linewidth=1, 
                                   edgecolor='red', facecolor='none', alpha=0.5)
            ax.add_patch(rect)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save visualization
    output_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/visualizations")
    output_dir.mkdir(exist_ok=True)
    
    output_path = output_dir / f"patch_visualization_huc_{huc_id}_samples_{len(selected_patches)}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    
    # Also save as PDF
    output_path_pdf = output_dir / f"patch_visualization_huc_{huc_id}_samples_{len(selected_patches)}.pdf"
    plt.savefig(output_path_pdf, bbox_inches='tight', facecolor='white')
    
    print(f"\\n💾 Saved patch visualization:")
    print(f"   PNG: {output_path}")
    print(f"   PDF: {output_path_pdf}")
    
    # Print patch summary
    print(f"\\n📊 Patch Summary:")
    print("=" * 40)
    for i, patch_file in enumerate(selected_patches):
        patch_data = load_patch_data(patch_file)
        if patch_data:
            modalities = list(patch_data.keys())
            patch_num = patch_file.stem.split('_')[1]
            print(f"Patch {patch_num}: {', '.join(modalities)}")
    
    print(f"\\n✅ Patch visualization completed for HUC {huc_id}!")
    return output_path, output_path_pdf

def copy_random_patches(source_huc='03030005', dest_dir=None, num_patches=10, 
                       train_test_split=True, random_seed=42):
    """
    Copy random patches to a destination directory for training/testing.
    
    Parameters:
    - source_huc: Source HUC to copy from
    - dest_dir: Destination directory (default: creates in pub_viz_data)
    - num_patches: Number of patches to copy
    - train_test_split: Whether to split into train/test directories
    - random_seed: Random seed for reproducibility
    """
    
    print(f"📋 Copying random patches from HUC {source_huc}")
    print("=" * 50)
    
    # Set random seed
    random.seed(random_seed)
    
    # Source and destination paths
    source_path = Path(f"/u/nathanj/national_ml/data/processed/patch_dataset/{source_huc}")
    
    if dest_dir is None:
        dest_dir = Path("/u/nathanj/national_ml/data/pub_viz_data/huc_10020007/sample_patches")
    else:
        dest_dir = Path(dest_dir)
    
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all patch files
    patch_files = sorted(list(source_path.glob("patch_*.npz")))
    if len(patch_files) == 0:
        print("❌ No patch files found")
        return
    
    # Randomly select patches
    selected_patches = random.sample(patch_files, min(num_patches, len(patch_files)))
    
    if train_test_split:
        # Split 80-20 for train-test
        split_idx = int(0.8 * len(selected_patches))
        train_patches = selected_patches[:split_idx]
        test_patches = selected_patches[split_idx:]
        
        # Create subdirectories
        train_dir = dest_dir / "train"
        test_dir = dest_dir / "test"
        train_dir.mkdir(exist_ok=True)
        test_dir.mkdir(exist_ok=True)
        
        # Copy files
        import shutil
        
        for patch_file in train_patches:
            shutil.copy2(patch_file, train_dir)
            # Also copy georeference template if exists
            georef_file = patch_file.parent / f"{patch_file.stem}_georef_template.tif"
            if georef_file.exists():
                shutil.copy2(georef_file, train_dir)
        
        for patch_file in test_patches:
            shutil.copy2(patch_file, test_dir)
            # Also copy georeference template if exists
            georef_file = patch_file.parent / f"{patch_file.stem}_georef_template.tif"
            if georef_file.exists():
                shutil.copy2(georef_file, test_dir)
        
        # Copy normalization stats to both directories
        stats_file = source_path / "normalization_stats.json"
        if stats_file.exists():
            shutil.copy2(stats_file, train_dir)
            shutil.copy2(stats_file, test_dir)
        
        print(f"✅ Copied {len(train_patches)} patches to train directory")
        print(f"✅ Copied {len(test_patches)} patches to test directory")
        
    else:
        # Copy all to single directory
        import shutil
        
        for patch_file in selected_patches:
            shutil.copy2(patch_file, dest_dir)
            # Also copy georeference template if exists
            georef_file = patch_file.parent / f"{patch_file.stem}_georef_template.tif"
            if georef_file.exists():
                shutil.copy2(georef_file, dest_dir)
        
        # Copy normalization stats
        stats_file = source_path / "normalization_stats.json"
        if stats_file.exists():
            shutil.copy2(stats_file, dest_dir)
        
        print(f"✅ Copied {len(selected_patches)} patches to {dest_dir}")
    
    print(f"📁 Destination: {dest_dir}")
    return dest_dir

if __name__ == "__main__":
    # List available HUCs
    base_path = Path("/u/nathanj/national_ml/data/processed/patch_dataset")
    available_hucs = [d.name for d in base_path.glob("*") if d.is_dir()]
    
    print("🌊 National ML Patch Visualization Tool")
    print("=" * 60)
    print(f"Available HUCs: {len(available_hucs)}")
    for huc in sorted(available_hucs[:10]):  # Show first 10
        print(f"   • {huc}")
    if len(available_hucs) > 10:
        print(f"   ... and {len(available_hucs) - 10} more")
    
    # Use first available HUC as example (or 03030005 if available)
    example_huc = '03030005' if '03030005' in available_hucs else available_hucs[0]
    print(f"\\n🎯 Using HUC {example_huc} for demonstration")
    
    try:
        # Create patch visualization
        png_path, pdf_path = visualize_patches(huc_id=example_huc, num_patches=4, random_seed=42)
        
        # Copy sample patches
        sample_dir = copy_random_patches(source_huc=example_huc, num_patches=20, 
                                       train_test_split=True, random_seed=42)
        
        print(f"\\n🎉 Patch analysis completed!")
        print(f"📊 Visualization: {png_path}")
        print(f"📋 Sample patches: {sample_dir}")
        
        # Show the plot
        plt.show()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()