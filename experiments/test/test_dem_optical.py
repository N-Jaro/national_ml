#!/usr/bin/env python3
"""
Example script demonstrating how to use the DEM + Optical model.
This shows how to load data and run inference with the model.
"""

import os
import sys
import torch
import numpy as np
from typing import Dict

# Add the experiments directory to path to import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from models.mdmt_dem_optical import MultimodalMultitaskModel_DEM_Optical
from data.patchDataLoader_dem_optical import MultimodalPatchDataset_DEM_Optical

def load_model(checkpoint_path: str, device: torch.device, optical_channels: int = 6) -> MultimodalMultitaskModel_DEM_Optical:
    """Load a trained model from checkpoint."""
    model = MultimodalMultitaskModel_DEM_Optical(
        n_classes_task1=1,
        n_classes_task2=1,
        base_channels=64,
        optical_channels=optical_channels
    )
    
    checkpoint = torch.load(checkpoint_path, map_location=device)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.to(device)
    model.eval()
    
    return model

def run_inference_example(data_path: str, huc_codes: list, model_path: str = None, optical_channels: int = 6):
    """Run inference example on a few samples."""
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    # Create dataset
    dataset = MultimodalPatchDataset_DEM_Optical(
        base_path=data_path,
        huc_codes=huc_codes,
        optical_channels=optical_channels,
        warn_missing_stats=True
    )
    
    print(f"Dataset size: {len(dataset)}")
    
    if len(dataset) == 0:
        print("No valid samples found!")
        return
    
    # Create or load model
    if model_path and os.path.exists(model_path):
        print(f"Loading model from {model_path}")
        model = load_model(model_path, device, optical_channels)
    else:
        print("Creating new model (random weights)")
        model = MultimodalMultitaskModel_DEM_Optical(
            n_classes_task1=1,
            n_classes_task2=1,
            base_channels=64,
            optical_channels=optical_channels
        ).to(device)
        model.eval()
    
    # Run inference on a few samples
    num_samples = min(3, len(dataset))
    
    with torch.no_grad():
        for i in range(num_samples):
            print(f"\\n--- Sample {i + 1} ---")
            
            # Get sample
            sample = dataset[i]
            print(f"Sample path: {sample['path']}")
            
            # Add batch dimension
            dem = sample['dem'].unsqueeze(0).to(device)
            optical = sample['optical'].unsqueeze(0).to(device)
            
            print(f"DEM shape: {dem.shape}")
            print(f"Optical shape: {optical.shape}")
            
            # Run inference
            pred_task1, pred_task2 = model(dem, optical)
            
            print(f"Prediction 1 shape: {pred_task1.shape}")
            print(f"Prediction 2 shape: {pred_task2.shape}")
            
            # Print some statistics
            print(f"DEM stats: min={dem.min():.3f}, max={dem.max():.3f}, mean={dem.mean():.3f}")
            print(f"Optical stats: min={optical.min():.3f}, max={optical.max():.3f}, mean={optical.mean():.3f}")
            print(f"Pred1 stats: min={pred_task1.min():.3f}, max={pred_task1.max():.3f}, mean={pred_task1.mean():.3f}")
            print(f"Pred2 stats: min={pred_task2.min():.3f}, max={pred_task2.max():.3f}, mean={pred_task2.mean():.3f}")

def show_data_compatibility():
    """
    Show how to adapt existing data loading code to work with the DEM + Optical model.
    """
    print("\\n" + "="*60)
    print("DATA COMPATIBILITY GUIDE")
    print("="*60)
    
    print("\\nIf you have existing data in the MultimodalPatchDataset format:")
    print("\\nOLD (4 modality) data loading:")
    print("""
    batch = dataset[0]  # Returns dict with keys:
    # - 'm1': DEM (1, H, W)
    # - 'm2': Optical (6, H, W) 
    # - 'm3': Thermal (1, H, W)
    # - 'm4': SAR (1, H, W)
    # - 'hydro_mask': (H, W)
    # - 'flow_dir': (H, W)
    
    pred1, pred2 = model(batch['m1'], batch['m2'], batch['m3'], batch['m4'])
    """)
    
    print("\\nNEW (DEM + Optical) data loading:")
    print("""
    batch = dataset[0]  # Returns dict with keys:
    # - 'dem': DEM (1, H, W)  
    # - 'optical': Optical (6, H, W)
    # - 'hydro_mask': (H, W)
    # - 'flow_dir': (H, W)
    
    pred1, pred2 = model(batch['dem'], batch['optical'])
    """)
    
    print("\\nTo adapt existing training code:")
    print("1. Replace MultimodalPatchDataset with MultimodalPatchDataset_DEM_Optical")
    print("2. Change model forward call from model(m1, m2, m3, m4) to model(dem, optical)")
    print("3. Update data dictionary key access from 'm1', 'm2' to 'dem', 'optical'")
    
    print("\\nModel differences:")
    print("- Reduced from 4 encoders to 2 encoders (DEM + Optical)")
    print("- Fusion layer now handles 2 modalities instead of 4")
    print("- Same decoder architecture and output format")
    print(f"- Parameter count reduced significantly (compared to 4-modality model)")
    print("- Supports configurable optical channels (default: 6 bands)")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='DEM + Optical Model Example')
    parser.add_argument('--data_path', type=str, 
                        help='Path to patch data directory (optional for demonstration)')
    parser.add_argument('--huc_codes', type=str, nargs='+', default=['10020007'],
                        help='List of HUC codes to test')
    parser.add_argument('--model_path', type=str,
                        help='Path to trained model checkpoint (optional)')
    parser.add_argument('--optical_channels', type=int, default=6,
                        help='Number of optical channels (default: 6)')
    parser.add_argument('--demo_only', action='store_true',
                        help='Only show compatibility guide without running inference')
    
    args = parser.parse_args()
    
    # Always show the compatibility guide
    show_data_compatibility()
    
    if args.demo_only:
        return
        
    if args.data_path and os.path.exists(args.data_path):
        print("\\n" + "="*60)
        print("RUNNING INFERENCE EXAMPLE")
        print("="*60)
        run_inference_example(args.data_path, args.huc_codes, args.model_path, args.optical_channels)
    else:
        print(f"\\nData path not provided or doesn't exist: {args.data_path}")
        print("Use --data_path to specify your patch data directory")
        print("Example: python example_dem_optical.py --data_path /path/to/your/data --huc_codes 10020007")

if __name__ == '__main__':
    main()