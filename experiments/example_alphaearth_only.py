#!/usr/bin/env python3
"""
Example script for AlphaEarth-only multitask model.

This script demonstrates:
1. Loading the AlphaEarth-only model
2. Creating sample data and running inference
3. Showcasing model capabilities with different embedding configurations
4. Performance analysis and comparison guidance

The AlphaEarth-only model uses Google's satellite embedding data to perform:
- Water segmentation (Task 1)
- D8 flow direction prediction (Task 2)
"""

import os
import sys
import torch
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# Add experiments directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
from data.patchDataLoader_alphaearth_only import MultimodalPatchDataset_AlphaEarth_Only, create_dataloader_alphaearth_only

def demonstrate_model_architecture():
    """Demonstrate the AlphaEarth-only model architecture and capabilities."""
    
    print("=" * 60)
    print("AlphaEarth-Only Multitask Model Demonstration")
    print("=" * 60)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    # Test different AlphaEarth configurations
    configs = [
        {"channels": 32, "description": "Reduced embedding (32 channels)"},
        {"channels": 64, "description": "Standard embedding (64 channels)"},
        {"channels": 128, "description": "Extended embedding (128 channels)"}
    ]
    
    for config in configs:
        print(f"\n--- {config['description']} ---")
        
        # Create model
        model = MultitaskModel_AlphaEarth_Only(
            n_classes_task1=1,  # Water segmentation (binary)
            n_classes_task2=8,  # D8 flow direction (8 classes)
            base_channels=64,
            alphaearth_channels=config["channels"]
        ).to(device)
        
        # Generate sample data
        batch_size = 2
        H, W = 224, 224
        alphaearth = torch.randn(batch_size, config["channels"], H, W).to(device)
        
        print(f"Input AlphaEarth shape: {alphaearth.shape}")
        
        # Forward pass
        with torch.no_grad():
            water_output, flow_output = model(alphaearth)
            
        print(f"Water segmentation output: {water_output.shape}")
        print(f"Flow direction output: {flow_output.shape}")
        
        # Model statistics
        total_params = sum(p.numel() for p in model.parameters())
        trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        
        print(f"Total parameters: {total_params:,}")
        print(f"Trainable parameters: {trainable_params:,}")
        print(f"Model size: {total_params * 4 / (1024 * 1024):.2f} MB")
        
        # Memory usage
        if torch.cuda.is_available():
            memory_allocated = torch.cuda.memory_allocated(device) / 1024**2
            print(f"GPU memory allocated: {memory_allocated:.2f} MB")

def demonstrate_data_loading():
    """Demonstrate data loading for AlphaEarth-only model."""
    
    print("\n" + "=" * 60)
    print("AlphaEarth Data Loading Demonstration")
    print("=" * 60)
    
    # Example data path (adjust to your actual data location)
    base_path = "/u/nathanj/national_ml/data/processed/patch_dataset/"
    huc_codes = ["10020007"]  # Example HUC
    
    if not os.path.exists(base_path):
        print(f"Data path {base_path} does not exist.")
        print("Please adjust the base_path to point to your processed patch data.")
        return
    
    try:
        # Create dataset
        dataset = MultimodalPatchDataset_AlphaEarth_Only(
            base_path=base_path,
            huc_codes=huc_codes,
            alphaearth_channels=64,
            warn_missing_stats=False  # Suppress warnings for demo
        )
        
        print(f"Dataset contains {len(dataset)} patches")
        
        if len(dataset) > 0:
            # Get a sample
            sample = dataset[0]
            print(f"\nSample data structure:")
            for key, value in sample.items():
                if torch.is_tensor(value):
                    print(f"  {key}: {value.shape} ({value.dtype})")
                else:
                    print(f"  {key}: {value}")
            
            # Create dataloader
            dataloader = create_dataloader_alphaearth_only(
                base_path=base_path,
                huc_codes=huc_codes,
                batch_size=4,
                shuffle=True,
                num_workers=0,  # Use 0 for demo to avoid multiprocessing issues
                alphaearth_channels=64
            )
            
            print(f"\nDataLoader created with batch_size=4")
            
            # Get one batch
            for batch in dataloader:
                print(f"Batch AlphaEarth shape: {batch['alphaearth'].shape}")
                print(f"Batch hydro_mask shape: {batch['hydro_mask'].shape}")
                print(f"Batch flow_dir shape: {batch['flow_dir'].shape}")
                break
                
        else:
            print("No valid patches found. Please check your data directory and HUC codes.")
            
    except Exception as e:
        print(f"Error loading data: {e}")
        print("This is expected if you don't have processed AlphaEarth data yet.")

def demonstrate_training_setup():
    """Show how to prepare for training the AlphaEarth-only model."""
    
    print("\n" + "=" * 60)
    print("Training Setup Demonstration")
    print("=" * 60)
    
    print("To train the AlphaEarth-only model, use:")
    print()
    print("cd /u/nathanj/national_ml/experiments/training")
    print("python run_lightning_train_alphaearth_only.py \\")
    print("    --hucs 10020007,03030005 \\")
    print("    --batch_size 8 \\")
    print("    --epochs 50 \\")
    print("    --lr 1e-3 \\")
    print("    --alphaearth_channels 64 \\")
    print("    --wandb_project my-alphaearth-experiments \\")
    print("    --wandb_run alphaearth-only-64ch-v1")
    print()
    print("Key parameters:")
    print("  --alphaearth_channels: Number of embedding channels (32, 64, 128)")
    print("  --water_loss_scale: Weight for water segmentation loss (default: 1.0)")
    print("  --d8_loss_scale: Weight for flow direction loss (default: 0.5)")
    print("  --no_dynamic_weighter: Disable uncertainty-based loss weighting")

def compare_with_other_variants():
    """Compare AlphaEarth-only with other model variants."""
    
    print("\n" + "=" * 60)
    print("Model Variant Comparison")
    print("=" * 60)
    
    print("AlphaEarth-only Model vs Other Variants:")
    print()
    print("Advantages:")
    print("  ✓ Rich pre-trained embeddings capture complex Earth surface patterns")
    print("  ✓ Single modality simplifies data pipeline and preprocessing")
    print("  ✓ Google's state-of-the-art satellite embedding technology")
    print("  ✓ Potentially strong performance from foundation model embeddings")
    print("  ✓ Consistent data availability (AlphaEarth is processed globally)")
    print()
    print("Considerations:")
    print("  • Relies entirely on pre-computed embeddings (less interpretable)")
    print("  • May miss modality-specific information (elevation, thermal, SAR)")
    print("  • Performance depends on quality of AlphaEarth embeddings for hydro tasks")
    print("  • Higher memory usage due to 64-channel inputs")
    print()
    print("Best for:")
    print("  - Leveraging state-of-the-art foundation model embeddings")
    print("  - Situations where other modalities are unreliable")
    print("  - Research into foundation model capabilities for hydro tasks")
    print("  - Simplified data pipelines with consistent global coverage")
    print()
    print("Comparison with other variants:")
    print("  • DEM-only: AlphaEarth may capture broader context beyond topography")
    print("  • DEM+Optical: AlphaEarth embeddings vs. raw optical bands")
    print("  • DEM+SAR: AlphaEarth vs. weather-independent radar data")
    print("  • DEM+Thermal: AlphaEarth vs. direct thermal information")

def performance_analysis_tips():
    """Provide tips for analyzing AlphaEarth-only model performance."""
    
    print("\n" + "=" * 60)
    print("Performance Analysis Tips")
    print("=" * 60)
    
    print("Key metrics to monitor:")
    print("  • Water segmentation: Dice coefficient, IoU, precision/recall")
    print("  • Flow direction: Multi-class accuracy (micro/macro)")
    print("  • Loss components: Water loss vs. D8 flow loss balance")
    print("  • Dynamic weighting: Uncertainty parameters (log-variance)")
    print()
    print("Visualization strategies:")
    print("  • First 3 AlphaEarth channels as pseudo-RGB")
    print("  • Average across all channels for overview")
    print("  • Channel-wise activation analysis")
    print("  • Comparison with ground truth and other modalities")
    print()
    print("Hyperparameter tuning:")
    print("  • AlphaEarth channels: 32, 64, 128 (memory vs. information trade-off)")
    print("  • Learning rate: Start with 1e-3, may need adjustment for embeddings")
    print("  • Loss scaling: Balance water (1.0) vs. flow (0.5) task importance")
    print("  • Base channels: Adjust model capacity (32, 64, 128)")
    print()
    print("Debugging strategies:")
    print("  • Check AlphaEarth data normalization and statistics")
    print("  • Monitor gradient flow through embedding processing layers")
    print("  • Compare performance with and without dynamic loss weighting")
    print("  • Analyze per-channel contribution and importance")

def main():
    """Run all demonstrations."""
    
    print("AlphaEarth-Only Multitask Model - Complete Demonstration")
    print("This script showcases the AlphaEarth-only model for water segmentation")
    print("and D8 flow direction prediction using Google's satellite embeddings.")
    
    # Run demonstrations
    demonstrate_model_architecture()
    demonstrate_data_loading()
    demonstrate_training_setup()
    compare_with_other_variants()
    performance_analysis_tips()
    
    print("\n" + "=" * 60)
    print("Demonstration Complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Ensure you have processed AlphaEarth data in your patch dataset")
    print("2. Run the training script with appropriate HUC codes")
    print("3. Monitor training progress with W&B logging")
    print("4. Compare results with other model variants")
    print("5. Analyze AlphaEarth embedding effectiveness for hydro tasks")

if __name__ == "__main__":
    main()