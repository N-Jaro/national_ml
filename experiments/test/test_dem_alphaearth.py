#!/usr/bin/env python3
"""
Example script for DEM + AlphaEarth multitask model.

This script demonstrates:
1. Loading the DEM + AlphaEarth model
2. Creating sample data and running inference
3. Showcasing model capabilities with different configurations
4. Performance analysis and comparison guidance

The DEM + AlphaEarth model combines:
- DEM (Digital Elevation Model) for explicit topographic information
- AlphaEarth (Google's satellite embeddings) for rich Earth surface patterns

This fusion provides both precise topographic data and contextual information
from Google's foundation model embeddings for enhanced hydrologic modeling.
"""

import os
import sys
import torch
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# Add experiments directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.mdmt_dem_alphaearth import MultimodalMultitaskModel_DEM_AlphaEarth
from data.patchDataLoader_dem_alphaearth import MultimodalPatchDataset_DEM_AlphaEarth, create_dataloader_dem_alphaearth

def demonstrate_model_architecture():
    """Demonstrate the DEM + AlphaEarth model architecture and capabilities."""
    
    print("=" * 60)
    print("DEM + AlphaEarth Multitask Model Demonstration")
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
        model = MultimodalMultitaskModel_DEM_AlphaEarth(
            n_classes_task1=1,  # Water segmentation (binary)
            n_classes_task2=8,  # D8 flow direction (8 classes)
            base_channels=64,
            alphaearth_channels=config["channels"]
        ).to(device)
        
        # Generate sample data
        batch_size = 2
        H, W = 224, 224
        dem = torch.randn(batch_size, 1, H, W).to(device)
        alphaearth = torch.randn(batch_size, config["channels"], H, W).to(device)
        
        print(f"Input DEM shape: {dem.shape}")
        print(f"Input AlphaEarth shape: {alphaearth.shape}")
        
        # Forward pass
        with torch.no_grad():
            water_output, flow_output = model(dem, alphaearth)
            
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
    """Demonstrate data loading for DEM + AlphaEarth model."""
    
    print("\n" + "=" * 60)
    print("DEM + AlphaEarth Data Loading Demonstration")
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
        dataset = MultimodalPatchDataset_DEM_AlphaEarth(
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
            dataloader = create_dataloader_dem_alphaearth(
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
                print(f"Batch DEM shape: {batch['dem'].shape}")
                print(f"Batch AlphaEarth shape: {batch['alphaearth'].shape}")
                print(f"Batch hydro_mask shape: {batch['hydro_mask'].shape}")
                print(f"Batch flow_dir shape: {batch['flow_dir'].shape}")
                break
                
        else:
            print("No valid patches found. Please check your data directory and HUC codes.")
            
    except Exception as e:
        print(f"Error loading data: {e}")
        print("This is expected if you don't have processed DEM + AlphaEarth data yet.")

def demonstrate_training_setup():
    """Show how to prepare for training the DEM + AlphaEarth model."""
    
    print("\n" + "=" * 60)
    print("Training Setup Demonstration")
    print("=" * 60)
    
    print("To train the DEM + AlphaEarth model, you would create a training script similar to:")
    print()
    print("cd /u/nathanj/national_ml/experiments/training")
    print("python run_lightning_train_dem_alphaearth.py \\")
    print("    --hucs 10020007,03030005 \\")
    print("    --batch_size 8 \\")
    print("    --epochs 50 \\")
    print("    --lr 1e-3 \\")
    print("    --alphaearth_channels 64 \\")
    print("    --wandb_project my-dem-alphaearth-experiments \\")
    print("    --wandb_run dem-alphaearth-64ch-v1")
    print()
    print("Key parameters:")
    print("  --alphaearth_channels: Number of embedding channels (32, 64, 128)")
    print("  --water_loss_scale: Weight for water segmentation loss (default: 1.0)")
    print("  --d8_loss_scale: Weight for flow direction loss (default: 0.5)")
    print("  --no_dynamic_weighter: Disable uncertainty-based loss weighting")
    print()
    print("Note: You'll need to create the training script following the pattern")
    print("of other model variants in the experiments/training/ directory.")

def compare_with_other_variants():
    """Compare DEM + AlphaEarth with other model variants."""
    
    print("\n" + "=" * 60)
    print("Model Variant Comparison")
    print("=" * 60)
    
    print("DEM + AlphaEarth Model vs Other Variants:")
    print()
    print("Advantages:")
    print("  ✓ Combines explicit topographic data (DEM) with rich embeddings (AlphaEarth)")
    print("  ✓ DEM provides precise elevation for hydrologic modeling")
    print("  ✓ AlphaEarth adds contextual Earth surface patterns from foundation model")
    print("  ✓ Leverages Google's state-of-the-art satellite embedding technology")
    print("  ✓ DEM is universally available and well-understood for hydrology")
    print("  ✓ Strong theoretical foundation - topography is fundamental to water flow")
    print()
    print("Considerations:")
    print("  • Higher computational cost due to AlphaEarth's 64-channel inputs")
    print("  • Requires both DEM and AlphaEarth data to be processed")
    print("  • AlphaEarth embeddings are less interpretable than raw sensor data")
    print("  • May be redundant if AlphaEarth already captures topographic information")
    print()
    print("Best for:")
    print("  - Combining interpretable topographic data with foundation model power")
    print("  - Situations requiring explicit elevation information")
    print("  - Leveraging both traditional hydrologic principles and modern AI embeddings")
    print("  - Research into optimal fusion of explicit and learned features")
    print()
    print("Comparison with other variants:")
    print("  • vs DEM-only: Adds rich contextual information beyond just elevation")
    print("  • vs AlphaEarth-only: Adds explicit topographic structure and interpretability")
    print("  • vs DEM+Optical: AlphaEarth embeddings vs. raw optical spectral bands")
    print("  • vs DEM+SAR: AlphaEarth foundation model vs. weather-independent radar")
    print("  • vs DEM+Thermal: AlphaEarth contextual info vs. direct thermal measurements")

def performance_analysis_tips():
    """Provide tips for analyzing DEM + AlphaEarth model performance."""
    
    print("\n" + "=" * 60)
    print("Performance Analysis Tips")
    print("=" * 60)
    
    print("Key metrics to monitor:")
    print("  • Water segmentation: Dice coefficient, IoU, precision/recall")
    print("  • Flow direction: Multi-class accuracy (micro/macro)")
    print("  • Loss components: Water loss vs. D8 flow loss balance")
    print("  • Dynamic weighting: Uncertainty parameters (log-variance)")
    print("  • Modality contribution: Attention weights between DEM and AlphaEarth")
    print()
    print("Visualization strategies:")
    print("  • DEM elevation maps vs predictions")
    print("  • First 3 AlphaEarth channels as pseudo-RGB")
    print("  • Attention fusion maps to see which modality dominates where")
    print("  • Overlay predictions on elevation gradients")
    print("  • Compare DEM gradients with predicted flow directions")
    print()
    print("Hyperparameter tuning:")
    print("  • AlphaEarth channels: 32, 64, 128 (memory vs. information trade-off)")
    print("  • Base channels: 32, 64, 128 (model capacity)")
    print("  • Learning rate: Start with 1e-3, may need different rates for modalities")
    print("  • Loss scaling: Balance water (1.0) vs. flow (0.5) task importance")
    print("  • Attention fusion parameters: How much to weight each modality")
    print()
    print("Ablation studies:")
    print("  • DEM-only vs AlphaEarth-only vs DEM+AlphaEarth")
    print("  • Different AlphaEarth channel numbers (32, 64, 128)")
    print("  • With/without attention fusion (simple concatenation vs. hierarchical)")
    print("  • DEM as primary vs. AlphaEarth as primary modality")
    print("  • Impact of DEM normalization strategies")
    print()
    print("Debugging strategies:")
    print("  • Check DEM elevation ranges and normalization")
    print("  • Verify AlphaEarth embedding statistics")
    print("  • Monitor gradient flow through both encoder branches")
    print("  • Analyze attention weights during training")
    print("  • Compare performance on different terrain types (flat vs. mountainous)")

def main():
    """Run all demonstrations."""
    
    print("DEM + AlphaEarth Multitask Model - Complete Demonstration")
    print("This script showcases the DEM + AlphaEarth model for water segmentation")
    print("and D8 flow direction prediction using topographic and embedding data.")
    
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
    print("1. Ensure you have processed DEM and AlphaEarth data in your patch dataset")
    print("2. Create a training script following the pattern of other model variants")
    print("3. Run training with appropriate HUC codes and hyperparameters")
    print("4. Monitor training progress with W&B logging")
    print("5. Compare results with other model variants")
    print("6. Analyze the fusion of topographic and embedding features")
    print("7. Conduct ablation studies to understand modality contributions")

if __name__ == "__main__":
    main()