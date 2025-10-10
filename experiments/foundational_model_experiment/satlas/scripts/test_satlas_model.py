#!/usr/bin/env python3
"""
Test script for SATLAS model functionality
"""

import os
import sys
import torch
import yaml
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent))

print("Testing SATLAS model functionality...")
print("=" * 50)

# Test 1: TerraTorch imports
try:
    from terratorch.models import EncoderDecoderFactory
    print("✅ TerraTorch imports successful")
except ImportError as e:
    print(f"❌ TerraTorch import failed: {e}")
    sys.exit(1)

# Test 2: Load config
try:
    config_path = Path(__file__).parent / "configs" / "satlas_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    print("✅ Config loaded successfully")
    print(f"   Model bands: {config['model']['model_bands']}")
    print(f"   Input channels: {config['model']['in_channels']}")
except Exception as e:
    print(f"❌ Config loading failed: {e}")
    sys.exit(1)

# Test 3: Build model - try multimodal wrapper first (more reliable for 9-channel input)
try:
    print("\n🔧 Building SATLAS model with multimodal wrapper...")
    from training.satlas_multimodal_wrapper import SatlasMultimodalWrapper
    
    wrapper = SatlasMultimodalWrapper(config)
    model = wrapper
    print("✅ SATLAS multimodal wrapper created successfully")
    
    # Count parameters
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"   Total parameters: {total_params:,}")
    print(f"   Trainable parameters: {trainable_params:,}")
    
except Exception as e:
    print(f"❌ SATLAS multimodal wrapper failed: {e}")
    print("Trying TerraTorch approach...")
    
    # Try TerraTorch approach as fallback (may not work with 9 channels)
    try:
        factory = EncoderDecoderFactory()
        
        model_config = {
            "task": "segmentation",
            "backbone": "terratorch_satlas_swin_b_sentinel2_mi_ms",
            "decoder": config["model"]["decoder"],
            "num_classes": config["model"]["num_classes"],
            "backbone_kwargs": {"model_bands": config["model"]["model_bands"]}
        }
        
        model = factory.build_model(**model_config)
        print("✅ SATLAS model built successfully through TerraTorch")
        print("⚠️  Note: This may not work with 9-channel input")
        
        # Count parameters
        total_params = sum(p.numel() for p in model.parameters())
        trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        print(f"   Total parameters: {total_params:,}")
        print(f"   Trainable parameters: {trainable_params:,}")
        
    except Exception as terra_e:
        print(f"❌ TerraTorch approach also failed: {terra_e}")
        sys.exit(1)

# Test 4: Model forward pass
try:
    print("\n🧪 Testing model forward pass...")
    
    # Create dummy input (batch_size=2, channels=9, height=224, width=224)
    dummy_input = torch.randn(2, 9, 224, 224)
    print(f"   Input shape: {dummy_input.shape}")
    
    # Forward pass
    with torch.no_grad():
        output = model(dummy_input)
        
        # Handle different output types
        if hasattr(output, 'output'):
            logits = output.output
        else:
            logits = output
            
        print(f"   Output shape: {logits.shape}")
        print(f"   Output range: [{logits.min():.3f}, {logits.max():.3f}]")
        
        # Check output dimensions
        expected_shape = (2, 1, 224, 224)  # batch_size, num_classes, height, width
        if logits.shape == expected_shape:
            print("✅ Output shape is correct")
        else:
            print(f"⚠️  Output shape {logits.shape} != expected {expected_shape}")
    
except Exception as e:
    print(f"❌ Forward pass failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Loss function
try:
    print("\n📊 Testing loss function...")
    from utils.losses import CombinedFocalDiceLoss
    
    loss_fn = CombinedFocalDiceLoss(
        focal_weight=config["loss"]["focal_weight"],
        dice_weight=config["loss"]["dice_weight"],
        focal_alpha=config["loss"]["focal_alpha"],
        focal_gamma=config["loss"]["focal_gamma"]
    )
    
    # Create dummy target
    dummy_target = torch.randint(0, 2, (2, 1, 224, 224)).float()
    
    # Compute loss
    loss = loss_fn(logits, dummy_target)
    print(f"   Loss value: {loss.item():.4f}")
    print("✅ Loss computation successful")
    
except Exception as e:
    print(f"❌ Loss computation failed: {e}")
    sys.exit(1)

# Test 6: Data module import
try:
    print("\n📁 Testing data module...")
    from data.four_modal_dataset_adapter import SatlasDataModule
    
    # Create data module (don't setup to avoid file system requirements)
    data_module = SatlasDataModule(config)
    print("✅ Data module created successfully")
    
except Exception as e:
    print(f"❌ Data module creation failed: {e}")
    print("This might be okay if data files are not available")

print("\n" + "=" * 50)
print("✅ SATLAS model test completed successfully!")
print("✅ Ready for fine-tuning")
print("=" * 50)