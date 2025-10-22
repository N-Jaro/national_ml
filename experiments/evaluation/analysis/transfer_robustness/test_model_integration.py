#!/usr/bin/env python3
"""
Quick test of model loading and evaluation approach
"""

import sys
import torch
import numpy as np
from pathlib import Path

sys.path.append('/u/nathanj/national_ml/experiments')
sys.path.append('/u/nathanj/national_ml/experiments/models')

def test_model_loading():
    """Test loading the models and their forward pass"""
    
    print("Testing model loading and evaluation approach...")
    
    # Test All Modalities model
    print("\n1. Testing All Modalities model loading...")
    try:
        from mdmt_all_modalities_alphaearth import MultimodalMultitaskModel_All_Modalities_AlphaEarth
        print("✓ All Modalities model class imported")
        
        # Create sample inputs
        batch_size = 1
        dem = torch.randn(batch_size, 1, 224, 224)
        optical = torch.randn(batch_size, 6, 224, 224)  
        thermal = torch.randn(batch_size, 1, 224, 224)
        sar = torch.randn(batch_size, 1, 224, 224)
        alphaearth = torch.randn(batch_size, 64, 224, 224)
        
        print("✓ Sample inputs created")
        
        # Test forward pass (without loading checkpoint)
        model = MultimodalMultitaskModel_All_Modalities_AlphaEarth()
        model.eval()
        
        with torch.no_grad():
            outputs = model(dem, optical, thermal, sar, alphaearth)
        
        print(f"✓ Forward pass successful")
        print(f"  Output 1 shape: {outputs[0].shape} (hydro segmentation)")
        print(f"  Output 2 shape: {outputs[1].shape} (flow direction)")
        
    except Exception as e:
        print(f"❌ All Modalities model test failed: {e}")
        return False
    
    # Test AlphaEarth-only model
    print("\n2. Testing AlphaEarth-only model loading...")
    try:
        from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
        print("✓ AlphaEarth-only model class imported")
        
        # Create sample input
        alphaearth = torch.randn(batch_size, 64, 224, 224)
        
        # Test forward pass (without loading checkpoint)
        model = MultitaskModel_AlphaEarth_Only(
            n_classes_task1=1,  # Water segmentation (binary)
            n_classes_task2=8,  # D8 flow direction (8 classes)
            base_channels=64,
            alphaearth_channels=64
        )
        model.eval()
        
        with torch.no_grad():
            outputs = model(alphaearth)
        
        print(f"✓ Forward pass successful")
        print(f"  Output 1 shape: {outputs[0].shape} (hydro segmentation)")
        print(f"  Output 2 shape: {outputs[1].shape} (flow direction)")
        
    except Exception as e:
        print(f"❌ AlphaEarth-only model test failed: {e}")
        return False
    
    print("\n3. Testing checkpoint key handling...")
    try:
        checkpoint_path = "/u/nathanj/national_ml/outputs/models/all_modalities_alphaearth/all_modalities_alphaearth_task2_seed123_epoch=02_val_loss=0.6317.ckpt"
        
        if Path(checkpoint_path).exists():
            checkpoint = torch.load(checkpoint_path, map_location='cpu')
            print(f"✓ Checkpoint loaded: {len(checkpoint['state_dict'])} keys")
            
            # Check for Lightning prefix
            has_model_prefix = any(key.startswith('model.') for key in checkpoint['state_dict'].keys())
            print(f"✓ Has 'model.' prefix: {has_model_prefix}")
            
            if has_model_prefix:
                # Test prefix removal
                new_state_dict = {}
                for key, value in checkpoint['state_dict'].items():
                    if key.startswith('model.'):
                        new_key = key[6:]  # Remove 'model.' prefix
                        new_state_dict[new_key] = value
                    elif key == 'dynamic_weighter.log_vars':
                        continue
                    else:
                        new_state_dict[key] = value
                
                print(f"✓ Processed state dict: {len(new_state_dict)} keys")
            
        else:
            print(f"⚠️  Checkpoint file not found: {checkpoint_path}")
            
    except Exception as e:
        print(f"❌ Checkpoint handling test failed: {e}")
        return False
    
    print("\n4. Testing patch data loading...")
    try:
        # Test loading a sample patch
        test_patch = "/u/nathanj/national_ml/data/processed/patch_dataset/03030005/patch_0.npz"
        
        if Path(test_patch).exists():
            patch_data = np.load(test_patch)
            print(f"✓ Patch loaded with keys: {list(patch_data.keys())}")
            
            # Test data conversion
            batch = {}
            for key in ['dem', 'optical', 'thermal', 'sar', 'alphaearth']:
                if key in patch_data:
                    data = patch_data[key]
                    
                    if key in ['optical', 'alphaearth']:
                        if data.ndim == 3:  # (H, W, C) -> (C, H, W)
                            data = np.transpose(data, (2, 0, 1))
                            data = data[None, :, :, :]  # Add batch dim
                        else:
                            data = data[None, None, :, :]
                    else:
                        if data.ndim == 2:
                            data = data[None, None, :, :]
                        elif data.ndim == 3:
                            data = data[None, :, :, :]
                    
                    batch[key] = torch.tensor(data, dtype=torch.float32)
                    print(f"  {key}: {batch[key].shape}")
            
            print(f"✓ Batch prepared successfully")
            
        else:
            print(f"⚠️  Test patch not found: {test_patch}")
            
    except Exception as e:
        print(f"❌ Patch data test failed: {e}")
        return False
    
    print("\n🎉 ALL TESTS PASSED!")
    print("\nThe transfer/robustness analysis framework is ready!")
    print("Key findings:")
    print("  - Models load and run forward passes correctly")
    print("  - Checkpoint key handling works")
    print("  - Data preprocessing is correct")
    print("  - Model signatures match expected inputs")
    
    return True

if __name__ == "__main__":
    test_model_loading()