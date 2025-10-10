#!/usr/bin/env python3
"""
Test SATLAS pretrained model using satlaspretrain-models package
This should work on GPU nodes
"""

import torch
import sys
from pathlib import Path

def test_satlaspretrain_models_gpu():
    """Test satlaspretrain-models on GPU."""
    print("Testing SATLAS pretrained model (satlaspretrain-models)")
    print("=" * 60)
    
    if not torch.cuda.is_available():
        print("❌ CUDA not available - this test requires GPU")
        print("Run this test on a GPU node with: srun --partition=gpu --gres=gpu:1 --pty bash")
        return False
    
    try:
        import satlaspretrain_models
        print("✓ satlaspretrain-models imported successfully")
        
        # Initialize weights manager
        weights_manager = satlaspretrain_models.Weights()
        print("✓ Weights manager initialized")
        
        # Test loading the model we'll use
        model_id = "Sentinel2_SwinB_MI_MS"  # Multi-Image Multi-Spectral
        print(f"Loading model: {model_id}")
        
        # Load backbone with FPN
        model = weights_manager.get_pretrained_model(model_id, fpn=True)
        print(f"✓ Pretrained model loaded successfully")
        print(f"✓ Model parameters: {sum(p.numel() for p in model.parameters()):,}")
        
        # Test forward pass
        print("\nTesting forward pass...")
        model.eval()
        model = model.cuda()
        
        with torch.no_grad():
            # For Sentinel-2 MS: 9 channels
            dummy_input = torch.randn(1, 9, 224, 224).cuda()
            output = model(dummy_input)
            print(f"✓ Forward pass successful")
            print(f"✓ Input shape: {dummy_input.shape}")
            print(f"✓ Output shapes: {[f.shape for f in output]}")
            print(f"✓ Output on GPU: {[f.is_cuda for f in output]}")
        
        print("\n✅ SATLAS satlaspretrain-models test PASSED!")
        print("Ready for training with pretrained SATLAS weights")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("SATLAS Pretrained Model Test (GPU Required)")
    print("Environment: terratorch_env")
    print("=" * 60)
    
    success = test_satlaspretrain_models_gpu()
    
    if success:
        print("\n🎉 Ready to run SATLAS pretrained training!")
        print("Submit job with: sbatch submit_satlas_satlaspretrain.sh")
    else:
        print("\n❌ Test failed. Check GPU availability and environment.")
        sys.exit(1)