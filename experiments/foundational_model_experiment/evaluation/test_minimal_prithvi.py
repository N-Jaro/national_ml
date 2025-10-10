#!/usr/bin/env python3
"""
Quick test to verify Prithvi can load and process a single HUC with minimal data.
"""

import sys
import os
from pathlib import Path

# Add paths for foundational model imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))

def test_minimal_prithvi():
    """Test minimal Prithvi functionality."""
    
    print("Testing minimal Prithvi evaluation...")
    
    # Test 1: Can we import the evaluator?
    try:
        from ultra_fast_prithvi_evaluator import FastFoundationDataset, compute_metrics_original
        print("✓ Successfully imported evaluator components")
    except Exception as e:
        print(f"✗ Failed to import evaluator: {e}")
        return False
    
    # Test 2: Can we load the dataset?
    try:
        test_hucs = ["03030005"]  # Use an available HUC
        base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
        
        dataset = FastFoundationDataset(base_path, test_hucs)
        print(f"✓ Dataset created with {len(dataset)} files")
        
        if len(dataset) == 0:
            print("✗ No valid files found in dataset")
            return False
            
    except Exception as e:
        print(f"✗ Failed to create dataset: {e}")
        return False
    
    # Test 3: Can we load a sample?
    try:
        sample = dataset[0]
        print(f"✓ Sample loaded - image shape: {sample['image'].shape}")
        
        # Verify 9-channel input
        if sample['image'].shape[0] != 9:
            print(f"✗ Wrong input channels: {sample['image'].shape[0]}, expected 9")
            return False
        else:
            print("✓ Correct 9-channel input format")
            
    except Exception as e:
        print(f"✗ Failed to load sample: {e}")
        return False
    
    # Test 4: Can we load the model checkpoint?
    try:
        from prithvi.training.train_prithvi import PrithviFoundationModel
        
        config_file = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config/prithvi_runs.txt"
        checkpoint_path = None
        
        with open(config_file, 'r') as f:
            for line in f:
                if line.startswith('run1|'):
                    checkpoint_path = line.split('|', 1)[1].strip()
                    break
        
        if not checkpoint_path or not os.path.exists(checkpoint_path):
            print(f"✗ Checkpoint not found: {checkpoint_path}")
            return False
        
        # Try to load model (this might take a while)
        print(f"Loading model from: {checkpoint_path}")
        model = PrithviFoundationModel.load_from_checkpoint(checkpoint_path)
        print(f"✓ Model loaded successfully with {sum(p.numel() for p in model.parameters()):,} parameters")
        
    except Exception as e:
        print(f"✗ Failed to load model: {e}")
        return False
    
    print("\n🎉 All minimal tests passed! Foundation model evaluation should work.")
    return True

if __name__ == "__main__":
    # This test should be run in terratorch_env
    import torch
    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")
    
    try:
        import terratorch
        print("✓ TerraTorch available")
    except ImportError:
        print("⚠️  TerraTorch not available - make sure you're in terratorch_env")
    
    print("\n" + "="*50)
    success = test_minimal_prithvi()
    
    if success:
        print("\n✅ Ready to run full evaluation!")
    else:
        print("\n❌ Fix issues above before running full evaluation.")