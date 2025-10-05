#!/usr/bin/env python3
"""
Simple debug script to check basic data loading issues
"""
import os
import sys
import yaml
import torch
import numpy as np
from pathlib import Path

# Add paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

def debug_data_basics():
    """Debug basic data loading without TerraTorch."""
    print("🔍 DEBUGGING DATA BASICS")
    print("=" * 50)
    
    # Load config
    with open('configs/prithvi_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Check data path
    data_path = Path(config['data']['base_path'])
    print(f"✅ Data path: {data_path}")
    print(f"✅ Data path exists: {data_path.exists()}")
    
    if not data_path.exists():
        print("❌ CRITICAL: Data path does not exist!")
        return
    
    # Check HUC directories
    huc_codes = config['data']['huc_codes'][:3]  # Just check first 3
    print(f"✅ Checking {len(huc_codes)} HUCs...")
    
    valid_hucs = 0
    total_patches = 0
    
    for huc in huc_codes:
        huc_path = data_path / huc
        if huc_path.exists():
            patches = list(huc_path.glob("*.npz"))
            print(f"   {huc}: {len(patches)} patches")
            valid_hucs += 1
            total_patches += len(patches)
        else:
            print(f"   {huc}: MISSING")
    
    print(f"✅ Valid HUCs: {valid_hucs}/{len(huc_codes)}")
    print(f"✅ Total patches: {total_patches}")
    
    if valid_hucs == 0:
        print("❌ CRITICAL: No valid HUC directories found!")
        return
    
    # Check a sample patch
    sample_huc = huc_codes[0]
    sample_path = data_path / sample_huc
    if sample_path.exists():
        patches = list(sample_path.glob("*.npz"))
        if patches:
            sample_patch = patches[0]
            print(f"✅ Checking sample patch: {sample_patch}")
            
            try:
                with np.load(sample_patch) as npz:
                    print(f"   Keys: {list(npz.keys())}")
                    
                    # Check required keys
                    required_keys = ['dem', 'optical', 'thermal', 'sar', 'hydro_mask']
                    missing_keys = [k for k in required_keys if k not in npz.keys()]
                    
                    if missing_keys:
                        print(f"❌ Missing keys: {missing_keys}")
                    else:
                        print("✅ All required keys present")
                    
                    # Check shapes and data types
                    for key in required_keys:
                        if key in npz:
                            data = npz[key]
                            print(f"   {key}: shape={data.shape}, dtype={data.dtype}, "
                                  f"range=[{data.min():.3f}, {data.max():.3f}]")
                            
                            # Check for issues
                            has_nan = np.isnan(data).any()
                            has_inf = np.isinf(data).any()
                            if has_nan or has_inf:
                                print(f"   ❌ {key} has NaN={has_nan}, Inf={has_inf}")
                    
                    # Check mask values specifically
                    if 'hydro_mask' in npz:
                        mask = npz['hydro_mask']
                        unique_vals = np.unique(mask)
                        water_ratio = np.mean(mask)
                        print(f"   Mask unique values: {unique_vals}")
                        print(f"   Water ratio: {water_ratio:.4f}")
                        
                        if not np.all(np.isin(unique_vals, [0, 1])):
                            print("   ❌ Mask is not binary!")
                            
            except Exception as e:
                print(f"❌ Error loading patch: {e}")

def test_simple_loss():
    """Test loss function in isolation."""
    print("\n🔍 TESTING LOSS FUNCTION")
    print("=" * 50)
    
    # Import loss function
    try:
        sys.path.append(str(Path(__file__).parent.parent.parent))
        from utils.losses import CombinedFocalDiceLoss
        print("✅ Loss function imported successfully")
    except Exception as e:
        print(f"❌ Failed to import loss function: {e}")
        return
    
    # Create loss function
    try:
        loss_fn = CombinedFocalDiceLoss(
            focal_weight=0.7,
            dice_weight=0.3,
            focal_alpha=0.75,
            focal_gamma=1.5
        )
        print("✅ Loss function created")
    except Exception as e:
        print(f"❌ Failed to create loss function: {e}")
        return
    
    # Test with simple data
    batch_size, height, width = 2, 10, 10
    
    # Test scenarios
    scenarios = [
        ("All zeros", torch.zeros(batch_size, 1, height, width), torch.zeros(batch_size, height, width)),
        ("All ones", torch.ones(batch_size, 1, height, width), torch.ones(batch_size, height, width)),
        ("Perfect match", torch.ones(batch_size, 1, height, width), torch.ones(batch_size, height, width)),
        ("Random", torch.randn(batch_size, 1, height, width), torch.randint(0, 2, (batch_size, height, width)).float()),
        ("Severe imbalance", torch.randn(batch_size, 1, height, width) * 0.1, 
         torch.bernoulli(torch.full((batch_size, height, width), 0.01)).float())
    ]
    
    for name, logits, targets in scenarios:
        try:
            loss = loss_fn(logits, targets)
            water_ratio = targets.mean().item()
            print(f"   {name:15}: loss={loss.item():.4f}, water_ratio={water_ratio:.4f}")
            
            if torch.isnan(loss) or torch.isinf(loss) or loss.item() < 0:
                print(f"   ❌ Invalid loss value!")
                
        except Exception as e:
            print(f"   ❌ {name} failed: {e}")

def analyze_training_behavior():
    """Analyze what might cause the weird training behavior."""
    print("\n🔍 ANALYZING TRAINING BEHAVIOR")
    print("=" * 50)
    
    # Load config
    with open('configs/prithvi_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    print("Current training config:")
    training_config = config['training']
    for key, value in training_config.items():
        print(f"   {key}: {value}")
    
    print("\nCurrent loss config:")
    loss_config = config['loss']
    for key, value in loss_config.items():
        print(f"   {key}: {value}")
    
    # Identify potential issues
    issues = []
    
    if training_config['learning_rate'] > 1e-3:
        issues.append("Learning rate might be too high for foundation model")
    
    if training_config['batch_size'] < 4:
        issues.append("Very small batch size might cause unstable training")
    
    if loss_config['focal_gamma'] > 2.0:
        issues.append("High focal gamma might cause training instability")
    
    if loss_config['focal_alpha'] < 0.5:
        issues.append("Low focal alpha might not handle class imbalance well")
    
    if issues:
        print("\n⚠️  Potential issues:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("\n✅ No obvious config issues found")

if __name__ == "__main__":
    print("🐛 SIMPLE PRITHVI DEBUG")
    print("=" * 60)
    
    debug_data_basics()
    test_simple_loss()
    analyze_training_behavior()
    
    print("\n✅ Simple debug complete!")