#!/usr/bin/env python3
"""
Test script for SATLAS real HUC data loading

This script tests the real HUC data adapter to ensure it can load actual
satellite data from the processed NPZ files.
"""

import os
import sys
import yaml
import logging
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))

from data.four_modal_dataset_adapter import SatlasDataModule

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_real_data_loading():
    """Test loading real HUC data"""
    print("="*60)
    print("Testing SATLAS Real HUC Data Loading")
    print("="*60)
    
    try:
        # Load config
        config_path = Path(__file__).parent.parent / "configs" / "satlas_test_config.yaml"
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        print(f"Config loaded from: {config_path}")
        print(f"Base path: {config['data']['base_path']}")
        print(f"HUC codes: {config['data']['huc_codes']}")
        
        # Check if HUC directories exist
        base_path = Path(config['data']['base_path'])
        existing_hucs = []
        
        for huc_code in config['data']['huc_codes']:
            huc_dir = base_path / huc_code
            if huc_dir.exists():
                npz_files = list(huc_dir.glob("*.npz"))
                if npz_files:
                    existing_hucs.append(huc_code)
                    print(f"✅ HUC {huc_code}: {len(npz_files)} NPZ files found")
                else:
                    print(f"⚠️  HUC {huc_code}: Directory exists but no NPZ files")
            else:
                print(f"❌ HUC {huc_code}: Directory not found")
        
        if not existing_hucs:
            print("❌ No HUC data found! Cannot test real data loading.")
            return False
        
        # Update config to use only existing HUCs
        config['data']['huc_codes'] = existing_hucs[:2]  # Use first 2 for testing
        print(f"Testing with HUCs: {config['data']['huc_codes']}")
        
        # Create data module
        print("\n" + "-"*40)
        print("Creating SATLAS data module...")
        data_module = SatlasDataModule(config)
        
        # Setup datasets
        print("Setting up datasets...")
        data_module.setup("fit")
        
        # Get dataset info
        dataset_info = data_module.get_dataset_info()
        print(f"Dataset info: {dataset_info}")
        
        # Test data loaders
        print("\n" + "-"*40)
        print("Testing data loaders...")
        
        train_loader = data_module.train_dataloader()
        val_loader = data_module.val_dataloader()
        
        print(f"Train loader created: {len(train_loader)} batches")
        print(f"Val loader created: {len(val_loader)} batches")
        
        # Test loading a batch
        print("\n" + "-"*40)
        print("Testing batch loading...")
        
        train_batch = next(iter(train_loader))
        print(f"Train batch loaded successfully!")
        print(f"  Image shape: {train_batch['image'].shape}")
        print(f"  Mask shape: {train_batch['mask'].shape}")
        print(f"  Image range: [{train_batch['image'].min():.3f}, {train_batch['image'].max():.3f}]")
        print(f"  Mask range: [{train_batch['mask'].min():.3f}, {train_batch['mask'].max():.3f}]")
        
        # Check metadata
        metadata = train_batch['metadata']
        print(f"  Metadata keys: {list(metadata.keys())}")
        print(f"  HUC codes in batch: {set(metadata['huc_code'])}")
        
        val_batch = next(iter(val_loader))
        print(f"Val batch loaded successfully!")
        print(f"  Image shape: {val_batch['image'].shape}")
        print(f"  Mask shape: {val_batch['mask'].shape}")
        
        # Test multiple batches
        print("\n" + "-"*40)
        print("Testing multiple batch loading...")
        
        batch_count = 0
        for batch in train_loader:
            batch_count += 1
            if batch_count >= 3:  # Test first 3 batches
                break
        
        print(f"Successfully loaded {batch_count} training batches")
        
        print("\n" + "="*60)
        print("✅ REAL HUC DATA LOADING TEST PASSED!")
        print("✅ SATLAS can load actual satellite data")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ REAL HUC DATA LOADING TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main test function"""
    print("SATLAS Foundation Model - Real Data Loading Test")
    
    success = test_real_data_loading()
    
    if success:
        print("\n🎉 Real data loading is working!")
        print("🚀 You can now train SATLAS with actual HUC data")
    else:
        print("\n⚠️  Real data loading test failed")
        print("💡 This might be expected if HUC data is not available")
        print("💡 SATLAS will fall back to synthetic data automatically")


if __name__ == "__main__":
    main()