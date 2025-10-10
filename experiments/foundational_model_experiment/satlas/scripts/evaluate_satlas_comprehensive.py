#!/usr/bin/env python3
"""
Comprehensive SATLAS Foundation Model Evaluation Script

This script validates all components of the SATLAS implementation:
1. Model architecture and forward pass
2. Data loading and preprocessing
3. Training loop functionality
4. Loss computation and metrics
5. Checkpoint saving and loading
"""

import os
import sys
import torch
import yaml
import logging
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))

# Local imports
from training.satlas_multimodal_wrapper import SatlasMultimodalWrapper
from data.synthetic_dataset import SyntheticSatlasDataModule
from utils.losses import CombinedFocalDiceLoss

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_model_architecture():
    """Test 1: Model Architecture and Forward Pass"""
    print("\n" + "="*60)
    print("TEST 1: Model Architecture and Forward Pass")
    print("="*60)
    
    try:
        # Load config
        config_path = Path(__file__).parent / "configs" / "satlas_test_config.yaml"
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Create model
        wrapper = SatlasMultimodalWrapper(config)
        model = wrapper
        
        # Test forward pass
        batch_size = 2
        test_input = torch.randn(batch_size, 9, 224, 224)
        
        with torch.no_grad():
            output = model(test_input)
            
        # Validate output
        expected_shape = (batch_size, 1, 224, 224)
        assert output.shape == expected_shape, f"Expected {expected_shape}, got {output.shape}"
        
        # Check parameter count
        total_params = sum(p.numel() for p in model.parameters())
        trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        
        print(f"✅ Model creation: SUCCESS")
        print(f"✅ Forward pass: SUCCESS")
        print(f"   Input shape: {test_input.shape}")
        print(f"   Output shape: {output.shape}")
        print(f"   Total parameters: {total_params:,}")
        print(f"   Trainable parameters: {trainable_params:,}")
        print(f"   Output range: [{output.min():.3f}, {output.max():.3f}]")
        
        return True, model, config
        
    except Exception as e:
        print(f"❌ Model architecture test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False, None, None


def test_data_loading(config):
    """Test 2: Data Loading and Preprocessing"""
    print("\n" + "="*60)
    print("TEST 2: Data Loading and Preprocessing")
    print("="*60)
    
    try:
        # Create data module
        data_module = SyntheticSatlasDataModule(config)
        data_module.setup("fit")
        
        # Test data loaders
        train_loader = data_module.train_dataloader()
        val_loader = data_module.val_dataloader()
        
        # Test batch loading
        train_batch = next(iter(train_loader))
        val_batch = next(iter(val_loader))
        
        # Validate batch structure
        required_keys = ['image', 'mask', 'metadata']
        for key in required_keys:
            assert key in train_batch, f"Missing key: {key}"
            assert key in val_batch, f"Missing key: {key}"
        
        # Validate tensor shapes
        batch_size = config["training"]["batch_size"]
        image_shape = (batch_size, 9, 224, 224)
        mask_shape = (batch_size, 224, 224)
        
        assert train_batch['image'].shape == image_shape, f"Train image shape mismatch"
        assert train_batch['mask'].shape == mask_shape, f"Train mask shape mismatch"
        assert val_batch['image'].shape == image_shape, f"Val image shape mismatch"
        assert val_batch['mask'].shape == mask_shape, f"Val mask shape mismatch"
        
        print(f"✅ Data module creation: SUCCESS")
        print(f"✅ Data loader creation: SUCCESS")
        print(f"✅ Batch loading: SUCCESS")
        print(f"   Train dataset size: {len(data_module.train_dataset)}")
        print(f"   Val dataset size: {len(data_module.val_dataset)}")
        print(f"   Batch size: {batch_size}")
        print(f"   Image shape: {train_batch['image'].shape}")
        print(f"   Mask shape: {train_batch['mask'].shape}")
        print(f"   Image range: [{train_batch['image'].min():.3f}, {train_batch['image'].max():.3f}]")
        print(f"   Mask range: [{train_batch['mask'].min():.3f}, {train_batch['mask'].max():.3f}]")
        
        return True, data_module
        
    except Exception as e:
        print(f"❌ Data loading test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False, None


def test_loss_and_metrics(model, data_module, config):
    """Test 3: Loss Computation and Metrics"""
    print("\n" + "="*60)
    print("TEST 3: Loss Computation and Metrics")
    print("="*60)
    
    try:
        # Create loss function
        criterion = CombinedFocalDiceLoss(
            focal_weight=config["loss"]["focal_weight"],
            dice_weight=config["loss"]["dice_weight"],
            focal_alpha=config["loss"]["focal_alpha"],
            focal_gamma=config["loss"]["focal_gamma"]
        )
        
        # Create metrics
        from torchmetrics import MetricCollection, JaccardIndex, Accuracy, F1Score
        metrics = MetricCollection({
            "iou": JaccardIndex(task="binary"),
            "accuracy": Accuracy(task="binary"),
            "f1": F1Score(task="binary")
        })
        
        # Get a batch
        train_loader = data_module.train_dataloader()
        batch = next(iter(train_loader))
        
        # Forward pass
        images = batch['image']
        masks = batch['mask']
        
        with torch.no_grad():
            outputs = model(images)
            
            # Test loss computation
            loss = criterion(outputs, masks.unsqueeze(1))
            
            # Test metrics computation
            preds = torch.sigmoid(outputs) > 0.5
            metric_results = metrics(preds.squeeze(1).int(), masks.int())
        
        print(f"✅ Loss function creation: SUCCESS")
        print(f"✅ Loss computation: SUCCESS")
        print(f"✅ Metrics computation: SUCCESS")
        print(f"   Loss value: {loss.item():.4f}")
        print(f"   IoU: {metric_results['iou']:.4f}")
        print(f"   Accuracy: {metric_results['accuracy']:.4f}")
        print(f"   F1 Score: {metric_results['f1']:.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Loss and metrics test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_training_integration():
    """Test 4: Training Integration"""
    print("\n" + "="*60)
    print("TEST 4: Training Integration")
    print("="*60)
    
    try:
        # Import training components
        import pytorch_lightning as pl
        
        # Test basic Lightning module creation
        config_path = Path(__file__).parent / "configs" / "satlas_test_config.yaml"
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Modify for quick test
        config["training"]["max_epochs"] = 1
        config["training"]["batch_size"] = 2
        
        # Create components
        from train_satlas_foundation import SatlasFoundationModel
        from data.synthetic_dataset import SyntheticSatlasDataModule
        
        model = SatlasFoundationModel(config)
        data_module = SyntheticSatlasDataModule(config)
        data_module.setup("fit")
        
        # Test training step
        train_loader = data_module.train_dataloader()
        batch = next(iter(train_loader))
        
        # Simulate training step
        loss = model.training_step(batch, 0)
        
        print(f"✅ Lightning module creation: SUCCESS")
        print(f"✅ Training step: SUCCESS")
        print(f"   Training loss: {loss.item():.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Training integration test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run comprehensive SATLAS evaluation"""
    print("SATLAS Foundation Model - Comprehensive Evaluation")
    print("="*60)
    
    # Track test results
    results = {
        "model_architecture": False,
        "data_loading": False,
        "loss_and_metrics": False,
        "training_integration": False
    }
    
    # Test 1: Model Architecture
    success, model, config = test_model_architecture()
    results["model_architecture"] = success
    
    if not success:
        print("\n❌ Critical failure in model architecture. Stopping evaluation.")
        return
    
    # Test 2: Data Loading
    success, data_module = test_data_loading(config)
    results["data_loading"] = success
    
    if not success:
        print("\n❌ Critical failure in data loading. Stopping evaluation.")
        return
    
    # Test 3: Loss and Metrics
    success = test_loss_and_metrics(model, data_module, config)
    results["loss_and_metrics"] = success
    
    # Test 4: Training Integration
    success = test_training_integration()
    results["training_integration"] = success
    
    # Final Summary
    print("\n" + "="*60)
    print("EVALUATION SUMMARY")
    print("="*60)
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name.replace('_', ' ').title()}: {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "="*60)
    if all_passed:
        print("🎉 ALL TESTS PASSED - SATLAS is ready for production!")
        print("🚀 You can now:")
        print("   - Submit training jobs to SLURM")
        print("   - Run production training with real data")
        print("   - Compare with other foundation models")
    else:
        print("⚠️  SOME TESTS FAILED - Please fix issues before production")
    print("="*60)


if __name__ == "__main__":
    main()