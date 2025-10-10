#!/usr/bin/env python3
"""
Quick test script to verify the foundation model evaluation system is working correctly.
Tests data loading, model interface, and metric computation without full evaluation.
"""

import sys
import torch
import numpy as np
from pathlib import Path
import logging

# Add paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_data_loading():
    """Test that the foundation dataset can load data correctly."""
    logger.info("Testing data loading...")
    
    try:
        from ultra_fast_prithvi_evaluator import FastFoundationDataset
        
        # Test with a small HUC list
        test_hucs = ["01030003"]  # First HUC from test list
        base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
        
        dataset = FastFoundationDataset(base_path, test_hucs)
        logger.info(f"Dataset created with {len(dataset)} potential files")
        
        # Try to load a sample
        if len(dataset) > 0:
            sample = dataset[0]
            logger.info(f"Sample loaded successfully:")
            logger.info(f"  Image shape: {sample['image'].shape}")
            logger.info(f"  Hydro mask shape: {sample['hydro_mask'].shape}")
            logger.info(f"  Flow dir shape: {sample['flow_dir'].shape}")
            logger.info(f"  Path: {Path(sample['path']).name}")
            
            # Verify 9-channel input
            if sample['image'].shape[0] == 9:
                logger.info("✓ Correct 9-channel input format")
            else:
                logger.error(f"✗ Wrong input channels: {sample['image'].shape[0]}, expected 9")
                return False
        else:
            logger.warning("No valid samples found in dataset")
            return False
            
        logger.info("✓ Data loading test passed")
        return True
        
    except Exception as e:
        logger.error(f"✗ Data loading test failed: {e}")
        return False

def test_metric_computation():
    """Test that metric computation works correctly."""
    logger.info("Testing metric computation...")
    
    try:
        from ultra_fast_prithvi_evaluator import compute_metrics_original
        
        # Create synthetic test data
        batch_size, height, width = 4, 224, 224
        
        # Water segmentation: binary prediction vs target
        water_pred = torch.randn(batch_size, 1, height, width)  # Logits
        water_target = torch.randint(0, 2, (batch_size, height, width)).float()
        
        # D8 flow direction: 9-class prediction vs target  
        d8_pred = torch.randn(batch_size, 9, height, width)  # Logits
        d8_target = torch.randint(0, 9, (batch_size, height, width))
        
        # Compute metrics
        metrics = compute_metrics_original(water_pred, water_target, d8_pred, d8_target)
        
        # Check that all expected metrics are present
        expected_metrics = [
            'water_dice', 'water_iou', 'water_accuracy', 'water_precision', 'water_recall', 'water_f1',
            'd8_accuracy', 'd8_precision_macro', 'd8_recall_macro', 'd8_f1_macro',
            'd8_precision_weighted', 'd8_recall_weighted', 'd8_f1_weighted'
        ]
        
        missing_metrics = [m for m in expected_metrics if m not in metrics]
        if missing_metrics:
            logger.error(f"✗ Missing metrics: {missing_metrics}")
            return False
        
        # Check that all metrics are in valid range [0, 1]
        invalid_metrics = []
        for metric, value in metrics.items():
            if not (0 <= value <= 1):
                invalid_metrics.append(f"{metric}: {value}")
        
        if invalid_metrics:
            logger.error(f"✗ Metrics out of range [0,1]: {invalid_metrics}")
            return False
        
        logger.info("✓ All metrics computed successfully")
        logger.info(f"  Sample metrics: Dice={metrics['water_dice']:.3f}, IoU={metrics['water_iou']:.3f}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Metric computation test failed: {e}")
        return False

def test_model_loading():
    """Test that model interfaces work (without actual checkpoints)."""
    logger.info("Testing model loading interface...")
    
    try:
        # Test Prithvi model import
        from prithvi.training.train_prithvi import PrithviFoundationModel
        logger.info("✓ Prithvi model class imported successfully")
        
        # Test Clay model import  
        from clay.training.train_clay import ClayFoundationModel
        logger.info("✓ Clay model class imported successfully")
        
        # Test model comparison framework
        from model_comparison_framework import ModelComparisionFramework
        logger.info("✓ Model comparison framework imported successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Model loading test failed: {e}")
        return False

def test_wandb_setup():
    """Test WandB setup functionality."""
    logger.info("Testing WandB setup...")
    
    try:
        from ultra_fast_prithvi_evaluator import setup_wandb
        import wandb
        
        # Test with disabled mode
        import os
        os.environ['WANDB_MODE'] = 'disabled'
        
        run = setup_wandb('test_run', '/fake/checkpoint/path.ckpt')
        logger.info("✓ WandB setup completed (disabled mode)")
        
        # Clean up
        wandb.finish()
        return True
        
    except Exception as e:
        logger.error(f"✗ WandB setup test failed: {e}")
        return False

def test_csv_output():
    """Test CSV output functionality."""
    logger.info("Testing CSV output...")
    
    try:
        from ultra_fast_prithvi_evaluator import append_to_csv
        import tempfile
        import os
        
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_csv = f.name
        
        try:
            # Test data
            test_result = {
                'timestamp': '2025-10-08T12:00:00',
                'model_variant': 'test_model',
                'huc_id': '01030003',
                'water_dice': 0.75,
                'water_iou': 0.60,
                'd8_accuracy': 0.45
            }
            
            # Append to CSV
            append_to_csv(test_result, temp_csv)
            
            # Verify file was created and has content
            import pandas as pd
            df = pd.read_csv(temp_csv)
            
            if len(df) == 1 and df.iloc[0]['water_dice'] == 0.75:
                logger.info("✓ CSV output test passed")
                return True
            else:
                logger.error("✗ CSV content verification failed")
                return False
                
        finally:
            # Clean up
            if os.path.exists(temp_csv):
                os.unlink(temp_csv)
        
    except Exception as e:
        logger.error(f"✗ CSV output test failed: {e}")
        return False

def main():
    """Run all tests."""
    logger.info("Starting Foundation Model Evaluation System Tests")
    logger.info("=" * 60)
    
    tests = [
        ("Data Loading", test_data_loading),
        ("Metric Computation", test_metric_computation), 
        ("Model Loading Interface", test_model_loading),
        ("WandB Setup", test_wandb_setup),
        ("CSV Output", test_csv_output)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        if test_func():
            passed += 1
        else:
            logger.error(f"{test_name} failed!")
    
    logger.info("\n" + "=" * 60)
    logger.info(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Foundation model evaluation system is ready.")
        return 0
    else:
        logger.error(f"❌ {total - passed} tests failed. Please fix issues before running evaluations.")
        return 1

if __name__ == "__main__":
    exit(main())