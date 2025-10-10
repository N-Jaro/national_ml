#!/usr/bin/env python3
"""
Debug Clay evaluation - test with optical-only input to isolate the tensor dimension issue
"""

import torch
import logging
import os
import sys

# Add project root to Python path
sys.path.append('/u/nathanj/national_ml/experiments/foundational_model_experiment')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Load model
    checkpoint_path = "/u/nathanj/national_ml/experiments/foundational_model_experiment/clay/outputs/models/clay_9ch_20251005_100452_run1/checkpoints/epoch=58-val_loss=0.2915.ckpt"
    
    logger.info("Loading Clay model...")
    
    # Import and load model
    from clay.training.train_clay import ClayFoundationModel
    model = ClayFoundationModel.load_from_checkpoint(checkpoint_path, map_location='cpu')
    model.eval()
    
    logger.info(f"Model loaded with {sum(p.numel() for p in model.parameters()):,} parameters")
    
    # Test with different input sizes
    logger.info("Testing different input configurations...")
    
    # Test 1: 9-channel input (should work with multimodal wrapper)
    try:
        logger.info("Test 1: 9-channel input")
        dummy_input_9ch = torch.randn(1, 9, 224, 224)
        logger.info(f"Input shape: {dummy_input_9ch.shape}")
        
        with torch.no_grad():
            outputs = model(dummy_input_9ch)
        
        logger.info(f"✓ 9-channel test successful!")
        logger.info(f"Output type: {type(outputs)}")
        if hasattr(outputs, 'output'):
            logger.info(f"Output shape: {outputs.output.shape}")
    except Exception as e:
        logger.error(f"✗ 9-channel test failed: {e}")
    
    # Test 2: 6-channel input (optical only - should work with base Clay)
    try:
        logger.info("\nTest 2: 6-channel optical input")
        dummy_input_6ch = torch.randn(1, 6, 224, 224)
        logger.info(f"Input shape: {dummy_input_6ch.shape}")
        
        # Access the base Clay model directly
        base_model = model.model.clay_model if hasattr(model.model, 'clay_model') else model.model
        
        with torch.no_grad():
            outputs = base_model(dummy_input_6ch)
        
        logger.info(f"✓ 6-channel test successful!")
        logger.info(f"Output type: {type(outputs)}")
        if hasattr(outputs, 'output'):
            logger.info(f"Output shape: {outputs.output.shape}")
    except Exception as e:
        logger.error(f"✗ 6-channel test failed: {e}")
    
    # Test 3: Check model architecture details
    try:
        logger.info("\nModel architecture details:")
        logger.info(f"Model type: {type(model.model)}")
        if hasattr(model.model, 'clay_model'):
            logger.info(f"Base Clay model type: {type(model.model.clay_model)}")
            logger.info(f"Clay model config: {model.model.clay_model.config if hasattr(model.model.clay_model, 'config') else 'No config'}")
    except Exception as e:
        logger.error(f"Architecture inspection failed: {e}")

if __name__ == '__main__':
    main()