#!/usr/bin/env python3
"""
Test Clay with different input sizes to find the correct dimensions
"""

import torch
import logging
import os
import sys

# Add project root to Python path
sys.path.append('/u/nathanj/national_ml/experiments/foundational_model_experiment')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_input_size(model, size, channels=6):
    """Test model with specific input size"""
    try:
        dummy_input = torch.randn(1, channels, size, size)
        logger.info(f"Testing size {size}x{size} with {channels} channels...")
        
        with torch.no_grad():
            outputs = model(dummy_input)
        
        logger.info(f"✓ SUCCESS: {size}x{size} works!")
        if hasattr(outputs, 'output'):
            logger.info(f"  Output shape: {outputs.output.shape}")
        return True
    except Exception as e:
        logger.error(f"✗ FAILED: {size}x{size} - {str(e)[:100]}...")
        return False

def main():
    # Load model
    checkpoint_path = "/u/nathanj/national_ml/experiments/foundational_model_experiment/clay/outputs/models/clay_9ch_20251005_100452_run1/checkpoints/epoch=58-val_loss=0.2915.ckpt"
    
    logger.info("Loading Clay model...")
    
    from clay.training.train_clay import ClayFoundationModel
    model = ClayFoundationModel.load_from_checkpoint(checkpoint_path, map_location='cpu')
    model.eval()
    
    # Access the base Clay model directly
    base_model = model.model.clay_model if hasattr(model.model, 'clay_model') else model.model
    
    logger.info(f"Model loaded: {type(base_model)}")
    
    # Test common image sizes
    sizes_to_test = [
        224, 256, 512, 192, 160, 128, 96, 
        # Clay-specific sizes that might work
        240, 480, 320, 384, 448, 
        # Powers of 2
        64, 128, 256, 512, 1024
    ]
    
    successful_sizes = []
    
    logger.info("\nTesting different input sizes...")
    for size in sizes_to_test:
        if test_input_size(base_model, size, channels=6):
            successful_sizes.append(size)
    
    logger.info(f"\nSuccessful sizes: {successful_sizes}")
    
    if successful_sizes:
        # Test with 9-channel input on successful size
        best_size = successful_sizes[0]
        logger.info(f"\nTesting 9-channel input with size {best_size}x{best_size}...")
        test_input_size(model.model, best_size, channels=9)

if __name__ == '__main__':
    main()