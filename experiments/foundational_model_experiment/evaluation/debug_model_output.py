#!/usr/bin/env python3
"""Debug script to inspect TerraTorch model output format."""

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
    checkpoint_path = "/u/nathanj/national_ml/experiments/foundational_model_experiment/prithvi/outputs/models/prithvi_9ch_20251004_223048_run7/checkpoints/epoch=28-val_loss=0.2528.ckpt"
    
    logger.info("Loading Prithvi model...")
    
    # Import and load model
    from prithvi.training.train_prithvi import PrithviFoundationModel
    model = PrithviFoundationModel.load_from_checkpoint(checkpoint_path, map_location='cpu')
    model.eval()
    
    logger.info(f"Model loaded with {sum(p.numel() for p in model.parameters()):,} parameters")
    
    # Create dummy 9-channel input (B, C, H, W)
    dummy_input = torch.randn(1, 9, 224, 224)  # 1 batch, 9 channels, 224x224
    
    logger.info(f"Dummy input shape: {dummy_input.shape}")
    
    # Get model output
    with torch.no_grad():
        outputs = model(dummy_input)
    
    logger.info(f"Output type: {type(outputs)}")
    logger.info(f"Output: {outputs}")
    
    if hasattr(outputs, '__dict__'):
        logger.info(f"Output attributes: {list(outputs.__dict__.keys())}")
        for attr_name in outputs.__dict__:
            attr_val = getattr(outputs, attr_name)
            logger.info(f"  {attr_name}: {type(attr_val)} - {attr_val.shape if hasattr(attr_val, 'shape') else attr_val}")
    
    # Check if it's a dictionary
    if isinstance(outputs, dict):
        logger.info(f"Output keys: {list(outputs.keys())}")
        for key, val in outputs.items():
            logger.info(f"  {key}: {type(val)} - {val.shape if hasattr(val, 'shape') else val}")
    
    # Check if it's a named tuple or similar
    if hasattr(outputs, '_fields'):
        logger.info(f"Output fields: {outputs._fields}")
        for field in outputs._fields:
            val = getattr(outputs, field)
            logger.info(f"  {field}: {type(val)} - {val.shape if hasattr(val, 'shape') else val}")

if __name__ == '__main__':
    main()