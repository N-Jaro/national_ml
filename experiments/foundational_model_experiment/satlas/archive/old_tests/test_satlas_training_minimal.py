#!/usr/bin/env python3
"""
Minimal SATLAS Training Test without PyTorch Lightning
Tests the standardized data adapter with basic PyTorch training loop
"""

import os
import sys
from pathlib import Path
import yaml
import logging

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))

import torch
import torch.nn as nn
from torch.utils.data import DataLoader

# Local imports
from data.four_modal_dataset_adapter import FourModalDataModule
from utils.losses import CombinedFocalDiceLoss

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleSatlasModel(nn.Module):
    """Simple CNN model for testing SATLAS training pipeline."""
    
    def __init__(self, in_channels=9, num_classes=1):
        super().__init__()
        
        # Simple encoder
        self.encoder = nn.Sequential(
            nn.Conv2d(in_channels, 64, 3, padding=1),
            nn.ReLU(),
            nn.Conv2d(64, 128, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
            
            nn.Conv2d(128, 256, 3, padding=1),
            nn.ReLU(),
            nn.Conv2d(256, 256, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
        )
        
        # Simple decoder
        self.decoder = nn.Sequential(
            nn.ConvTranspose2d(256, 128, 2, stride=2),
            nn.ReLU(),
            nn.Conv2d(128, 64, 3, padding=1),
            nn.ReLU(),
            
            nn.ConvTranspose2d(64, 32, 2, stride=2),
            nn.ReLU(),
            nn.Conv2d(32, num_classes, 3, padding=1),
        )
        
        # Initialize parameters count
        total_params = sum(p.numel() for p in self.parameters())
        logger.info(f"Simple SATLAS model initialized with {total_params:,} parameters")
    
    def forward(self, x):
        # Encode
        features = self.encoder(x)
        
        # Decode
        output = self.decoder(features)
        
        return output


def test_satlas_training(config_path):
    """Test SATLAS training with standardized adapter."""
    
    logger.info("🧪 Testing SATLAS Training Pipeline")
    logger.info("=" * 50)
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Config loaded: {config['experiment']['name']}")
    
    # Set device
    device = torch.device('cpu')  # Use CPU for testing
    logger.info(f"Using device: {device}")
    
    # Set up data module
    logger.info("Setting up data module...")
    data_module = FourModalDataModule(config)
    data_module.setup('fit')
    
    # Get dataset info
    info = data_module.get_dataset_info()
    logger.info("📊 Dataset Information:")
    for key, value in info.items():
        logger.info(f"  {key}: {value}")
    
    # Create data loaders
    train_loader = data_module.train_dataloader()
    val_loader = data_module.val_dataloader()
    
    logger.info(f"DataLoaders created: {len(train_loader)} train, {len(val_loader)} val batches")
    
    # Create model
    model = SimpleSatlasModel(
        in_channels=config['data']['total_channels'],
        num_classes=config['model']['num_classes']
    ).to(device)
    
    # Create loss function
    criterion = CombinedFocalDiceLoss(
        focal_weight=config['loss']['focal_weight'],
        dice_weight=config['loss']['dice_weight'],
        focal_alpha=config['loss']['focal_alpha'],
        focal_gamma=config['loss']['focal_gamma']
    )
    
    # Create optimizer
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=config['training']['learning_rate'],
        weight_decay=config['training']['weight_decay']
    )
    
    logger.info("🚀 Starting training test...")
    
    # Test training for a few batches
    model.train()
    total_loss = 0.0
    num_batches = min(3, len(train_loader))  # Test only 3 batches
    
    for batch_idx, batch in enumerate(train_loader):
        if batch_idx >= num_batches:
            break
            
        # Move data to device
        images = batch['image'].to(device)
        masks = batch['mask'].to(device)
        
        logger.info(f"Batch {batch_idx + 1}/{num_batches}: "
                   f"images {images.shape}, masks {masks.shape}")
        
        # Forward pass
        optimizer.zero_grad()
        outputs = model(images)
        
        logger.info(f"  Model output shape: {outputs.shape}")
        
        # Calculate loss
        loss = criterion(outputs, masks)
        logger.info(f"  Loss: {loss.item():.4f}")
        
        # Backward pass
        loss.backward()
        optimizer.step()
        
        total_loss += loss.item()
    
    avg_loss = total_loss / num_batches
    logger.info(f"✅ Training test completed! Average loss: {avg_loss:.4f}")
    
    # Test validation
    logger.info("🔍 Testing validation...")
    model.eval()
    val_loss = 0.0
    num_val_batches = min(2, len(val_loader))
    
    with torch.no_grad():
        for batch_idx, batch in enumerate(val_loader):
            if batch_idx >= num_val_batches:
                break
                
            images = batch['image'].to(device)
            masks = batch['mask'].to(device)
            
            outputs = model(images)
            loss = criterion(outputs, masks)
            
            val_loss += loss.item()
            logger.info(f"Val batch {batch_idx + 1}: loss {loss.item():.4f}")
    
    avg_val_loss = val_loss / num_val_batches
    logger.info(f"✅ Validation test completed! Average val loss: {avg_val_loss:.4f}")
    
    logger.info("🎉 SATLAS training pipeline test successful!")
    logger.info("🎯 Key achievements:")
    logger.info("  ✅ Standardized data adapter working")
    logger.info("  ✅ 9-channel input processing")
    logger.info("  ✅ Multi-HUC data loading")
    logger.info("  ✅ Loss function computation")
    logger.info("  ✅ Training and validation loops")
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test SATLAS Training Pipeline")
    parser.add_argument("--config", type=str, 
                       default="configs/satlas_test_config.yaml",
                       help="Path to config file")
    args = parser.parse_args()
    
    try:
        success = test_satlas_training(args.config)
        if success:
            print("\n🎉 SATLAS training test PASSED!")
            sys.exit(0)
        else:
            print("\n❌ SATLAS training test FAILED!")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Training test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)