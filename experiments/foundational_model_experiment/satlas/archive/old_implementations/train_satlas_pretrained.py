#!/usr/bin/env python3
"""
SATLAS Pretrained Foundation Model Training Script
Uses actual SATLAS pretrained weights via TerraTorch integration
"""

import os
import sys
import argparse
from pathlib import Path
import yaml
import logging
from datetime import datetime

import torch
import torch.nn as nn
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping, LearningRateMonitor
from pytorch_lightning.loggers import WandbLogger

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))  # Add experiment directory

# TerraTorch imports for pretrained SATLAS
try:
    from terratorch.models import EncoderDecoderFactory
    from terratorch import BACKBONE_REGISTRY
    TERRATORCH_AVAILABLE = True
except ImportError as e:
    print(f"TerraTorch import error: {e}")
    print("Please ensure you're using the terratorch_env environment")
    TERRATORCH_AVAILABLE = False

# Local imports
from data.four_modal_dataset_adapter import FourModalDataModule
from utils.losses import CombinedFocalDiceLoss

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SatlasPretrainedModel(pl.LightningModule):
    """
    SATLAS Foundation Model with actual pretrained weights via TerraTorch.
    """
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.save_hyperparameters()
        
        # Build model with pretrained SATLAS weights
        self.model = self._build_model()
        
        # Build loss function
        self.criterion = self._build_loss()
        
        # Build metrics
        self.train_metrics = self._build_metrics("train")
        self.val_metrics = self._build_metrics("val")
        
        logger.info(f"SATLAS pretrained model initialized with {sum(p.numel() for p in self.parameters()):,} parameters")

    def _build_model(self):
        """Build SATLAS model with pretrained weights through TerraTorch."""
        if not TERRATORCH_AVAILABLE:
            raise ImportError("TerraTorch not available. Please use terratorch_env environment.")
        
        try:
            logger.info("Building SATLAS model with pretrained weights...")
            
            # Use the Multi-Image Multi-Spectral SATLAS model for 9-channel input
            # This model is pretrained on Sentinel-2 multi-spectral data
            backbone_name = "terratorch_satlas_swin_b_sentinel2_mi_ms"
            
            if backbone_name not in BACKBONE_REGISTRY:
                raise ValueError(f"SATLAS backbone {backbone_name} not found in registry")
            
            logger.info(f"Using SATLAS backbone: {backbone_name}")
            
            # Build encoder-decoder model with correct TerraTorch API
            logger.info("Creating TerraTorch SATLAS encoder-decoder model...")
            
            # Initialize factory
            factory = EncoderDecoderFactory()
            
            # Build model using the correct TerraTorch API pattern
            model = factory.build_model(
                task="segmentation",
                backbone=backbone_name,
                backbone_bands=list(range(9)),  # 9 channels: [0,1,2,3,4,5,6,7,8]
                backbone_pretrained=True,
                backbone_model_bands=list(range(9)),  # Also try this parameter name
                necks=[
                    {"name": "SelectIndices", "indices": [-1]},
                    {"name": "ReshapeTokensToImage"}
                ],
                decoder=self.config["model"]["decoder"],
                decoder_channels=self.config["model"].get("decoder_channels", 128),
                head_dropout=self.config["model"].get("head_dropout", 0.1),
                num_classes=self.config["model"]["num_classes"]
            )
            
            logger.info("Successfully built SATLAS model with pretrained weights")
            return model
            
        except Exception as e:
            logger.error(f"Failed to build SATLAS pretrained model: {e}")
            logger.error("Make sure you're using terratorch_env and the model is available")
            raise

    def _build_loss(self):
        """Build combined focal-dice loss."""
        loss_config = self.config["loss"]
        return CombinedFocalDiceLoss(
            focal_weight=loss_config["focal_weight"],
            dice_weight=loss_config["dice_weight"],
            focal_alpha=loss_config["focal_alpha"], 
            focal_gamma=loss_config["focal_gamma"]
        )

    def _build_metrics(self, stage):
        """Build metrics collection."""
        from torchmetrics import MetricCollection, JaccardIndex, Accuracy, F1Score
        
        return MetricCollection({
            "iou": JaccardIndex(task="binary"),
            "accuracy": Accuracy(task="binary"),
            "f1": F1Score(task="binary"),
        })

    def forward(self, x):
        """Forward pass through SATLAS model."""
        return self.model(x)

    def training_step(self, batch, batch_idx):
        images = batch['image']
        masks = batch['mask']
        
        # Forward pass
        outputs = self(images)
        
        # Calculate loss
        loss = self.criterion(outputs, masks)
        
        # Log metrics
        self.log('train_loss', loss, on_step=True, on_epoch=True, prog_bar=True)
        
        # Update metrics
        preds = torch.sigmoid(outputs) > 0.5
        preds = preds.squeeze(1)  # Remove channel dimension to match mask shape
        metrics = self.train_metrics(preds.int(), masks.int())
        self.log_dict({f"train_{k}": v for k, v in metrics.items()}, on_epoch=True)
        
        return loss

    def validation_step(self, batch, batch_idx):
        images = batch['image']
        masks = batch['mask']
        
        # Forward pass
        outputs = self(images)
        
        # Calculate loss
        loss = self.criterion(outputs, masks)
        
        # Log metrics
        self.log('val_loss', loss, on_step=False, on_epoch=True, prog_bar=True)
        
        # Update metrics
        preds = torch.sigmoid(outputs) > 0.5
        preds = preds.squeeze(1)  # Remove channel dimension to match mask shape
        metrics = self.val_metrics(preds.int(), masks.int())
        self.log_dict({f"val_{k}": v for k, v in metrics.items()}, on_epoch=True)
        
        return loss

    def configure_optimizers(self):
        """Configure optimizer and scheduler."""
        training_config = self.config["training"]
        
        # Build optimizer
        if training_config["optimizer"] == "AdamW":
            optimizer = torch.optim.AdamW(
                self.parameters(),
                lr=training_config["learning_rate"],
                weight_decay=training_config["weight_decay"],
                betas=(training_config["beta1"], training_config["beta2"]),
                eps=training_config["eps"]
            )
        else:
            raise ValueError(f"Unsupported optimizer: {training_config['optimizer']}")
        
        # Build scheduler
        if training_config["scheduler"] == "CosineAnnealingLR":
            scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
                optimizer,
                T_max=training_config["T_max"],
                eta_min=training_config["eta_min"]
            )
            
            return {
                "optimizer": optimizer,
                "lr_scheduler": {
                    "scheduler": scheduler,
                    "monitor": training_config["monitor_metric"],
                    "interval": "epoch",
                    "frequency": 1,
                }
            }
        
        return optimizer


def main():
    """Main training function."""
    parser = argparse.ArgumentParser(description="Train SATLAS Pretrained Foundation Model")
    parser.add_argument("--config", type=str, required=True, help="Path to config file")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--gpus", type=int, default=1, help="Number of GPUs")
    args = parser.parse_args()
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Starting SATLAS Pretrained Foundation Model Experiment: {config['experiment']['name']}")
    logger.info("This uses actual SATLAS pretrained weights via TerraTorch")
    
    # Set up reproducibility
    if config.get("reproducibility"):
        pl.seed_everything(config["reproducibility"]["seed"])
        if config["reproducibility"]["deterministic"]:
            torch.backends.cudnn.deterministic = True
            torch.backends.cudnn.benchmark = False
        elif config["reproducibility"]["benchmark"]:
            torch.backends.cudnn.benchmark = True
    
    # Set up data module
    logger.info("Setting up data module...")
    data_module = FourModalDataModule(config)
    data_module.setup(stage="fit")
    
    # Set up model
    logger.info("Setting up SATLAS pretrained model...")
    model = SatlasPretrainedModel(config)
    
    # Set up wandb run name following same pattern as Prithvi
    wandb_run_name = os.environ.get('WANDB_NAME') or os.environ.get('WANDB_RUN_ID') or "satlas_pretrained_9ch_water_segmentation"
    
    # Set up logger
    wandb_logger = WandbLogger(
        project=config["logging"]["project_name"],
        name=wandb_run_name,
        tags=config["logging"]["wandb"]["tags"] + ["pretrained_weights"],
        group=config["logging"]["wandb"]["group"],
        notes=config["logging"]["wandb"]["notes"] + " - Using SATLAS pretrained weights"
    )
    
    # Log dataset info
    dataset_info = {
        "experiment": config["experiment"]["name"],
        "total_patches": len(data_module.train_dataset) + len(data_module.val_dataset),
        "train_patches": len(data_module.train_dataset),
        "val_patches": len(data_module.val_dataset),
        "huc_codes": config["data"]["huc_codes"],
        "patch_size": config["data"]["image_size"],
        "num_channels": config["data"]["total_channels"],
        "modalities": config["data"]["modalities"],
        "pretrained_weights": "SATLAS via TerraTorch",
        "backbone": "terratorch_satlas_swin_b_sentinel2_mi_ms"
    }
    wandb_logger.log_hyperparams(dataset_info)
    logger.info(f"Dataset info: {dataset_info}")
    
    # Set up checkpoint directory based on wandb run (following Prithvi pattern)
    wandb_run_name_for_dir = os.environ.get('WANDB_NAME') or os.environ.get('WANDB_RUN_ID')
    if wandb_run_name_for_dir:
        checkpoint_dir = os.path.join("outputs", "models", wandb_run_name_for_dir, "checkpoints")
    else:
        # Fallback to generic directory
        checkpoint_dir = os.path.join("outputs", "models", "checkpoints")
    
    # Create checkpoint directory
    os.makedirs(checkpoint_dir, exist_ok=True)
    logger.info(f"Checkpoints will be saved to: {checkpoint_dir}")
    
    # Set up callbacks
    callbacks = [
        ModelCheckpoint(
            dirpath=checkpoint_dir,
            filename="{epoch:02d}-{val_loss:.4f}",
            monitor=config["logging"]["monitor"],
            mode=config["training"]["monitor_mode"],
            save_top_k=config["logging"]["save_top_k"],
            save_last=config["logging"]["save_last"]
        ),
        EarlyStopping(
            monitor=config["training"]["monitor_metric"],
            mode=config["training"]["monitor_mode"],
            patience=config["training"]["patience"],
            verbose=True
        ),
        LearningRateMonitor(logging_interval="epoch")
    ]
    
    # Set up trainer
    trainer = pl.Trainer(
        max_epochs=config["training"]["max_epochs"],
        accelerator="cpu" if args.gpus == 0 else "gpu",
        devices=args.gpus if args.gpus > 0 else 1,
        precision=config["training"]["precision"],
        logger=wandb_logger,
        callbacks=callbacks,
        gradient_clip_val=config["training"]["gradient_clip_val"],
        log_every_n_steps=config["logging"]["log_every_n_steps"],
        val_check_interval=config["logging"]["val_check_interval"],
        deterministic=config.get("reproducibility", {}).get("deterministic", False),
        benchmark=config.get("reproducibility", {}).get("benchmark", True)
    )
    
    # Train model
    logger.info("Starting training with SATLAS pretrained weights...")
    trainer.fit(model, data_module)
    
    # Final evaluation
    logger.info("Running final validation...")
    results = trainer.validate(model, data_module)
    
    logger.info("Training completed!")
    logger.info(f"Final validation results: {results}")


if __name__ == "__main__":
    main()