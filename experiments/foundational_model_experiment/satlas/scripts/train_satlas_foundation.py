#!/usr/bin/env python3
"""
Production SATLAS Foundation Model Training Script

This script provides a robust training pipeline for SATLAS foundation model
with both real and synthetic data support.
"""

import os
import sys
import argparse
from pathlib import Path
import yaml
import logging

import torch
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping, LearningRateMonitor
from pytorch_lightning.loggers import WandbLogger

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))

# Local imports
from data.synthetic_dataset import SyntheticSatlasDataModule
from data.four_modal_dataset_adapter import SatlasDataModule
from training.satlas_multimodal_wrapper import SatlasMultimodalWrapper
from utils.losses import CombinedFocalDiceLoss

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SatlasFoundationModel(pl.LightningModule):
    """SATLAS Foundation Model for Water Segmentation"""
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.save_hyperparameters()
        
        # Build model using multimodal wrapper (proven to work)
        wrapper = SatlasMultimodalWrapper(config)
        self.model = wrapper
        
        # Loss function
        self.criterion = CombinedFocalDiceLoss(
            focal_weight=config["loss"]["focal_weight"],
            dice_weight=config["loss"]["dice_weight"],
            focal_alpha=config["loss"]["focal_alpha"],
            focal_gamma=config["loss"]["focal_gamma"]
        )
        
        # Metrics
        from torchmetrics import MetricCollection, JaccardIndex, Accuracy, F1Score, Precision, Recall
        
        metrics = {
            "iou": JaccardIndex(task="binary"),
            "accuracy": Accuracy(task="binary"),
            "f1": F1Score(task="binary"),
            "precision": Precision(task="binary"),
            "recall": Recall(task="binary")
        }
        
        self.train_metrics = MetricCollection(metrics)
        self.val_metrics = MetricCollection(metrics)
        
        logger.info(f"SATLAS Foundation Model initialized")
        logger.info(f"Parameters: {sum(p.numel() for p in self.parameters()):,}")
        logger.info(f"Architecture: Custom U-Net CNN with 9-channel input")
    
    def forward(self, x):
        return self.model(x)
    
    def training_step(self, batch, batch_idx):
        images = batch['image']
        masks = batch['mask']
        
        # Forward pass
        outputs = self(images)
        
        # Ensure masks have correct shape for loss
        if masks.dim() == 3:
            masks = masks.unsqueeze(1)  # Add channel dimension
        
        # Calculate loss
        loss = self.criterion(outputs, masks)
        
        # Calculate metrics
        preds = torch.sigmoid(outputs) > 0.5
        mask_for_metrics = masks.squeeze(1) if masks.dim() == 4 else masks
        pred_for_metrics = preds.squeeze(1) if preds.dim() == 4 else preds
        
        metrics = self.train_metrics(pred_for_metrics.int(), mask_for_metrics.int())
        
        # Log metrics
        self.log('train_loss', loss, on_step=True, on_epoch=True, prog_bar=True)
        self.log_dict({f"train_{k}": v for k, v in metrics.items()}, on_epoch=True)
        
        return loss
    
    def validation_step(self, batch, batch_idx):
        images = batch['image']
        masks = batch['mask']
        
        # Forward pass
        outputs = self(images)
        
        # Ensure masks have correct shape for loss
        if masks.dim() == 3:
            masks = masks.unsqueeze(1)  # Add channel dimension
        
        # Calculate loss
        loss = self.criterion(outputs, masks)
        
        # Calculate metrics
        preds = torch.sigmoid(outputs) > 0.5
        mask_for_metrics = masks.squeeze(1) if masks.dim() == 4 else masks
        pred_for_metrics = preds.squeeze(1) if preds.dim() == 4 else preds
        
        metrics = self.val_metrics(pred_for_metrics.int(), mask_for_metrics.int())
        
        # Log metrics
        self.log('val_loss', loss, on_step=False, on_epoch=True, prog_bar=True)
        self.log_dict({f"val_{k}": v for k, v in metrics.items()}, on_epoch=True)
        
        return loss
    
    def configure_optimizers(self):
        """Configure optimizer and learning rate scheduler"""
        # Optimizer
        optimizer = torch.optim.AdamW(
            self.parameters(),
            lr=self.config["training"]["learning_rate"],
            weight_decay=self.config["training"]["weight_decay"],
            betas=(self.config["training"]["beta1"], self.config["training"]["beta2"]),
            eps=self.config["training"]["eps"]
        )
        
        # Learning rate scheduler
        scheduler_config = {
            "scheduler": torch.optim.lr_scheduler.CosineAnnealingLR(
                optimizer,
                T_max=self.config["training"]["T_max"],
                eta_min=self.config["training"]["eta_min"]
            ),
            "monitor": self.config["training"]["monitor_metric"],
            "interval": "epoch",
            "frequency": 1,
        }
        
        return {
            "optimizer": optimizer,
            "lr_scheduler": scheduler_config
        }


def main():
    parser = argparse.ArgumentParser(description="SATLAS Foundation Model Training")
    parser.add_argument("--config", type=str, required=True, help="Path to config file")
    parser.add_argument("--gpus", type=int, default=1, help="Number of GPUs")
    parser.add_argument("--synthetic", action="store_true", help="Use synthetic data")
    parser.add_argument("--test", action="store_true", help="Run quick test")
    parser.add_argument("--offline", action="store_true", help="Run WandB offline")
    args = parser.parse_args()
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Test mode modifications
    if args.test:
        config["training"]["max_epochs"] = 5
        config["training"]["batch_size"] = 4
        config["logging"]["project_name"] = f"{config['logging']['project_name']}_test"
        logger.info("Running in test mode with reduced epochs and batch size")
    
    logger.info(f"Starting SATLAS Foundation Model Training")
    logger.info(f"Config: {args.config}")
    logger.info(f"GPUs: {args.gpus}")
    logger.info(f"Synthetic data: {args.synthetic}")
    logger.info(f"Test mode: {args.test}")
    
    # Set reproducibility
    if config.get("reproducibility"):
        pl.seed_everything(config["reproducibility"]["seed"])
    
    # Data module - use real HUC data or synthetic
    logger.info("Setting up data module...")
    if args.synthetic:
        data_module = SyntheticSatlasDataModule(config)
        logger.info("Using synthetic data module")
    else:
        try:
            data_module = SatlasDataModule(config)
            logger.info("Using real HUC data module")
        except Exception as e:
            logger.warning(f"Failed to load real data: {e}")
            logger.info("Falling back to synthetic data module")
            data_module = SyntheticSatlasDataModule(config)
            args.synthetic = True  # Update flag for logging
    
    data_module.setup("fit")
    
    # Model
    logger.info("Setting up SATLAS foundation model...")
    model = SatlasFoundationModel(config)
    
    # WandB Logger
    experiment_name = f"satlas_foundation_{'test' if args.test else 'full'}"
    wandb_logger = WandbLogger(
        project=config["logging"]["project_name"],
        name=experiment_name,
        tags=config["logging"]["wandb"]["tags"] + (["test"] if args.test else []),
        group=config["logging"]["wandb"]["group"],
        notes=config["logging"]["wandb"]["notes"],
        offline=args.offline
    )
    
    # Log hyperparameters
    wandb_logger.log_hyperparams({
        "model_architecture": "SATLAS_UNet_CNN",
        "input_channels": 9,
        "output_classes": 1,
        "data_type": "synthetic" if args.synthetic else "real",
        **config
    })
    
    # Callbacks
    callbacks = [
        ModelCheckpoint(
            dirpath=f"{config['output']['base_dir']}/{config['output']['model_dir']}",
            filename="satlas-foundation-{epoch:02d}-{val_iou:.3f}",
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
    
    # Trainer
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
        benchmark=config.get("reproducibility", {}).get("benchmark", True),
        enable_progress_bar=True
    )
    
    # Train model
    logger.info("Starting training...")
    trainer.fit(model, data_module)
    
    # Final validation
    logger.info("Running final validation...")
    results = trainer.validate(model, data_module)
    
    logger.info("Training completed successfully!")
    logger.info(f"Final validation results: {results}")
    
    # Print summary
    best_val_iou = trainer.callback_metrics.get('val_iou', 0.0)
    logger.info(f"Best validation IoU: {best_val_iou:.4f}")


if __name__ == "__main__":
    main()