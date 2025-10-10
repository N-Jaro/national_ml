#!/usr/bin/env python3
"""
Simple SATLAS training test with synthetic data
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
from training.satlas_multimodal_wrapper import SatlasMultimodalWrapper
from utils.losses import CombinedFocalDiceLoss

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleSatlasModel(pl.LightningModule):
    """Simple SATLAS model for testing"""
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.save_hyperparameters()
        
        # Use multimodal wrapper
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
        from torchmetrics import MetricCollection, JaccardIndex, Accuracy, F1Score
        self.train_metrics = MetricCollection({
            "iou": JaccardIndex(task="binary"),
            "accuracy": Accuracy(task="binary"),
            "f1": F1Score(task="binary")
        })
        self.val_metrics = MetricCollection({
            "iou": JaccardIndex(task="binary"),
            "accuracy": Accuracy(task="binary"),
            "f1": F1Score(task="binary")
        })
        
        logger.info(f"SATLAS model initialized with {sum(p.numel() for p in self.parameters()):,} parameters")
    
    def forward(self, x):
        return self.model(x)
    
    def training_step(self, batch, batch_idx):
        images = batch['image']
        masks = batch['mask']
        
        outputs = self(images)
        loss = self.criterion(outputs, masks.unsqueeze(1))  # Add channel dim for loss
        
        # Metrics
        preds = torch.sigmoid(outputs) > 0.5
        metrics = self.train_metrics(preds.squeeze(1).int(), masks.int())
        
        self.log('train_loss', loss, on_step=True, on_epoch=True, prog_bar=True)
        self.log_dict({f"train_{k}": v for k, v in metrics.items()}, on_epoch=True)
        
        return loss
    
    def validation_step(self, batch, batch_idx):
        images = batch['image']
        masks = batch['mask']
        
        outputs = self(images)
        loss = self.criterion(outputs, masks.unsqueeze(1))  # Add channel dim for loss
        
        # Metrics
        preds = torch.sigmoid(outputs) > 0.5
        metrics = self.val_metrics(preds.squeeze(1).int(), masks.int())
        
        self.log('val_loss', loss, on_step=False, on_epoch=True, prog_bar=True)
        self.log_dict({f"val_{k}": v for k, v in metrics.items()}, on_epoch=True)
        
        return loss
    
    def configure_optimizers(self):
        optimizer = torch.optim.AdamW(
            self.parameters(),
            lr=self.config["training"]["learning_rate"],
            weight_decay=self.config["training"]["weight_decay"]
        )
        
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer,
            T_max=self.config["training"]["max_epochs"],
            eta_min=self.config["training"]["eta_min"]
        )
        
        return {
            "optimizer": optimizer,
            "lr_scheduler": {
                "scheduler": scheduler,
                "monitor": "val_iou",
                "interval": "epoch",
                "frequency": 1,
            }
        }


def main():
    parser = argparse.ArgumentParser(description="Simple SATLAS test with synthetic data")
    parser.add_argument("--config", type=str, required=True, help="Path to config file")
    parser.add_argument("--gpus", type=int, default=0, help="Number of GPUs")
    args = parser.parse_args()
    
    # Load config
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Override for testing
    config["training"]["max_epochs"] = 3
    config["training"]["batch_size"] = 4
    config["logging"]["project_name"] = "satlas_synthetic_test"
    
    logger.info("Starting SATLAS synthetic test...")
    
    # Set seed
    pl.seed_everything(42)
    
    # Data module
    data_module = SyntheticSatlasDataModule(config)
    data_module.setup("fit")
    
    # Model
    model = SimpleSatlasModel(config)
    
    # Logger
    wandb_logger = WandbLogger(
        project=config["logging"]["project_name"],
        name="satlas_synthetic_test",
        tags=["satlas", "synthetic", "test"],
        offline=True  # Keep offline for testing
    )
    
    # Callbacks
    callbacks = [
        ModelCheckpoint(
            monitor="val_iou",
            mode="max",
            save_top_k=1,
            filename="satlas-synthetic-{epoch:02d}-{val_iou:.3f}"
        ),
        EarlyStopping(
            monitor="val_iou",
            mode="max",
            patience=10
        ),
        LearningRateMonitor(logging_interval="epoch")
    ]
    
    # Trainer
    trainer = pl.Trainer(
        max_epochs=config["training"]["max_epochs"],
        accelerator="cpu" if args.gpus == 0 else "gpu",
        devices=1,
        precision="bf16-mixed" if args.gpus == 0 else "16-mixed",
        logger=wandb_logger,
        callbacks=callbacks,
        log_every_n_steps=10,
        deterministic=False,
        enable_progress_bar=True
    )
    
    # Train
    logger.info("Starting training...")
    trainer.fit(model, data_module)
    
    # Test final validation
    logger.info("Running final validation...")
    results = trainer.validate(model, data_module)
    logger.info(f"Final results: {results}")
    
    logger.info("SATLAS synthetic test completed!")


if __name__ == "__main__":
    main()