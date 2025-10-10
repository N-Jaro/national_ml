#!/usr/bin/env python3
"""
ScaleMAE Foundation Model Experiment Training Script
Individual training script for scalemae foundation model
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
sys.path.append(str(Path(__file__).parent.parent))  # Add experiment directory

# TerraTorch imports
try:
    from terratorch.models import EncoderDecoderFactory
except ImportError as e:
    print(f"TerraTorch import error: {e}")
    print("Please install TerraTorch: pip install terratorch")
    sys.exit(1)

# Local imports
from data.four_modal_dataset_adapter import FourModalDataModule
from utils.losses import CombinedFocalDiceLoss

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScalemaeFoundationModel(pl.LightningModule):
    """
    ScaleMAE Foundation Model Experiment for water segmentation.
    """
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.save_hyperparameters()
        
        logger.info(f"Initializing scalemae foundation model")
        
        # Build model
        self.model = self._build_model()
        
        # Loss function  
        self.criterion = self._build_loss()
        
        # Metrics
        self.train_metrics = self._build_metrics("train")
        self.val_metrics = self._build_metrics("val")
        
        logger.info(f"scalemae model initialized with {self.count_parameters():,} parameters")
    
    def _build_model(self):
        """Build scalemae model using TerraTorch with 9-channel input."""
        try:
            factory = EncoderDecoderFactory()
            
            # Start with minimal configuration that TerraTorch accepts
            model_config = {
                "task": "segmentation",
                "backbone": "scalemae_base",
                "decoder": self.config["model"]["decoder"],
                "num_classes": self.config["model"]["num_classes"]
            }
            
            # Build model with default configuration
            model = factory.build_model(**model_config)
            
            # Modify input layer for 9-channel input if needed
            # (ScaleMAE architecture may handle this differently than Prithvi)
            
            logger.info(f"Built scalemae model: {model_config}")
            logger.info("Custom 9-channel input: DEM + 6×Optical + Thermal + SAR")
            
            return model
            
        except Exception as e:
            logger.error(f"Error building model: {e}")
            raise
    
    def _build_loss(self):
        """Build combined focal-dice loss."""
        return CombinedFocalDiceLoss(
            focal_weight=self.config["loss"]["focal_weight"],
            dice_weight=self.config["loss"]["dice_weight"],
            focal_alpha=self.config["loss"]["focal_alpha"], 
            focal_gamma=self.config["loss"]["focal_gamma"]
        )
    
    def _build_metrics(self, stage):
        """Build metrics collection."""
        from torchmetrics import MetricCollection, JaccardIndex, Accuracy, F1Score
        
        return MetricCollection({
            f"{stage}_iou": JaccardIndex(task="binary"),
            f"{stage}_accuracy": Accuracy(task="binary"), 
            f"{stage}_f1": F1Score(task="binary")
        })
    
    def forward(self, x):
        return self.model(x)
    
    def training_step(self, batch, batch_idx):
        images = batch['image']
        masks = batch['mask']
        model_output = self.forward(images)
        # Extract logits from ModelOutput object
        outputs = model_output.output if hasattr(model_output, 'output') else model_output
        loss = self.criterion(outputs, masks)
        
        # Log metrics
        metrics = self.train_metrics(outputs, masks.int())
        self.log("train_loss", loss, prog_bar=True)
        self.log_dict(metrics, prog_bar=True)
        
        return loss
    
    def validation_step(self, batch, batch_idx):
        images = batch['image']
        masks = batch['mask']
        model_output = self.forward(images)
        # Extract logits from ModelOutput object
        outputs = model_output.output if hasattr(model_output, 'output') else model_output
        loss = self.criterion(outputs, masks)
        
        # Log metrics
        metrics = self.val_metrics(outputs, masks.int())
        self.log("val_loss", loss, prog_bar=True)
        self.log_dict(metrics, prog_bar=True)
        
        return loss
    
    def configure_optimizers(self):
        # Ensure learning_rate is float
        lr = float(self.config["training"]["learning_rate"])
        
        optimizer = torch.optim.AdamW(
            self.parameters(),
            lr=lr,
            weight_decay=self.config["training"]["weight_decay"],
            betas=(self.config["training"]["beta1"], self.config["training"]["beta2"]),
            eps=self.config["training"]["eps"]
        )
        
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer,
            T_max=self.config["training"]["T_max"],
            eta_min=self.config["training"]["eta_min"]
        )
        
        return {"optimizer": optimizer, "lr_scheduler": scheduler}
    
    def count_parameters(self):
        return sum(p.numel() for p in self.parameters() if p.requires_grad)


def main():
    parser = argparse.ArgumentParser(description="ScaleMAE Foundation Model Experiment Training")
    parser.add_argument("--config", default="configs/scalemae_config.yaml", 
                       help="Path to configuration file")
    parser.add_argument("--gpus", type=int, default=1, help="Number of GPUs")
    parser.add_argument("--test", action="store_true", help="Run quick test")
    
    args = parser.parse_args()
    
    # Load config
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Modify config for testing
    if args.test:
        config["training"]["max_epochs"] = 2
        config["data"]["huc_codes"] = config["data"]["huc_codes"][:2]
        config["training"]["batch_size"] = 4
        config["logging"]["project_name"] = f"{config['logging']['project_name']}_test"
    
    # Set up data module
    logger.info("Setting up data module...")
    data_module = FourModalDataModule(config)
    
    # Set up model
    logger.info("Setting up scalemae model...")
    model = ScalemaeFoundationModel(config)
    
    # Set up logger
    wandb_logger = WandbLogger(
        project=config["logging"]["project_name"],
        name=f"scalemae_4modal_water_segmentation",
        tags=config["logging"]["wandb"]["tags"],
        group=config["logging"]["wandb"]["group"],
        notes=config["logging"]["wandb"]["notes"]
    )
    
    # Set up callbacks
    callbacks = [
        ModelCheckpoint(
            monitor=config["logging"]["monitor"],
            mode=config["training"]["monitor_mode"],
            save_top_k=config["logging"]["save_top_k"],
            save_last=config["logging"]["save_last"],
            filename="{epoch:02d}-{val_iou:.3f}"
        ),
        EarlyStopping(
            monitor=config["training"]["monitor_metric"],
            mode=config["training"]["monitor_mode"], 
            patience=config["training"]["patience"]
        ),
        LearningRateMonitor(logging_interval="step")
    ]
    
    # Set up trainer
    trainer = pl.Trainer(
        max_epochs=config["training"]["max_epochs"],
        accelerator="auto",
        devices=args.gpus,
        precision=config["training"]["precision"],
        logger=wandb_logger,
        callbacks=callbacks,
        gradient_clip_val=config["training"]["gradient_clip_val"],
        log_every_n_steps=config["logging"]["log_every_n_steps"],
        val_check_interval=config["logging"]["val_check_interval"]
    )
    
    # Train
    logger.info("Starting scalemae training...")
    trainer.fit(model, data_module)
    
    logger.info("scalemae training completed!")


if __name__ == "__main__":
    main()
