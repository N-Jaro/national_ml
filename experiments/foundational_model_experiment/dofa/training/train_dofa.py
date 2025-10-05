#!/usr/bin/env python3
"""
DOFA Foundation Model Experiment Training Script
Individual training script for dofa foundation model
"""

import os
import sys
import argparse
from pathlib import Path
import yaml
import logging

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


class DofaFoundationModel(pl.LightningModule):
    """
    DOFA Foundation Model Experiment for water segmentation.
    """
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.save_hyperparameters()
        
        logger.info(f"Initializing dofa foundation model")
        
        # Build model
        self.model = self._build_model()
        
        # Loss function  
        self.criterion = self._build_loss()
        
        # Metrics
        self.train_metrics = self._build_metrics("train")
        self.val_metrics = self._build_metrics("val")
        
        logger.info(f"dofa model initialized with {self.count_parameters():,} parameters")
        
        # Debug counter for logging
        self._debug_step = 0
    
    def _build_model(self):
        """Build DOFA model with multimodal wrapper for 9-channel input."""
        try:
            from dofa_multimodal_wrapper import DofaMultimodalWrapper
            
            # Try TerraTorch approach first, fall back to wrapper if needed
            try:
                logger.info("Attempting to build DOFA model through TerraTorch...")
                factory = EncoderDecoderFactory()
                
                # Try with some common satellite band configurations
                band_configs_to_try = [
                    # HLS band configuration (common format)
                    ['HLS.B01', 'HLS.B02', 'HLS.B03', 'HLS.B04', 'HLS.B05', 'HLS.B06', 'HLS.B07', 'HLS.B08', 'HLS.B09'],
                    # Alternative format
                    ['LANDSAT.B1', 'LANDSAT.B2', 'LANDSAT.B3', 'LANDSAT.B4', 'LANDSAT.B5', 'LANDSAT.B6', 'LANDSAT.B7', 'LANDSAT.B8', 'LANDSAT.B9'],
                ]
                
                for bands in band_configs_to_try:
                    try:
                        model_config = {
                            "task": "segmentation",
                            "backbone": "terratorch_dofa_base_patch16_224",
                            "decoder": self.config["model"]["decoder"],
                            "num_classes": self.config["model"]["num_classes"],
                            "backbone_kwargs": {"model_bands": bands}
                        }
                        
                        model = factory.build_model(**model_config)
                        logger.info(f"Successfully built DOFA model through TerraTorch with bands: {bands[:3]}...{bands[-3:]}")
                        return model
                        
                    except Exception as e:
                        logger.debug(f"TerraTorch config with {bands[0]} failed: {e}")
                        continue
                
                # If all TerraTorch attempts fail, fall back to wrapper
                raise RuntimeError("All TerraTorch configurations failed")
                
            except Exception as terratorch_error:
                logger.warning(f"TerraTorch approach failed: {terratorch_error}")
                logger.info("Falling back to DOFA multimodal wrapper...")
                
                # Use the multimodal wrapper approach
                dofa_wrapper = DofaMultimodalWrapper(
                    num_classes=1000,  # Will be reset by decoder
                    pretrained=False,  # For 9-channel input
                    strategy="direct_wavelengths"
                )
                
                # Wrap in a simple container that matches TerraTorch interface
                class DofaWrapperContainer(nn.Module):
                    def __init__(self, dofa_wrapper, config):
                        super().__init__()
                        self.config = config
                        self.encoder = dofa_wrapper
                        self.decoder = self._build_decoder()
                    
                    def _build_decoder(self):
                        # Segmentation decoder for DOFA spatial features
                        # DOFA wrapper outputs (B, embed_dim, 14, 14) spatial features
                        embed_dim = 768  # DOFA base embedding dimension
                        
                        return nn.Sequential(
                            # Upsample spatial features to target resolution (14x14 -> 224x224)
                            nn.ConvTranspose2d(embed_dim, 512, kernel_size=4, stride=2, padding=1),  # 14->28
                            nn.BatchNorm2d(512),
                            nn.ReLU(),
                            
                            nn.ConvTranspose2d(512, 256, kernel_size=4, stride=2, padding=1),  # 28->56
                            nn.BatchNorm2d(256),
                            nn.ReLU(),
                            
                            nn.ConvTranspose2d(256, 128, kernel_size=4, stride=2, padding=1),  # 56->112
                            nn.BatchNorm2d(128),
                            nn.ReLU(),
                            
                            nn.ConvTranspose2d(128, 64, kernel_size=4, stride=2, padding=1),   # 112->224
                            nn.BatchNorm2d(64),
                            nn.ReLU(),
                            
                            # Final segmentation layer
                            nn.Conv2d(64, self.config["model"]["num_classes"], kernel_size=1)
                        )
                    
                    def forward(self, x):
                        features = self.encoder(x)
                        # Simple output structure to match TerraTorch
                        class SimpleOutput:
                            def __init__(self, output):
                                self.output = output
                        
                        decoded = self.decoder(features)
                        return SimpleOutput(decoded)
                
                model = DofaWrapperContainer(dofa_wrapper, self.config)
                logger.info("Built DOFA model using multimodal wrapper")
                logger.info("DOFA multimodal input: DEM + 6×Optical + Thermal + SAR = 9 channels")
                
                return model
            
        except Exception as e:
            logger.error(f"Error building DOFA model: {e}")
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
            "iou": JaccardIndex(task="binary"),
            "accuracy": Accuracy(task="binary"), 
            "f1": F1Score(task="binary")
        })
    
    def forward(self, x):
        return self.model(x)
    
    def training_step(self, batch, batch_idx):
        # Debug logging for first few steps
        if self._debug_step < 5:
            images = batch['image']
            logger.info(f"Debug step {self._debug_step}:")
            logger.info(f"  Images shape: {images.shape}, range: [{images.min():.3f}, {images.max():.3f}]")
            self._debug_step += 1
            
        loss = self._step(batch, "train")
        
        # Check for problematic loss values
        if torch.isnan(loss) or torch.isinf(loss):
            logger.error(f"Invalid loss detected: {loss.item()}")
            
        return loss
    
    def _step(self, batch, stage: str):
        """Common step logic for train/val"""
        images = batch['image']
        masks = batch['mask']
        model_output = self.forward(images)
        # Extract logits from ModelOutput object
        outputs = model_output.output if hasattr(model_output, 'output') else model_output
        
        # Ensure masks have correct shape for loss computation
        if masks.dim() == 3 and outputs.dim() == 4:  # (B,H,W) -> (B,1,H,W)
            masks = masks.unsqueeze(1)
        
        loss = self.criterion(outputs, masks)
        
        # Log metrics (ensure shapes match)
        mask_for_metrics = masks.squeeze(1) if masks.dim() == 4 else masks
        outputs_for_metrics = outputs.squeeze(1) if outputs.dim() == 4 else outputs
        
        if stage == "train":
            metrics = self.train_metrics(outputs_for_metrics, mask_for_metrics.int())
        else:
            metrics = self.val_metrics(outputs_for_metrics, mask_for_metrics.int())
            
        # Get batch size for proper logging
        batch_size = images.size(0)
        
        self.log(f"{stage}_loss", loss, prog_bar=True, batch_size=batch_size)
        self.log_dict({f"{stage}_{k}": v for k, v in metrics.items()}, prog_bar=True, batch_size=batch_size)
        
        return loss
    
    def validation_step(self, batch, batch_idx):
        # Log visualizations only on first val batch to avoid spam
        if batch_idx == 0:
            self._log_val_visuals(batch)
        return self._step(batch, "val")
    
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
        
        # Create warmup + cosine annealing scheduler
        warmup_epochs = self.config["training"]["warmup_epochs"]
        max_epochs = self.config["training"]["max_epochs"]
        
        def lr_lambda(epoch):
            if epoch < warmup_epochs:
                # Linear warmup
                return epoch / warmup_epochs
            else:
                # Cosine annealing after warmup
                import math
                cos_progress = (epoch - warmup_epochs) / (max_epochs - warmup_epochs)
                return 0.5 * (1 + math.cos(math.pi * cos_progress))
        
        scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)
        
        return {
            "optimizer": optimizer, 
            "lr_scheduler": {
                "scheduler": scheduler,
                "interval": "epoch",
                "frequency": 1
            }
        }
    
    def count_parameters(self):
        return sum(p.numel() for p in self.parameters() if p.requires_grad)
    
    def _log_val_visuals(self, batch):
        """Log validation visualizations to WandB every 10 epochs."""
        if not isinstance(self.logger, WandbLogger):
            return
        if self.global_rank != 0:  # avoid duplicate logs under DDP
            return
        if (self.current_epoch % 10) != 0:
            return
        
        try:
            import wandb
            
            images = batch['image']  # (B, 9, H, W)
            masks = batch['mask']    # (B, H, W)
            
            # Get predictions
            with torch.no_grad():
                model_output = self.forward(images)
                outputs = model_output.output if hasattr(model_output, 'output') else model_output
                pred_probs = torch.sigmoid(outputs)  # (B, 1, H, W)
                pred_masks = (pred_probs >= 0.5).float()  # (B, 1, H, W)
                
                # Debug: Log shapes to understand the issue
                logger.info(f"Visualization Debug - Shapes:")
                logger.info(f"  images: {images.shape}")
                logger.info(f"  masks: {masks.shape}")
                logger.info(f"  outputs: {outputs.shape}")
                logger.info(f"  pred_probs: {pred_probs.shape}")
                logger.info(f"  pred_masks: {pred_masks.shape}")
            
            # Convert to numpy for visualization (first sample only)
            sample_idx = 0
            
            # Extract individual channels for visualization (convert from bfloat16)
            dem_ch = images[sample_idx, 0].float().cpu().numpy()        # DEM channel
            optical_rgb = images[sample_idx, 1:4].float().cpu().numpy() # RGB channels (2,3,4 -> 0,1,2)
            thermal_ch = images[sample_idx, 7].float().cpu().numpy()    # Thermal channel
            sar_ch = images[sample_idx, 8].float().cpu().numpy()        # SAR channel
            
            # Ground truth and predictions (convert from bfloat16 to float32)
            gt_mask = masks[sample_idx].float().cpu().numpy()
            pred_mask = pred_masks[sample_idx, 0].float().cpu().numpy()
            pred_prob = pred_probs[sample_idx, 0].float().cpu().numpy()
            
            # Create visualizations
            visualizations = []
            
            # 1. DEM
            visualizations.append(wandb.Image(
                dem_ch, caption="DEM Channel"
            ))
            
            # 2. RGB Composite (normalize for visualization)
            rgb_composite = np.transpose(optical_rgb, (1, 2, 0))
            rgb_composite = (rgb_composite - rgb_composite.min()) / (rgb_composite.max() - rgb_composite.min() + 1e-8)
            visualizations.append(wandb.Image(
                rgb_composite, caption="RGB Composite"
            ))
            
            # 3. Thermal
            visualizations.append(wandb.Image(
                thermal_ch, caption="Thermal Channel"
            ))
            
            # 4. SAR
            visualizations.append(wandb.Image(
                sar_ch, caption="SAR Channel"
            ))
            
            # 5. Ground Truth Mask
            visualizations.append(wandb.Image(
                gt_mask, caption="Ground Truth Water Mask"
            ))
            
            # 6. Prediction Probability
            visualizations.append(wandb.Image(
                pred_prob, caption="Prediction Probability"
            ))
            
            # 7. Prediction Mask
            visualizations.append(wandb.Image(
                pred_mask, caption="Prediction Mask (>0.5)"
            ))
            
            # Log to WandB
            self.logger.experiment.log({
                f"validation_samples_epoch_{self.current_epoch}": visualizations,
                "epoch": self.current_epoch
            })
            
            logger.info(f"Logged {len(visualizations)} validation visualizations for epoch {self.current_epoch}")
            
        except Exception as e:
            logger.warning(f"Failed to log validation visualizations: {e}")
            import traceback
            traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description="DOFA Foundation Model Experiment Training")
    parser.add_argument("--config", default="configs/dofa_config.yaml", 
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
    data_module.setup("fit")  # Setup data module first
    
    # Set up model
    logger.info("Setting up dofa model...")
    model = DofaFoundationModel(config)
    
    # Set up logger
    experiment_name = f"dofa_4modal_water_segmentation_{'test' if args.test else 'full'}"
    wandb_logger = WandbLogger(
        project=config["logging"]["project_name"],
        name=experiment_name,
        tags=config["logging"]["wandb"]["tags"],
        group=config["logging"]["wandb"]["group"],
        notes=config["logging"]["wandb"]["notes"]
    )
    
    # Log config and dataset info
    wandb_logger.experiment.config.update(config)
    dataset_info = data_module.get_dataset_info()
    wandb_logger.experiment.config.update({"dataset_info": dataset_info})
    logger.info(f"Dataset info: {dataset_info}")
    
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
    logger.info("Starting dofa training...")
    trainer.fit(model, data_module)
    
    logger.info("dofa training completed!")


if __name__ == "__main__":
    main()
