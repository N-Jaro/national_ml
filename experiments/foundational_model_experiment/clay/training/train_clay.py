#!/usr/bin/env python3
"""
Clay Foundation Model Experiment Training Script
Individual training script for clay foundation model with fine-tuning pattern
"""

import os
import sys
import argparse
from pathlib import Path
import yaml
import logging
import json

import torch
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping, LearningRateMonitor
from pytorch_lightning.loggers import WandbLogger
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environments

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


class ClayFoundationModel(pl.LightningModule):
    """
    Clay Foundation Model Experiment for water segmentation with fine-tuning pattern.
    """
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.save_hyperparameters()
        
        logger.info(f"Initializing clay foundation model")
        
        # Build model
        self.model = self._build_model()
        
        # Loss function  
        self.criterion = self._build_loss()
        
        # Metrics
        self.train_metrics = self._build_metrics("train")
        self.val_metrics = self._build_metrics("val")
        
        logger.info(f"clay model initialized with {self.count_parameters():,} parameters")
        
        # Add debugging flag
        self._debug_step = 0
    
    def _build_model(self):
        """Build clay model using TerraTorch with 9-channel input and fine-tuning support."""
        try:
            from clay_multimodal_wrapper import ClayMultimodalWrapper
            
            factory = EncoderDecoderFactory()
            
            # Start with minimal configuration that TerraTorch accepts
            model_config = {
                "task": "segmentation",
                "backbone": "timm_clay_v1_base",  # Use the correct Clay model name from registry
                "decoder": self.config["model"]["decoder"], 
                "num_classes": self.config["model"]["num_classes"]
            }
            
            # Build base Clay model (expects 6 channels)
            base_clay_model = factory.build_model(**model_config)
            
            # Get multimodal strategy from config (default to channel_fusion)
            multimodal_strategy = self.config["model"].get("multimodal_strategy", "channel_fusion")
            
            # Wrap Clay model to handle 9-channel multimodal input
            model = ClayMultimodalWrapper(base_clay_model, strategy=multimodal_strategy)
            
            strategy_info = model.get_strategy_info()
            logger.info(f"Built Clay multimodal wrapper: {model_config}")
            logger.info(f"Multimodal strategy: {strategy_info['strategy']}")
            logger.info(f"Strategy description: {strategy_info['description']}")
            logger.info("9-channel multimodal input: DEM + 6×Optical + Thermal + SAR")
            
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
            "iou": JaccardIndex(task="binary"),
            "accuracy": Accuracy(task="binary"), 
            "f1": F1Score(task="binary")
        })
    
    def forward(self, x):
        # Clay typically expects standard 4D input: (batch, channels, height, width)
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
        
        # Log metrics (squeeze masks back for metrics)
        mask_for_metrics = masks.squeeze(1) if masks.dim() == 4 else masks
        if stage == "train":
            metrics = self.train_metrics(outputs, mask_for_metrics.int())
        else:
            metrics = self.val_metrics(outputs, mask_for_metrics.int())
            
        # Get batch size for proper logging
        batch_size = images.size(0)
        
        self.log(f"{stage}_loss", loss, prog_bar=True, batch_size=batch_size)
        self.log_dict({f"{stage}_{k}": v for k, v in metrics.items()}, prog_bar=True, batch_size=batch_size)
        
        return loss
    
    def validation_step(self, batch, batch_idx):
        # log panels only on first val batch to avoid spam
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
            
            B = images.size(0)
            max_samples = min(B, 6)  # Reduce to 6 samples to avoid memory issues
            wandb_images = []
            
            def safe_normalize(x):
                """Safely normalize tensor to 0-1 range and convert to numpy."""
                x = x.detach().cpu().float()
                x_min, x_max = x.min(), x.max()
                if x_max > x_min:
                    x = (x - x_min) / (x_max - x_min)
                # Ensure values are in [0, 1] and convert to numpy
                x = torch.clamp(x, 0, 1).numpy().astype(np.float32)
                return x
            
            import numpy as np
            
            def safe_to_uint8_rgb(img: np.ndarray) -> np.ndarray:
                """Convert normalized image to uint8 RGB format for visualization."""
                if img.ndim == 2:  # Grayscale (H, W)
                    img = np.stack([img, img, img], axis=-1)  # Convert to (H, W, 3)
                elif img.ndim == 3 and img.shape[-1] == 1:  # (H, W, 1) 
                    img = np.repeat(img, 3, axis=-1)  # Convert to (H, W, 3)
                elif img.ndim == 3 and img.shape[-1] == 3:  # Already RGB (H, W, 3)
                    pass  # Keep as is
                else:
                    # Fallback: convert to grayscale and then RGB
                    if img.ndim == 3:
                        img = img.mean(axis=-1)  # Average channels to get grayscale
                    img = np.stack([img, img, img], axis=-1)  # Convert to (H, W, 3)
                
                # Ensure values are in [0, 1] then convert to uint8
                img = np.clip(img, 0, 1)
                return (img * 255).astype(np.uint8)
            
            for i in range(max_samples):
                # Extract and normalize individual modalities from 9-channel input
                # Channels: [DEM(0), B(1), G(2), R(3), NIR(4), SWIR1(5), SWIR2(6), Thermal(7), SAR(8)]
                dem = safe_normalize(images[i, 0])      # (H, W)
                thermal = safe_normalize(images[i, 7])   # (H, W)  
                sar = safe_normalize(images[i, 8])       # (H, W)
                
                # Create RGB composite from optical channels
                optical = images[i, 1:7]  # 6 optical channels
                if optical.size(0) >= 3:
                    # Use RED, GREEN, BLUE channels (indices 2, 1, 0 in optical = 3,2,1 in full image)
                    rgb_r = safe_normalize(optical[2])  # RED
                    rgb_g = safe_normalize(optical[1])  # GREEN  
                    rgb_b = safe_normalize(optical[0])  # BLUE
                    rgb_composite = np.stack([rgb_r, rgb_g, rgb_b], axis=-1)  # (H, W, 3)
                else:
                    rgb_composite = safe_normalize(optical[0])  # Fallback to first optical
                
                # Create false-color composite using NIR (for vegetation visualization)
                if optical.size(0) >= 4:
                    # False color: NIR=Red, RED=Green, GREEN=Blue
                    nir_r = safe_normalize(optical[3])   # NIR as red
                    nir_g = safe_normalize(optical[2])   # RED as green
                    nir_b = safe_normalize(optical[1])   # GREEN as blue
                    nir_composite = np.stack([nir_r, nir_g, nir_b], axis=-1)  # (H, W, 3)
                else:
                    nir_composite = rgb_composite
                
                # Ground truth and predictions - with robust shape handling
                gt_mask = safe_normalize(masks[i].float())           # (H, W)
                
                # Robust extraction of prediction tensors
                if pred_masks.dim() == 4:  # (B, 1, H, W)
                    pred_mask_tensor = pred_masks[i, 0]  # (H, W)
                elif pred_masks.dim() == 3:  # (B, H, W)
                    pred_mask_tensor = pred_masks[i]     # (H, W)
                else:
                    logger.warning(f"Unexpected pred_masks shape: {pred_masks.shape}")
                    pred_mask_tensor = pred_masks[i].reshape(224, 224)  # Force reshape
                
                if pred_probs.dim() == 4:  # (B, 1, H, W)
                    pred_prob_tensor = pred_probs[i, 0]  # (H, W)
                elif pred_probs.dim() == 3:  # (B, H, W)
                    pred_prob_tensor = pred_probs[i]     # (H, W)
                else:
                    logger.warning(f"Unexpected pred_probs shape: {pred_probs.shape}")
                    pred_prob_tensor = pred_probs[i].reshape(224, 224)  # Force reshape
                
                pred_mask = safe_normalize(pred_mask_tensor)
                pred_prob = safe_normalize(pred_prob_tensor)
                
                # Debug: Log individual sample shapes
                logger.info(f"Sample {i} shapes before visualization:")
                logger.info(f"  gt_mask: {gt_mask.shape}")
                logger.info(f"  pred_mask: {pred_mask.shape}")
                logger.info(f"  pred_prob: {pred_prob.shape}")
                logger.info(f"  dem: {dem.shape}")
                logger.info(f"  rgb_composite: {rgb_composite.shape if hasattr(rgb_composite, 'shape') else type(rgb_composite)}")
                
                # Convert all to uint8 RGB format for consistent panel creation
                dem_rgb = safe_to_uint8_rgb(dem)
                thermal_rgb = safe_to_uint8_rgb(thermal)  
                sar_rgb = safe_to_uint8_rgb(sar)
                rgb_rgb = safe_to_uint8_rgb(rgb_composite)
                nir_rgb = safe_to_uint8_rgb(nir_composite)
                gt_rgb = safe_to_uint8_rgb(gt_mask)
                pred_rgb = safe_to_uint8_rgb(pred_mask)
                prob_rgb = safe_to_uint8_rgb(pred_prob)
                
                # Create a comprehensive panel using matplotlib subplots: 2 rows x 4 columns
                fig, axes = plt.subplots(2, 4, figsize=(16, 8))
                fig.suptitle(f'Sample {i+1} - Epoch {self.current_epoch} - Clay Multimodal', fontsize=14)
                
                # Top row: [RGB Optical | DEM | Thermal | SAR]
                axes[0, 0].imshow(rgb_rgb)
                axes[0, 0].set_title('RGB Optical')
                axes[0, 0].axis('off')
                
                axes[0, 1].imshow(dem_rgb)
                axes[0, 1].set_title('DEM')
                axes[0, 1].axis('off')
                
                axes[0, 2].imshow(thermal_rgb)
                axes[0, 2].set_title('Thermal')
                axes[0, 2].axis('off')
                
                axes[0, 3].imshow(sar_rgb)
                axes[0, 3].set_title('SAR')
                axes[0, 3].axis('off')
                
                # Bottom row: [GT Water | Pred Water | Pred Prob | blank]
                axes[1, 0].imshow(gt_rgb)
                axes[1, 0].set_title('Ground Truth Water')
                axes[1, 0].axis('off')
                
                axes[1, 1].imshow(pred_rgb)
                axes[1, 1].set_title('Predicted Water')
                axes[1, 1].axis('off')
                
                axes[1, 2].imshow(prob_rgb)
                axes[1, 2].set_title('Prediction Probability')
                axes[1, 2].axis('off')
                
                axes[1, 3].axis('off')  # blank
                
                plt.tight_layout()
                
                wandb_images.append(wandb.Image(
                    fig,
                    caption=f"epoch {self.current_epoch} | sample {i+1} | clay"
                ))
                
                plt.close(fig)  # Clean up memory
            
            # Log to WandB with better error handling
            if wandb_images:
                self.logger.experiment.log({
                    "val/visualizations": wandb_images,
                    "epoch": self.current_epoch
                }, commit=False)
                
                logger.info(f"Successfully logged {len(wandb_images)} validation visualizations for epoch {self.current_epoch}")
            else:
                logger.warning("No valid images to log for visualization")
            
        except Exception as e:
            import traceback
            logger.warning(f"Failed to log validation visuals: {e}")
            logger.warning(f"Traceback: {traceback.format_exc()}")


def main():
    parser = argparse.ArgumentParser(description="Clay Foundation Model Experiment Training")
    parser.add_argument("--config", default="configs/clay_config.yaml", 
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
    logger.info("Setting up clay model...")
    model = ClayFoundationModel(config)
    
    # Get run name from environment (set by SLURM array) or use default
    wandb_run_name = os.environ.get('WANDB_NAME') or os.environ.get('WANDB_RUN_ID') or "clay_4modal_water_segmentation"
    
    # Set up logger
    wandb_logger = WandbLogger(
        project=config["logging"]["project_name"],
        name=wandb_run_name,
        tags=config["logging"]["wandb"]["tags"],
        group=config["logging"]["wandb"]["group"],
        notes=config["logging"]["wandb"]["notes"]
    )
    
    # Set up checkpoint directory based on wandb run
    # If WANDB_RUN_ID or WANDB_NAME is set (from SLURM array), use it for unique directory
    wandb_run_name = os.environ.get('WANDB_NAME') or os.environ.get('WANDB_RUN_ID')
    if wandb_run_name:
        checkpoint_dir = os.path.join("outputs", "models", wandb_run_name, "checkpoints")
    else:
        # Fallback to generic directory
        checkpoint_dir = os.path.join("outputs", "models", "checkpoints")
    
    # Create checkpoint directory
    os.makedirs(checkpoint_dir, exist_ok=True)
    logger.info(f"Checkpoints will be saved to: {checkpoint_dir}")
    
    # Set up log directory with same pattern as checkpoints
    if wandb_run_name:
        log_dir = os.path.join("logs", wandb_run_name)
    else:
        log_dir = os.path.join("logs", "training")
    
    # Create log directory
    os.makedirs(log_dir, exist_ok=True)
    logger.info(f"Training logs will be saved to: {log_dir}")
    
    # Set up file logging
    log_file = os.path.join(log_dir, "training.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    logger.info(f"File logging initialized: {log_file}")
    
    # Set up callbacks
    callbacks = [
        ModelCheckpoint(
            dirpath=checkpoint_dir,
            monitor=config["logging"]["monitor"],
            mode=config["training"]["monitor_mode"],
            save_top_k=config["logging"]["save_top_k"],
            save_last=config["logging"]["save_last"],
            filename="{epoch:02d}-{val_loss:.4f}"
        ),
        EarlyStopping(
            monitor=config["training"]["monitor_metric"],
            mode=config["training"]["monitor_mode"], 
            patience=config["training"]["patience"]
        ),
        LearningRateMonitor(logging_interval="step")
    ]
    
    # Set up trainer with custom log directory
    trainer = pl.Trainer(
        max_epochs=config["training"]["max_epochs"],
        accelerator="auto",
        devices=args.gpus,
        precision=config["training"]["precision"],
        logger=wandb_logger,
        callbacks=callbacks,
        gradient_clip_val=config["training"]["gradient_clip_val"],
        log_every_n_steps=config["logging"]["log_every_n_steps"],
        val_check_interval=config["logging"]["val_check_interval"],
        default_root_dir=log_dir  # PyTorch Lightning logs go here
    )
    
    # Save configuration to log directory
    config_log_file = os.path.join(log_dir, "config.json")
    with open(config_log_file, 'w') as f:
        json.dump(config, f, indent=2)
    logger.info(f"Configuration saved to: {config_log_file}")
    
    # Save run information
    run_info = {
        "wandb_run_name": wandb_run_name or "local_development",
        "checkpoint_dir": checkpoint_dir,
        "log_dir": log_dir,
        "model_parameters": model.count_parameters(),
        "input_channels": 9,
        "model_type": "timm_clay_v1_base",
        "start_time": logging.Formatter().formatTime(logging.LogRecord("", 0, "", 0, "", (), None))
    }
    
    run_info_file = os.path.join(log_dir, "run_info.json")
    with open(run_info_file, 'w') as f:
        json.dump(run_info, f, indent=2)
    logger.info(f"Run information saved to: {run_info_file}")
    
    # Train
    logger.info("Starting clay training...")
    trainer.fit(model, data_module)
    
    # Save training completion info
    completion_info = {
        "status": "completed",
        "best_model_path": callbacks[0].best_model_path if callbacks[0].best_model_path else "N/A",
        "best_model_score": float(callbacks[0].best_model_score) if callbacks[0].best_model_score else "N/A",
        "end_time": logging.Formatter().formatTime(logging.LogRecord("", 0, "", 0, "", (), None))
    }
    
    completion_file = os.path.join(log_dir, "completion_info.json")
    with open(completion_file, 'w') as f:
        json.dump(completion_info, f, indent=2)
    logger.info(f"Training completed! Completion info saved to: {completion_file}")
    
    logger.info("clay training completed!")


if __name__ == "__main__":
    main()
