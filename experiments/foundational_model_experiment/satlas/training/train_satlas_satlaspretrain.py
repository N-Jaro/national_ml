#!/usr/bin/env python3
"""
SATLAS Pretrained Foundation Model Training Script (satlaspretrain-models approach)
Uses the official satlaspretrain-models package for proper pretrained weights loading
"""

import os
import sys
import argparse
from pathlib import Path
import yaml
import logging
import json
import math
import traceback
from datetime import datetime

import torch
import torch.nn as nn
import torch.nn.functional as F
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping, LearningRateMonitor
from pytorch_lightning.loggers import WandbLogger

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import wandb

matplotlib.use('Agg')  # Use non-interactive backend for server environments

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))
sys.path.append(str(Path(__file__).parent.parent))  # Add experiment directory

# SATLAS pretrained models import
try:
    import satlaspretrain_models
    SATLASPRETRAIN_AVAILABLE = True
except ImportError as e:
    print(f"satlaspretrain-models import error: {e}")
    print("Please install: pip install satlaspretrain-models")
    SATLASPRETRAIN_AVAILABLE = False

# Local imports
from data.four_modal_dataset_adapter import FourModalDataModule
from utils.losses import CombinedFocalDiceLoss

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SatlasPretrainedSegmentationHead(nn.Module):
    """
    Proper FPN-based segmentation head for SATLAS pretrained model.
    Uses skip connections and proper upsampling to maintain spatial resolution.
    """
    
    def __init__(self, fpn_channels=128, num_classes=1):
        super().__init__()
        
        self.fpn_channels = fpn_channels
        
        # Use the highest resolution feature (index 0) as primary
        # and add skip connections from lower resolution features
        
        # Lateral connections to reduce channel dimensions
        self.lateral_convs = nn.ModuleList([
            nn.Conv2d(fpn_channels, 64, kernel_size=1) for _ in range(5)
        ])
        
        # Upsampling path with skip connections (like U-Net)
        self.upconv1 = nn.Sequential(
            nn.ConvTranspose2d(64, 32, kernel_size=4, stride=2, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(inplace=True)
        )
        
        self.upconv2 = nn.Sequential(
            nn.ConvTranspose2d(64, 16, kernel_size=4, stride=2, padding=1),  # 64 = 32 + 32 from skip
            nn.BatchNorm2d(16),
            nn.ReLU(inplace=True)
        )
        
        self.upconv3 = nn.Sequential(
            nn.ConvTranspose2d(32, 8, kernel_size=4, stride=2, padding=1),   # 32 = 16 + 16 from skip
            nn.BatchNorm2d(8),
            nn.ReLU(inplace=True)
        )
        
        # Final layers
        self.final_conv = nn.Sequential(
            nn.Conv2d(16, 8, kernel_size=3, padding=1),  # 16 = 8 + 8 from skip
            nn.BatchNorm2d(8),
            nn.ReLU(inplace=True),
            nn.Dropout2d(0.1),
            nn.Conv2d(8, num_classes, kernel_size=1)  # 1x1 for final prediction
        )
        
        # Initialize weights properly
        self._initialize_weights()
        
    def forward(self, features):
        """
        Forward pass through proper FPN-based segmentation head.
        
        Args:
            features: List of feature maps from SATLAS FPN
                     Features[0]: [B, 128, 224, 224] (highest resolution)
                     Features[1]: [B, 128, 56, 56]
                     Features[2]: [B, 128, 28, 28]
                     Features[3]: [B, 128, 14, 14]
                     Features[4]: [B, 128, 7, 7]   (lowest resolution)
        """
        if len(features) < 4:
            raise ValueError(f"Expected at least 4 FPN features, got {len(features)}")
        
        # Apply lateral convolutions to reduce channels
        laterals = []
        for i, feat in enumerate(features[:4]):  # Use top 4 features
            if i < len(self.lateral_convs):
                lateral = self.lateral_convs[i](feat)  # 128 -> 64 channels
                laterals.append(lateral)
        
        # Start from the lowest resolution feature (index 3: 14x14)
        x = laterals[3]  # [B, 64, 14, 14]
        
        # Upsampling path with skip connections
        # Upsample to 28x28 and add skip from features[2]
        x = self.upconv1(x)  # [B, 32, 28, 28]
        if len(laterals) > 2:
            skip = laterals[2]  # [B, 64, 28, 28]
            skip = F.interpolate(skip, size=x.shape[2:], mode='bilinear', align_corners=False)
            x = torch.cat([x, skip[:, :32]], dim=1)  # [B, 64, 28, 28]
        
        # Upsample to 56x56 and add skip from features[1]
        x = self.upconv2(x)  # [B, 16, 56, 56]
        if len(laterals) > 1:
            skip = laterals[1]  # [B, 64, 56, 56]
            skip = F.interpolate(skip, size=x.shape[2:], mode='bilinear', align_corners=False)
            x = torch.cat([x, skip[:, :16]], dim=1)  # [B, 32, 56, 56]
        
        # Upsample to 112x112 and add skip from features[0]
        x = self.upconv3(x)  # [B, 8, 112, 112]
        if len(laterals) > 0:
            skip = laterals[0]  # [B, 64, 224, 224]
            # Downsample skip to match current resolution
            skip = F.interpolate(skip, size=x.shape[2:], mode='bilinear', align_corners=False)
            x = torch.cat([x, skip[:, :8]], dim=1)  # [B, 16, 112, 112]
        
        # Final upsampling to full resolution and prediction
        x = F.interpolate(x, size=(224, 224), mode='bilinear', align_corners=False)  # [B, 16, 224, 224]
        output = self.final_conv(x)  # [B, num_classes, 224, 224]
        
        return output
    
    def _initialize_weights(self):
        """Initialize all layers with proper initialization for stable training."""
        for m in self.modules():
            if isinstance(m, nn.Conv2d):
                nn.init.kaiming_normal_(m.weight, mode='fan_out', nonlinearity='relu')
                if m.bias is not None:
                    nn.init.constant_(m.bias, 0)
            elif isinstance(m, nn.ConvTranspose2d):
                # Use bilinear interpolation weights for upsampling layers
                nn.init.kaiming_normal_(m.weight, mode='fan_out', nonlinearity='relu')
                if m.bias is not None:
                    nn.init.constant_(m.bias, 0)
            elif isinstance(m, nn.BatchNorm2d):
                nn.init.constant_(m.weight, 1)
                nn.init.constant_(m.bias, 0)
        
        # Special initialization for final prediction layer
        final_conv = None
        for m in self.final_conv.modules():
            if isinstance(m, nn.Conv2d) and m.kernel_size == (1, 1):  # Final 1x1 conv
                final_conv = m
                break
        
        if final_conv is not None:
            # Small positive bias for water detection with severe class imbalance
            # Use smaller weight initialization to prevent exploding gradients
            nn.init.xavier_uniform_(final_conv.weight, gain=0.1)  # Smaller gain for stability
            nn.init.constant_(final_conv.bias, 0.05)  # Smaller positive bias


class SatlasPretrainedModel(pl.LightningModule):
    """
    SATLAS Foundation Model with official pretrained weights from satlaspretrain-models.
    """
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.save_hyperparameters()
        
        # Build model with pretrained SATLAS weights
        self.backbone, self.segmentation_head = self._build_model()
        
        # Build loss function
        self.criterion = self._build_loss()
        
        # Build metrics
        self.train_metrics = self._build_metrics("train")
        self.val_metrics = self._build_metrics("val")
        
        logger.info(f"SATLAS pretrained model initialized with {self.count_parameters():,} parameters")
        
        # Add debugging flag
        self._debug_step = 0

    def _build_model(self):
        """Build SATLAS model with pretrained weights using satlaspretrain-models."""
        if not SATLASPRETRAIN_AVAILABLE:
            raise ImportError("satlaspretrain-models not available. Please install: pip install satlaspretrain-models")
        
        try:
            logger.info("Loading SATLAS pretrained model using satlaspretrain-models package...")
            
            # Detect device - auto use GPU if available, fallback to CPU
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            logger.info(f"Using device: {device}")
            
            # Initialize weights manager
            weights_manager = satlaspretrain_models.Weights()
            
            # Load SATLAS Sentinel-2 Multi-Image Multi-Spectral model
            # This model is pretrained on Sentinel-2 multi-spectral data (9 channels)
            model_id = "Sentinel2_SwinB_MI_MS"
            
            logger.info(f"Loading pretrained model: {model_id}")
            
            # Load backbone with FPN (Feature Pyramid Network)
            # Handle CPU/GPU loading properly
            if device.type == "cpu":
                logger.info("Loading model for CPU execution...")
                # Patch the satlaspretrain_models module to use CPU loading
                import satlaspretrain_models.model as satlas_model
                original_torch_load = satlas_model.torch.load
                
                def cpu_torch_load(weights_file, *args, **kwargs):
                    logger.info(f"Loading weights from {weights_file} with CPU map_location")
                    return original_torch_load(weights_file, map_location=torch.device('cpu'))
                
                # Temporarily replace torch.load in the satlaspretrain_models module
                satlas_model.torch.load = cpu_torch_load
                try:
                    backbone = weights_manager.get_pretrained_model(model_id, fpn=True)
                finally:
                    # Restore original torch.load
                    satlas_model.torch.load = original_torch_load
            else:
                logger.info("Loading model for GPU execution...")
                backbone = weights_manager.get_pretrained_model(model_id, fpn=True)
            
            # Move backbone to appropriate device
            backbone = backbone.to(device)
            
            # Create segmentation head
            # SATLAS FPN outputs have 128 channels at all scales
            segmentation_head = SatlasPretrainedSegmentationHead(
                fpn_channels=128,  # FPN output channels
                num_classes=self.config["model"]["num_classes"]
            ).to(device)
            
            logger.info("Successfully loaded SATLAS pretrained model with segmentation head")
            logger.info(f"Backbone parameters: {sum(p.numel() for p in backbone.parameters()):,}")
            logger.info(f"Segmentation head parameters: {sum(p.numel() for p in segmentation_head.parameters()):,}")
            
            return backbone, segmentation_head
            
        except Exception as e:
            logger.error(f"Failed to load SATLAS pretrained model: {e}")
            logger.error(f"Error details: {str(e)}")
            
            # If loading pretrained weights fails, provide fallback option
            if "CUDA" in str(e) and not torch.cuda.is_available():
                logger.warning("CUDA weights loading failed on CPU. Consider using CPU-compatible model.")
            
            raise

    def _build_loss(self):
        """Build combined focal-dice loss following Prithvi's proven stable pattern."""
        loss_config = self.config["loss"]
        
        # Use EXACT same pattern as Prithvi for stability
        # No modifications to proven parameters
        return CombinedFocalDiceLoss(
            focal_weight=loss_config["focal_weight"],   # 0.7 (matches Prithvi)
            dice_weight=loss_config["dice_weight"],     # 0.3 (matches Prithvi)
            focal_alpha=loss_config["focal_alpha"],     # 0.75 (stable, no doubling)
            focal_gamma=loss_config["focal_gamma"]      # 1.5 (matches Prithvi)
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
        """Forward pass through SATLAS backbone and segmentation head."""
        # Debug input shape on first forward pass
        if self._debug_step == 0:
            logger.info(f"Forward pass - input shape: {x.shape}")
            logger.info(f"Input value ranges: min={x.min().item():.3f}, max={x.max().item():.3f}")
        
        # SATLAS backbone expects input in specific format
        # For Sentinel-2 MS: 9 channels with specific normalization
        # Our data: [DEM, B2, B3, B4, B5, B6, B7, B10, SAR] -> need to map to S2 bands
        
        try:
            # Extract features using SATLAS backbone
            features = self.backbone(x)
            
            if self._debug_step == 0:
                logger.info(f"Backbone output - {len(features)} feature maps")
                for i, feat in enumerate(features):
                    logger.info(f"  Feature {i}: {feat.shape}")
            
            # Apply segmentation head
            output = self.segmentation_head(features)
            
            if self._debug_step == 0:
                logger.info(f"Segmentation head output: {output.shape}")
                logger.info(f"Output value ranges: min={output.min().item():.3f}, max={output.max().item():.3f}")
                
            return output
            
        except Exception as e:
            logger.error(f"Forward pass error: {e}")
            logger.error(f"Input shape: {x.shape}")
            raise

    def training_step(self, batch, batch_idx):
        # Minimal debug logging for first step only
        if self._debug_step == 0:
            images = batch['image']
            logger.info(f"Training started - batch shape: {images.shape}")
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
        
        # Ensure model is in correct mode
        if stage == "train":
            self.train()
        else:
            self.eval()
        
        outputs = self(images)
        
        # Ensure masks have correct shape for loss computation
        if masks.dim() == 3 and outputs.dim() == 4:  # (B,H,W) -> (B,1,H,W)
            masks = masks.unsqueeze(1)
        
        loss = self.criterion(outputs, masks)
        
        # Prepare data for metrics computation
        mask_for_metrics = masks.squeeze(1) if masks.dim() == 4 else masks
        
        # Convert model outputs to binary predictions for metrics
        with torch.no_grad():
            pred_probs = torch.sigmoid(outputs)
            pred_for_metrics = (pred_probs > 0.5).float()
            pred_for_metrics = pred_for_metrics.squeeze(1) if pred_for_metrics.dim() == 4 else pred_for_metrics
        
        # Update metrics (don't compute here, just update)
        if stage == "train":
            self.train_metrics.update(pred_for_metrics.int(), mask_for_metrics.int())
        else:
            self.val_metrics.update(pred_for_metrics.int(), mask_for_metrics.int())
        
        # Get batch size for proper logging
        batch_size = images.size(0)
        
        # Log loss immediately
        self.log(f"{stage}_loss", loss, prog_bar=True, batch_size=batch_size, on_step=True, on_epoch=True)
        
        # Log step-level metrics for monitoring (but don't use for final epoch metrics)
        if stage == "train" and self.global_step % 50 == 0:  # Log every 50 steps for training
            step_metrics = self.train_metrics.compute()
            for metric_name, metric_value in step_metrics.items():
                self.log(f"train_{metric_name}_step", metric_value, prog_bar=False, on_step=True, on_epoch=False)
        elif stage == "val" and self.global_step % 100 == 0:  # Log every 100 steps for validation
            step_metrics = self.val_metrics.compute()
            for metric_name, metric_value in step_metrics.items():
                self.log(f"val_{metric_name}_step", metric_value, prog_bar=False, on_step=True, on_epoch=False)
        
        return loss

    def validation_step(self, batch, batch_idx):
        # Debug logging for first validation batch of first epoch
        if batch_idx == 0 and self.current_epoch == 0:
            logger.info(f"Validation step - batch {batch_idx}, epoch {self.current_epoch}")
            logger.info(f"Input shape: {batch['image'].shape}, mask shape: {batch['mask'].shape}")
        
        # log panels only on first val batch to avoid spam
        if batch_idx == 0:
            self._log_val_visuals(batch)
            
        return self._step(batch, "val")
    
    def on_validation_epoch_end(self):
        """Called at the end of validation epoch to compute and log final metrics."""
        # Compute validation metrics - this ensures metrics are properly calculated
        val_metrics = self.val_metrics.compute()
        
        # Log final validation metrics for the epoch
        for metric_name, metric_value in val_metrics.items():
            self.log(f"val_{metric_name}_epoch", metric_value, prog_bar=True, sync_dist=True)
        
        # Reset validation metrics for next epoch
        self.val_metrics.reset()
        
        # Log additional debugging info every 5 epochs
        if self.current_epoch % 5 == 0:
            logger.info(f"Epoch {self.current_epoch} validation completed:")
            for metric_name, metric_value in val_metrics.items():
                logger.info(f"  val_{metric_name}: {metric_value:.4f}")
    
    def on_train_epoch_end(self):
        """Called at the end of training epoch to compute and log final metrics."""
        # Compute training metrics
        train_metrics = self.train_metrics.compute()
        
        # Log final training metrics for the epoch
        for metric_name, metric_value in train_metrics.items():
            self.log(f"train_{metric_name}_epoch", metric_value, prog_bar=True, sync_dist=True)
        
        # Reset training metrics for next epoch
        self.train_metrics.reset()

    def _log_val_visuals(self, batch):
        """Log validation visualizations to WandB every 10 epochs."""
        if not isinstance(self.logger, WandbLogger):
            return
        if self.global_rank != 0:  # avoid duplicate logs under DDP
            return
        if (self.current_epoch % 10) != 0:  # Log visualizations every 10 epochs
            return
        
        try:
            import wandb
            
            images = batch['image']  # (B, 9, H, W)
            masks = batch['mask']    # (B, H, W)
            
            # Get predictions
            with torch.no_grad():
                outputs = self(images)
                pred_probs = torch.sigmoid(outputs)  # (B, 1, H, W)
                pred_masks = (pred_probs >= 0.5).float()  # (B, 1, H, W)
                
                # Minimal shape logging
                logger.info(f"Logging validation visualizations for epoch {self.current_epoch}")
            
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
                # Extract and normalize individual channels
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
                
                # Minimal sample logging (removed detailed shape debug)
                
                # Convert all to uint8 RGB format for consistent panel creation
                dem_rgb = safe_to_uint8_rgb(dem)
                thermal_rgb = safe_to_uint8_rgb(thermal)  
                sar_rgb = safe_to_uint8_rgb(sar)
                rgb_rgb = safe_to_uint8_rgb(rgb_composite)
                gt_rgb = safe_to_uint8_rgb(gt_mask)
                pred_rgb = safe_to_uint8_rgb(pred_mask)
                prob_rgb = safe_to_uint8_rgb(pred_prob)
                
                # Create a comprehensive panel using matplotlib subplots: 2 rows x 4 columns
                fig, axes = plt.subplots(2, 4, figsize=(16, 8))
                fig.suptitle(f'Sample {i+1} - Epoch {self.current_epoch} - SATLAS Pretrained', fontsize=14)
                
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
                    caption=f"epoch {self.current_epoch} | sample {i+1} | satlas_pretrained"
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
            logger.warning(f"Failed to log validation visuals: {e}")
            logger.warning(f"Traceback: {traceback.format_exc()}")

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

    def count_parameters(self):
        return sum(p.numel() for p in self.parameters() if p.requires_grad)


def main():
    """Main training function."""
    parser = argparse.ArgumentParser(description="Train SATLAS Pretrained Foundation Model (satlaspretrain-models)")
    parser.add_argument("--config", type=str, required=True, help="Path to config file")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--gpus", type=int, default=1, help="Number of GPUs")
    args = parser.parse_args()
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Starting SATLAS Pretrained Foundation Model Experiment: {config['experiment']['name']}")
    logger.info("This uses official SATLAS pretrained weights via satlaspretrain-models package")
    
    # Modify config for testing
    if args.test:
        config["training"]["max_epochs"] = 2
        config["data"]["huc_codes"] = config["data"]["huc_codes"][:2]
        config["training"]["batch_size"] = 4
        config["logging"]["project_name"] = f"{config['logging']['project_name']}_test"
    
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
    wandb_run_name = os.environ.get('WANDB_NAME') or os.environ.get('WANDB_RUN_ID') or "satlas_satlaspretrain_9ch_water_segmentation"
    
    # Set up logger
    wandb_logger = WandbLogger(
        project=config["logging"]["project_name"],
        name=wandb_run_name,
        tags=config["logging"]["wandb"]["tags"] + ["satlaspretrain_models"],
        group=config["logging"]["wandb"]["group"],
        notes=config["logging"]["wandb"]["notes"] + " - Using official satlaspretrain-models package"
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
        "pretrained_weights": "SATLAS via satlaspretrain-models",
        "backbone": "Sentinel2_SwinB_MI_MS"
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
    
    # Set up log directory with same pattern as checkpoints
    if wandb_run_name_for_dir:
        log_dir = os.path.join("logs", wandb_run_name_for_dir)
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
    
    # Auto-detect accelerator and devices
    if args.gpus == 0 or not torch.cuda.is_available():
        accelerator = "cpu"
        devices = 1
        precision = "32"  # Use 32-bit precision for CPU
        logger.info("Using CPU for training (GPU not available or --gpus 0)")
    else:
        accelerator = "gpu"
        devices = args.gpus
        precision = config["training"]["precision"]
        logger.info(f"Using GPU for training ({devices} GPUs)")
    
    # Set up trainer with auto-detected configuration
    trainer = pl.Trainer(
        max_epochs=config["training"]["max_epochs"],
        accelerator=accelerator,
        devices=devices,
        precision=precision,
        logger=wandb_logger,
        callbacks=callbacks,
        gradient_clip_val=config["training"]["gradient_clip_val"],
        log_every_n_steps=config["logging"]["log_every_n_steps"],
        val_check_interval=config["logging"]["val_check_interval"],
        default_root_dir=log_dir,  # PyTorch Lightning logs go here
        deterministic=config.get("reproducibility", {}).get("deterministic", False),
        benchmark=config.get("reproducibility", {}).get("benchmark", True)
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
        "model_type": "satlas_satlaspretrain_sentinel2_swinb_mi_ms",
        "pretrained_weights": "SATLAS via satlaspretrain-models",
        "backbone": "Sentinel2_SwinB_MI_MS",
        "start_time": logging.Formatter().formatTime(logging.LogRecord("", 0, "", 0, "", (), None))
    }
    
    run_info_file = os.path.join(log_dir, "run_info.json")
    with open(run_info_file, 'w') as f:
        json.dump(run_info, f, indent=2)
    logger.info(f"Run information saved to: {run_info_file}")
    
    # Train model
    logger.info("Starting SATLAS pretrained training...")
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
    
    logger.info("SATLAS pretrained training completed!")


if __name__ == "__main__":
    main()