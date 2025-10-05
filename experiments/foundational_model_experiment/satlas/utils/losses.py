#!/usr/bin/env python3
"""
Loss Functions for SatLas Foundation Model Experiment

SatLas-specific loss functions optimized for Sentinel-2-based water segmentation.
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Optional


class SatlasFocalLoss(nn.Module):
    """
    Focal Loss implementation optimized for SatLas water segmentation.
    
    Addresses class imbalance in water detection by down-weighting
    easy examples and focusing on hard negatives.
    """
    
    def __init__(self, alpha: float = 0.25, gamma: float = 2.0, 
                 reduction: str = 'mean'):
        """
        Initialize SatLas Focal Loss.
        
        Args:
            alpha: Weighting factor for rare class (water)
            gamma: Focusing parameter (higher = more focus on hard examples)
            reduction: Reduction method ('mean', 'sum', 'none')
        """
        super().__init__()
        self.alpha = alpha
        self.gamma = gamma
        self.reduction = reduction
    
    def forward(self, inputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """
        Compute SatLas focal loss.
        
        Args:
            inputs: Predictions of shape (N, 1, H, W) or (N, H, W)
            targets: Ground truth of shape (N, H, W) or (N, 1, H, W)
            
        Returns:
            Focal loss value
        """
        # Ensure correct shapes
        if inputs.dim() == 4 and inputs.size(1) == 1:
            inputs = inputs.squeeze(1)  # (N, H, W)
        if targets.dim() == 4 and targets.size(1) == 1:
            targets = targets.squeeze(1)  # (N, H, W)
        
        # Compute BCE with logits
        bce_loss = F.binary_cross_entropy_with_logits(
            inputs, targets, reduction='none'
        )
        
        # Compute probabilities and focal weight
        p_t = torch.sigmoid(inputs)
        p_t = torch.where(targets == 1, p_t, 1 - p_t)
        
        # Alpha weighting
        alpha_t = torch.where(targets == 1, self.alpha, 1 - self.alpha)
        
        # Focal weight
        focal_weight = alpha_t * (1 - p_t) ** self.gamma
        
        # Apply focal weight
        focal_loss = focal_weight * bce_loss
        
        # Apply reduction
        if self.reduction == 'mean':
            return focal_loss.mean()
        elif self.reduction == 'sum':
            return focal_loss.sum()
        else:
            return focal_loss


class SatlasDiceLoss(nn.Module):
    """
    Dice Loss implementation optimized for SatLas water segmentation.
    
    Optimizes for spatial overlap, particularly important for water body
    boundary detection in satellite imagery.
    """
    
    def __init__(self, smooth: float = 1e-6, reduction: str = 'mean'):
        """
        Initialize SatLas Dice Loss.
        
        Args:
            smooth: Smoothing factor to avoid division by zero
            reduction: Reduction method ('mean', 'sum', 'none')
        """
        super().__init__()
        self.smooth = smooth
        self.reduction = reduction
    
    def forward(self, inputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """
        Compute SatLas dice loss.
        
        Args:
            inputs: Predictions of shape (N, 1, H, W) or (N, H, W)
            targets: Ground truth of shape (N, H, W) or (N, 1, H, W)
            
        Returns:
            Dice loss value
        """
        # Ensure correct shapes
        if inputs.dim() == 4 and inputs.size(1) == 1:
            inputs = inputs.squeeze(1)  # (N, H, W)
        if targets.dim() == 4 and targets.size(1) == 1:
            targets = targets.squeeze(1)  # (N, H, W)
        
        # Apply sigmoid to get probabilities
        inputs_sigmoid = torch.sigmoid(inputs)
        
        # Flatten tensors for computation
        inputs_flat = inputs_sigmoid.view(inputs_sigmoid.size(0), -1)
        targets_flat = targets.view(targets.size(0), -1)
        
        # Compute Dice coefficient per sample
        intersection = (inputs_flat * targets_flat).sum(dim=1)
        union = inputs_flat.sum(dim=1) + targets_flat.sum(dim=1)
        
        dice_coeff = (2.0 * intersection + self.smooth) / (union + self.smooth)
        dice_loss = 1.0 - dice_coeff
        
        # Apply reduction
        if self.reduction == 'mean':
            return dice_loss.mean()
        elif self.reduction == 'sum':
            return dice_loss.sum()
        else:
            return dice_loss


class SatlasCombinedLoss(nn.Module):
    """
    Combined Focal + Dice Loss optimized for SatLas water segmentation.
    
    Combines the benefits of both losses:
    - Focal loss for handling class imbalance
    - Dice loss for optimizing spatial overlap
    """
    
    def __init__(self, 
                 focal_weight: float = 0.7,
                 dice_weight: float = 0.3,
                 focal_alpha: float = 0.25,
                 focal_gamma: float = 2.0,
                 dice_smooth: float = 1e-6):
        """
        Initialize SatLas combined loss.
        
        Args:
            focal_weight: Weight for focal loss component
            dice_weight: Weight for dice loss component  
            focal_alpha: Alpha parameter for focal loss
            focal_gamma: Gamma parameter for focal loss
            dice_smooth: Smoothing parameter for dice loss
        """
        super().__init__()
        
        self.focal_weight = focal_weight
        self.dice_weight = dice_weight
        
        self.focal_loss = SatlasFocalLoss(
            alpha=focal_alpha,
            gamma=focal_gamma,
            reduction='mean'
        )
        
        self.dice_loss = SatlasDiceLoss(
            smooth=dice_smooth,
            reduction='mean'
        )
    
    def forward(self, inputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """
        Compute combined focal + dice loss.
        
        Args:
            inputs: Predictions of shape (N, 1, H, W) or (N, H, W)
            targets: Ground truth of shape (N, H, W) or (N, 1, H, W)
            
        Returns:
            Combined loss value
        """
        focal_loss_value = self.focal_loss(inputs, targets)
        dice_loss_value = self.dice_loss(inputs, targets)
        
        combined_loss = (
            self.focal_weight * focal_loss_value +
            self.dice_weight * dice_loss_value
        )
        
        return combined_loss


class SatlasIoULoss(nn.Module):
    """
    IoU (Jaccard) Loss for SatLas water segmentation.
    
    Directly optimizes the IoU metric, which is commonly used
    for evaluating water segmentation performance.
    """
    
    def __init__(self, smooth: float = 1e-6, reduction: str = 'mean'):
        """
        Initialize SatLas IoU Loss.
        
        Args:
            smooth: Smoothing factor to avoid division by zero
            reduction: Reduction method ('mean', 'sum', 'none')
        """
        super().__init__()
        self.smooth = smooth
        self.reduction = reduction
    
    def forward(self, inputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """
        Compute IoU loss.
        
        Args:
            inputs: Predictions of shape (N, 1, H, W) or (N, H, W)
            targets: Ground truth of shape (N, H, W) or (N, 1, H, W)
            
        Returns:
            IoU loss value
        """
        # Ensure correct shapes
        if inputs.dim() == 4 and inputs.size(1) == 1:
            inputs = inputs.squeeze(1)
        if targets.dim() == 4 and targets.size(1) == 1:
            targets = targets.squeeze(1)
        
        # Apply sigmoid
        inputs_sigmoid = torch.sigmoid(inputs)
        
        # Flatten
        inputs_flat = inputs_sigmoid.view(inputs_sigmoid.size(0), -1)
        targets_flat = targets.view(targets.size(0), -1)
        
        # Compute IoU per sample
        intersection = (inputs_flat * targets_flat).sum(dim=1)
        union = inputs_flat.sum(dim=1) + targets_flat.sum(dim=1) - intersection
        
        iou = (intersection + self.smooth) / (union + self.smooth)
        iou_loss = 1.0 - iou
        
        # Apply reduction
        if self.reduction == 'mean':
            return iou_loss.mean()
        elif self.reduction == 'sum':
            return iou_loss.sum()
        else:
            return iou_loss


class SatlasAdaptiveLoss(nn.Module):
    """
    Adaptive loss that adjusts weights based on training progress
    for SatLas water segmentation.
    """
    
    def __init__(self, 
                 initial_focal_weight: float = 0.8,
                 initial_dice_weight: float = 0.2,
                 adaptation_epochs: int = 50):
        """
        Initialize adaptive loss.
        
        Args:
            initial_focal_weight: Initial weight for focal loss
            initial_dice_weight: Initial weight for dice loss
            adaptation_epochs: Number of epochs over which to adapt
        """
        super().__init__()
        
        self.initial_focal_weight = initial_focal_weight
        self.initial_dice_weight = initial_dice_weight
        self.adaptation_epochs = adaptation_epochs
        
        self.focal_loss = SatlasFocalLoss()
        self.dice_loss = SatlasDiceLoss()
        
        self.current_epoch = 0
    
    def set_epoch(self, epoch: int):
        """Set current training epoch for adaptation."""
        self.current_epoch = epoch
    
    def forward(self, inputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """
        Compute adaptive loss.
        
        Args:
            inputs: Predictions
            targets: Ground truth
            
        Returns:
            Adaptive loss value
        """
        # Compute adaptation factor (0 -> 1 over adaptation_epochs)
        adaptation_factor = min(self.current_epoch / self.adaptation_epochs, 1.0)
        
        # Gradually shift from focal-heavy to dice-heavy
        focal_weight = self.initial_focal_weight * (1 - adaptation_factor * 0.3)
        dice_weight = self.initial_dice_weight * (1 + adaptation_factor * 1.5)
        
        # Normalize weights
        total_weight = focal_weight + dice_weight
        focal_weight /= total_weight
        dice_weight /= total_weight
        
        # Compute weighted loss
        focal_loss_value = self.focal_loss(inputs, targets)
        dice_loss_value = self.dice_loss(inputs, targets)
        
        return focal_weight * focal_loss_value + dice_weight * dice_loss_value


# Alias for backward compatibility  
CombinedFocalDiceLoss = SatlasCombinedLoss


def test_satlas_losses():
    """Test SatLas loss functions."""
    print("Testing SatLas Loss Functions...")
    
    try:
        # Create test data
        batch_size, height, width = 2, 64, 64
        
        # Predictions (logits)
        inputs = torch.randn(batch_size, height, width)
        
        # Ground truth (binary)
        targets = torch.randint(0, 2, (batch_size, height, width)).float()
        
        print(f"Input shape: {inputs.shape}")
        print(f"Target shape: {targets.shape}")
        
        # Test each loss function
        losses = {
            'Focal': SatlasFocalLoss(),
            'Dice': SatlasDiceLoss(), 
            'Combined': SatlasCombinedLoss(),
            'IoU': SatlasIoULoss(),
            'Adaptive': SatlasAdaptiveLoss()
        }
        
        for name, loss_fn in losses.items():
            loss_value = loss_fn(inputs, targets)
            print(f"{name} Loss: {loss_value.item():.4f}")
        
        print("✅ SatLas loss functions test successful!")
        return True
        
    except Exception as e:
        print(f"❌ SatLas loss functions test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_satlas_losses()