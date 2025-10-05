"""
Clay Multimodal Wrapper - Handles 9-channel input for Clay foundation model.

This wrapper enables Clay (which expects 6 channels) to work with 9-channel multimodal input
(DEM + 6×Optical + Thermal + SAR) by using different channel adaptation strategies.
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class ClayMultimodalWrapper(nn.Module):
    """
    Wrapper that adapts 9-channel multimodal input to work with Clay's 6-channel expectation.
    
    Strategies:
    1. 'optical_only': Use only 6 optical channels (channels 1-6)
    2. 'channel_fusion': Combine modalities intelligently into 6 channels
    3. 'learned_projection': Learn a projection from 9 to 6 channels
    """
    
    def __init__(self, clay_model, strategy='channel_fusion'):
        super().__init__()
        self.clay_model = clay_model
        self.strategy = strategy
        
        if strategy == 'learned_projection':
            # Learnable 1x1 conv to project 9 channels to 6
            self.channel_projection = nn.Conv2d(9, 6, kernel_size=1, bias=True)
            # Initialize projection intelligently
            self._init_projection_weights()
        
        logger.info(f"ClayMultimodalWrapper initialized with strategy: {strategy}")
    
    def _init_projection_weights(self):
        """Initialize projection weights intelligently based on modality understanding."""
        with torch.no_grad():
            # Initialize projection matrix (9 -> 6)
            weight = self.channel_projection.weight  # Shape: (6, 9, 1, 1)
            
            # Strategy: Map multimodal channels to optical-like channels
            # Input channels: [DEM(0), B(1), G(2), R(3), NIR(4), SWIR1(5), SWIR2(6), Thermal(7), SAR(8)]
            # Output channels: [B_out, G_out, R_out, NIR_out, SWIR1_out, SWIR2_out]
            
            # Initialize as identity for optical channels where possible
            weight[0, 1] = 1.0  # B_out <- B
            weight[1, 2] = 1.0  # G_out <- G  
            weight[2, 3] = 1.0  # R_out <- R
            weight[3, 4] = 1.0  # NIR_out <- NIR
            weight[4, 5] = 1.0  # SWIR1_out <- SWIR1
            weight[5, 6] = 1.0  # SWIR2_out <- SWIR2
            
            # Add small contributions from other modalities for richer representation
            weight[0, 0] = 0.1  # DEM contributes to Blue (elevation affects water appearance)
            weight[1, 7] = 0.1  # Thermal contributes to Green
            weight[2, 8] = 0.1  # SAR contributes to Red (water detection)
            weight[3, 7] = 0.1  # Thermal contributes to NIR
            weight[4, 8] = 0.2  # SAR contributes to SWIR1 (strong for water detection)
            weight[5, 0] = 0.1  # DEM contributes to SWIR2
            
            # Initialize bias to zero
            self.channel_projection.bias.zero_()
            
            logger.info("Initialized learned projection weights for multimodal fusion")
    
    def _apply_channel_fusion(self, x):
        """
        Intelligent channel fusion strategy.
        Combines 9 channels into 6 by fusing complementary modalities.
        """
        # Input: (B, 9, H, W) - [DEM, B, G, R, NIR, SWIR1, SWIR2, Thermal, SAR]
        batch_size, channels, height, width = x.shape
        
        # Extract individual modalities
        dem = x[:, 0:1]        # (B, 1, H, W)
        optical = x[:, 1:7]    # (B, 6, H, W) - [B, G, R, NIR, SWIR1, SWIR2]
        thermal = x[:, 7:8]    # (B, 1, H, W)
        sar = x[:, 8:9]        # (B, 1, H, W)
        
        # Strategy: Enhance optical channels with information from other modalities
        enhanced_channels = []
        
        # Channel 0: Blue + DEM information (elevation affects water appearance)
        blue_enhanced = optical[:, 0:1] + 0.1 * dem
        enhanced_channels.append(blue_enhanced)
        
        # Channel 1: Green + thermal information (temperature affects vegetation)
        green_enhanced = optical[:, 1:2] + 0.1 * thermal
        enhanced_channels.append(green_enhanced)
        
        # Channel 2: Red + SAR information (SAR strong for water detection)
        red_enhanced = optical[:, 2:3] + 0.15 * sar
        enhanced_channels.append(red_enhanced)
        
        # Channel 3: NIR + thermal (both useful for vegetation and water)
        nir_enhanced = optical[:, 3:4] + 0.1 * thermal
        enhanced_channels.append(nir_enhanced)
        
        # Channel 4: SWIR1 + SAR (both excellent for water detection)
        swir1_enhanced = optical[:, 4:5] + 0.2 * sar
        enhanced_channels.append(swir1_enhanced)
        
        # Channel 5: SWIR2 + DEM (elevation context for SWIR)
        swir2_enhanced = optical[:, 5:6] + 0.1 * dem
        enhanced_channels.append(swir2_enhanced)
        
        # Combine enhanced channels
        fused = torch.cat(enhanced_channels, dim=1)  # (B, 6, H, W)
        
        return fused
    
    def _apply_optical_only(self, x):
        """Extract only the 6 optical channels (channels 1-6)."""
        return x[:, 1:7]  # (B, 6, H, W)
    
    def forward(self, x):
        """
        Forward pass with multimodal input adaptation.
        
        Args:
            x: Input tensor of shape (B, 9, H, W)
            
        Returns:
            Output from Clay model
        """
        if x.size(1) != 9:
            raise ValueError(f"Expected 9-channel input, got {x.size(1)} channels")
        
        # Apply channel adaptation strategy
        if self.strategy == 'optical_only':
            adapted_input = self._apply_optical_only(x)
        elif self.strategy == 'channel_fusion':
            adapted_input = self._apply_channel_fusion(x)
        elif self.strategy == 'learned_projection':
            adapted_input = self.channel_projection(x)
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")
        
        # Forward through Clay model
        output = self.clay_model(adapted_input)
        
        return output
    
    def get_strategy_info(self):
        """Return information about the current adaptation strategy."""
        info = {
            'strategy': self.strategy,
            'input_channels': 9,
            'adapted_channels': 6,
            'description': {
                'optical_only': 'Uses only 6 optical channels, ignoring DEM, thermal, and SAR',
                'channel_fusion': 'Intelligently fuses multimodal information into optical channels',
                'learned_projection': 'Learns optimal projection from 9 to 6 channels during training'
            }[self.strategy]
        }
        return info