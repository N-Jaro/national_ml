#!/usr/bin/env python3
"""
DOFA Multimodal Wrapper for 9-channel input processing.

This wrapper enables DOFA foundation model to process 9-channel multimodal input
(DEM + 6×optical + thermal + SAR) by creating a compatible interface.
"""

import logging
import torch
import torch.nn as nn
from timm import create_model

logger = logging.getLogger(__name__)


class DofaMultimodalWrapper(nn.Module):
    """
    Wrapper to enable DOFA foundation model to process 9-channel multimodal input.
    
    DOFA expects wavelengths parameter in forward pass, so this wrapper:
    1. Creates a DOFA model directly from timm/torchgeo
    2. Handles the wavelengths parameter automatically
    3. Provides channel mapping for our 9-channel input
    """
    
    def __init__(self, num_classes=1000, pretrained=True, strategy="channel_mapping"):
        """
        Initialize DOFA multimodal wrapper.
        
        Args:
            num_classes: Number of output classes
            pretrained: Whether to use pretrained weights
            strategy: Strategy for handling 9-channel input
        """
        super().__init__()
        
        self.num_classes = num_classes
        self.pretrained = pretrained
        self.strategy = strategy
        
        # Define wavelengths for our 9 channels (in micrometers)
        # These represent our DEM + 6×optical + thermal + SAR modalities
        self.wavelengths = [
            0.5,    # DEM (pseudo-wavelength)
            0.48,   # Blue (Band 2)
            0.56,   # Green (Band 3)  
            0.665,  # Red (Band 4)
            0.86,   # NIR (Band 5)
            1.61,   # SWIR1 (Band 6)
            2.2,    # SWIR2 (Band 7)
            10.9,   # Thermal TIR (Band 10)
            0.03    # SAR (pseudo-wavelength, much shorter)
        ]
        
        # Initialize the DOFA model
        self.dofa_model = self._create_dofa_model()
        
        logger.info(f"DOFA multimodal wrapper initialized with strategy: {strategy}")
        logger.info(f"Wavelengths: {self.wavelengths}")
    
    def _create_dofa_model(self):
        """Create DOFA model directly from torchgeo/timm."""
        try:
            # Try to import and create DOFA model directly
            from torchgeo.models import dofa_base_patch16_224
            
            # Create DOFA model with large feature dimensions for segmentation
            # We'll extract features before the final classification head
            model = dofa_base_patch16_224(
                weights=None,  # Don't use pretrained weights initially
                num_classes=0  # No classification head - we want features
            )
            
            logger.info("Successfully created DOFA model from torchgeo (feature extraction mode)")
            return model
            
        except ImportError:
            logger.warning("torchgeo not available, trying alternative approach")
            try:
                # Fallback: try timm if available
                model = create_model(
                    'dofa_base_patch16_224',
                    pretrained=self.pretrained,
                    num_classes=0  # No classification head
                )
                logger.info("Successfully created DOFA model from timm (feature extraction mode)")
                return model
            except Exception as e:
                logger.error(f"Failed to create DOFA model: {e}")
                raise RuntimeError(f"Could not create DOFA model: {e}")
    
    def forward(self, x):
        """
        Forward pass with 9-channel input.
        
        Args:
            x: Input tensor of shape (B, 9, H, W)
            
        Returns:
            Output spatial features tensor for segmentation (B, embed_dim, 14, 14)
        """
        batch_size, channels, height, width = x.shape
        
        if channels != 9:
            raise ValueError(f"Expected 9 channels, got {channels}")
        
        # DOFA expects wavelengths as a list of floats
        wavelengths = self.wavelengths
        
        try:
            # Manually extract patch-level features from DOFA
            # This replicates the forward_features method but stops before global pooling
            
            # 1. Patch embedding
            waves_tensor = torch.tensor(wavelengths, device=x.device).float()
            x_patches, _ = self.dofa_model.patch_embed(x, waves_tensor)  # (B, 196, 768)
            
            # 2. Add positional embeddings
            x_patches = x_patches + self.dofa_model.pos_embed[:, 1:, :]
            
            # 3. Add CLS token
            cls_token = self.dofa_model.cls_token + self.dofa_model.pos_embed[:, :1, :]
            cls_tokens = cls_token.expand(x_patches.shape[0], -1, -1)
            x_with_cls = torch.cat((cls_tokens, x_patches), dim=1)  # (B, 197, 768)
            
            # 4. Apply transformer blocks
            for block in self.dofa_model.blocks:
                x_with_cls = block(x_with_cls)
            
            # 5. Extract patch tokens (exclude CLS token) and reshape to spatial
            patch_tokens = x_with_cls[:, 1:, :]  # (B, 196, 768) - remove CLS token
            
            # Reshape to spatial format: (B, 196, 768) -> (B, 768, 14, 14)
            embed_dim = patch_tokens.shape[-1]  # 768
            patch_dim = int(patch_tokens.shape[1] ** 0.5)  # sqrt(196) = 14
            
            spatial_features = patch_tokens.transpose(1, 2).reshape(
                batch_size, embed_dim, patch_dim, patch_dim
            )
            
            return spatial_features
            
        except Exception as e:
            logger.error(f"DOFA manual forward pass failed: {e}")
            # Fallback to original forward_features if available
            try:
                features = self.dofa_model.forward_features(x, wavelengths)
                # If it returns global features, just return them
                return features
            except Exception as e2:
                logger.error(f"DOFA fallback forward pass also failed: {e2}")
                raise
    
    def get_classifier(self):
        """Get the classifier head of the model."""
        if hasattr(self.dofa_model, 'head'):
            return self.dofa_model.head
        elif hasattr(self.dofa_model, 'classifier'):
            return self.dofa_model.classifier
        else:
            return None
    
    def reset_classifier(self, num_classes):
        """Reset the classifier head for different number of classes."""
        self.num_classes = num_classes
        
        if hasattr(self.dofa_model, 'head'):
            in_features = self.dofa_model.head.in_features
            self.dofa_model.head = nn.Linear(in_features, num_classes)
        elif hasattr(self.dofa_model, 'classifier'):
            in_features = self.dofa_model.classifier.in_features
            self.dofa_model.classifier = nn.Linear(in_features, num_classes)
        else:
            logger.warning("Could not find classifier head to reset")


def test_dofa_wrapper():
    """Test the DOFA multimodal wrapper."""
    print("Testing DOFA Multimodal Wrapper...")
    
    try:
        # Create wrapper
        wrapper = DofaMultimodalWrapper(num_classes=1, pretrained=False)
        
        # Test forward pass
        batch_size = 2
        test_input = torch.randn(batch_size, 9, 224, 224)
        
        print(f"Input shape: {test_input.shape}")
        
        with torch.no_grad():
            output = wrapper(test_input)
            print(f"Output shape: {output.shape}")
        
        print("✅ DOFA wrapper test successful!")
        return True
        
    except Exception as e:
        print(f"❌ DOFA wrapper test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    

if __name__ == "__main__":
    test_dofa_wrapper()