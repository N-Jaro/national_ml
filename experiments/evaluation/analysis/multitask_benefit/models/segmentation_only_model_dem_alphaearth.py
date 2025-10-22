import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import List, Dict

# --- Building Blocks (U-Net components) ---

class DoubleConv(nn.Module):
    """(Convolution => [BatchNorm] => ReLU) * 2"""

    def __init__(self, in_channels, out_channels, mid_channels=None):
        super().__init__()
        if not mid_channels:
            mid_channels = out_channels
        self.double_conv = nn.Sequential(
            nn.Conv2d(in_channels, mid_channels, kernel_size=3, padding=1, bias=False),
            nn.BatchNorm2d(mid_channels),
            nn.ReLU(inplace=True),
            nn.Conv2d(mid_channels, out_channels, kernel_size=3, padding=1, bias=False),
            nn.BatchNorm2d(out_channels),
            nn.ReLU(inplace=True)
        )

    def forward(self, x):
        return self.double_conv(x)

class Down(nn.Module):
    """Downscaling with maxpool then double conv"""

    def __init__(self, in_channels, out_channels):
        super().__init__()
        self.maxpool_conv = nn.Sequential(
            nn.MaxPool2d(2),
            DoubleConv(in_channels, out_channels)
        )

    def forward(self, x):
        return self.maxpool_conv(x)

class Up(nn.Module):
    """Upscaling then double conv"""

    def __init__(self, in_channels, out_channels, bilinear=True):
        super().__init__()

        if bilinear:
            self.up = nn.Upsample(scale_factor=2, mode='bilinear', align_corners=True)
            self.conv = DoubleConv(in_channels, out_channels, in_channels // 2)
        else:
            self.up = nn.ConvTranspose2d(in_channels, in_channels // 2, kernel_size=2, stride=2)
            self.conv = DoubleConv(in_channels, out_channels)

    def forward(self, x1, x2):
        x1 = self.up(x1)
        diffY = x2.size()[2] - x1.size()[2]
        diffX = x2.size()[3] - x1.size()[3]

        x1 = F.pad(x1, [diffX // 2, diffX - diffX // 2,
                        diffY // 2, diffY - diffY // 2])
        x = torch.cat([x2, x1], dim=1)
        return self.conv(x)

class OutConv(nn.Module):
    def __init__(self, in_channels, out_channels):
        super(OutConv, self).__init__()
        self.conv = nn.Conv2d(in_channels, out_channels, kernel_size=1)

    def forward(self, x):
        return self.conv(x)

# --- Modality-Specific Encoders ---

class DEMEncoder(nn.Module):
    """DEM-specific encoder with topographic feature extraction"""
    
    def __init__(self, input_channels=1):
        super().__init__()
        self.input_channels = input_channels
        
        # Initial conv with smaller kernel for elevation gradients
        self.initial_conv = DoubleConv(input_channels, 64)
        
        # Downsampling path
        self.down1 = Down(64, 128)
        self.down2 = Down(128, 256)
        self.down3 = Down(256, 512)
        self.down4 = Down(512, 1024)
        
    def forward(self, x):
        # Store features at each level for skip connections
        x1 = self.initial_conv(x)  # 64 channels
        x2 = self.down1(x1)        # 128 channels  
        x3 = self.down2(x2)        # 256 channels
        x4 = self.down3(x3)        # 512 channels
        x5 = self.down4(x4)        # 1024 channels (bottleneck)
        
        return [x1, x2, x3, x4, x5]

class AlphaEarthEncoder(nn.Module):
    """AlphaEarth embedding encoder"""
    
    def __init__(self, input_channels=64):
        super().__init__()
        self.input_channels = input_channels
        
        # Initial conv to process AlphaEarth embeddings
        self.initial_conv = DoubleConv(input_channels, 64)
        
        # Downsampling path
        self.down1 = Down(64, 128)
        self.down2 = Down(128, 256) 
        self.down3 = Down(256, 512)
        self.down4 = Down(512, 1024)
        
    def forward(self, x):
        # Store features at each level
        x1 = self.initial_conv(x)  # 64 channels
        x2 = self.down1(x1)        # 128 channels
        x3 = self.down2(x2)        # 256 channels
        x4 = self.down3(x3)        # 512 channels  
        x5 = self.down4(x4)        # 1024 channels (bottleneck)
        
        return [x1, x2, x3, x4, x5]

# --- Feature Fusion Module ---

class CrossModalAttentionFusion(nn.Module):
    """Cross-modal attention fusion for DEM + AlphaEarth features"""
    
    def __init__(self, channels):
        super().__init__()
        self.channels = channels
        
        # Attention mechanism
        self.query_conv = nn.Conv2d(channels, channels // 8, 1)
        self.key_conv = nn.Conv2d(channels, channels // 8, 1)
        self.value_conv = nn.Conv2d(channels, channels, 1)
        self.gamma = nn.Parameter(torch.zeros(1))
        
        # Feature refinement
        self.fusion_conv = DoubleConv(channels * 2, channels)
        
    def forward(self, dem_features, alphaearth_features):
        batch_size, channels, height, width = dem_features.size()
        
        # Attention weights from DEM to AlphaEarth
        query = self.query_conv(dem_features).view(batch_size, -1, height * width).permute(0, 2, 1)
        key = self.key_conv(alphaearth_features).view(batch_size, -1, height * width)
        attention = torch.bmm(query, key)
        attention = F.softmax(attention, dim=-1)
        
        # Apply attention to AlphaEarth features
        value = self.value_conv(alphaearth_features).view(batch_size, -1, height * width)
        attended_features = torch.bmm(value, attention.permute(0, 2, 1))
        attended_features = attended_features.view(batch_size, channels, height, width)
        
        # Combine with original DEM features
        enhanced_alphaearth = self.gamma * attended_features + alphaearth_features
        
        # Fusion
        fused = torch.cat([dem_features, enhanced_alphaearth], dim=1)
        fused = self.fusion_conv(fused)
        
        return fused

# --- Segmentation-Only Decoder ---

class SegmentationDecoder(nn.Module):
    """U-Net decoder for water segmentation (single task)"""
    
    def __init__(self, n_classes=1):
        super().__init__()
        
        # Fusion modules for each level
        self.fusion_modules = nn.ModuleList([
            CrossModalAttentionFusion(64),   # Level 1
            CrossModalAttentionFusion(128),  # Level 2  
            CrossModalAttentionFusion(256),  # Level 3
            CrossModalAttentionFusion(512),  # Level 4
            CrossModalAttentionFusion(1024)  # Level 5 (bottleneck)
        ])
        
        # Upsampling path (accounting for skip connections)
        self.up1 = Up(1024 + 512, 512)  # bottleneck + skip
        self.up2 = Up(512 + 256, 256)   # up1 + skip
        self.up3 = Up(256 + 128, 128)   # up2 + skip
        self.up4 = Up(128 + 64, 64)     # up3 + skip
        
        # Output head for water segmentation
        self.outc = OutConv(64, n_classes)
        
    def forward(self, dem_features, alphaearth_features):
        # Fuse features at each level
        fused_features = []
        for i, fusion_module in enumerate(self.fusion_modules):
            fused = fusion_module(dem_features[i], alphaearth_features[i])
            fused_features.append(fused)
        
        # Decoder path (single task - water segmentation)
        x = self.up1(fused_features[4], fused_features[3])  # 1024 -> 512
        x = self.up2(x, fused_features[2])                   # 512 -> 256
        x = self.up3(x, fused_features[1])                   # 256 -> 128  
        x = self.up4(x, fused_features[0])                   # 128 -> 64
        
        # Water segmentation output
        water_seg = self.outc(x)
        
        return water_seg

# --- Main Segmentation-Only Model ---

class SegmentationOnlyModel_DEM_AlphaEarth(nn.Module):
    """
    Segmentation-Only Model: DEM + AlphaEarth -> Water Segmentation
    
    This is identical architecture to multitask version but trained ONLY on segmentation loss.
    Key difference: No flow direction head, only water segmentation prediction.
    """
    
    def __init__(self, n_classes=1, alphaearth_channels=64):
        super().__init__()
        
        self.n_classes = n_classes
        self.alphaearth_channels = alphaearth_channels
        
        # Modality-specific encoders
        self.dem_encoder = DEMEncoder(input_channels=1)
        self.alphaearth_encoder = AlphaEarthEncoder(input_channels=alphaearth_channels)
        
        # Single-task decoder (segmentation only)
        self.decoder = SegmentationDecoder(n_classes=n_classes)
        
    def forward(self, dem, alphaearth):
        """
        Args:
            dem: DEM tensor (B, 1, H, W)
            alphaearth: AlphaEarth embeddings (B, alphaearth_channels, H, W)
            
        Returns:
            water_seg: Water segmentation logits (B, n_classes, H, W)
        """
        # Extract hierarchical features from each modality
        dem_features = self.dem_encoder(dem)
        alphaearth_features = self.alphaearth_encoder(alphaearth)
        
        # Single task prediction (water segmentation only)
        water_seg = self.decoder(dem_features, alphaearth_features)
        
        return water_seg

# --- Example Usage ---
if __name__ == "__main__":
    # --- Example Usage ---
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")

    model = SegmentationOnlyModel_DEM_AlphaEarth(
        n_classes=1, 
        alphaearth_channels=64
    ).to(device)

    batch_size = 2
    dem = torch.randn(batch_size, 1, 224, 224).to(device)
    alphaearth = torch.randn(batch_size, 64, 224, 224).to(device)

    print("\n--- Input Shapes ---")
    print(f"DEM (Priority): {dem.shape}")
    print(f"AlphaEarth: {alphaearth.shape}")

    try:
        water_seg = model(dem, alphaearth)

        print("\n--- Output Shapes ---")
        print(f"Water Segmentation: {water_seg.shape}")

        assert water_seg.shape == (batch_size, 1, 224, 224)
        print("\nSegmentation-only model forward pass successful!")

    except Exception as e:
        print(f"\nAn error occurred during the forward pass: {e}")

    # Print model summary
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"\nTotal parameters: {total_params:,}")
    print(f"Trainable parameters: {trainable_params:,}")
    print(f"Model size: {total_params * 4 / (1024 * 1024):.2f} MB")

    # Test with different AlphaEarth channel configurations
    print("\n--- Testing Different Channel Configurations ---")
    
    # Test with 32 channels
    model_32 = SegmentationOnlyModel_DEM_AlphaEarth(
        n_classes=1, alphaearth_channels=32
    ).to(device)
    alphaearth_32 = torch.randn(batch_size, 32, 224, 224).to(device)
    out_32 = model_32(dem, alphaearth_32)
    print(f"32-channel AlphaEarth: Input {alphaearth_32.shape} -> Output {out_32.shape}")
    
    # Test with 128 channels
    model_128 = SegmentationOnlyModel_DEM_AlphaEarth(
        n_classes=1, alphaearth_channels=128
    ).to(device)
    alphaearth_128 = torch.randn(batch_size, 128, 224, 224).to(device)
    out_128 = model_128(dem, alphaearth_128)
    print(f"128-channel AlphaEarth: Input {alphaearth_128.shape} -> Output {out_128.shape}")
    
    print("\n✅ Segmentation-Only DEM+AlphaEarth model architecture validated!")
    print("📊 Ready for single-task training with segmentation loss only")