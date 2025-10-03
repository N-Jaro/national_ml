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

# --- Core Components ---

class ModalityEncoder(nn.Module):
    """A dedicated encoder for a single modality with skip connections."""
    def __init__(self, in_channels, base_channels=64):
        super().__init__()
        self.inc = DoubleConv(in_channels, base_channels)
        self.down1 = Down(base_channels, base_channels * 2)
        self.down2 = Down(base_channels * 2, base_channels * 4)
        self.down3 = Down(base_channels * 4, base_channels * 8)
        self.down4 = Down(base_channels * 8, base_channels * 16)

    def forward(self, x):
        # Returns the final feature map and a dictionary of skip connections
        s1 = self.inc(x)
        s2 = self.down1(s1)
        s3 = self.down2(s2)
        s4 = self.down3(s3)
        s5 = self.down4(s4)
        
        skips = {'s1': s1, 's2': s2, 's3': s3, 's4': s4}
        return s5, skips

class AlphaEarthEncoder(nn.Module):
    """
    A U-Net encoder specifically designed for AlphaEarth embedding data.
    AlphaEarth provides 64-band embeddings from Google's satellite embedding model.
    Returns both features and skip connections for the decoder.
    """
    def __init__(self, in_channels=64, base_channels=64):
        super().__init__()
        # Additional input convolution to handle the high-dimensional AlphaEarth input
        self.input_reducer = nn.Sequential(
            nn.Conv2d(in_channels, base_channels, kernel_size=1, bias=False),
            nn.BatchNorm2d(base_channels),
            nn.ReLU(inplace=True)
        )
        
        self.inc = DoubleConv(base_channels, base_channels)
        self.down1 = Down(base_channels, base_channels * 2)
        self.down2 = Down(base_channels * 2, base_channels * 4)
        self.down3 = Down(base_channels * 4, base_channels * 8)
        self.down4 = Down(base_channels * 8, base_channels * 16)

    def forward(self, x):
        # Reduce the high-dimensional AlphaEarth input to manageable channels
        x = self.input_reducer(x)
        
        x1 = self.inc(x)
        x2 = self.down1(x1)
        x3 = self.down2(x2)
        x4 = self.down3(x3)
        x5 = self.down4(x4)
        
        # Return features and skip connections
        skips = {
            's1': x1,
            's2': x2,
            's3': x3,
            's4': x4
        }
        
        return x5, skips

class HierarchicalAttentionFusion(nn.Module):
    """
    Fuses features hierarchically. The primary modality's feature is used to
    generate attention maps for the other modalities, guiding the fusion process.
    """
    
    def __init__(self, primary_channels, other_channels_list, fused_channels):
        super().__init__()
        self.primary_channels = primary_channels
        
        # Create separate attention networks for each of the "other" modalities.
        # This allows learning specialized attention for each secondary source.
        self.attention_nets = nn.ModuleList()
        for other_ch in other_channels_list:
            # The context for attention is the concatenation of the primary and the other modality.
            context_channels = primary_channels + other_ch
            # The output of the attention net should have the same channel count as the "other" modality
            # so it can be applied as a multiplicative mask.
            self.attention_nets.append(
                nn.Sequential(
                    nn.Conv2d(context_channels, context_channels // 4, kernel_size=1),
                    nn.ReLU(inplace=True),
                    nn.Conv2d(context_channels // 4, other_ch, kernel_size=1),
                    nn.Sigmoid() # Sigmoid scales weights between 0 and 1
                )
            )
            
        # The final reducer takes the primary feature + all attended other features.
        total_attended_channels = primary_channels + sum(other_channels_list)
        self.channel_reducer = nn.Sequential(
            nn.Conv2d(total_attended_channels, fused_channels, kernel_size=1, bias=False),
            nn.BatchNorm2d(fused_channels),
            nn.ReLU(inplace=True)
        )

    def forward(self, primary_feature: torch.Tensor, other_features: List[torch.Tensor]):
        attended_others = []
        for i, other_feat in enumerate(other_features):
            # Create context by concatenating primary and current other feature
            context = torch.cat([primary_feature, other_feat], dim=1)
            
            # Generate attention weights for the current other feature
            attention_weights = self.attention_nets[i](context)
            
            # Apply attention by element-wise multiplication
            attended_feat = other_feat * attention_weights
            attended_others.append(attended_feat)
            
        # Create the final multi-context feature map.
        # This includes the original primary feature and the attended other features.
        final_concatenated = torch.cat([primary_feature] + attended_others, dim=1)
        
        # Reduce channels to create the final fused map
        fused_map = self.channel_reducer(final_concatenated)
        
        return fused_map

class TaskDecoder(nn.Module):
    """
    A dedicated decoder for a single task.
    Uses skip connections from a single priority encoder only.
    """
    def __init__(self, n_classes, base_channels=64, bilinear=True):
        super().__init__()
        # Channel counts are now based on a single set of skip connections, not four.
        self.up1 = Up(base_channels * 16 + base_channels * 8, base_channels * 8, bilinear)
        self.up2 = Up(base_channels * 8 + base_channels * 4, base_channels * 4, bilinear)
        self.up3 = Up(base_channels * 4 + base_channels * 2, base_channels * 2, bilinear)
        self.up4 = Up(base_channels * 2 + base_channels, base_channels, bilinear)
        self.outc = OutConv(base_channels, n_classes)

    def forward(self, shared_features, priority_skips: Dict[str, torch.Tensor]):
        # Use skip connections only from the priority encoder
        s4 = priority_skips['s4']
        s3 = priority_skips['s3']
        s2 = priority_skips['s2']
        s1 = priority_skips['s1']

        x = self.up1(shared_features, s4)
        x = self.up2(x, s3)
        x = self.up3(x, s2)
        x = self.up4(x, s1)
        logits = self.outc(x)
        return logits

# --- The Main Model ---

class MultimodalMultitaskModel_DEM_AlphaEarth(nn.Module):
    """
    DEM + AlphaEarth model for multitask learning.
    
    Combines:
    - DEM (Digital Elevation Model) for topographic information - priority modality
    - AlphaEarth (Google's satellite embeddings) for rich Earth surface patterns
    
    This fusion provides both explicit topographic information from DEM and
    rich contextual Earth surface patterns from AlphaEarth's foundation model embeddings.
    """
    def __init__(self, n_classes_task1=1, n_classes_task2=1, base_channels=64, alphaearth_channels=64):
        super().__init__()
        
        # 1. Separate Encoders for DEM and AlphaEarth modalities
        self.encoder_dem = ModalityEncoder(in_channels=1, base_channels=base_channels) # DEM (Priority modality)
        self.encoder_alphaearth = AlphaEarthEncoder(in_channels=alphaearth_channels, base_channels=base_channels) # AlphaEarth
        
        # 2. Hierarchical Attention Fusion (DEM as primary, AlphaEarth as secondary)
        encoder_out_channels = base_channels * 16
        self.hierarchical_fusion = HierarchicalAttentionFusion(
            primary_channels=encoder_out_channels,
            other_channels_list=[encoder_out_channels], # Only AlphaEarth as secondary modality
            fused_channels=encoder_out_channels
        )
        
        # 3. Shared Shallow Encoder
        self.shared_encoder = nn.Sequential(
            DoubleConv(encoder_out_channels, encoder_out_channels),
            # Additional processing for the rich fusion of topography and embeddings
            DoubleConv(encoder_out_channels, encoder_out_channels),
        )
        
        # 4. Separate Decoders for each task (using DEM skips as priority)
        self.decoder_task1 = TaskDecoder(n_classes=n_classes_task1, base_channels=base_channels)
        self.decoder_task2 = TaskDecoder(n_classes=n_classes_task2, base_channels=base_channels)

    def forward(self, dem, alphaearth):
        # Pass each modality through its encoder
        f_dem, dem_skips = self.encoder_dem(dem)               # DEM as priority modality (features and skips)
        f_alphaearth, _ = self.encoder_alphaearth(alphaearth)  # AlphaEarth features (skips ignored)
        
        # Fuse the features hierarchically using the fusion module
        # f_dem is the primary feature, [f_alphaearth] is the secondary
        fused_features = self.hierarchical_fusion(f_dem, [f_alphaearth])
        
        # Pass fused features through the shared encoder
        shared_representation = self.shared_encoder(fused_features)
        
        # Pass the shared representation and DEM skips (priority) to each decoder
        output1 = self.decoder_task1(shared_representation, dem_skips)
        output2 = self.decoder_task2(shared_representation, dem_skips)
        
        return output1, output2


if __name__ == '__main__':
    # --- Example Usage ---
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")

    model = MultimodalMultitaskModel_DEM_AlphaEarth(
        n_classes_task1=1, 
        n_classes_task2=8, 
        alphaearth_channels=64
    ).to(device)

    batch_size = 2
    dem = torch.randn(batch_size, 1, 224, 224).to(device)
    alphaearth = torch.randn(batch_size, 64, 224, 224).to(device)  # 64-channel AlphaEarth embeddings

    print("\n--- Input Shapes ---")
    print(f"DEM (Priority): {dem.shape}")
    print(f"AlphaEarth: {alphaearth.shape}")

    try:
        output_task1, output_task2 = model(dem, alphaearth)

        print("\n--- Output Shapes ---")
        print(f"Task 1 Output (Water Segmentation): {output_task1.shape}")
        print(f"Task 2 Output (D8 Flow Direction): {output_task2.shape}")

        assert output_task1.shape == (batch_size, 1, 224, 224)
        assert output_task2.shape == (batch_size, 8, 224, 224)
        print("\nModel forward pass successful with DEM + AlphaEarth architecture!")

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
    
    # Test with 32 channels (reduced embedding)
    model_32 = MultimodalMultitaskModel_DEM_AlphaEarth(
        n_classes_task1=1, n_classes_task2=8, alphaearth_channels=32
    ).to(device)
    alphaearth_32 = torch.randn(batch_size, 32, 224, 224).to(device)
    out1_32, out2_32 = model_32(dem, alphaearth_32)
    print(f"32-channel AlphaEarth: Input {alphaearth_32.shape} -> Outputs {out1_32.shape}, {out2_32.shape}")
    
    # Test with 128 channels (extended embedding)
    model_128 = MultimodalMultitaskModel_DEM_AlphaEarth(
        n_classes_task1=1, n_classes_task2=8, alphaearth_channels=128
    ).to(device)
    alphaearth_128 = torch.randn(batch_size, 128, 224, 224).to(device)
    out1_128, out2_128 = model_128(dem, alphaearth_128)
    print(f"128-channel AlphaEarth: Input {alphaearth_128.shape} -> Outputs {out1_128.shape}, {out2_128.shape}")

    print("\nDEM + AlphaEarth model combines topographic precision with foundation model embeddings!")