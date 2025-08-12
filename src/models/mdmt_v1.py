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
    MODIFIED: This version uses skip connections from a single priority encoder only.
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

class MultimodalMultitaskModel(nn.Module):
    def __init__(self, n_classes_task1=1, n_classes_task2=1, base_channels=64):
        super().__init__()
        
        # 1. Separate Encoders for each modality
        self.encoder1 = ModalityEncoder(in_channels=1, base_channels=base_channels) # Priority modality
        self.encoder2 = ModalityEncoder(in_channels=3, base_channels=base_channels)
        self.encoder3 = ModalityEncoder(in_channels=1, base_channels=base_channels)
        self.encoder4 = ModalityEncoder(in_channels=1, base_channels=base_channels)
        
        # 2. Hierarchical Attention Fusion
        encoder_out_channels = base_channels * 16
        self.hierarchical_fusion = HierarchicalAttentionFusion(
            primary_channels=encoder_out_channels,
            other_channels_list=[encoder_out_channels] * 3, # For modalities 2, 3, 4
            fused_channels=encoder_out_channels
        )
        
        # 3. Shared Shallow Encoder
        self.shared_encoder = nn.Sequential(
            DoubleConv(encoder_out_channels, encoder_out_channels),
        )
        
        # 4. Separate Decoders for each task (now with priority skips)
        self.decoder_task1 = TaskDecoder(n_classes=n_classes_task1, base_channels=base_channels)
        self.decoder_task2 = TaskDecoder(n_classes=n_classes_task2, base_channels=base_channels)

    def forward(self, m1, m2, m3, m4):
        # Pass each modality through its encoder
        f1, skips1 = self.encoder1(m1) # Priority modality feature and skips
        f2, _ = self.encoder2(m2)      # Skips from others are now ignored
        f3, _ = self.encoder3(m3)
        f4, _ = self.encoder4(m4)
        
        # Fuse the features hierarchically using the new fusion module
        # f1 is the primary feature, [f2, f3, f4] are the others
        fused_features = self.hierarchical_fusion(f1, [f2, f3, f4])
        
        # Pass fused features through the shared encoder
        shared_representation = self.shared_encoder(fused_features)
        
        # Pass the shared representation and ONLY the priority skips to each decoder
        output1 = self.decoder_task1(shared_representation, skips1)
        output2 = self.decoder_task2(shared_representation, skips1)
        
        return output1, output2


if __name__ == '__main__':
    # --- Example Usage ---
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")

    model = MultimodalMultitaskModel(n_classes_task1=1, n_classes_task2=1).to(device)

    batch_size = 2
    modality1 = torch.randn(batch_size, 1, 224, 224).to(device)
    modality2 = torch.randn(batch_size, 3, 224, 224).to(device)
    modality3 = torch.randn(batch_size, 1, 224, 224).to(device)
    modality4 = torch.randn(batch_size, 1, 224, 224).to(device)

    print("\n--- Input Shapes ---")
    print(f"Modality 1 (Priority): {modality1.shape}")
    print(f"Modality 2: {modality2.shape}")
    print(f"Modality 3: {modality3.shape}")
    print(f"Modality 4: {modality4.shape}")

    try:
        output_task1, output_task2 = model(modality1, modality2, modality3, modality4)

        print("\n--- Output Shapes ---")
        print(f"Task 1 Output: {output_task1.shape}")
        print(f"Task 2 Output: {output_task2.shape}")

        assert output_task1.shape == (batch_size, 1, 224, 224)
        assert output_task2.shape == (batch_size, 1, 224, 224)
        print("\nModel forward pass successful with new architecture!")

    except Exception as e:
        print(f"\nAn error occurred during the forward pass: {e}")

