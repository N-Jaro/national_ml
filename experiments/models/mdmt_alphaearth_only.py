import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict

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

    def forward(self, x, skip_connection):
        x = self.up(x)
        diffY = skip_connection.size()[2] - x.size()[2]
        diffX = skip_connection.size()[3] - x.size()[3]

        x = F.pad(x, [diffX // 2, diffX - diffX // 2,
                      diffY // 2, diffY - diffY // 2])

        x = torch.cat([skip_connection, x], dim=1)
        return self.conv(x)

class OutConv(nn.Module):
    def __init__(self, in_channels, out_channels):
        super(OutConv, self).__init__()
        self.conv = nn.Conv2d(in_channels, out_channels, kernel_size=1)

    def forward(self, x):
        return self.conv(x)

# --- AlphaEarth-only Encoder ---

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

# --- Task Decoder ---

class TaskDecoder(nn.Module):
    """
    A dedicated decoder for a single task using skip connections from AlphaEarth encoder.
    """
    def __init__(self, n_classes, base_channels=64, bilinear=True):
        super().__init__()
        self.up1 = Up(base_channels * 16 + base_channels * 8, base_channels * 8, bilinear)
        self.up2 = Up(base_channels * 8 + base_channels * 4, base_channels * 4, bilinear)
        self.up3 = Up(base_channels * 4 + base_channels * 2, base_channels * 2, bilinear)
        self.up4 = Up(base_channels * 2 + base_channels, base_channels, bilinear)
        self.outc = OutConv(base_channels, n_classes)

    def forward(self, features, skips: Dict[str, torch.Tensor]):
        s4 = skips['s4']
        s3 = skips['s3']
        s2 = skips['s2']
        s1 = skips['s1']

        x = self.up1(features, s4)
        x = self.up2(x, s3)
        x = self.up3(x, s2)
        x = self.up4(x, s1)
        logits = self.outc(x)
        return logits

# --- The Main AlphaEarth-only Model ---

class MultitaskModel_AlphaEarth_Only(nn.Module):
    """
    AlphaEarth-only model for multitask learning.
    Uses only Google's satellite embedding data (AlphaEarth) for both water segmentation and D8 flow direction tasks.
    
    AlphaEarth provides rich, pre-trained embeddings that capture complex Earth surface patterns
    from satellite imagery, potentially offering strong performance even without other modalities.
    """
    def __init__(self, n_classes_task1=1, n_classes_task2=1, base_channels=64, alphaearth_channels=64):
        super().__init__()
        
        # 1. AlphaEarth Encoder
        self.encoder_alphaearth = AlphaEarthEncoder(in_channels=alphaearth_channels, base_channels=base_channels)
        
        # 2. Shared processing layer for representation learning from embeddings
        encoder_out_channels = base_channels * 16
        self.shared_encoder = nn.Sequential(
            DoubleConv(encoder_out_channels, encoder_out_channels),
            # Additional layer to better process the rich embedding information
            DoubleConv(encoder_out_channels, encoder_out_channels),
        )
        
        # 3. Separate Decoders for each task
        self.decoder_task1 = TaskDecoder(n_classes=n_classes_task1, base_channels=base_channels)
        self.decoder_task2 = TaskDecoder(n_classes=n_classes_task2, base_channels=base_channels)

    def forward(self, alphaearth):
        # Pass AlphaEarth embeddings through the encoder
        alphaearth_features, alphaearth_skips = self.encoder_alphaearth(alphaearth)
        
        # Pass features through shared encoder to further process the embeddings
        shared_representation = self.shared_encoder(alphaearth_features)
        
        # Pass the shared representation and AlphaEarth skips to each decoder
        output1 = self.decoder_task1(shared_representation, alphaearth_skips)
        output2 = self.decoder_task2(shared_representation, alphaearth_skips)
        
        return output1, output2


if __name__ == '__main__':
    # --- Example Usage ---
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")

    model = MultitaskModel_AlphaEarth_Only(n_classes_task1=1, n_classes_task2=1, alphaearth_channels=64).to(device)

    batch_size = 2
    alphaearth = torch.randn(batch_size, 64, 224, 224).to(device)  # 64-band AlphaEarth embeddings

    print("\n--- Input Shapes ---")
    print(f"AlphaEarth Embeddings: {alphaearth.shape}")

    try:
        output_task1, output_task2 = model(alphaearth)

        print("\n--- Output Shapes ---")
        print(f"Task 1 Output (Water Segmentation): {output_task1.shape}")
        print(f"Task 2 Output (D8 Flow Direction): {output_task2.shape}")

        assert output_task1.shape == (batch_size, 1, 224, 224)
        assert output_task2.shape == (batch_size, 8, 224, 224)
        print("\nModel forward pass successful with AlphaEarth-only architecture!")

        # Count parameters
        total_params = sum(p.numel() for p in model.parameters())
        trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        print(f"\nModel Parameters:")
        print(f"Total: {total_params:,}")
        print(f"Trainable: {trainable_params:,}")
        print(f"Model size: {total_params * 4 / (1024 * 1024):.2f} MB")

        # Test with different AlphaEarth channel configurations
        print("\n--- Testing Different Channel Configurations ---")
        
        # Test with 32 channels (reduced embedding)
        model_32 = MultitaskModel_AlphaEarth_Only(n_classes_task1=1, n_classes_task2=1, alphaearth_channels=32).to(device)
        alphaearth_32 = torch.randn(batch_size, 32, 224, 224).to(device)
        out1_32, out2_32 = model_32(alphaearth_32)
        print(f"32-channel AlphaEarth: Input {alphaearth_32.shape} -> Outputs {out1_32.shape}, {out2_32.shape}")
        
        # Test with 128 channels (extended embedding)
        model_128 = MultitaskModel_AlphaEarth_Only(n_classes_task1=1, n_classes_task2=1, alphaearth_channels=128).to(device)
        alphaearth_128 = torch.randn(batch_size, 128, 224, 224).to(device)
        out1_128, out2_128 = model_128(alphaearth_128)
        print(f"128-channel AlphaEarth: Input {alphaearth_128.shape} -> Outputs {out1_128.shape}, {out2_128.shape}")

    except Exception as e:
        print(f"Error during forward pass: {e}")
        import traceback
        traceback.print_exc()