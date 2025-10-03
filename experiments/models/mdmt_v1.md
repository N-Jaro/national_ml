# Prioritized Multimodal Multitask Model

This repository contains a PyTorch implementation of a deep learning model designed for multitask learning from multimodal spatial data. The architecture is specifically structured to handle multiple input data sources (modalities) and produce multiple-image-like outputs, with one modality designated as having higher priority.

This model is ideal for problems in geospatial analysis, medical imaging, or robotics, where a primary, high-fidelity sensor (like LiDAR or a high-resolution DEM) should guide the interpretation of secondary data sources (like RGB imagery or lower-resolution sensor data).

## Model Architecture

The model employs a sophisticated encoder-decoder structure that integrates several key concepts:

1.  **Separate Modality Encoders**: Each of the four input modalities is fed into its own dedicated encoder. These encoders are based on a U-Net downsampling path, allowing the model to learn specialized, low-level features for each data type independently.

2.  **Prioritized Modality**: The first modality is treated as the primary source of information. Its features and spatial structure are given precedence throughout the model.

3.  **Hierarchical Attention Fusion**: Instead of simply combining all features, the model uses a guided fusion mechanism. The feature map from the primary modality's encoder is used as a "context" to generate attention masks for the other three modalities. This forces the model to learn which parts of the secondary data are most relevant *in the context of the primary data*, leading to a more intelligent and focused feature fusion.

4.  **Shared Encoder**: The resulting fused feature map, which now represents a rich synthesis of all inputs, is passed through a shallow shared encoder. This step refines the combined features, creating a robust, abstract representation that is beneficial for all downstream tasks.

5.  **Task-Specific Decoders**: The shared representation is fed into two separate decoders, one for each output task. This is a form of hard parameter sharing, where the bulk of the network (the "backbone") is shared between tasks, while the final layers are specialized.

6.  **Prioritized Skip Connections**: To ensure the final outputs retain the high-frequency spatial details of the most important input, the skip connections used by the decoders are taken *exclusively* from the primary modality's encoder.

### Data Flow Diagram

```
Inputs (m1, m2, m3, m4)
   |
   +--> Encoder 1 (Priority) -> [f1, skips1]
   |
   +--> Encoder 2 ----------> [f2]
   |
   +--> Encoder 3 ----------> [f3]
   |
   +--> Encoder 4 ----------> [f4]
         |
         |
         +--> HierarchicalAttentionFusion(f1, [f2, f3, f4]) -> fused_features
               |
               +--> Shared Encoder -> shared_representation
                     |
                     +--> Decoder 1(shared_representation, skips1) -> Output 1
                     |
                     +--> Decoder 2(shared_representation, skips1) -> Output 2
```

## How to Use

### 1. Model Initialization

First, instantiate the model. You can specify the number of output classes for each task and the base number of channels, which controls the model's capacity.

```python
import torch
from model_script import MultimodalMultitaskModel # Assuming the code is in model_script.py

# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# Initialize the model with 1 output channel for each task
model = MultimodalMultitaskModel(
    n_classes_task1=1,
    n_classes_task2=1,
    base_channels=64
).to(device)
```

### 2. Prepare Inputs

Create dummy tensors or load your data with the correct dimensions. The model expects four inputs.

```python
batch_size = 4

# Modality 1 (Priority): [B, 1, 224, 224]
modality1 = torch.randn(batch_size, 1, 224, 224).to(device)

# Modality 2 (e.g., RGB): [B, 6, 224, 224]
modality2 = torch.randn(batch_size, 6, 224, 224).to(device)

# Modality 3: [B, 1, 224, 224]
modality3 = torch.randn(batch_size, 1, 224, 224).to(device)

# Modality 4: [B, 1, 224, 224]
modality4 = torch.randn(batch_size, 1, 224, 224).to(device)
```

### 3. Forward Pass

Pass the inputs through the model to get the two outputs.

```python
# Get the model outputs
output1, output2 = model(modality1, modality2, modality3, modality4)

print(f"Shape of Task 1 Output: {output1.shape}")
print(f"Shape of Task 2 Output: {output2.shape}")
```

### 4. Training Loop

In a typical training loop, you would compute a loss for each task and combine them. The combined loss is then used for backpropagation.

```python
# Example loss functions and optimizer
loss_fn1 = torch.nn.MSELoss()
loss_fn2 = torch.nn.BCEWithLogitsLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=1e-4)

# Dummy targets
target1 = torch.randn(batch_size, 1, 224, 224).to(device)
target2 = torch.randint(0, 2, (batch_size, 8, 224, 224), dtype=torch.float32).to(device)

# --- Inside your training loop ---
optimizer.zero_grad()

# Forward pass
output1, output2 = model(modality1, modality2, modality3, modality4)

# Calculate losses
loss1 = loss_fn1(output1, target1)
loss2 = loss_fn2(output2, target2)

# Combine losses (e.g., simple sum or weighted sum)
total_loss = loss1 + loss2

# Backpropagation
total_loss.backward()
optimizer.step()

print(f"Total Loss: {total_loss.item()}")