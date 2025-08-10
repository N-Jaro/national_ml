import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import torch
import torch.nn as nn
import torch.optim as optim
from mdmt_v1 import MultimodalMultitaskModel
from utils.losses import WaterSegmentationLoss, D8FlowDirectionLoss, DynamicLossWeighter

# --- Hyperparameters ---
batch_size = 4
epochs = 3
lr = 1e-3
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# --- Model ---
model = MultimodalMultitaskModel(n_classes_task1=1, n_classes_task2=8).to(device)  # D8 task has 8 classes

# --- Losses ---
water_loss_fn = WaterSegmentationLoss().to(device)
d8_loss_fn = D8FlowDirectionLoss().to(device)
dynamic_weighter = DynamicLossWeighter(num_tasks=2).to(device)

# --- Optimizer (include dynamic_weighter parameters) ---
optimizer = optim.Adam(list(model.parameters()) + list(dynamic_weighter.parameters()), lr=lr)

# --- Dummy Data ---
modality1 = torch.randn(batch_size, 1, 224, 224).to(device)
modality2 = torch.randn(batch_size, 3, 224, 224).to(device)
modality3 = torch.randn(batch_size, 1, 224, 224).to(device)
modality4 = torch.randn(batch_size, 1, 224, 224).to(device)

# Dummy targets
target_water = torch.randint(0, 2, (batch_size, 1, 224, 224)).float().to(device)
d8_values = [1, 2, 4, 8, 16, 32, 64, 128]
target_d8 = torch.tensor([d8_values[i % 8] for i in range(batch_size * 224 * 224)], device=device).view(batch_size, 1, 224, 224)

# --- Training Loop ---
for epoch in range(epochs):
    model.train()
    optimizer.zero_grad()
    
    # Forward pass
    out_water, out_d8 = model(modality1, modality2, modality3, modality4)
    
    # Compute losses
    loss_water = water_loss_fn(out_water, target_water)
    loss_d8 = d8_loss_fn(out_d8, target_d8)
    total_loss = dynamic_weighter(loss_water, loss_d8)
    
    # Backward and optimize
    total_loss.backward()
    optimizer.step()
    
    print(f"Epoch [{epoch+1}/{epochs}] - Water Loss: {loss_water.item():.4f} | D8 Loss: {loss_d8.item():.4f} | Total Loss: {total_loss.item():.4f}")

print("Dummy training with custom losses complete.")