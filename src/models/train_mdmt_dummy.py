import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader

from mdmt_v1 import MultimodalMultitaskModel
from utils.losses import WaterSegmentationLoss, D8FlowDirectionLoss, DynamicLossWeighter
# Make sure your dataset class file exports MultimodalPatchDataset
from data.patchDataLoader import MultimodalPatchDataset

# --- Hyperparameters ---
batch_size = 4
epochs = 3
lr = 1e-3
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# --- Model ---
# Task 1: water (binary logits), Task 2: D8 classes (8)
model = MultimodalMultitaskModel(n_classes_task1=1, n_classes_task2=8).to(device)

# --- Losses ---
water_loss_fn = WaterSegmentationLoss().to(device)   # expects (B,1,H,W) vs (B,1,H,W) or (B,H,W)
d8_loss_fn    = D8FlowDirectionLoss().to(device)     # expects (B,8,H,W) vs (B,H,W) class indices 0..7
dynamic_weighter = DynamicLossWeighter(num_tasks=2).to(device)

# --- Optimizer (include dynamic_weighter parameters) ---
optimizer = optim.Adam(list(model.parameters()) + list(dynamic_weighter.parameters()), lr=lr)

# --- Data Loader ---
base_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
huc_list = ["03030005"]

dataset = MultimodalPatchDataset(
    base_path=base_path,
    huc_codes=huc_list,
    optical_channels=3,
    dtype_inputs=torch.float32,
    normalize_dem=None
)

loader = DataLoader(
    dataset,
    batch_size=batch_size,
    shuffle=True,
    num_workers=4,
    pin_memory=(device.type == "cuda"),
    drop_last=False
)

print(f"Using {len(dataset)} valid patches")

# --- Valid D8 codes for sanitization ---
# Keep only {0,1,2,4,8,16,32,64,128}; map everything else -> 0
VALID_D8_CODES = torch.tensor([0, 1, 2, 4, 8, 16, 32, 64, 128], dtype=torch.int64)

def sanitize_d8(flow_dir: torch.Tensor) -> torch.Tensor:
    """
    flow_dir: (B,H,W) int64 with (possibly) noisy codes.
    Returns (B,1,H,W) int64 containing only valid codes (others set to 0).
    """
    device = flow_dir.device
    valid = VALID_D8_CODES.to(device)
    is_valid = (flow_dir[..., None] == valid).any(dim=-1)  # (B,H,W)
    flow_dir_sanitized = torch.where(is_valid, flow_dir, torch.zeros_like(flow_dir))
    return flow_dir_sanitized.unsqueeze(1)  # (B,1,H,W) for your loss (it squeezes dim 1)

# --- Training ---
model.train()
for epoch in range(1, epochs + 1):
    running_water, running_d8, running_total = 0.0, 0.0, 0.0

    for batch in loader:
        # Move inputs
        m1 = batch["m1"].to(device)  # dem:     (B,1,H,W)
        m2 = batch["m2"].to(device)  # optical: (B,3,H,W)
        m3 = batch["m3"].to(device)  # thermal: (B,1,H,W)
        m4 = batch["m4"].to(device)  # sar:     (B,1,H,W)

        # Targets
        target_water = batch["hydro_mask"].to(device).float().unsqueeze(1)  # (B,1,H,W)
        flow_dir_raw = batch["flow_dir"].to(device).long()                  # (B,H,W) raw codes
        target_d8 = sanitize_d8(flow_dir_raw)                               # (B,1,H,W) raw, cleaned

        optimizer.zero_grad(set_to_none=True)

        # Forward
        out_water, out_d8 = model(m1, m2, m3, m4)  # out_water (B,1,H,W), out_d8 (B,8,H,W)

        # Losses
        loss_water = water_loss_fn(out_water, target_water)
        loss_d8    = d8_loss_fn(out_d8, target_d8)   # your loss maps raw codes internally
        total_loss = dynamic_weighter(loss_water, loss_d8)

        # Backprop
        total_loss.backward()
        optimizer.step()

        # Logs
        running_water += loss_water.item()
        running_d8    += loss_d8.item()
        running_total += total_loss.item()

    n_batches = max(len(loader), 1)
    print(f"Epoch [{epoch}/{epochs}] "
          f"| Water: {running_water / n_batches:.4f} "
          f"| D8: {running_d8 / n_batches:.4f} "
          f"| Total: {running_total / n_batches:.4f}")

print("Training complete.")