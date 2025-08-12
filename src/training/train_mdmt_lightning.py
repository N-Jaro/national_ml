# train_mdmt_lightning.py

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import torch
from torch import nn
import pytorch_lightning as pl

from models.mdmt_v1 import MultimodalMultitaskModel
from utils.losses import WaterSegmentationLoss, D8FlowDirectionLoss, DynamicLossWeighter

VALID_D8_CODES = torch.tensor([0, 1, 2, 4, 8, 16, 32, 64, 128], dtype=torch.int64)

def sanitize_d8(flow_dir: torch.Tensor) -> torch.Tensor:
    """flow_dir: (B,H,W) -> returns (B,1,H,W) with invalid codes mapped to 0."""
    device = flow_dir.device
    valid = VALID_D8_CODES.to(device)
    is_valid = (flow_dir[..., None] == valid).any(dim=-1)  # (B,H,W)
    flow_dir_sanitized = torch.where(is_valid, flow_dir, torch.zeros_like(flow_dir))
    return flow_dir_sanitized.unsqueeze(1)  # (B,1,H,W) for the custom loss

class MDMTLitModule(pl.LightningModule):
    def __init__(self, lr: float = 1e-3, base_channels: int = 64):
        super().__init__()
        self.save_hyperparameters()

        # Model: Task1 (water, 1 logit), Task2 (D8, 8 classes)
        self.model = MultimodalMultitaskModel(
            n_classes_task1=1,
            n_classes_task2=8,
            base_channels=base_channels
        )

        # Losses
        self.water_loss_fn = WaterSegmentationLoss()
        self.d8_loss_fn    = D8FlowDirectionLoss()
        self.dynamic_weighter = DynamicLossWeighter(num_tasks=2)

    def forward(self, m1, m2, m3, m4):
        return self.model(m1, m2, m3, m4)

    def _step(self, batch, stage: str):
        m1, m2, m3, m4 = batch["m1"], batch["m2"], batch["m3"], batch["m4"]
        B = m1.size(0)

        target_water = batch["hydro_mask"].float().unsqueeze(1)
        flow_dir_raw = batch["flow_dir"].long()
        target_d8 = sanitize_d8(flow_dir_raw)  # (B,1,H,W)

        out_water, out_d8 = self(m1, m2, m3, m4)

        loss_water = self.water_loss_fn(out_water, target_water)
        loss_d8    = self.d8_loss_fn(out_d8, target_d8)
        total_loss = self.dynamic_weighter(loss_water, loss_d8)

        is_train = (stage == "train")

        # STEP logs (only during training)
        if is_train:
            self.log(f"{stage}_loss_water_step", loss_water, on_step=True, on_epoch=False, prog_bar=True,  batch_size=B)
            self.log(f"{stage}_loss_d8_step",    loss_d8,    on_step=True, on_epoch=False, prog_bar=True,  batch_size=B)
            self.log(f"{stage}_loss_step",       total_loss, on_step=True, on_epoch=False, prog_bar=True,  batch_size=B)

        # EPOCH logs (both train and val)
        self.log(f"{stage}_loss_water", loss_water, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_loss_d8",    loss_d8,    on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_loss",       total_loss, on_step=False, on_epoch=True, prog_bar=True,  batch_size=B)

        # Learned uncertainties (logvars)
        s1, s2 = self.dynamic_weighter.log_vars[0], self.dynamic_weighter.log_vars[1]
        if is_train:
            self.log(f"{stage}_logvar_water_step", s1, on_step=True, on_epoch=False, prog_bar=False, batch_size=B)
            self.log(f"{stage}_logvar_d8_step",    s2, on_step=True, on_epoch=False, prog_bar=False, batch_size=B)
        self.log(f"{stage}_logvar_water", s1, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_logvar_d8",    s2, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)

        return total_loss

    def training_step(self, batch, batch_idx):
        return self._step(batch, "train")

    def validation_step(self, batch, batch_idx):
        self._step(batch, "val")

    def configure_optimizers(self):
        params = list(self.model.parameters()) + list(self.dynamic_weighter.parameters())
        optimizer = torch.optim.Adam(params, lr=self.hparams.lr)
        return optimizer
