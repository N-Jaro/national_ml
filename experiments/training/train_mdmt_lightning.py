# train_mdmt_lightning.py
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import torch
from torch import nn
from torch.optim import AdamW, Adam # <-- Import AdamW
import pytorch_lightning as pl
from pytorch_lightning.loggers import WandbLogger
import wandb

from models.mdmt_v1 import MultimodalMultitaskModel
from models.mdmt_landsat_6b import MultimodalMultitaskModel_ls6b  # 09/22/2025
from utils.losses import WaterSegmentationLoss, D8FlowDirectionLoss, DynamicLossWeighter, CombinedFocalDiceLoss, DiceLoss, AdaptiveLoss, ClDiceLoss

VALID_D8_CODES = torch.tensor([1, 2, 4, 8, 16, 32, 64, 128], dtype=torch.int64)
IGNORE_INDEX = 255  # for CE

def make_d8_targets_and_mask(flow_dir_raw: torch.Tensor):
    """
    flow_dir_raw: (B,H,W) int64 of possibly noisy D8 codes.
    returns:
      target_idx: (B,H,W) long in [0..7] or IGNORE_INDEX where invalid
      valid_mask: (B,H,W) bool
    """
    device = flow_dir_raw.device
    valid = VALID_D8_CODES.to(device)
    valid_mask = (flow_dir_raw[..., None] == valid).any(dim=-1)  # (B,H,W)
    # map codes->0..7 using index in VALID_D8_CODES
    idx_map = {int(v.item()): i for i, v in enumerate(valid)}
    # Vectorized mapping:
    target_idx = torch.full_like(flow_dir_raw, IGNORE_INDEX, dtype=torch.long)
    for i, code in enumerate(valid):
        target_idx[flow_dir_raw == code] = i
    return target_idx, valid_mask

def dice_coefficient(y_pred_logits: torch.Tensor, y_true: torch.Tensor, eps=1e-6):
    # y_pred_logits: (B,1,H,W), y_true: (B,1,H,W) in {0,1}
    probs = torch.sigmoid(y_pred_logits)
    probs = probs.view(probs.size(0), -1)
    y_true = y_true.view(y_true.size(0), -1)
    inter = (probs * y_true).sum(dim=1)
    denom = probs.sum(dim=1) + y_true.sum(dim=1) + eps
    dice = (2 * inter + eps) / denom
    return dice.mean()

def iou_binary(y_pred_logits: torch.Tensor, y_true: torch.Tensor, eps=1e-6, thresh=0.5):
    # y_pred_logits: (B,1,H,W), y_true: (B,1,H,W)
    y_pred = (torch.sigmoid(y_pred_logits) >= thresh).float()
    y_pred = y_pred.view(y_pred.size(0), -1)
    y_true = y_true.view(y_true.size(0), -1)
    inter = (y_pred * y_true).sum(dim=1)
    union = y_pred.sum(dim=1) + y_true.sum(dim=1) - inter + eps
    iou = inter / union
    return iou.mean()

def accuracy_micro_macro(logits: torch.Tensor, target_idx: torch.Tensor, num_classes=8):
    """
    logits: (B,8,H,W)
    target_idx: (B,H,W) with [0..7] or IGNORE_INDEX
    returns: micro_acc, macro_acc
    """
    with torch.no_grad():
        pred = logits.argmax(dim=1)  # (B,H,W)
        mask = (target_idx != IGNORE_INDEX)
        total = mask.sum().item()
        if total == 0:
            return torch.tensor(0.0, device=logits.device), torch.tensor(0.0, device=logits.device)

        correct = ((pred == target_idx) & mask).sum().float()
        micro = correct / max(total, 1)

        # macro: average per-class accuracy
        cl_accs = []
        for c in range(num_classes):
            cmask = mask & (target_idx == c)
            denom = cmask.sum().item()
            if denom == 0:
                continue
            cl_accs.append(((pred == target_idx) & cmask).sum().float() / denom)
        if len(cl_accs) == 0:
            macro = torch.tensor(0.0, device=logits.device)
        else:
            macro = torch.stack(cl_accs).mean()
        return micro, macro

def _minmax(x: torch.Tensor, eps=1e-6):
    # per-image minmax to 0..1
    x = x.float()
    mn = x.amin(dim=(-2,-1), keepdim=True)
    mx = x.amax(dim=(-2,-1), keepdim=True)
    return (x - mn) / (mx - mn + eps)

def _to_uint8_rgb(img01: torch.Tensor) -> torch.Tensor:
    # img01: (C,H,W) in 0..1, C=1 or 3 -> returns (H,W,3) uint8
    if img01.size(0) == 1:
        img01 = img01.repeat(3,1,1)
    img01 = (img01.clamp(0,1) * 255.0).byte()
    return img01.permute(1,2,0).cpu()  # (H,W,3)

# fixed color palette for the 8 D8 classes (R,G,B)
_D8_PALETTE = torch.tensor([
    [230,  25,  75],  # 1
    [ 60, 180,  75],  # 2
    [255, 225,  25],  # 4
    [  0, 130, 200],  # 8
    [245, 130,  48],  # 16
    [145,  30, 180],  # 32
    [ 70, 240, 240],  # 64
    [240,  50, 230],  # 128
], dtype=torch.uint8)

def _colorize_d8(idx: torch.Tensor) -> torch.Tensor:
    """
    idx: (H,W) with class ids 0..7 (others will be clamped)
    returns: (H,W,3) uint8 color image
    """
    idx = idx.clamp(0, 7).cpu()
    rgb = _D8_PALETTE[idx]  # (H,W,3)
    return rgb

class MDMTLitModule(pl.LightningModule):
    def __init__(
        self,
        lr: float = 1e-3,
        optimizer: str = "AdamW", # <-- Add optimizer as a hyperparameter 09/22/2025
        base_channels: int = 64,
        # ---- weighting knobs (can be passed in from DataModule stats) ----
        water_pos_weight: float | None = None,
        d8_class_weights: list[float] | None = None,
        d8_label_smoothing: float = 0.05,
        # >>> NEW: simple per-task scaling <<<
        water_loss_scale: float = 1.0,
        d8_loss_scale: float = 0.5,        # <--- smaller = pay less attention to D8
        use_dynamic_weighter: bool = True, # keep uncertainty weighting by default
    ):
        super().__init__()
        self.save_hyperparameters()

        # #orginal Landsat with 3 channels
        # # Model
        # self.model = MultimodalMultitaskModel(
        #     n_classes_task1=1,
        #     n_classes_task2=8,
        #     base_channels=base_channels
        # )

        #09/22/2025 modified landsat to 6 channels (4 optical + 2 swir)
        # Model
        self.model = MultimodalMultitaskModel_ls6b(
            n_classes_task1=1,
            n_classes_task2=8,
            base_channels=base_channels
        )

        # Losses (instantiate with weights if provided)
        # Water: BCE pos_weight (for class imbalance) + Dice inside WaterSegmentationLoss
        # self.water_loss_fn = WaterSegmentationLoss()
        # if water_pos_weight is not None and water_pos_weight > 0:
        #     # Replace BCE with a weighted one
        #     self.water_loss_fn.bce_loss = nn.BCEWithLogitsLoss(
        #         pos_weight=torch.tensor([water_pos_weight], dtype=torch.float32)
        #     )
        
        # 8/18/2025 - use CombinedFocalDiceLoss for Task 1
        self.water_loss_fn = CombinedFocalDiceLoss()
        
        # 8/21/2025 - use AdaptiveLoss from https://doi.org/10.1109/LGRS.2018.2811754 for Task 1
        # self.water_loss_fn = AdaptiveLoss() # Not working well, try ClDiceLoss instead
        # self.water_loss_fn = ClDiceLoss()  # 8/21/2025
        
        # D8: weighted CE + label smoothing + ignore_index
        weight_tensor = None
        if d8_class_weights is not None:
            weight_tensor = torch.tensor(d8_class_weights, dtype=torch.float32)
        self.d8_loss_fn = nn.CrossEntropyLoss(
            weight=weight_tensor,
            label_smoothing=d8_label_smoothing,
            ignore_index=IGNORE_INDEX
        )
        
        # >>> NEW <<<
        self.water_loss_scale = float(water_loss_scale)
        self.d8_loss_scale = float(d8_loss_scale)
        self.use_dynamic_weighter = bool(use_dynamic_weighter)

        # Dynamic weighter
        self.dynamic_weighter = DynamicLossWeighter(num_tasks=2)

    def forward(self, m1, m2, m3, m4):
        return self.model(m1, m2, m3, m4)

    def _step(self, batch, stage: str):
        m1, m2, m3, m4 = batch["m1"], batch["m2"], batch["m3"], batch["m4"]
        B = m1.size(0)

        target_water = batch["hydro_mask"].float().unsqueeze(1)     # (B,1,H,W)
        flow_dir_raw = batch["flow_dir"].long()                     # (B,H,W)
        target_idx, valid_mask = make_d8_targets_and_mask(flow_dir_raw)  # (B,H,W), (B,H,W)

        # Forward
        out_water, out_d8 = self(m1, m2, m3, m4)                    # (B,1,H,W), (B,8,H,W)

        # --- raw task losses ---
        loss_water_raw = self.water_loss_fn(out_water, target_water)
        loss_d8_raw    = self.d8_loss_fn(out_d8, target_idx)

        # --- scaled task losses (this is where we bias toward Task 1) ---
        loss_water = self.water_loss_scale * loss_water_raw
        loss_d8    = self.d8_loss_scale    * loss_d8_raw

        # --- combine ---
        if self.use_dynamic_weighter:
            total_loss = self.dynamic_weighter(loss_water, loss_d8)
        else:
            total_loss = loss_water + loss_d8

        # Metrics
        dice = dice_coefficient(out_water, target_water)
        iou  = iou_binary(out_water, target_water)
        micro, macro = accuracy_micro_macro(out_d8, target_idx, num_classes=8)

        is_train = (stage == "train")
        # STEP logs (only train)
        if is_train:
            # step logs
            self.log(f"{stage}_loss_water_step", loss_water_raw, on_step=True,  on_epoch=False, prog_bar=True,  batch_size=B)
            self.log(f"{stage}_loss_d8_step",    loss_d8_raw,    on_step=True,  on_epoch=False, prog_bar=True,  batch_size=B)
            self.log(f"{stage}_loss_step",       total_loss,     on_step=True,  on_epoch=False, prog_bar=True,  batch_size=B)
            # also log scaled variants for clarity
            self.log(f"{stage}_loss_water_scaled_step", loss_water, on_step=True, on_epoch=False, prog_bar=False, batch_size=B)
            self.log(f"{stage}_loss_d8_scaled_step",    loss_d8,    on_step=True, on_epoch=False, prog_bar=False, batch_size=B)

        # epoch logs
        self.log(f"{stage}_loss_water", loss_water_raw, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_loss_d8",    loss_d8_raw,    on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_loss",       total_loss,     on_step=False, on_epoch=True, prog_bar=True,  batch_size=B)

        self.log(f"{stage}_dice", dice, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_iou",  iou,  on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_acc_micro", micro, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_acc_macro", macro, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)

        invalid_pct = ((~valid_mask).sum().float() / valid_mask.numel())
        self.log(f"{stage}_d8_invalid_pct", invalid_pct, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)

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
        # log panels only on first val batch to avoid spam
        if batch_idx == 0:
            self._log_val_visuals(batch)
        self._step(batch, "val")

    def configure_optimizers(self):
        params = list(self.model.parameters()) + list(self.dynamic_weighter.parameters())
        
        # --- NEW LOGIC HERE ---
        # 09/22/2025: allow choice of Adam or AdamW via hparams
        if self.hparams.optimizer == "AdamW":
            optimizer = AdamW(params, lr=self.hparams.lr)
        elif self.hparams.optimizer == "Adam":
            optimizer = Adam(params, lr=self.hparams.lr)
        else:
            raise ValueError(f"Unknown optimizer: {self.hparams.optimizer}")
            
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=self.trainer.max_epochs)
        return {"optimizer": optimizer, "lr_scheduler": scheduler}
    
    def _log_val_visuals(self, batch):
        """Log a few samples (first val batch) every 10 epochs to W&B."""
        if not isinstance(self.logger, WandbLogger):
            return
        if self.global_rank != 0:  # avoid duplicate logs under DDP
            return
        if (self.current_epoch % 10) != 0:
            return

        m1, m2, m3, m4 = batch["m1"], batch["m2"], batch["m3"], batch["m4"]
        target_water = batch["hydro_mask"].float().unsqueeze(1)  # (B,1,H,W)
        flow_dir_raw = batch["flow_dir"].long()                  # (B,H,W)
        target_idx, _ = make_d8_targets_and_mask(flow_dir_raw)   # (B,H,W)

        # forward for predictions
        with torch.no_grad():
            out_water, out_d8 = self(m1, m2, m3, m4)             # (B,1,H,W), (B,8,H,W)
            pred_water = (torch.sigmoid(out_water) >= 0.5).float()   # (B,1,H,W)
            pred_idx = out_d8.argmax(dim=1)                          # (B,H,W)

        B = m1.size(0)
        max_samples = min(B, 6)  # log up to 4 samples
        images = []

        for i in range(max_samples):
            # Inputs
            dem_rgb = _to_uint8_rgb(_minmax(m1[i]))          # (H,W,3)
            opt_rgb = _to_uint8_rgb(_minmax(m2[i]))          # (H,W,3)

            # Water GT / Pred as 3-ch grayscale
            water_gt_rgb  = _to_uint8_rgb(target_water[i])
            water_pr_rgb  = _to_uint8_rgb(pred_water[i])

            # D8 GT / Pred colorized
            d8_gt_rgb = _colorize_d8(target_idx[i])
            d8_pr_rgb = _colorize_d8(pred_idx[i])

            # Create a simple 2x3 panel: [Opt | DEM | Water GT] / [Water Pred | D8 GT | D8 Pred]
            top_row  = torch.cat([opt_rgb[:,:,:3], dem_rgb, water_gt_rgb], dim=1)      # concat width
            bot_row  = torch.cat([water_pr_rgb[:,:,:3], d8_gt_rgb, d8_pr_rgb], dim=1)
            panel    = torch.cat([top_row, bot_row], dim=0)                    # concat height

            images.append(wandb.Image(panel.numpy(),
                                    caption=f"epoch {self.current_epoch} | sample {i}"))

        self.logger.experiment.log({"val/panels": images}, commit=False)

