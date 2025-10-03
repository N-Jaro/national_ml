# train_dem_only_lightning.py
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import torch
from torch import nn
from torch.optim import AdamW, Adam
import pytorch_lightning as pl
from pytorch_lightning.loggers import WandbLogger
import wandb

from models.mdmt_dem_only import MultitaskModel_DEM_Only
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
    union = y_pred.sum(dim=1) + y_true.sum(dim=1) - inter
    iou = (inter + eps) / (union + eps)
    return iou.mean()

def accuracy_micro_macro(y_pred: torch.Tensor, y_true: torch.Tensor, num_classes=8, ignore_index=255):
    # y_pred: (B,C,H,W), y_true: (B,H,W)
    pred_cls = y_pred.argmax(dim=1)  # (B,H,W)
    valid = (y_true != ignore_index)
    pred_valid = pred_cls[valid]
    true_valid = y_true[valid]
    
    # Micro accuracy
    micro_acc = (pred_valid == true_valid).float().mean()
    
    # Macro accuracy
    macro_acc_list = []
    for cls in range(num_classes):
        cls_mask = (true_valid == cls)
        if cls_mask.sum() > 0:
            cls_pred = pred_valid[cls_mask]
            cls_acc = (cls_pred == cls).float().mean()
            macro_acc_list.append(cls_acc)
    
    macro_acc = torch.tensor(macro_acc_list).mean() if macro_acc_list else torch.tensor(0.0)
    return micro_acc, macro_acc


class MDMT_DEM_Only_LitModule(pl.LightningModule):
    """
    PyTorch Lightning module for DEM-only multitask learning.
    Baseline model using only Digital Elevation Model data.
    """
    def __init__(
        self,
        base_channels: int = 64,
        learning_rate: float = 1e-3,
        optimizer_type: str = "AdamW",
        water_pos_weight: float = 1.0,
        d8_class_weights = None,
        d8_label_smoothing: float = 0.0,
        water_loss_scale: float = 1.0,
        d8_loss_scale: float = 0.5,
        use_dynamic_weighter: bool = True, # keep uncertainty weighting by default
        **kwargs
    ):
        super().__init__()
        self.save_hyperparameters()
        
        # Model
        self.model = MultitaskModel_DEM_Only(
            n_classes_task1=1,
            n_classes_task2=8,
            base_channels=base_channels
        )

        # Losses - use CombinedFocalDiceLoss for Task 1
        self.water_loss_fn = CombinedFocalDiceLoss()
        
        # D8: weighted CE + label smoothing + ignore_index
        weight_tensor = None
        if d8_class_weights is not None:
            if isinstance(d8_class_weights, torch.Tensor):
                weight_tensor = d8_class_weights.clone().detach()
            else:
                weight_tensor = torch.tensor(d8_class_weights, dtype=torch.float32)
        self.d8_loss_fn = nn.CrossEntropyLoss(
            weight=weight_tensor,
            label_smoothing=d8_label_smoothing,
            ignore_index=IGNORE_INDEX
        )
        
        # NEW
        self.water_loss_scale = float(water_loss_scale)
        self.d8_loss_scale = float(d8_loss_scale)
        self.use_dynamic_weighter = bool(use_dynamic_weighter)

        # Dynamic weighter
        self.dynamic_weighter = DynamicLossWeighter(num_tasks=2)
        
        self.learning_rate = learning_rate
        self.optimizer_type = optimizer_type

    def _step(self, batch, stage: str):
        dem = batch['dem']  # (B, 1, H, W)
        hydro_mask = batch['hydro_mask']  # water GT: (B, 1, H, W) float32 in {0,1}
        flow_dir = batch['flow_dir']     # D8 GT:    (B, H, W) int64

        # Forward pass - DEM only
        out_water, out_d8 = self.model(dem)

        # Task 1: Water segmentation loss (already preprocessed for binary classification)
        loss_water = self.water_loss_fn(out_water, hydro_mask)

        # Task 2: D8 flow direction loss (need to convert targets and mask)
        target_idx, valid_mask = make_d8_targets_and_mask(flow_dir)
        loss_d8_raw    = self.d8_loss_fn(out_d8, target_idx)
        
        # Apply mask: only compute loss where valid D8 codes exist
        valid_pixels = (target_idx != IGNORE_INDEX).sum()
        if valid_pixels > 0:
            loss_d8 = loss_d8_raw
        else:
            loss_d8 = torch.tensor(0.0, device=loss_water.device, requires_grad=True)

        # Total loss with dynamic weighting
        if self.use_dynamic_weighter:
            total_loss = self.dynamic_weighter(loss_water, loss_d8)
        else:
            total_loss = self.water_loss_scale * loss_water + self.d8_loss_scale * loss_d8

        # Compute metrics
        dice = dice_coefficient(out_water, hydro_mask)
        iou = iou_binary(out_water, hydro_mask)
        micro_acc, macro_acc = accuracy_micro_macro(out_d8, target_idx, num_classes=8, ignore_index=IGNORE_INDEX)

        # Log metrics
        self.log(f'{stage}_loss', total_loss, on_step=False, on_epoch=True, prog_bar=True)
        self.log(f'{stage}_loss_water', loss_water, on_step=False, on_epoch=True)
        self.log(f'{stage}_loss_d8', loss_d8, on_step=False, on_epoch=True)
        self.log(f'{stage}_dice', dice, on_step=False, on_epoch=True)
        self.log(f'{stage}_iou', iou, on_step=False, on_epoch=True)
        self.log(f'{stage}_d8_micro_acc', micro_acc, on_step=False, on_epoch=True)
        self.log(f'{stage}_d8_macro_acc', macro_acc, on_step=False, on_epoch=True)

        # Log dynamic weighter parameters if enabled
        if self.use_dynamic_weighter and stage == 'train':
            s1, s2 = self.dynamic_weighter.log_vars[0], self.dynamic_weighter.log_vars[1]
            self.log('train_weight_water', torch.exp(-s1), on_step=False, on_epoch=True)
            self.log('train_weight_d8', torch.exp(-s2), on_step=False, on_epoch=True)
            self.log('train_uncertainty_water', s1, on_step=False, on_epoch=True)
            self.log('train_uncertainty_d8', s2, on_step=False, on_epoch=True)

        return total_loss

    def training_step(self, batch, batch_idx):
        return self._step(batch, "train")

    def validation_step(self, batch, batch_idx):
        loss = self._step(batch, "val")
        
        # Log validation visualizations every few epochs
        if batch_idx == 0 and self.current_epoch % 5 == 0:
            self._log_val_visuals(batch)
        
        return loss

    def _log_val_visuals(self, batch):
        """Log validation visualizations to W&B."""
        if not isinstance(self.logger, WandbLogger):
            return
        
        # Get a few samples for visualization
        dem = batch['dem'][:3]  # (3, 1, H, W)
        hydro_mask = batch['hydro_mask'][:3]  # (3, 1, H, W)
        flow_dir = batch['flow_dir'][:3]  # (3, H, W)
        
        with torch.no_grad():
            out_water, out_d8 = self.model(dem)
            out_water = out_water[:3]
            out_d8 = out_d8[:3]
        
        # Convert to numpy for visualization
        dem_np = dem.cpu().numpy()
        hydro_np = hydro_mask.cpu().numpy()
        flow_dir_np = flow_dir.cpu().numpy()
        water_pred_np = torch.sigmoid(out_water).cpu().numpy()
        d8_pred_np = torch.softmax(out_d8, dim=1).argmax(dim=1).cpu().numpy()
        
        # Convert D8 predictions back to codes for visualization
        target_idx, _ = make_d8_targets_and_mask(flow_dir)
        target_idx_np = target_idx[:3].cpu().numpy()
        
        def _colorize_d8(d8_array):
            """Colorize D8 flow direction for visualization."""
            # Simple colorization - map each direction to a different color
            colors = torch.tensor([
                [0, 0, 0],      # 0: black (invalid)
                [255, 0, 0],    # 1: red (east)
                [255, 127, 0],  # 2: orange (southeast)  
                [255, 255, 0],  # 3: yellow (south)
                [127, 255, 0],  # 4: lime (southwest)
                [0, 255, 0],    # 5: green (west)
                [0, 255, 127],  # 6: teal (northwest)
                [0, 255, 255],  # 7: cyan (north)
                [0, 127, 255],  # 8: blue (northeast)
            ], dtype=torch.uint8)
            
            # Handle IGNORE_INDEX (255) by clamping to valid range
            d8_clamped = torch.clamp(d8_array.long(), 0, len(colors) - 1)
            colored = colors[d8_clamped]  # (H, W, 3)
            return colored.permute(2, 0, 1).float() / 255.0  # (3, H, W) in [0,1]

        images = []
        for i in range(3):
            # Normalize DEM for visualization
            dem_viz = dem_np[i, 0]
            dem_min, dem_max = dem_viz.min(), dem_viz.max()
            if dem_max > dem_min:
                dem_norm = (dem_viz - dem_min) / (dem_max - dem_min)
            else:
                dem_norm = dem_viz
            dem_rgb = torch.tensor(dem_norm)[None, :, :].repeat(3, 1, 1)  # (3, H, W)
            
            # Water GT and prediction
            water_gt_rgb = torch.tensor(hydro_np[i, 0])[None, :, :].repeat(3, 1, 1)
            water_pr_rgb = torch.tensor(water_pred_np[i, 0])[None, :, :].repeat(3, 1, 1)
            
            # D8 GT and prediction colorized
            d8_gt_rgb = _colorize_d8(torch.tensor(target_idx_np[i]))
            d8_pr_rgb = _colorize_d8(torch.tensor(d8_pred_np[i]))

            # Create a 2x3 panel: [DEM | Water GT | Water Pred] / [D8 GT | D8 Pred | Empty]
            top_row = torch.cat([dem_rgb, water_gt_rgb, water_pr_rgb], dim=2)      # concat width
            bot_row = torch.cat([d8_gt_rgb, d8_pr_rgb, torch.zeros_like(dem_rgb)], dim=2)
            panel = torch.cat([top_row, bot_row], dim=1)                           # concat height

            images.append(wandb.Image(panel.permute(1, 2, 0).numpy(),
                                    caption=f"epoch {self.current_epoch} | sample {i} | DEM-only"))

        self.logger.experiment.log({"val/panels": images}, commit=False)
    
    def configure_optimizers(self):
        """Configure optimizers and schedulers."""
        if self.use_dynamic_weighter:
            params = list(self.model.parameters()) + list(self.dynamic_weighter.parameters())
        else:
            params = self.model.parameters()
            
        if self.optimizer_type == 'AdamW':
            optimizer = AdamW(
                params,
                lr=self.learning_rate,
                weight_decay=1e-4
            )
        else:
            optimizer = Adam(
                params,
                lr=self.learning_rate
            )
        
        # Learning rate scheduler (cosine annealing)
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer,
            T_max=100,  # Will be adjusted based on actual training
            eta_min=1e-6
        )
        
        return {
            'optimizer': optimizer,
            'lr_scheduler': {
                'scheduler': scheduler,
                'monitor': 'val_loss'
            }
        }
    
    def get_model_info(self):
        """Get model parameter information."""
        total_params = sum(p.numel() for p in self.parameters())
        trainable_params = sum(p.numel() for p in self.parameters() if p.requires_grad)
        
        return {
            'total_params': total_params,
            'trainable_params': trainable_params,
            'model_size_mb': total_params * 4 / (1024 * 1024)  # Assuming float32
        }