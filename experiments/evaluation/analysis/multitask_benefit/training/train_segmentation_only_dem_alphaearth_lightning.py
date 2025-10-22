# train_segmentation_only_dem_alphaearth_lightning.py
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import torch
from torch import nn
from torch.optim import AdamW, Adam
import pytorch_lightning as pl
from pytorch_lightning.loggers import WandbLogger
import wandb

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models')))
from segmentation_only_model_dem_alphaearth import SegmentationOnlyModel_DEM_AlphaEarth

# Import data loader
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data')))
from segmentation_only_dataloader_dem_alphaearth import SegmentationOnlyPatchDataset_DEM_AlphaEarth

# Import utilities from experiments/utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'utils')))
try:
    from losses import WaterSegmentationLoss, CombinedFocalDiceLoss, DiceLoss, ClDiceLoss
except ImportError:
    # Fallback to basic losses if utils not available
    print("Warning: Could not import custom losses, using basic BCE")
    WaterSegmentationLoss = None
    CombinedFocalDiceLoss = None
    DiceLoss = None
    ClDiceLoss = None

def dice_coefficient(y_pred_logits: torch.Tensor, y_true: torch.Tensor, eps=1e-6):
    """Calculate Dice coefficient for binary segmentation"""
    # y_pred_logits: (B,1,H,W), y_true: (B,H,W) in {0,1}
    probs = torch.sigmoid(y_pred_logits)
    
    # Handle shape differences
    if len(y_true.shape) == 3:  # (B,H,W)
        y_true = y_true.unsqueeze(1)  # (B,1,H,W)
    
    probs = probs.view(probs.size(0), -1)
    y_true = y_true.view(y_true.size(0), -1)
    inter = (probs * y_true).sum(dim=1)
    denom = probs.sum(dim=1) + y_true.sum(dim=1) + eps
    dice = (2 * inter + eps) / denom
    return dice.mean()

def iou_binary(y_pred_logits: torch.Tensor, y_true: torch.Tensor, eps=1e-6, thresh=0.5):
    """Calculate IoU for binary segmentation"""
    # y_pred_logits: (B,1,H,W), y_true: (B,H,W) in {0,1}
    y_pred = (torch.sigmoid(y_pred_logits) >= thresh).float()
    
    # Handle shape differences
    if len(y_true.shape) == 3:  # (B,H,W)
        y_true = y_true.unsqueeze(1)  # (B,1,H,W)
    
    y_pred = y_pred.view(y_pred.size(0), -1)
    y_true = y_true.view(y_true.size(0), -1)
    inter = (y_pred * y_true).sum(dim=1)
    union = y_pred.sum(dim=1) + y_true.sum(dim=1) - inter + eps
    iou = inter / union
    return iou.mean()

class MDMT_SegmentationOnly_DEM_AlphaEarth_LitModule(pl.LightningModule):
    """
    PyTorch Lightning module for segmentation-only DEM+AlphaEarth model.
    
    Following the exact same pattern as MDMT_DEM_AlphaEarth_LitModule but for single-task training.
    This trains ONLY on water segmentation task (no flow direction).
    """
    
    def __init__(
        self,
        lr: float = 1e-3,
        optimizer: str = "AdamW",
        alphaearth_channels: int = 64,
        water_pos_weight: float = None,
        **kwargs
    ):
        super().__init__()
        self.save_hyperparameters()
        
        # Model - same architecture as multitask but only one output
        self.model = SegmentationOnlyModel_DEM_AlphaEarth(
            n_classes=1,
            alphaearth_channels=alphaearth_channels
        )
        
        # Loss function - segmentation only using WaterSegmentationLoss if available
        if WaterSegmentationLoss is not None:
            self.water_loss = WaterSegmentationLoss(
                pos_weight=torch.tensor([water_pos_weight]) if water_pos_weight else None
            )
        else:
            pos_weight = torch.tensor([water_pos_weight]) if water_pos_weight else None
            self.water_loss = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
        
        print(f"✅ Segmentation-Only Model initialized:")
        print(f"   Architecture: DEM + AlphaEarth ({alphaearth_channels} channels)")
        print(f"   Task: Water segmentation ONLY (no flow direction)")
        print(f"   Loss: WaterSegmentationLoss" + (f" (pos_weight={water_pos_weight:.3f})" if water_pos_weight else ""))
        print(f"   Learning rate: {lr}")
        print(f"   Optimizer: {optimizer}")
    
    def forward(self, dem, alphaearth):
        """Forward pass - returns only segmentation logits"""
        return self.model(dem, alphaearth)
    
    def _step(self, batch, stage):
        """Unified step function following MDMT pattern"""
        # Inputs
        dem = batch['dem']          # (B, 1, H, W)
        alphaearth = batch['alphaearth']  # (B, C, H, W)
        
        # Target (SEGMENTATION ONLY)
        hydro_mask = batch['hydro_mask']  # (B, H, W) - ONLY target used
        # Note: flow_dir is present in batch but IGNORED during training
        
        B = dem.size(0)
        
        # Forward pass
        water_logits = self.forward(dem, alphaearth)  # (B, 1, H, W)
        
        # Prepare target - ensure (B, 1, H, W) format for loss
        target_water = hydro_mask.unsqueeze(1) if len(hydro_mask.shape) == 3 else hydro_mask  # (B, 1, H, W)
        
        # Loss computation (SINGLE TASK)
        loss_water = self.water_loss(water_logits, target_water)
        total_loss = loss_water  # Only water loss for segmentation-only
        
        # Metrics
        dice = dice_coefficient(water_logits, target_water)
        iou = iou_binary(water_logits, target_water)
        
        # Logging following MDMT pattern
        is_train = (stage == "train")
        
        # STEP logs (only train)
        if is_train:
            self.log(f"{stage}_loss_step", total_loss, on_step=True, on_epoch=False, prog_bar=True, batch_size=B)
            self.log(f"{stage}_loss_water_step", loss_water, on_step=True, on_epoch=False, prog_bar=True, batch_size=B)
        
        # EPOCH logs
        self.log(f"{stage}_loss", total_loss, on_step=False, on_epoch=True, prog_bar=True, batch_size=B)
        self.log(f"{stage}_loss_water", loss_water, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_dice", dice, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        self.log(f"{stage}_iou", iou, on_step=False, on_epoch=True, prog_bar=False, batch_size=B)
        
        return total_loss
    
    def training_step(self, batch, batch_idx):
        return self._step(batch, "train")
    
    def validation_step(self, batch, batch_idx):
        # log visuals only on first val batch to avoid spam (following MDMT pattern)
        if batch_idx == 0:
            self._log_val_visuals(batch)
        return self._step(batch, "val")
    
    def _log_val_visuals(self, batch):
        """Log validation visualizations (placeholder for now)"""
        # TODO: Add visualization logging similar to MDMT pattern
        pass
    
    def configure_optimizers(self):
        """Configure optimizer and scheduler following MDMT pattern"""
        if self.hparams.optimizer == "AdamW":
            optimizer = AdamW(self.parameters(), lr=self.hparams.lr)
        elif self.hparams.optimizer == "Adam":
            optimizer = Adam(self.parameters(), lr=self.hparams.lr)
        else:
            raise ValueError(f"Unknown optimizer: {self.hparams.optimizer}")
            
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=self.trainer.max_epochs)
        return {"optimizer": optimizer, "lr_scheduler": scheduler}

class SegmentationOnlyDEMAlphaEarthDataModule(pl.LightningDataModule):
    """
    PyTorch Lightning data module for segmentation-only training.
    """
    
    def __init__(
        self,
        train_data_path: str,
        val_data_path: str = None,
        train_huc_codes: list = None,
        val_huc_codes: list = None,
        batch_size: int = 8,
        num_workers: int = 4,
        alphaearth_channels: int = 64,
        **kwargs
    ):
        super().__init__()
        self.save_hyperparameters()
        
        self.train_data_path = train_data_path
        self.val_data_path = val_data_path or train_data_path
        self.train_huc_codes = train_huc_codes
        self.val_huc_codes = val_huc_codes
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.alphaearth_channels = alphaearth_channels
        
        print(f"✅ Segmentation-Only DataModule initialized:")
        print(f"   Train path: {train_data_path}")
        print(f"   Val path: {self.val_data_path}")
        print(f"   AlphaEarth channels: {alphaearth_channels}")
        print(f"   Batch size: {batch_size}")
        print(f"   Focus: SEGMENTATION-ONLY (hydro_mask target)")
    
    def setup(self, stage=None):
        if stage == "fit" or stage is None:
            # Training dataset
            self.train_dataset = SegmentationOnlyPatchDataset_DEM_AlphaEarth(
                base_path=self.train_data_path,
                huc_codes=self.train_huc_codes,
                expected_alphaearth_channels=self.alphaearth_channels,
                debug=False
            )
            
            # Validation dataset
            self.val_dataset = SegmentationOnlyPatchDataset_DEM_AlphaEarth(
                base_path=self.val_data_path,
                huc_codes=self.val_huc_codes,
                expected_alphaearth_channels=self.alphaearth_channels,
                debug=False
            )
            
            print(f"📊 Dataset sizes:")
            print(f"   Train: {len(self.train_dataset)} patches")
            print(f"   Val: {len(self.val_dataset)} patches")
    
    def train_dataloader(self):
        return torch.utils.data.DataLoader(
            self.train_dataset,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.num_workers,
            pin_memory=True,
            drop_last=True
        )
    
    def val_dataloader(self):
        return torch.utils.data.DataLoader(
            self.val_dataset,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True,
            drop_last=False
        )

# --- Example Usage ---
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Segmentation-Only DEM+AlphaEarth Lightning Module")
    parser.add_argument('--data-path', type=str, required=True, help='Path to patch dataset')
    parser.add_argument('--huc-codes', type=str, nargs='+', help='HUC codes for testing')
    parser.add_argument('--batch-size', type=int, default=4, help='Batch size')
    parser.add_argument('--alphaearth-channels', type=int, default=64, help='AlphaEarth channels')
    
    args = parser.parse_args()
    
    print("🔬 Testing Segmentation-Only Lightning Module")
    print("=" * 60)
    
    # Create data module
    data_module = SegmentationOnlyDEMAlphaEarthDataModule(
        train_data_path=args.data_path,
        train_huc_codes=args.huc_codes,
        val_huc_codes=args.huc_codes,
        batch_size=args.batch_size,
        alphaearth_channels=args.alphaearth_channels
    )
    
    # Create model
    model = SegmentationOnlyDEMAlphaEarthModule(
        alphaearth_channels=args.alphaearth_channels,
        learning_rate=1e-4,
        loss_type="combined_focal_dice"
    )
    
    # Setup data
    data_module.setup("fit")
    
    # Test training step
    train_loader = data_module.train_dataloader()
    batch = next(iter(train_loader))
    
    print(f"\n📊 Batch shapes:")
    print(f"   DEM: {batch['dem'].shape}")
    print(f"   AlphaEarth: {batch['alphaearth'].shape}")
    print(f"   Hydro Mask: {batch['hydro_mask'].shape} (TARGET)")
    print(f"   Flow Dir: {batch['flow_dir'].shape} (ignored)")
    
    # Test forward pass
    with torch.no_grad():
        water_logits = model(batch['dem'], batch['alphaearth'])
        print(f"   Output: {water_logits.shape}")
        
        # Test loss
        loss = model.loss_fn(water_logits, batch['hydro_mask'])
        print(f"   Loss: {loss.item():.4f}")
        
        # Test metrics
        dice = dice_coefficient(water_logits, batch['hydro_mask'])
        iou = iou_binary(water_logits, batch['hydro_mask'])
        print(f"   Dice: {dice.item():.4f}")
        print(f"   IoU: {iou.item():.4f}")
    
    print(f"\n✅ Segmentation-Only Lightning module test successful!")
    print(f"🎯 Ready for single-task training!")