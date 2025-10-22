# data_module_segmentation_only_dem_alphaearth.py 
import os
import sys
import torch
import numpy as np
import pytorch_lightning as pl
from torch.utils.data import DataLoader, random_split

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data')))
from segmentation_only_dataloader_dem_alphaearth import SegmentationOnlyPatchDataset_DEM_AlphaEarth

class PatchDataModule_SegmentationOnly_DEM_AlphaEarth(pl.LightningDataModule):
    """
    Data module for segmentation-only DEM+AlphaEarth model following the exact MDMT pattern.
    
    Key differences from multitask:
    - Uses SegmentationOnlyPatchDataset (emphasizes hydro_mask as primary target)
    - Only computes water pos_weight (no D8 class weights)
    - Same HUC-level splitting logic as MDMT variants
    """
    
    def __init__(
        self,
        base_path,
        huc_list=None,              # For backward compatibility
        train_hucs=None,            # New: explicit train HUCs
        val_hucs=None,              # New: explicit val HUCs
        batch_size=4,
        num_workers=4,
        val_split=0.1,              # Only used if huc_list provided
        alphaearth_channels=64,
        estimate_weights_from=64,   # number of samples for quick stats
    ):
        super().__init__()
        self.base_path = base_path
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.alphaearth_channels = alphaearth_channels
        self.estimate_weights_from = estimate_weights_from
        
        # Handle both old and new HUC specification methods
        if train_hucs is not None and val_hucs is not None:
            # New explicit train/val HUC split
            self.train_hucs = train_hucs
            self.val_hucs = val_hucs
            self.use_explicit_split = True
        elif huc_list is not None:
            # Old method: split HUCs randomly
            self.huc_list = huc_list  
            self.val_split = val_split
            self.use_explicit_split = False
        else:
            raise ValueError("Must provide either (train_hucs, val_hucs) or huc_list")

        # to be set in setup()
        self.train_ds = None
        self.val_ds = None
        self.water_pos_weight = None

    def setup(self, stage=None):
        if self.use_explicit_split:
            # New approach: HUC-level train/val split
            self.train_ds = SegmentationOnlyPatchDataset_DEM_AlphaEarth(
                base_path=self.base_path,
                huc_codes=self.train_hucs,
                expected_alphaearth_channels=self.alphaearth_channels,
                debug=False
            )
            self.val_ds = SegmentationOnlyPatchDataset_DEM_AlphaEarth(
                base_path=self.base_path,
                huc_codes=self.val_hucs,
                expected_alphaearth_channels=self.alphaearth_channels,
                debug=False
            )
            n_train, n_val = len(self.train_ds), len(self.val_ds)
            print(f"HUC-level split: {n_train} train + {n_val} val patches")
            print(f"Train HUCs ({len(self.train_hucs)}): {self.train_hucs}")
            print(f"Val HUCs ({len(self.val_hucs)}): {self.val_hucs}")
        else:
            # Old approach: random patch-level split
            full = SegmentationOnlyPatchDataset_DEM_AlphaEarth(
                base_path=self.base_path,
                huc_codes=self.huc_list,
                expected_alphaearth_channels=self.alphaearth_channels,
                debug=False
            )
            n_total = len(full)
            n_val = int(self.val_split * n_total)
            n_train = n_total - n_val
            
            self.train_ds, self.val_ds = random_split(
                full, [n_train, n_val],
                generator=torch.Generator().manual_seed(42)
            )
            print(f"Random split: {n_train} train + {n_val} val patches from {len(self.huc_list)} HUCs")

        # Estimate water class balance for pos_weight (segmentation-only)
        self._estimate_class_weights()

    def _estimate_class_weights(self):
        """Estimate water positive weight for segmentation-only training"""
        print(f"Estimating water class balance from {self.estimate_weights_from} samples...")
        
        # Sample from training set
        n_samples = min(self.estimate_weights_from, len(self.train_ds))
        indices = torch.randperm(len(self.train_ds))[:n_samples]
        
        water_pos_count = 0
        water_total_count = 0
        
        for i in indices:
            sample = self.train_ds[i]
            hydro_mask = sample['hydro_mask']  # (H, W)
            
            water_pos_count += hydro_mask.sum().item()
            water_total_count += hydro_mask.numel()
        
        # Compute pos_weight for water segmentation
        water_neg_count = water_total_count - water_pos_count
        if water_pos_count > 0:
            self.water_pos_weight = water_neg_count / water_pos_count
        else:
            self.water_pos_weight = 1.0
            
        water_pos_frac = water_pos_count / water_total_count if water_total_count > 0 else 0.0
        
        print(f"Water class balance:")
        print(f"  Positive pixels: {water_pos_count}/{water_total_count} ({water_pos_frac:.3%})")
        print(f"  Computed pos_weight: {self.water_pos_weight:.3f}")

    def train_dataloader(self):
        return DataLoader(
            self.train_ds,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.num_workers,
            pin_memory=True,
            drop_last=True,
        )

    def val_dataloader(self):
        return DataLoader(
            self.val_ds,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True,
            drop_last=False,
        )