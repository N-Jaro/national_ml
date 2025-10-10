#!/usr/bin/env python3
"""
PyTorch Lightning data module for DEM + Thermal model variant.
"""

import os
import sys
from typing import Optional, List
import pytorch_lightning as pl
import torch
from torch.utils.data import DataLoader, random_split
from sklearn.utils.class_weight import compute_class_weight
import numpy as np

# Add the experiments directory to path to import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from data.patchDataLoader_dem_thermal import MultimodalPatchDataset_DEM_Thermal


class MDMT_DEM_Thermal_DataModule(pl.LightningDataModule):
    """Lightning data module for DEM + Thermal multitask learning."""
    
    def __init__(
        self,
        base_path: str,
        huc_codes: List[str] = None,        # For backward compatibility
        train_hucs: List[str] = None,       # New: explicit train HUCs
        val_hucs: List[str] = None,         # New: explicit val HUCs
        batch_size: int = 32,
        num_workers: int = 4,
        val_split: float = 0.2,             # Only used if huc_codes provided
        **kwargs
    ):
        super().__init__()
        self.save_hyperparameters()
        
        self.base_path = base_path
        self.batch_size = batch_size
        self.num_workers = num_workers
        
        # Handle both old and new HUC specification methods
        if train_hucs is not None and val_hucs is not None:
            # New explicit train/val HUC split
            self.train_hucs = train_hucs
            self.val_hucs = val_hucs
            self.use_explicit_split = True
        elif huc_codes is not None:
            # Old method: split HUCs randomly
            self.huc_codes = huc_codes  
            self.val_split = val_split
            self.use_explicit_split = False
        else:
            raise ValueError("Must provide either (train_hucs, val_hucs) or huc_codes")
        
        # Initialize datasets
        self.train_dataset = None
        self.val_dataset = None
        self.class_weights = None
        
    def setup(self, stage: Optional[str] = None):
        """Setup datasets for training and validation."""
        
        if stage == "fit" or stage is None:
            # Create full dataset
            full_dataset = MultimodalPatchDataset_DEM_Thermal(
                base_path=self.base_path,
                huc_codes=self.huc_codes
            )
            
            print(f"Total dataset size: {len(full_dataset)}")
            
            # Calculate split sizes
            val_size = int(len(full_dataset) * self.val_split)
            train_size = len(full_dataset) - val_size
            
            # Random split
            self.train_dataset, self.val_dataset = random_split(
                full_dataset, 
                [train_size, val_size],
                generator=torch.Generator().manual_seed(42)
            )
            
            print(f"Train dataset size: {len(self.train_dataset)}")
            print(f"Validation dataset size: {len(self.val_dataset)}")
            
            # Compute class weights for both tasks
            self._compute_class_weights()
    
    def _compute_class_weights(self):
        """Compute class weights for balanced training."""
        print("Computing class weights...")
        
        # Sample from training dataset to get class distributions
        sample_size = min(1000, len(self.train_dataset))
        indices = np.random.choice(len(self.train_dataset), sample_size, replace=False)
        
        water_labels = []
        d8_labels = []
        
        for idx in indices:
            try:
                sample = self.train_dataset[idx]
                hydro_mask = sample['hydro_mask']  # DEM+Thermal uses 'hydro_mask' not 'water_mask'
                flow_dir = sample['flow_dir']      # DEM+Thermal uses 'flow_dir' not 'd8_flow'
                
                # Flatten and collect labels
                water_labels.extend(hydro_mask.flatten().numpy())
                d8_labels.extend(flow_dir.flatten().numpy())
                
            except Exception as e:
                print(f"Warning: Could not process sample {idx}: {e}")
                continue
        
        if water_labels and d8_labels:
            # Compute class weights for water segmentation (task 1)
            unique_water = np.unique(water_labels)
            water_weights = compute_class_weight(
                'balanced', 
                classes=unique_water, 
                y=water_labels
            )
            
            # Compute class weights for D8 flow direction (task 2)
            # Only compute weights for the 8 valid D8 codes, not all possible values
            VALID_D8_CODES = [1, 2, 4, 8, 16, 32, 64, 128]
            d8_counts = np.zeros(len(VALID_D8_CODES), dtype=np.float64)
            
            # Count occurrences of each valid D8 code
            for i, d8_code in enumerate(VALID_D8_CODES):
                d8_counts[i] = np.sum(np.array(d8_labels) == d8_code)
            
            # Compute class weights using inverse frequency normalized
            d8_counts_tensor = torch.tensor(d8_counts, dtype=torch.float32)
            inv = d8_counts_tensor.sum() / torch.clamp(d8_counts_tensor, min=1.0)
            inv = inv / inv.mean()
            d8_class_weights = inv.tolist()
            
            self.class_weights = {
                'water': dict(zip(unique_water, water_weights)),
                'd8': dict(zip(VALID_D8_CODES, d8_class_weights))
            }
            
            # Create attributes compatible with other data modules
            if len(unique_water) == 2:  # Binary classification
                # Convert to pos_weight for binary water segmentation
                neg_weight = self.class_weights['water'].get(0, 1.0)
                pos_weight = self.class_weights['water'].get(1, 1.0)
                self.water_pos_weight = torch.tensor(pos_weight / neg_weight, dtype=torch.float32)
            else:
                self.water_pos_weight = torch.tensor(1.0, dtype=torch.float32)
            
            # D8 class weights tensor for the 8 valid classes (0-7 after mapping)
            self.d8_class_weights = torch.tensor(d8_class_weights, dtype=torch.float32)
            
            print("Class weights computed:")
            print(f"Water pos_weight: {self.water_pos_weight.item():.4f}")
            print(f"D8 classes: {len(self.d8_class_weights)} classes")
            print(f"D8 weights: {self.d8_class_weights.tolist()}")
            print(f"[INFO] Water pos_weight = {self.water_pos_weight.item():.3f}")
            print(f"[INFO] D8 class weights = {self.d8_class_weights.tolist()}")
        else:
            print("Warning: Could not compute class weights")
            self.class_weights = None
            # Set default values
            self.water_pos_weight = torch.tensor(1.0, dtype=torch.float32)
            self.d8_class_weights = torch.ones(8, dtype=torch.float32)  # Default for 8 D8 classes
    
    def train_dataloader(self):
        """Create training dataloader."""
        return DataLoader(
            self.train_dataset,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.num_workers,
            pin_memory=True,
            persistent_workers=True if self.num_workers > 0 else False
        )
    
    def val_dataloader(self):
        """Create validation dataloader."""
        return DataLoader(
            self.val_dataset,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True,
            persistent_workers=True if self.num_workers > 0 else False
        )
    
    def get_class_weights(self):
        """Get computed class weights."""
        return self.class_weights