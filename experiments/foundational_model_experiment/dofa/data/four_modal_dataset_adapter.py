"""
Enhanced data adapter for 4-modal foundation model comparison.
Handles DEM + optical + thermal + SAR data for Prithvi vs MDMT comparison.
"""

import os
import random
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import logging

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader
import pytorch_lightning as pl

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FourModalPatchDatasetAdapter:
    """
    Adapter for national_ml patch dataset format to support 4-modal foundation model training.
    Handles DEM + optical (6-band) + thermal + SAR modalities.
    
    Features:
    - Multi-modal data loading with all 4 modalities
    - Patch-level train/validation splitting (matching MDMT experiments)
    - Per-HUC normalization support
    - TerraTorch compatibility
    - Quality control and validation
    """
    
    def __init__(self, base_path: str, huc_codes: List[str], config: dict):
        self.base_path = Path(base_path)
        self.huc_codes = huc_codes
        self.config = config
        
        # Expected modalities for 4-modal comparison
        self.modalities = ["dem", "optical", "thermal", "sar"]
        
        # Validate configuration
        assert set(config['data']['modalities']) == set(self.modalities), \
            f"Config modalities {config['data']['modalities']} don't match expected {self.modalities}"
        
        logger.info(f"Initializing 4-modal dataset adapter for {len(huc_codes)} HUCs")
        
        # Load and validate patch files
        self.patch_metadata = self._load_patch_metadata()
        self.train_patches, self.val_patches = self._split_patches()
        
        logger.info(f"Dataset ready: {len(self.train_patches)} train, {len(self.val_patches)} val patches")
    
    def _load_patch_metadata(self) -> pd.DataFrame:
        """Load comprehensive patch metadata for all HUCs and modalities."""
        metadata_list = []
        
        for huc in self.huc_codes:
            huc_path = self.base_path / huc
            if not huc_path.exists():
                logger.warning(f"HUC {huc} path not found: {huc_path}")
                continue
                
            # Look for npz files (patch format from your data loader)
            patch_files = list(huc_path.glob("patch_*.npz"))
            
            if not patch_files:
                logger.warning(f"No patch files found in {huc_path}")
                continue
            
            # Validate each patch file has all required modalities
            for patch_file in patch_files:
                try:
                    # Quick check if file contains all modalities
                    with np.load(patch_file) as npz:
                        available_keys = set(npz.keys())
                        required_keys = set(self.modalities + ["hydro_mask", "flow_dir"])
                        
                        if not required_keys.issubset(available_keys):
                            missing = required_keys - available_keys
                            logger.debug(f"Skipping {patch_file}: missing {missing}")
                            continue
                        
                        # Validate spatial dimensions match
                        shapes = {key: npz[key].shape[:2] for key in required_keys}
                        reference_shape = shapes["dem"]
                        
                        if not all(shape == reference_shape for shape in shapes.values()):
                            logger.debug(f"Skipping {patch_file}: shape mismatch")
                            continue
                    
                    # Extract patch ID from filename
                    patch_id = patch_file.stem  # e.g., "patch_001"
                    
                    metadata_list.append({
                        'huc': huc,
                        'patch_id': patch_id,
                        'patch_path': patch_file,
                        'file_size': patch_file.stat().st_size,
                        'spatial_shape': reference_shape
                    })
                    
                except Exception as e:
                    logger.debug(f"Error processing {patch_file}: {e}")
                    continue
        
        if not metadata_list:
            raise RuntimeError("No valid patch files found with all required modalities")
        
        df = pd.DataFrame(metadata_list)
        logger.info(f"Found {len(df)} valid 4-modal patches across {df['huc'].nunique()} HUCs")
        
        return df
    
    def _split_patches(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Apply patch-level train/validation split matching MDMT experiments."""
        
        # Get unique patch IDs (patches may span multiple HUCs)
        patch_ids = self.patch_metadata['patch_id'].unique().tolist()
        
        # Reproducible random split
        random.seed(self.config['data']['random_seed'])
        random.shuffle(patch_ids)
        
        # Apply split ratio
        split_ratio = self.config['data']['split_ratio']
        split_idx = int(len(patch_ids) * split_ratio)
        
        train_patch_ids = set(patch_ids[:split_idx])
        val_patch_ids = set(patch_ids[split_idx:])
        
        # Filter patches based on split
        train_patches = self.patch_metadata[
            self.patch_metadata['patch_id'].isin(train_patch_ids)
        ].reset_index(drop=True)
        
        val_patches = self.patch_metadata[
            self.patch_metadata['patch_id'].isin(val_patch_ids)
        ].reset_index(drop=True)
        
        logger.info(f"Split: {len(train_patches)} train, {len(val_patches)} val patches")
        logger.info(f"Train HUCs: {train_patches['huc'].nunique()}, Val HUCs: {val_patches['huc'].nunique()}")
        
        return train_patches, val_patches


class FourModalPatchDataset(Dataset):
    """
    PyTorch Dataset for 4-modal patch data (DEM + optical + thermal + SAR).
    Compatible with TerraTorch foundation models.
    """
    
    def __init__(self, 
                 patch_metadata: pd.DataFrame, 
                 config: dict,
                 is_training: bool = True):
        self.patch_metadata = patch_metadata
        self.config = config
        self.is_training = is_training
        
        # Channel configuration
        self.optical_channels = config['data'].get('optical_channels', 6)
        
        logger.info(f"{'Training' if is_training else 'Validation'} dataset: {len(patch_metadata)} patches")
    
    def __len__(self):
        return len(self.patch_metadata)
    
    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        row = self.patch_metadata.iloc[idx]
        patch_path = row['patch_path']
        huc = row['huc']
        
        try:
            with np.load(patch_path) as npz:
                # Load all modalities
                dem = npz["dem"].astype(np.float32)                    # (H,W)
                optical = npz["optical"].astype(np.float32)           # (H,W,C) or (H,W)
                thermal = npz["thermal"].astype(np.float32)           # (H,W)
                sar = npz["sar"].astype(np.float32)                   # (H,W)
                
                # Load targets
                hydro_mask = npz["hydro_mask"].astype(np.float32)     # (H,W)
                flow_dir = npz["flow_dir"].astype(np.int64)           # (H,W) - not used for foundation model
                
                H, W = dem.shape
                
                # Process optical data to ensure correct channel count
                if optical.ndim == 2:
                    # Single channel - replicate to match expected channels
                    optical = np.repeat(optical[..., None], self.optical_channels, axis=2)
                elif optical.ndim == 3:
                    c = optical.shape[2]
                    if c < self.optical_channels:
                        # Pad with zeros if insufficient channels
                        pad = np.zeros((H, W, self.optical_channels - c), dtype=np.float32)
                        optical = np.concatenate([optical, pad], axis=2)
                    elif c > self.optical_channels:
                        # Take first N channels if too many
                        optical = optical[:, :, :self.optical_channels]
                else:
                    raise ValueError(f"Unexpected optical dimensions: {optical.shape}")
                
                # Apply per-HUC normalization if available
                if self.config['data'].get('normalize_per_huc', True):
                    dem, optical, thermal, sar = self._apply_normalization(
                        dem, optical, thermal, sar, huc
                    )
                
                # Convert to tensor format expected by TerraTorch
                # Combine all modalities into single multi-channel image
                # Order: DEM (1ch) + Optical (6ch) + Thermal (1ch) + SAR (1ch) = 9 channels total
                
                # Ensure all are (H, W) except optical which is (H, W, C)
                dem_channel = dem[..., None]        # (H, W, 1)
                thermal_channel = thermal[..., None]  # (H, W, 1)  
                sar_channel = sar[..., None]        # (H, W, 1)
                
                # Combine all channels: (H, W, 9)
                multi_modal_image = np.concatenate([
                    dem_channel,      # Channel 0
                    optical,          # Channels 1-6
                    thermal_channel,  # Channel 7
                    sar_channel       # Channel 8
                ], axis=2)
                
                # Convert to (C, H, W) format for PyTorch
                multi_modal_image = np.transpose(multi_modal_image, (2, 0, 1))  # (9, H, W)
                
                # Prepare sample in TerraTorch format
                sample = {
                    'image': torch.from_numpy(multi_modal_image).float(),  # (9, H, W)
                    'mask': torch.from_numpy(hydro_mask).float(),          # (H, W)
                    'patch_id': row['patch_id'],
                    'huc': huc,
                    'patch_path': str(patch_path)
                }
                
                return sample
                
        except Exception as e:
            logger.error(f"Error loading patch {patch_path}: {e}")
            raise
    
    def _apply_normalization(self, dem, optical, thermal, sar, huc):
        """Apply per-HUC normalization if normalization stats are available."""
        # This would integrate with your existing normalization stats
        # For now, return as-is - can be enhanced with actual stats
        return dem, optical, thermal, sar


class FourModalDataModule(pl.LightningDataModule):
    """
    PyTorch Lightning DataModule for 4-modal foundation model training.
    """
    
    def __init__(self, config: dict):
        super().__init__()
        self.config = config
        self.adapter = None
        
    def setup(self, stage: Optional[str] = None):
        """Setup datasets for training/validation."""
        if stage == "fit" or stage is None:
            # Create dataset adapter
            self.adapter = FourModalPatchDatasetAdapter(
                base_path=self.config['data']['base_path'],
                huc_codes=self.config['data']['huc_codes'],
                config=self.config
            )
            
            # Create train and validation datasets
            self.train_dataset = FourModalPatchDataset(
                self.adapter.train_patches,
                self.config,
                is_training=True
            )
            
            self.val_dataset = FourModalPatchDataset(
                self.adapter.val_patches, 
                self.config,
                is_training=False
            )
    
    def train_dataloader(self):
        return DataLoader(
            self.train_dataset,
            batch_size=self.config['training']['batch_size'],
            shuffle=True,
            num_workers=4,
            pin_memory=True,
            persistent_workers=True,
            drop_last=True  # Ensure consistent batch sizes
        )
    
    def val_dataloader(self):
        return DataLoader(
            self.val_dataset,
            batch_size=self.config['training']['batch_size'],
            shuffle=False,
            num_workers=4,
            pin_memory=True,
            persistent_workers=True
        )
    
    def get_dataset_info(self) -> Dict[str, Any]:
        """Return information about the dataset for logging."""
        if self.adapter is None:
            return {
                'status': 'not_setup',
                'message': 'DataModule setup() has not been called yet'
            }
        
        return {
            'total_patches': len(self.adapter.patch_metadata),
            'train_patches': len(self.adapter.train_patches),
            'val_patches': len(self.adapter.val_patches),
            'num_hucs': len(self.adapter.huc_codes),
            'modalities': self.adapter.modalities,
            'input_channels': 9,  # 1 + 6 + 1 + 1
            'split_ratio': self.config['data']['split_ratio'],
            'batch_size': self.config['training']['batch_size']
        }


# Test function
def test_four_modal_dataset():
    """Test the 4-modal dataset loader."""
    
    # Test configuration
    test_config = {
        'data': {
            'base_path': "/u/nathanj/national_ml/data/processed/patch_dataset",
            'huc_codes': ["03030005"],  # Single HUC for testing
            'modalities': ["dem", "optical", "thermal", "sar"],
            'split_ratio': 0.9,
            'random_seed': 42,
            'optical_channels': 6,
            'normalize_per_huc': True
        },
        'training': {
            'batch_size': 2
        }
    }
    
    try:
        # Test data module
        data_module = FourModalDataModule(test_config)
        data_module.setup("fit")
        
        # Test data loading
        train_loader = data_module.train_dataloader()
        
        # Get a sample batch
        batch = next(iter(train_loader))
        
        print("✅ 4-Modal Dataset Test Results:")
        print(f"   Image shape: {batch['image'].shape}")  # Should be (batch_size, 9, H, W)
        print(f"   Mask shape: {batch['mask'].shape}")    # Should be (batch_size, H, W)
        print(f"   Image channels: {batch['image'].shape[1]} (should be 9)")
        print(f"   Batch size: {batch['image'].shape[0]}")
        print(f"   Spatial size: {batch['image'].shape[2:4]}")
        print(f"   HUCs in batch: {batch['huc']}")
        
        # Verify channel composition
        image_sample = batch['image'][0]  # First sample
        print(f"   Channel value ranges:")
        print(f"     DEM (ch 0): [{image_sample[0].min():.3f}, {image_sample[0].max():.3f}]")
        print(f"     Optical (ch 1-6): [{image_sample[1:7].min():.3f}, {image_sample[1:7].max():.3f}]")
        print(f"     Thermal (ch 7): [{image_sample[7].min():.3f}, {image_sample[7].max():.3f}]")
        print(f"     SAR (ch 8): [{image_sample[8].min():.3f}, {image_sample[8].max():.3f}]")
        
        dataset_info = data_module.get_dataset_info()
        print(f"   Dataset info: {dataset_info}")
        
        return True
        
    except Exception as e:
        print(f"❌ 4-Modal Dataset Test Failed: {e}")
        return False


if __name__ == "__main__":
    test_four_modal_dataset()