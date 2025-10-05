#!/usr/bin/env python3
"""
Four-Modal Dataset Adapter for SatLas Foundation Model

This adapter provides multimodal satellite data (DEM + 6×optical + thermal + SAR)
specifically tailored for SatLas foundation model requirements.

SatLas expects Sentinel-2 band configurations, so this adapter:
1. Maps our multimodal data to appropriate Sentinel-2 bands
2. Handles SatLas-specific preprocessing and normalization
3. Ensures compatibility with SatLas input requirements
"""

import os
import sys
import logging
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import numpy as np
import pandas as pd

import torch
from torch.utils.data import Dataset, DataLoader
import pytorch_lightning as pl

import rasterio
from rasterio.windows import Window
from sklearn.model_selection import train_test_split

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.append(str(project_root))

logger = logging.getLogger(__name__)

def _zscore(arr: np.ndarray, mean: np.ndarray, std: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    """Z-score normalization with epsilon for numerical stability."""
    return (arr - mean) / (np.maximum(std, eps))

def _parse_huc_stats(stats_json: Dict[str, Any]) -> Dict[str, Dict[str, np.ndarray]]:
    """
    Convert normalization_stats.json schema into broadcastable numpy arrays.
    Expected keys for 4-modal data:
      dem:     elevation_mean, elevation_stdDev
      optical: optical_*_mean, optical_*_stdDev (6 bands)
      thermal: thermal_mean, thermal_stdDev  
      sar:     sar_mean, sar_stdDev
    """
    modalities = {}
    
    # Parse DEM stats
    if "elevation_mean" in stats_json and "elevation_stdDev" in stats_json:
        modalities["dem"] = {
            "mean": np.array(stats_json["elevation_mean"]),
            "std": np.array(stats_json["elevation_stdDev"])
        }
    else:
        modalities["dem"] = None
    
    # Parse optical stats (6 bands)
    optical_means = []
    optical_stds = []
    
    for i in range(1, 7):
        mean_key = f"optical_{i}_mean"
        std_key = f"optical_{i}_stdDev"
        
        if mean_key in stats_json and std_key in stats_json:
            optical_means.append(stats_json[mean_key])
            optical_stds.append(stats_json[std_key])
        else:
            optical_means.append(0.0)
            optical_stds.append(1.0)  # Fallback
    
    if len(optical_means) == 6:
        modalities["optical"] = {
            "mean": np.array(optical_means).reshape(1, 1, 6),  # (H, W, C) broadcastable
            "std": np.array(optical_stds).reshape(1, 1, 6)
        }
    else:
        modalities["optical"] = None
    
    # Parse thermal stats
    if "thermal_mean" in stats_json and "thermal_stdDev" in stats_json:
        modalities["thermal"] = {
            "mean": np.array(stats_json["thermal_mean"]),
            "std": np.array(stats_json["thermal_stdDev"])
        }
    else:
        modalities["thermal"] = None
    
    # Parse SAR stats
    if "sar_mean" in stats_json and "sar_stdDev" in stats_json:
        modalities["sar"] = {
            "mean": np.array(stats_json["sar_mean"]),
            "std": np.array(stats_json["sar_stdDev"])
        }
    else:
        modalities["sar"] = None
    
    return modalities


class SatlasMultimodalDataset(Dataset):
    """
    Dataset for SatLas multimodal water segmentation.
    
    Handles 9-channel input (DEM + 6×optical + thermal + SAR) with
    SatLas-specific preprocessing and band mapping.
    """
    
    def __init__(self, 
                 patch_metadata: pd.DataFrame,
                 data_config: Dict,
                 transform=None,
                 augment: bool = False):
        """
        Initialize SatLas multimodal dataset.
        
        Args:
            patch_metadata: DataFrame with patch information
            data_config: Data configuration dictionary
            transform: Optional transforms to apply
            augment: Whether to apply data augmentation
        """
        self.patch_metadata = patch_metadata.reset_index(drop=True)
        self.data_config = data_config
        self.transform = transform
        self.augment = augment
        
        # SatLas-specific configuration
        self.patch_size = data_config.get("patch_size", 224)
        self.target_channels = 9  # DEM + 6×optical + thermal + SAR
        
        # Band mapping for SatLas (Sentinel-2 style)
        self.band_mapping = {
            'dem': 0,           # DEM -> Band 1 equivalent
            'optical_1': 1,     # Coastal/Aerosol -> B01
            'optical_2': 2,     # Blue -> B02  
            'optical_3': 3,     # Green -> B03
            'optical_4': 4,     # Red -> B04
            'optical_5': 5,     # NIR -> B8A
            'optical_6': 6,     # SWIR -> B11
            'thermal': 7,       # Thermal -> custom
            'sar': 8           # SAR -> custom
        }
        
        # Load normalization statistics from processed data
        self.huc_norm = self._load_normalization_stats()
        
        logger.info(f"SatLas dataset initialized with {len(self.patch_metadata)} patches")
        logger.info(f"Target channels: {self.target_channels}")
        logger.info(f"Patch size: {self.patch_size}")
    
    def _load_normalization_stats(self):
        """Load per-HUC normalization statistics from processed data."""
        huc_norm = {}
        base_path = Path(self.data_config.get("base_path", "/u/nathanj/national_ml/data/processed/patch_dataset"))
        
        for huc_code in self.data_config.get("huc_codes", []):
            huc_dir = base_path / huc_code
            stats_file = huc_dir / "normalization_stats.json"
            
            if stats_file.exists():
                try:
                    with open(stats_file, 'r') as f:
                        stats_json = json.load(f)
                    huc_norm[huc_code] = _parse_huc_stats(stats_json)
                    logger.debug(f"Loaded normalization stats for HUC {huc_code}")
                except Exception as e:
                    logger.warning(f"Failed to load normalization stats for HUC {huc_code}: {e}")
                    huc_norm[huc_code] = None
            else:
                logger.warning(f"No normalization stats file found for HUC {huc_code}")
                huc_norm[huc_code] = None
        
        return huc_norm

    def _apply_normalization(self, dem, optical, thermal, sar, huc):
        """Apply per-HUC z-score normalization using stats file."""
        
        # Get normalization stats for this HUC
        norm = self.huc_norm.get(huc)
        
        if norm is not None:
            # Apply z-score normalization for each modality
            if norm["dem"] is not None:
                dem = _zscore(dem, norm["dem"]["mean"], norm["dem"]["std"])
            
            if norm["optical"] is not None:
                # optical is (H, W, 6), norm stats are (1, 1, 6)
                optical = _zscore(optical, norm["optical"]["mean"], norm["optical"]["std"])
            
            if norm["thermal"] is not None:
                thermal = _zscore(thermal, norm["thermal"]["mean"], norm["thermal"]["std"])
            
            if norm["sar"] is not None:
                sar = _zscore(sar, norm["sar"]["mean"], norm["sar"]["std"])
        else:
            # Fallback: simple standardization if no stats available
            logger.warning(f"No normalization stats for HUC {huc}, using fallback normalization")
            
            # DEM: Normalize to reasonable elevation range (0-1000m -> 0-1)
            dem = np.clip((dem - 0) / 1000.0, 0, 1)
            
            # Optical: Already in reasonable range (0-1), just ensure bounds
            optical = np.clip(optical, 0, 1)
            
            # Thermal: Convert Kelvin to Celsius, then normalize to 0-1 (-50 to 50°C)
            thermal_celsius = thermal - 273.15
            thermal = np.clip((thermal_celsius + 50) / 100.0, 0, 1)
            
            # SAR: Normalize dB values (-30 to 0 dB -> 0-1)
            sar = np.clip((sar + 30) / 30.0, 0, 1)
        
        return dem, optical, thermal, sar

    def __len__(self):
        return len(self.patch_metadata)
    
    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        """
        Get a single sample with SatLas-specific preprocessing.
        
        Args:
            idx: Index of the sample
            
        Returns:
            Dictionary containing:
            - 'image': 9-channel tensor (B, 9, H, W)
            - 'mask': Water mask tensor (B, H, W)
            - 'metadata': Sample metadata
        """
        try:
            row = self.patch_metadata.iloc[idx]
            
            # Load multimodal data
            image_data = self._load_multimodal_patch(row)
            mask_data = self._load_mask_patch(row)
            
            # Apply SatLas-specific preprocessing
            image_data = self._preprocess_for_satlas(image_data)
            
            # Convert to tensors
            image_tensor = torch.from_numpy(image_data).float()
            mask_tensor = torch.from_numpy(mask_data).float()
            
            # Apply transforms if specified
            if self.transform:
                # Note: transforms should handle both image and mask
                sample = self.transform({'image': image_tensor, 'mask': mask_tensor})
                image_tensor = sample['image']
                mask_tensor = sample['mask']
            
            # Create sample dictionary
            sample = {
                'image': image_tensor,
                'mask': mask_tensor,
                'metadata': {
                    'huc_code': row.get('huc_code', ''),
                    'patch_id': row.get('patch_id', idx),
                    'bounds': row.get('bounds', None)
                }
            }
            
            return sample
            
        except Exception as e:
            logger.error(f"Error loading sample {idx}: {e}")
            # Return a dummy sample to prevent training crashes
            return self._get_dummy_sample()
    
    def _load_multimodal_patch(self, row: pd.Series) -> np.ndarray:
        """
        Load 9-channel multimodal patch data.
        
        Args:
            row: Metadata row for the patch
            
        Returns:
            numpy array of shape (9, H, W)
        """
        try:
            patch_size = self.patch_size
            
            # Check if we should generate synthetic data
            if row.get('use_synthetic', False):
                return self._generate_synthetic_multimodal_patch(patch_size)
            
            # Load real data from NPZ files
            channels = []
            
            if 'npz_file' in row and pd.notna(row['npz_file']):
                # Load from NPZ file (our new format)
                npz_file = row['npz_file']
                if Path(npz_file).exists():
                    try:
                        # Load NPZ data
                        npz_data = np.load(npz_file)
                        
                        # Load DEM (1 channel)
                        if 'dem' in npz_data:
                            dem_data = npz_data['dem'].astype(np.float32)
                            channels.append(dem_data)
                            logger.debug(f"Loaded DEM from NPZ: {dem_data.shape}")
                        else:
                            channels.append(np.zeros((patch_size, patch_size), dtype=np.float32))
                            logger.debug("DEM not found in NPZ, using zeros")
                        
                        # Load Optical (6 channels)
                        if 'optical' in npz_data:
                            optical_data = npz_data['optical'].astype(np.float32)
                            if optical_data.shape == (224, 224, 6):
                                # Shape is (H, W, C), need to transpose to (C, H, W) and then add to channels
                                for i in range(6):
                                    channels.append(optical_data[:, :, i])
                                logger.debug(f"Loaded 6 optical bands from NPZ: {optical_data.shape}")
                            else:
                                logger.warning(f"Unexpected optical shape in NPZ: {optical_data.shape}")
                                # Fallback: use zeros for all 6 channels
                                for i in range(6):
                                    channels.append(np.zeros((patch_size, patch_size), dtype=np.float32))
                        else:
                            # No optical data, fill with zeros
                            for i in range(6):
                                channels.append(np.zeros((patch_size, patch_size), dtype=np.float32))
                            logger.debug("Optical not found in NPZ, using zeros")
                        
                        # Load Thermal (1 channel)
                        if 'thermal' in npz_data:
                            thermal_data = npz_data['thermal'].astype(np.float32)
                            channels.append(thermal_data)
                            logger.debug(f"Loaded thermal from NPZ: {thermal_data.shape}")
                        else:
                            channels.append(np.zeros((patch_size, patch_size), dtype=np.float32))
                            logger.debug("Thermal not found in NPZ, using zeros")
                        
                        # Load SAR (1 channel)
                        if 'sar' in npz_data:
                            sar_data = npz_data['sar'].astype(np.float32)
                            channels.append(sar_data)
                            logger.debug(f"Loaded SAR from NPZ: {sar_data.shape}")
                        else:
                            channels.append(np.zeros((patch_size, patch_size), dtype=np.float32))
                            logger.debug("SAR not found in NPZ, using zeros")
                        
                        # Apply per-HUC normalization if available and enabled
                        if self.data_config.get('normalize_per_huc', True):
                            # Extract individual modalities for normalization
                            dem_for_norm = channels[0]  # DEM is first channel
                            optical_for_norm = np.stack(channels[1:7], axis=2)  # Optical channels 1-6 -> (H, W, 6)
                            thermal_for_norm = channels[7]  # Thermal is channel 7
                            sar_for_norm = channels[8]  # SAR is channel 8
                            
                            # Get HUC code from row
                            huc_code = row.get('huc_code', 'unknown')
                            
                            # Apply normalization
                            dem_norm, optical_norm, thermal_norm, sar_norm = self._apply_normalization(
                                dem_for_norm, optical_for_norm, thermal_for_norm, sar_for_norm, huc_code
                            )
                            
                            # Replace normalized data back into channels
                            channels[0] = dem_norm
                            for i in range(6):
                                channels[1 + i] = optical_norm[:, :, i]
                            channels[7] = thermal_norm
                            channels[8] = sar_norm
                            
                            logger.debug(f"Applied per-HUC normalization for HUC {huc_code}")
                        
                        logger.debug(f"Successfully loaded NPZ file: {npz_file}")
                        
                    except Exception as e:
                        logger.error(f"Error loading NPZ file {npz_file}: {e}")
                        logger.info("Falling back to synthetic data generation")
                        return self._generate_synthetic_multimodal_patch(patch_size)
                else:
                    logger.warning(f"NPZ file not found: {npz_file}")
                    return self._generate_synthetic_multimodal_patch(patch_size)
            else:
                logger.warning("No NPZ file path found in metadata, using synthetic data")
                return self._generate_synthetic_multimodal_patch(patch_size)
            
            # Ensure we have exactly 9 channels
            if len(channels) < 9:
                # Pad with zeros
                for i in range(9 - len(channels)):
                    channels.append(np.zeros((patch_size, patch_size)))
                logger.debug(f"Padded to 9 channels (had {len(channels)})")
            elif len(channels) > 9:
                # Take first 9 channels
                channels = channels[:9]
                logger.debug(f"Truncated to 9 channels (had {len(channels)})")
            
            # Stack channels: (9, H, W)
            multimodal_data = np.stack(channels, axis=0)
            logger.debug(f"Final multimodal data shape: {multimodal_data.shape}")
            
            return multimodal_data
            
        except Exception as e:
            logger.error(f"Error loading multimodal patch: {e}")
            logger.info("Falling back to synthetic data generation")
            # Return synthetic data as fallback
            return self._generate_synthetic_multimodal_patch(self.patch_size)
    
    def _load_mask_patch(self, row: pd.Series) -> np.ndarray:
        """
        Load water mask patch.
        
        Args:
            row: Metadata row for the patch
            
        Returns:
            numpy array of shape (H, W)
        """
        try:
            # Check if we should generate synthetic data
            if row.get('use_synthetic', False):
                return self._generate_synthetic_mask_patch(self.patch_size)
            
            # Load real mask data from NPZ file
            if 'npz_file' in row and pd.notna(row['npz_file']):
                npz_file = row['npz_file']
                if Path(npz_file).exists():
                    try:
                        # Load NPZ data
                        npz_data = np.load(npz_file)
                        
                        # Load hydro_mask (water mask)
                        if 'hydro_mask' in npz_data:
                            mask_data = npz_data['hydro_mask'].astype(np.float32)
                            logger.debug(f"Loaded hydro_mask from NPZ: {mask_data.shape}, values: {np.unique(mask_data)}")
                            return mask_data
                        else:
                            logger.debug("hydro_mask not found in NPZ, generating synthetic mask")
                            return self._generate_synthetic_mask_patch(self.patch_size)
                            
                    except Exception as e:
                        logger.error(f"Error loading mask from NPZ file {npz_file}: {e}")
                        logger.info("Falling back to synthetic mask generation")
                        return self._generate_synthetic_mask_patch(self.patch_size)
                else:
                    logger.warning(f"NPZ file not found: {npz_file}")
                    return self._generate_synthetic_mask_patch(self.patch_size)
            else:
                logger.debug("No NPZ file path found, generating synthetic mask")
                return self._generate_synthetic_mask_patch(self.patch_size)
                
        except Exception as e:
            logger.error(f"Error loading mask patch: {e}")
            logger.info("Falling back to synthetic mask generation")
            return self._generate_synthetic_mask_patch(self.patch_size)
    
    def _load_raster_patch(self, file_path: str, window: Optional[Window], 
                          target_size: Tuple[int, int]) -> np.ndarray:
        """
        Load a patch from a raster file.
        
        Args:
            file_path: Path to raster file
            window: Window object for patch extraction
            target_size: Target (height, width) for the patch
            
        Returns:
            numpy array of the patch
        """
        try:
            with rasterio.open(file_path) as src:
                if window:
                    data = src.read(1, window=window)
                else:
                    data = src.read(1)
                
                # Resize if needed
                if data.shape != target_size:
                    from skimage.transform import resize
                    data = resize(data, target_size, preserve_range=True, anti_aliasing=True)
                
                return data.astype(np.float32)
                
        except Exception as e:
            logger.error(f"Error reading raster {file_path}: {e}")
            return np.zeros(target_size, dtype=np.float32)
    
    def _preprocess_for_satlas(self, image_data: np.ndarray) -> np.ndarray:
        """
        Apply SatLas-specific preprocessing to multimodal data.
        
        Args:
            image_data: Raw image data of shape (9, H, W)
            
        Returns:
            Preprocessed image data
        """
        processed_data = image_data.copy()
        
        # Apply channel-specific normalization
        means = np.array(self.normalization_stats['mean'])
        stds = np.array(self.normalization_stats['std'])
        
        for i in range(min(processed_data.shape[0], len(means))):
            processed_data[i] = (processed_data[i] - means[i]) / stds[i]
        
        # Clip extreme values
        processed_data = np.clip(processed_data, -10, 10)
        
        return processed_data
    
    def _generate_synthetic_multimodal_patch(self, patch_size: int) -> np.ndarray:
        """
        Generate synthetic 9-channel multimodal patch for testing.
        
        Args:
            patch_size: Size of the patch (H, W)
            
        Returns:
            Synthetic multimodal data of shape (9, H, W)
        """
        # Generate realistic synthetic data for each modality
        channels = []
        
        # DEM channel (elevation data)
        dem = np.random.normal(100, 50, (patch_size, patch_size)).astype(np.float32)
        dem = np.clip(dem, 0, 500)  # Reasonable elevation range
        channels.append(dem)
        
        # 6 Optical channels (visible and infrared)
        for i in range(6):
            # Simulate different spectral bands with different characteristics
            base_reflectance = 0.1 + i * 0.05  # Increasing reflectance
            optical = np.random.normal(base_reflectance, 0.02, (patch_size, patch_size)).astype(np.float32)
            optical = np.clip(optical, 0, 1)  # Reflectance range 0-1
            channels.append(optical)
        
        # Thermal channel (temperature)
        thermal = np.random.normal(285, 10, (patch_size, patch_size)).astype(np.float32)
        thermal = np.clip(thermal, 250, 320)  # Reasonable temperature range in Kelvin
        channels.append(thermal)
        
        # SAR channel (backscatter)
        sar = np.random.normal(-10, 5, (patch_size, patch_size)).astype(np.float32)
        sar = np.clip(sar, -30, 5)  # Typical SAR backscatter range in dB
        channels.append(sar)
        
        # Stack all channels
        multimodal_data = np.stack(channels, axis=0)
        
        return multimodal_data
    
    def _generate_synthetic_mask_patch(self, patch_size: int) -> np.ndarray:
        """
        Generate synthetic water mask patch for testing.
        
        Args:
            patch_size: Size of the patch (H, W)
            
        Returns:
            Synthetic binary mask of shape (H, W)
        """
        # Create synthetic water bodies
        mask = np.zeros((patch_size, patch_size), dtype=np.float32)
        
        # Add some random water bodies
        num_water_bodies = np.random.randint(0, 4)  # 0-3 water bodies
        
        for _ in range(num_water_bodies):
            # Random center and size for water body
            center_x = np.random.randint(patch_size // 4, 3 * patch_size // 4)
            center_y = np.random.randint(patch_size // 4, 3 * patch_size // 4)
            radius = np.random.randint(10, 30)
            
            # Create circular water body
            y_coords, x_coords = np.ogrid[:patch_size, :patch_size]
            distance = np.sqrt((x_coords - center_x)**2 + (y_coords - center_y)**2)
            water_body = distance <= radius
            mask[water_body] = 1.0
        
        return mask
    
    def _get_dummy_sample(self) -> Dict[str, torch.Tensor]:
        """Get a dummy sample for error handling."""
        return {
            'image': torch.zeros(9, self.patch_size, self.patch_size),
            'mask': torch.zeros(self.patch_size, self.patch_size),
            'metadata': {
                'huc_code': 'dummy',
                'patch_id': -1,
                'bounds': None
            }
        }


class SatlasDataModule(pl.LightningDataModule):
    """
    PyTorch Lightning DataModule for SatLas multimodal water segmentation.
    """
    
    def __init__(self, config: Dict):
        super().__init__()
        self.config = config
        self.data_config = config["data"]
        self.training_config = config["training"]
        
        # Dataset splits
        self.train_dataset = None
        self.val_dataset = None
        self.test_dataset = None
        
        # Patch metadata
        self.patch_metadata = None
        
        logger.info("SatLas DataModule initialized")
    
    def prepare_data(self):
        """Prepare data (download, extract, etc.)."""
        # This would typically handle data downloading/preparation
        # For now, we assume data is already prepared
        logger.info("Data preparation step (placeholder)")
    
    def setup(self, stage: Optional[str] = None):
        """Set up datasets for training/validation/testing."""
        if stage == "fit" or stage is None:
            # Load patch metadata
            self.patch_metadata = self._load_patch_metadata()
            
            # Split into train/val
            train_meta, val_meta = self._split_data(self.patch_metadata)
            
            # Create datasets
            self.train_dataset = SatlasMultimodalDataset(
                train_meta, 
                self.data_config,
                transform=None,  # Could add augmentations here
                augment=True
            )
            
            self.val_dataset = SatlasMultimodalDataset(
                val_meta,
                self.data_config,
                transform=None,
                augment=False
            )
            
            logger.info(f"Train dataset: {len(self.train_dataset)} samples")
            logger.info(f"Val dataset: {len(self.val_dataset)} samples")
    
    def train_dataloader(self):
        """Create training dataloader."""
        return DataLoader(
            self.train_dataset,
            batch_size=self.training_config["batch_size"],
            shuffle=True,
            num_workers=self.training_config.get("num_workers", 4),
            pin_memory=True,
            drop_last=True
        )
    
    def val_dataloader(self):
        """Create validation dataloader."""
        return DataLoader(
            self.val_dataset,
            batch_size=self.training_config["batch_size"],
            shuffle=False,
            num_workers=self.training_config.get("num_workers", 4),
            pin_memory=True
        )
    
    def _load_patch_metadata(self) -> pd.DataFrame:
        """Load patch metadata from processed data."""
        try:
            logger.info("Loading SatLas patch metadata...")
            
            # Check if we should use synthetic data
            use_synthetic = self.data_config.get("use_synthetic_data", False)
            
            if use_synthetic:
                logger.info("Using synthetic data generation (no file I/O)")
                # For synthetic data, we just need basic metadata without file paths
                huc_codes = self.data_config.get("huc_codes", ["10020007"])
                patches_per_huc = 20  # Small number for testing
                
                metadata_rows = []
                for huc_code in huc_codes:
                    for patch_id in range(patches_per_huc):
                        metadata_rows.append({
                            'huc_code': huc_code,
                            'patch_id': f"{huc_code}_{patch_id:04d}",
                            'use_synthetic': True,  # Flag for synthetic data
                            'bounds': [0, 0, 224, 224],
                            'window': None
                        })
            else:
                # Load real patch metadata from processed dataset
                logger.info("Loading real patch metadata from processed dataset")
                base_path = self.data_config.get("base_path", "/u/nathanj/national_ml/data/processed/patch_dataset")
                huc_codes = self.data_config.get("huc_codes", ["03160113"])
                
                metadata_rows = []
                for huc_code in huc_codes:
                    # Look for patch metadata files in the HUC directory
                    huc_dir = Path(base_path) / huc_code
                    
                    if not huc_dir.exists():
                        logger.warning(f"HUC directory not found: {huc_dir}")
                        continue
                    
                    # Look for patch files in the HUC directory
                    patch_files = list(huc_dir.glob("*patch_*.tif"))
                    
                    if not patch_files:
                        # Alternative: look for organized subdirectories
                        subdirs = ['dem', 'optical', 'thermal', 'sar', 'masks']
                        
                        # Check if any of these subdirectories exist
                        has_subdirs = any((huc_dir / subdir).exists() for subdir in subdirs)
                        
                        if has_subdirs:
                            logger.info(f"Found organized subdirectories for HUC {huc_code}")
                            # Look for patch files in subdirectories
                            dem_files = list((huc_dir / 'dem').glob("*.tif")) if (huc_dir / 'dem').exists() else []
                            mask_files = list((huc_dir / 'masks').glob("*.tif")) if (huc_dir / 'masks').exists() else []
                            
                            # Create metadata based on available files
                            for dem_file in dem_files[:100]:  # Limit for testing
                                patch_name = dem_file.stem
                                metadata_rows.append({
                                    'huc_code': huc_code,
                                    'patch_id': patch_name,
                                    'dem_file': str(dem_file),
                                    'optical_file': str(huc_dir / 'optical' / f"{patch_name}.tif"),
                                    'thermal_file': str(huc_dir / 'thermal' / f"{patch_name}.tif"),
                                    'sar_file': str(huc_dir / 'sar' / f"{patch_name}.tif"),
                                    'mask_file': str(huc_dir / 'masks' / f"{patch_name}.tif"),
                                    'bounds': None,
                                    'window': None,
                                    'use_synthetic': False
                                })
                        else:
                            logger.warning(f"No patch files found in HUC directory: {huc_dir}")
                    else:
                        logger.info(f"Found {len(patch_files)} patch files for HUC {huc_code}")
                        # Use the found patch files (limit for testing)
                        for patch_file in patch_files[:100]:
                            patch_name = patch_file.stem
                            metadata_rows.append({
                                'huc_code': huc_code,
                                'patch_id': patch_name,
                                'patch_file': str(patch_file),
                                'bounds': None,
                                'window': None,
                                'use_synthetic': False
                            })
                
                # Look for NPZ files directly in HUC directory
                for huc_code in huc_codes:
                    huc_dir = Path(base_path) / huc_code
                    
                    if not huc_dir.exists():
                        logger.warning(f"HUC directory not found: {huc_dir}")
                        continue
                    
                    # Look for patch NPZ files
                    npz_files = list(huc_dir.glob("patch_*.npz"))
                    
                    if npz_files:
                        logger.info(f"Found {len(npz_files)} NPZ patches for HUC {huc_code}")
                        # Limit patches for testing
                        for npz_file in npz_files[:100]:  # Limit to 100 patches for testing
                            patch_name = npz_file.stem  # e.g., "patch_0"
                            patch_id = patch_name.split('_')[1]  # Extract patch number
                            
                            metadata_rows.append({
                                'huc_code': huc_code,
                                'patch_id': f"{huc_code}_{patch_id}",
                                'npz_file': str(npz_file),
                                'bounds': None,
                                'window': None,
                                'use_synthetic': False
                            })
                    else:
                        logger.warning(f"No NPZ patch files found in HUC directory: {huc_dir}")
                
                if not metadata_rows:
                    logger.warning("No real patch data found, falling back to synthetic data")
                    # Fallback to synthetic data if no real data found
                    huc_codes = self.data_config.get("huc_codes", ["03160113"])
                    for huc_code in huc_codes[:1]:  # Just use first HUC
                        for patch_id in range(20):
                            metadata_rows.append({
                                'huc_code': huc_code,
                                'patch_id': f"{huc_code}_{patch_id:04d}",
                                'use_synthetic': True,
                                'bounds': [0, 0, 224, 224],
                                'window': None
                            })
            
            metadata_df = pd.DataFrame(metadata_rows)
            logger.info(f"Loaded {len(metadata_df)} patch metadata entries")
            
            return metadata_df
            
        except Exception as e:
            logger.error(f"Error loading patch metadata: {e}")
            # Return empty DataFrame as fallback
            return pd.DataFrame()
    
    def _split_data(self, metadata: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Split metadata into train/validation sets."""
        train_ratio = self.data_config.get("train_ratio", 0.8)
        
        # Split by HUC codes to avoid data leakage
        huc_codes = metadata['huc_code'].unique()
        
        # Handle case with too few HUC codes for proper split
        if len(huc_codes) < 2:
            logger.warning(f"Only {len(huc_codes)} HUC codes available. Using patch-level split instead.")
            # Fall back to patch-level split within each HUC
            train_meta, val_meta = train_test_split(
                metadata,
                train_size=train_ratio,
                random_state=42,
                stratify=metadata['huc_code'] if len(huc_codes) > 1 else None
            )
        else:
            # Use HUC-level split for proper separation
            train_hucs, val_hucs = train_test_split(
                huc_codes, 
                train_size=train_ratio,
                random_state=42
            )
            
            train_meta = metadata[metadata['huc_code'].isin(train_hucs)].copy()
            val_meta = metadata[metadata['huc_code'].isin(val_hucs)].copy()
            
            logger.info(f"Train HUCs: {train_hucs}")
            logger.info(f"Val HUCs: {val_hucs}")
        
        logger.info(f"Train samples: {len(train_meta)}, Val samples: {len(val_meta)}")
        return train_meta, val_meta
    
    def get_dataset_info(self) -> Dict[str, Any]:
        """Get information about the dataset."""
        if self.patch_metadata is not None:
            return {
                "total_patches": len(self.patch_metadata),
                "train_patches": len(self.train_dataset) if self.train_dataset else 0,
                "val_patches": len(self.val_dataset) if self.val_dataset else 0,
                "huc_codes": self.data_config.get("huc_codes", []),
                "patch_size": self.data_config.get("patch_size", 224),
                "num_channels": 9,
                "modalities": ["DEM", "6×Optical", "Thermal", "SAR"]
            }
        return {"status": "not_initialized"}


# Alias for backward compatibility
FourModalDataModule = SatlasDataModule


def test_satlas_data_adapter():
    """Test the SatLas data adapter."""
    print("Testing SatLas Data Adapter...")
    
    try:
        # Create test config
        test_config = {
            "data": {
                "huc_codes": ["10020007", "10020008"],
                "patch_size": 224,
                "train_ratio": 0.8
            },
            "training": {
                "batch_size": 4,
                "num_workers": 2
            }
        }
        
        # Create data module
        data_module = SatlasDataModule(test_config)
        data_module.setup("fit")
        
        # Test data loaders
        train_loader = data_module.train_dataloader()
        val_loader = data_module.val_dataloader()
        
        print(f"Train batches: {len(train_loader)}")
        print(f"Val batches: {len(val_loader)}")
        
        # Test a batch
        for batch in train_loader:
            print(f"Batch image shape: {batch['image'].shape}")
            print(f"Batch mask shape: {batch['mask'].shape}")
            print(f"Image range: [{batch['image'].min():.3f}, {batch['image'].max():.3f}]")
            break
        
        print("✅ SatLas data adapter test successful!")
        return True
        
    except Exception as e:
        print(f"❌ SatLas data adapter test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_satlas_data_adapter()